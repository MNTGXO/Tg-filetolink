package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

const (
	// Telegram requires offset % 4 KiB == 0 and the requested part to stay inside
	// one 1 MiB-aligned block. 512 KiB chunks at 512 KiB-aligned offsets satisfy both.
	chunkSize = 512 * 1024
	// Number of chunks fetched ahead of the writer (RAM per stream ~ (prefetch+2) * 512 KiB).
	prefetch = 3
)

var (
	errNotFound        = errors.New("file not found")
	errNotReady        = errors.New("backend not ready")
	errCDN             = errors.New("cdn redirect is not supported")
	errRangeInvalid    = errors.New("invalid range")
	errRangeUnsatisfied = errors.New("range not satisfiable")
)

// fileMeta is the lightweight metadata kept in the TTL cache.
type fileMeta struct {
	ID   int64
	Name string
	Mime string
	Size int64
}

// ---------------------------------------------------------------------------
// Media parsing
// ---------------------------------------------------------------------------

func defaultName(prefix string, id int64, mimeType string) string {
	ext := ".bin"
	switch strings.ToLower(mimeType) {
	case "video/mp4":
		ext = ".mp4"
	case "video/x-matroska":
		ext = ".mkv"
	case "video/webm":
		ext = ".webm"
	case "audio/mpeg":
		ext = ".mp3"
	case "audio/ogg":
		ext = ".ogg"
	case "audio/mp4", "audio/x-m4a":
		ext = ".m4a"
	case "image/jpeg":
		ext = ".jpg"
	case "image/png":
		ext = ".png"
	case "image/webp":
		ext = ".webp"
	case "application/pdf":
		ext = ".pdf"
	case "application/zip":
		ext = ".zip"
	}
	return fmt.Sprintf("%s_%d%s", prefix, id, ext)
}

// parseMedia extracts metadata, the input file location and the DC id from a
// message that carries a document or a photo.
func parseMedia(msg *tg.Message) (fileMeta, tg.InputFileLocationClass, int, bool) {
	switch m := msg.Media.(type) {
	case *tg.MessageMediaDocument:
		doc, ok := m.Document.AsNotEmpty()
		if !ok {
			return fileMeta{}, nil, 0, false
		}
		name := ""
		for _, a := range doc.Attributes {
			if fn, ok := a.(*tg.DocumentAttributeFilename); ok {
				name = fn.FileName
			}
		}
		if name == "" {
			name = defaultName("file", doc.ID, doc.MimeType)
		}
		meta := fileMeta{ID: doc.ID, Name: name, Mime: doc.MimeType, Size: int64(doc.Size)}
		loc := &tg.InputDocumentFileLocation{
			ID:            doc.ID,
			AccessHash:    doc.AccessHash,
			FileReference: doc.FileReference,
			ThumbSize:     "",
		}
		return meta, loc, doc.DCID, true

	case *tg.MessageMediaPhoto:
		photo, ok := m.Photo.AsNotEmpty()
		if !ok {
			return fileMeta{}, nil, 0, false
		}
		var bestType string
		var bestSize int64
		for _, sz := range photo.Sizes {
			switch s := sz.(type) {
			case *tg.PhotoSize:
				if int64(s.Size) > bestSize {
					bestSize, bestType = int64(s.Size), s.Type
				}
			case *tg.PhotoSizeProgressive:
				if n := len(s.Sizes); n > 0 && int64(s.Sizes[n-1]) > bestSize {
					bestSize, bestType = int64(s.Sizes[n-1]), s.Type
				}
			}
		}
		if bestType == "" || bestSize <= 0 {
			return fileMeta{}, nil, 0, false
		}
		meta := fileMeta{
			ID:   photo.ID,
			Name: defaultName("photo", photo.ID, "image/jpeg"),
			Mime: "image/jpeg",
			Size: bestSize,
		}
		loc := &tg.InputPhotoFileLocation{
			ID:            photo.ID,
			AccessHash:    photo.AccessHash,
			FileReference: photo.FileReference,
			ThumbSize:     bestType,
		}
		return meta, loc, photo.DCID, true
	}
	return fileMeta{}, nil, 0, false
}

// ---------------------------------------------------------------------------
// Metadata TTL cache
// ---------------------------------------------------------------------------

// entry caches one BIN_CHANNEL message. File locations (document id, access
// hash, file reference) are valid per bot account, so they are kept per worker
// and fetched lazily; parallel Range connections for the same file do not call
// channels.GetMessages again.
type entry struct {
	id      int
	meta    fileMeta
	expires time.Time

	loaded chan struct{} // closed when meta/err are final
	err    error

	mu   sync.Mutex
	dc   int
	locs map[int]tg.InputFileLocationClass // worker idx -> location
}

type MetaCache struct {
	mu  sync.Mutex
	m   map[int]*entry
	ttl time.Duration
	max int
}

func NewMetaCache(ttl time.Duration, max int) *MetaCache {
	return &MetaCache{m: make(map[int]*entry), ttl: ttl, max: max}
}

func (c *MetaCache) evictLocked() {
	now := time.Now()
	for k, e := range c.m {
		if now.After(e.expires) {
			delete(c.m, k)
		}
	}
	if len(c.m) < c.max {
		return
	}
	drop := c.max/10 + 1
	for k := range c.m {
		delete(c.m, k)
		if drop--; drop <= 0 {
			break
		}
	}
}

// Janitor periodically drops expired entries until ctx is cancelled.
func (c *MetaCache) Janitor(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.mu.Lock()
			now := time.Now()
			for k, e := range c.m {
				if now.After(e.expires) {
					delete(c.m, k)
				}
			}
			c.mu.Unlock()
		}
	}
}

// Server wires config, pool and cache into HTTP handlers.
type Server struct {
	cfg   *Config
	pool  *Pool
	cache *MetaCache
}

// getEntry returns cached metadata, loading it once even when many Range
// connections for the same file arrive simultaneously.
func (s *Server) getEntry(ctx context.Context, id int) (*entry, error) {
	c := s.cache
	c.mu.Lock()
	if e, ok := c.m[id]; ok && time.Now().Before(e.expires) {
		c.mu.Unlock()
		select {
		case <-e.loaded:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if e.err != nil {
			return nil, e.err
		}
		return e, nil
	}
	if len(c.m) >= c.max {
		c.evictLocked()
	}
	e := &entry{
		id:      id,
		expires: time.Now().Add(c.ttl),
		loaded:  make(chan struct{}),
		locs:    make(map[int]tg.InputFileLocationClass),
	}
	c.m[id] = e
	c.mu.Unlock()

	// Detached context: a disconnecting first client must not fail the waiters.
	lctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := s.loadEntry(lctx, e)
	if err != nil {
		e.err = err
		c.mu.Lock()
		if c.m[id] == e {
			delete(c.m, id)
		}
		c.mu.Unlock()
	}
	close(e.loaded)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (s *Server) loadEntry(ctx context.Context, e *entry) error {
	wk, release := s.pool.Acquire()
	if wk == nil {
		return errNotReady
	}
	defer release()

	meta, loc, dc, err := s.fetchMedia(ctx, wk, e.id)
	if err != nil {
		return err
	}
	e.meta = meta
	e.mu.Lock()
	e.dc = dc
	e.locs[wk.idx] = loc
	e.mu.Unlock()
	return nil
}

// fetchMedia reads one message from BIN_CHANNEL using the given bot.
func (s *Server) fetchMedia(ctx context.Context, wk *Worker, id int) (fileMeta, tg.InputFileLocationClass, int, error) {
	api := wk.api.Load()
	if api == nil {
		return fileMeta{}, nil, 0, errNotReady
	}
	ch, err := wk.inputChannel(ctx, s.cfg.BinChannelID)
	if err != nil {
		return fileMeta{}, nil, 0, fmt.Errorf("resolve BIN_CHANNEL: %w", err)
	}
	res, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: ch,
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: id}},
	})
	if err != nil {
		return fileMeta{}, nil, 0, err
	}
	g, ok := res.(interface{ GetMessages() []tg.MessageClass })
	if !ok {
		return fileMeta{}, nil, 0, errNotFound
	}
	for _, mc := range g.GetMessages() {
		// tg.MessageEmpty (deleted message) falls through to errNotFound.
		if m, ok := mc.(*tg.Message); ok && m.ID == id {
			if meta, loc, dc, ok := parseMedia(m); ok {
				return meta, loc, dc, nil
			}
		}
	}
	return fileMeta{}, nil, 0, errNotFound
}

// locFor returns this worker's file location for the entry, fetching it once.
func (s *Server) locFor(ctx context.Context, e *entry, wk *Worker) (tg.InputFileLocationClass, int, error) {
	e.mu.Lock()
	loc, ok := e.locs[wk.idx]
	dc := e.dc
	e.mu.Unlock()
	if ok {
		return loc, dc, nil
	}
	_, loc, dc, err := s.fetchMedia(ctx, wk, e.id)
	if err != nil {
		return nil, 0, err
	}
	e.mu.Lock()
	e.locs[wk.idx] = loc
	e.dc = dc
	e.mu.Unlock()
	return loc, dc, nil
}

func (e *entry) invalidate(idx int) {
	e.mu.Lock()
	delete(e.locs, idx)
	e.mu.Unlock()
}

func (e *entry) setDC(dc int) {
	e.mu.Lock()
	e.dc = dc
	e.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Chunk fetching and streaming
// ---------------------------------------------------------------------------

// fetchChunk downloads the 512 KiB chunk that starts at the aligned offset off.
func (s *Server) fetchChunk(ctx context.Context, e *entry, wk *Worker, off int64) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		loc, dc, err := s.locFor(ctx, e, wk)
		if err != nil {
			return nil, err
		}
		api, err := wk.fileAPI(dc)
		if err != nil {
			return nil, err
		}

		res, err := api.UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Location: loc,
			Offset:   off,
			Limit:    chunkSize,
		})
		if err == nil {
			f, ok := res.(*tg.UploadFile)
			if !ok {
				return nil, errCDN
			}
			return f.Bytes, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var rpcErr *tgerr.Error
		if !errors.As(err, &rpcErr) {
			return nil, err
		}
		switch {
		case rpcErr.IsType("FILE_REFERENCE_EXPIRED"):
			e.invalidate(wk.idx) // re-read the message for a fresh file reference
		case rpcErr.IsType("FILE_MIGRATE"):
			e.setDC(rpcErr.Argument) // use a dedicated media connection to that DC
		case rpcErr.IsType("FLOOD_WAIT"):
			wait := time.Duration(rpcErr.Argument) * time.Second
			if wait > 10*time.Second {
				return nil, err
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case rpcErr.IsType("AUTH_BYTES_INVALID"), rpcErr.IsType("OFFSET_INVALID"), rpcErr.IsType("LIMIT_INVALID"):
			return nil, err
		default:
			return nil, err
		}
	}
	return nil, lastErr
}

type chunkResult struct {
	data []byte
	err  error
}

// streamRange writes bytes [start, end] (inclusive) to w. Chunks are fetched
// ahead of the writer (ordered pipeline) and written straight to the response
// with a flush per chunk: no disk, no big buffers. Everything stops as soon as
// ctx (the client's request context) is cancelled.
func (s *Server) streamRange(ctx context.Context, w http.ResponseWriter, e *entry, wk *Worker, start, end int64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	flusher, _ := w.(http.Flusher)
	first := start - start%chunkSize

	pending := make(chan chan chunkResult, prefetch)
	go func() {
		defer close(pending)
		for off := first; off <= end; off += chunkSize {
			rc := make(chan chunkResult, 1) // buffered: fetcher never blocks/leaks
			go func(off int64) {
				data, err := s.fetchChunk(ctx, e, wk, off)
				rc <- chunkResult{data: data, err: err}
			}(off)
			select {
			case pending <- rc:
			case <-ctx.Done():
				return
			}
		}
	}()

	cur := first
	for rc := range pending {
		var res chunkResult
		select {
		case res = <-rc:
		case <-ctx.Done():
			return ctx.Err()
		}
		if res.err != nil {
			return res.err
		}

		data := res.data
		lo := int64(0)
		if start > cur {
			lo = start - cur
		}
		hi := int64(len(data))
		if rem := end + 1 - cur; rem < hi {
			hi = rem
		}
		if lo >= hi {
			return io.ErrUnexpectedEOF
		}
		if _, err := w.Write(data[lo:hi]); err != nil {
			return err // client went away
		}
		if flusher != nil {
			flusher.Flush()
		}
		// A short chunk before the requested end means Telegram has less data than advertised.
		if len(data) < chunkSize && cur+int64(len(data)) < end+1 {
			return io.ErrUnexpectedEOF
		}
		cur += chunkSize
	}
	if cur <= end {
		if err := ctx.Err(); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	return nil
}

// ---------------------------------------------------------------------------
// HTTP Range parsing (RFC 9110 section 14)
// ---------------------------------------------------------------------------

// parseRange parses a single-range "Range: bytes=..." header for a resource of
// the given size. It returns (start, end, partial). Headers with another unit
// or with multiple ranges are ignored (the full body is served), as RFC 9110
// allows. Malformed or unsatisfiable ranges return an error (-> 416).
func parseRange(h string, size int64) (start, end int64, partial bool, err error) {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0, size - 1, false, nil
	}
	if !strings.HasPrefix(strings.ToLower(h), "bytes=") {
		return 0, size - 1, false, nil
	}
	spec := strings.TrimSpace(h[len("bytes="):])
	if strings.Contains(spec, ",") {
		return 0, size - 1, false, nil
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false, errRangeInvalid
	}
	a, b := strings.TrimSpace(spec[:dash]), strings.TrimSpace(spec[dash+1:])

	if a == "" { // suffix range: last N bytes
		n, perr := strconv.ParseInt(b, 10, 64)
		if perr != nil || n <= 0 {
			return 0, 0, false, errRangeInvalid
		}
		if size == 0 {
			return 0, 0, false, errRangeUnsatisfied
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, true, nil
	}

	start, perr := strconv.ParseInt(a, 10, 64)
	if perr != nil || start < 0 {
		return 0, 0, false, errRangeInvalid
	}
	if b == "" {
		end = size - 1
	} else {
		end, perr = strconv.ParseInt(b, 10, 64)
		if perr != nil || end < start {
			return 0, 0, false, errRangeInvalid
		}
	}
	if start >= size {
		return 0, 0, false, errRangeUnsatisfied
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true, nil
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

// handleHealth is used by Koyeb health checks and by the Worker's cron ping.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}

func (s *Server) writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errNotFound):
		http.Error(w, "file not found", http.StatusNotFound)
	case errors.Is(err, errNotReady):
		w.Header().Set("Retry-After", "3")
		http.Error(w, "backend is starting, retry shortly", http.StatusServiceUnavailable)
	case errors.Is(err, context.Canceled):
		// client disconnected; nothing useful to send
	default:
		log.Printf("upstream error: %v", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
	}
}

// handleStream serves GET/HEAD /stream/{id} with full Range support.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	// Only the Cloudflare Worker may call this endpoint.
	got := []byte(r.Header.Get("X-Worker-Secret"))
	if subtle.ConstantTimeCompare(got, []byte(s.cfg.WorkerSecret)) != 1 {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil || id <= 0 {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}

	e, err := s.getEntry(r.Context(), id)
	if err != nil {
		s.writeErr(w, err)
		return
	}

	size := e.meta.Size
	etag := fmt.Sprintf(`"%x-%d"`, e.meta.ID, size)

	if inm := r.Header.Get("If-None-Match"); inm != "" && (inm == "*" || strings.Contains(inm, etag)) {
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusNotModified)
		return
	}

	rangeHdr := r.Header.Get("Range")
	if ir := r.Header.Get("If-Range"); ir != "" && ir != etag {
		rangeHdr = "" // validator mismatch: send the whole file
	}

	start, end, partial, rerr := parseRange(rangeHdr, size)
	if rerr != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	ct := e.meta.Mime
	if ct == "" {
		ct = "application/octet-stream"
	}
	disposition := "attachment"
	if r.URL.Query().Get("inline") == "1" {
		disposition = "inline"
	}
	cd := mime.FormatMediaType(disposition, map[string]string{"filename": e.meta.Name})
	if cd == "" {
		cd = disposition
	}

	length := end - start + 1
	if size == 0 {
		length = 0
	}

	h := w.Header()
	h.Set("Content-Type", ct)
	h.Set("Accept-Ranges", "bytes")
	h.Set("ETag", etag)
	h.Set("Content-Disposition", cd)
	h.Set("Cache-Control", "private, no-transform")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Length", strconv.FormatInt(length, 10))
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
	}

	if r.Method == http.MethodHead || length == 0 {
		w.WriteHeader(status)
		return
	}

	wk, release := s.pool.Acquire()
	if wk == nil {
		h.Del("Content-Length")
		h.Del("Content-Range")
		s.writeErr(w, errNotReady)
		return
	}
	defer release()

	w.WriteHeader(status)
	if err := s.streamRange(r.Context(), w, e, wk, start, end); err != nil &&
		!errors.Is(err, context.Canceled) && r.Context().Err() == nil {
		// Headers are already sent; abort so the client sees a truncated body
		// and can resume with a Range request.
		log.Printf("stream %d (%d-%d) via worker %d failed: %v", id, start, end, wk.idx, err)
		panic(http.ErrAbortHandler)
	}
}
