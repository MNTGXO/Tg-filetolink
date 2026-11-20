package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// dcConn is a media connection pool to a foreign datacenter (used when a file
// lives in a different DC than the bot's home DC, i.e. FILE_MIGRATE_X).
type dcConn struct {
	api    *tg.Client
	closer func() error
}

// Worker is one authenticated bot client in the pool.
type Worker struct {
	idx     int
	token   string
	primary bool

	busy   atomic.Int64 // number of active streams
	ready  atomic.Bool
	homeDC atomic.Int64
	api    atomic.Pointer[tg.Client]

	readyOnce sync.Once
	readyCh   chan struct{}

	mu      sync.Mutex
	client  *telegram.Client
	runCtx  context.Context
	channel *tg.InputChannel
	dcs     map[int]*dcConn
}

// Pool is a set of bot clients. Requests are spread over the pool so that
// Telegram's per-session throttling is bypassed.
type Pool struct {
	appID   int
	appHash string
	workers []*Worker
	rr      atomic.Uint64
}

// NewPool builds (but does not start) a pool: Tokens[0] is the primary bot.
func NewPool(cfg *Config) *Pool {
	p := &Pool{appID: cfg.APIID, appHash: cfg.APIHash}
	for i, t := range cfg.Tokens {
		p.workers = append(p.workers, &Worker{
			idx:     i,
			token:   t,
			primary: i == 0,
			readyCh: make(chan struct{}),
			dcs:     make(map[int]*dcConn),
		})
	}
	return p
}

// Primary returns the bot that runs the update dispatcher.
func (p *Pool) Primary() *Worker { return p.workers[0] }

// Ready is closed once the worker has authenticated for the first time.
func (w *Worker) Ready() <-chan struct{} { return w.readyCh }

// ReadyCount returns how many workers are currently usable.
func (p *Pool) ReadyCount() int {
	n := 0
	for _, w := range p.workers {
		if w.ready.Load() {
			n++
		}
	}
	return n
}

// Start launches every client concurrently (lightly staggered to avoid
// tripping Telegram's auth flood limits). It returns immediately; wg is
// released when every client has stopped after ctx is cancelled.
func (p *Pool) Start(ctx context.Context, handler telegram.UpdateHandler, wg *sync.WaitGroup) {
	wg.Add(len(p.workers))
	for _, w := range p.workers {
		go func(w *Worker) {
			defer wg.Done()
			p.run(ctx, w, handler)
		}(w)
	}
}

// run keeps one bot client alive: authenticate, serve, reconnect on failure.
func (p *Pool) run(ctx context.Context, w *Worker, handler telegram.UpdateHandler) {
	select {
	case <-time.After(time.Duration(w.idx) * 150 * time.Millisecond):
	case <-ctx.Done():
		return
	}

	backoff := 2 * time.Second
	for ctx.Err() == nil {
		opts := telegram.Options{
			SessionStorage: &session.StorageMemory{}, // zero disk writes
			NoUpdates:      !w.primary,               // only the primary receives updates
		}
		if w.primary {
			opts.UpdateHandler = handler
		}
		client := telegram.NewClient(p.appID, p.appHash, opts)

		started := time.Now()
		err := client.Run(ctx, func(runCtx context.Context) error {
			if _, err := client.Auth().Bot(runCtx, w.token); err != nil {
				return fmt.Errorf("bot auth: %w", err)
			}
			w.attach(runCtx, client)
			log.Printf("worker %d ready (primary=%v, home DC %d)", w.idx, w.primary, w.homeDC.Load())
			<-runCtx.Done()
			return nil
		})
		w.detach()
		if ctx.Err() != nil {
			return
		}

		var rpcErr *tgerr.Error
		if errors.As(err, &rpcErr) && rpcErr.IsType("ACCESS_TOKEN_INVALID") {
			log.Printf("worker %d: token rejected by Telegram (ACCESS_TOKEN_INVALID); giving up on it", w.idx)
			return
		}

		if time.Since(started) > time.Minute {
			backoff = 2 * time.Second
		}
		log.Printf("worker %d stopped: %v (retrying in %s)", w.idx, err, backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, 60*time.Second)
	}
}

func (w *Worker) attach(ctx context.Context, c *telegram.Client) {
	w.mu.Lock()
	w.client = c
	w.runCtx = ctx
	w.channel = nil
	w.dcs = make(map[int]*dcConn)
	w.mu.Unlock()

	w.homeDC.Store(int64(c.Config().ThisDC))
	w.api.Store(c.API())
	w.ready.Store(true)
	w.readyOnce.Do(func() { close(w.readyCh) })
}

func (w *Worker) detach() {
	w.ready.Store(false)
	w.api.Store(nil)

	w.mu.Lock()
	for _, d := range w.dcs {
		if d.closer != nil {
			_ = d.closer()
		}
	}
	w.dcs = make(map[int]*dcConn)
	w.client = nil
	w.runCtx = nil
	w.channel = nil
	w.mu.Unlock()
}

// Acquire picks the least-busy ready worker (round-robin on ties) and marks it
// busy. The returned func must be called when the request finishes.
func (p *Pool) Acquire() (*Worker, func()) {
	n := len(p.workers)
	if n == 0 {
		return nil, nil
	}
	start := int((p.rr.Add(1) - 1) % uint64(n))

	var best *Worker
	var bestLoad int64
	for i := 0; i < n; i++ {
		w := p.workers[(start+i)%n]
		if !w.ready.Load() {
			continue
		}
		if l := w.busy.Load(); best == nil || l < bestLoad {
			best, bestLoad = w, l
		}
	}
	if best == nil {
		return nil, nil
	}
	best.busy.Add(1)
	return best, func() { best.busy.Add(-1) }
}

// inputChannel resolves BIN_CHANNEL for this bot (access hashes are per account).
// The bot must be a member/admin of the channel.
func (w *Worker) inputChannel(ctx context.Context, channelID int64) (*tg.InputChannel, error) {
	w.mu.Lock()
	ch := w.channel
	w.mu.Unlock()
	if ch != nil {
		return ch, nil
	}

	api := w.api.Load()
	if api == nil {
		return nil, errNotReady
	}

	res, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{&tg.InputChannel{ChannelID: channelID}})
	if err != nil {
		return nil, err
	}
	var hash int64
	if g, ok := res.(interface{ GetChats() []tg.ChatClass }); ok {
		for _, c := range g.GetChats() {
			if chn, ok := c.(*tg.Channel); ok && chn.ID == channelID {
				hash = chn.AccessHash
			}
		}
	}
	ch = &tg.InputChannel{ChannelID: channelID, AccessHash: hash}

	w.mu.Lock()
	w.channel = ch
	w.mu.Unlock()
	return ch, nil
}

// fileAPI returns the tg.Client that must be used for upload.getFile on a file
// stored in datacenter dc: the home client, or a cached media pool for a
// foreign DC (explicit handling of FILE_MIGRATE_X).
func (w *Worker) fileAPI(dc int) (*tg.Client, error) {
	api := w.api.Load()
	if api == nil {
		return nil, errNotReady
	}
	if dc == 0 || int64(dc) == w.homeDC.Load() {
		return api, nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if d, ok := w.dcs[dc]; ok {
		return d.api, nil
	}
	if w.client == nil || w.runCtx == nil {
		return nil, errNotReady
	}
	inv, err := w.client.DC(w.runCtx, dc, 2)
	if err != nil {
		return nil, fmt.Errorf("connect to DC %d: %w", dc, err)
	}
	conn := &dcConn{api: tg.NewClient(inv), closer: inv.Close}
	w.dcs[dc] = conn
	return conn.api, nil
}
