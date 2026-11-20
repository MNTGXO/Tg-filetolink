package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/tg"
)

// signID returns the hex HMAC-SHA256 of the message id. The Worker recomputes
// it with the same HMAC_SECRET, so links cannot be forged or enumerated.
func signID(secret string, id int) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.Itoa(id)))
	return hex.EncodeToString(mac.Sum(nil))
}

func humanSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// forwardedMsgID finds the id of the message that landed in BIN_CHANNEL.
func forwardedMsgID(upd tg.UpdatesClass) (int, bool) {
	var list []tg.UpdateClass
	switch v := upd.(type) {
	case *tg.Updates:
		list = v.Updates
	case *tg.UpdatesCombined:
		list = v.Updates
	}
	for _, u := range list {
		if nm, ok := u.(*tg.UpdateNewChannelMessage); ok {
			if m, ok := nm.Message.(*tg.Message); ok {
				return m.ID, true
			}
		}
	}
	return 0, false
}

// registerBot installs the primary bot's message handler.
func registerBot(d *tg.UpdateDispatcher, cfg *Config, pool *Pool) {
	d.OnNewMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
		msg, ok := u.Message.(*tg.Message)
		if !ok || msg.Out {
			return nil
		}
		peer, ok := msg.PeerID.(*tg.PeerUser) // private chats only
		if !ok {
			return nil
		}
		user, ok := e.Users[peer.UserID]
		if !ok {
			return nil
		}
		primary := pool.Primary()
		api := primary.api.Load()
		if api == nil {
			return nil
		}
		say := func(text string) error {
			_, err := message.NewSender(api).Reply(e, u).Text(ctx, text)
			return err
		}

		if len(cfg.AllowedUsers) > 0 {
			if _, ok := cfg.AllowedUsers[user.ID]; !ok {
				return say("You are not allowed to use this bot.")
			}
		}

		meta, _, _, hasMedia := parseMedia(msg)
		if !hasMedia {
			return say("Send or forward me any file, video, audio or photo and I will reply with a direct download link and a stream link.")
		}

		ch, err := primary.inputChannel(ctx, cfg.BinChannelID)
		if err != nil {
			log.Printf("bot: resolve BIN_CHANNEL: %v", err)
			return say("Storage channel is not reachable. Make sure the bot is an admin of BIN_CHANNEL.")
		}

		var rb [8]byte
		if _, err := rand.Read(rb[:]); err != nil {
			return say("Internal error, please try again.")
		}
		randomID := int64(binary.LittleEndian.Uint64(rb[:]))

		upd, err := api.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
			FromPeer:   &tg.InputPeerUser{UserID: user.ID, AccessHash: user.AccessHash},
			ID:         []int{msg.ID},
			RandomID:   []int64{randomID},
			ToPeer:     &tg.InputPeerChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash},
			DropAuthor: true, // copy instead of showing "forwarded from"
		})
		if err != nil {
			log.Printf("bot: forward failed: %v", err)
			return say("Could not store that file (it may be protected from forwarding). Please try another one.")
		}
		binID, ok := forwardedMsgID(upd)
		if !ok {
			return say("Could not determine the stored message id. Please try again.")
		}

		sig := signID(cfg.HMACSecret, binID)
		dl := fmt.Sprintf("%s/dl/%d?hash=%s", cfg.WorkerURL, binID, sig)
		st := dl + "&inline=1"

		return say(fmt.Sprintf("Your link is ready\n\nFile: %s\nSize: %s\n\nDownload:\n%s\n\nStream:\n%s",
			meta.Name, humanSize(meta.Size), dl, st))
	})
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := NewPool(cfg)
	dispatcher := tg.NewUpdateDispatcher()
	registerBot(&dispatcher, cfg, pool)

	srv := &Server{
		cfg:   cfg,
		pool:  pool,
		cache: NewMetaCache(cfg.MetaTTL, cfg.MetaMax),
	}
	go srv.cache.Janitor(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", srv.handleHealth) // also answers HEAD
	mux.HandleFunc("GET /stream/{id}", srv.handleStream) // also answers HEAD

	httpSrv := &http.Server{
		Addr:              "0.0.0.0:" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    16 << 10,
		// No WriteTimeout: large downloads are long-lived; Range requests handle resumes.
	}

	// Serve HTTP immediately so /health answers during Telegram auth (fast cold starts).
	httpErr := make(chan error, 1)
	go func() {
		log.Printf("listening on %s (%d bot token(s))", httpSrv.Addr, len(cfg.Tokens))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()

	var wg sync.WaitGroup
	pool.Start(ctx, &dispatcher, &wg)

	// The primary bot must come up; the rest join the pool as they authenticate.
	go func() {
		select {
		case <-pool.Primary().Ready():
			log.Printf("primary bot authenticated")
		case <-time.After(2 * time.Minute):
			log.Printf("primary bot failed to authenticate within 2 minutes; exiting")
			stop()
		case <-ctx.Done():
		}
	}()

	exitCode := 0
	select {
	case <-ctx.Done():
	case err := <-httpErr:
		log.Printf("http server: %v", err)
		exitCode = 1
		stop()
	}

	log.Printf("shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		_ = httpSrv.Close() // force-close long downloads after the grace period
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}
