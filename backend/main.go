package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gotd/td/tg"
)

// signID returns the hex HMAC-SHA256 of the message id. The Worker recomputes
// it with the same HMAC_SECRET, so links cannot be forged or enumerated.
func signID(secret string, id int) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.Itoa(id)))
	return hex.EncodeToString(mac.Sum(nil))
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if len(cfg.Admins) == 0 {
		log.Printf("warning: ADMINS is empty; /fsub, /broadcast and /stats are unavailable")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbCtx, dbCancel := context.WithTimeout(ctx, 20*time.Second)
	store, err := OpenStore(dbCtx, cfg.MongoURI, cfg.MongoDB)
	dbCancel()
	if err != nil {
		log.Fatalf("mongodb: %v", err)
	}

	pool := NewPool(cfg)
	bot := NewBot(ctx, cfg, pool, store)
	dispatcher := tg.NewUpdateDispatcher()
	bot.Register(&dispatcher)

	srv := &Server{
		cfg:   cfg,
		pool:  pool,
		cache: NewMetaCache(cfg.MetaTTL, cfg.MetaMax),
	}
	go srv.cache.Janitor(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", srv.handleHealth)      // also answers HEAD
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
	store.Close()
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}
