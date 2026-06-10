// Command chronicle serves the Durable Streams protocol over HTTP.
// Configuration precedence: flags over environment variables over defaults.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	chronicle "gecgithub01.walmart.com/auk000v/chronicle"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// newRedisStore is the Redis backend seam: the store/redis package (on the
// feat/redis-store branch) replaces this nil with its constructor when it
// lands. Until then --store redis reports itself unavailable.
var newRedisStore func(redisURL string) (store.Store, error)

func newStore(cfg chronicle.Config) (store.Store, error) {
	switch cfg.StoreBackend {
	case "memory":
		return store.NewMemoryStore(), nil
	case "redis":
		if newRedisStore == nil {
			return nil, errors.New("redis backend not built in this branch; use --store memory (or CHRONICLE_STORE=memory)")
		}
		return newRedisStore(cfg.RedisURL)
	default:
		return nil, fmt.Errorf("unknown store backend %q (want %q or %q)", cfg.StoreBackend, "redis", "memory")
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chronicle:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := chronicle.DefaultConfig()
	if err := cfg.LoadEnv(os.LookupEnv); err != nil {
		return err
	}

	logLevel := "info"
	flag.StringVar(&cfg.Listen, "listen", cfg.Listen, "HTTP listen address")
	flag.StringVar(&cfg.StreamRoot, "stream-root", cfg.StreamRoot, "URL prefix the protocol is served under")
	flag.StringVar(&cfg.RedisURL, "redis-url", cfg.RedisURL, "redis connection URL (redis backend)")
	flag.StringVar(&cfg.StoreBackend, "store", cfg.StoreBackend, `storage backend: "redis" or "memory"`)
	flag.DurationVar(&cfg.LongPollTimeout, "long-poll-timeout", cfg.LongPollTimeout, "server-side long-poll timeout")
	flag.DurationVar(&cfg.SSEReconnectInterval, "sse-reconnect-interval", cfg.SSEReconnectInterval, "SSE connection reconnect interval")
	flag.StringVar(&logLevel, "log-level", logLevel, "log level: debug, info, warn or error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		return fmt.Errorf("invalid -log-level %q: %w", logLevel, err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	st, err := newStore(cfg)
	if err != nil {
		return err
	}
	defer st.Close() //nolint:errcheck // best-effort release on shutdown

	handler := &chronicle.Handler{
		Store:                st,
		LongPollTimeout:      cfg.LongPollTimeout,
		SSEReconnectInterval: cfg.SSEReconnectInterval,
		Logger:               logger,
	}
	mux, err := chronicle.Mount(cfg.StreamRoot, handler)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: mux,
		// No WriteTimeout: long-poll and SSE responses are open-ended.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()
	logger.Info("chronicle listening",
		"addr", cfg.Listen,
		"root", cfg.StreamRoot,
		"store", cfg.StoreBackend,
		"long_poll_timeout", cfg.LongPollTimeout,
		"sse_reconnect_interval", cfg.SSEReconnectInterval)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	logger.Info("shutting down, draining connections")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Open-ended SSE connections can outlive the drain window; cut them.
		logger.Warn("graceful shutdown incomplete, forcing close", "error", err)
		return srv.Close()
	}
	return nil
}
