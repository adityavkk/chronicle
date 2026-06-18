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
	"strings"
	"sync"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"

	chronicle "gecgithub01.walmart.com/auk000v/chronicle"
	"gecgithub01.walmart.com/auk000v/chronicle/metrics"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

type redisReconnectHook struct {
	mu sync.RWMutex
	fn func()
}

func (h *redisReconnectHook) set(fn func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fn = fn
}

func (h *redisReconnectHook) fire() {
	h.mu.RLock()
	fn := h.fn
	h.mu.RUnlock()
	if fn != nil {
		go fn()
	}
}

// newStore builds the stream store. For the redis backend it also returns the
// concrete Redis store and the shared client so the subscription layer can run
// on the same Redis; both are nil for the memory backend.
func newStore(cfg chronicle.Config, logger *slog.Logger, onRedisConnect func()) (store.Store, *redisstore.Store, goredis.UniversalClient, error) {
	switch cfg.StoreBackend {
	case "memory":
		return store.NewMemoryStore(), nil, nil, nil
	case "redis":
		opt, err := goredis.ParseURL(cfg.RedisURL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("invalid redis URL: %w", err)
		}
		if onRedisConnect != nil {
			opt.OnConnect = func(context.Context, *goredis.Conn) error {
				onRedisConnect()
				return nil
			}
		}
		client := goredis.NewClient(opt)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Ping(ctx).Err(); err != nil {
			return nil, nil, nil, fmt.Errorf("redis unreachable at %s: %w", cfg.RedisURL, err)
		}
		rs := redisstore.New(client, redisstore.Options{Logger: logger})
		return rs, rs, client, nil
	default:
		return nil, nil, nil, fmt.Errorf("unknown store backend %q (want %q or %q)", cfg.StoreBackend, "redis", "memory")
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
	flag.StringVar(&cfg.PublicBaseURL, "public-url", cfg.PublicBaseURL, "externally reachable origin for webhook callback/JWKS URLs")
	flag.BoolVar(&cfg.Subscriptions, "subscriptions", cfg.Subscriptions, "enable the reserved __ds subscription APIs (redis backend only)")
	flag.BoolVar(&cfg.WebhookAllowPrivate, "webhook-allow-private", cfg.WebhookAllowPrivate, "accept webhook URLs on private/RFC1918 addresses (trusted networks only)")
	flag.DurationVar(&cfg.SweepInterval, "sweep-interval", cfg.SweepInterval, "coarse recovery floor interval (subscriptions)")
	flag.DurationVar(&cfg.ReconcileInterval, "reconcile-interval", cfg.ReconcileInterval, "slow reconcile loop interval (subscriptions)")
	flag.IntVar(&cfg.SweepBatch, "sweep-batch", cfg.SweepBatch, "max subscriptions evaluated per sweep tick, 0 = no cap (subscriptions)")
	flag.StringVar(&cfg.MetricsListen, "metrics-listen", cfg.MetricsListen, "address for /metrics + /healthz + /readyz, e.g. :9090 (empty disables)")
	flag.StringVar(&logLevel, "log-level", logLevel, "log level: debug, info, warn or error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		return fmt.Errorf("invalid -log-level %q: %w", logLevel, err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	reconnectHook := &redisReconnectHook{}
	st, rs, client, err := newStore(cfg, logger, reconnectHook.fire)
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

	// Observability surface (/metrics, /healthz, /readyz). Created independently
	// of subscriptions so Go/process/health metrics are exposed either way; the
	// recorder is handed to the subscription Manager when subscriptions are on.
	var subMetrics webhook.Metrics
	var metricsSrv *http.Server
	if cfg.MetricsListen != "" {
		prom := metrics.New()
		subMetrics = prom
		ready := func() error { return nil }
		if client != nil {
			ready = func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				return client.Ping(ctx).Err()
			}
		}
		metricsSrv = &http.Server{
			Addr:              cfg.MetricsListen,
			Handler:           prom.Mux(ready),
			ReadHeaderTimeout: 10 * time.Second,
		}
		go func() {
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics server", "error", err)
			}
		}()
		logger.Info("metrics enabled", "addr", cfg.MetricsListen)
	}

	subscriptionsEnabled := false
	if cfg.Subscriptions {
		if client == nil {
			return fmt.Errorf("subscriptions require the redis backend")
		}
		streamRootURL := strings.TrimSuffix(cfg.PublicBaseURL, "/") + cfg.StreamRoot
		tuning := chronicle.SubscriptionTuning{
			SweepInterval:     cfg.SweepInterval,
			ReconcileInterval: cfg.ReconcileInterval,
			SweepBatch:        cfg.SweepBatch,
			Metrics:           subMetrics,
		}
		router, service, err := chronicle.NewSubscriptions(client, st, rs, streamRootURL, cfg.WebhookAllowPrivate, tuning, logger)
		if err != nil {
			return fmt.Errorf("subscriptions: %w", err)
		}
		reconnectHook.set(service.RunRedisReconnect)
		handler.Subscriptions = router
		handler.SubHooks = service
		service.RunSweep() // boot reconcile: re-fire anything owed before serving
		service.Start()
		defer service.Stop()
		subscriptionsEnabled = true
		logger.Info("subscriptions enabled", "stream_root_url", streamRootURL)
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
		"subscriptions", subscriptionsEnabled,
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
	if metricsSrv != nil {
		_ = metricsSrv.Shutdown(shutdownCtx)
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Open-ended SSE connections can outlive the drain window; cut them.
		logger.Warn("graceful shutdown incomplete, forcing close", "error", err)
		return srv.Close()
	}
	return nil
}
