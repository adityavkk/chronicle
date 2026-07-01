// Command chronicle serves the Durable Streams protocol over HTTP.
// Configuration precedence: flags over environment variables over defaults.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"

	chronicle "gecgithub01.walmart.com/auk000v/chronicle"
	"gecgithub01.walmart.com/auk000v/chronicle/metrics"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// newStore builds the stream store. For the redis backend it also returns the
// concrete Redis store and the shared client so the subscription layer can run
// on the same Redis; both are nil for the memory backend.
//
// Two URL schemes are supported:
//   - redis://host:port/db — standalone (Memorystore STANDARD_HA or single node)
//   - redis+cluster://host1:port,host2:port,... — sharded cluster
//     (Memorystore for Redis Cluster; gate #2 cross-node RTT testing)
func newStore(cfg chronicle.Config, logger *slog.Logger) (store.Store, *redisstore.Store, goredis.UniversalClient, error) {
	switch cfg.StoreBackend {
	case "memory":
		return store.NewMemoryStore(), nil, nil, nil
	case "redis":
		client, err := newRedisClient(cfg.RedisURL)
		if err != nil {
			return nil, nil, nil, err
		}
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

// newRedisClient parses a Redis URL and creates the appropriate client.
// redis://host:port/db creates a standalone client; redis+cluster://h1,h2,h3
// creates a ClusterClient that speaks the Redis Cluster protocol (required for
// Memorystore for Redis Cluster, which shards keys across nodes — gate #2).
func newRedisClient(rawURL string) (goredis.UniversalClient, error) {
	// rediss+cluster:// = Redis Cluster over TLS; redis+cluster:// = plaintext.
	useTLS := strings.HasPrefix(rawURL, "rediss+cluster://")
	if useTLS || strings.HasPrefix(rawURL, "redis+cluster://") {
		rest := strings.TrimPrefix(strings.TrimPrefix(rawURL, "rediss+cluster://"), "redis+cluster://")
		// Optional user:pass@ credentials precede the comma-separated seed list.
		// Managed Redis Cluster (e.g. the squiggly ms-df-redis cluster) requires
		// AUTH; the standalone path gets creds via ParseURL, so parse them here too.
		var username, password string
		if at := strings.LastIndex(rest, "@"); at >= 0 {
			cred := rest[:at]
			rest = rest[at+1:]
			if c := strings.IndexByte(cred, ':'); c >= 0 {
				username = cred[:c]
				if pw, err := url.QueryUnescape(cred[c+1:]); err == nil {
					password = pw
				} else {
					password = cred[c+1:]
				}
			} else {
				username = cred
			}
		}
		// Strip any /db suffix — cluster mode ignores DB selection.
		if i := strings.LastIndex(rest, "/"); i >= 0 {
			rest = rest[:i]
		}
		seeds := strings.Split(rest, ",")
		for i := range seeds {
			seeds[i] = strings.TrimSpace(seeds[i])
		}
		opts := &goredis.ClusterOptions{
			Addrs:    seeds,
			Username: username,
			Password: password,
		}
		if useTLS {
			// ms-df-redis requires TLS. Cluster node addrs come from CLUSTER SLOTS
			// and won't match the cert SAN, so skip hostname verification.
			opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} // #nosec G402
		}
		return goredis.NewClusterClient(opts), nil
	}
	opt, err := goredis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}
	return goredis.NewClient(opt), nil
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
	flag.BoolVar(&cfg.UI, "ui", cfg.UI, "serve the embedded dsui console alongside the API (false = backend API only)")
	flag.StringVar(&cfg.UIServer, "ui-server", cfg.UIServer, "server URL the served console prefills (empty = same-origin)")
	flag.BoolVar(&cfg.WebhookAllowPrivate, "webhook-allow-private", cfg.WebhookAllowPrivate, "accept webhook URLs on private/RFC1918 addresses (trusted networks only)")
	flag.DurationVar(&cfg.SweepInterval, "sweep-interval", cfg.SweepInterval, "recovery sweep interval (subscriptions)")
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

	st, rs, client, err := newStore(cfg, logger)
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
			Consistency:       cfg.Consistency,
			WaitReplicas:      cfg.WaitReplicas,
			WaitTimeoutMs:     cfg.WaitTimeoutMs,
		}
		router, service, err := chronicle.NewSubscriptions(client, st, rs, streamRootURL, cfg.WebhookAllowPrivate, tuning, logger)
		if err != nil {
			return fmt.Errorf("subscriptions: %w", err)
		}
		handler.Subscriptions = router
		handler.SubHooks = service
		// Start runs the boot reconcile synchronously before launching its loops, so
		// anything owed is re-fired before serving (issue #13 — the boot recovery
		// event closes the restart gap; no separate RunSweep is needed).
		service.Start()
		defer service.Stop()
		subscriptionsEnabled = true
		logger.Info("subscriptions enabled", "stream_root_url", streamRootURL)
	}

	api, err := chronicle.Mount(cfg.StreamRoot, handler)
	if err != nil {
		return err
	}
	// Optionally serve the embedded dsui console alongside the API so chronicle is
	// a single binary + single origin (no separate UI service, no CORS). API paths
	// under the stream root win; everything else is the SPA. uiEnabled is false
	// when -ui=false (backend-only) or the UI was not built into this binary — the
	// UI is fully optional and decoupled from the backend.
	root, uiEnabled := withUI(cfg.StreamRoot, api, cfg.UI, cfg.UIServer, logger)

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: root,
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
		"ui", uiEnabled,
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

// withUI wraps the Durable Streams API handler so chronicle also serves the
// embedded dsui console from the same binary and origin. Requests under
// streamRoot go to the API; everything else is the single-page app. The SPA
// fetches /dsui-config.json, which reports the request's own origin as the
// server, so the browser drives this same chronicle instance (same-origin, no
// CORS, no separate UI deployment). When the UI was not built into the binary
// (no embedded/index.html), the API handler is returned unchanged (uiEnabled
// false) so an API-only build still works.
func withUI(streamRoot string, api http.Handler, enabled bool, serverOverride string, logger *slog.Logger) (http.Handler, bool) {
	if !enabled {
		logger.Info("UI serving disabled (-ui=false); serving API only")
		return api, false
	}
	webRoot, err := fs.Sub(embeddedFS, "embedded")
	if err != nil {
		logger.Warn("embedded UI unavailable, serving API only", "error", err)
		return api, false
	}
	if _, err := fs.Stat(webRoot, "index.html"); err != nil {
		logger.Info("embedded UI not built in, serving API only")
		return api, false
	}
	fileServer := http.FileServer(http.FS(webRoot))
	serveIndex := func(w http.ResponseWriter, r *http.Request) {
		r2 := r.Clone(r.Context())
		r2.URL.Path = "/"
		fileServer.ServeHTTP(w, r2)
	}

	mux := http.NewServeMux()
	mux.Handle(streamRoot, api) // /v1/stream/* -> Durable Streams API

	// Runtime config the SPA fetches on load. defaultServer = the request's own
	// origin so the console talks to this same server; captureBase is null (the
	// webhook-capture relay is a dsui-only dev convenience, not served here).
	mux.HandleFunc("/dsui-config.json", func(w http.ResponseWriter, r *http.Request) {
		scheme := "http"
		if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
			scheme = "https"
		}
		server := serverOverride
		if server == "" && r.Host != "" {
			server = scheme + "://" + r.Host
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"defaultServer": server, "captureBase": nil})
	})

	// Embedded assets, with a single-page-app fallback to index.html for any
	// path that is not a real asset (client-side routes).
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			serveIndex(w, r)
			return
		}
		if _, statErr := fs.Stat(webRoot, p); statErr != nil {
			serveIndex(w, r)
			return
		}
		fileServer.ServeHTTP(w, r)
	})
	return mux, true
}
