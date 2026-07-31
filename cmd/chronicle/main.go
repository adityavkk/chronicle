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
	"sync"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"

	chronicle "gecgithub01.walmart.com/auk000v/chronicle"
	"gecgithub01.walmart.com/auk000v/chronicle/metrics"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
	"gecgithub01.walmart.com/auk000v/chronicle/store/segments"
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
func newStore(cfg chronicle.Config, logger *slog.Logger, redisEvents *redisEventSink) (store.Store, *redisstore.Store, goredis.UniversalClient, error) {
	switch cfg.StoreBackend {
	case "memory":
		if cfg.SegmentMode != string(segments.ModeOff) {
			return nil, nil, nil, errors.New("experimental segment modes require the redis primary store")
		}
		return store.NewMemoryStore(), nil, nil, nil
	case "redis":
		client, err := newRedisClient(cfg.RedisURL, cfg.RedisPoolSize, redisEvents)
		if err != nil {
			return nil, nil, nil, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Ping(ctx).Err(); err != nil {
			return nil, nil, nil, fmt.Errorf("redis unreachable at %s: %w", cfg.RedisURL, err)
		}
		rs := redisstore.New(client, redisstore.Options{Logger: logger})
		mode, err := segments.ParseMode(cfg.SegmentMode)
		if err != nil {
			_ = client.Close()
			return nil, nil, nil, err
		}
		var backend segments.Backend
		switch mode {
		case segments.ModeOff:
			return rs, rs, client, nil
		case segments.ModeRedisChunks:
			backend = segments.NewRedisBackend(client, nil)
		case segments.ModeLocalFiles, segments.ModeObjectCache:
			backend, err = segments.NewFileBackend(mode, cfg.SegmentDir, cfg.SegmentCacheBytes, nil)
			if err != nil {
				_ = client.Close()
				return nil, nil, nil, err
			}
		}
		state := segments.MigrationState(cfg.SegmentInitialState)
		segmented, err := segments.New(rs, segments.Options{
			Backend:      backend,
			TargetBytes:  cfg.SegmentTargetBytes,
			IndexStride:  cfg.SegmentIndexStride,
			AutoSealRead: cfg.SegmentAutoSealRead,
			InitialState: state,
		}, logger)
		if err != nil {
			_ = client.Close()
			return nil, nil, nil, err
		}
		if _, ok := segmented.NotificationSubscriber(); !ok {
			_ = segmented.Close()
			return nil, nil, nil, errors.New(
				"segment wrapper requires the Redis primary notification capability",
			)
		}
		return segmented, rs, client, nil
	default:
		return nil, nil, nil, fmt.Errorf("unknown store backend %q (want %q or %q)", cfg.StoreBackend, "redis", "memory")
	}
}

// redisEventSink bridges go-redis connection events to the subscription service.
// The client is built before subscriptions exist, so the hook is installed first
// and armed once the concrete Manager has started.
type redisEventSink struct {
	mu      sync.RWMutex
	service chronicle.SubscriptionService
}

func (s *redisEventSink) Set(service chronicle.SubscriptionService) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.service = service
	s.mu.Unlock()
}

func (s *redisEventSink) OnConnect(_ context.Context, _ *goredis.Conn) error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	service := s.service
	s.mu.RUnlock()
	if service != nil {
		service.OnRedisReconnect()
	}
	return nil
}

// newRedisClient parses a Redis URL and creates the appropriate client.
// redis://host:port/db creates a standalone client; redis+cluster://h1,h2,h3
// creates a ClusterClient that speaks the Redis Cluster protocol (required for
// Memorystore for Redis Cluster, which shards keys across nodes — gate #2).
func newRedisClient(rawURL string, poolSize int, redisEvents *redisEventSink) (goredis.UniversalClient, error) {
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
		if poolSize > 0 {
			opts.PoolSize = poolSize
		}
		if redisEvents != nil {
			opts.OnConnect = redisEvents.OnConnect
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
	if redisEvents != nil {
		opt.OnConnect = redisEvents.OnConnect
	}
	if poolSize > 0 {
		opt.PoolSize = poolSize
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
	flag.IntVar(&cfg.RedisPoolSize, "redis-pool-size", cfg.RedisPoolSize, "go-redis per-node connection pool size, 0 = default")
	flag.StringVar(&cfg.StoreBackend, "store", cfg.StoreBackend, `storage backend: "redis" or "memory"`)
	flag.StringVar(&cfg.SegmentMode, "segment-mode", cfg.SegmentMode, `experimental immutable read plane: "off", "redis-chunks", "local-files", or "object-cache"`)
	flag.StringVar(&cfg.SegmentDir, "segment-dir", cfg.SegmentDir, "root directory for local-files/object-cache segment modes")
	flag.IntVar(&cfg.SegmentTargetBytes, "segment-target-bytes", cfg.SegmentTargetBytes, "approximate immutable segment data bytes")
	flag.IntVar(&cfg.SegmentIndexStride, "segment-index-stride", cfg.SegmentIndexStride, "records per fixed-width sparse index entry")
	flag.Int64Var(&cfg.SegmentCacheBytes, "segment-cache-bytes", cfg.SegmentCacheBytes, "bounded local cache bytes for object-cache mode")
	flag.BoolVar(&cfg.SegmentAutoSealRead, "segment-auto-seal-read", cfg.SegmentAutoSealRead, "reconcile a durable segment generation before catch-up reads")
	flag.StringVar(&cfg.SegmentInitialState, "segment-initial-state", cfg.SegmentInitialState, `migration state for new manifests: "shadow" or "serving"`)
	flag.DurationVar(&cfg.LongPollTimeout, "long-poll-timeout", cfg.LongPollTimeout, "server-side long-poll timeout")
	flag.DurationVar(&cfg.SSEReconnectInterval, "sse-reconnect-interval", cfg.SSEReconnectInterval, "SSE connection reconnect interval")
	flag.IntVar(&cfg.ReadPageBytes, "read-page-bytes", cfg.ReadPageBytes, "catch-up returned page payload target in bytes")
	flag.IntVar(&cfg.SSEHubReplayBytes, "sse-hub-replay-bytes", cfg.SSEHubReplayBytes, "per-stream SSE hub replay memory bound in bytes")
	flag.IntVar(&cfg.SSEHubBatchBytes, "sse-hub-batch-bytes", cfg.SSEHubBatchBytes, "target retained bytes per shared SSE data event")
	flag.DurationVar(&cfg.SSEClientWriteTimeout, "sse-client-write-timeout", cfg.SSEClientWriteTimeout, "maximum duration of one SSE client event flush")
	flag.StringVar(&cfg.PublicBaseURL, "public-url", cfg.PublicBaseURL, "externally reachable origin for webhook callback/JWKS URLs")
	flag.BoolVar(&cfg.Subscriptions, "subscriptions", cfg.Subscriptions, "enable the reserved __ds subscription APIs (redis backend only)")
	flag.BoolVar(&cfg.UI, "ui", cfg.UI, "serve the embedded dsui console alongside the API (false = backend API only)")
	flag.StringVar(&cfg.UIServer, "ui-server", cfg.UIServer, "server URL the served console prefills (empty = same-origin)")
	flag.BoolVar(&cfg.WebhookAllowPrivate, "webhook-allow-private", cfg.WebhookAllowPrivate, "accept webhook URLs on private/RFC1918 addresses (trusted networks only)")
	flag.DurationVar(&cfg.SweepInterval, "sweep-interval", cfg.SweepInterval, "recovery sweep interval (subscriptions)")
	flag.DurationVar(&cfg.ReconcileInterval, "reconcile-interval", cfg.ReconcileInterval, "slow reconcile loop interval (subscriptions)")
	flag.IntVar(&cfg.SweepBatch, "sweep-batch", cfg.SweepBatch, "max subscriptions evaluated per sweep tick, 0 = no cap (subscriptions)")
	flag.StringVar(&cfg.MetricsListen, "metrics-listen", cfg.MetricsListen, "address for /metrics + /healthz + /readyz, e.g. :9090 (empty disables)")
	flag.BoolVar(&cfg.MetricsPprof, "metrics-pprof", cfg.MetricsPprof, "expose Go runtime profiles on the protected metrics listener")
	flag.StringVar(&logLevel, "log-level", logLevel, "log level: debug, info, warn or error")
	flag.Parse()
	if cfg.ReadPageBytes <= 0 {
		return fmt.Errorf("-read-page-bytes must be positive")
	}
	if err := validateSegmentConfig(cfg); err != nil {
		return err
	}
	if err := validateSSEConfig(cfg); err != nil {
		return err
	}
	if err := validateObservabilityConfig(cfg); err != nil {
		return err
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		return fmt.Errorf("invalid -log-level %q: %w", logLevel, err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	redisEvents := &redisEventSink{}
	st, rs, client, err := newStore(cfg, logger, redisEvents)
	if err != nil {
		return err
	}
	defer st.Close() //nolint:errcheck // best-effort release on shutdown

	handler := &chronicle.Handler{
		Store:                 st,
		LongPollTimeout:       cfg.LongPollTimeout,
		SSEReconnectInterval:  cfg.SSEReconnectInterval,
		ReadPageBytes:         cfg.ReadPageBytes,
		SSEHubReplayBytes:     cfg.SSEHubReplayBytes,
		SSEHubBatchBytes:      cfg.SSEHubBatchBytes,
		SSEClientWriteTimeout: cfg.SSEClientWriteTimeout,
		Logger:                logger,
		AuthMode:              cfg.AuthMode,
	}

	// Trusted-backend service principals (issue #126 TB4). Counts only in the
	// log — credential material must never reach a log line.
	if len(cfg.ServiceCredentials) > 0 || len(cfg.TrustedSPIFFEIDs) > 0 {
		handler.ServiceAuth = &chronicle.ServiceAuth{
			Credentials:            cfg.ServiceCredentials,
			TrustedSPIFFEIDs:       cfg.TrustedSPIFFEIDs,
			SidecarMarkerName:      cfg.XFCCMarkerName,
			SidecarMarkerValue:     cfg.XFCCMarkerValue,
			AllowXFCCWithoutMarker: cfg.AllowXFCCWithoutMarker,
		}
		logger.Info("service principal auth enabled",
			"bearer_credentials", len(cfg.ServiceCredentials),
			"trusted_spiffe_ids", len(cfg.TrustedSPIFFEIDs),
			"xfcc_marker", cfg.XFCCMarkerName != "")
		// LoadEnv already fails closed on a SPIFFE allowlist with neither a
		// marker nor the explicit opt-in (#130), so reaching here without a
		// marker means the operator consciously accepted the marker-less
		// posture — surface it loudly, since XFCC trust then rests entirely on
		// the sidecar sanitizing inbound XFCC (issue #126 hardening).
		if len(cfg.TrustedSPIFFEIDs) > 0 && cfg.XFCCMarkerName == "" {
			logger.Warn("XFCC mesh identity trusted WITHOUT a sidecar marker (CHRONICLE_XFCC_TRUST_WITHOUT_MARKER set): the sidecar MUST strip client-supplied X-Forwarded-Client-Cert (Envoy forward_client_cert_details SANITIZE_SET), else an external client can forge a service principal; set CHRONICLE_XFCC_REQUIRED_HEADER for defense in depth")
		}
	}

	// OIDC user principals (issue #126 TB5): the multi-issuer route for
	// PingFed-verified users. Only the issuer is logged — never tokens.
	if cfg.OIDC.Issuer != "" {
		userAuth, err := chronicle.NewOIDCUserAuth(cfg.OIDC, nil, logger)
		if err != nil {
			return fmt.Errorf("oidc: %w", err)
		}
		handler.UserAuth = userAuth
		logger.Info("oidc user auth enabled", "issuer", cfg.OIDC.Issuer)
	}

	// Observability surface (/metrics, /healthz, /readyz). Created independently
	// of subscriptions so Go/process/health metrics are exposed either way; the
	// recorder is handed to the subscription Manager when subscriptions are on.
	var subMetrics webhook.Metrics
	var metricsSrv *http.Server
	if cfg.MetricsListen != "" {
		if cfg.MetricsPprof {
			disableRuntimeProfiles := metrics.EnableRuntimeProfiles()
			defer disableRuntimeProfiles()
		}
		prom := metrics.New()
		if source, ok := st.(metrics.SegmentStatsSource); ok {
			prom.RegisterSegments(source)
		}
		subMetrics = prom
		handler.ReadMetrics = prom
		handler.SSEMetrics = prom
		handler.AppendMetrics = prom
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
			Handler:           prom.Mux(ready, cfg.MetricsPprof),
			ReadHeaderTimeout: 10 * time.Second,
		}
		go func() {
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics server", "error", err)
			}
		}()
		logger.Info("metrics enabled", "addr", cfg.MetricsListen, "pprof", cfg.MetricsPprof)
		if cfg.MetricsPprof {
			logger.Warn("pprof enabled; keep the metrics listener private", "addr", cfg.MetricsListen)
		}
	}

	subscriptionsEnabled := false
	if cfg.Subscriptions {
		if client == nil {
			return fmt.Errorf("subscriptions require the redis backend")
		}
		streamRootURL := strings.TrimSuffix(cfg.PublicBaseURL, "/") + cfg.StreamRoot
		tuning := chronicle.SubscriptionTuning{
			SweepInterval:          cfg.SweepInterval,
			ReconcileInterval:      cfg.ReconcileInterval,
			SweepBatch:             cfg.SweepBatch,
			Metrics:                subMetrics,
			WakeTokenAudience:      cfg.WakeTokenAudience,
			Consistency:            cfg.Consistency,
			WaitReplicas:           cfg.WaitReplicas,
			WaitTimeoutMs:          cfg.WaitTimeoutMs,
			AuthMode:               cfg.AuthMode,
			KeysFile:               cfg.KeysFile,
			KeysFileAllowGroupRead: cfg.KeysFileAllowGroupRead,
			KeyRotationOverlap:     cfg.KeyRotationOverlap,
		}
		router, service, authz, err := chronicle.NewSubscriptions(client, st, rs, streamRootURL, cfg.WebhookAllowPrivate, tuning, logger)
		if err != nil {
			return fmt.Errorf("subscriptions: %w", err)
		}
		handler.Subscriptions = router
		handler.SubHooks = service
		// The token gates share the subscription layer's persisted keys, so
		// claim-minted write tokens, read capabilities, and caller tokens all
		// validate across restarts and replicas (issue #126).
		handler.AppendAuth = authz.Append
		handler.ReadAuth = authz.Read
		handler.CallerAuth = authz.Caller
		handler.EntityAuth = authz.Entity
		// Start runs the boot reconcile synchronously before launching its loops, so
		// anything owed is re-fired before serving (issue #13 — the boot recovery
		// event closes the restart gap; no separate RunSweep is needed).
		service.Start()
		redisEvents.Set(service)
		stopPromotionSignals := startPromotionSignalHandler(service, logger)
		defer service.Stop()
		defer stopPromotionSignals()
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
		"segment_mode", cfg.SegmentMode,
		"segment_state", cfg.SegmentInitialState,
		"subscriptions", subscriptionsEnabled,
		"ui", uiEnabled,
		"auth_mode", cfg.AuthMode.String(),
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

func validateSegmentConfig(cfg chronicle.Config) error {
	mode, err := segments.ParseMode(cfg.SegmentMode)
	if err != nil {
		return fmt.Errorf("-segment-mode: %w", err)
	}
	if cfg.SegmentTargetBytes <= 0 {
		return fmt.Errorf("-segment-target-bytes must be positive")
	}
	if cfg.SegmentIndexStride <= 0 {
		return fmt.Errorf("-segment-index-stride must be positive")
	}
	if cfg.SegmentCacheBytes <= 0 {
		return fmt.Errorf("-segment-cache-bytes must be positive")
	}
	state := segments.MigrationState(cfg.SegmentInitialState)
	if state != segments.StateShadow && state != segments.StateServing {
		return fmt.Errorf(
			"-segment-initial-state must be %q or %q",
			segments.StateShadow,
			segments.StateServing,
		)
	}
	if (mode == segments.ModeLocalFiles || mode == segments.ModeObjectCache) &&
		cfg.SegmentDir == "" {
		return fmt.Errorf("-segment-dir is required for -segment-mode=%s", mode)
	}
	return nil
}

func validateSSEConfig(cfg chronicle.Config) error {
	if cfg.SSEHubReplayBytes <= 0 {
		return fmt.Errorf("-sse-hub-replay-bytes must be positive")
	}
	if cfg.SSEHubBatchBytes <= 0 {
		return fmt.Errorf("-sse-hub-batch-bytes must be positive")
	}
	if cfg.SSEHubBatchBytes > cfg.SSEHubReplayBytes {
		return fmt.Errorf("-sse-hub-batch-bytes must not exceed -sse-hub-replay-bytes")
	}
	if cfg.SSEClientWriteTimeout <= 0 {
		return fmt.Errorf("-sse-client-write-timeout must be positive")
	}
	return nil
}

func validateObservabilityConfig(cfg chronicle.Config) error {
	if cfg.MetricsPprof && cfg.MetricsListen == "" {
		return fmt.Errorf("-metrics-pprof requires -metrics-listen")
	}
	return nil
}

// startPromotionSignalHandler exposes the regional-DR controller hook: after an
// active-passive Redis promotion and endpoint flip, the controller sends SIGUSR1
// to this process to re-establish slot ownership on the promoted primary and run
// the failover-aware eager reconcile (SubscriptionService.Promote). The reconnect
// hook above covers ordinary healed connections; this explicit signal covers the
// promotion decision itself.
func startPromotionSignalHandler(service chronicle.SubscriptionService, logger *slog.Logger) func() {
	sigC := make(chan os.Signal, 1)
	stopC := make(chan struct{})
	signal.Notify(sigC, syscall.SIGUSR1)
	go promotionSignalLoop(sigC, stopC, service, logger)
	return func() {
		signal.Stop(sigC)
		close(stopC)
	}
}

func promotionSignalLoop(sigC <-chan os.Signal, stopC <-chan struct{}, service chronicle.SubscriptionService, logger *slog.Logger) {
	for {
		select {
		case <-stopC:
			return
		case sig := <-sigC:
			logger.Info("redis promotion signal received; running eager subscription reconcile", "signal", sig.String())
			service.Promote()
		}
	}
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
