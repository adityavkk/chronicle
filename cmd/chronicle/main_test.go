package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	chronicle "gecgithub01.walmart.com/auk000v/chronicle"
)

type recordingSubscriptionService struct {
	reconnects  atomic.Int64
	reconnected chan struct{}
	promotes    atomic.Int64
	promoted    chan struct{}
}

type redisConfigGetterStub struct {
	values map[string]string
	err    error
	calls  int
}

func (s *redisConfigGetterStub) ConfigGet(ctx context.Context, parameter string) *goredis.MapStringStringCmd {
	s.calls++
	cmd := goredis.NewMapStringStringCmd(ctx, "config", "get", parameter)
	cmd.SetVal(s.values)
	cmd.SetErr(s.err)
	return cmd
}

func TestValidateAppendCeiling(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	disabled := &redisConfigGetterStub{err: errors.New("must not be called")}
	if err := validateAppendCeiling(context.Background(), disabled, 0, logger); err != nil {
		t.Fatal(err)
	}
	if disabled.calls != 0 {
		t.Fatalf("disabled ceiling performed %d probe(s)", disabled.calls)
	}

	valid := &redisConfigGetterStub{values: map[string]string{"proto-max-bulk-len": "1000"}}
	if err := validateAppendCeiling(context.Background(), valid, 100, logger); err != nil {
		t.Fatalf("valid ceiling: %v", err)
	}
	if valid.calls != 1 {
		t.Fatalf("valid ceiling probes = %d, want 1", valid.calls)
	}

	tooLarge := &redisConfigGetterStub{values: map[string]string{"proto-max-bulk-len": "100"}}
	if err := validateAppendCeiling(context.Background(), tooLarge, 100, logger); err == nil {
		t.Fatal("ceiling that leaves no frame-prefix room must fail")
	}

	malformed := &redisConfigGetterStub{values: map[string]string{"proto-max-bulk-len": "unknown"}}
	if err := validateAppendCeiling(context.Background(), malformed, 100, logger); err == nil {
		t.Fatal("malformed proto-max-bulk-len must fail")
	}

	var logs strings.Builder
	warnLogger := slog.New(slog.NewTextHandler(&logs, nil))
	denied := &redisConfigGetterStub{err: errors.New("NOPERM CONFIG GET")}
	if err := validateAppendCeiling(context.Background(), denied, 100, warnLogger); err != nil {
		t.Fatalf("denied probe must defer to deployment enforcement: %v", err)
	}
	if !strings.Contains(logs.String(), "deployment must enforce") || !strings.Contains(logs.String(), "NOPERM") {
		t.Fatalf("denied probe warning missing evidence: %s", logs.String())
	}
	unsupported := &redisConfigGetterStub{err: errors.New("ERR unknown command 'CONFIG'")}
	if err := validateAppendCeiling(context.Background(), unsupported, 100, warnLogger); err != nil {
		t.Fatalf("unsupported probe must defer to deployment enforcement: %v", err)
	}
	transient := &redisConfigGetterStub{err: syscall.ECONNRESET}
	if err := validateAppendCeiling(context.Background(), transient, 100, warnLogger); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("transient probe error = %v, want ECONNRESET", err)
	}
	missing := &redisConfigGetterStub{values: map[string]string{}}
	if err := validateAppendCeiling(context.Background(), missing, 100, warnLogger); err == nil ||
		!strings.Contains(err.Error(), "omitted proto-max-bulk-len") {
		t.Fatalf("missing probe value error = %v", err)
	}

	if err := validateAppendCeiling(context.Background(), nil, 100, logger); err != nil {
		t.Fatalf("memory backend ceiling: %v", err)
	}
	if err := validateAppendCeiling(context.Background(), nil, -1, logger); err == nil {
		t.Fatal("negative ceiling must fail")
	}
}

func TestValidateSegmentConfig(t *testing.T) {
	valid := chronicle.DefaultConfig()
	if err := validateSegmentConfig(valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*chronicle.Config)
	}{
		{name: "mode", mutate: func(cfg *chronicle.Config) { cfg.SegmentMode = "unknown" }},
		{name: "target", mutate: func(cfg *chronicle.Config) { cfg.SegmentTargetBytes = 0 }},
		{name: "stride", mutate: func(cfg *chronicle.Config) { cfg.SegmentIndexStride = 0 }},
		{name: "cache", mutate: func(cfg *chronicle.Config) { cfg.SegmentCacheBytes = 0 }},
		{name: "state", mutate: func(cfg *chronicle.Config) { cfg.SegmentInitialState = "cutover" }},
		{name: "local directory", mutate: func(cfg *chronicle.Config) {
			cfg.SegmentMode = "local-files"
			cfg.SegmentDir = ""
		}},
		{name: "object directory", mutate: func(cfg *chronicle.Config) {
			cfg.SegmentMode = "object-cache"
			cfg.SegmentDir = ""
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := chronicle.DefaultConfig()
			test.mutate(&cfg)
			if err := validateSegmentConfig(cfg); err == nil {
				t.Fatal("invalid segment config was accepted")
			}
		})
	}

	for _, mode := range []string{"local-files", "object-cache"} {
		cfg := chronicle.DefaultConfig()
		cfg.SegmentMode = mode
		cfg.SegmentDir = t.TempDir()
		cfg.SegmentInitialState = "serving"
		if err := validateSegmentConfig(cfg); err != nil {
			t.Fatalf("valid %s config: %v", mode, err)
		}
	}
}

func TestValidateSSEConfig(t *testing.T) {
	valid := chronicle.DefaultConfig()
	if err := validateSSEConfig(valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*chronicle.Config)
	}{
		{name: "replay", mutate: func(cfg *chronicle.Config) { cfg.SSEHubReplayBytes = 0 }},
		{name: "batch", mutate: func(cfg *chronicle.Config) { cfg.SSEHubBatchBytes = 0 }},
		{name: "batch exceeds replay", mutate: func(cfg *chronicle.Config) {
			cfg.SSEHubBatchBytes = cfg.SSEHubReplayBytes + 1
		}},
		{name: "notification connections", mutate: func(cfg *chronicle.Config) {
			cfg.SSENotificationGroups = 0
		}},
		{name: "write timeout", mutate: func(cfg *chronicle.Config) { cfg.SSEClientWriteTimeout = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := chronicle.DefaultConfig()
			test.mutate(&cfg)
			if err := validateSSEConfig(cfg); err == nil {
				t.Fatal("invalid SSE config was accepted")
			}
		})
	}
}

func TestValidateObservabilityConfig(t *testing.T) {
	cfg := chronicle.DefaultConfig()
	if err := validateObservabilityConfig(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.MetricsPprof = true
	if err := validateObservabilityConfig(cfg); err == nil {
		t.Fatal("pprof without a metrics listener was accepted")
	}
	cfg.MetricsListen = ":9090"
	if err := validateObservabilityConfig(cfg); err != nil {
		t.Fatalf("pprof with a metrics listener: %v", err)
	}
}

func (s *recordingSubscriptionService) OnStreamCreated(string) {}
func (s *recordingSubscriptionService) OnStreamAppend(string)  {}
func (s *recordingSubscriptionService) OnStreamDeleted(string) {}
func (s *recordingSubscriptionService) Start()                 {}
func (s *recordingSubscriptionService) Stop()                  {}
func (s *recordingSubscriptionService) RunSweep()              {}

func (s *recordingSubscriptionService) Promote() {
	s.promotes.Add(1)
	select {
	case s.promoted <- struct{}{}:
	default:
	}
}

func (s *recordingSubscriptionService) OnRedisReconnect() {
	s.reconnects.Add(1)
	select {
	case s.reconnected <- struct{}{}:
	default:
	}
}

func TestPromotionSignalTriggersSubscriptionService(t *testing.T) {
	svc := &recordingSubscriptionService{promoted: make(chan struct{}, 1)}
	sigC := make(chan os.Signal, 1)
	stopC := make(chan struct{})
	t.Cleanup(func() { close(stopC) })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go promotionSignalLoop(sigC, stopC, svc, logger)

	sigC <- syscall.SIGUSR1
	select {
	case <-svc.promoted:
		return
	case <-time.After(time.Second):
		t.Fatalf("promotion signal did not call Promote (count=%d)", svc.promotes.Load())
	}
}

func TestRedisReconnectTriggersSubscriptionService(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis integration test in -short mode")
	}
	rawURL := os.Getenv("REDIS_URL")
	if rawURL == "" {
		rawURL = "redis://localhost:6379/13"
	}
	if strings.Contains(rawURL, "+cluster://") {
		t.Skip("CLIENT KILL reconnect test uses standalone Redis")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events := &redisEventSink{}
	client, err := newRedisClient(rawURL, 0, events)
	if err != nil {
		t.Fatalf("new redis client: %v", err)
	}
	defer client.Close() //nolint:errcheck // test cleanup
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unreachable (%s): %v", rawURL, err)
	}

	// Capture the existing pooled connection before arming the sink, so the first
	// connection does not satisfy the assertion. The killed socket must reconnect
	// in the same process and drive SubscriptionService.OnRedisReconnect.
	id, err := client.Do(ctx, "CLIENT", "ID").Int64()
	if err != nil {
		t.Skipf("CLIENT ID unavailable: %v", err)
	}
	svc := &recordingSubscriptionService{reconnected: make(chan struct{}, 1)}
	events.Set(svc)

	opt, err := goredis.ParseURL(rawURL)
	if err != nil {
		t.Fatalf("parse redis URL: %v", err)
	}
	killer := goredis.NewClient(opt)
	defer killer.Close() //nolint:errcheck // test cleanup
	if err := killer.Do(ctx, "CLIENT", "KILL", "ID", id).Err(); err != nil {
		t.Skipf("CLIENT KILL unavailable: %v", err)
	}

	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := client.Ping(ctx).Err(); err != nil && ctx.Err() != nil {
			t.Fatal(err)
		}
		select {
		case <-svc.reconnected:
			return
		case <-deadline:
			t.Fatalf("Redis reconnect did not notify subscription service (count=%d)", svc.reconnects.Load())
		case <-tick.C:
		}
	}
}
