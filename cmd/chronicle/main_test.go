package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type recordingSubscriptionService struct {
	reconnects  atomic.Int64
	reconnected chan struct{}
	promotes    atomic.Int64
	promoted    chan struct{}
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
