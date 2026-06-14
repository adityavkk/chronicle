package chronicle

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// benchRedis returns a flushed client on an isolated db for sweep benchmarks.
// It skips (not fails) when Redis is unreachable so `go test ./...` stays green
// without a server. Override the target with BENCH_REDIS_URL.
func benchRedis(b *testing.B) redis.UniversalClient {
	b.Helper()
	url := os.Getenv("BENCH_REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/13"
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		b.Skipf("bad BENCH_REDIS_URL: %v", err)
	}
	c := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		b.Skipf("redis not reachable at %s: %v (run `docker compose up -d redis`)", url, err)
	}
	c.FlushDB(ctx)
	return c
}

// BenchmarkSweepOnce measures the wall-time of a single recovery sweep as the
// subscription count K and links-per-subscription P grow. The sweep reads each
// subscription and each linked stream's tail, so its cost is dominated by Redis
// round trips scaling with K and K*P. Subscriptions are seeded idle and linked
// to non-existent streams: the tail HGET costs the same round trip whether the
// stream exists or not, so this isolates the read cost (no wakes fire).
func BenchmarkSweepOnce(b *testing.B) {
	cases := []struct{ K, P int }{
		{1000, 1},
		{1000, 5},
		{5000, 5},
		{10000, 1},
	}
	for _, tc := range cases {
		b.Run(fmt.Sprintf("K%d_P%d", tc.K, tc.P), func(b *testing.B) {
			client := benchRedis(b)
			defer client.Close()

			rs := redisstore.New(client, redisstore.Options{})
			wstore := webhook.NewRedisStore(client)
			mgr, err := webhook.NewManager(wstore, streamAdapter{st: rs, rs: rs}, webhook.ManagerOptions{
				StreamRootURL: "http://bench.invalid/v1/stream/",
			})
			if err != nil {
				b.Fatalf("new manager: %v", err)
			}

			now := time.Now()
			begin := "0000000000000000_0000000000000000"
			for i := 0; i < tc.K; i++ {
				id := fmt.Sprintf("bench-%d", i)
				cfg := webhook.Config{Type: webhook.DispatchPullWake, WakeStream: "bench/wake", LeaseTTLMs: 30000}
				links := make([]webhook.StreamLink, tc.P)
				for j := 0; j < tc.P; j++ {
					p := fmt.Sprintf("bench/s/%d/%d", i, j)
					cfg.Streams = append(cfg.Streams, p)
					links[j] = webhook.StreamLink{Path: p, LinkType: webhook.LinkExplicit, AckedOffset: begin}
				}
				if _, err := wstore.CreateOrConfirm(id, cfg, links, now); err != nil {
					b.Fatalf("seed %s: %v", id, err)
				}
			}

			// Sanity: a seeded subscription must be idle, or the sweep would skip
			// the pending-work read path this benchmark exists to measure.
			if sub, ok, _ := wstore.Get("bench-0"); !ok || sub.Phase != webhook.PhaseIdle {
				b.Fatalf("seeded sub not idle (ok=%v phase=%q); benchmark would measure nothing", ok, sub.Phase)
			}

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				mgr.RunSweep()
			}
		})
	}
}
