package chronicle

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
)

type sseRedisBenchmarkStore struct {
	*redisstore.Store
	reads atomic.Int64
}

func (s *sseRedisBenchmarkStore) Read(
	path string,
	offset store.Offset,
) ([]store.Message, bool, error) {
	s.reads.Add(1)
	return s.Store.Read(path, offset)
}

// BenchmarkSSEHubReadAmplification1000Clients is the cheap, exact-counter guard
// for the issue-4 mechanism gate. Run it alone against an otherwise idle Redis:
//
//	REDIS_URL=redis://localhost:6382/12 go test -run '^$' \
//	  -bench '^BenchmarkSSEHubReadAmplification1000Clients$' -benchtime=100x
//
// The setup and per-client catch-up reads finish before the commandstats
// baseline. Each measured append then waits for the shared hub refresh.
func BenchmarkSSEHubReadAmplification1000Clients(b *testing.B) {
	rawURL := os.Getenv("REDIS_URL")
	if rawURL == "" {
		b.Skip("REDIS_URL is required for the Redis amplification benchmark")
	}
	options, err := goredis.ParseURL(rawURL)
	if err != nil {
		b.Fatal(err)
	}
	client := goredis.NewClient(options)
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		b.Fatalf("Redis ping: %v", err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		b.Fatalf("Redis flush: %v", err)
	}
	st := &sseRedisBenchmarkStore{
		Store: redisstore.New(client, redisstore.Options{}),
	}
	b.Cleanup(func() {
		_ = st.Close()
	})

	h := newHubTestHandler(st)
	h.SSEHubPollInterval = time.Hour
	if _, created, err := st.Create(
		"/amplification",
		store.CreateOptions{ContentType: "text/plain"},
	); err != nil {
		b.Fatal(err)
	} else if !created {
		b.Fatal("amplification stream already exists")
	}
	server := httptest.NewServer(h)
	b.Cleanup(server.Close)

	const clients = 1000
	httpClient := &http.Client{}
	bodies := make([]io.ReadCloser, 0, clients)
	b.Cleanup(func() {
		for _, body := range bodies {
			_ = body.Close()
		}
	})
	for range clients {
		response, err := httpClient.Get( //nolint:bodyclose // stays open as an active SSE client until cleanup
			server.URL + "/amplification?offset=-1&live=sse",
		)
		if err != nil {
			b.Fatalf("open SSE client %d: %v", len(bodies), err)
		}
		bodies = append(bodies, response.Body)
	}
	waitForBenchmarkReadCount(b, &st.reads, clients+1)
	waitForBenchmarkReadsToSettle(b, &st.reads)

	before, err := redisCommandCounters(ctx, client)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readsBeforeAppend := st.reads.Load()
		if _, err := st.Append(
			"/amplification",
			[]byte("x"),
			store.AppendOptions{ContentType: "text/plain"},
		); err != nil {
			b.Fatalf("append %d: %v", i, err)
		}
		waitForBenchmarkReadCount(b, &st.reads, readsBeforeAppend+1)
	}
	b.StopTimer()

	after, err := redisCommandCounters(ctx, client)
	if err != nil {
		b.Fatal(err)
	}
	reads := after.zrangeByLex - before.zrangeByLex
	publishes := after.publish - before.publish
	if publishes != int64(b.N) {
		b.Fatalf("Redis PUBLISH delta = %d, want %d", publishes, b.N)
	}
	ratio := float64(reads) / float64(publishes)
	b.ReportMetric(float64(before.zrangeByLex), "redis_zrangebylex_before")
	b.ReportMetric(float64(after.zrangeByLex), "redis_zrangebylex_after")
	b.ReportMetric(float64(before.publish), "redis_publish_before")
	b.ReportMetric(float64(after.publish), "redis_publish_after")
	b.ReportMetric(float64(reads), "redis_zrangebylex")
	b.ReportMetric(float64(publishes), "redis_publish")
	b.ReportMetric(ratio, "redis_reads/publish")
	if ratio > 1.2 {
		b.Fatalf("Redis read amplification = %.6f, want <= 1.2", ratio)
	}
}

type commandCounters struct {
	zrangeByLex int64
	publish     int64
}

func redisCommandCounters(
	ctx context.Context,
	client *goredis.Client,
) (commandCounters, error) {
	info, err := client.Info(ctx, "commandstats").Result()
	if err != nil {
		return commandCounters{}, fmt.Errorf("Redis INFO commandstats: %w", err)
	}
	zrangeByLex, err := redisCommandCalls(info, "zrangebylex")
	if err != nil {
		return commandCounters{}, err
	}
	publish, err := redisCommandCalls(info, "publish")
	if err != nil {
		return commandCounters{}, err
	}
	return commandCounters{zrangeByLex: zrangeByLex, publish: publish}, nil
}

func redisCommandCalls(info, command string) (int64, error) {
	prefix := "cmdstat_" + command + ":"
	for _, line := range strings.Split(info, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		for _, field := range strings.Split(strings.TrimSpace(strings.TrimPrefix(line, prefix)), ",") {
			value, found := strings.CutPrefix(field, "calls=")
			if !found {
				continue
			}
			calls, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse %s calls %q: %w", command, value, err)
			}
			return calls, nil
		}
	}
	return 0, nil
}

func waitForBenchmarkReadCount(b *testing.B, value *atomic.Int64, want int64) {
	b.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if value.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	b.Fatalf("read count = %d, want at least %d", value.Load(), want)
}

func waitForBenchmarkReadsToSettle(b *testing.B, value *atomic.Int64) {
	b.Helper()
	last := value.Load()
	stableSince := time.Now()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		current := value.Load()
		if current != last {
			last = current
			stableSince = time.Now()
		}
		if time.Since(stableSince) >= 100*time.Millisecond {
			return
		}
		time.Sleep(time.Millisecond)
	}
	b.Fatalf("read count did not settle; last = %d", value.Load())
}
