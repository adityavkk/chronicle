package chronicle

import (
	"context"
	"fmt"
	"io"
	"net"
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
	evals atomic.Int64
}

func (s *sseRedisBenchmarkStore) DialHook(next goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (s *sseRedisBenchmarkStore) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		err := next(ctx, cmd)
		if strings.EqualFold(cmd.Name(), "evalsha") {
			s.evals.Add(1)
		}
		return err
	}
}

func (s *sseRedisBenchmarkStore) ProcessPipelineHook(
	next goredis.ProcessPipelineHook,
) goredis.ProcessPipelineHook {
	return func(ctx context.Context, commands []goredis.Cmder) error {
		err := next(ctx, commands)
		for _, command := range commands {
			if strings.EqualFold(command.Name(), "evalsha") {
				s.evals.Add(1)
			}
		}
		return err
	}
}

func (s *sseRedisBenchmarkStore) Read(
	path string,
	offset store.Offset,
) ([]store.Message, bool, error) {
	messages, upToDate, err := s.Store.Read(path, offset)
	if err == nil {
		s.reads.Add(1)
	}
	return messages, upToDate, err
}

func (s *sseRedisBenchmarkStore) ReadPage(
	ctx context.Context,
	path string,
	offset store.Offset,
	opts store.ReadPageOptions,
) (store.ReadPage, error) {
	page, err := s.Store.ReadPage(ctx, path, offset, opts)
	if err == nil {
		s.reads.Add(1)
	}
	return page, err
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
	waitForBenchmarkReadCount(b, &st.reads, clients)
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

// BenchmarkRedisLiveReadModes measures the live-mode decision gate separately
// from root-frame fusion. Read counts are exact: register-first offset=now
// long-poll needs an initial and a final page, while a new SSE hub is seeded by
// its single authoritative client page.
func BenchmarkRedisLiveReadModes(b *testing.B) {
	b.Run("long-poll-timeout", func(b *testing.B) {
		st, h, _ := newRedisLiveBenchmarkHarness(b)
		h.LongPollTimeout = 2 * time.Millisecond
		mustCreateBenchmarkStream(b, st, "/long-poll-timeout", nil)
		before := st.reads.Load()
		beforeEvals := st.evals.Load()
		b.ResetTimer()
		for b.Loop() {
			recorder := do(
				h,
				http.MethodGet,
				"/long-poll-timeout?offset=now&live=long-poll",
				nil,
				nil,
			)
			if recorder.Code != http.StatusNoContent {
				b.Fatalf("status = %d, want 204", recorder.Code)
			}
		}
		b.StopTimer()
		reportLiveBenchmarkReads(b, st.reads.Load()-before, 2)
		reportLiveBenchmarkScripts(b, st.evals.Load()-beforeEvals, 2)
	})

	b.Run("long-poll-wake", func(b *testing.B) {
		st, h, _ := newRedisLiveBenchmarkHarness(b)
		h.LongPollTimeout = time.Second
		mustCreateBenchmarkStream(b, st, "/long-poll-wake", nil)
		before := st.reads.Load()
		beforeEvals := st.evals.Load()
		b.ResetTimer()
		for b.Loop() {
			readsBefore := st.reads.Load()
			evalsBefore := st.evals.Load()
			result := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				result <- do(
					h,
					http.MethodGet,
					"/long-poll-wake?offset=now&live=long-poll",
					nil,
					nil,
				)
			}()
			waitForBenchmarkReadCount(b, &st.reads, readsBefore+1)
			waitForBenchmarkReadCount(b, &st.evals, evalsBefore+1)
			if _, err := st.Append(
				"/long-poll-wake",
				[]byte("x"),
				store.AppendOptions{ContentType: "text/plain"},
			); err != nil {
				b.Fatal(err)
			}
			recorder := <-result
			if recorder.Code != http.StatusOK || recorder.Body.String() != "x" {
				b.Fatalf("response = %d %q, want 200 x", recorder.Code, recorder.Body.String())
			}
		}
		b.StopTimer()
		reportLiveBenchmarkReads(b, st.reads.Load()-before, 2)
		reportLiveBenchmarkScripts(b, st.evals.Load()-beforeEvals-int64(b.N), 2)
	})

	for _, tc := range []struct {
		name    string
		initial []byte
		offset  string
	}{
		{name: "sse-initial-catchup", initial: []byte("frame"), offset: "-1"},
		{name: "sse-empty-new-hub", offset: "now"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			st, _, server := newRedisLiveBenchmarkHarness(b)
			client := server.Client()
			before := st.reads.Load()
			beforeEvals := st.evals.Load()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				path := fmt.Sprintf("/%s-%d", tc.name, i)
				mustCreateBenchmarkStream(b, st, path, tc.initial)
				requestCtx, cancel := context.WithCancel(context.Background())
				request, err := http.NewRequestWithContext(
					requestCtx,
					http.MethodGet,
					server.URL+path+"?offset="+tc.offset+"&live=sse",
					nil,
				)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				response, err := client.Do(request) //nolint:bodyclose // closed below on every iteration
				if err != nil {
					b.Fatal(err)
				}
				if response.StatusCode != http.StatusOK {
					b.Fatalf("status = %d, want 200", response.StatusCode)
				}
				cancel()
				_ = response.Body.Close()
			}
			b.StopTimer()
			waitForBenchmarkReadsToSettle(b, &st.reads)
			reportLiveBenchmarkReads(b, st.reads.Load()-before, 1)
			reportLiveBenchmarkScripts(b, st.evals.Load()-beforeEvals-int64(b.N), 1)
		})
	}
}

func newRedisLiveBenchmarkHarness(
	b *testing.B,
) (*sseRedisBenchmarkStore, *Handler, *httptest.Server) {
	b.Helper()
	rawURL := os.Getenv("REDIS_URL")
	if rawURL == "" {
		b.Skip("REDIS_URL is required for Redis live-read benchmarks")
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
	st := &sseRedisBenchmarkStore{}
	client.AddHook(st)
	st.Store = redisstore.New(client, redisstore.Options{})
	h := newHubTestHandler(st)
	h.SSEHubPollInterval = time.Hour
	server := httptest.NewServer(h)
	b.Cleanup(func() {
		server.Close()
		_ = st.Close()
	})
	return st, h, server
}

func mustCreateBenchmarkStream(
	b *testing.B,
	st *sseRedisBenchmarkStore,
	path string,
	initial []byte,
) {
	b.Helper()
	if _, created, err := st.Create(path, store.CreateOptions{
		ContentType: "text/plain",
		InitialData: initial,
	}); err != nil {
		b.Fatal(err)
	} else if !created {
		b.Fatalf("stream %q already exists", path)
	}
}

func reportLiveBenchmarkReads(b *testing.B, reads, wantPerOp int64) {
	b.Helper()
	ratio := float64(reads) / float64(b.N)
	b.ReportMetric(ratio, "redis_pages/op")
	if reads != wantPerOp*int64(b.N) {
		b.Fatalf("Redis pages = %d over %d operations, want %d per operation", reads, b.N, wantPerOp)
	}
}

func reportLiveBenchmarkScripts(b *testing.B, scripts, wantPerOp int64) {
	b.Helper()
	ratio := float64(scripts) / float64(b.N)
	b.ReportMetric(ratio, "redis_scripts/op")
	if scripts != wantPerOp*int64(b.N) {
		b.Fatalf("Redis scripts = %d over %d operations, want %d per operation", scripts, b.N, wantPerOp)
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
