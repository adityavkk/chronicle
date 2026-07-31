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
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
)

func TestRedisRegisterFirstLiveReadCommandShape(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis integration test in -short mode")
	}
	rawURL := os.Getenv("CHRONICLE_LIVE_READ_REDIS_URL")
	if rawURL == "" {
		rawURL = "redis://localhost:6379/12"
	}
	options, err := goredis.ParseURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	control := goredis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := control.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unreachable at %s: %v", rawURL, err)
	}
	if err := control.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })

	storeClient := goredis.NewClient(options)
	subject := redisstore.New(storeClient, redisstore.Options{})
	t.Cleanup(func() { _ = subject.Close() })
	h := newHubTestHandler(subject)
	h.SSEHubPollInterval = time.Hour
	h.LongPollTimeout = 20 * time.Millisecond
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)

	for _, tc := range []struct {
		name          string
		path          string
		query         string
		wantEvalSHA   int
		wantMetaReads int
	}{
		{
			name:  "new SSE hub",
			path:  "/register-first-sse",
			query: "?offset=now&live=sse",
			// The initial authoritative page and the bounded incarnation
			// confirmation are both metadata-only at offset=now. There is no
			// third readiness refresh for the first confirmed generation.
			wantEvalSHA:   2,
			wantMetaReads: 2,
		},
		{
			name:          "offset now long poll timeout",
			path:          "/register-first-long-poll",
			query:         "?offset=now&live=long-poll",
			wantEvalSHA:   2,
			wantMetaReads: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, created, err := subject.Create(tc.path, store.CreateOptions{
				ContentType: "text/plain",
			}); err != nil || !created {
				t.Fatalf("create: created=%v err=%v", created, err)
			}
			// Preload read.lua so the trace proves the steady-state EVALSHA path.
			if _, err := subject.ReadPage(
				context.Background(),
				tc.path,
				store.NowOffset,
				store.ReadPageOptions{NoTouch: true},
			); err != nil {
				t.Fatal(err)
			}

			lines := monitorLiveReadRedis(t, options, control, func() {
				requestCtx, requestCancel := context.WithCancel(context.Background())
				request, requestErr := http.NewRequestWithContext(
					requestCtx,
					http.MethodGet,
					server.URL+tc.path+tc.query,
					nil,
				)
				if requestErr != nil {
					t.Fatal(requestErr)
				}
				response, requestErr := http.DefaultClient.Do(request)
				if requestErr != nil {
					t.Fatal(requestErr)
				}
				if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
					t.Fatalf("status = %d", response.StatusCode)
				}
				requestCancel()
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
			})

			metaKey := "ds:{" + tc.path + "}:meta"
			messageKey := "ds:{" + tc.path + "}:msg"
			producerKey := "ds:{" + tc.path + "}:prod"
			forksKey := "ds:{" + tc.path + "}:forks"
			if got := countLiveReadCommand(lines, "evalsha", metaKey); got != tc.wantEvalSHA {
				t.Fatalf("EVALSHA calls = %d, want %d\n%s", got, tc.wantEvalSHA, strings.Join(lines, "\n"))
			}
			if got := countLiveReadCommand(lines, "hgetall", metaKey); got != tc.wantMetaReads {
				t.Fatalf("metadata HGETALL calls = %d, want %d\n%s", got, tc.wantMetaReads, strings.Join(lines, "\n"))
			}
			if got := countLiveReadCommand(lines, "hgetall", producerKey); got != 0 {
				t.Fatalf("producer HGETALL calls = %d, want 0\n%s", got, strings.Join(lines, "\n"))
			}
			if got := countLiveReadCommand(lines, "zrangebylex", messageKey); got != 0 {
				t.Fatalf("message ZRANGEBYLEX calls = %d, want 0\n%s", got, strings.Join(lines, "\n"))
			}
			for _, command := range []string{"hset", "hsetnx", "pexpire", "persist", "del", "zadd"} {
				for _, key := range []string{metaKey, messageKey, producerKey, forksKey} {
					if got := countLiveReadCommand(lines, command, key); got != 0 {
						t.Fatalf("%s %s calls = %d, want 0\n%s", command, key, got, strings.Join(lines, "\n"))
					}
				}
			}
		})
	}
}

func monitorLiveReadRedis(
	t *testing.T,
	options *goredis.Options,
	control *goredis.Client,
	action func(),
) []string {
	t.Helper()
	monitorOptions := *options
	monitorOptions.Protocol = 2
	monitorClient := goredis.NewClient(&monitorOptions)
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan string, 4096)
	monitor := monitorClient.Monitor(ctx, events)
	monitor.Start()
	defer func() {
		stopLiveReadMonitor(t, ctx, control, monitorClient, monitor)
		cancel()
	}()

	start := fmt.Sprintf("chronicle-live-read-monitor-start-%d", time.Now().UnixNano())
	waitForLiveReadMonitor(t, ctx, control, events, start)
	action()
	end := start + "-end"
	if err := control.Echo(ctx, end).Err(); err != nil {
		t.Fatal(err)
	}

	var lines []string
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case line := <-events:
			if strings.Contains(line, strconv.Quote(end)) {
				return lines
			}
			lines = append(lines, line)
		case <-timer.C:
			t.Fatal("timed out waiting for Redis MONITOR end marker")
		}
	}
}

func waitForLiveReadMonitor(
	t *testing.T,
	ctx context.Context,
	control *goredis.Client,
	events <-chan string,
	marker string,
) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := control.Echo(ctx, marker).Err(); err != nil {
			t.Fatal(err)
		}
		select {
		case line := <-events:
			if strings.Contains(line, strconv.Quote(marker)) {
				return
			}
		case <-ticker.C:
		case <-timer.C:
			t.Fatal("timed out waiting for Redis MONITOR start marker")
		}
	}
}

func stopLiveReadMonitor(
	t *testing.T,
	ctx context.Context,
	control, monitorClient *goredis.Client,
	monitor *goredis.MonitorCmd,
) {
	t.Helper()
	stopped := make(chan struct{})
	go func() {
		monitor.Stop()
		close(stopped)
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-stopped:
			_ = control.Echo(ctx, "chronicle-live-read-monitor-stopped").Err()
			time.Sleep(10 * time.Millisecond)
			if err := monitorClient.Close(); err != nil {
				t.Errorf("close Redis MONITOR client: %v", err)
			}
			return
		case <-ticker.C:
			_ = control.Echo(ctx, "chronicle-live-read-monitor-stop").Err()
		case <-timer.C:
			_ = monitorClient.Close()
			t.Errorf("timed out stopping Redis MONITOR")
			return
		}
	}
}

func countLiveReadCommand(lines []string, command, key string) int {
	quotedKey := strconv.Quote(key)
	count := 0
	for _, line := range lines {
		_, rawCommand, ok := strings.Cut(line, "] ")
		if !ok {
			continue
		}
		name, _, _ := strings.Cut(rawCommand, " ")
		if strings.EqualFold(strings.Trim(name, `"`), command) && strings.Contains(line, quotedKey) {
			count++
		}
	}
	return count
}
