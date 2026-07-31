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

func TestIntegrationSSERecoversOrderedDataAfterPubSubConnectionKill(t *testing.T) {
	rawURL := os.Getenv("REDIS_URL")
	if rawURL == "" {
		t.Skip("REDIS_URL is required for the destructive Pub/Sub connection-kill test")
	}
	options, err := goredis.ParseURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	options.ClientName = fmt.Sprintf("chronicle-issue13-recovery-%d", time.Now().UnixNano())
	dataClient := goredis.NewClient(options)
	controlOptions := *options
	controlOptions.ClientName = options.ClientName + "-control"
	control := goredis.NewClient(&controlOptions)
	t.Cleanup(func() { _ = control.Close() })
	ctx := t.Context()
	if err := control.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis ping: %v", err)
	}
	if err := control.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Redis flush: %v", err)
	}

	st := redisstore.New(dataClient, redisstore.Options{})
	t.Cleanup(func() { _ = st.Close() })
	metrics := &recordingSSEMetrics{}
	h := newHubTestHandler(st)
	h.SSEMetrics = metrics
	h.SSEHubPollInterval = time.Second
	if _, created, err := st.Create(
		"/connection-kill",
		store.CreateOptions{ContentType: "text/plain"},
	); err != nil || !created {
		t.Fatalf("create stream: created=%t err=%v", created, err)
	}

	server := httptest.NewServer(h)
	defer server.Close()
	response, err := http.Get(server.URL + "/connection-kill?offset=-1&live=sse")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close() //nolint:errcheck // test cleanup
	waitForCount(t, &metrics.subscriptions, 1)
	waitForCount(t, &metrics.physical, 1)
	waitForAttachedSSEWatchers(t, h, "/connection-kill", 1)

	pubsubID, err := redisPubSubClientID(ctx, control, options.ClientName)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	killed, err := control.ClientKillByFilter(ctx, "ID", strconv.FormatInt(pubsubID, 10)).Result()
	if err != nil || killed != 1 {
		t.Fatalf("kill Pub/Sub client %d: killed=%d err=%v", pubsubID, killed, err)
	}

	const messages = 20
	for n := range messages {
		result, appendErr := st.Append(
			"/connection-kill",
			[]byte(fmt.Sprintf("|%02d|", n)),
			store.AppendOptions{
				ContentType: "text/plain",
				Close:       n == messages-1,
			},
		)
		if appendErr != nil {
			t.Fatalf("append %d: %v", n, appendErr)
		}
		if result.Offset.IsZero() {
			t.Fatalf("append %d returned zero offset", n)
		}
		time.Sleep(20 * time.Millisecond)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	if elapsed > 1200*time.Millisecond {
		t.Fatalf("ordered recovery took %s, want <= 1.2s around the documented 1s bound", elapsed)
	}
	text := string(body)
	previous := -1
	for n := range messages {
		marker := fmt.Sprintf("|%02d|", n)
		if count := strings.Count(text, marker); count != 1 {
			t.Fatalf("marker %s count = %d, want 1", marker, count)
		}
		index := strings.Index(text, marker)
		if index <= previous {
			t.Fatalf("marker %s reordered at %d after %d", marker, index, previous)
		}
		previous = index
	}
	if !strings.Contains(text, `"streamClosed":true`) {
		t.Fatalf("close control missing after Pub/Sub recovery: %q", text)
	}
	if reconnects := metrics.reconnects.Load(); reconnects < 1 {
		t.Fatal("notification multiplexer did not record a reconnect generation")
	}
}

func TestIntegrationClusterSSEUsesGlobalNotificationMultiplexer(t *testing.T) {
	rawAddresses := os.Getenv("REDIS_CLUSTER_ADDRS")
	if rawAddresses == "" {
		t.Skip("REDIS_CLUSTER_ADDRS is required for Redis Cluster SSE integration")
	}
	client := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs: strings.Split(rawAddresses, ","),
	})
	ctx := t.Context()
	if err := client.ForEachMaster(ctx, func(ctx context.Context, master *goredis.Client) error {
		return master.FlushDB(ctx).Err()
	}); err != nil {
		t.Fatalf("flush cluster: %v", err)
	}
	st := redisstore.New(client, redisstore.Options{})
	t.Cleanup(func() { _ = st.Close() })
	metrics := &recordingSSEMetrics{}
	h := newHubTestHandler(st)
	h.SSEMetrics = metrics
	if _, created, err := st.Create(
		"/cluster-sse",
		store.CreateOptions{ContentType: "text/plain"},
	); err != nil || !created {
		t.Fatalf("create cluster stream: created=%t err=%v", created, err)
	}

	server := httptest.NewServer(h)
	defer server.Close()
	response, err := http.Get(server.URL + "/cluster-sse?offset=-1&live=sse")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close() //nolint:errcheck // test cleanup
	waitForCount(t, &metrics.subscriptions, 1)
	waitForCount(t, &metrics.physical, 1)
	if _, err := st.Append(
		"/cluster-sse",
		[]byte("cluster-global"),
		store.AppendOptions{ContentType: "text/plain", Close: true},
	); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "event: data\ndata:cluster-global\n\n") ||
		!strings.Contains(string(body), `"streamClosed":true`) {
		t.Fatalf("cluster SSE framing incomplete: %q", body)
	}
}

func redisPubSubClientID(
	ctx context.Context,
	client *goredis.Client,
	name string,
) (int64, error) {
	list, err := client.ClientList(ctx).Result()
	if err != nil {
		return 0, fmt.Errorf("CLIENT LIST: %w", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(list), "\n") {
		fields := make(map[string]string)
		for _, field := range strings.Fields(line) {
			key, value, found := strings.Cut(field, "=")
			if found {
				fields[key] = value
			}
		}
		if fields["name"] != name || fields["sub"] == "0" {
			continue
		}
		id, parseErr := strconv.ParseInt(fields["id"], 10, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("parse Pub/Sub client id %q: %w", fields["id"], parseErr)
		}
		return id, nil
	}
	return 0, fmt.Errorf("Pub/Sub client named %q not found in CLIENT LIST", name)
}

func waitForAttachedSSEWatchers(
	t *testing.T,
	h *Handler,
	path string,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.sseHubs.mu.Lock()
		entry := h.sseHubs.hubs[path]
		attached := 0
		if entry != nil {
			entry.hub.mu.Lock()
			for watcher := range entry.hub.watchers {
				if watcher.attached {
					attached++
				}
			}
			entry.hub.mu.Unlock()
		}
		h.sseHubs.mu.Unlock()
		if attached == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("attached SSE watchers for %s did not reach %d", path, want)
}
