package webhook

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
)

// jsonLogger returns a Debug-level JSON logger and the buffer it writes to.
func jsonLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

// logEvents decodes the JSON records in buf whose "event" field equals event.
func logEvents(t *testing.T, buf *bytes.Buffer, event string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		if rec["event"] == event {
			out = append(out, rec)
		}
	}
	return out
}

// recordingTransport answers each delivery with the next status in statuses
// (200 once they run out), records the value of header on every POST, and
// closes settled once the first response body is closed, which the Manager
// does only after it has recorded that delivery's outcome.
type recordingTransport struct {
	header   string
	statuses []int
	settled  chan struct{}
	once     sync.Once
	mu       sync.Mutex
	headers  []http.Header
}

func newRecordingTransport(header string, statuses ...int) *recordingTransport {
	return &recordingTransport{header: header, statuses: statuses, settled: make(chan struct{})}
}

type settleOnClose struct {
	io.Reader
	settle func()
}

func (b settleOnClose) Close() error { b.settle(); return nil }

func (t *recordingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.mu.Lock()
	call := len(t.headers)
	t.headers = append(t.headers, r.Header.Clone())
	status := http.StatusOK
	if call < len(t.statuses) {
		status = t.statuses[call]
	}
	t.mu.Unlock()
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       settleOnClose{Reader: strings.NewReader(`{}`), settle: func() { t.once.Do(func() { close(t.settled) }) }},
	}, nil
}

// headerValues returns the value of header on each POST so far.
func (t *recordingTransport) headerValues() []string {
	headers := t.postHeaders()
	values := make([]string, 0, len(headers))
	for _, h := range headers {
		values = append(values, h.Get(t.header))
	}
	return values
}

// postHeaders returns the full header set of each POST so far.
func (t *recordingTransport) postHeaders() []http.Header {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]http.Header(nil), t.headers...)
}

func (t *recordingTransport) waitSettled(t0 *testing.T) {
	t0.Helper()
	select {
	case <-t.settled:
	case <-time.After(5 * time.Second):
		t0.Fatal("webhook delivery did not settle")
	}
}

// webhookFixture arms one webhook subscription on events/a through the dirty
// worker with the given request id and returns the Manager and the store.
func webhookFixture(t *testing.T, opts ManagerOptions, requestID string) (*Manager, *RedisStore) {
	t.Helper()
	return armWebhookFixture(t, opts, correlation.WithRequestID(context.Background(), requestID))
}

// armWebhookFixture is webhookFixture for an append whose context is given:
// its request id, its trace, or neither.
func armWebhookFixture(t *testing.T, opts ManagerOptions, appendCtx context.Context) (*Manager, *RedisStore) {
	t.Helper()
	base, _ := newTestStore(t)
	streams := &fakeStreams{tails: map[string]string{"events/a": "0000000000000001_0000000000000000"}}
	opts.StreamRootURL = "http://x/v1/stream/"
	mgr, err := NewManager(base, streams, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := base.Link("s1", "events/a", LinkGlob, streams.BeginningOffset()); err != nil {
		t.Fatal(err)
	}
	mgr.OnStreamAppend(appendCtx, "events/a")
	if got := mgr.RunDirtyWorker(); got != 1 {
		t.Fatalf("dirty worker processed %d hints, want 1", got)
	}
	return mgr, base
}

func TestWebhookDeliveryCarriesRequestID(t *testing.T) {
	post := newRecordingTransport(correlation.DefaultHeader)
	_, _ = webhookFixture(t, ManagerOptions{HTTPClient: &http.Client{Transport: post}}, "gateway-request-123")
	post.waitSettled(t)
	if got := post.headerValues(); len(got) != 1 || got[0] != "gateway-request-123" {
		t.Fatalf("delivery %s values = %v, want [gateway-request-123]", correlation.DefaultHeader, got)
	}
}

func TestWebhookDeliveryUsesConfiguredHeader(t *testing.T) {
	const header = "My-Platform-Request-ID"
	post := newRecordingTransport(header)
	_, _ = webhookFixture(t, ManagerOptions{HTTPClient: &http.Client{Transport: post}, RequestIDHeader: header}, "platform-7")
	post.waitSettled(t)
	if got := post.headerValues(); len(got) != 1 || got[0] != "platform-7" {
		t.Fatalf("delivery %s values = %v, want [platform-7]", header, got)
	}
}

func TestWebhookRetryReusesRequestID(t *testing.T) {
	post := newRecordingTransport(correlation.DefaultHeader, http.StatusServiceUnavailable)
	mgr, base := webhookFixture(t, ManagerOptions{HTTPClient: &http.Client{Transport: post}}, "gateway-request-123")
	post.waitSettled(t)
	sub, _, _ := base.Get("s1")
	if sub.RetryCount != 1 {
		t.Fatalf("first failure did not schedule a retry: %+v", sub)
	}
	mgr.deliverWebhookUnscoped("s1", sub.Generation, sub.WakeID)
	if got := post.headerValues(); len(got) != 2 || got[0] != "gateway-request-123" || got[1] != got[0] {
		t.Fatalf("request ids across retry = %v, want the same gateway-request-123 twice", got)
	}
}

// postCallback presents a fresh callback token for (gen, wakeID) on a callback
// with no acks, a heartbeat unless done, whose request context carries
// ctxRequestID when non-empty.
func postCallback(t *testing.T, rt *Routes, id string, gen int64, wakeID, ctxRequestID string, done bool) *httptest.ResponseRecorder {
	t.Helper()
	token, err := GenerateToken(rt.mgr.tokenKey, id, gen, time.Now(), time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	req := CallbackRequest{WakeID: wakeID, Generation: gen}
	if done {
		req.Done = &done
	}
	body, _ := json.Marshal(req)
	r := httptest.NewRequest(http.MethodPost, "/__ds/subscriptions/"+id+"/callback", bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	if ctxRequestID != "" {
		r = r.WithContext(correlation.WithRequestID(r.Context(), ctxRequestID))
	}
	w := httptest.NewRecorder()
	rt.HandleRequest(w, r)
	return w
}

func TestCallbackLogsTheWakeRequestID(t *testing.T) {
	logger, logs := jsonLogger()
	post := newRecordingTransport(correlation.DefaultHeader)
	mgr, base := webhookFixture(t, ManagerOptions{HTTPClient: &http.Client{Transport: post}, Logger: logger}, "gateway-request-123")
	post.waitSettled(t)
	sub, _, _ := base.Get("s1")
	rt := NewRoutes(mgr)

	// Without an id of its own, the callback is attributed to the wake's id.
	if w := postCallback(t, rt, "s1", sub.Generation, sub.WakeID, "", false); w.Code != http.StatusOK {
		t.Fatalf("heartbeat status = %d body %s", w.Code, w.Body.String())
	}
	// With its own id, both are logged so the two sides can be joined.
	if w := postCallback(t, rt, "s1", sub.Generation, sub.WakeID, "callback-9", false); w.Code != http.StatusOK {
		t.Fatalf("heartbeat status = %d body %s", w.Code, w.Body.String())
	}
	events := logEvents(t, logs, "subscription_ack_completed")
	if len(events) != 2 {
		t.Fatalf("ack events = %d, want 2: %s", len(events), logs.String())
	}
	for i, want := range []struct{ request, wake string }{{"gateway-request-123", "gateway-request-123"}, {"callback-9", "gateway-request-123"}} {
		ev := events[i]
		if ev["request_id"] != want.request || ev["wake_request_id"] != want.wake ||
			ev["wake_id"] != sub.WakeID || ev["operation"] != "callback" || ev["ack_mode"] != "heartbeat" || ev["outcome"] != "accepted" {
			t.Fatalf("ack event %d = %#v, want request_id %q wake_request_id %q", i, ev, want.request, want.wake)
		}
	}
}

func TestReleaseLogsTheWakeRequestID(t *testing.T) {
	logger, logs := jsonLogger()
	base, _ := newTestStore(t)
	mgr, err := NewManager(base, &fakeStreams{tails: map[string]string{}}, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := base.CreateOrConfirm("p1", pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	claim, err := base.Claim("p1", "worker-1", "w_release", now, pullWakeCfg().LeaseTTLMs)
	if err != nil || !claim.Claimed {
		t.Fatalf("claim = %+v err=%v", claim, err)
	}
	token, err := GenerateToken(mgr.tokenKey, "p1", claim.Generation, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(ReleaseRequest{WakeID: claim.WakeID, Generation: claim.Generation})
	r := httptest.NewRequest(http.MethodPost, "/__ds/subscriptions/p1/release", bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	NewRoutes(mgr).HandleRequest(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("release status = %d body %s", w.Code, w.Body.String())
	}
	events := logEvents(t, logs, "subscription_release_completed")
	// A wake this process never armed falls back to the stable wake-<id>.
	if len(events) != 1 || events[0]["wake_request_id"] != "wake-"+claim.WakeID || events[0]["request_id"] != "wake-"+claim.WakeID || events[0]["outcome"] != "released" {
		t.Fatalf("release events = %#v, want one released event attributed to wake-%s", events, claim.WakeID)
	}
}

func TestAppendHintLogsAtDebugAndWarnsOncePerOverflowEpoch(t *testing.T) {
	logger, logs := jsonLogger()
	base, _ := newTestStore(t)
	mgr, err := NewManager(base, &fakeStreams{tails: map[string]string{}}, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	ctx := correlation.WithRequestID(context.Background(), "gateway-request-123")
	for i := 0; i < dirtyQueueCapacity+2; i++ {
		mgr.OnStreamAppend(ctx, "events/"+string(rune('a'+i%26))+"/"+strings.Repeat("x", i/26+1))
	}
	events := logEvents(t, logs, "append_hint_queued")
	if len(events) != dirtyQueueCapacity+2 {
		t.Fatalf("append_hint_queued events = %d, want one per append", len(events))
	}
	var warns int
	for _, ev := range events {
		if ev["request_id"] != "gateway-request-123" {
			t.Fatalf("append hint event without the request id: %#v", ev)
		}
		if ev["level"] == "WARN" {
			warns++
		} else if ev["level"] != "DEBUG" {
			t.Fatalf("append hint event at %v, want DEBUG: %#v", ev["level"], ev)
		}
	}
	if warns != 1 {
		t.Fatalf("WARN append hint events = %d, want exactly one for the overflow epoch", warns)
	}
}

// wakeMemoryTTL is how long a wake's request id may go unused before this
// replica forgets it: the subscription's lease plus the longest retry gap.
func wakeMemoryTTL(sub Subscription) time.Duration {
	return time.Duration(sub.Config.LeaseTTLMs)*time.Millisecond + maxRetryDelay
}

func TestCallbackOnAnotherReplicaFallsBackAndTheArmingReplicaForgets(t *testing.T) {
	post := newRecordingTransport(correlation.DefaultHeader)
	a, base := webhookFixture(t, ManagerOptions{HTTPClient: &http.Client{Transport: post}}, "gateway-request-123")
	post.waitSettled(t)
	sub, _, _ := base.Get("s1")

	loggerB, logsB := jsonLogger()
	b, err := NewManager(base, &fakeStreams{tails: map[string]string{}}, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Logger: loggerB})
	if err != nil {
		t.Fatal(err)
	}
	if w := postCallback(t, NewRoutes(b), "s1", sub.Generation, sub.WakeID, "", true); w.Code != http.StatusOK {
		t.Fatalf("done callback on replica B = %d body %s", w.Code, w.Body.String())
	}
	events := logEvents(t, logsB, "subscription_ack_completed")
	if len(events) != 1 || events[0]["wake_request_id"] != "wake-"+sub.WakeID || events[0]["ack_mode"] != "done" {
		t.Fatalf("replica B never armed the wake and must fall back to wake-<id>: %#v", events)
	}

	// Replica A saw neither the done nor a release, so only time frees its
	// memory of the wake: once the lease and retry window lapse with no use,
	// the worker tick sweeps it.
	if got := a.requestIDForWake(sub.WakeID); got != "gateway-request-123" {
		t.Fatalf("replica A remembers %q before the window lapses, want gateway-request-123", got)
	}
	later := time.Now().Add(2 * wakeMemoryTTL(sub))
	a.now = func() time.Time { return later }
	a.RunDirtyWorker()
	if got := a.requestIDForWake(sub.WakeID); got != "wake-"+sub.WakeID {
		t.Fatalf("replica A still remembers %q after the lease and retry window lapsed, want wake-%s", got, sub.WakeID)
	}
}

func TestWakeRequestIDExpiresWithTheLeaseAndRetryWindow(t *testing.T) {
	post := newRecordingTransport(correlation.DefaultHeader)
	mgr, base := webhookFixture(t, ManagerOptions{HTTPClient: &http.Client{Transport: post}}, "gateway-request-123")
	post.waitSettled(t)
	sub, _, _ := base.Get("s1")
	ttl := wakeMemoryTTL(sub)
	start := time.Now()

	// Still inside the window: remembered (and the use refreshes the window).
	mgr.now = func() time.Time { return start.Add(ttl - time.Second) }
	mgr.RunDirtyWorker()
	if got := mgr.requestIDForWake(sub.WakeID); got != "gateway-request-123" {
		t.Fatalf("forgot the wake's id %v before its window lapsed: %q", ttl, got)
	}
	// A full window of silence after that use: forgotten.
	mgr.now = func() time.Time { return start.Add(2*ttl + time.Second) }
	mgr.RunDirtyWorker()
	if got := mgr.requestIDForWake(sub.WakeID); got != "wake-"+sub.WakeID {
		t.Fatalf("lease and retry window lapsed with no use but the id is still remembered: %q", got)
	}
}
