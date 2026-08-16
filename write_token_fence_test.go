package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

type writeFenceStreams struct{}

type deposeOnEOFReader struct {
	data []byte
	done bool
	fn   func()
}

func (r *deposeOnEOFReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	if !r.done {
		r.done = true
		r.fn()
	}
	return 0, io.EOF
}

func (writeFenceStreams) TailOffset(string) (string, bool) {
	return "0000000000000000_0000000000000001", true
}

func (writeFenceStreams) TailOffsets(paths []string) (map[string]string, error) {
	out := make(map[string]string, len(paths))
	for _, p := range paths {
		out[p] = "0000000000000000_0000000000000001"
	}
	return out, nil
}

func (writeFenceStreams) BeginningOffset() string              { return "0000000000000000_0000000000000000" }
func (writeFenceStreams) AppendWakeEvent(string, []byte) error { return nil }

type pausedAppendStore struct {
	store.Store
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (s *pausedAppendStore) Append(path string, data []byte, opts store.AppendOptions) (store.AppendResult, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.resume
	return s.Store.Append(path, data, opts)
}

func newWriteFenceManager(t *testing.T) (*webhook.Manager, *webhook.RedisStore, func()) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Redis integration test in -short mode")
	}
	rawURL := os.Getenv("CHRONICLE_ITEST_REDIS_URL")
	if rawURL == "" {
		rawURL = "redis://localhost:6379/13"
	}
	options, err := goredis.ParseURL(rawURL)
	if err != nil {
		t.Fatalf("parse Redis URL: %v", err)
	}
	client := goredis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unreachable at %s: %v", rawURL, err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	subStore := webhook.NewRedisStore(client)
	mgr, err := webhook.NewManager(subStore, writeFenceStreams{}, webhook.ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		_ = client.Close()
		t.Fatalf("new manager: %v", err)
	}
	return mgr, subStore, func() { _ = client.Close() }
}

func claimForWriteFence(t *testing.T, rt *webhook.Routes, subStore *webhook.RedisStore) webhook.ClaimResponse {
	t.Helper()
	now := time.Now()
	cfg := webhook.Config{Type: webhook.DispatchPullWake, Pattern: "events/*", WakeStream: "wake/pool", LeaseTTLMs: 1000}
	if _, err := subStore.CreateOrConfirm("s1", cfg, nil, now); err != nil {
		t.Fatalf("create sub: %v", err)
	}
	if err := subStore.Link("s1", "events/a", webhook.LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatalf("link: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/__ds/subscriptions/s1/claim", bytes.NewReader([]byte(`{"worker":"worker-A"}`)))
	rec := httptest.NewRecorder()
	if !rt.HandleRequest(rec, req) {
		t.Fatal("claim route did not handle request")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("claim = %d body %q", rec.Code, rec.Body.String())
	}
	var cr webhook.ClaimResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &cr); err != nil {
		t.Fatal(err)
	}
	if cr.WriteToken == "" {
		t.Fatal("claim response missing write token")
	}
	return cr
}

func TestHandleAppendRejectsDeposedWriteToken(t *testing.T) {
	mgr, subStore, cleanup := newWriteFenceManager(t)
	defer cleanup()
	rt := webhook.NewRoutes(mgr)
	crA := claimForWriteFence(t, rt, subStore)

	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeEnforce
	h.AppendAuth = mgr.WriteAuthorizer()
	createDirect(t, h, "/events/a", "application/json")
	before := tailOf(t, h, "/events/a")

	takeoverAt := time.Now().Add(2 * time.Second)
	crB, err := subStore.Claim("s1", "worker-B", "w_b", takeoverAt, 1000)
	if err != nil || !crB.Claimed || crB.Generation == crA.Generation {
		t.Fatalf("takeover claim = %+v err=%v", crB, err)
	}
	if st, _ := subStore.AckUnscoped("s1", crB.Generation, crB.WakeID, crB.Generation, true, nil, takeoverAt, 1000); st != "OK" {
		t.Fatalf("current holder ack = %q, want OK", st)
	}

	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type":   "application/json",
		ClaimTokenHeader: crA.WriteToken,
	}, []byte(`{"stale":true}`))
	if rec.Code != http.StatusConflict {
		t.Fatalf("deposed append = %d body %q, want 409", rec.Code, rec.Body.String())
	}
	var body webhook.ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != webhook.ErrCodeFenced {
		t.Fatalf("error code = %q, want FENCED", body.Error.Code)
	}
	if after := tailOf(t, h, "/events/a"); !after.Equal(before) {
		t.Fatalf("deposed append mutated stream: tail %s -> %s", before, after)
	}
}

func TestHandleAppendAllowsCurrentWriteToken(t *testing.T) {
	mgr, subStore, cleanup := newWriteFenceManager(t)
	defer cleanup()
	cr := claimForWriteFence(t, webhook.NewRoutes(mgr), subStore)

	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeEnforce
	h.AppendAuth = mgr.WriteAuthorizer()
	createDirect(t, h, "/events/a", "application/json")

	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type":   "application/json",
		ClaimTokenHeader: cr.WriteToken,
	}, []byte(`{"ok":true}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("current append = %d body %q, want 204", rec.Code, rec.Body.String())
	}
	if got := tailOf(t, h, "/events/a"); got.ByteOffset == 0 {
		t.Fatal("current holder append did not land")
	}
}

func TestHandleAppendFencesAfterBodyRead(t *testing.T) {
	mgr, subStore, cleanup := newWriteFenceManager(t)
	defer cleanup()
	crA := claimForWriteFence(t, webhook.NewRoutes(mgr), subStore)

	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeEnforce
	h.AppendAuth = mgr.WriteAuthorizer()
	createDirect(t, h, "/events/a", "application/json")
	before := tailOf(t, h, "/events/a")

	body := &deposeOnEOFReader{data: []byte(`{"stale":true}`), fn: func() {
		takeoverAt := time.Now().Add(2 * time.Second)
		crB, err := subStore.Claim("s1", "worker-B", "w_b", takeoverAt, 1000)
		if err != nil || !crB.Claimed {
			t.Fatalf("takeover claim during body read = %+v err=%v", crB, err)
		}
	}}
	req := httptest.NewRequest(http.MethodPost, "/events/a", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(ClaimTokenHeader, crA.WriteToken)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("append after body-read deposition = %d body %q, want 409", rec.Code, rec.Body.String())
	}
	if after := tailOf(t, h, "/events/a"); !after.Equal(before) {
		t.Fatalf("body-read TOCTOU mutated stream: tail %s -> %s", before, after)
	}
}

func TestHeartbeatRefreshesWriteTokenForLongLiveHolder(t *testing.T) {
	mgr, subStore, cleanup := newWriteFenceManager(t)
	defer cleanup()
	rt := webhook.NewRoutes(mgr)
	cr := claimForWriteFence(t, rt, subStore)
	currentWriteToken := cr.WriteToken

	deadline := time.Now().Add(7 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		body, _ := json.Marshal(webhook.CallbackRequest{WakeID: cr.WakeID, Generation: cr.Generation})
		req := httptest.NewRequest(http.MethodPost, "/__ds/subscriptions/s1/ack", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+cr.Token)
		rec := httptest.NewRecorder()
		if !rt.HandleRequest(rec, req) {
			t.Fatal("ack route did not handle request")
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("heartbeat = %d body %q", rec.Code, rec.Body.String())
		}
		var resp webhook.AckResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if resp.WriteToken == "" {
			t.Fatalf("heartbeat response missing refreshed write_token: %s", rec.Body.String())
		}
		currentWriteToken = resp.WriteToken
	}

	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeEnforce
	h.AppendAuth = mgr.WriteAuthorizer()
	createDirect(t, h, "/events/a", "application/json")
	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type":   "application/json",
		ClaimTokenHeader: currentWriteToken,
	}, []byte(`{"ok":true}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("append after long heartbeat = %d body %q, want 204", rec.Code, rec.Body.String())
	}
}

func TestHandleAppendReleaseRaceIsFencedInsideRedisCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis integration test in -short mode")
	}
	rawURL := os.Getenv("CHRONICLE_ITEST_REDIS_URL")
	if rawURL == "" {
		rawURL = "redis://localhost:6379/13"
	}
	options, err := goredis.ParseURL(rawURL)
	if err != nil {
		t.Fatalf("parse Redis URL: %v", err)
	}
	client := goredis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("redis unreachable at %s: %v", rawURL, err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("flushdb: %v", err)
	}

	dataStore := redisstore.New(client, redisstore.Options{})
	t.Cleanup(func() { _ = dataStore.Close() })
	subStore := webhook.NewRedisStore(client)
	streams := redisFenceStreamAdapter{streamAdapter{st: dataStore, rs: dataStore}}
	mgr, err := webhook.NewManager(subStore, streams, webhook.ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	rt := webhook.NewRoutes(mgr)

	paused := &pausedAppendStore{
		Store:   dataStore,
		entered: make(chan struct{}),
		resume:  make(chan struct{}),
	}
	h := testHandler(time.Second, time.Second)
	h.Store = paused
	h.AuthMode = auth.ModeEnforce
	h.AppendAuth = mgr.WriteAuthorizer()
	createDirect(t, h, "/events/a", "application/json")
	cr := claimForWriteFence(t, rt, subStore)
	before := tailOf(t, h, "/events/a")

	appendResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		appendResult <- do(h, http.MethodPost, "/events/a", map[string]string{
			"Content-Type":   "application/json",
			ClaimTokenHeader: cr.WriteToken,
		}, []byte(`{"stale":true}`))
	}()
	select {
	case <-paused.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("append did not reach the paused store commit")
	}

	releaseBody, err := json.Marshal(webhook.ReleaseRequest{
		Generation: cr.Generation,
		WakeID:     cr.WakeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	releaseReq := httptest.NewRequest(http.MethodPost, "/__ds/subscriptions/s1/release", bytes.NewReader(releaseBody))
	releaseReq.Header.Set("Authorization", "Bearer "+cr.Token)
	releaseRec := httptest.NewRecorder()
	if !rt.HandleRequest(releaseRec, releaseReq) {
		t.Fatal("release route did not handle request")
	}
	if releaseRec.Code != http.StatusNoContent {
		t.Fatalf("release = %d body %q, want 204", releaseRec.Code, releaseRec.Body.String())
	}

	close(paused.resume)
	var rec *httptest.ResponseRecorder
	select {
	case rec = <-appendResult:
	case <-time.After(5 * time.Second):
		t.Fatal("append did not return after release")
	}
	if rec.Code != http.StatusConflict {
		t.Fatalf("append after release linearized = %d body %q, want 409", rec.Code, rec.Body.String())
	}
	var body webhook.ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != webhook.ErrCodeFenced {
		t.Fatalf("error code = %q, want %q", body.Error.Code, webhook.ErrCodeFenced)
	}
	if after := tailOf(t, h, "/events/a"); !after.Equal(before) {
		t.Fatalf("atomic append fence mutated stream: tail %s -> %s", before, after)
	}
}

func TestNewSubscriptionsEnforceRequiresAtomicStreamStore(t *testing.T) {
	_, _, _, err := NewSubscriptions(
		nil,
		store.NewMemoryStore(),
		nil,
		"http://x/v1/stream/",
		false,
		SubscriptionTuning{AuthMode: auth.ModeEnforce},
		slog.Default(),
	)
	if err == nil {
		t.Fatal("enforce mode accepted a stream store without Redis append fencing")
	}
}
