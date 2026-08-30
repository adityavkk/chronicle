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
	"strconv"
	"strings"
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
	return claimForWriteFenceOn(t, rt, subStore, "events/a")
}

// claimForWriteFenceOn creates subscription s1 linked to paths and claims it
// through the routes as worker-A, so every linked stream that exists gets its
// marker and the write token is scoped to exactly those paths.
func claimForWriteFenceOn(t *testing.T, rt *webhook.Routes, subStore *webhook.RedisStore, paths ...string) webhook.ClaimResponse {
	t.Helper()
	now := time.Now()
	cfg := webhook.Config{Type: webhook.DispatchPullWake, Pattern: "events/*", WakeStream: "wake/pool", LeaseTTLMs: 1000}
	if _, err := subStore.CreateOrConfirm("s1", cfg, nil, now); err != nil {
		t.Fatalf("create sub: %v", err)
	}
	for _, path := range paths {
		if err := subStore.Link("s1", path, webhook.LinkGlob, "0000000000000000_0000000000000000"); err != nil {
			t.Fatalf("link %s: %v", path, err)
		}
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

// TestHandleCloseRejectsDeposedWriteToken pins A.0 Q7 (addendum §1): a
// close-only POST is classed and fenced like any append, so a deposed
// holder's token cannot close a stream — 409 FENCED, the stream stays open,
// the tail is unchanged, and with producer headers the terminal pair is
// carried — while the live holder's close-only lands (the positive control).
func TestHandleCloseRejectsDeposedWriteToken(t *testing.T) {
	mgr, subStore, cleanup := newWriteFenceManager(t)
	defer cleanup()
	rt := webhook.NewRoutes(mgr)
	crA := claimForWriteFenceOn(t, rt, subStore, "events/a", "events/b")

	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeEnforce
	h.AppendAuth = mgr.WriteAuthorizer()
	createDirect(t, h, "/events/a", "application/json")
	createDirect(t, h, "/events/b", "application/json")

	rec := do(h, http.MethodPost, "/events/b", map[string]string{"Stream-Closed": "true", ClaimTokenHeader: crA.WriteToken}, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("live holder close-only = %d body %q, want 204", rec.Code, rec.Body.String())
	}
	if meta, _ := h.Store.Get("/events/b"); !meta.Closed {
		t.Fatal("live holder close-only did not close the stream")
	}
	before := tailOf(t, h, "/events/a")

	takeoverAt := time.Now().Add(2 * time.Second)
	crB, err := subStore.Claim("s1", "worker-B", "w_b", takeoverAt, 1000)
	if err != nil || !crB.Claimed || crB.Generation == crA.Generation {
		t.Fatalf("takeover claim = %+v err=%v", crB, err)
	}
	if st, _ := subStore.AckUnscoped("s1", crB.Generation, crB.WakeID, crB.Generation, true, nil, takeoverAt, 1000); st != "OK" {
		t.Fatalf("current holder ack = %q, want OK", st)
	}

	for _, c := range []struct {
		name    string
		headers map[string]string
		seq     string // "" = no producer headers, hence no terminal pair
	}{
		{"without producer headers", map[string]string{}, ""},
		{"with producer headers", map[string]string{"Producer-Id": "p1", "Producer-Epoch": "1", "Producer-Seq": "3"}, "3"},
	} {
		t.Run(c.name, func(t *testing.T) {
			headers := map[string]string{"Stream-Closed": "true", ClaimTokenHeader: crA.WriteToken}
			for k, v := range c.headers {
				headers[k] = v
			}
			rec := do(h, http.MethodPost, "/events/a", headers, nil)
			if rec.Code != http.StatusConflict {
				t.Fatalf("deposed close-only = %d body %q, want 409", rec.Code, rec.Body.String())
			}
			eb := decodeEnvelope(t, rec)
			if eb.Error.Code != webhook.ErrCodeFenced || eb.Error.Reason != "precheck" {
				t.Fatalf("envelope = %+v, want FENCED/precheck", eb.Error)
			}
			if exp, rcv := rec.Header().Get("Producer-Expected-Seq"), rec.Header().Get("Producer-Received-Seq"); exp != c.seq || rcv != c.seq {
				t.Errorf("terminal pair = (%q, %q), want both %q", exp, rcv, c.seq)
			}
			if meta, _ := h.Store.Get("/events/a"); meta.Closed {
				t.Fatal("deposed close-only closed the stream")
			}
			if after := tailOf(t, h, "/events/a"); !after.Equal(before) {
				t.Fatalf("deposed close-only mutated stream: tail %s -> %s", before, after)
			}
		})
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

// redisFenceStack is the root integration fixture of the write fence on live
// Redis (db13): the Redis stream store is both the handler's store and the
// manager's fence-capable streams, so a claim through the routes grants the
// stream-slot marker and every append runs the Lua rung. The handler enforces,
// verifies write tokens under the manager's key, and trusts tb4SvcBearer as a
// gateway service principal for open-class writes.
type redisFenceStack struct {
	h        *Handler
	rt       *webhook.Routes
	subStore *webhook.RedisStore
	data     *redisstore.Store
}

func newRedisFenceStack(t *testing.T) *redisFenceStack {
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
		_ = client.Close()
		t.Skipf("redis unreachable at %s: %v", rawURL, err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("flushdb: %v", err)
	}

	dataStore := redisstore.New(client, redisstore.Options{})
	t.Cleanup(func() {
		_ = dataStore.Close()
		_ = client.Close()
	})
	subStore := webhook.NewRedisStore(client)
	streams := redisFenceStreamAdapter{streamAdapter{st: dataStore, rs: dataStore}}
	mgr, err := webhook.NewManager(subStore, streams, webhook.ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	creds, err := auth.ParseServiceBearerConfig("agents-server:" + tb4SvcBearer)
	if err != nil {
		t.Fatal(err)
	}
	h := testHandler(time.Second, time.Second)
	h.Store = dataStore
	h.AuthMode = auth.ModeEnforce
	h.AppendAuth = mgr.WriteAuthorizer()
	h.ServiceAuth = &ServiceAuth{Credentials: creds, Policies: gatewayPolicies(t, "agents-server")}
	return &redisFenceStack{h: h, rt: webhook.NewRoutes(mgr), subStore: subStore, data: dataStore}
}

// createFenced seeds a write-fenced stream through the Redis store.
func (s *redisFenceStack) createFenced(t *testing.T, path string) {
	t.Helper()
	if _, _, err := s.data.Create(path, store.CreateOptions{ContentType: "application/json", WriteFence: true}); err != nil {
		t.Fatal(err)
	}
}

// TestHandleAppendFencedStreamRequiresProducerEpochEqualGeneration pins WF-10
// and WF-11 end to end on Redis: on a fenced stream the holder's write lands
// only with all three producer headers and Producer-Epoch equal to its claim
// generation (positive control first, then the idempotent retry), a different
// epoch is 409 epoch with the generation, the holder, the Producer-Epoch echo
// and the terminal pair, and missing producer headers are the 400 — with the
// tail unchanged on every refusal.
func TestHandleAppendFencedStreamRequiresProducerEpochEqualGeneration(t *testing.T) {
	s := newRedisFenceStack(t)
	s.createFenced(t, "/events/a")
	cr := claimForWriteFence(t, s.rt, s.subStore)
	gen := strconv.FormatInt(cr.Generation, 10)
	holder := func(epoch, seq string) map[string]string {
		return map[string]string{
			"Content-Type": "application/json", WriteTokenHeader: cr.WriteToken,
			"Producer-Id": "entity-events/a", "Producer-Epoch": epoch, "Producer-Seq": seq,
		}
	}

	rec := do(s.h, http.MethodPost, "/events/a", holder(gen, "0"), []byte(`{"turn":1}`))
	if rec.Code != http.StatusOK || rec.Header().Get("Producer-Epoch") != gen {
		t.Fatalf("holder append at epoch == generation = %d body %q Producer-Epoch %q, want 200 at %s", rec.Code, rec.Body.String(), rec.Header().Get("Producer-Epoch"), gen)
	}
	if rec = do(s.h, http.MethodPost, "/events/a", holder(gen, "0"), []byte(`{"turn":1}`)); rec.Code != http.StatusNoContent {
		t.Fatalf("holder retry = %d, want 204", rec.Code)
	}
	before := tailOf(t, s.h, "/events/a")

	rec = do(s.h, http.MethodPost, "/events/a", holder(strconv.FormatInt(cr.Generation+1, 10), "0"), []byte(`{"turn":2}`))
	if rec.Code != http.StatusConflict {
		t.Fatalf("holder append at epoch != generation = %d body %q, want 409", rec.Code, rec.Body.String())
	}
	eb := decodeEnvelope(t, rec)
	if eb.Error.Code != webhook.ErrCodeFenced || eb.Error.Reason != "epoch" || eb.Error.Generation != cr.Generation || eb.Error.CurrentHolder != "worker-A" {
		t.Fatalf("envelope = %+v, want FENCED/epoch at generation %d held by worker-A", eb.Error, cr.Generation)
	}
	if got := rec.Header().Get("Producer-Epoch"); got != gen {
		t.Errorf("Producer-Epoch = %q, want %s", got, gen)
	}
	if exp, rcv := rec.Header().Get("Producer-Expected-Seq"), rec.Header().Get("Producer-Received-Seq"); exp != "0" || rcv != "0" {
		t.Errorf("terminal pair = (%q, %q), want (0, 0)", exp, rcv)
	}
	if v := rec.Header().Get("Stream-Next-Offset"); v != "" {
		t.Errorf("Stream-Next-Offset = %q on a fenced 409, want absent", v)
	}
	if after := tailOf(t, s.h, "/events/a"); !after.Equal(before) {
		t.Fatalf("epoch-refused append mutated stream: tail %s -> %s", before, after)
	}

	rec = do(s.h, http.MethodPost, "/events/a", map[string]string{"Content-Type": "application/json", WriteTokenHeader: cr.WriteToken}, []byte(`{"turn":3}`))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "fenced write requires Producer-Id, Producer-Epoch, and Producer-Seq") {
		t.Fatalf("holder append without producer headers = %d body %q, want 400", rec.Code, rec.Body.String())
	}
	if after := tailOf(t, s.h, "/events/a"); !after.Equal(before) {
		t.Fatalf("producer-less fenced append mutated stream: tail %s -> %s", before, after)
	}
}

// TestHandleAppendBoundProducerFencedInSlot pins rule 5 of the rung through
// the handler on Redis (WF-16/WF-18): after the holder's fenced write binds
// its producer id, a service principal's open-class write naming that id is
// 409 bound with the bound generation as Producer-Epoch and the terminal pair
// — even at a higher epoch, the auto-claim shape — while the same principal
// establishing an unbound producer id on the fenced stream is accepted.
func TestHandleAppendBoundProducerFencedInSlot(t *testing.T) {
	s := newRedisFenceStack(t)
	s.createFenced(t, "/events/a")
	cr := claimForWriteFence(t, s.rt, s.subStore)
	gen := strconv.FormatInt(cr.Generation, 10)
	service := map[string]string{"Content-Type": "application/json", "Authorization": "Bearer " + tb4SvcBearer}
	open := func(id, epoch string) map[string]string {
		h := map[string]string{"Producer-Id": id, "Producer-Epoch": epoch, "Producer-Seq": "0"}
		for k, v := range service {
			h[k] = v
		}
		return h
	}

	rec := do(s.h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type": "application/json", WriteTokenHeader: cr.WriteToken,
		"Producer-Id": "entity-events/a", "Producer-Epoch": gen, "Producer-Seq": "0",
	}, []byte(`{"turn":1}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("holder append = %d body %q, want 200", rec.Code, rec.Body.String())
	}
	if rec = do(s.h, http.MethodPost, "/events/a", open("wake-reg-7", "0"), []byte(`{"cmd":"inbox"}`)); rec.Code != http.StatusOK {
		t.Fatalf("open-class append with an unbound producer = %d body %q, want 200", rec.Code, rec.Body.String())
	}
	before := tailOf(t, s.h, "/events/a")

	rec = do(s.h, http.MethodPost, "/events/a", open("entity-events/a", strconv.FormatInt(cr.Generation+1, 10)), []byte(`{"zombie":true}`))
	if rec.Code != http.StatusConflict {
		t.Fatalf("open-class append with the bound producer = %d body %q, want 409", rec.Code, rec.Body.String())
	}
	eb := decodeEnvelope(t, rec)
	if eb.Error.Code != webhook.ErrCodeFenced || eb.Error.Reason != "bound" || eb.Error.Generation != cr.Generation || eb.Error.CurrentHolder != "" {
		t.Fatalf("envelope = %+v, want FENCED/bound at generation %d with no holder", eb.Error, cr.Generation)
	}
	if got := rec.Header().Get("Producer-Epoch"); got != gen {
		t.Errorf("Producer-Epoch = %q, want %s", got, gen)
	}
	if exp, rcv := rec.Header().Get("Producer-Expected-Seq"), rec.Header().Get("Producer-Received-Seq"); exp != "0" || rcv != "0" {
		t.Errorf("terminal pair = (%q, %q), want (0, 0)", exp, rcv)
	}
	if after := tailOf(t, s.h, "/events/a"); !after.Equal(before) {
		t.Fatalf("bound-refused append mutated stream: tail %s -> %s", before, after)
	}
}

// TestHandleCreateWriteFenceEchoesOnRedis pins WF-01/WF-02 on the Redis store:
// Write-Fence: true is config-matched by create.lua's probe and echoed on
// PUT 201/200 and HEAD 200 (with no sealed headers before any seal), and a
// stream that never opted in carries none of the three.
func TestHandleCreateWriteFenceEchoesOnRedis(t *testing.T) {
	s := newRedisFenceStack(t)
	s.h.AuthMode = auth.ModeInsecure
	fenced := map[string]string{"Content-Type": "application/json", "Write-Fence": "true"}
	plain := map[string]string{"Content-Type": "application/json"}

	if rec := do(s.h, http.MethodPut, "/events/a", fenced, nil); rec.Code != http.StatusCreated || rec.Header().Get("Write-Fence") != "true" {
		t.Fatalf("fenced create = %d Write-Fence %q, want 201 true", rec.Code, rec.Header().Get("Write-Fence"))
	}
	if rec := do(s.h, http.MethodPut, "/events/a", fenced, nil); rec.Code != http.StatusOK || rec.Header().Get("Write-Fence") != "true" {
		t.Fatalf("fenced re-create = %d Write-Fence %q, want 200 true", rec.Code, rec.Header().Get("Write-Fence"))
	}
	if rec := do(s.h, http.MethodPut, "/events/a", plain, nil); rec.Code != http.StatusConflict {
		t.Fatalf("re-create without Write-Fence = %d body %q, want 409", rec.Code, rec.Body.String())
	}
	if rec := do(s.h, http.MethodPut, "/events/plain", plain, nil); rec.Code != http.StatusCreated || rec.Header().Get("Write-Fence") != "" {
		t.Fatalf("plain create = %d Write-Fence %q, want 201 and no echo", rec.Code, rec.Header().Get("Write-Fence"))
	}
	for path, want := range map[string]string{"/events/a": "true", "/events/plain": ""} {
		rec := do(s.h, http.MethodHead, path, nil, nil)
		if rec.Code != http.StatusOK || rec.Header().Get("Write-Fence") != want {
			t.Fatalf("HEAD %s = %d Write-Fence %q, want 200 %q", path, rec.Code, rec.Header().Get("Write-Fence"), want)
		}
		if g, o := rec.Header().Get("Write-Fence-Sealed-Generation"), rec.Header().Get("Write-Fence-Sealed-Offset"); g != "" || o != "" {
			t.Errorf("HEAD %s sealed headers = (%q, %q) before any seal, want absent", path, g, o)
		}
	}
}

func TestHandleAppendReleaseRaceIsFencedInsideRedisCommit(t *testing.T) {
	s := newRedisFenceStack(t)
	paused := &pausedAppendStore{
		Store:   s.data,
		entered: make(chan struct{}),
		resume:  make(chan struct{}),
	}
	h, rt, subStore := s.h, s.rt, s.subStore
	h.Store = paused
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
	if body.Error.Code != webhook.ErrCodeFenced || body.Error.Reason != "marker" {
		t.Fatalf("error = %+v, want %s from the in-slot marker check", body.Error, webhook.ErrCodeFenced)
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
