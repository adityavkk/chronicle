package chronicle

import (
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// The TB1 acceptance suite (issue #126): the append gate end to end through
// the real handler with the in-memory store. Every deny path asserts the
// store is unmutated and no subscription wake fired — a denial must return
// before any store access.

func testAuthKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return key
}

// enforcedHandler is a handler in ModeEnforce whose append gate validates
// against key, with a hook recorder attached.
func enforcedHandler(t *testing.T, key []byte) (*Handler, *hookRecorder) {
	t.Helper()
	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeEnforce
	h.AppendAuth = webhook.NewWriteTokenAuthorizer(key)
	rec := &hookRecorder{}
	h.SubHooks = rec
	return h, rec
}

// hookRecorder records lifecycle hook fires so deny paths can assert no wake.
type hookRecorder struct {
	mu      sync.Mutex
	appends []string
}

func (r *hookRecorder) OnStreamCreated(string) {}
func (r *hookRecorder) OnStreamDeleted(string) {}
func (r *hookRecorder) OnStreamAppend(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.appends = append(r.appends, path)
}

func (r *hookRecorder) appendCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.appends)
}

// mintWriteToken mints a token scoped to the given wire paths (no leading
// slash), valid for an hour from now.
func mintWriteToken(t *testing.T, key []byte, paths ...string) string {
	t.Helper()
	scope := make([]auth.StreamPath, len(paths))
	for i, p := range paths {
		sp, err := auth.NormalizeStreamPath(p)
		if err != nil {
			t.Fatalf("normalize %q: %v", p, err)
		}
		scope[i] = sp
	}
	tok, err := webhook.GenerateWriteToken(key, "sub-1", 1, scope, time.Now(), time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func tailOf(t *testing.T, h *Handler, path string) store.Offset {
	t.Helper()
	meta, err := h.Store.Get(path)
	if err != nil {
		t.Fatalf("get %s: %v", path, err)
	}
	return meta.CurrentOffset
}

func decodeEnvelope(t *testing.T, rec *httptest.ResponseRecorder) webhook.ErrorBody {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("deny Content-Type = %q, want application/json", ct)
	}
	var eb webhook.ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &eb); err != nil {
		t.Fatalf("decode error envelope: %v; raw=%q", err, rec.Body.String())
	}
	return eb
}

func TestAppendAllowedWithClaimToken(t *testing.T) {
	key := testAuthKey(t)
	h, hooks := enforcedHandler(t, key)
	mustCreate(t, h, "/events/a", "application/json", nil)
	tok := mintWriteToken(t, key, "events/a")

	rec := do(h, http.MethodPost, "/events/a",
		map[string]string{"Content-Type": "application/json", ClaimTokenHeader: tok},
		[]byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("append = %d, body %q; want 204", rec.Code, rec.Body.String())
	}
	if got := tailOf(t, h, "/events/a"); got.ByteOffset == 0 {
		t.Fatal("write did not land")
	}
	if hooks.appendCount() != 1 {
		t.Fatalf("append hook fired %d times, want 1", hooks.appendCount())
	}
}

func TestAppendAllowedWithBearerFallback(t *testing.T) {
	key := testAuthKey(t)
	h, _ := enforcedHandler(t, key)
	mustCreate(t, h, "/events/a", "application/json", nil)
	tok := mintWriteToken(t, key, "events/a")

	rec := do(h, http.MethodPost, "/events/a",
		map[string]string{"Content-Type": "application/json", "Authorization": "Bearer " + tok},
		[]byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("append with Bearer fallback = %d, want 204", rec.Code)
	}
}

// TestAppendClaimHeaderPreferredOverBearer: when both credentials are present
// the electric-claim-token header wins (Electric's extraction order); a stale
// Bearer alongside a good claim token must not deny.
func TestAppendClaimHeaderPreferredOverBearer(t *testing.T) {
	key := testAuthKey(t)
	h, _ := enforcedHandler(t, key)
	mustCreate(t, h, "/events/a", "application/json", nil)
	tok := mintWriteToken(t, key, "events/a")

	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type":   "application/json",
		ClaimTokenHeader: tok,
		"Authorization":  "Bearer garbage",
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("append = %d, want 204 (claim header must win)", rec.Code)
	}
}

func TestAppendDeniedFailClosed(t *testing.T) {
	key := testAuthKey(t)

	expired, err := webhook.GenerateWriteToken(key, "sub-1", 1,
		[]auth.StreamPath{mustStreamPath(t, "events/a")},
		time.Now().Add(-2*time.Hour), time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name       string
		headers    map[string]string
		wantStatus int
		wantCode   string
	}{
		{"no token", nil, http.StatusUnauthorized, "UNAUTHENTICATED"},
		{"garbage token", map[string]string{ClaimTokenHeader: "garbage"}, http.StatusUnauthorized, "UNAUTHENTICATED"},
		{"garbage bearer", map[string]string{"Authorization": "Bearer nope"}, http.StatusUnauthorized, "UNAUTHENTICATED"},
		{"expired token", map[string]string{ClaimTokenHeader: expired}, http.StatusUnauthorized, "UNAUTHENTICATED"},
		{"wrong-path token", map[string]string{ClaimTokenHeader: ""}, http.StatusForbidden, "FORBIDDEN"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, hooks := enforcedHandler(t, key)
			mustCreate(t, h, "/events/a", "application/json", nil)
			before := tailOf(t, h, "/events/a")

			headers := map[string]string{"Content-Type": "application/json"}
			for k, v := range c.headers {
				headers[k] = v
			}
			if c.name == "wrong-path token" {
				headers[ClaimTokenHeader] = mintWriteToken(t, key, "events/b")
			}

			rec := do(h, http.MethodPost, "/events/a", headers, []byte(`{"n":1}`))
			if rec.Code != c.wantStatus {
				t.Fatalf("status = %d, body %q; want %d", rec.Code, rec.Body.String(), c.wantStatus)
			}
			eb := decodeEnvelope(t, rec)
			if eb.Error.Code != c.wantCode {
				t.Errorf("code = %q, want %q", eb.Error.Code, c.wantCode)
			}
			if c.wantStatus == http.StatusUnauthorized {
				if rec.Header().Get("WWW-Authenticate") == "" {
					t.Error("401 must carry WWW-Authenticate (RFC 6750)")
				}
			}
			assertSecurityHeaders(t, rec)

			if after := tailOf(t, h, "/events/a"); !after.Equal(before) {
				t.Fatalf("store mutated on deny: tail %s -> %s", before, after)
			}
			if hooks.appendCount() != 0 {
				t.Fatalf("append hook fired on deny")
			}
		})
	}
}

// TestAppendDenyDoesNotLeakExistence: an unauthenticated append to a stream
// that does not exist is the same 401 as to one that does — the gate runs
// before the store lookup (§12.2).
func TestAppendDenyDoesNotLeakExistence(t *testing.T) {
	key := testAuthKey(t)
	h, _ := enforcedHandler(t, key)
	mustCreate(t, h, "/events/exists", "application/json", nil)

	for _, path := range []string{"/events/exists", "/events/missing"} {
		rec := do(h, http.MethodPost, path,
			map[string]string{"Content-Type": "application/json"}, []byte(`{"n":1}`))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("append %s = %d, want identical 401", path, rec.Code)
		}
	}
}

// TestCloseOnlyAppendGated: a close-only POST (empty body, Stream-Closed:
// true) is a mutation and takes the same gate.
func TestCloseOnlyAppendGated(t *testing.T) {
	key := testAuthKey(t)
	h, _ := enforcedHandler(t, key)
	mustCreate(t, h, "/events/a", "application/json", nil)

	rec := do(h, http.MethodPost, "/events/a",
		map[string]string{"Stream-Closed": "true"}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("close-only without token = %d, want 401", rec.Code)
	}
	if meta, _ := h.Store.Get("/events/a"); meta.Closed {
		t.Fatal("stream closed despite denied close-only append")
	}

	tok := mintWriteToken(t, key, "events/a")
	rec = do(h, http.MethodPost, "/events/a",
		map[string]string{"Stream-Closed": "true", ClaimTokenHeader: tok}, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("close-only with token = %d, want 204", rec.Code)
	}
	if meta, _ := h.Store.Get("/events/a"); !meta.Closed {
		t.Fatal("stream not closed after authorized close-only append")
	}
}

// TestAppendProducerHeadersComposeWithGate: the producer protocol still works
// behind the gate (200 for a new producer append, per the base protocol).
func TestAppendProducerHeadersComposeWithGate(t *testing.T) {
	key := testAuthKey(t)
	h, _ := enforcedHandler(t, key)
	mustCreate(t, h, "/events/a", "application/json", nil)
	tok := mintWriteToken(t, key, "events/a")

	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type":   "application/json",
		ClaimTokenHeader: tok,
		"Producer-Id":    "p1",
		"Producer-Epoch": "1",
		"Producer-Seq":   "0",
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("producer append = %d, body %q; want 200", rec.Code, rec.Body.String())
	}
}

// TestAppendInsecureModeIsTelemetryOnly: the default mode never denies — with
// an authorizer wired and no token, the append still lands (the decision is
// logged, not enforced), so a base-protocol client is never broken.
func TestAppendInsecureModeIsTelemetryOnly(t *testing.T) {
	key := testAuthKey(t)
	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeInsecure
	h.AppendAuth = webhook.NewWriteTokenAuthorizer(key)
	mustCreate(t, h, "/events/a", "application/json", nil)

	rec := do(h, http.MethodPost, "/events/a",
		map[string]string{"Content-Type": "application/json"}, []byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("insecure-mode append without token = %d, want 204", rec.Code)
	}
}

// TestAppendZeroValueHandlerUnchanged: a Handler with no auth configuration
// at all (the base-protocol deployment) behaves exactly as before.
func TestAppendZeroValueHandlerUnchanged(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	mustCreate(t, h, "/events/a", "application/json", nil)
	mustAppend(t, h, "/events/a", "application/json", []byte(`{"n":1}`))
}

// TestAppendEnforceWithoutAuthorizerFailsClosed: enforcement with nothing to
// verify against denies everything rather than allowing anything.
func TestAppendEnforceWithoutAuthorizerFailsClosed(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeEnforce
	mustCreate(t, h, "/events/a", "application/json", nil)

	rec := do(h, http.MethodPost, "/events/a",
		map[string]string{"Content-Type": "application/json"}, []byte(`{"n":1}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("enforce without authorizer = %d, want 401 (fail closed)", rec.Code)
	}
}

// TestAppendTraversalPathDenied: a path with dot segments can never be
// authorized — normalization rejects it before any scope comparison (§12.2).
func TestAppendTraversalPathDenied(t *testing.T) {
	key := testAuthKey(t)
	h, _ := enforcedHandler(t, key)
	tok := mintWriteToken(t, key, "events/a")

	req := httptest.NewRequest(http.MethodPost, "/ignored", nil)
	req.URL.Path = "/events/../events/a" // bypass client-side path cleaning
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(ClaimTokenHeader, tok)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("traversal path = %d, want 403", rec.Code)
	}
}

// TestReadAndCreateUngatedInTB1 pins TB1's scope: only appends are gated; a
// base client can still create and read while later bullets land read/create
// enforcement.
func TestReadAndCreateUngatedInTB1(t *testing.T) {
	key := testAuthKey(t)
	h, _ := enforcedHandler(t, key)
	mustCreate(t, h, "/events/a", "application/json", nil)

	rec := do(h, http.MethodGet, "/events/a", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("read = %d, want 200 (read gating is TB5)", rec.Code)
	}
}

func mustStreamPath(t *testing.T, s string) auth.StreamPath {
	t.Helper()
	p, err := auth.NormalizeStreamPath(s)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
