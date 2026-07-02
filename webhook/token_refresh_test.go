package webhook

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// These are the issue #77 in-band token-refresh HTTP tests: they drive the real
// callback handler over a live-claimed pull-wake subscription and assert the
// response shape for the near-expiry, comfortably-valid, and expired token cases.
// They need Redis (via newTestManager -> newTestStore) and are skipped under
// -short; the pure refresh/expiry decisions are table-tested in crypto_test.go.

// claimForRefreshTest creates a pull-wake subscription and claims it, returning
// the routes, subscription id, and the claim's generation + wake id so a test can
// mint tokens of a chosen expiry and present them to the callback handler.
func claimForRefreshTest(t *testing.T) (*Routes, string, int64, string) {
	t.Helper()
	mgr, store, _ := newTestManager(t)
	const id = "s77"
	now := time.Now()
	if _, err := store.CreateOrConfirm(id, pullWakeCfg(), nil, now); err != nil {
		t.Fatalf("create sub: %v", err)
	}
	claim, err := store.Claim(id, "worker-77", "w_77", now, pullWakeCfg().LeaseTTLMs)
	if err != nil || !claim.Claimed {
		t.Fatalf("claim = %+v err=%v", claim, err)
	}
	return NewRoutes(mgr), id, claim.Generation, claim.WakeID
}

// postHeartbeat presents token on a heartbeat callback (done unset, no acks) for
// (gen, wakeID) and returns the recorder.
func postHeartbeat(t *testing.T, rt *Routes, id, token string, gen int64, wakeID string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(CallbackRequest{WakeID: wakeID, Generation: gen})
	r := httptest.NewRequest(http.MethodPost, "/__ds/subscriptions/"+id+"/callback", bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	rt.handleAckLike(w, r, id)
	return w
}

// TestCallbackRefreshesNearExpiryToken: a still-valid token within the refresh
// threshold of expiry comes back refreshed in the "token" field, and the returned
// token is valid with a later expiry than the one presented.
func TestCallbackRefreshesNearExpiryToken(t *testing.T) {
	rt, id, gen, wakeID := claimForRefreshTest(t)
	now := time.Now()

	// 60s of life left: inside the 300s threshold, so a successful callback refreshes.
	presented, err := GenerateToken(rt.mgr.tokenKey, id, gen, now, 60*time.Second, randReader)
	if err != nil {
		t.Fatal(err)
	}
	presentedExp := ValidateToken(rt.mgr.tokenKey, presented, id, now).Exp

	w := postHeartbeat(t, rt, id, presented, gen, wakeID)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp AckResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (%s)", err, w.Body.String())
	}
	if !resp.OK || resp.NextWake {
		t.Fatalf("unexpected ack fields: %+v", resp)
	}
	if resp.Token == "" {
		t.Fatal("near-expiry callback must return a refreshed token")
	}
	tv := ValidateToken(rt.mgr.tokenKey, resp.Token, id, now)
	if !tv.Valid {
		t.Fatalf("refreshed token does not validate: %+v", tv)
	}
	if tv.Exp <= presentedExp {
		t.Fatalf("refreshed token exp %d must be later than presented %d", tv.Exp, presentedExp)
	}
	if tv.Generation != gen {
		t.Fatalf("refreshed token generation = %d, want %d", tv.Generation, gen)
	}
}

// TestCallbackDoesNotRefreshComfortablyValidToken: a comfortably-valid token
// yields NO "token" field, and the response is byte-identical to the historical
// {ok,next_wake} body the conformance suite deep-equals.
func TestCallbackDoesNotRefreshComfortablyValidToken(t *testing.T) {
	rt, id, gen, wakeID := claimForRefreshTest(t)
	now := time.Now()

	// A full hour of life: well outside the 300s threshold, so no refresh.
	presented, err := GenerateToken(rt.mgr.tokenKey, id, gen, now, time.Hour, randReader)
	if err != nil {
		t.Fatal(err)
	}

	w := postHeartbeat(t, rt, id, presented, gen, wakeID)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	// Byte-for-byte: json.Encoder appends a trailing newline; omitempty drops "token".
	const want = "{\"ok\":true,\"next_wake\":false}\n"
	if got := w.Body.String(); got != want {
		t.Fatalf("no-refresh body must be byte-identical\n got: %q\nwant: %q", got, want)
	}
}

// TestCallbackExpiredTokenReturnsFreshToken: an expired (but ours and well-formed)
// token gets a distinct 401 TOKEN_EXPIRED whose body carries a freshly minted,
// valid token so the worker can retry immediately.
func TestCallbackExpiredTokenReturnsFreshToken(t *testing.T) {
	rt, id, gen, wakeID := claimForRefreshTest(t)
	now := time.Now()

	// Minted 10s in the past: already expired.
	presented, err := GenerateToken(rt.mgr.tokenKey, id, gen, now, -10*time.Second, randReader)
	if err != nil {
		t.Fatal(err)
	}

	w := postHeartbeat(t, rt, id, presented, gen, wakeID)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", w.Code, w.Body.String())
	}
	var body ErrorBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v (%s)", err, w.Body.String())
	}
	if body.Error.Code != ErrCodeTokenExpired {
		t.Fatalf("error code = %q, want %q", body.Error.Code, ErrCodeTokenExpired)
	}
	if body.Token == "" {
		t.Fatal("TOKEN_EXPIRED response must carry a freshly minted token")
	}
	if tv := ValidateToken(rt.mgr.tokenKey, body.Token, id, now); !tv.Valid || tv.Generation != gen {
		t.Fatalf("fresh token invalid or wrong generation: %+v", tv)
	}
}

// TestCallbackForeignTokenStaysInvalid: a malformed/foreign token keeps the bare
// TOKEN_INVALID 401 with no token in the body (unchanged behavior).
func TestCallbackForeignTokenStaysInvalid(t *testing.T) {
	rt, id, gen, wakeID := claimForRefreshTest(t)

	w := postHeartbeat(t, rt, id, "not-a-real-token", gen, wakeID)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", w.Code, w.Body.String())
	}
	const want = "{\"error\":{\"code\":\"TOKEN_INVALID\"}}\n"
	if got := w.Body.String(); got != want {
		t.Fatalf("foreign-token body must be unchanged\n got: %q\nwant: %q", got, want)
	}
}
