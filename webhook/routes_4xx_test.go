package webhook

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// doDS drives a reserved __ds request through the full Routes surface and asserts
// the path was claimed (every /__ds/ path is reserved). token, when non-empty, is
// sent as the Bearer credential.
func doDS(t *testing.T, rt *Routes, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	if !rt.HandleRequest(rec, req) {
		t.Fatalf("HandleRequest did not claim %s %s", method, path)
	}
	return rec
}

func errCodeOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var eb ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &eb); err != nil {
		t.Fatalf("decode error body: %v; raw=%q", err, rec.Body.String())
	}
	return eb.Error.Code
}

// setupClaim creates a claimable pull-wake subscription and performs an HTTP
// claim, returning the claim response (token/wake_id/generation) used to exercise
// the ack/callback/release control-plane paths.
func setupClaim(t *testing.T, rt *Routes, store *RedisStore, id string) ClaimResponse {
	t.Helper()
	now := time.Now()
	if _, err := store.CreateOrConfirm(id, pullWakeCfg(), nil, now); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := store.Link(id, "events/a", LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatalf("link: %v", err)
	}
	rec := doDS(t, rt, http.MethodPost, subsPrefix+id+"/claim", "", `{"worker":"w1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("claim status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	var cr ClaimResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &cr); err != nil {
		t.Fatalf("decode claim: %v", err)
	}
	if cr.Token == "" || cr.WakeID == "" {
		t.Fatalf("claim response missing token/wake_id: %+v", cr)
	}
	return cr
}

// TestAckMissingFieldsReturns400 asserts that a callback/ack body missing the
// required fenced fields is a 400 INVALID_REQUEST before the request reaches the
// fence — an absent field is a malformed request, not a stale one.
func TestAckMissingFieldsReturns400(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	cr := setupClaim(t, rt, store, "s1")

	cases := []struct {
		name string
		path string
		body string
	}{
		{"ack missing both", subsPrefix + "s1/ack", `{}`},
		{"ack missing generation", subsPrefix + "s1/ack", `{"wake_id":"` + cr.WakeID + `"}`},
		{"ack missing wake_id", subsPrefix + "s1/ack", `{"generation":1}`},
		{"ack empty body", subsPrefix + "s1/ack", ``},
		{"callback missing both", subsPrefix + "s1/callback", `{}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doDS(t, rt, http.MethodPost, tc.path, cr.Token, tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%q", rec.Code, rec.Body.String())
			}
			if code := errCodeOf(t, rec); code != ErrCodeInvalidRequest {
				t.Fatalf("code = %q, want %q", code, ErrCodeInvalidRequest)
			}
		})
	}
}

// TestReleaseMissingFieldsReturns400 mirrors the ack path for release.
func TestReleaseMissingFieldsReturns400(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	cr := setupClaim(t, rt, store, "s1")

	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/release", cr.Token, `{"generation":1}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%q", rec.Code, rec.Body.String())
	}
	if code := errCodeOf(t, rec); code != ErrCodeInvalidRequest {
		t.Fatalf("code = %q, want %q", code, ErrCodeInvalidRequest)
	}
}

// TestAckPresentButZeroReachesFence proves the 400 check keys off presence, not
// value: an explicit generation:0 / wake_id:"" is a well-formed request that
// fails the fence as 409 FENCED, not a 400.
func TestAckPresentButZeroReachesFence(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	cr := setupClaim(t, rt, store, "s1")

	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/ack", cr.Token, `{"generation":0,"wake_id":""}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%q", rec.Code, rec.Body.String())
	}
	if code := errCodeOf(t, rec); code != ErrCodeFenced {
		t.Fatalf("code = %q, want %q", code, ErrCodeFenced)
	}
}

// TestAckStaleFenceReturns409 keeps the classic behavior: present-but-wrong
// (generation, wake_id) is a genuine fence mismatch.
func TestAckStaleFenceReturns409(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	cr := setupClaim(t, rt, store, "s1")

	body := ackBody(cr.Generation+99, cr.WakeID, true)
	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/ack", cr.Token, body)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%q", rec.Code, rec.Body.String())
	}
	if code := errCodeOf(t, rec); code != ErrCodeFenced {
		t.Fatalf("code = %q, want %q", code, ErrCodeFenced)
	}
}

// TestAckGoneSubscriptionReturns410 asserts a callback/ack targeting a deleted
// subscription is a distinct 410 SUBSCRIPTION_GONE rather than a 409 FENCED.
func TestAckGoneSubscriptionReturns410(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	cr := setupClaim(t, rt, store, "s1")

	if err := store.Delete("s1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	body := ackBody(cr.Generation, cr.WakeID, true)
	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/ack", cr.Token, body)
	if rec.Code != http.StatusGone {
		t.Fatalf("status = %d, want 410; body=%q", rec.Code, rec.Body.String())
	}
	if code := errCodeOf(t, rec); code != ErrCodeSubscriptionGone {
		t.Fatalf("code = %q, want %q", code, ErrCodeSubscriptionGone)
	}
}

// TestReleaseGoneSubscriptionReturns410 mirrors the gone case for release.
func TestReleaseGoneSubscriptionReturns410(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	cr := setupClaim(t, rt, store, "s1")

	if err := store.Delete("s1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	body := ackBody(cr.Generation, cr.WakeID, false)
	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/release", cr.Token, body)
	if rec.Code != http.StatusGone {
		t.Fatalf("status = %d, want 410; body=%q", rec.Code, rec.Body.String())
	}
	if code := errCodeOf(t, rec); code != ErrCodeSubscriptionGone {
		t.Fatalf("code = %q, want %q", code, ErrCodeSubscriptionGone)
	}
}

// TestAckSuccessShapeUnchanged confirms a valid done-ack still returns 200 with
// the {ok, next_wake} body.
func TestAckSuccessShapeUnchanged(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	cr := setupClaim(t, rt, store, "s1")

	body := ackBody(cr.Generation, cr.WakeID, true)
	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/ack", cr.Token, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode ack response: %v", err)
	}
	if _, ok := got["ok"]; !ok {
		t.Fatalf("ack response missing ok: %v", got)
	}
	if _, ok := got["next_wake"]; !ok {
		t.Fatalf("ack response missing next_wake: %v", got)
	}
	if got["ok"] != true {
		t.Fatalf("ok = %v, want true", got["ok"])
	}
}

// TestReleaseSuccessShapeUnchanged confirms a valid release still returns 204.
func TestReleaseSuccessShapeUnchanged(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	cr := setupClaim(t, rt, store, "s1")

	body := ackBody(cr.Generation, cr.WakeID, false)
	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/release", cr.Token, body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%q", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("release body = %q, want empty", rec.Body.String())
	}
}

func ackBody(generation int64, wakeID string, done bool) string {
	b, _ := json.Marshal(map[string]any{
		"generation": generation,
		"wake_id":    wakeID,
		"done":       done,
	})
	return string(b)
}
