package webhook

import (
	"crypto/rand"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// newAuthTestManager mirrors newTestManager with an explicit AuthMode, for
// exercising the control-plane gates end to end against Redis.
func newAuthTestManager(t *testing.T, mode auth.Mode) (*Manager, *RedisStore, *fakeStreams) {
	t.Helper()
	store, _ := newTestStore(t) // skips when Redis is unavailable / -short
	fs := &fakeStreams{tails: map[string]string{}}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", AuthMode: mode})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mgr, store, fs
}

// claimableSub creates a pull-wake subscription linked to events/a, ready to
// claim.
func claimableSub(t *testing.T, store *RedisStore, id string) {
	t.Helper()
	now := time.Now()
	if _, err := store.CreateOrConfirm(id, pullWakeCfg(), nil, now); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := store.Link(id, "events/a", LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatalf("link: %v", err)
	}
}

// TestClaimEnforceRequiresCaller is the TB2 acceptance matrix: in enforce
// mode every unauthenticated or unverifiable path is a 401 UNAUTHENTICATED
// that neither claims the lease nor mints any token, and a valid caller
// token yields the full claim including the write capability.
func TestClaimEnforceRequiresCaller(t *testing.T) {
	mgr, store, _ := newAuthTestManager(t, auth.ModeEnforce)
	rt := NewRoutes(mgr)
	claimableSub(t, store, "s1")
	now := time.Now()

	// Credentials for the deny matrix.
	expired, err := GenerateCallerToken(mgr.activeSigningKey(), mgr.streamRootURL, "u:1", nil,
		now.Add(-2*time.Hour), time.Minute, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongTyp, err := SignCompactJWS(mgr.activeSigningKey(), "application/wake+jwt", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	cbToken, err := GenerateToken(mgr.tokenKey, "s1", 1, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	denies := map[string]string{
		"no credential":       "",
		"garbage bearer":      "garbage",
		"expired caller":      expired,
		"wrong typ":           wrongTyp,
		"hmac callback token": cbToken,
	}
	for name, cred := range denies {
		t.Run(name, func(t *testing.T) {
			rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/claim", cred, `{"worker":"w1"}`)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401; body=%q", rec.Code, rec.Body.String())
			}
			if code := errCodeOf(t, rec); code != ErrCodeUnauthenticated {
				t.Fatalf("code = %s, want %s", code, ErrCodeUnauthenticated)
			}
			// The deny happened before any store access: nothing was claimed.
			sub, ok, err := store.Get("s1")
			if err != nil || !ok {
				t.Fatalf("get: %v %v", ok, err)
			}
			if sub.Phase != PhaseIdle || sub.Generation != 0 {
				t.Fatalf("denied claim mutated state: phase=%v gen=%d", sub.Phase, sub.Generation)
			}
		})
	}

	// The allow path: a valid caller token claims, and the returned write
	// token authorizes the TB1 append gate for exactly the claimed stream.
	caller, err := GenerateCallerToken(mgr.activeSigningKey(), mgr.streamRootURL, "u:1", []string{"events"},
		now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/claim", caller, `{"worker":"w1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("authenticated claim = %d, body %q", rec.Code, rec.Body.String())
	}
	var cr ClaimResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &cr); err != nil {
		t.Fatal(err)
	}
	if cr.Token == "" || cr.WriteToken == "" {
		t.Fatalf("claim response missing tokens: %+v", cr)
	}
	az := mgr.WriteAuthorizer()
	if d := az.AuthorizeAppend(cr.WriteToken, mustPath(t, "events/a"), now); !d.Allowed() {
		t.Fatalf("write token must authorize the claimed stream: %s", d.Detail())
	}
	if d := az.AuthorizeAppend(cr.WriteToken, mustPath(t, "events/b"), now); d.Allowed() {
		t.Fatal("write token must not authorize an unclaimed stream")
	}

	// The caller token is not an ack credential: the ack/callback routes
	// still validate the per-wake HMAC token only.
	ackRec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/ack", caller,
		`{"wake_id":"`+cr.WakeID+`","generation":`+jsonInt(cr.Generation)+`}`)
	if ackRec.Code != http.StatusUnauthorized {
		t.Fatalf("ack with caller token = %d, want 401", ackRec.Code)
	}
	if code := errCodeOf(t, ackRec); code != ErrCodeTokenInvalid {
		t.Fatalf("ack with caller token code = %s, want %s", code, ErrCodeTokenInvalid)
	}
	// And the real HMAC token from the claim still acks — TB2 does not touch
	// callback/ack/release semantics.
	okRec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/ack", cr.Token,
		`{"wake_id":"`+cr.WakeID+`","generation":`+jsonInt(cr.Generation)+`}`)
	if okRec.Code != http.StatusOK {
		t.Fatalf("ack with claim token = %d, body %q", okRec.Code, okRec.Body.String())
	}
}

// TestClaimInsecureDefaultUnchanged pins the telemetry default: with no
// AuthMode configured a credential-less claim still succeeds with the exact
// response key set the base contract exposes — a deploy sync can never break
// a base client.
func TestClaimInsecureDefaultUnchanged(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	claimableSub(t, store, "s1")

	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/claim", "", `{"worker":"w1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("insecure claim = %d, body %q", rec.Code, rec.Body.String())
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	// wake_token joined the set with TB6a: the single-entity fixture mints the
	// entity-identity assertion in both modes (an additive §11.2 field; the
	// conformance suite field-reads claim responses, so the pin here is that
	// nothing is REMOVED or renamed in the insecure default).
	want := []string{"wake_id", "generation", "token", "write_token", "wake_token", "streams", "lease_ttl_ms"}
	if len(got) != len(want) {
		t.Fatalf("claim response keys = %d, want %d: %v", len(got), len(want), got)
	}
	for _, k := range want {
		if _, ok := got[k]; !ok {
			t.Fatalf("claim response missing %q", k)
		}
	}
}
