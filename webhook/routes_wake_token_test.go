package webhook

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// jwksResolver builds a KidResolver over a served JWKS document, the way a
// gateway verifies: keys selected strictly by kid.
func jwksResolver(t *testing.T, jwks JWKS) KidResolver {
	t.Helper()
	return func(kid string) (ed25519.PublicKey, bool) {
		for _, k := range jwks.Keys {
			if k.Kid == kid {
				raw, err := base64.RawURLEncoding.DecodeString(k.X)
				if err != nil {
					t.Fatalf("jwks x: %v", err)
				}
				return ed25519.PublicKey(raw), true
			}
		}
		return nil, false
	}
}

// TestClaimMintsWakeToken is the TB6a acceptance: a woken entity's claim
// response carries a wake_token that verifies against the served JWKS with
// the right sub/typ/exp under a kid separate from the webhook key — and a
// verifier trusting only the webhook key rejects it.
func TestClaimMintsWakeToken(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	// A 10s lease keeps the exp−iat assertion clear of unix-second truncation
	// (a 1s lease mints a 500ms token, which rounds to 0 or 1 depending on
	// sub-second alignment).
	now := time.Now()
	cfg := Config{Type: DispatchPullWake, Pattern: "events/*", WakeStream: "wake/pool", LeaseTTLMs: 10_000}
	if _, err := store.CreateOrConfirm("s1", cfg, nil, now); err != nil {
		t.Fatal(err)
	}
	if err := store.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatal(err)
	}
	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/claim", "", `{"worker":"w1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("claim = %d body %q", rec.Code, rec.Body.String())
	}
	var cr ClaimResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &cr); err != nil {
		t.Fatal(err)
	}

	if cr.WakeToken == "" {
		t.Fatal("claim response missing wake_token")
	}

	jwks, err := mgr.JWKS()
	if err != nil {
		t.Fatal(err)
	}
	claims, err := ValidateWakeToken(cr.WakeToken, jwksResolver(t, jwks), "", time.Now(), 5*time.Second)
	if err != nil {
		t.Fatalf("wake_token does not verify against the served JWKS: %v", err)
	}
	if claims.Sub != "events/a" {
		t.Fatalf("sub = %q, want the entity path events/a", claims.Sub)
	}
	if claims.Generation != cr.Generation || claims.WakeID != cr.WakeID {
		t.Fatalf("fence binding = (gen %d, wake %q), want (%d, %q)",
			claims.Generation, claims.WakeID, cr.Generation, cr.WakeID)
	}
	if claims.Iss != mgr.streamRootURL {
		t.Fatalf("iss = %q, want %q", claims.Iss, mgr.streamRootURL)
	}

	// exp strictly under one lease from mint: a 10s lease mints a 5s token.
	if life := claims.Exp - claims.Iat; life <= 0 || life >= 10 {
		t.Fatalf("token life %ds not strictly inside the 10s lease", life)
	}

	// Separate kid: the wake_token's kid is not the webhook signing kid, and a
	// verifier holding only the webhook key rejects the token.
	if mgr.activeWakeKey().Kid == mgr.activeSigningKey().Kid {
		t.Fatal("wake key kid equals webhook signing kid")
	}
	if _, err := ValidateWakeToken(cr.WakeToken, resolverFor(mgr.activeSigningKey()), "", time.Now(), 5*time.Second); err == nil {
		t.Fatal("webhook-key-only verifier accepted the wake_token")
	}

	// Both kids are published in the JWKS (kid-selective consumers).
	var haveSigning, haveWake bool
	for _, k := range jwks.Keys {
		haveSigning = haveSigning || k.Kid == mgr.activeSigningKey().Kid
		haveWake = haveWake || k.Kid == mgr.activeWakeKey().Kid
	}
	if !haveSigning || !haveWake {
		t.Fatalf("JWKS missing a family: signing=%v wake=%v", haveSigning, haveWake)
	}
}

// TestAckRefreshesWakeToken: a heartbeat (non-done) ack re-mints the
// wake_token; a done ack never does — the exact condition that keeps the
// conformance suite's deep-equaled {ok,next_wake} ack body unchanged (it only
// ever sends done acks).
func TestAckRefreshesWakeToken(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	cr := setupClaim(t, rt, store, "s1")

	// Heartbeat: done absent → refreshed wake_token rides the response.
	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/ack", cr.Token,
		`{"wake_id":"`+cr.WakeID+`","generation":`+jsonInt(cr.Generation)+`,"acks":[]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("heartbeat ack = %d body %q", rec.Code, rec.Body.String())
	}
	var hb AckResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &hb); err != nil {
		t.Fatal(err)
	}
	if hb.WakeToken == "" {
		t.Fatal("heartbeat ack must refresh the wake_token")
	}
	jwks, _ := mgr.JWKS()
	claims, err := ValidateWakeToken(hb.WakeToken, jwksResolver(t, jwks), "", time.Now(), 5*time.Second)
	if err != nil {
		t.Fatalf("refreshed wake_token invalid: %v", err)
	}
	if claims.Sub != "events/a" || claims.Generation != cr.Generation || claims.WakeID != cr.WakeID {
		t.Fatalf("refreshed claims = %+v", claims)
	}

	// Done: the wake is over — no wake_token, body shape {ok,next_wake} only.
	rec = doDS(t, rt, http.MethodPost, subsPrefix+"s1/ack", cr.Token,
		`{"wake_id":"`+cr.WakeID+`","generation":`+jsonInt(cr.Generation)+`,"acks":[],"done":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("done ack = %d body %q", rec.Code, rec.Body.String())
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if _, has := raw["wake_token"]; has {
		t.Fatal("done ack must not carry wake_token")
	}
}

// TestMultiLinkSubscriptionMintsNoWakeToken pins the honest single-entity
// rule: a subscription linked to more than one stream names no entity, so no
// wake_token is minted anywhere it would otherwise appear.
func TestMultiLinkSubscriptionMintsNoWakeToken(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	now := time.Now()
	if _, err := store.CreateOrConfirm("s2", pullWakeCfg(), nil, now); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"events/a", "events/b"} {
		if err := store.Link("s2", p, LinkGlob, "0000000000000000_0000000000000000"); err != nil {
			t.Fatal(err)
		}
	}
	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s2/claim", "", `{"worker":"w1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("claim = %d body %q", rec.Code, rec.Body.String())
	}
	var cr ClaimResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &cr); err != nil {
		t.Fatal(err)
	}
	if cr.WakeToken != "" {
		t.Fatal("multi-link subscription must mint no wake_token")
	}
	if cr.WriteToken == "" {
		t.Fatal("write token is per-claim, not per-entity — it must still mint")
	}
}
