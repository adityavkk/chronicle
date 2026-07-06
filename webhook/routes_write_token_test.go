package webhook

import (
	"net/http"
	"testing"
	"time"
)

// TestClaimMintsWriteToken drives a real HTTP claim and asserts the response
// carries a claim-scoped write token that (a) the Manager's own
// WriteAuthorizer accepts for exactly the claimed stream, (b) is rejected for
// any other path, and (c) does not double as a callback/ack credential. This
// is the mint half of the issue-#126 TB1 slice, sharing the Manager's
// persisted token key with the append gate.
func TestClaimMintsWriteToken(t *testing.T) {
	mgr, store, _ := newTestManager(t)
	rt := NewRoutes(mgr)
	cr := setupClaim(t, rt, store, "s1")

	if cr.WriteToken == "" {
		t.Fatal("claim response missing write_token")
	}

	az := mgr.WriteAuthorizer()
	now := time.Now()

	if d := az.AuthorizeAppend(cr.WriteToken, mustPath(t, "events/a"), now); !d.Allowed() {
		t.Fatalf("write token must authorize the claimed stream, got deny: %s", d.Detail())
	}
	d := az.AuthorizeAppend(cr.WriteToken, mustPath(t, "events/b"), now)
	if d.Allowed() {
		t.Fatal("write token must not authorize an unclaimed stream")
	}

	// Validation binds the token to the claim that minted it.
	v := ValidateWriteToken(mgr.tokenKey, cr.WriteToken, mustPath(t, "events/a"), now)
	if v.Status != WriteTokenValid || v.SubID != "s1" || v.Generation != cr.Generation {
		t.Fatalf("write token attribution = %+v, want s1 gen %d", v, cr.Generation)
	}

	// The write token is not a callback credential: presenting it on ack is a
	// 401 TOKEN_INVALID (PROTOCOL §12.9's per-purpose token validation).
	rec := doDS(t, rt, http.MethodPost, subsPrefix+"s1/ack", cr.WriteToken,
		`{"wake_id":"`+cr.WakeID+`","generation":1}`)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ack with write token = %d, want 401", rec.Code)
	}
	if code := errCodeOf(t, rec); code != ErrCodeTokenInvalid {
		t.Fatalf("ack with write token code = %s, want %s", code, ErrCodeTokenInvalid)
	}
}
