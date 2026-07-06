package chronicle

import (
	"net/http"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// The TB4 acceptance suite (issue #126): trusted-backend service principals
// end to end through the real handler. A verified service request is served
// pre-authorized; everything else falls through to the TB1 claim-token path.

const (
	tb4AgentsID  = "spiffe://cluster.local/ns/electric/sa/agents-server"
	tb4OtherID   = "spiffe://cluster.local/ns/other/sa/other"
	tb4XFCCHdr   = "X-Forwarded-Client-Cert"
	tb4SvcBearer = "svc-bearer-token-1"
)

// serviceHandler is an enforce-mode handler trusting tb4SvcBearer and
// tb4AgentsID, with the TB1 claim-token authorizer wired under key.
func serviceHandler(t *testing.T, key []byte) (*Handler, *hookRecorder) {
	t.Helper()
	h, rec := enforcedHandler(t, key)
	creds, err := auth.ParseServiceBearerConfig("agents-server:" + tb4SvcBearer)
	if err != nil {
		t.Fatal(err)
	}
	h.ServiceAuth = &ServiceAuth{
		Credentials:      creds,
		TrustedSPIFFEIDs: []string{tb4AgentsID},
	}
	return h, rec
}

func TestServiceBearerAppendServedPreAuthorized(t *testing.T) {
	key := testAuthKey(t)
	h, hooks := serviceHandler(t, key)
	mustCreate(t, h, "/events/a", "application/json", nil)

	// No claim token at all — the service credential alone authorizes.
	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + tb4SvcBearer,
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("service append = %d, body %q; want 204", rec.Code, rec.Body.String())
	}
	if hooks.appendCount() != 1 {
		t.Fatalf("append hook fired %d times, want 1", hooks.appendCount())
	}
}

// TestServiceBearerClaimTokenPassthrough pins the Q4 decision: in
// trusted-backend mode chronicle does not validate electric-claim-token — a
// junk claim token riding alongside a valid service credential must not deny.
func TestServiceBearerClaimTokenPassthrough(t *testing.T) {
	key := testAuthKey(t)
	h, _ := serviceHandler(t, key)
	mustCreate(t, h, "/events/a", "application/json", nil)

	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type":   "application/json",
		"Authorization":  "Bearer " + tb4SvcBearer,
		ClaimTokenHeader: "junk-upstream-validated-this-not-us",
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("service append with junk claim token = %d, want 204 (passthrough)", rec.Code)
	}
}

func TestUnauthenticatedPeerRejectedInBackendMode(t *testing.T) {
	key := testAuthKey(t)
	h, hooks := serviceHandler(t, key)
	mustCreate(t, h, "/events/a", "application/json", nil)
	before := tailOf(t, h, "/events/a")

	cases := map[string]map[string]string{
		"no credential":    {"Content-Type": "application/json"},
		"junk bearer":      {"Content-Type": "application/json", "Authorization": "Bearer not-the-service"},
		"near-miss bearer": {"Content-Type": "application/json", "Authorization": "Bearer " + tb4SvcBearer + "x"},
	}
	for name, headers := range cases {
		rec := do(h, http.MethodPost, "/events/a", headers, []byte(`{"n":1}`))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status = %d, want 401", name, rec.Code)
		}
		eb := decodeEnvelope(t, rec)
		if eb.Error.Code != "UNAUTHENTICATED" {
			t.Errorf("%s: code = %q", name, eb.Error.Code)
		}
	}
	if after := tailOf(t, h, "/events/a"); !after.Equal(before) {
		t.Fatal("store mutated on deny")
	}
	if hooks.appendCount() != 0 {
		t.Fatal("append hook fired on deny")
	}
}

// TestServiceBearerDoesNotShadowClaimTokens: a non-service Bearer still works
// as the TB1 write-token fallback when it validates as one.
func TestServiceBearerDoesNotShadowClaimTokens(t *testing.T) {
	key := testAuthKey(t)
	h, _ := serviceHandler(t, key)
	mustCreate(t, h, "/events/a", "application/json", nil)
	tok := mintWriteToken(t, key, "events/a")

	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + tok,
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("write-token Bearer fallback = %d, want 204", rec.Code)
	}
}

func TestXFCCServicePrincipal(t *testing.T) {
	key := testAuthKey(t)
	h, _ := serviceHandler(t, key)
	mustCreate(t, h, "/events/a", "application/json", nil)

	// Allowlisted mesh peer, no other credential → served.
	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type": "application/json",
		tb4XFCCHdr:     `By=spiffe://cluster.local/ns/chronicle/sa/chronicle;Hash=abcd;URI=` + tb4AgentsID,
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("allowlisted XFCC append = %d, want 204", rec.Code)
	}

	// Untrusted mesh identity → 401.
	rec = do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type": "application/json",
		tb4XFCCHdr:     `URI=` + tb4OtherID,
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("untrusted XFCC append = %d, want 401", rec.Code)
	}

	// The trusted id planted in an earlier element (forwarded hearsay) must
	// not authenticate — only the last element is sidecar-attested.
	rec = do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type": "application/json",
		tb4XFCCHdr:     `Hash=aa;URI=` + tb4AgentsID + `,Hash=bb;URI=` + tb4OtherID,
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("hearsay-element XFCC append = %d, want 401", rec.Code)
	}
}

// TestXFCCIgnoredWhenNotConfigured: without an explicit SPIFFE allowlist the
// header is inert — an operator who has not asserted a sanitizing sidecar
// gets no mesh trust path.
func TestXFCCIgnoredWhenNotConfigured(t *testing.T) {
	key := testAuthKey(t)
	h, _ := enforcedHandler(t, key)
	creds, err := auth.ParseServiceBearerConfig("agents-server:" + tb4SvcBearer)
	if err != nil {
		t.Fatal(err)
	}
	h.ServiceAuth = &ServiceAuth{Credentials: creds} // no TrustedSPIFFEIDs
	mustCreate(t, h, "/events/a", "application/json", nil)

	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type": "application/json",
		tb4XFCCHdr:     `URI=` + tb4AgentsID,
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("XFCC without allowlist = %d, want 401", rec.Code)
	}
}

// TestServiceAuthWithoutAppendAuthorizer: a trusted-backend-only deployment
// (no subscription layer, so no claim-token authorizer) serves its service
// and denies everyone else — the service check runs before the
// no-authorizer fail-closed guard.
func TestServiceAuthWithoutAppendAuthorizer(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeEnforce
	creds, err := auth.ParseServiceBearerConfig(tb4SvcBearer)
	if err != nil {
		t.Fatal(err)
	}
	h.ServiceAuth = &ServiceAuth{Credentials: creds}
	mustCreate(t, h, "/events/a", "application/json", nil)

	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + tb4SvcBearer,
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("service append without AppendAuth = %d, want 204", rec.Code)
	}
	// The bare-token form names the default subject; the request still
	// authenticates identically.
	rec = do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type": "application/json",
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous append without AppendAuth = %d, want 401", rec.Code)
	}
}

// TestServiceAuthInsecureModeStaysTelemetry: the default mode never denies,
// service config present or not.
func TestServiceAuthInsecureModeStaysTelemetry(t *testing.T) {
	key := testAuthKey(t)
	h := testHandler(time.Second, time.Second)
	h.AppendAuth = webhook.NewWriteTokenAuthorizer(key)
	creds, err := auth.ParseServiceBearerConfig("agents-server:" + tb4SvcBearer)
	if err != nil {
		t.Fatal(err)
	}
	h.ServiceAuth = &ServiceAuth{Credentials: creds}
	mustCreate(t, h, "/events/a", "application/json", nil)

	rec := do(h, http.MethodPost, "/events/a",
		map[string]string{"Content-Type": "application/json"}, []byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("insecure-mode anonymous append = %d, want 204", rec.Code)
	}
}
