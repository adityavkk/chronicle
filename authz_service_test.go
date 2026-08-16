package chronicle

import (
	"net/http"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// Service-principal acceptance suite through the real handler. Every verified
// identity is evaluated against an explicit action and namespace policy before
// any fallback to the claim-token path.

const (
	tb4AgentsID  = "spiffe://cluster.local/ns/electric/sa/agents-server"
	tb4OtherID   = "spiffe://cluster.local/ns/other/sa/other"
	tb4XFCCHdr   = "X-Forwarded-Client-Cert"
	tb4SvcBearer = "svc-bearer-token-1"
)

func gatewayPolicies(t *testing.T, identities ...string) auth.ServicePolicies {
	t.Helper()
	configs := make([]auth.ServicePolicyConfig, len(identities))
	for i, identity := range identities {
		configs[i] = auth.ServicePolicyConfig{Identity: identity, TrustedGateway: true}
	}
	policies, err := auth.NewServicePolicies(configs)
	if err != nil {
		t.Fatal(err)
	}
	return policies
}

// serviceHandler is an enforce-mode handler trusting tb4SvcBearer and
// tb4AgentsID, with the TB1 claim-token authorizer wired under key. It sets
// AllowXFCCWithoutMarker so the XFCC-matching tests exercise the allowlist
// path directly; the fail-closed default (no marker, no opt-in) is pinned
// separately by TestXFCCFailsClosedWithoutMarkerOrOptIn (#130).
func serviceHandler(t *testing.T, key []byte) (*Handler, *hookRecorder) {
	t.Helper()
	h, rec := enforcedHandler(t, key)
	creds, err := auth.ParseServiceBearerConfig("agents-server:" + tb4SvcBearer)
	if err != nil {
		t.Fatal(err)
	}
	h.ServiceAuth = &ServiceAuth{
		Credentials:            creds,
		TrustedSPIFFEIDs:       []string{tb4AgentsID},
		AllowXFCCWithoutMarker: true,
		Policies:               gatewayPolicies(t, "agents-server", tb4AgentsID),
	}
	return h, rec
}

func TestServicePolicyScopesActionsAndNamespacesForMeshAndBearer(t *testing.T) {
	const markerName, markerValue = "X-Chronicle-Sidecar", "verified"
	creds, err := auth.ParseServiceBearerConfig("reader:" + tb4SvcBearer)
	if err != nil {
		t.Fatal(err)
	}
	policies, err := auth.NewServicePolicies([]auth.ServicePolicyConfig{
		{Identity: "reader", Actions: []auth.Action{auth.ActionRead}, Namespaces: []string{"tenant-a"}},
		{Identity: tb4AgentsID, Actions: []auth.Action{auth.ActionRead}, Namespaces: []string{"tenant-a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeEnforce
	h.ServiceAuth = &ServiceAuth{
		Credentials:        creds,
		TrustedSPIFFEIDs:   []string{tb4AgentsID},
		Policies:           policies,
		SidecarMarkerName:  markerName,
		SidecarMarkerValue: markerValue,
	}
	createDirect(t, h, "/tenant-a/events", "application/json")
	createDirect(t, h, "/tenant-ab/events", "application/json")

	credentials := []struct {
		name    string
		headers map[string]string
	}{
		{"mesh", map[string]string{tb4XFCCHdr: "URI=" + tb4AgentsID, markerName: markerValue}},
		{"bearer", map[string]string{"Authorization": "Bearer " + tb4SvcBearer}},
	}
	for _, credential := range credentials {
		t.Run(credential.name, func(t *testing.T) {
			rec := do(h, http.MethodGet, "/tenant-a/events", credential.headers, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("authorized read = %d, want 200", rec.Code)
			}
			rec = do(h, http.MethodGet, "/tenant-ab/events", credential.headers, nil)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("cross-namespace read = %d, want 403", rec.Code)
			}
			appendHeaders := map[string]string{"Content-Type": "application/json"}
			for key, value := range credential.headers {
				appendHeaders[key] = value
			}
			rec = do(h, http.MethodPost, "/tenant-a/events", appendHeaders, []byte(`{"n":1}`))
			if rec.Code != http.StatusForbidden {
				t.Fatalf("append without action = %d, want 403", rec.Code)
			}
			rec = do(h, http.MethodPut, "/tenant-a/new", appendHeaders, nil)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("create without action = %d, want 403", rec.Code)
			}
			rec = do(h, http.MethodDelete, "/tenant-a/events", credential.headers, nil)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("delete without action = %d, want 403", rec.Code)
			}
		})
	}
}

func TestServiceBearerAppendServedPreAuthorized(t *testing.T) {
	key := testAuthKey(t)
	h, hooks := serviceHandler(t, key)
	createDirect(t, h, "/events/a", "application/json")

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
	createDirect(t, h, "/events/a", "application/json")

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
	createDirect(t, h, "/events/a", "application/json")
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
	createDirect(t, h, "/events/a", "application/json")
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
	createDirect(t, h, "/events/a", "application/json")

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
	h.ServiceAuth = &ServiceAuth{
		Credentials: creds,
		Policies:    gatewayPolicies(t, "agents-server"),
	} // no TrustedSPIFFEIDs
	createDirect(t, h, "/events/a", "application/json")

	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type": "application/json",
		tb4XFCCHdr:     `URI=` + tb4AgentsID,
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("XFCC without allowlist = %d, want 401", rec.Code)
	}
}

// TestXFCCFailsClosedWithoutMarkerOrOptIn is the #130 re-review fix: with a
// SPIFFE allowlist configured but NO sidecar marker and NO explicit
// AllowXFCCWithoutMarker opt-in, raw client XFCC must never authenticate — the
// gate fails closed rather than defaulting to trusting the header. Otherwise
// an external peer behind a non-sanitizing ingress could forge a service
// principal just by sending an allowlisted SPIFFE id in X-Forwarded-Client-Cert.
func TestXFCCFailsClosedWithoutMarkerOrOptIn(t *testing.T) {
	key := testAuthKey(t)
	h, hooks := enforcedHandler(t, key)
	creds, err := auth.ParseServiceBearerConfig("agents-server:" + tb4SvcBearer)
	if err != nil {
		t.Fatal(err)
	}
	// Allowlist set, but neither a marker nor the explicit opt-in: fail closed.
	h.ServiceAuth = &ServiceAuth{
		Credentials:      creds,
		TrustedSPIFFEIDs: []string{tb4AgentsID},
		Policies:         gatewayPolicies(t, "agents-server", tb4AgentsID),
	}
	createDirect(t, h, "/events/a", "application/json")
	before := tailOf(t, h, "/events/a")

	// The exact header that authenticates under serviceHandler (which opts in)
	// must be denied here.
	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type": "application/json",
		tb4XFCCHdr:     `By=spiffe://cluster.local/ns/chronicle/sa/chronicle;Hash=abcd;URI=` + tb4AgentsID,
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("allowlisted XFCC with no marker/opt-in = %d, want 401 (fail closed)", rec.Code)
	}
	if after := tailOf(t, h, "/events/a"); !after.Equal(before) {
		t.Fatal("store mutated on a denied XFCC append")
	}
	if hooks.appendCount() != 0 {
		t.Fatal("append hook fired on a denied XFCC append")
	}

	// The still-valid service bearer proves the handler itself is functional —
	// only the raw-XFCC path is closed.
	rec = do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + tb4SvcBearer,
	}, []byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("service bearer append = %d, want 204 (bearer path unaffected)", rec.Code)
	}
}

// TestServiceAuthWithoutAppendAuthorizer pins a service-only deployment: its
// explicit trusted-gateway policy authorizes appends without the claim-token
// authorizer, while everyone else fails closed.
func TestServiceAuthWithoutAppendAuthorizer(t *testing.T) {
	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeEnforce
	creds, err := auth.ParseServiceBearerConfig(tb4SvcBearer)
	if err != nil {
		t.Fatal(err)
	}
	h.ServiceAuth = &ServiceAuth{
		Credentials: creds,
		Policies:    gatewayPolicies(t, "service"),
	}
	createDirect(t, h, "/events/a", "application/json")

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
	h.ServiceAuth = &ServiceAuth{
		Credentials: creds,
		Policies:    gatewayPolicies(t, "agents-server"),
	}
	mustCreate(t, h, "/events/a", "application/json", nil)

	rec := do(h, http.MethodPost, "/events/a",
		map[string]string{"Content-Type": "application/json"}, []byte(`{"n":1}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("insecure-mode anonymous append = %d, want 204", rec.Code)
	}
}
