package auth

import (
	"strings"
	"testing"
)

func TestParseServicePoliciesAndAuthorize(t *testing.T) {
	policies, err := ParseServicePolicies([]byte(`{
		"services": [
			{"identity":"spiffe://cluster.local/ns/electric/sa/reader","actions":["read"],"namespaces":["/tenant-a"]},
			{"identity":"spiffe://cluster.local/ns/electric/sa/gateway","trusted_gateway":true}
		]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	reader, ok := VerifyXFCC("URI=spiffe://cluster.local/ns/electric/sa/reader", policies.SPIFFEIdentities())
	if !ok {
		t.Fatal("reader SPIFFE identity did not authenticate")
	}
	tenantA, _ := NormalizeStreamPath("tenant-a/events")
	tenantB, _ := NormalizeStreamPath("tenant-ab/events")
	if decision, delegated := policies.Authorize(reader, ActionRead, tenantA); !decision.Allowed() || delegated {
		t.Fatalf("reader read = allowed %v delegated %v", decision.Allowed(), delegated)
	}
	if decision, _ := policies.Authorize(reader, ActionAppend, tenantA); decision.Allowed() || decision.Reason() != ReasonForbidden {
		t.Fatalf("reader append = allowed %v reason %v", decision.Allowed(), decision.Reason())
	}
	if decision, _ := policies.Authorize(reader, ActionRead, tenantB); decision.Allowed() {
		t.Fatal("whole-segment namespace boundary was not enforced")
	}

	gateway, ok := VerifyXFCC("URI=spiffe://cluster.local/ns/electric/sa/gateway", policies.SPIFFEIdentities())
	if !ok {
		t.Fatal("gateway SPIFFE identity did not authenticate")
	}
	if decision, delegated := policies.Authorize(gateway, ActionClaim, tenantB); !decision.Allowed() || !delegated {
		t.Fatalf("gateway claim = allowed %v delegated %v", decision.Allowed(), delegated)
	}
	if policies.TrustedGateway(reader) || !policies.TrustedGateway(gateway) {
		t.Fatal("trusted gateway designation applied to the wrong subject")
	}
}

func TestParseServicePoliciesStrictFailures(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"empty", `{"services":[]}`, "empty"},
		{"unknown field", `{"services":[],"extra":true}`, "unknown field"},
		{"missing identity", `{"services":[{"actions":["read"],"namespaces":["tenant-a"]}]}`, "identity is required"},
		{"duplicate identity", `{"services":[{"identity":"svc","trusted_gateway":true},{"identity":"svc","trusted_gateway":true}]}`, "duplicate identity"},
		{"unknown action", `{"services":[{"identity":"svc","actions":["admin"],"namespaces":["tenant-a"]}]}`, "unknown action"},
		{"malformed namespace", `{"services":[{"identity":"svc","actions":["read"],"namespaces":["tenant-a//events"]}]}`, "invalid namespace"},
		{"empty non-gateway", `{"services":[{"identity":"svc","actions":["read"]}]}`, "requires actions and namespaces"},
		{"ambiguous gateway restrictions", `{"services":[{"identity":"svc","trusted_gateway":true,"actions":["read"],"namespaces":["tenant-a"]}]}`, "must not set actions or namespaces"},
		{"trailing JSON", `{"services":[{"identity":"svc","trusted_gateway":true}]} {}`, "multiple values"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseServicePolicies([]byte(tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestServiceAccessPrefersAttestedSPIFFEAndRejectsDowngrade(t *testing.T) {
	creds, err := ParseServiceBearerConfig("fallback:secret")
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewServicePolicies([]ServicePolicyConfig{
		{Identity: "fallback", TrustedGateway: true},
		{Identity: "spiffe://cluster.local/ns/electric/sa/reader", Actions: []Action{ActionRead}, Namespaces: []string{"tenant-a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	access := ServiceAccess{
		Credentials:        creds,
		TrustedSPIFFEIDs:   []string{"spiffe://cluster.local/ns/electric/sa/reader"},
		Policies:           policies,
		SidecarMarkerName:  "X-Chronicle-Sidecar",
		SidecarMarkerValue: "verified",
	}
	principal, status := access.Authenticate("secret", "URI=spiffe://cluster.local/ns/electric/sa/reader", "verified")
	if status != ServiceAuthenticated || principal.Subject() != "spiffe://cluster.local/ns/electric/sa/reader" {
		t.Fatalf("mesh-first authentication = %v %q", status, principal.Subject())
	}
	if _, status := access.Authenticate("secret", "URI=spiffe://cluster.local/ns/electric/sa/reader", ""); status != ServiceRejected {
		t.Fatalf("missing marker status = %v, want rejected", status)
	}
	if _, status := access.Authenticate("secret", "URI=spiffe://cluster.local/ns/electric/sa/unlisted", "verified"); status != ServiceRejected {
		t.Fatalf("unlisted SPIFFE status = %v, want rejected", status)
	}
	principal, status = access.Authenticate("secret", "", "")
	if status != ServiceAuthenticated || principal.Subject() != "fallback" {
		t.Fatalf("bearer fallback = %v %q", status, principal.Subject())
	}
}
