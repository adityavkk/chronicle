package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"strings"
)

// This file contains the service-principal authentication core. Mesh-attested
// SPIFFE workload identity is the primary service credential. Static bearers
// remain an optional compatibility fallback. Both verifiers are the only
// constructors of a service Principal: a raw header string can never stand in
// for a verified identity.

// ServiceAccess is the shared service authentication and authorization
// configuration used by the data plane and subscription control plane.
// Mesh-attested SPIFFE is evaluated before the static-bearer compatibility
// fallback. The zero value authenticates and authorizes nothing.
type ServiceAccess struct {
	Credentials            []ServiceCredential
	TrustedSPIFFEIDs       []string
	Policies               ServicePolicies
	SidecarMarkerName      string
	SidecarMarkerValue     string
	AllowXFCCWithoutMarker bool
}

// ServiceAuthenticationStatus describes whether service authentication was
// applicable and whether it succeeded.
type ServiceAuthenticationStatus uint8

const (
	// ServiceNotAttempted means no service credential family matched.
	ServiceNotAttempted ServiceAuthenticationStatus = iota
	// ServiceAuthenticated means a service credential verified.
	ServiceAuthenticated
	// ServiceRejected means XFCC was presented but failed its attestation gate
	// or exact identity allowlist.
	ServiceRejected
)

// Authenticate resolves one service principal from request credential
// primitives. joinedXFCC must contain every XFCC header line joined in HTTP
// order. marker is accepted only when the caller observed exactly one marker
// header value.
func (s *ServiceAccess) Authenticate(bearer, joinedXFCC, marker string) (Principal, ServiceAuthenticationStatus) {
	if s == nil {
		return Principal{}, ServiceNotAttempted
	}
	// SPIFFE is first-class. If XFCC is present, a failed mesh attestation is a
	// terminal service-authentication failure, never a downgrade to bearer.
	if joinedXFCC != "" {
		if !s.xfccGatePasses(marker) {
			return Principal{}, ServiceRejected
		}
		if principal, ok := VerifyXFCC(joinedXFCC, s.TrustedSPIFFEIDs); ok {
			return principal, ServiceAuthenticated
		}
		return Principal{}, ServiceRejected
	}
	if principal, ok := VerifyServiceBearer(bearer, s.Credentials); ok {
		return principal, ServiceAuthenticated
	}
	return Principal{}, ServiceNotAttempted
}

func (s *ServiceAccess) xfccGatePasses(marker string) bool {
	if s.SidecarMarkerName != "" && s.SidecarMarkerValue != "" {
		return subtle.ConstantTimeCompare([]byte(marker), []byte(s.SidecarMarkerValue)) == 1
	}
	return s.AllowXFCCWithoutMarker
}

// Authorize evaluates the verified service against its explicit policy.
func (s *ServiceAccess) Authorize(principal Principal, action Action, paths ...StreamPath) (Decision, bool) {
	if s == nil {
		return Deny(ReasonUnauthenticated, "service authorization is not configured"), false
	}
	return s.Policies.Authorize(principal, action, paths...)
}

// AuthorizeAction evaluates an action when no target path exists to inspect.
func (s *ServiceAccess) AuthorizeAction(principal Principal, action Action) (Decision, bool) {
	if s == nil {
		return Deny(ReasonUnauthenticated, "service authorization is not configured"), false
	}
	return s.Policies.AuthorizeAction(principal, action)
}

// TrustedGateway reports whether principal is the exact identity carrying the
// explicit delegated-gateway policy.
func (s *ServiceAccess) TrustedGateway(principal Principal) bool {
	return s != nil && s.Policies.TrustedGateway(principal)
}

// ServiceCredential is one configured service identity: a subject name and
// its static bearer token. The token is sealed — no accessor, and the
// formatting interfaces are overridden — so credential material cannot reach
// a log by accident. Build one only through ParseServiceBearerConfig.
type ServiceCredential struct {
	name  string
	token string
}

// Name is the subject the credential authenticates (e.g. "agents-server").
func (c ServiceCredential) Name() string { return c.name }

// String redacts the token (fmt %v / %+v safety).
func (c ServiceCredential) String() string { return "ServiceCredential(" + c.name + ")" }

// GoString redacts the token (fmt %#v safety).
func (c ServiceCredential) GoString() string { return c.String() }

// defaultServiceSubject names a bare-token credential with no explicit name.
const defaultServiceSubject = "service"

// ParseServiceBearerConfig parses the CHRONICLE_SERVICE_BEARER value:
// comma-separated entries, each "name:token" or a bare "token" (subject
// "service"). Two entries may share a name — that is the rotation overlap
// (old and new token both valid while the upstream rolls). Empty entries,
// names, or tokens are configuration typos and fail startup; error text
// carries positions, never token material.
func ParseServiceBearerConfig(s string) ([]ServiceCredential, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("service bearer config is empty")
	}
	entries := strings.Split(s, ",")
	out := make([]ServiceCredential, 0, len(entries))
	for i, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			return nil, fmt.Errorf("service bearer entry %d is empty", i+1)
		}
		name, token := defaultServiceSubject, e
		// Split on the FIRST colon only: tokens may themselves contain colons.
		if n, t, ok := strings.Cut(e, ":"); ok {
			name, token = strings.TrimSpace(n), t
			if name == "" {
				return nil, fmt.Errorf("service bearer entry %d has an empty name", i+1)
			}
			if token == "" {
				return nil, fmt.Errorf("service bearer entry %d has an empty token", i+1)
			}
		}
		out = append(out, ServiceCredential{name: name, token: token})
	}
	return out, nil
}

// VerifyServiceBearer checks a presented bearer against the configured
// service credentials. Only an exact match yields a Principal; an empty
// presentation or empty credential set never authenticates.
//
// The comparison hashes both sides to a fixed 32-byte SHA-256 digest before
// the constant-time compare, so it leaks neither the configured token's
// length (subtle.ConstantTimeCompare short-circuits on differing lengths) nor
// any prefix-match information — the digests are always equal width and
// unrelated to the plaintext bytes. SHA-256 preimage resistance means the
// digest compare accepts exactly the same inputs as a raw compare would.
func VerifyServiceBearer(presented string, creds []ServiceCredential) (Principal, bool) {
	if presented == "" {
		return Principal{}, false
	}
	presentedHash := sha256.Sum256([]byte(presented))
	for _, c := range creds {
		credHash := sha256.Sum256([]byte(c.token))
		if subtle.ConstantTimeCompare(presentedHash[:], credHash[:]) == 1 {
			return Principal{kind: KindService, subject: c.name}, true
		}
	}
	return Principal{}, false
}

// ParseTrustedSPIFFEIDs parses the CHRONICLE_TRUSTED_SPIFFE_IDS value: a
// comma-separated allowlist of SPIFFE URIs. Every entry must be a spiffe://
// URI; anything else is a configuration typo and fails startup.
func ParseTrustedSPIFFEIDs(s string) ([]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("trusted SPIFFE id list is empty")
	}
	entries := strings.Split(s, ",")
	out := make([]string, 0, len(entries))
	for i, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			return nil, fmt.Errorf("trusted SPIFFE entry %d is empty", i+1)
		}
		if !strings.HasPrefix(e, "spiffe://") {
			return nil, fmt.Errorf("trusted SPIFFE entry %d is not a spiffe:// URI", i+1)
		}
		out = append(out, e)
	}
	return out, nil
}

// VerifyXFCC authenticates an in-mesh peer from an Envoy
// X-Forwarded-Client-Cert header against a trusted SPIFFE allowlist.
//
// Only the LAST element of the header is honored. Envoy builds XFCC by
// appending one element per hop, each describing that proxy's own mTLS-
// verified client — so the last element is the one chronicle's own sidecar
// attested about its immediate downstream peer. Every earlier element is
// forwarded hearsay from upstream hops (or attacker input, if any hop
// forwards without sanitizing), so it must never authenticate anyone.
//
// The match is an exact, case-sensitive comparison of the element's URI SAN
// (SPIFFE IDs are case-sensitive by spec) against the allowlist. An empty
// header or an empty allowlist never authenticates.
func VerifyXFCC(header string, trusted []string) (Principal, bool) {
	if header == "" || len(trusted) == 0 {
		return Principal{}, false
	}
	elements := splitXFCC(header, ',')
	last := elements[len(elements)-1]
	for _, uri := range xfccURIs(last) {
		for _, t := range trusted {
			if uri == t {
				return Principal{kind: KindService, subject: uri}, true
			}
		}
	}
	return Principal{}, false
}

// splitXFCC splits on sep outside double-quoted values, honoring Envoy's
// quoted-string escaping (backslash escapes inside quotes).
func splitXFCC(s string, sep byte) []string {
	var parts []string
	var cur strings.Builder
	inQuotes := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inQuotes && c == '\\' && i+1 < len(s):
			cur.WriteByte(c)
			i++
			cur.WriteByte(s[i])
		case c == '"':
			inQuotes = !inQuotes
			cur.WriteByte(c)
		case c == sep && !inQuotes:
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	parts = append(parts, cur.String())
	return parts
}

// xfccURIs extracts every URI SAN from one XFCC element. Envoy emits fixed
// key names; the comparison is case-insensitive on the key purely as
// defensive slack, never on the value.
func xfccURIs(element string) []string {
	var uris []string
	for _, pair := range splitXFCC(element, ';') {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(k), "URI") {
			continue
		}
		if u := unquoteXFCC(strings.TrimSpace(v)); u != "" {
			uris = append(uris, u)
		}
	}
	return uris
}

// unquoteXFCC strips one layer of double quotes and backslash escapes, the
// quoting Envoy applies to values containing separators.
func unquoteXFCC(v string) string {
	if len(v) < 2 || v[0] != '"' || v[len(v)-1] != '"' {
		return v
	}
	inner := v[1 : len(v)-1]
	var b strings.Builder
	for i := 0; i < len(inner); i++ {
		if inner[i] == '\\' && i+1 < len(inner) {
			i++
		}
		b.WriteByte(inner[i])
	}
	return b.String()
}
