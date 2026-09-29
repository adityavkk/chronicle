package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
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
	// ServiceRejected means XFCC was presented but failed its attestation gate,
	// was refused by the XFCC parser as malformed, or missed the exact identity
	// allowlist. AuthenticateDetail tells the three apart for the operator.
	ServiceRejected
)

// serviceRejectedDetail is the operator-facing detail of a ServiceRejected
// status caused by a failed marker gate or an allowlist miss. The two are
// deliberately indistinguishable on the wire and in logs.
const serviceRejectedDetail = "invalid service identity"

// Authenticate resolves one service principal from request credential
// primitives. joinedXFCC must contain every XFCC header line joined in HTTP
// order. marker is accepted only when the caller observed exactly one marker
// header value.
func (s *ServiceAccess) Authenticate(bearer, joinedXFCC, marker string) (Principal, ServiceAuthenticationStatus) {
	principal, status, _ := s.AuthenticateDetail(bearer, joinedXFCC, marker)
	return principal, status
}

// AuthenticateDetail is Authenticate plus the operator-facing detail of a
// ServiceRejected status, for the denial log line and the error envelope. A
// failed marker gate and an allowlist miss both report "invalid service
// identity". A header the XFCC parser refused reports that distinctly —
// "invalid service identity: malformed X-Forwarded-Client-Cert: <reason>" —
// so an operator can tell a hop that emits non-Envoy quoting, or a client
// probing the parser, from a peer that is simply not allowlisted. The detail
// never carries header, marker, or credential material; it is empty unless
// the status is ServiceRejected.
func (s *ServiceAccess) AuthenticateDetail(bearer, joinedXFCC, marker string) (Principal, ServiceAuthenticationStatus, string) {
	if s == nil {
		return Principal{}, ServiceNotAttempted, ""
	}
	// SPIFFE is first-class. If XFCC is present, a failed mesh attestation is a
	// terminal service-authentication failure, never a downgrade to bearer.
	if joinedXFCC != "" {
		if !s.xfccGatePasses(marker) {
			return Principal{}, ServiceRejected, serviceRejectedDetail
		}
		principal, ok, err := verifyXFCC(joinedXFCC, s.TrustedSPIFFEIDs)
		if err != nil {
			return Principal{}, ServiceRejected, serviceRejectedDetail + ": " + err.Error()
		}
		if ok {
			return principal, ServiceAuthenticated, ""
		}
		return Principal{}, ServiceRejected, serviceRejectedDetail
	}
	if principal, ok := VerifyServiceBearer(bearer, s.Credentials); ok {
		return principal, ServiceAuthenticated, ""
	}
	return Principal{}, ServiceNotAttempted, ""
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
// The last-element rule is only as good as the element boundaries, so the
// header is first checked against Envoy's quoting grammar by parseXFCC and
// refused as a whole when it violates it: a client-controlled prefix must not
// be able to move or hide the comma in front of the sidecar's element.
//
// The match is an exact, case-sensitive comparison of the element's URI SAN
// (SPIFFE IDs are case-sensitive by spec) against the allowlist. An empty
// header, a malformed header, or an empty allowlist never authenticates.
func VerifyXFCC(header string, trusted []string) (Principal, bool) {
	principal, ok, _ := verifyXFCC(header, trusted)
	return principal, ok
}

// verifyXFCC is VerifyXFCC plus the parse error, so AuthenticateDetail can
// report a refused header distinctly from an allowlist miss. A non-nil error
// never comes with ok == true.
func verifyXFCC(header string, trusted []string) (Principal, bool, error) {
	if header == "" || len(trusted) == 0 {
		return Principal{}, false, nil
	}
	elements, err := parseXFCC(header)
	if err != nil {
		return Principal{}, false, err
	}
	last := elements[len(elements)-1]
	for _, uri := range last.uris() {
		for _, t := range trusted {
			if uri == t {
				return Principal{kind: KindService, subject: uri}, true, nil
			}
		}
	}
	return Principal{}, false, nil
}

// ErrMalformedXFCC is wrapped by every X-Forwarded-Client-Cert grammar
// violation. Chronicle refuses such a header outright instead of guessing
// where its elements begin and end; see parseXFCC.
var ErrMalformedXFCC = errors.New("malformed X-Forwarded-Client-Cert")

// The parser's refusal reasons. They are a fixed, low-cardinality set and
// never quote header content, so they are safe in logs and error envelopes.
const (
	// xfccReasonUnterminatedQuote: the input ended inside a quoted value,
	// including after a trailing lone backslash.
	xfccReasonUnterminatedQuote = "unterminated quoted value"
	// xfccReasonMisplacedQuote: a double quote that does not open a whole
	// value — inside a key, or inside or after the start of an unquoted value.
	xfccReasonMisplacedQuote = "double quote outside a quoted value"
	// xfccReasonTrailingAfterQuote: bytes between a closing quote and the next
	// separator or the end of the header.
	xfccReasonTrailingAfterQuote = "data after a quoted value"
)

func xfccErr(reason string) error { return fmt.Errorf("%w: %s", ErrMalformedXFCC, reason) }

// xfccPair is one key=value of an XFCC element with Envoy's quoting removed.
type xfccPair struct{ key, value string }

// xfccElement is the key=value pairs one hop added to the header.
type xfccElement []xfccPair

// uris returns the element's URI SAN values. Envoy emits fixed key names; the
// comparison is case-insensitive on the key purely as defensive slack, never
// on the value.
func (e xfccElement) uris() []string {
	var uris []string
	for _, p := range e {
		if strings.EqualFold(p.key, "URI") && p.value != "" {
			uris = append(uris, p.value)
		}
	}
	return uris
}

// parseXFCC tokenizes an X-Forwarded-Client-Cert value into its elements
// (comma-separated, one per hop) of key=value pairs (semicolon-separated),
// applying Envoy's quoting rules and refusing anything outside them.
//
// Envoy wraps a value that contains ',', ';' or '=' — and always Subject —
// in double quotes and escapes an embedded '"' as '\"'; RFC 2253 subjects
// arrive with their own backslash escapes, which are read the same way (a
// backslash always pairs with the byte after it, and that byte is kept). The
// grammar therefore has exactly one place a double quote may appear: opening
// a value, right after '=', and closing it, right before ';', ',' or the end.
// A quote anywhere else, a quoted value the input ends inside (including a
// trailing lone backslash), or bytes between a closing quote and the next
// separator is malformed, and the whole header is refused with an error that
// wraps ErrMalformedXFCC — never parsed with a guessed element boundary.
//
// The strictness is what makes the last-element rule hold under
// APPEND_FORWARD, where the prefix is client-controlled: a lenient parser that
// let an unbalanced quote run across the comma before the sidecar's appended
// element treated the client's URI as part of the "last" element (the 2026-09
// review finding against the former splitXFCC).
//
// Everything else stays lenient and unambiguous: a pair without '=' carries no
// value and is ignored, blanks around keys and values are trimmed, and an
// empty element (a leading, trailing, or doubled comma) is kept as an element
// with no pairs, so "URI=x," still ends with an empty last element.
func parseXFCC(s string) ([]xfccElement, error) {
	var (
		elements []xfccElement
		element  xfccElement
		i, n     = 0, len(s)
	)
	for {
		// Key: everything up to '=' or a separator; never quoted.
		start := i
		for i < n && s[i] != '=' && s[i] != ';' && s[i] != ',' {
			if s[i] == '"' {
				return nil, xfccErr(xfccReasonMisplacedQuote)
			}
			i++
		}
		key := strings.TrimSpace(s[start:i])
		if i < n && s[i] == '=' {
			i++
			i = skipXFCCBlanks(s, i)
			var value string
			if i < n && s[i] == '"' {
				// Quoted value: read to the closing quote, honoring escapes.
				i++
				var b strings.Builder
				closed := false
				for i < n {
					c := s[i]
					if c == '\\' {
						if i+1 >= n {
							return nil, xfccErr(xfccReasonUnterminatedQuote)
						}
						b.WriteByte(s[i+1])
						i += 2
						continue
					}
					i++
					if c == '"' {
						closed = true
						break
					}
					b.WriteByte(c)
				}
				if !closed {
					return nil, xfccErr(xfccReasonUnterminatedQuote)
				}
				i = skipXFCCBlanks(s, i)
				if i < n && s[i] != ';' && s[i] != ',' {
					return nil, xfccErr(xfccReasonTrailingAfterQuote)
				}
				value = b.String()
			} else {
				// Unquoted value: up to the next separator; a quote here is
				// not Envoy's grammar and would be the start of a boundary game.
				vstart := i
				for i < n && s[i] != ';' && s[i] != ',' {
					if s[i] == '"' {
						return nil, xfccErr(xfccReasonMisplacedQuote)
					}
					i++
				}
				value = strings.TrimSpace(s[vstart:i])
			}
			element = append(element, xfccPair{key: key, value: value})
		}
		if i >= n {
			return append(elements, element), nil
		}
		if s[i] == ',' {
			elements = append(elements, element)
			element = nil
		}
		i++ // consume ';' or ','
	}
}

// skipXFCCBlanks advances past spaces and tabs, the only blanks a hop might
// put around a quoted value (Envoy itself emits none).
func skipXFCCBlanks(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	return i
}
