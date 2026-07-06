package webhook

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// The read-capability token (issue #126, TB5): a chronicle-issued EdDSA
// compact JWS granting read on a set of namespace prefixes. Same key family
// and JWS grammar as the caller token, distinct typ so neither validates as
// the other (RFC 8725 §3.12). It answers "may read what" for direct clients
// that are not IdP principals — who mints one, and against what policy, is
// the operator's delegation decision (the remaining half of Q1); chronicle
// defines the format and enforces the scope.

// ReadCapabilityTyp is the read capability's explicit JWS typ.
const ReadCapabilityTyp = "application/ds-read+jwt"

// readCapClaims is the read capability's payload. Decoding is strict: an
// unknown claim is a rejected token (parse, don't validate).
type readCapClaims struct {
	Iss   string   `json:"iss"`
	Sub   string   `json:"sub"`
	Paths []string `json:"paths"`
	Exp   int64    `json:"exp"`
	Iat   int64    `json:"iat"`
	Jti   string   `json:"jti"`
}

// VerifiedReadCapability is a read grant whose credential has verified.
// ValidateReadCapability is the only constructor.
type VerifiedReadCapability struct {
	subject string
	paths   []auth.StreamPath
}

// Subject is the verified holder identity string.
func (c VerifiedReadCapability) Subject() string { return c.subject }

// Covers reports whether the grant covers path (whole-segment prefix
// semantics — the same auth.PathWithinPrefixes predicate every scope check
// in the authz model evaluates).
func (c VerifiedReadCapability) Covers(path auth.StreamPath) bool {
	return auth.PathWithinPrefixes(path, c.paths)
}

// GenerateReadCapability mints a read capability for sub, scoped to the
// given namespace prefixes, issued by iss and expiring at now+ttl. Ops and
// tests mint with the active signing key; the verifier accepts any key the
// JWKS currently publishes.
func GenerateReadCapability(key SigningKey, iss, sub string, paths []string, now time.Time, ttl time.Duration, rand io.Reader) (string, error) {
	if sub == "" {
		return "", errors.New("read capability: subject required")
	}
	if len(paths) == 0 {
		return "", errors.New("read capability: at least one path prefix required")
	}
	scope := make([]string, len(paths))
	for i, raw := range paths {
		p, err := auth.NormalizeStreamPath(raw)
		if err != nil {
			return "", fmt.Errorf("read capability: path %q: %w", raw, err)
		}
		scope[i] = p.String()
	}
	jti := make([]byte, 8)
	if _, err := io.ReadFull(rand, jti); err != nil {
		return "", fmt.Errorf("read capability jti: %w", err)
	}
	payload, err := json.Marshal(readCapClaims{
		Iss:   iss,
		Sub:   sub,
		Paths: scope,
		Exp:   now.Add(ttl).Unix(),
		Iat:   now.Unix(),
		Jti:   hex.EncodeToString(jti),
	})
	if err != nil {
		return "", err
	}
	return SignCompactJWS(key, ReadCapabilityTyp, payload)
}

// ValidateReadCapability verifies a read capability: JWS signature and
// pinned header via VerifyCompactJWS, then strict claim decoding, issuer
// match, expiry and issued-at inside the skew bound, and path
// normalization. Fail-closed; errors never embed token material.
func ValidateReadCapability(token, expectedIss string, keyFor KidResolver, now time.Time) (VerifiedReadCapability, error) {
	payload, err := VerifyCompactJWS(token, ReadCapabilityTyp, keyFor)
	if err != nil {
		return VerifiedReadCapability{}, err
	}
	var claims readCapClaims
	if err := strictJSONUnmarshal(payload, &claims); err != nil {
		return VerifiedReadCapability{}, fmt.Errorf("read capability: claims: %w", err)
	}
	if claims.Iss != expectedIss {
		return VerifiedReadCapability{}, errors.New("read capability: issuer not accepted")
	}
	if claims.Sub == "" {
		return VerifiedReadCapability{}, errors.New("read capability: subject required")
	}
	if claims.Exp == 0 || now.Add(-callerClockSkew).Unix() > claims.Exp {
		return VerifiedReadCapability{}, errors.New("read capability: expired")
	}
	if claims.Iat > now.Add(callerClockSkew).Unix() {
		return VerifiedReadCapability{}, errors.New("read capability: issued in the future")
	}
	if len(claims.Paths) == 0 {
		return VerifiedReadCapability{}, errors.New("read capability: empty scope")
	}
	paths := make([]auth.StreamPath, len(claims.Paths))
	for i, raw := range claims.Paths {
		p, err := auth.NormalizeStreamPath(raw)
		if err != nil {
			return VerifiedReadCapability{}, errors.New("read capability: invalid path in scope")
		}
		paths[i] = p
	}
	return VerifiedReadCapability{subject: claims.Sub, paths: paths}, nil
}

// ReadCapabilityAuthorizer authorizes data-plane reads with a chronicle
// read-capability JWS. It implements chronicle's ReadAuthorizer seam;
// resolver is called per decision so key rotation, the overlap window, and
// the kid denylist are all honored without restart.
type ReadCapabilityAuthorizer struct {
	iss      string
	resolver func(now time.Time) KidResolver
}

// NewReadCapabilityAuthorizer builds an authorizer for tokens issued by iss,
// verifying against the resolver's trust set at the decision time. Tests
// supply a fixed set (StaticKidResolver); production wires
// Manager.ReadAuthorizer().
func NewReadCapabilityAuthorizer(iss string, resolver func(now time.Time) KidResolver) ReadCapabilityAuthorizer {
	return ReadCapabilityAuthorizer{iss: iss, resolver: resolver}
}

// ReadAuthorizer exposes the Manager's custody-source-fed key snapshot as the
// read authorizer the HTTP handler enforces with — the SAME rotation- and
// denylist-aware trust set the JWKS publishes and the control-plane caller
// gate verifies against, so file custody is honored and an emergency kid
// revocation takes effect on the data plane too (issue #126 hardening: read
// capabilities previously verified against m.store, which bypassed both).
func (m *Manager) ReadAuthorizer() ReadCapabilityAuthorizer {
	return NewReadCapabilityAuthorizer(m.streamRootURL, m.callerKidResolver)
}

// AuthorizeRead maps a presented (possibly absent) read credential to the
// Decision for a read at path. Fail-closed: an unverifiable token denies
// (401-class), and a verified grant that does not cover the path is forbidden
// (403-class).
func (a ReadCapabilityAuthorizer) AuthorizeRead(token string, path auth.StreamPath, now time.Time) auth.Decision {
	if token == "" {
		return auth.Deny(auth.ReasonUnauthenticated, "missing read credential")
	}
	cap, err := ValidateReadCapability(token, a.iss, a.resolver(now), now)
	if err != nil {
		return auth.Deny(auth.ReasonUnauthenticated, "invalid read capability")
	}
	if !cap.Covers(path) {
		return auth.Deny(auth.ReasonForbidden, "read capability not scoped to this stream")
	}
	return auth.Allow()
}
