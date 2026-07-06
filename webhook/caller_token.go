package webhook

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// The control-plane caller token (issue #126, TB2/TB3): a chronicle-issued
// EdDSA compact JWS proving who is calling the mutating subscription routes.
// It is the credential the claim gate requires before minting the callback
// and write tokens (TB2), and it carries the namespace prefixes link-time
// authorization checks against (TB3). One credential shape for both bullets.
//
// Verification is pure given its inputs (token, expected issuer, key set,
// clock); minting is pure given its randomness. Real IdP issuance (PingFed
// multi-issuer routing) is TB5's widening — this file only defines
// chronicle's own issuer.

// CallerTokenTyp is the caller token's explicit JWS typ (RFC 8725 §3.11).
// Distinct from the wake token's typ, so neither family can validate as the
// other (§3.12).
const CallerTokenTyp = "application/ds-caller+jwt"

// callerClockSkew bounds the clock drift tolerated between the minter and
// the verifier when checking exp and iat.
const callerClockSkew = 60 * time.Second

// callerClaims is the caller token's payload. Decoding is strict: an unknown
// claim is a rejected token, never silently ignored (parse, don't validate).
type callerClaims struct {
	Iss string   `json:"iss"`
	Sub string   `json:"sub"`
	Ns  []string `json:"ns"`
	Exp int64    `json:"exp"`
	Iat int64    `json:"iat"`
	Jti string   `json:"jti"`
}

// VerifiedCaller is a control-plane caller identity whose credential has
// verified. ValidateCallerToken is the only constructor — holding one is
// proof the JWS checked out against chronicle's key set, unexpired, from the
// expected issuer.
type VerifiedCaller struct {
	subject    string
	namespaces []auth.StreamPath
}

// Subject is the verified caller identity string.
func (c VerifiedCaller) Subject() string { return c.subject }

// Namespaces returns the normalized namespace prefixes the caller was
// granted, as a defensive copy.
func (c VerifiedCaller) Namespaces() []auth.StreamPath {
	out := make([]auth.StreamPath, len(c.namespaces))
	copy(out, c.namespaces)
	return out
}

// MayLink reports whether path falls under one of the caller's namespace
// prefixes. Prefix matching is whole-segment: namespace "events" covers
// "events/a" and "events" itself, never "eventsx". This is the (principal,
// path) predicate TB3's link-time authorization enforces.
func (c VerifiedCaller) MayLink(path auth.StreamPath) bool {
	p := path.String()
	for _, ns := range c.namespaces {
		n := ns.String()
		if p == n || strings.HasPrefix(p, n+"/") {
			return true
		}
	}
	return false
}

// GenerateCallerToken mints a caller token for sub, scoped to the given
// namespace prefixes (normalized stream-path prefixes), issued by iss and
// expiring at now+ttl. Ops and tests mint with the active signing key; the
// verifier accepts any key the JWKS currently publishes.
func GenerateCallerToken(key SigningKey, iss, sub string, namespaces []string, now time.Time, ttl time.Duration, rand io.Reader) (string, error) {
	if sub == "" {
		return "", errors.New("caller token: subject required")
	}
	ns := make([]string, len(namespaces))
	for i, raw := range namespaces {
		p, err := auth.NormalizeStreamPath(raw)
		if err != nil {
			return "", fmt.Errorf("caller token: namespace %q: %w", raw, err)
		}
		ns[i] = p.String()
	}
	jti := make([]byte, 8)
	if _, err := io.ReadFull(rand, jti); err != nil {
		return "", fmt.Errorf("caller token jti: %w", err)
	}
	payload, err := json.Marshal(callerClaims{
		Iss: iss,
		Sub: sub,
		Ns:  ns,
		Exp: now.Add(ttl).Unix(),
		Iat: now.Unix(),
		Jti: hex.EncodeToString(jti),
	})
	if err != nil {
		return "", err
	}
	return SignCompactJWS(key, CallerTokenTyp, payload)
}

// ValidateCallerToken verifies a caller token: JWS signature and pinned
// header via VerifyCompactJWS, then strict claim decoding, issuer match,
// expiry and issued-at inside the skew bound, and namespace normalization.
// Errors never embed token material. Fail-closed: the zero VerifiedCaller is
// returned with every error.
func ValidateCallerToken(token, expectedIss string, keyFor KidResolver, now time.Time) (VerifiedCaller, error) {
	payload, err := VerifyCompactJWS(token, CallerTokenTyp, keyFor)
	if err != nil {
		return VerifiedCaller{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	var claims callerClaims
	if err := dec.Decode(&claims); err != nil {
		return VerifiedCaller{}, fmt.Errorf("caller token: claims: %w", err)
	}
	if dec.More() {
		return VerifiedCaller{}, errors.New("caller token: claims: trailing data")
	}
	if claims.Iss != expectedIss {
		return VerifiedCaller{}, errors.New("caller token: issuer not accepted")
	}
	if claims.Sub == "" {
		return VerifiedCaller{}, errors.New("caller token: subject required")
	}
	if claims.Exp == 0 || now.Add(-callerClockSkew).Unix() > claims.Exp {
		return VerifiedCaller{}, errors.New("caller token: expired")
	}
	if claims.Iat > now.Add(callerClockSkew).Unix() {
		return VerifiedCaller{}, errors.New("caller token: issued in the future")
	}
	ns := make([]auth.StreamPath, len(claims.Ns))
	for i, raw := range claims.Ns {
		p, err := auth.NormalizeStreamPath(raw)
		if err != nil {
			return VerifiedCaller{}, errors.New("caller token: invalid namespace")
		}
		ns[i] = p
	}
	return VerifiedCaller{subject: claims.Sub, namespaces: ns}, nil
}
