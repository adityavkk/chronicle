package webhook

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// This file is the minimal compact-JWS layer for chronicle-minted EdDSA
// tokens (issues #126/#123): the wake_token and the control-plane caller
// token. It is deliberately NOT a general JOSE library — the accepted
// grammar is pinned per RFC 8725: alg is exactly "EdDSA", typ is exactly the
// expected token type (mutually-exclusive validation across token families,
// §3.12), kid is required and resolved only against chronicle's own key set,
// and any other protected-header member (including crit) is rejected.
// Time-based claims (exp/nbf/iat) are the caller's to enforce — the claim
// schema differs per token family, and this layer never parses the payload.

// joseHeader is the protected header chronicle mints and the only shape the
// verifier accepts.
type joseHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
	Kid string `json:"kid"`
}

const jwsAlgEdDSA = "EdDSA"

// jwsB64 is strict unpadded base64url: decoding rejects non-zero trailing
// padding bits. The lenient default would let an attacker flip the discarded
// bits of a segment's final character and mint a *different token string*
// that still decodes to the same signature bytes — token malleability that
// breaks any string-keyed dedup/denylist and violates JWS canonicality
// (RFC 7515 base64url; caught by the tamper property in CI).
var jwsB64 = base64.RawURLEncoding.Strict()

// SignCompactJWS signs payload as a compact EdDSA JWS under key, stamping
// the given typ (RFC 8725 §3.11 explicit typing).
func SignCompactJWS(key SigningKey, typ string, payload []byte) (string, error) {
	if len(key.Private) == 0 {
		return "", errors.New("jws: signing key has no private half")
	}
	if typ == "" {
		return "", errors.New("jws: a token type is required (RFC 8725 §3.11)")
	}
	hdr, err := json.Marshal(joseHeader{Alg: jwsAlgEdDSA, Typ: typ, Kid: key.Kid})
	if err != nil {
		return "", err
	}
	signingInput := jwsB64.EncodeToString(hdr) + "." + jwsB64.EncodeToString(payload)
	sig := ed25519.Sign(key.Private, []byte(signingInput))
	return signingInput + "." + jwsB64.EncodeToString(sig), nil
}

// resolverForKeys adapts a signing-key slice to a KidResolver.
func resolverForKeys(keys []SigningKey) KidResolver {
	return func(kid string) (ed25519.PublicKey, bool) {
		for _, k := range keys {
			if k.Kid == kid {
				return k.Public, true
			}
		}
		return nil, false
	}
}

// StaticKidResolver builds a time-independent resolver provider over a fixed
// key set — for tests and simple single-key deployments that do not rotate.
// Production wires the Manager's snapshot resolver (m.callerKidResolver /
// m.WakeKidResolver) instead, which additionally honors the rotation overlap
// window and the kid denylist at each decision's clock.
func StaticKidResolver(keys ...SigningKey) func(now time.Time) KidResolver {
	r := resolverForKeys(keys)
	return func(time.Time) KidResolver { return r }
}

// PeekIssuer reads the iss claim of a compact JWS WITHOUT any verification.
// It exists solely to route a bearer to the right verifier (chronicle's own
// EdDSA family vs a configured OIDC issuer) — the returned value carries no
// trust whatsoever, and every verifier re-checks iss after signature
// verification. Returns false for anything that does not even parse as a
// three-segment JWS with a JSON payload (e.g. the opaque HMAC write token).
func PeekIssuer(token string) (string, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", false
	}
	payload, err := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	if err != nil {
		return "", false
	}
	var claims struct {
		Iss string `json:"iss"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Iss == "" {
		return "", false
	}
	return claims.Iss, true
}

// PeekTyp reads the JOSE typ of a compact JWS header WITHOUT any
// verification. Like PeekIssuer it exists solely to route a bearer to the
// right verifier family — the returned value carries no trust, and the
// routed-to verifier re-pins typ (and everything else) after signature
// verification. Returns false for anything that does not parse as a
// three-segment JWS with a JSON header.
func PeekTyp(token string) (string, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", false
	}
	hdrRaw, err := base64.RawURLEncoding.Strict().DecodeString(parts[0])
	if err != nil {
		return "", false
	}
	var hdr struct {
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(hdrRaw, &hdr); err != nil || hdr.Typ == "" {
		return "", false
	}
	return hdr.Typ, true
}

// KidResolver returns the verifying public key for a kid, or false when the
// kid is unknown, retired, or denylisted. The resolver is the verifier's only
// trust source — an attacker-supplied kid resolves to nothing, so a token can
// never carry or choose its own key (RFC 8725 §2.8).
type KidResolver func(kid string) (ed25519.PublicKey, bool)

// VerifyCompactJWS parses and verifies a compact EdDSA JWS, returning the raw
// payload only when every check passes: exactly three base64url segments; a
// strict protected header carrying alg exactly "EdDSA" (never "none" or any
// other algorithm), typ exactly expectedTyp, a kid keyFor resolves, and no
// other member; then the Ed25519 signature over the exact received signing
// input. Every failure is the same generic error shape — a caller cannot
// probe which check rejected a forgery.
func VerifyCompactJWS(token, expectedTyp string, keyFor KidResolver) ([]byte, error) {
	if expectedTyp == "" {
		return nil, errors.New("jws: an expected token type is required")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errors.New("jws: not a compact JWS")
	}
	hdrRaw, err := jwsB64.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("jws: header: %w", err)
	}
	var hdr joseHeader
	if err := strictJSONUnmarshal(hdrRaw, &hdr); err != nil {
		return nil, fmt.Errorf("jws: header: %w", err)
	}
	if hdr.Alg != jwsAlgEdDSA {
		return nil, errors.New("jws: algorithm not accepted")
	}
	if hdr.Typ != expectedTyp {
		return nil, errors.New("jws: token type not accepted")
	}
	if hdr.Kid == "" {
		return nil, errors.New("jws: kid required")
	}
	pub, ok := keyFor(hdr.Kid)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return nil, errors.New("jws: unknown kid")
	}
	sig, err := jwsB64.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("jws: signature: %w", err)
	}
	signingInput := parts[0] + "." + parts[1]
	if !ed25519.Verify(pub, []byte(signingInput), sig) {
		return nil, errors.New("jws: signature invalid")
	}
	payload, err := jwsB64.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("jws: payload: %w", err)
	}
	return payload, nil
}
