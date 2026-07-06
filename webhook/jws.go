package webhook

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	signingInput := base64.RawURLEncoding.EncodeToString(hdr) + "." + base64.RawURLEncoding.EncodeToString(payload)
	sig := ed25519.Sign(key.Private, []byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
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
	hdrRaw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("jws: header: %w", err)
	}
	var hdr joseHeader
	dec := json.NewDecoder(bytes.NewReader(hdrRaw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&hdr); err != nil {
		return nil, fmt.Errorf("jws: header: %w", err)
	}
	if dec.More() {
		return nil, errors.New("jws: header: trailing data")
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
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("jws: signature: %w", err)
	}
	signingInput := parts[0] + "." + parts[1]
	if !ed25519.Verify(pub, []byte(signingInput), sig) {
		return nil, errors.New("jws: signature invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("jws: payload: %w", err)
	}
	return payload, nil
}
