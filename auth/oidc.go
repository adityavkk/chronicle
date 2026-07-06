package auth

import (
	"bytes"
	"crypto"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// OIDC access-token verification (issue #126 TB5): the multi-issuer widening
// of the read/mutate gates. A PingFed-issued RS256/ES256 JWT becomes a user
// Principal carrying the namespace prefixes named by a configured claim —
// the Q1 identity decision: direct-client authorization anchors on the
// enterprise IdP's claims, and the claim→namespace mapping is deployment
// configuration, not chronicle policy.
//
// This verifier is deliberately SEPARATE from the chronicle-family JWS
// verifier (webhook's VerifyCompactJWS): different entry point, different
// trust roots (the IdP's JWKS, never chronicle's), and disjoint algorithm
// families (RS256/ES256 here, EdDSA there), so a token from one family is
// structurally incapable of verifying under the other. Verification is pure
// given the token, config, key set, and clock; fetching the IdP's JWKS is
// the shell's job.

// OIDCConfig pins what an acceptable IdP token looks like. All fields are
// required — a partially configured issuer refuses at parse time rather
// than verifying weakly.
type OIDCConfig struct {
	// Issuer is the exact iss the token must carry (also the discovery base).
	Issuer string
	// Audience must appear in the token's aud (string or array form).
	Audience string
	// NamespaceClaim names the claim holding the caller's namespace
	// prefixes (string or array of strings), e.g. "ds_namespaces". The
	// claim→scope mapping is deploy-side IdP configuration (Q1).
	NamespaceClaim string
}

// Validate reports whether the config is complete.
func (c OIDCConfig) Validate() error {
	switch {
	case strings.TrimSpace(c.Issuer) == "":
		return errors.New("oidc: issuer required")
	case strings.TrimSpace(c.Audience) == "":
		return errors.New("oidc: audience required")
	case strings.TrimSpace(c.NamespaceClaim) == "":
		return errors.New("oidc: namespace claim required")
	default:
		return nil
	}
}

// OIDCKey is one verification key from the IdP's JWKS: the parsed public key
// plus the single JWS alg it is pinned to (RS256 for RSA keys, ES256 for
// P-256 keys). Pinning by key type forecloses algorithm confusion — a token
// header can request an alg, but only the key's own alg is ever used.
type OIDCKey struct {
	Alg string
	Key crypto.PublicKey
}

// ParseOIDCJWKS parses an IdP JWKS document into kid→key. Unsupported key
// types and curves are skipped (an IdP JWKS may carry keys for other
// purposes); a kid seen twice keeps the first. A malformed document errors.
func ParseOIDCJWKS(data []byte) (map[string]OIDCKey, error) {
	var doc struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			Use string `json:"use"`
			Crv string `json:"crv"`
			N   string `json:"n"`
			E   string `json:"e"`
			X   string `json:"x"`
			Y   string `json:"y"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("oidc jwks: %w", err)
	}
	out := make(map[string]OIDCKey, len(doc.Keys))
	for _, k := range doc.Keys {
		if k.Kid == "" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		if _, dup := out[k.Kid]; dup {
			continue
		}
		switch k.Kty {
		case "RSA":
			n, err := base64.RawURLEncoding.DecodeString(k.N)
			if err != nil || len(n) == 0 {
				continue
			}
			e, err := base64.RawURLEncoding.DecodeString(k.E)
			if err != nil || len(e) == 0 || len(e) > 8 {
				continue
			}
			pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
			if pub.N.BitLen() < 2048 || pub.E < 3 {
				continue
			}
			out[k.Kid] = OIDCKey{Alg: "RS256", Key: pub}
		case "EC":
			if k.Crv != "P-256" {
				continue
			}
			x, errX := base64.RawURLEncoding.DecodeString(k.X)
			y, errY := base64.RawURLEncoding.DecodeString(k.Y)
			if errX != nil || errY != nil || len(x) > 32 || len(y) > 32 {
				continue
			}
			// On-curve validation via crypto/ecdh (the supported check since
			// Go 1.21): NewPublicKey rejects off-curve and infinity points for
			// the uncompressed encoding.
			point := make([]byte, 65)
			point[0] = 0x04
			copy(point[1+32-len(x):33], x)
			copy(point[33+32-len(y):], y)
			if _, err := ecdh.P256().NewPublicKey(point); err != nil {
				continue
			}
			pub := &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(x), Y: new(big.Int).SetBytes(y)}
			out[k.Kid] = OIDCKey{Alg: "ES256", Key: pub}
		default:
			// OKP/oct etc.: never used for IdP access-token verification here.
		}
	}
	return out, nil
}

// oidcClockSkew bounds tolerated clock drift against the IdP.
const oidcClockSkew = 60 * time.Second

// oidcHeader is the JOSE header subset the verifier honors. Unlike the
// chronicle-family verifier, unknown header members are tolerated — the IdP
// mints them (x5t, jku, ...) — but crit is still rejected (RFC 7515 §4.1.11:
// an unhonored critical extension MUST fail), and alg/typ/kid are pinned.
type oidcHeader struct {
	Alg  string          `json:"alg"`
	Typ  string          `json:"typ"`
	Kid  string          `json:"kid"`
	Crit json.RawMessage `json:"crit"`
}

// acceptedOIDCTyp reports whether the header typ is an access-token typ.
// PingFed mints "at+jwt" (RFC 9068), "application/at+jwt", or legacy "JWT";
// anything else — including every chronicle-family typ — is rejected.
func acceptedOIDCTyp(typ string) bool {
	switch typ {
	case "at+jwt", "application/at+jwt", "JWT":
		return true
	default:
		return false
	}
}

// VerifyOIDCUser verifies an IdP access token and returns the user
// Principal it proves: subject from sub, namespace prefixes from the
// configured claim. Fail-closed on every path; errors never embed token
// material. keys resolves a kid to its pinned key (the shell's cached JWKS).
func VerifyOIDCUser(token string, cfg OIDCConfig, keys func(kid string) (OIDCKey, bool), now time.Time) (Principal, error) {
	if err := cfg.Validate(); err != nil {
		return Principal{}, err
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Principal{}, errors.New("oidc: not a compact JWS")
	}
	hdrRaw, err := decodeSegment(parts[0])
	if err != nil {
		return Principal{}, fmt.Errorf("oidc: header: %w", err)
	}
	var hdr oidcHeader
	if err := json.Unmarshal(hdrRaw, &hdr); err != nil {
		return Principal{}, fmt.Errorf("oidc: header: %w", err)
	}
	if len(hdr.Crit) > 0 {
		return Principal{}, errors.New("oidc: critical header extensions not supported")
	}
	if hdr.Typ != "" && !acceptedOIDCTyp(hdr.Typ) {
		return Principal{}, errors.New("oidc: token type not accepted")
	}
	if hdr.Kid == "" {
		return Principal{}, errors.New("oidc: kid required")
	}
	key, ok := keys(hdr.Kid)
	if !ok {
		return Principal{}, errors.New("oidc: unknown kid")
	}
	// The key's pinned alg is authoritative; the header must agree with it,
	// never the other way around (no algorithm negotiation).
	if hdr.Alg != key.Alg {
		return Principal{}, errors.New("oidc: algorithm not accepted")
	}
	sig, err := decodeSegment(parts[2])
	if err != nil {
		return Principal{}, fmt.Errorf("oidc: signature: %w", err)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	switch k := key.Key.(type) {
	case *rsa.PublicKey:
		if err := rsa.VerifyPKCS1v15(k, crypto.SHA256, digest[:], sig); err != nil {
			return Principal{}, errors.New("oidc: signature invalid")
		}
	case *ecdsa.PublicKey:
		// JWS ES256 signatures are raw R||S (RFC 7518 §3.4), not ASN.1.
		if len(sig) != 64 {
			return Principal{}, errors.New("oidc: signature invalid")
		}
		r := new(big.Int).SetBytes(sig[:32])
		s := new(big.Int).SetBytes(sig[32:])
		if !ecdsa.Verify(k, digest[:], r, s) {
			return Principal{}, errors.New("oidc: signature invalid")
		}
	default:
		return Principal{}, errors.New("oidc: unsupported key")
	}

	payload, err := decodeSegment(parts[1])
	if err != nil {
		return Principal{}, fmt.Errorf("oidc: payload: %w", err)
	}
	return oidcPrincipalFromClaims(payload, cfg, now)
}

// oidcPrincipalFromClaims applies the claim checks to a signature-verified
// payload. IdP tokens carry many claims; only the pinned set is read, but
// aud and the namespace claim accept both string and array forms.
func oidcPrincipalFromClaims(payload []byte, cfg OIDCConfig, now time.Time) (Principal, error) {
	var claims map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(payload))
	if err := dec.Decode(&claims); err != nil {
		return Principal{}, fmt.Errorf("oidc: claims: %w", err)
	}

	iss, _ := stringClaim(claims["iss"])
	if iss != cfg.Issuer {
		return Principal{}, errors.New("oidc: issuer not accepted")
	}
	auds, err := stringsClaim(claims["aud"])
	if err != nil || len(auds) == 0 {
		return Principal{}, errors.New("oidc: audience missing")
	}
	found := false
	for _, a := range auds {
		if a == cfg.Audience {
			found = true
			break
		}
	}
	if !found {
		return Principal{}, errors.New("oidc: audience not accepted")
	}
	var exp int64
	if raw, ok := claims["exp"]; !ok || json.Unmarshal(raw, &exp) != nil || exp == 0 {
		return Principal{}, errors.New("oidc: exp required")
	}
	if now.Add(-oidcClockSkew).Unix() > exp {
		return Principal{}, errors.New("oidc: token expired")
	}
	if raw, ok := claims["nbf"]; ok {
		var nbf int64
		if json.Unmarshal(raw, &nbf) != nil || nbf > now.Add(oidcClockSkew).Unix() {
			return Principal{}, errors.New("oidc: token not yet valid")
		}
	}
	if raw, ok := claims["iat"]; ok {
		var iat int64
		if json.Unmarshal(raw, &iat) != nil || iat > now.Add(oidcClockSkew).Unix() {
			return Principal{}, errors.New("oidc: issued in the future")
		}
	}
	sub, ok := stringClaim(claims["sub"])
	if !ok || sub == "" {
		return Principal{}, errors.New("oidc: subject required")
	}
	rawNS, ok := claims[cfg.NamespaceClaim]
	if !ok {
		return Principal{}, errors.New("oidc: namespace claim missing")
	}
	nsStrings, err := stringsClaim(rawNS)
	if err != nil || len(nsStrings) == 0 {
		return Principal{}, errors.New("oidc: namespace claim empty")
	}
	ns := make([]StreamPath, len(nsStrings))
	for i, raw := range nsStrings {
		p, err := NormalizeStreamPath(raw)
		if err != nil {
			// A malformed grant is never partially honored (fail closed).
			return Principal{}, errors.New("oidc: invalid namespace in claim")
		}
		ns[i] = p
	}
	return Principal{kind: KindUser, subject: sub, namespaces: ns}, nil
}

// decodeSegment strictly decodes one base64url JWS segment (no padding, no
// alternate alphabets).
func decodeSegment(s string) ([]byte, error) {
	return base64.RawURLEncoding.Strict().DecodeString(s)
}

// stringClaim reads a JSON string claim.
func stringClaim(raw json.RawMessage) (string, bool) {
	if raw == nil {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// stringsClaim reads a claim that is either one string or an array of
// strings (the two shapes aud and group-style claims take).
func stringsClaim(raw json.RawMessage) ([]string, error) {
	if raw == nil {
		return nil, errors.New("claim missing")
	}
	if s, ok := stringClaim(raw); ok {
		return []string{s}, nil
	}
	var arr []string
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, err
	}
	return arr, nil
}
