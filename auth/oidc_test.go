package auth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"
)

// ---- in-test IdP: RS256/ES256 minting + JWKS rendering ----

type testIdP struct {
	rsaKey *rsa.PrivateKey
	ecKey  *ecdsa.PrivateKey
}

func newTestIdP(t *testing.T) *testIdP {
	t.Helper()
	rk, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ek, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &testIdP{rsaKey: rk, ecKey: ek}
}

func (idp *testIdP) jwks(t *testing.T) []byte {
	t.Helper()
	pad32 := func(b []byte) []byte {
		out := make([]byte, 32)
		copy(out[32-len(b):], b)
		return out
	}
	doc := map[string]any{"keys": []map[string]string{
		{
			"kty": "RSA", "kid": "rsa-1", "use": "sig",
			"n": base64.RawURLEncoding.EncodeToString(idp.rsaKey.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(idp.rsaKey.E)).Bytes()),
		},
		{
			"kty": "EC", "kid": "ec-1", "use": "sig", "crv": "P-256",
			"x": base64.RawURLEncoding.EncodeToString(pad32(idp.ecKey.X.Bytes())),
			"y": base64.RawURLEncoding.EncodeToString(pad32(idp.ecKey.Y.Bytes())),
		},
		{"kty": "OKP", "kid": "okp-1", "crv": "Ed25519", "x": "AAAA"},
	}}
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// mint signs header+claims with the named kid ("rsa-1" or "ec-1").
func (idp *testIdP) mint(t *testing.T, header map[string]any, claims map[string]any) string {
	t.Helper()
	h, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	c, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	input := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(c)
	digest := sha256.Sum256([]byte(input))
	var sig []byte
	switch header["kid"] {
	case "ec-1":
		r, s, err := ecdsa.Sign(rand.Reader, idp.ecKey, digest[:])
		if err != nil {
			t.Fatal(err)
		}
		sig = make([]byte, 64)
		r.FillBytes(sig[:32])
		s.FillBytes(sig[32:])
	default:
		sig, err = rsa.SignPKCS1v15(rand.Reader, idp.rsaKey, crypto.SHA256, digest[:])
		if err != nil {
			t.Fatal(err)
		}
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

const (
	testIss = "https://idp.example.com"
	testAud = "chronicle"
	testNS  = "ds_namespaces"
)

func testOIDCCfg() OIDCConfig {
	return OIDCConfig{Issuer: testIss, Audience: testAud, NamespaceClaim: testNS}
}

func keyfuncFor(t *testing.T, idp *testIdP) func(string) (OIDCKey, bool) {
	t.Helper()
	keys, err := ParseOIDCJWKS(idp.jwks(t))
	if err != nil {
		t.Fatal(err)
	}
	return func(kid string) (OIDCKey, bool) {
		k, ok := keys[kid]
		return k, ok
	}
}

func baseClaims(now time.Time) map[string]any {
	return map[string]any{
		"iss": testIss, "aud": testAud, "sub": "user-7",
		"exp": now.Add(time.Hour).Unix(), "iat": now.Unix(),
		testNS: []string{"events", "agents/x"},
		// Typical IdP baggage the verifier must tolerate:
		"client_id": "app-1", "scope": "openid profile", "jti": "abc",
	}
}

func TestParseOIDCJWKS(t *testing.T) {
	idp := newTestIdP(t)
	keys, err := ParseOIDCJWKS(idp.jwks(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("parsed %d keys, want 2 (OKP skipped)", len(keys))
	}
	if keys["rsa-1"].Alg != "RS256" || keys["ec-1"].Alg != "ES256" {
		t.Fatalf("alg pinning: rsa=%q ec=%q", keys["rsa-1"].Alg, keys["ec-1"].Alg)
	}
	if _, err := ParseOIDCJWKS([]byte("not json")); err == nil {
		t.Fatal("malformed JWKS must error")
	}
}

func TestVerifyOIDCUserHappyPaths(t *testing.T) {
	idp := newTestIdP(t)
	now := time.Unix(1_700_000_000, 0)
	keys := keyfuncFor(t, idp)

	for _, kid := range []string{"rsa-1", "ec-1"} {
		alg := map[string]string{"rsa-1": "RS256", "ec-1": "ES256"}[kid]
		tok := idp.mint(t, map[string]any{"alg": alg, "typ": "at+jwt", "kid": kid, "x5t": "extra-ok"}, baseClaims(now))
		p, err := VerifyOIDCUser(tok, testOIDCCfg(), keys, now)
		if err != nil {
			t.Fatalf("%s: %v", kid, err)
		}
		if p.Kind() != KindUser || p.Subject() != "user-7" {
			t.Fatalf("%s: principal (%v,%q)", kid, p.Kind(), p.Subject())
		}
		if !p.NamespaceCovers(mustSP(t, "events/a")) || p.NamespaceCovers(mustSP(t, "other/x")) {
			t.Fatalf("%s: namespace grant wrong", kid)
		}
	}

	// aud as an array; ns claim as a single string; typ "JWT".
	claims := baseClaims(now)
	claims["aud"] = []string{"other", testAud}
	claims[testNS] = "events"
	tok := idp.mint(t, map[string]any{"alg": "RS256", "typ": "JWT", "kid": "rsa-1"}, claims)
	p, err := VerifyOIDCUser(tok, testOIDCCfg(), keys, now)
	if err != nil {
		t.Fatal(err)
	}
	if !p.NamespaceCovers(mustSP(t, "events/a")) {
		t.Fatal("string-form namespace claim not honored")
	}
}

func TestVerifyOIDCUserRejections(t *testing.T) {
	idp := newTestIdP(t)
	now := time.Unix(1_700_000_000, 0)
	keys := keyfuncFor(t, idp)
	cfg := testOIDCCfg()

	mint := func(hdr map[string]any, mutate func(map[string]any)) string {
		claims := baseClaims(now)
		if mutate != nil {
			mutate(claims)
		}
		return idp.mint(t, hdr, claims)
	}
	rs := map[string]any{"alg": "RS256", "typ": "at+jwt", "kid": "rsa-1"}

	cases := map[string]string{
		"wrong issuer":      mint(rs, func(c map[string]any) { c["iss"] = "https://evil" }),
		"missing aud":       mint(rs, func(c map[string]any) { delete(c, "aud") }),
		"wrong aud":         mint(rs, func(c map[string]any) { c["aud"] = []string{"other"} }),
		"expired":           mint(rs, func(c map[string]any) { c["exp"] = now.Add(-2 * time.Minute).Unix() }),
		"missing exp":       mint(rs, func(c map[string]any) { delete(c, "exp") }),
		"nbf future":        mint(rs, func(c map[string]any) { c["nbf"] = now.Add(2 * time.Minute).Unix() }),
		"iat future":        mint(rs, func(c map[string]any) { c["iat"] = now.Add(2 * time.Minute).Unix() }),
		"missing sub":       mint(rs, func(c map[string]any) { delete(c, "sub") }),
		"missing ns claim":  mint(rs, func(c map[string]any) { delete(c, testNS) }),
		"empty ns claim":    mint(rs, func(c map[string]any) { c[testNS] = []string{} }),
		"invalid ns entry":  mint(rs, func(c map[string]any) { c[testNS] = []string{"events", "../evil"} }),
		"chronicle typ":     mint(map[string]any{"alg": "RS256", "typ": "application/ds-read+jwt", "kid": "rsa-1"}, nil),
		"wake typ":          mint(map[string]any{"alg": "RS256", "typ": "application/wake+jwt", "kid": "rsa-1"}, nil),
		"crit header":       mint(map[string]any{"alg": "RS256", "typ": "at+jwt", "kid": "rsa-1", "crit": []string{"exp"}}, nil),
		"missing kid":       mint(map[string]any{"alg": "RS256", "typ": "at+jwt"}, nil),
		"unknown kid":       mint(map[string]any{"alg": "RS256", "typ": "at+jwt", "kid": "nope"}, nil),
		"alg none":          mint(map[string]any{"alg": "none", "typ": "at+jwt", "kid": "rsa-1"}, nil),
		"alg EdDSA":         mint(map[string]any{"alg": "EdDSA", "typ": "at+jwt", "kid": "rsa-1"}, nil),
		"alg HS256":         mint(map[string]any{"alg": "HS256", "typ": "at+jwt", "kid": "rsa-1"}, nil),
		"alg/key confusion": mint(map[string]any{"alg": "ES256", "typ": "at+jwt", "kid": "rsa-1"}, nil),
		"key/alg confusion": mint(map[string]any{"alg": "RS256", "typ": "at+jwt", "kid": "ec-1"}, nil),
		"not a jws":         "opaque-token",
	}
	for name, tok := range cases {
		if _, err := VerifyOIDCUser(tok, cfg, keys, now); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	// Tampered claims: flip one payload byte of a valid token.
	tok := mint(rs, nil)
	parts := strings.Split(tok, ".")
	payload := []byte(parts[1])
	payload[0] ^= 0x01
	if _, err := VerifyOIDCUser(parts[0]+"."+string(payload)+"."+parts[2], cfg, keys, now); err == nil {
		t.Error("tampered payload accepted")
	}
}

// TestVerifyOIDCUserNeverAcceptsChronicleFamily: an EdDSA chronicle-family
// token is structurally unverifiable here (alg family disjoint), even if a
// kid collided.
func TestVerifyOIDCUserNeverAcceptsChronicleFamily(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"EdDSA","typ":"application/ds-caller+jwt","kid":"rsa-1"}`))
	pay := base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"` + testIss + `"}`))
	fake := hdr + "." + pay + ".AAAA"
	idp := newTestIdP(t)
	if _, err := VerifyOIDCUser(fake, testOIDCCfg(), keyfuncFor(t, idp), now); err == nil {
		t.Fatal("chronicle-family token accepted by the OIDC verifier")
	}
}

func mustSP(t *testing.T, s string) StreamPath {
	t.Helper()
	p, err := NormalizeStreamPath(s)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
