package webhook

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func testJWSKey(t testing.TB) SigningKey {
	t.Helper()
	key, err := GenerateSigningKey(rand.Reader, time.Unix(1000, 0))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func resolverFor(keys ...SigningKey) KidResolver {
	return func(kid string) (ed25519.PublicKey, bool) {
		for _, k := range keys {
			if k.Kid == kid {
				return k.Public, true
			}
		}
		return nil, false
	}
}

const testTyp = "application/test+jwt"

func TestJWSRoundTrip(t *testing.T) {
	key := testJWSKey(t)
	payload := []byte(`{"sub":"agents/a-1","exp":123}`)

	tok, err := SignCompactJWS(key, testTyp, payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := VerifyCompactJWS(tok, testTyp, resolverFor(key))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: %q != %q", got, payload)
	}
}

// TestJWSRejectsForeignAlgorithms pins the RFC 8725 §2.1 checks: alg=none,
// any non-EdDSA alg, and a missing/duplicated shape are all rejected before
// signature verification is even attempted.
func TestJWSRejectsForeignAlgorithms(t *testing.T) {
	key := testJWSKey(t)
	payload := []byte(`{"x":1}`)
	tok, err := SignCompactJWS(key, testTyp, payload)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(tok, ".")

	forge := func(hdr string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(hdr)) + "." + parts[1] + "." + parts[2]
	}

	cases := map[string]string{
		"alg none":        forge(`{"alg":"none","typ":"` + testTyp + `","kid":"` + key.Kid + `"}`),
		"alg RS256":       forge(`{"alg":"RS256","typ":"` + testTyp + `","kid":"` + key.Kid + `"}`),
		"alg HS256":       forge(`{"alg":"HS256","typ":"` + testTyp + `","kid":"` + key.Kid + `"}`),
		"missing alg":     forge(`{"typ":"` + testTyp + `","kid":"` + key.Kid + `"}`),
		"crit member":     forge(`{"alg":"EdDSA","typ":"` + testTyp + `","kid":"` + key.Kid + `","crit":["exp"]}`),
		"extra member":    forge(`{"alg":"EdDSA","typ":"` + testTyp + `","kid":"` + key.Kid + `","jku":"https://evil"}`),
		"missing kid":     forge(`{"alg":"EdDSA","typ":"` + testTyp + `"}`),
		"wrong typ":       forge(`{"alg":"EdDSA","typ":"application/other+jwt","kid":"` + key.Kid + `"}`),
		"missing typ":     forge(`{"alg":"EdDSA","kid":"` + key.Kid + `"}`),
		"header not json": forge(`EdDSA`),
		"header trailing": forge(`{"alg":"EdDSA","typ":"` + testTyp + `","kid":"` + key.Kid + `"}{}`),
		"two segments":    parts[0] + "." + parts[1],
		"four segments":   tok + ".extra",
		"empty":           "",
		"padded header":   parts[0] + "==." + parts[1] + "." + parts[2],
		"empty signature": parts[0] + "." + parts[1] + ".",
	}
	for name, bad := range cases {
		if _, err := VerifyCompactJWS(bad, testTyp, resolverFor(key)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestJWSKidIsOnlyTrustSource: an unknown kid — including a syntactically
// perfect thumbprint over an attacker's own key — resolves to nothing, so a
// token can never bring its own key.
func TestJWSKidIsOnlyTrustSource(t *testing.T) {
	trusted := testJWSKey(t)
	attacker := testJWSKey(t)
	payload := []byte(`{"x":1}`)

	tok, err := SignCompactJWS(attacker, testTyp, payload)
	if err != nil {
		t.Fatal(err)
	}
	// The attacker's kid is a valid RFC 7638 thumbprint of the attacker's key,
	// but only the trusted set is honored.
	if _, err := VerifyCompactJWS(tok, testTyp, resolverFor(trusted)); err == nil {
		t.Fatal("token signed by an untrusted kid was accepted")
	}
	// The same token verifies once its key IS in the trusted set (the check
	// above failed on trust, not parsing).
	if _, err := VerifyCompactJWS(tok, testTyp, resolverFor(trusted, attacker)); err != nil {
		t.Fatalf("control: %v", err)
	}
}

// TestJWSCrossTypeRejection pins §3.12 mutually-exclusive validation: one
// signed token never verifies under another family's expected typ.
func TestJWSCrossTypeRejection(t *testing.T) {
	key := testJWSKey(t)
	tok, err := SignCompactJWS(key, "application/wake+jwt", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyCompactJWS(tok, "application/ds-caller+jwt", resolverFor(key)); err == nil {
		t.Fatal("wake token verified under the caller-token typ")
	}
}

// TestJWSTamperProperty: flipping any single byte of any segment of a valid
// token makes it unverifiable (or, for the rare header byte flip that keeps
// JSON valid, still never verifies as the same trusted token).
func TestJWSTamperProperty(t *testing.T) {
	key := testJWSKey(t)
	rapid.Check(t, func(t *rapid.T) {
		payload := []byte(rapid.StringMatching(`\{"v":[0-9]{1,9}\}`).Draw(t, "payload"))
		tok, err := SignCompactJWS(key, testTyp, payload)
		if err != nil {
			t.Fatal(err)
		}
		idx := rapid.IntRange(0, len(tok)-1).Draw(t, "idx")
		delta := byte(rapid.IntRange(1, 255).Draw(t, "delta"))
		mut := []byte(tok)
		mut[idx] ^= delta
		if string(mut) == tok {
			return
		}
		if got, err := VerifyCompactJWS(string(mut), testTyp, resolverFor(key)); err == nil {
			// The only acceptable survival is byte-identical payload under the
			// same key — which cannot happen for a real mutation of a fixed
			// token; treat any acceptance as failure.
			t.Fatalf("mutated token accepted (idx %d): payload %q", idx, got)
		}
	})
}

// TestJWSHeaderIsExactlyMinted: the minted header is the canonical three-member
// object, so verifiers with stricter byte-level expectations interoperate.
func TestJWSHeaderIsExactlyMinted(t *testing.T) {
	key := testJWSKey(t)
	tok, err := SignCompactJWS(key, testTyp, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	hdrRaw, err := base64.RawURLEncoding.DecodeString(strings.Split(tok, ".")[0])
	if err != nil {
		t.Fatal(err)
	}
	var hdr map[string]string
	if err := json.Unmarshal(hdrRaw, &hdr); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"alg": "EdDSA", "typ": testTyp, "kid": key.Kid}
	if len(hdr) != len(want) || hdr["alg"] != want["alg"] || hdr["typ"] != want["typ"] || hdr["kid"] != want["kid"] {
		t.Fatalf("header = %v, want %v", hdr, want)
	}
}

// TestJWSRejectsNonCanonicalBase64 pins the malleability fix the CI tamper
// property caught: a signature (or payload) segment whose final character
// differs only in the discarded trailing padding bits decodes to the very
// same bytes under lenient base64url, yielding a DIFFERENT token string that
// still verifies. Strict decoding rejects the non-canonical spelling, so one
// signature has exactly one token string.
func TestJWSRejectsNonCanonicalBase64(t *testing.T) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	key := testJWSKey(t)
	tok, err := SignCompactJWS(key, testTyp, []byte(`{"v":0}`))
	if err != nil {
		t.Fatal(err)
	}
	mutated := 0
	for _, segIdx := range []int{1, 2} { // payload, signature
		parts := strings.Split(tok, ".")
		seg := parts[segIdx]
		spare := len(seg)*6 - len(seg)/4*3*8 - map[int]int{0: 0, 2: 8, 3: 16}[len(seg)%4]
		if spare == 0 {
			continue // segment length leaves no discarded bits to flip
		}
		last := seg[len(seg)-1]
		v := strings.IndexByte(alphabet, last)
		if v < 0 {
			t.Fatalf("segment %d does not end in a base64url char: %q", segIdx, last)
		}
		// Flip the lowest discarded bit: same decoded bytes under lenient
		// decoding, different token string.
		alt := alphabet[v^(1<<(spare-1))]
		if alt == last {
			t.Fatalf("segment %d: mutation did not change the character", segIdx)
		}
		parts[segIdx] = seg[:len(seg)-1] + string(alt)
		bad := strings.Join(parts, ".")
		if bad == tok {
			t.Fatal("mutation produced an identical token")
		}
		if _, err := VerifyCompactJWS(bad, testTyp, resolverFor(key)); err == nil {
			t.Fatalf("segment %d: non-canonical spelling accepted", segIdx)
		}
		mutated++
	}
	if mutated == 0 {
		t.Fatal("no segment had discarded bits; test exercised nothing")
	}
}
