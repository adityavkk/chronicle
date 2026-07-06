package webhook

import (
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

const testReadIss = "http://x/v1/stream/"

func TestReadCapabilityRoundTrip(t *testing.T) {
	key := testJWSKey(t)
	now := time.Unix(10_000, 0)

	tok, err := GenerateReadCapability(key, testReadIss, "reader-1", []string{"events", "agents/x"}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cap, err := ValidateReadCapability(tok, testReadIss, resolverForKeys([]SigningKey{key}), now)
	if err != nil {
		t.Fatal(err)
	}
	if cap.Subject() != "reader-1" {
		t.Fatalf("subject = %q", cap.Subject())
	}

	covers := map[string]bool{
		"events":          true, // the prefix itself
		"events/a":        true,
		"events/a/nested": true,
		"agents/x":        true,
		"agents/x/inbox":  true,
		"eventsx":         false, // whole-segment, never string prefix
		"agents":          false,
		"agents/y":        false,
		"other":           false,
	}
	for p, want := range covers {
		if got := cap.Covers(mustPath(t, p)); got != want {
			t.Errorf("Covers(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestReadCapabilityRejections(t *testing.T) {
	key := testJWSKey(t)
	other := testJWSKey(t)
	now := time.Unix(10_000, 0)
	keyFor := resolverForKeys([]SigningKey{key})

	tok, err := GenerateReadCapability(key, testReadIss, "reader-1", []string{"events"}, now, time.Minute, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]struct {
		token string
		iss   string
		at    time.Time
		keys  KidResolver
	}{
		"expired":      {tok, testReadIss, now.Add(time.Minute + 61*time.Second), keyFor},
		"wrong issuer": {tok, "http://evil/", now, keyFor},
		"unknown kid":  {tok, testReadIss, now, resolverForKeys([]SigningKey{other})},
		"garbage":      {"not-a-token", testReadIss, now, keyFor},
		"tampered":     {tok[:len(tok)-3] + "zzz", testReadIss, now, keyFor},
		"empty":        {"", testReadIss, now, keyFor},
		"two segments": {strings.Join(strings.Split(tok, ".")[:2], "."), testReadIss, now, keyFor},
	}
	for name, c := range cases {
		if _, err := ValidateReadCapability(c.token, c.iss, c.keys, c.at); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	// Inside the skew bound the token still verifies (mirror of expired).
	if _, err := ValidateReadCapability(tok, testReadIss, keyFor, now.Add(time.Minute+30*time.Second)); err != nil {
		t.Fatalf("within skew: %v", err)
	}
}

// TestReadCapabilityCrossFamilyRejection pins §3.12 across every credential
// grammar in the model: the read capability never validates as a caller
// token, and neither the caller token, the opaque HMAC write token, nor the
// callback token validates as a read capability.
func TestReadCapabilityCrossFamilyRejection(t *testing.T) {
	key := testJWSKey(t)
	now := time.Unix(10_000, 0)
	keyFor := resolverForKeys([]SigningKey{key})
	tokenKey := testTokenKey(t)

	readCap, err := GenerateReadCapability(key, testReadIss, "r", []string{"events"}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	callerTok, err := GenerateCallerToken(key, testReadIss, "c", []string{"events"}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	writeTok, err := GenerateWriteToken(tokenKey, "sub-1", 1, []auth.StreamPath{mustPath(t, "events/a")}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cbTok, err := GenerateToken(tokenKey, "sub-1", 1, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := ValidateCallerToken(readCap, testReadIss, keyFor, now); err == nil {
		t.Fatal("read capability accepted as caller token")
	}
	for name, tok := range map[string]string{
		"caller token":   callerTok,
		"write token":    writeTok,
		"callback token": cbTok,
	} {
		if _, err := ValidateReadCapability(tok, testReadIss, keyFor, now); err == nil {
			t.Fatalf("%s accepted as read capability", name)
		}
	}
}

func TestReadCapabilityAuthorizerMapping(t *testing.T) {
	key := testJWSKey(t)
	now := time.Unix(10_000, 0)
	az := NewReadCapabilityAuthorizer(testReadIss, StaticKidResolver(key))

	tok, err := GenerateReadCapability(key, testReadIss, "r", []string{"events"}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name       string
		token      string
		path       string
		wantAllow  bool
		wantReason auth.DenyReason
	}{
		{"covered", tok, "events/a", true, auth.ReasonNone},
		{"prefix itself", tok, "events", true, auth.ReasonNone},
		{"missing", "", "events/a", false, auth.ReasonUnauthenticated},
		{"garbage", "junk", "events/a", false, auth.ReasonUnauthenticated},
		{"out of scope", tok, "other/a", false, auth.ReasonForbidden},
	}
	for _, c := range cases {
		d := az.AuthorizeRead(c.token, mustPath(t, c.path), now)
		if d.Allowed() != c.wantAllow || d.Reason() != c.wantReason {
			t.Errorf("%s: allowed=%v reason=%v, want allowed=%v reason=%v",
				c.name, d.Allowed(), d.Reason(), c.wantAllow, c.wantReason)
		}
	}

	// A key-source failure denies (fail closed), never errors open.
	broken := NewReadCapabilityAuthorizer(testReadIss, StaticKidResolver())
	if d := broken.AuthorizeRead(tok, mustPath(t, "events/a"), now); d.Allowed() {
		t.Fatal("key-source failure must deny")
	}
}

// TestReadCapabilityTamperProperty: any single-byte corruption of a valid
// capability is rejected, whatever byte and whatever delta.
func TestReadCapabilityTamperProperty(t *testing.T) {
	key := testJWSKey(t)
	now := time.Unix(10_000, 0)
	keyFor := resolverForKeys([]SigningKey{key})
	tok, err := GenerateReadCapability(key, testReadIss, "r", []string{"events"}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rapid.Check(t, func(t *rapid.T) {
		idx := rapid.IntRange(0, len(tok)-1).Draw(t, "idx")
		delta := byte(rapid.IntRange(1, 255).Draw(t, "delta"))
		mut := []byte(tok)
		mut[idx] ^= delta
		if string(mut) == tok {
			return
		}
		if _, err := ValidateReadCapability(string(mut), testReadIss, keyFor, now); err == nil {
			t.Fatalf("corrupted capability accepted (idx %d)", idx)
		}
	})
}

func TestPeekIssuer(t *testing.T) {
	key := testJWSKey(t)
	now := time.Unix(10_000, 0)
	tok, err := GenerateReadCapability(key, testReadIss, "r", []string{"events"}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if iss, ok := PeekIssuer(tok); !ok || iss != testReadIss {
		t.Fatalf("PeekIssuer = (%q,%v)", iss, ok)
	}

	tokenKey := testTokenKey(t)
	writeTok, err := GenerateWriteToken(tokenKey, "s", 1, []auth.StreamPath{mustPath(t, "e/a")}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]string{
		"opaque write token": writeTok, // two segments, no iss — never routed
		"garbage":            "x.y.z",
		"empty":              "",
	} {
		if _, ok := PeekIssuer(bad); ok {
			t.Errorf("%s: PeekIssuer accepted", name)
		}
	}
}
