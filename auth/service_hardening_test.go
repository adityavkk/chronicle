package auth

import (
	"strings"
	"testing"
)

// TestServiceBearerHashedCompare is finding F7: the hardened compare hashes
// both sides to a fixed width before the constant-time compare, so it accepts
// exactly the configured token and rejects every non-match — including
// wrong-length presentations that would previously short-circuit
// subtle.ConstantTimeCompare and leak the configured token's length. The
// timing property itself is not unit-testable; this pins that behavior is
// otherwise unchanged and length-independent.
func TestServiceBearerHashedCompare(t *testing.T) {
	const real = "the-real-64-byte-token-abcdefghijklmnopqrstuvwxyz0123456789ABCD"
	creds, err := ParseServiceBearerConfig("svc:" + real)
	if err != nil {
		t.Fatal(err)
	}

	p, ok := VerifyServiceBearer(real, creds)
	if !ok || p.Kind() != KindService || p.Subject() != "svc" {
		t.Fatalf("exact token must authenticate as svc, got ok=%v kind=%v", ok, p.Kind())
	}

	// Non-matches of every length — shorter, longer, prefix, and empty — all
	// deny without panic.
	for _, bad := range []string{
		"",
		"x",
		real[:len(real)-1], // one byte short
		real + "x",         // one byte long
		strings.Repeat("z", 256),
	} {
		if _, ok := VerifyServiceBearer(bad, creds); ok {
			t.Fatalf("non-matching bearer (len %d) must not authenticate", len(bad))
		}
	}

	// Empty credential set never authenticates.
	if _, ok := VerifyServiceBearer(real, nil); ok {
		t.Fatal("empty credential set must not authenticate")
	}
}
