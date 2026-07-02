package webhook

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

var sigHeaderRe = regexp.MustCompile(`^t=(\d+),kid=([^,]+),ed25519=([A-Za-z0-9_-]+)$`)

func TestSignWebhookPayloadVerifies(t *testing.T) {
	key, err := GenerateSigningKey(rand.Reader, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key.Kid, "ds_") {
		t.Fatalf("kid %q must start with ds_", key.Kid)
	}
	body := []byte(`{"subscription_id":"sub-1","wake_id":"w_abc"}`)
	now := time.Unix(1778324210, 0)
	header := SignWebhookPayload(key, body, now)

	m := sigHeaderRe.FindStringSubmatch(header)
	if m == nil {
		t.Fatalf("signature header %q does not match the conformance regex", header)
	}
	if m[2] != key.Kid {
		t.Fatalf("kid mismatch: header %q key %q", m[2], key.Kid)
	}
	ts, _ := strconv.ParseInt(m[1], 10, 64)
	sig, err := base64.RawURLEncoding.DecodeString(m[3])
	if err != nil {
		t.Fatal(err)
	}
	// Reconstruct the public key from the JWK x, exactly as the conformance
	// receiver does, and verify over "<ts>.<rawBody>".
	jwk := key.JWK()
	pubBytes, err := base64.RawURLEncoding.DecodeString(jwk.X)
	if err != nil {
		t.Fatal(err)
	}
	signed := []byte(fmt.Sprintf("%d.%s", ts, body))
	if !ed25519.Verify(ed25519.PublicKey(pubBytes), signed, sig) {
		t.Fatal("signature did not verify against the JWK public key")
	}
}

func TestTokenRoundTrip(t *testing.T) {
	tokenKey := make([]byte, 32)
	if _, err := rand.Read(tokenKey); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0)
	tok, err := GenerateToken(tokenKey, "sub-1", 7, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	v := ValidateToken(tokenKey, tok, "sub-1", now)
	if !v.Valid || v.Generation != 7 {
		t.Fatalf("valid token rejected: %+v", v)
	}
	// Wrong subscription.
	if ValidateToken(tokenKey, tok, "sub-2", now).Valid {
		t.Fatal("token must be bound to its subscription")
	}
	// Tampered body.
	if ValidateToken(tokenKey, "x"+tok, "sub-1", now).Valid {
		t.Fatal("tampered token must not validate")
	}
	// Wrong key.
	other := make([]byte, 32)
	if ValidateToken(other, tok, "sub-1", now).Valid {
		t.Fatal("token signed with a different key must not validate")
	}
	// Expired.
	exp := ValidateToken(tokenKey, tok, "sub-1", now.Add(2*time.Hour))
	if exp.Valid || !exp.Expired {
		t.Fatalf("expired token should report expired: %+v", exp)
	}
}

// TestTokenExpiredBoundary pins the pure expiry predicate to ValidateToken's
// boundary: valid while now <= exp, expired once now > exp (issue #77).
func TestTokenExpiredBoundary(t *testing.T) {
	const exp = 1000
	cases := []struct {
		name string
		now  int64
		want bool
	}{
		{"well before", 500, false},
		{"one before", 999, false},
		{"exactly at exp", 1000, false},
		{"one after", 1001, true},
		{"long after", 5000, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := TokenExpired(exp, tc.now); got != tc.want {
				t.Fatalf("TokenExpired(%d, %d) = %v, want %v", exp, tc.now, got, tc.want)
			}
		})
	}
}

// TestShouldRefreshToken table-tests the pure in-band refresh decision (issue
// #77): refresh a still-valid token only when it is within the threshold of
// expiry; never refresh a comfortably-valid or an already-expired token.
func TestShouldRefreshToken(t *testing.T) {
	const (
		exp       = 10_000
		threshold = 300 * time.Second
	)
	cases := []struct {
		name string
		now  int64
		want bool
	}{
		{"comfortably valid (1h out)", exp - 3600, false},
		{"just outside threshold", exp - 301, false},
		{"exactly at threshold", exp - 300, true},
		{"inside threshold", exp - 60, true},
		{"one second to expiry", exp - 1, true},
		{"exactly at exp (still valid)", exp, true},
		{"one past exp (expired)", exp + 1, false},
		{"long expired", exp + 5000, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldRefreshToken(exp, tc.now, threshold); got != tc.want {
				t.Fatalf("ShouldRefreshToken(%d, %d, %s) = %v, want %v", exp, tc.now, threshold, got, tc.want)
			}
		})
	}
}

func TestWakeIDFormat(t *testing.T) {
	id, err := GenerateWakeID(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id, "w_") || len(id) != 2+24 {
		t.Fatalf("wake id %q malformed", id)
	}
}
