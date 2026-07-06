package webhook

import (
	"crypto/rand"
	"testing"
	"time"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// ---- pure core: mint / validate ----

func TestWakeTokenRoundTrip(t *testing.T) {
	wakeKey := testJWSKey(t)
	now := time.Unix(1_700_000_000, 0)

	claims, err := NewWakeTokenClaims("http://x/v1/stream/", "agents/a-1", "https://gw.example", 7, "w_abc", now, 15*time.Second, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := MintWakeToken(wakeKey, claims)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ValidateWakeToken(tok, resolverFor(wakeKey), "https://gw.example", now.Add(5*time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got.Sub != "agents/a-1" || got.Generation != 7 || got.WakeID != "w_abc" || got.Iss != "http://x/v1/stream/" {
		t.Fatalf("claims = %+v", got)
	}
	if got.Exp != now.Unix()+15 || got.Iat != now.Unix() || got.Nbf != now.Unix() {
		t.Fatalf("time claims = exp %d iat %d nbf %d", got.Exp, got.Iat, got.Nbf)
	}
	if got.Jti == "" {
		t.Fatal("jti missing")
	}
}

// TestWakeTokenWebhookKeyCannotValidate is the TB6a acceptance separation: a
// wake_token names the wake key's kid, so a verifier trusting only the
// webhook-envelope key rejects it — and a token minted UNDER the webhook key
// is exactly the cross-protocol confusion the separate key exists to prevent.
func TestWakeTokenWebhookKeyCannotValidate(t *testing.T) {
	webhookKey := testJWSKey(t)
	wakeKey := testJWSKey(t)
	now := time.Unix(1_700_000_000, 0)

	claims, err := NewWakeTokenClaims("iss", "agents/a-1", "", 1, "w_1", now, 10*time.Second, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := MintWakeToken(wakeKey, claims)
	if err != nil {
		t.Fatal(err)
	}
	// A resolver holding only the webhook key: unknown kid, rejected.
	if _, err := ValidateWakeToken(tok, resolverFor(webhookKey), "", now, time.Second); err == nil {
		t.Fatal("webhook-key resolver accepted a wake_token")
	}
	// Control: the wake-key resolver accepts.
	if _, err := ValidateWakeToken(tok, resolverFor(wakeKey), "", now, time.Second); err != nil {
		t.Fatalf("control: %v", err)
	}
}

func TestWakeTokenTimeWindow(t *testing.T) {
	wakeKey := testJWSKey(t)
	now := time.Unix(1_700_000_000, 0)
	claims, err := NewWakeTokenClaims("iss", "e/1", "", 1, "w_1", now, 10*time.Second, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := MintWakeToken(wakeKey, claims)
	if err != nil {
		t.Fatal(err)
	}
	keyFor := resolverFor(wakeKey)

	cases := []struct {
		name string
		at   time.Time
		skew time.Duration
		ok   bool
	}{
		{"fresh", now.Add(time.Second), time.Second, true},
		{"at exp", now.Add(10 * time.Second), time.Second, true},
		{"past exp within skew", now.Add(10*time.Second + 500*time.Millisecond), time.Second, true},
		{"past exp beyond skew", now.Add(13 * time.Second), time.Second, false},
		{"before nbf beyond skew", now.Add(-5 * time.Second), time.Second, false},
		{"before nbf within skew", now.Add(-500 * time.Millisecond), time.Second, true},
	}
	for _, c := range cases {
		_, err := ValidateWakeToken(tok, keyFor, "", c.at, c.skew)
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok=%v", c.name, err, c.ok)
		}
	}
}

func TestWakeTokenAudiencePinned(t *testing.T) {
	wakeKey := testJWSKey(t)
	now := time.Unix(1_700_000_000, 0)

	withAud, _ := NewWakeTokenClaims("iss", "e/1", "https://gw", 1, "w_1", now, 10*time.Second, rand.Reader)
	tokAud, err := MintWakeToken(wakeKey, withAud)
	if err != nil {
		t.Fatal(err)
	}
	noAud, _ := NewWakeTokenClaims("iss", "e/1", "", 1, "w_1", now, 10*time.Second, rand.Reader)
	tokNoAud, err := MintWakeToken(wakeKey, noAud)
	if err != nil {
		t.Fatal(err)
	}
	keyFor := resolverFor(wakeKey)

	if _, err := ValidateWakeToken(tokAud, keyFor, "https://gw", now, time.Second); err != nil {
		t.Fatalf("matching aud rejected: %v", err)
	}
	if _, err := ValidateWakeToken(tokAud, keyFor, "", now, time.Second); err == nil {
		t.Fatal("aud-scoped token accepted by an aud-less verifier")
	}
	if _, err := ValidateWakeToken(tokAud, keyFor, "https://other", now, time.Second); err == nil {
		t.Fatal("wrong aud accepted")
	}
	if _, err := ValidateWakeToken(tokNoAud, keyFor, "", now, time.Second); err != nil {
		t.Fatalf("aud-less token rejected by aud-less verifier: %v", err)
	}
	if _, err := ValidateWakeToken(tokNoAud, keyFor, "https://gw", now, time.Second); err == nil {
		t.Fatal("aud-less token accepted by an aud-pinning verifier")
	}
}

// ---- cross-grammar separation (issue #123's three-token-grammar vectors) ----

func TestWakeTokenCrossGrammarRejection(t *testing.T) {
	wakeKey := testJWSKey(t)
	hmacKey := testTokenKey(t)
	now := time.Unix(1_700_000_000, 0)
	path := mustPath(t, "events/a")

	claims, _ := NewWakeTokenClaims("iss", "events/a", "", 1, "w_1", now, 10*time.Second, rand.Reader)
	wakeTok, err := MintWakeToken(wakeKey, claims)
	if err != nil {
		t.Fatal(err)
	}
	cbTok, err := GenerateToken(hmacKey, "s1", 1, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	writeTok, err := GenerateWriteToken(hmacKey, "s1", 1, []auth.StreamPath{path}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	envelope := SignWebhookPayload(wakeKey, []byte(`{"x":1}`), now)

	// A wake_token is not an HMAC callback token, not a write token.
	if tv := ValidateToken(hmacKey, wakeTok, "s1", now); tv.Valid || tv.Expired {
		t.Fatalf("wake_token accepted by callback validation: %+v", tv)
	}
	if v := ValidateWriteToken(hmacKey, wakeTok, path, now); v.Status != WriteTokenInvalid {
		t.Fatalf("wake_token accepted by write-token validation: %v", v.Status)
	}
	// Neither HMAC token nor the envelope signature is a wake_token.
	keyFor := resolverFor(wakeKey)
	for name, s := range map[string]string{"callback token": cbTok, "write token": writeTok, "envelope signature": envelope} {
		if _, err := ValidateWakeToken(s, keyFor, "", now, time.Second); err == nil {
			t.Errorf("%s accepted as wake_token", name)
		}
	}
}

// ---- TTL / entity rules ----

// TestWakeTokenTTLUnderLease is the #123 exp invariant: the minted TTL is
// strictly under one lease for every legal lease, and never the callback
// token's lease+1h convention.
func TestWakeTokenTTLUnderLease(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		leaseMs := rapid.Int64Range(MinLeaseTTLMs, MaxLeaseTTLMs).Draw(t, "lease")
		ttl := WakeTokenTTL(leaseMs)
		lease := time.Duration(leaseMs) * time.Millisecond
		if ttl <= 0 {
			t.Fatalf("lease %dms: non-positive ttl %v", leaseMs, ttl)
		}
		if ttl >= lease {
			t.Fatalf("lease %dms: ttl %v not strictly under the lease", leaseMs, ttl)
		}
		if ttl > maxWakeTokenTTL {
			t.Fatalf("lease %dms: ttl %v above the cap", leaseMs, ttl)
		}
	})
	if WakeTokenTTL(0) != 0 || WakeTokenTTL(-5) != 0 {
		t.Fatal("non-positive lease must mint nothing")
	}
}

func TestWakeEntityPath(t *testing.T) {
	if _, ok := WakeEntityPath(nil); ok {
		t.Fatal("no links must name no entity")
	}
	p, ok := WakeEntityPath([]StreamLink{{Path: "agents/a-1"}})
	if !ok || p != "agents/a-1" {
		t.Fatalf("single link = (%q, %v)", p, ok)
	}
	if _, ok := WakeEntityPath([]StreamLink{{Path: "a"}, {Path: "b"}}); ok {
		t.Fatal("multi-link subscription must name no entity")
	}
}

func TestShouldRefreshWakeToken(t *testing.T) {
	if ShouldRefreshWakeToken(true) {
		t.Fatal("a done ack must never refresh — the wake is over")
	}
	if !ShouldRefreshWakeToken(false) {
		t.Fatal("a heartbeat ack must refresh")
	}
}

// ---- tamper property over the full wake_token ----

func TestWakeTokenTamperProperty(t *testing.T) {
	wakeKey := testJWSKey(t)
	now := time.Unix(1_700_000_000, 0)
	rapid.Check(t, func(t *rapid.T) {
		sub := rapid.StringMatching(`[a-z0-9/]{1,24}`).Draw(t, "sub")
		claims, err := NewWakeTokenClaims("iss", sub, "", 1, "w_1", now, 10*time.Second, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tok, err := MintWakeToken(wakeKey, claims)
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
		if _, err := ValidateWakeToken(string(mut), resolverFor(wakeKey), "", now, time.Second); err == nil {
			t.Fatalf("tampered wake_token accepted (idx %d)", idx)
		}
	})
}
