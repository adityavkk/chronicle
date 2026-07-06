package webhook

import (
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

const testIssuer = "http://x/v1/stream/"

func mintCaller(t testing.TB, key SigningKey, sub string, ns []string, now time.Time, ttl time.Duration) string {
	t.Helper()
	tok, err := GenerateCallerToken(key, testIssuer, sub, ns, now, ttl, rand.Reader)
	if err != nil {
		t.Fatalf("mint caller token: %v", err)
	}
	return tok
}

func TestCallerTokenRoundTrip(t *testing.T) {
	key := testJWSKey(t)
	now := time.Unix(10_000, 0)
	tok := mintCaller(t, key, "svc:agents-server", []string{"/events", "agents/x"}, now, time.Hour)

	caller, err := ValidateCallerToken(tok, testIssuer, resolverFor(key), now)
	if err != nil {
		t.Fatal(err)
	}
	if caller.Subject() != "svc:agents-server" {
		t.Fatalf("subject = %q", caller.Subject())
	}
	got := caller.Namespaces()
	if len(got) != 2 || got[0].String() != "events" || got[1].String() != "agents/x" {
		t.Fatalf("namespaces = %v", got)
	}
}

// TestCallerMayLink pins the whole-segment prefix semantics TB3 enforces
// with: "events" covers itself and its subtree, never a sibling that merely
// shares the byte prefix.
func TestCallerMayLink(t *testing.T) {
	key := testJWSKey(t)
	now := time.Unix(10_000, 0)
	tok := mintCaller(t, key, "u:1", []string{"events", "agents/x"}, now, time.Hour)
	caller, err := ValidateCallerToken(tok, testIssuer, resolverFor(key), now)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		path string
		want bool
	}{
		{"events", true},
		{"events/a", true},
		{"events/a/b", true},
		{"eventsx", false},
		{"agents/x", true},
		{"agents/x/inbox", true},
		{"agents/xy", false},
		{"agents", false},
		{"other", false},
	}
	for _, c := range cases {
		if got := caller.MayLink(mustPath(t, c.path)); got != c.want {
			t.Errorf("MayLink(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

func TestCallerTokenMintRejectsBadInput(t *testing.T) {
	key := testJWSKey(t)
	now := time.Unix(10_000, 0)
	if _, err := GenerateCallerToken(key, testIssuer, "", nil, now, time.Hour, rand.Reader); err == nil {
		t.Fatal("empty subject minted")
	}
	if _, err := GenerateCallerToken(key, testIssuer, "u:1", []string{"../evil"}, now, time.Hour, rand.Reader); err == nil {
		t.Fatal("traversal namespace minted")
	}
}

func TestCallerTokenValidation(t *testing.T) {
	key := testJWSKey(t)
	now := time.Unix(100_000, 0)
	keyFor := resolverFor(key)

	valid := mintCaller(t, key, "u:1", []string{"events"}, now, time.Minute)

	t.Run("wrong issuer", func(t *testing.T) {
		if _, err := ValidateCallerToken(valid, "http://other/", keyFor, now); err == nil {
			t.Fatal("accepted")
		}
	})
	t.Run("expired beyond skew", func(t *testing.T) {
		if _, err := ValidateCallerToken(valid, testIssuer, keyFor, now.Add(time.Minute+callerClockSkew+time.Second)); err == nil {
			t.Fatal("accepted")
		}
	})
	t.Run("expired within skew accepted", func(t *testing.T) {
		if _, err := ValidateCallerToken(valid, testIssuer, keyFor, now.Add(time.Minute+callerClockSkew-time.Second)); err != nil {
			t.Fatalf("rejected: %v", err)
		}
	})
	t.Run("issued in the future beyond skew", func(t *testing.T) {
		future := mintCaller(t, key, "u:1", nil, now.Add(10*time.Minute), time.Hour)
		if _, err := ValidateCallerToken(future, testIssuer, keyFor, now); err == nil {
			t.Fatal("accepted")
		}
	})
	t.Run("unknown kid", func(t *testing.T) {
		other := testJWSKey(t)
		tok := mintCaller(t, other, "u:1", nil, now, time.Hour)
		if _, err := ValidateCallerToken(tok, testIssuer, keyFor, now); err == nil {
			t.Fatal("accepted")
		}
	})
	t.Run("wrong typ", func(t *testing.T) {
		payload, _ := json.Marshal(callerClaims{Iss: testIssuer, Sub: "u:1", Exp: now.Add(time.Hour).Unix(), Iat: now.Unix()})
		tok, err := SignCompactJWS(key, "application/wake+jwt", payload)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ValidateCallerToken(tok, testIssuer, keyFor, now); err == nil {
			t.Fatal("accepted")
		}
	})
	t.Run("unknown claim rejected", func(t *testing.T) {
		raw := `{"iss":"` + testIssuer + `","sub":"u:1","exp":` +
			jsonInt(now.Add(time.Hour).Unix()) + `,"iat":` + jsonInt(now.Unix()) + `,"admin":true}`
		tok, err := SignCompactJWS(key, CallerTokenTyp, []byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ValidateCallerToken(tok, testIssuer, keyFor, now); err == nil {
			t.Fatal("accepted a token with an unknown claim")
		}
	})
	t.Run("missing exp rejected", func(t *testing.T) {
		payload, _ := json.Marshal(callerClaims{Iss: testIssuer, Sub: "u:1", Iat: now.Unix()})
		tok, err := SignCompactJWS(key, CallerTokenTyp, payload)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ValidateCallerToken(tok, testIssuer, keyFor, now); err == nil {
			t.Fatal("accepted a token with no expiry")
		}
	})
	t.Run("invalid namespace claim rejected", func(t *testing.T) {
		payload, _ := json.Marshal(callerClaims{
			Iss: testIssuer, Sub: "u:1", Ns: []string{"a/../b"},
			Exp: now.Add(time.Hour).Unix(), Iat: now.Unix(),
		})
		tok, err := SignCompactJWS(key, CallerTokenTyp, payload)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ValidateCallerToken(tok, testIssuer, keyFor, now); err == nil {
			t.Fatal("accepted a traversal namespace")
		}
	})
}

func jsonInt(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// TestCallerTokenNotInterchangeable pins the three-grammar separation: the
// caller JWS, the HMAC callback token, and the HMAC write token each fail
// the other validators (issue #123's cross-grammar collision vectors).
func TestCallerTokenNotInterchangeable(t *testing.T) {
	key := testJWSKey(t)
	hmacKey := testTokenKey(t)
	now := time.Unix(10_000, 0)
	path := mustPath(t, "events/a")

	callerTok := mintCaller(t, key, "u:1", []string{"events"}, now, time.Hour)
	cbTok, err := GenerateToken(hmacKey, "sub-1", 1, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	writeTok, err := GenerateWriteToken(hmacKey, "sub-1", 1, []auth.StreamPath{path}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	if tv := ValidateToken(hmacKey, callerTok, "sub-1", now); tv.Valid || tv.Expired {
		t.Fatal("caller token accepted as callback token")
	}
	if v := ValidateWriteToken(hmacKey, callerTok, path, now); v.Status != WriteTokenInvalid {
		t.Fatal("caller token accepted as write token")
	}
	if _, err := ValidateCallerToken(cbTok, testIssuer, resolverFor(key), now); err == nil {
		t.Fatal("callback token accepted as caller token")
	}
	if _, err := ValidateCallerToken(writeTok, testIssuer, resolverFor(key), now); err == nil {
		t.Fatal("write token accepted as caller token")
	}
}

// TestCallerTokenTamperProperty: any single-byte corruption of a valid caller
// token is rejected.
func TestCallerTokenTamperProperty(t *testing.T) {
	key := testJWSKey(t)
	now := time.Unix(10_000, 0)
	rapid.Check(t, func(t *rapid.T) {
		sub := rapid.StringMatching(`[a-z]{1,8}:[a-z0-9-]{1,12}`).Draw(t, "sub")
		ns := rapid.SliceOfN(rapid.StringMatching(`[a-z0-9]{1,8}(/[a-z0-9]{1,8})?`), 0, 3).Draw(t, "ns")
		tok, err := GenerateCallerToken(key, testIssuer, sub, ns, now, time.Hour, rand.Reader)
		if err != nil {
			t.Fatalf("mint: %v", err)
		}

		caller, verr := ValidateCallerToken(tok, testIssuer, resolverFor(key), now)
		if verr != nil || caller.Subject() != sub {
			t.Fatalf("round trip: %v (subject %q)", verr, caller.Subject())
		}

		idx := rapid.IntRange(0, len(tok)-1).Draw(t, "idx")
		delta := byte(rapid.IntRange(1, 255).Draw(t, "delta"))
		mut := []byte(tok)
		mut[idx] ^= delta
		if string(mut) == tok {
			return
		}
		if got, err := ValidateCallerToken(string(mut), testIssuer, resolverFor(key), now); err == nil {
			t.Fatalf("corrupted token accepted (idx %d): subject %q", idx, got.Subject())
		}
	})
}

// TestVerifiedCallerZeroValueGrantsNothing: the zero VerifiedCaller (what
// every error path returns) has no subject and can link nothing.
func TestVerifiedCallerZeroValueGrantsNothing(t *testing.T) {
	var c VerifiedCaller
	if c.Subject() != "" || len(c.Namespaces()) != 0 {
		t.Fatalf("zero caller = %q %v", c.Subject(), c.Namespaces())
	}
	if c.MayLink(mustPath(t, "events/a")) {
		t.Fatal("zero caller may link")
	}
}
