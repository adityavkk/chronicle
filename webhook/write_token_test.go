package webhook

import (
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

func mustPath(t testing.TB, s string) auth.StreamPath {
	t.Helper()
	p, err := auth.NormalizeStreamPath(s)
	if err != nil {
		t.Fatalf("normalize %q: %v", s, err)
	}
	return p
}

func testTokenKey(t testing.TB) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestWriteTokenRoundTrip(t *testing.T) {
	key := testTokenKey(t)
	now := time.Unix(1000, 0)
	scope := []auth.StreamPath{mustPath(t, "events/a"), mustPath(t, "agents/x/inbox")}

	tok, err := GenerateWriteToken(key, "sub-1", 7, scope, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range scope {
		v := ValidateWriteToken(key, tok, p, now)
		if v.Status != WriteTokenValid {
			t.Fatalf("status for %q = %v, want valid", p.String(), v.Status)
		}
		if v.SubID != "sub-1" || v.Generation != 7 {
			t.Fatalf("attribution = %q gen %d, want sub-1 gen 7", v.SubID, v.Generation)
		}
	}
}

func TestWriteTokenWrongPath(t *testing.T) {
	key := testTokenKey(t)
	now := time.Unix(1000, 0)
	tok, err := GenerateWriteToken(key, "sub-1", 1, []auth.StreamPath{mustPath(t, "events/a")}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	v := ValidateWriteToken(key, tok, mustPath(t, "events/b"), now)
	if v.Status != WriteTokenWrongPath {
		t.Fatalf("status = %v, want wrong-path", v.Status)
	}
	// A scope for "events/a" must not cover a prefix, suffix, or nested path.
	for _, p := range []string{"events", "events/a2", "events/a/nested"} {
		if v := ValidateWriteToken(key, tok, mustPath(t, p), now); v.Status != WriteTokenWrongPath {
			t.Fatalf("path %q: status = %v, want wrong-path (exact-match scope)", p, v.Status)
		}
	}
}

func TestWriteTokenExpired(t *testing.T) {
	key := testTokenKey(t)
	now := time.Unix(1000, 0)
	tok, err := GenerateWriteToken(key, "sub-1", 1, []auth.StreamPath{mustPath(t, "events/a")}, now, time.Minute, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// Valid through exp, expired after (the boundary ValidateToken shares).
	if v := ValidateWriteToken(key, tok, mustPath(t, "events/a"), now.Add(time.Minute)); v.Status != WriteTokenValid {
		t.Fatalf("at exp: status = %v, want valid", v.Status)
	}
	v := ValidateWriteToken(key, tok, mustPath(t, "events/a"), now.Add(time.Minute+time.Second))
	if v.Status != WriteTokenExpired {
		t.Fatalf("past exp: status = %v, want expired", v.Status)
	}
	if v.SubID != "sub-1" {
		t.Fatalf("expired token keeps attribution for telemetry, got %q", v.SubID)
	}
}

func TestWriteTokenGarbageInvalid(t *testing.T) {
	key := testTokenKey(t)
	now := time.Unix(1000, 0)
	path := mustPath(t, "events/a")
	tok, err := GenerateWriteToken(key, "sub-1", 1, []auth.StreamPath{path}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]string{
		"empty":        "",
		"no dot":       strings.ReplaceAll(tok, ".", ""),
		"bad base64":   "!!!." + strings.SplitN(tok, ".", 2)[1],
		"sig tampered": tok[:len(tok)-2] + "zz",
		"sig empty":    strings.SplitN(tok, ".", 2)[0] + ".",
	}
	for name, bad := range cases {
		if v := ValidateWriteToken(key, bad, path, now); v.Status != WriteTokenInvalid {
			t.Errorf("%s: status = %v, want invalid", name, v.Status)
		}
	}

	// A foreign key never validates.
	other := testTokenKey(t)
	if v := ValidateWriteToken(other, tok, path, now); v.Status != WriteTokenInvalid {
		t.Fatalf("foreign key: status = %v, want invalid", v.Status)
	}
}

// TestWriteAndCallbackTokensNotInterchangeable pins the domain separation: the
// same tokenKey signs both token families, but presenting one to the other's
// validator fails the MAC — a freely-obtained callback token can never become
// a write capability, and a write token can never drive ack/release.
func TestWriteAndCallbackTokensNotInterchangeable(t *testing.T) {
	key := testTokenKey(t)
	now := time.Unix(1000, 0)
	path := mustPath(t, "events/a")

	writeTok, err := GenerateWriteToken(key, "sub-1", 1, []auth.StreamPath{path}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cbTok, err := GenerateToken(key, "sub-1", 1, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	if tv := ValidateToken(key, writeTok, "sub-1", now); tv.Valid || tv.Expired {
		t.Fatalf("write token accepted by callback validation: %+v", tv)
	}
	if v := ValidateWriteToken(key, cbTok, path, now); v.Status != WriteTokenInvalid {
		t.Fatalf("callback token accepted by write validation: %v", v.Status)
	}
}

// TestWriteTokenProperties: any minted token validates for exactly its scope,
// under exactly its key, and any single-byte corruption invalidates it.
func TestWriteTokenProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOfN(rapid.Byte(), 16, 64).Draw(t, "key")
		subID := rapid.StringMatching(`[a-z0-9-]{1,16}`).Draw(t, "sub")
		gen := rapid.Int64Range(0, 1<<40).Draw(t, "gen")
		rawPaths := rapid.SliceOfNDistinct(
			rapid.StringMatching(`[a-z0-9]{1,8}(/[a-z0-9]{1,8}){0,2}`),
			1, 4, func(s string) string { return s },
		).Draw(t, "paths")
		ttlSec := rapid.Int64Range(1, 86400).Draw(t, "ttl")
		now := time.Unix(rapid.Int64Range(0, 1<<32).Draw(t, "now"), 0)

		scope := make([]auth.StreamPath, len(rawPaths))
		for i, s := range rawPaths {
			p, err := auth.NormalizeStreamPath(s)
			if err != nil {
				t.Fatalf("generator produced unnormalizable path %q: %v", s, err)
			}
			scope[i] = p
		}

		tok, err := GenerateWriteToken(key, subID, gen, scope, now, time.Duration(ttlSec)*time.Second, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}

		for _, p := range scope {
			v := ValidateWriteToken(key, tok, p, now)
			if v.Status != WriteTokenValid || v.SubID != subID || v.Generation != gen {
				t.Fatalf("mint/validate disagree: %+v for %q", v, p.String())
			}
		}

		outside, err := auth.NormalizeStreamPath("outside/" + rawPaths[0])
		if err != nil {
			t.Fatal(err)
		}
		if v := ValidateWriteToken(key, tok, outside, now); v.Status != WriteTokenWrongPath {
			t.Fatalf("out-of-scope path: %v, want wrong-path", v.Status)
		}

		wrongKey := append(append([]byte{}, key...), 0x01)
		if v := ValidateWriteToken(wrongKey, tok, scope[0], now); v.Status != WriteTokenInvalid {
			t.Fatalf("wrong key: %v, want invalid", v.Status)
		}

		// Corrupt one byte anywhere in the token: never valid afterward. (A
		// corrupted body fails the MAC; a corrupted sig fails the compare.)
		idx := rapid.IntRange(0, len(tok)-1).Draw(t, "idx")
		corrupted := []byte(tok)
		corrupted[idx] ^= 0x01
		if v := ValidateWriteToken(key, string(corrupted), scope[0], now); v.Status == WriteTokenValid {
			t.Fatalf("corrupted token at %d still valid", idx)
		}
	})
}
