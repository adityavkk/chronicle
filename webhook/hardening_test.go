package webhook

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// Regression tests for the #126 post-merge security review. Each pins one
// confirmed-live finding in the webhook token/key layer.

// TestWriteTokenEmptyKeyFailsClosed is finding F2 on the write-token path: an
// empty or short HMAC key must authorize nothing (a degenerate key is
// publicly forgeable), not validate a token minted with that same key.
func TestWriteTokenEmptyKeyFailsClosed(t *testing.T) {
	path := mustPath(t, "events/a")
	now := time.Unix(1000, 0)
	scope := []auth.StreamPath{path}

	for _, key := range [][]byte{nil, {}, make([]byte, 16), make([]byte, 31)} {
		// A token minted with the degenerate key (what an attacker who knows
		// the key is degenerate would forge) must still be rejected.
		tok, err := GenerateWriteToken(key, "sub-1", 1, scope, now, time.Hour, rand.Reader)
		if err != nil {
			t.Fatalf("mint with len(key)=%d: %v", len(key), err)
		}
		if v := ValidateWriteToken(key, tok, path, now); v.Status != WriteTokenInvalid {
			t.Fatalf("len(key)=%d: status = %v, want invalid (fail closed)", len(key), v.Status)
		}
		if d := NewWriteTokenAuthorizer(key).AuthorizeAppend(tok, path, now); d.Allowed() {
			t.Fatalf("len(key)=%d: AuthorizeAppend allowed with a degenerate key", len(key))
		}
	}

	// A full-length key still works — the guard is a floor, not a wall.
	good := make([]byte, minTokenKeyBytes)
	if _, err := rand.Read(good); err != nil {
		t.Fatal(err)
	}
	tok, err := GenerateWriteToken(good, "sub-1", 1, scope, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if v := ValidateWriteToken(good, tok, path, now); v.Status != WriteTokenValid {
		t.Fatalf("healthy key: status = %v, want valid", v.Status)
	}
}

// TestCallbackTokenEmptyKeyFailsClosed is finding F2 on the callback/claim
// token path: the same degenerate-key guard.
func TestCallbackTokenEmptyKeyFailsClosed(t *testing.T) {
	now := time.Unix(1000, 0)
	for _, key := range [][]byte{nil, {}, make([]byte, 16)} {
		tok, err := GenerateToken(key, "sub-1", 1, now, time.Hour, rand.Reader)
		if err != nil {
			t.Fatalf("mint with len(key)=%d: %v", len(key), err)
		}
		if tv := ValidateToken(key, tok, "sub-1", now); tv.Valid || tv.Expired {
			t.Fatalf("len(key)=%d: token validated against a degenerate key: %+v", len(key), tv)
		}
	}
}

// TestStrictJSONUnmarshalRejectsTrailing is findings F4/F8: json.Decoder.More()
// accepts a trailing '}' or ']'; the strict helper (a second Decode expecting
// io.EOF) rejects every trailing byte.
func TestStrictJSONUnmarshalRejectsTrailing(t *testing.T) {
	type doc struct {
		A int `json:"a"`
	}
	accept := []string{`{"a":1}`, "{\"a\":1}\n", `{"a":1}   `}
	reject := []string{`{"a":1}}`, `{"a":1}]`, `{"a":1}{"b":2}`, `{"a":1}garbage`, `{"a":1},`, `{"a":1} {}`}

	for _, in := range accept {
		var d doc
		if err := strictJSONUnmarshal([]byte(in), &d); err != nil {
			t.Errorf("strictJSONUnmarshal(%q) = %v, want accept", in, err)
		}
	}
	for _, in := range reject {
		var d doc
		if err := strictJSONUnmarshal([]byte(in), &d); err == nil {
			t.Errorf("strictJSONUnmarshal(%q) accepted trailing data, want reject", in)
		}
	}
	// Unknown fields are still rejected (DisallowUnknownFields preserved).
	var d doc
	if err := strictJSONUnmarshal([]byte(`{"a":1,"b":2}`), &d); err == nil {
		t.Error("unknown field must be rejected")
	}
}

// TestCallerTokenRejectsTrailingBrace is F4/F8 at the token layer: even a
// correctly-signed JWS whose payload carries a trailing '}' — the exact case
// More() missed — is rejected by the strict claim decode. Signing arbitrary
// payload bytes proves the parser, not the signature, is what rejects it.
func TestCallerTokenRejectsTrailingBrace(t *testing.T) {
	key := testJWSKey(t)
	now := time.Unix(1000, 0)
	iss := "http://x/v1/stream/"
	payload := []byte(`{"iss":"` + iss + `","sub":"u:1","ns":[],"exp":9999999999,"iat":1,"jti":"x"}}`)
	tok, err := SignCompactJWS(key, CallerTokenTyp, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateCallerToken(tok, iss, StaticKidResolver(key)(now), now); err == nil {
		t.Fatal("a signed caller token with a trailing '}' in its payload must be rejected")
	}
	// The same claims WITHOUT the trailing brace verify — proving it was the
	// trailing byte, not the claims, that was rejected above.
	clean := []byte(`{"iss":"` + iss + `","sub":"u:1","ns":[],"exp":9999999999,"iat":1,"jti":"x"}`)
	tok2, err := SignCompactJWS(key, CallerTokenTyp, clean)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateCallerToken(tok2, iss, StaticKidResolver(key)(now), now); err != nil {
		t.Fatalf("clean caller token must verify: %v", err)
	}
}

// TestFileKeySourceReturnsDeepCopies is finding F10: the FileKeySource
// accessors promise defensive copies; mutating a returned key's bytes must
// not corrupt the source or a later reader.
func TestFileKeySourceReturnsDeepCopies(t *testing.T) {
	path := writeKeysFile(t, validKeysFile())
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	src, err := LoadFileKeySource(path)
	if err != nil {
		t.Fatal(err)
	}

	k1, _ := src.LoadSigningKey(time.Now())
	orig := k1.Private[0]
	k1.Private[0] ^= 0xff
	k2, _ := src.LoadSigningKey(time.Now())
	if k2.Private[0] != orig {
		t.Fatal("LoadSigningKey aliases internal Private bytes (F10)")
	}

	ks, _ := src.SigningKeys()
	ks[0].Public[0] ^= 0xff
	ks2, _ := src.SigningKeys()
	if ks2[0].Public[0] == ks[0].Public[0] {
		t.Fatal("SigningKeys aliases internal Public bytes (F10)")
	}

	tk := src.mustTokenKey(t)
	tk[0] ^= 0xff
	tk2 := src.mustTokenKey(t)
	if tk2[0] == tk[0] {
		t.Fatal("LoadTokenKey aliases internal token-key bytes")
	}
}

func (s *FileKeySource) mustTokenKey(t *testing.T) []byte {
	t.Helper()
	k, err := s.LoadTokenKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// TestKeysFileRejectsTrailingBrace is F8 end-to-end: a valid keys file with a
// trailing '}' refuses to load (fail-closed custody).
func TestKeysFileRejectsTrailingBrace(t *testing.T) {
	content := validKeysFile() + "}"
	path := filepath.Join(t.TempDir(), "keys.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFileKeySource(path); err == nil {
		t.Fatal("a keys file with a trailing '}' must be refused")
	}
}
