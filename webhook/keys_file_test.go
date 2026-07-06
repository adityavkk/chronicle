package webhook

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

func b64Seed(fill byte) string {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = fill
	}
	return base64.RawURLEncoding.EncodeToString(seed)
}

func b64TokenKey(n int) string {
	key := make([]byte, n)
	for i := range key {
		key[i] = byte(i)
	}
	return base64.RawURLEncoding.EncodeToString(key)
}

func writeKeysFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "chronicle-keys.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func validKeysFile() string {
	return fmt.Sprintf(`{
		"token_key": %q,
		"signing_keys": [
			{"seed": %q, "created_at": "2026-07-01T00:00:00Z", "status": "active", "purpose": "webhook"}
		]
	}`, b64TokenKey(32), b64Seed(1))
}

func TestLoadFileKeySourceValid(t *testing.T) {
	src, err := LoadFileKeySource(writeKeysFile(t, validKeysFile()))
	if err != nil {
		t.Fatal(err)
	}
	active, err := src.LoadSigningKey(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if active.Status != keyStatusActive || !strings.HasPrefix(active.Kid, "ds_") {
		t.Fatalf("active = %q status %q", active.Kid, active.Status)
	}
	// kid is derived, deterministic, and matches the JWK derivation.
	if got := deriveKid(active.Public); got != active.Kid {
		t.Fatalf("kid %q != derived %q", active.Kid, got)
	}
	tokenKey, err := src.LoadTokenKey()
	if err != nil {
		t.Fatal(err)
	}
	if len(tokenKey) != 32 {
		t.Fatalf("token key = %d bytes", len(tokenKey))
	}
	keys, err := src.SigningKeys()
	if err != nil || len(keys) != 1 || keys[0].Kid != active.Kid {
		t.Fatalf("SigningKeys = %v (err %v)", keys, err)
	}
	if len(src.Warnings()) != 0 {
		t.Fatalf("0600 file must warn nothing, got %v", src.Warnings())
	}
	// The signing key signs and its published JWK verifies (usable material).
	sig := SignWebhookPayload(active, []byte("body"), time.Unix(2000, 0))
	if !strings.Contains(sig, active.Kid) {
		t.Fatalf("signature %q missing kid", sig)
	}
}

func TestLoadFileKeySourceActivePlusRetiring(t *testing.T) {
	// Retiring listed FIRST in the file; the source must still put active first.
	content := fmt.Sprintf(`{
		"token_key": %q,
		"signing_keys": [
			{"seed": %q, "created_at": "2026-01-01T00:00:00Z", "status": "retiring", "purpose": "webhook"},
			{"seed": %q, "created_at": "2026-07-01T00:00:00Z", "status": "active", "purpose": "webhook"}
		]
	}`, b64TokenKey(32), b64Seed(2), b64Seed(3))
	src, err := LoadFileKeySource(writeKeysFile(t, content))
	if err != nil {
		t.Fatal(err)
	}
	keys, err := src.SigningKeys()
	if err != nil || len(keys) != 2 {
		t.Fatalf("SigningKeys = %d keys (err %v)", len(keys), err)
	}
	if keys[0].Status != keyStatusActive || keys[1].Status != keyStatusRetiring {
		t.Fatalf("order = %q,%q; active must be first", keys[0].Status, keys[1].Status)
	}
	active, _ := src.LoadSigningKey(time.Now())
	if active.Kid != keys[0].Kid {
		t.Fatal("LoadSigningKey must return the active key")
	}
	if len(src.Kids()) != 2 || !strings.Contains(src.Kids()[0], "(active)") {
		t.Fatalf("Kids() = %v", src.Kids())
	}
}

func TestLoadFileKeySourceRejects(t *testing.T) {
	seed, tok := b64Seed(1), b64TokenKey(32)
	entry := func(seed, status, purpose string) string {
		return fmt.Sprintf(`{"seed": %q, "created_at": "2026-07-01T00:00:00Z", "status": %q, "purpose": %q}`, seed, status, purpose)
	}
	doc := func(tokenKey, entries string) string {
		return fmt.Sprintf(`{"token_key": %q, "signing_keys": [%s]}`, tokenKey, entries)
	}

	cases := map[string]string{
		"no active":         doc(tok, entry(seed, "retiring", "webhook")),
		"two active":        doc(tok, entry(b64Seed(1), "active", "webhook")+","+entry(b64Seed(2), "active", "webhook")),
		"short seed":        doc(tok, entry(base64.RawURLEncoding.EncodeToString(make([]byte, 16)), "active", "webhook")),
		"bad seed base64":   doc(tok, entry("!!not-base64!!", "active", "webhook")),
		"padded seed":       doc(tok, entry(b64Seed(1)+"==", "active", "webhook")),
		"short token key":   doc(b64TokenKey(16), entry(seed, "active", "webhook")),
		"bad token base64":  doc("!!nope!!", entry(seed, "active", "webhook")),
		"missing token key": fmt.Sprintf(`{"signing_keys": [%s]}`, entry(seed, "active", "webhook")),
		"unknown purpose":   doc(tok, entry(seed, "active", "wake")),
		"unknown status":    doc(tok, entry(seed, "revoked", "webhook")),
		"bad created_at":    doc(tok, `{"seed": "`+seed+`", "created_at": "yesterday", "status": "active", "purpose": "webhook"}`),
		"empty keys":        doc(tok, ""),
		"unknown top field": `{"token_key": "` + tok + `", "signing_keys": [` + entry(seed, "active", "webhook") + `], "kid": "ds_x"}`,
		"unknown key field": doc(tok, `{"seed": "`+seed+`", "created_at": "2026-07-01T00:00:00Z", "status": "active", "purpose": "webhook", "kid": "ds_x"}`),
		"trailing garbage":  doc(tok, entry(seed, "active", "webhook")) + `{"more": true}`,
		"not json":          `token_key=abc`,
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := LoadFileKeySource(writeKeysFile(t, content))
			if err == nil {
				t.Fatal("loaded; want fail-closed error")
			}
			// Errors must never echo key material.
			if strings.Contains(err.Error(), seed) || strings.Contains(err.Error(), tok) {
				t.Fatalf("error leaks key material: %v", err)
			}
		})
	}

	if _, err := LoadFileKeySource(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Fatal("missing file must fail")
	}
}

func TestLoadFileKeySourceWarnsOnBroadPermissions(t *testing.T) {
	path := writeKeysFile(t, validKeysFile())
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := LoadFileKeySource(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(src.Warnings()) == 0 {
		t.Fatal("0644 keys file must produce a permissions warning")
	}
	for _, w := range src.Warnings() {
		if strings.Contains(w, b64Seed(1)) || strings.Contains(w, b64TokenKey(32)) {
			t.Fatalf("warning leaks material: %q", w)
		}
	}
}

// TestFileKeySourceProperty: any structurally valid file loads; the derived
// kid is stable across loads; the loaded material is usable (webhook
// signatures verify, HMAC tokens round-trip); and mutating the returned
// slices cannot corrupt the source.
func TestFileKeySourceProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		nRetiring := rapid.IntRange(0, 3).Draw(t, "retiring")
		tokenLen := rapid.IntRange(32, 64).Draw(t, "tokenLen")

		tokenKey := rapid.SliceOfN(rapid.Byte(), tokenLen, tokenLen).Draw(t, "tokenKey")
		entries := make([]map[string]string, 0, nRetiring+1)
		mk := func(status string, seed []byte) map[string]string {
			return map[string]string{
				"seed":       base64.RawURLEncoding.EncodeToString(seed),
				"created_at": time.Unix(rapid.Int64Range(0, 1<<32).Draw(t, "at"), 0).UTC().Format(time.RFC3339),
				"status":     status,
				"purpose":    "webhook",
			}
		}
		activeSeed := rapid.SliceOfN(rapid.Byte(), 32, 32).Draw(t, "activeSeed")
		entries = append(entries, mk("active", activeSeed))
		for i := 0; i < nRetiring; i++ {
			seed := rapid.SliceOfN(rapid.Byte(), 32, 32).Draw(t, fmt.Sprintf("seed%d", i))
			entries = append(entries, mk("retiring", seed))
		}

		docBytes, err := json.Marshal(map[string]any{
			"token_key":    base64.RawURLEncoding.EncodeToString(tokenKey),
			"signing_keys": entries,
		})
		if err != nil {
			t.Fatal(err)
		}
		dir := os.TempDir()
		f, err := os.CreateTemp(dir, "keysprop-*.json")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		if _, err := f.Write(docBytes); err != nil {
			t.Fatal(err)
		}
		f.Close()

		src1, err := LoadFileKeySource(f.Name())
		if err != nil {
			t.Fatalf("valid doc rejected: %v", err)
		}
		src2, err := LoadFileKeySource(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		k1, _ := src1.LoadSigningKey(time.Now())
		k2, _ := src2.LoadSigningKey(time.Now())
		if k1.Kid != k2.Kid {
			t.Fatalf("kid unstable: %q vs %q", k1.Kid, k2.Kid)
		}

		// HMAC token key round-trips through the real token machinery.
		fileTok, _ := src1.LoadTokenKey()
		minted, err := GenerateToken(fileTok, "s1", 1, time.Unix(5000, 0), time.Hour, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if tv := ValidateToken(fileTok, minted, "s1", time.Unix(5000, 0)); !tv.Valid {
			t.Fatal("token minted under file key does not validate")
		}

		// Returned slices are copies: scribbling on them must not corrupt src.
		keys, _ := src1.SigningKeys()
		if len(keys) != nRetiring+1 {
			t.Fatalf("SigningKeys = %d, want %d", len(keys), nRetiring+1)
		}
		keys[0].Kid = "ds_corrupted"
		fileTok[0] ^= 0xFF
		again, _ := src1.SigningKeys()
		tokAgain, _ := src1.LoadTokenKey()
		if again[0].Kid == "ds_corrupted" || tokAgain[0] == fileTok[0] {
			t.Fatal("FileKeySource state is aliased to returned slices")
		}
	})
}

// TestManagerWithFileKeySource is the custody integration proof: a Manager
// booted from a keys file mints and validates with the file's material,
// serves exactly the file's kids in its JWKS, writes NO key material to
// Redis, survives a restart with a stable kid, and ignores (without
// touching) any legacy key material already persisted in Redis.
func TestManagerWithFileKeySource(t *testing.T) {
	store, client := newTestStore(t)
	path := writeKeysFile(t, validKeysFile())
	src, err := LoadFileKeySource(path)
	if err != nil {
		t.Fatal(err)
	}
	fileKid := func() string { k, _ := src.LoadSigningKey(time.Now()); return k.Kid }()
	fileTokenKey, _ := src.LoadTokenKey()

	fs := &fakeStreams{tails: map[string]string{}}
	mgr, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Keys: src})
	if err != nil {
		t.Fatal(err)
	}

	// JWKS: the webhook-envelope family is exactly the file's kid — no
	// Redis-persisted webhook key may appear when a file is the custody
	// source. The wake-token family (#123/TB6a) is a separate key that still
	// persists in Redis until its file custody lands with the Wave-2 rotation
	// work, so the set is exactly {file webhook kid, wake kid}.
	jwks, err := mgr.JWKS()
	if err != nil || len(jwks.Keys) != 2 || jwks.Keys[0].Kid != fileKid {
		t.Fatalf("JWKS = %+v (err %v), want [%s, <wake kid>]", jwks, err, fileKid)
	}
	if jwks.Keys[1].Kid != mgr.wakeKey.Kid || jwks.Keys[1].Kid == fileKid {
		t.Fatalf("JWKS second key = %s, want the distinct wake kid %s", jwks.Keys[1].Kid, mgr.wakeKey.Kid)
	}

	// The claim/callback and write-token paths run on the file's HMAC key:
	// a token minted with the file key validates via the manager, both ways.
	now := time.Now()
	cb, err := GenerateToken(fileTokenKey, "s1", 1, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if tv := ValidateToken(mgr.tokenKey, cb, "s1", now); !tv.Valid {
		t.Fatal("callback token minted under the file key must validate in the manager")
	}
	wt, err := GenerateWriteToken(fileTokenKey, "s1", 1, []auth.StreamPath{mustPath(t, "events/a")}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if d := mgr.WriteAuthorizer().AuthorizeAppend(wt, mustPath(t, "events/a"), now); !d.Allowed() {
		t.Fatalf("write token minted under the file key must authorize via the manager: %s", d.Detail())
	}
	if d := mgr.WriteAuthorizer().AuthorizeAppend(cb, mustPath(t, "events/a"), now); d.Allowed() {
		t.Fatal("callback token must not authorize appends (cross-family)")
	}

	// Custody proof: booting from the file wrote NOTHING to Redis.
	n, err := client.Exists(t.Context(), jwksKey, activeKidKey, tokenKeyKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("file-sourced boot persisted %d key artifacts in Redis", n)
	}

	// Restart: a new Manager over the same file has the same kid and still
	// validates tokens minted before the restart.
	mgr2, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Keys: src})
	if err != nil {
		t.Fatal(err)
	}
	if tv := ValidateToken(mgr2.tokenKey, cb, "s1", now); !tv.Valid {
		t.Fatal("pre-restart token must validate after restart")
	}
	jwks2, _ := mgr2.JWKS()
	if len(jwks2.Keys) != 2 || jwks2.Keys[0].Kid != fileKid {
		t.Fatal("kid must be stable across restart")
	}

	// Legacy state: a Redis-custody manager (no Keys) installs Redis keys;
	// a file-sourced manager afterwards still serves the file kid only and
	// leaves the Redis material byte-identical.
	legacy, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/"})
	if err != nil {
		t.Fatal(err)
	}
	legacyJWKS, _ := legacy.JWKS()
	if len(legacyJWKS.Keys) != 2 || legacyJWKS.Keys[0].Kid == fileKid {
		t.Fatalf("legacy manager JWKS = %+v; must be a distinct Redis-installed kid", legacyJWKS)
	}
	beforeHash, _ := client.HGetAll(t.Context(), jwksKey).Result()
	beforeTok, _ := client.Get(t.Context(), tokenKeyKey).Result()

	mgr3, err := NewManager(store, fs, ManagerOptions{StreamRootURL: "http://x/v1/stream/", Keys: src})
	if err != nil {
		t.Fatal(err)
	}
	jwks3, _ := mgr3.JWKS()
	if len(jwks3.Keys) != 2 || jwks3.Keys[0].Kid != fileKid {
		t.Fatalf("file source must win over legacy Redis keys, got %+v", jwks3)
	}
	afterHash, _ := client.HGetAll(t.Context(), jwksKey).Result()
	afterTok, _ := client.Get(t.Context(), tokenKeyKey).Result()
	if len(afterHash) != len(beforeHash) || afterTok != beforeTok {
		t.Fatal("file-sourced boot mutated legacy Redis key material")
	}
	for kid, material := range beforeHash {
		if afterHash[kid] != material {
			t.Fatal("file-sourced boot rewrote a legacy Redis key entry")
		}
	}
}
