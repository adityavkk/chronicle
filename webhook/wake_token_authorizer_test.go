package webhook

import (
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

func mintWake(t *testing.T, key SigningKey, sub, aud string, now time.Time, ttl time.Duration) string {
	t.Helper()
	claims, err := NewWakeTokenClaims("http://x/v1/stream/", sub, aud, 7, "w_1", now, ttl, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := MintWakeToken(key, claims)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// TestAuthorizeEntitySubtreeScope pins the agent scope predicate: an entity
// covers itself and its descendants, never a sibling, a string-prefix
// near-miss, or an ancestor.
func TestAuthorizeEntitySubtreeScope(t *testing.T) {
	wakeKey := testJWSKey(t)
	now := time.Unix(50_000, 0)
	az := NewWakeTokenAuthorizer("", fixedKeys(wakeKey))
	tok := mintWake(t, wakeKey, "agents/a", "", now, time.Minute)

	covers := map[string]bool{
		"agents/a":        true,
		"agents/a/inbox":  true,
		"agents/a/x/deep": true,
		"agents/a2":       false, // string prefix, not a segment
		"agents":          false, // ancestor
		"agents/b":        false, // sibling
		"agents/b/inbox":  false,
		"other/agents/a":  false,
		"agents/a-suffix": false,
	}
	for p, want := range covers {
		d := az.AuthorizeEntity(tok, mustPath(t, p), now)
		if d.Allowed() != want {
			t.Errorf("entity agents/a at %q: allowed=%v, want %v (detail %q)", p, d.Allowed(), want, d.Detail())
		}
		if !want && d.Reason() != auth.ReasonForbidden {
			t.Errorf("out-of-scope %q: reason=%v, want forbidden (verified credential)", p, d.Reason())
		}
	}
}

func TestAuthorizeEntityRejections(t *testing.T) {
	wakeKey := testJWSKey(t)
	webhookKey := testJWSKey(t)
	now := time.Unix(50_000, 0)
	path := mustPath(t, "agents/a/inbox")
	az := NewWakeTokenAuthorizer("", fixedKeys(wakeKey))

	good := mintWake(t, wakeKey, "agents/a", "", now, time.Minute)

	cases := map[string]struct {
		az  WakeTokenAuthorizer
		tok string
		at  time.Time
	}{
		"empty token":           {az, "", now},
		"garbage":               {az, "junk", now},
		"tampered":              {az, good[:len(good)-3] + "zzz", now},
		"expired past skew":     {az, good, now.Add(time.Minute + 6*time.Second)},
		"webhook-key mint":      {az, mintWake(t, webhookKey, "agents/a", "", now, time.Minute), now},
		"aud minted, gate bare": {az, mintWake(t, wakeKey, "agents/a", "https://gw", now, time.Minute), now},
		"bare minted, gate aud": {NewWakeTokenAuthorizer("https://gw", fixedKeys(wakeKey)), good, now},
		"traversal subject":     {az, mintWake(t, wakeKey, "../evil", "", now, time.Minute), now},
		"keys unavailable": {NewWakeTokenAuthorizer("", func() ([]SigningKey, error) {
			return nil, errors.New("store down")
		}), good, now},
	}
	for name, c := range cases {
		d := c.az.AuthorizeEntity(c.tok, path, c.at)
		if d.Allowed() {
			t.Errorf("%s: allowed", name)
			continue
		}
		if d.Reason() != auth.ReasonUnauthenticated {
			t.Errorf("%s: reason=%v, want unauthenticated", name, d.Reason())
		}
	}

	// Inside the tight gate skew the token still verifies.
	if d := az.AuthorizeEntity(good, path, now.Add(time.Minute+4*time.Second)); !d.Allowed() {
		t.Fatalf("within gate skew: %s", d.Detail())
	}

	// The matching-audience control for the aud cases above.
	gw := NewWakeTokenAuthorizer("https://gw", fixedKeys(wakeKey))
	if d := gw.AuthorizeEntity(mintWake(t, wakeKey, "agents/a", "https://gw", now, time.Minute), path, now); !d.Allowed() {
		t.Fatalf("matching audience denied: %s", d.Detail())
	}
}

// TestAuthorizeEntityCrossFamily: no other credential grammar authorizes on
// the entity arm, and the wake_token authorizes on no other arm.
func TestAuthorizeEntityCrossFamily(t *testing.T) {
	wakeKey := testJWSKey(t)
	signKey := testJWSKey(t)
	now := time.Unix(50_000, 0)
	path := mustPath(t, "agents/a/inbox")
	az := NewWakeTokenAuthorizer("", fixedKeys(wakeKey))

	readCap, err := GenerateReadCapability(signKey, testReadIss, "r", []string{"agents/a"}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	callerTok, err := GenerateCallerToken(signKey, testReadIss, "c", []string{"agents/a"}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wtok, err := GenerateWriteToken(testTokenKey(t), "s", 1, []auth.StreamPath{path}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for name, tok := range map[string]string{
		"read capability": readCap, "caller token": callerTok, "write token": wtok,
	} {
		if d := az.AuthorizeEntity(tok, path, now); d.Allowed() {
			t.Errorf("%s authorized on the entity arm", name)
		}
	}

	wake := mintWake(t, wakeKey, "agents/a", "", now, time.Minute)
	if _, err := ValidateReadCapability(wake, testReadIss, resolverForKeys([]SigningKey{wakeKey}), now); err == nil {
		t.Error("wake token accepted as read capability")
	}
	if _, err := ValidateCallerToken(wake, testReadIss, resolverForKeys([]SigningKey{wakeKey}), now); err == nil {
		t.Error("wake token accepted as caller token")
	}
}

func TestPeekTyp(t *testing.T) {
	wakeKey := testJWSKey(t)
	now := time.Unix(50_000, 0)
	wake := mintWake(t, wakeKey, "agents/a", "", now, time.Minute)
	if typ, ok := PeekTyp(wake); !ok || typ != WakeTokenTyp {
		t.Fatalf("PeekTyp(wake) = (%q,%v)", typ, ok)
	}
	readCap, err := GenerateReadCapability(wakeKey, testReadIss, "r", []string{"x"}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if typ, ok := PeekTyp(readCap); !ok || typ != ReadCapabilityTyp {
		t.Fatalf("PeekTyp(readCap) = (%q,%v)", typ, ok)
	}
	wtok, err := GenerateWriteToken(testTokenKey(t), "s", 1, []auth.StreamPath{mustPath(t, "x")}, now, time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]string{"opaque write token": wtok, "garbage": "a.b", "empty": ""} {
		if _, ok := PeekTyp(bad); ok {
			t.Errorf("%s: PeekTyp accepted", name)
		}
	}
}

// TestEntitySubtreeProperty: for any well-formed entity path, the scope
// covers exactly {entity} ∪ {entity/<anything>} and never a mutated sibling.
func TestEntitySubtreeProperty(t *testing.T) {
	wakeKey := testJWSKey(t)
	now := time.Unix(50_000, 0)
	az := NewWakeTokenAuthorizer("", fixedKeys(wakeKey))
	rapid.Check(t, func(t *rapid.T) {
		entity := rapid.StringMatching(`[a-z0-9]{1,8}(/[a-z0-9]{1,8}){0,2}`).Draw(t, "entity")
		tok := mintWake2(t, wakeKey, entity, now)

		if d := az.AuthorizeEntity(tok, mustPathR(t, entity), now); !d.Allowed() {
			t.Fatalf("entity %q does not cover itself", entity)
		}
		child := entity + "/" + rapid.StringMatching(`[a-z0-9]{1,8}`).Draw(t, "child")
		if d := az.AuthorizeEntity(tok, mustPathR(t, child), now); !d.Allowed() {
			t.Fatalf("entity %q does not cover child %q", entity, child)
		}
		sibling := entity + rapid.StringMatching(`[a-z0-9]{1,4}`).Draw(t, "suffix")
		if d := az.AuthorizeEntity(tok, mustPathR(t, sibling), now); d.Allowed() {
			t.Fatalf("entity %q covers string-prefix sibling %q", entity, sibling)
		}
	})
}

// mintWake2 is the property-test mint: rapid's *rapid.T is not a testing.TB,
// so this variant takes anything with Fatal.
func mintWake2(tb interface {
	Fatal(...any)
}, key SigningKey, sub string, now time.Time,
) string {
	claims, err := NewWakeTokenClaims("http://x/v1/stream/", sub, "", 1, "w_1", now, time.Minute, rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	tok, err := MintWakeToken(key, claims)
	if err != nil {
		tb.Fatal(err)
	}
	return tok
}

func mustPathR(t *rapid.T, s string) auth.StreamPath {
	p, err := auth.NormalizeStreamPath(s)
	if err != nil {
		t.Fatalf("normalize %q: %v", s, err)
	}
	return p
}
