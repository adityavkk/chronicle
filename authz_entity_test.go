package chronicle

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// The TB6b acceptance suite (issue #126): a woken entity's wake_token is an
// agent principal scoped to exactly its own entity subtree — entity A acts
// on A's streams, never on B's, and never creates or destroys anything.

// tb6bHandler is an enforce-mode handler with the wake gate wired under a
// dedicated wake key, alongside the full TB1–TB5 authorizer set under a
// separate signing key (cross-family interplay stays observable).
func tb6bHandler(t *testing.T, aud string) (*Handler, webhook.SigningKey, []byte, *hookRecorder) {
	t.Helper()
	wakeKey, err := webhook.GenerateSigningKey(rand.Reader, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	signKey, err := webhook.GenerateSigningKey(rand.Reader, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	tokenKey := testAuthKey(t)
	h := testHandler(time.Second, time.Second)
	h.AuthMode = auth.ModeEnforce
	h.AppendAuth = webhook.NewWriteTokenAuthorizer(tokenKey)
	h.ReadAuth = webhook.NewReadCapabilityAuthorizer(tb5Iss, webhook.StaticKidResolver(signKey))
	h.CallerAuth = webhook.NewCallerTokenAuthorizer(tb5Iss, webhook.StaticKidResolver(signKey))
	h.EntityAuth = webhook.NewWakeTokenAuthorizer(aud, webhook.StaticKidResolver(wakeKey))
	rec := &hookRecorder{}
	h.SubHooks = rec
	return h, wakeKey, tokenKey, rec
}

func mintWakeFor(t *testing.T, key webhook.SigningKey, sub, aud string, ttl time.Duration) string {
	t.Helper()
	claims, err := webhook.NewWakeTokenClaims(tb5Iss, sub, aud, 3, "w_e2e", time.Now(), ttl, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := webhook.MintWakeToken(key, claims)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestEntityActsWithinItsOwnSubtree(t *testing.T) {
	h, wakeKey, _, hooks := tb6bHandler(t, "")
	createDirect(t, h, "/agents/a/inbox", "application/json")
	tok := mintWakeFor(t, wakeKey, "agents/a", "", time.Minute)

	// Read its own stream: 200, private (credentialed posture).
	rec := do(h, http.MethodGet, "/agents/a/inbox", map[string]string{"Authorization": "Bearer " + tok}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("agent read = %d %q, want 200", rec.Code, rec.Body.String())
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "private" {
		t.Errorf("agent read Cache-Control = %q, want private", cc)
	}

	// Append home on the wake_token alone — no claim token (the woken
	// entity acting as itself).
	rec = do(h, http.MethodPost, "/agents/a/inbox", map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + tok,
	}, []byte(`{"act":"reply"}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("agent append = %d %q, want 204", rec.Code, rec.Body.String())
	}
	if hooks.appendCount() != 1 {
		t.Fatalf("append hook fired %d times, want 1", hooks.appendCount())
	}
}

// TestEntityCannotActAsAnother is the acceptance core: A's verified token at
// B's streams is FORBIDDEN (403, not 401), with nothing read or mutated.
func TestEntityCannotActAsAnother(t *testing.T) {
	h, wakeKey, _, hooks := tb6bHandler(t, "")
	createDirect(t, h, "/agents/b/inbox", "application/json")
	before := tailOf(t, h, "/agents/b/inbox")
	tokA := mintWakeFor(t, wakeKey, "agents/a", "", time.Minute)

	rec := do(h, http.MethodGet, "/agents/b/inbox", map[string]string{"Authorization": "Bearer " + tokA}, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("A's token reading B = %d, want 403", rec.Code)
	}
	if eb := decodeEnvelope(t, rec); eb.Error.Code != "FORBIDDEN" {
		t.Errorf("code = %q, want FORBIDDEN", eb.Error.Code)
	}

	rec = do(h, http.MethodPost, "/agents/b/inbox", map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + tokA,
	}, []byte(`{"forged":1}`))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("A's token appending to B = %d, want 403", rec.Code)
	}
	if after := tailOf(t, h, "/agents/b/inbox"); !after.Equal(before) {
		t.Fatal("store mutated by cross-entity append")
	}
	if hooks.appendCount() != 0 {
		t.Fatal("wake fired on cross-entity deny")
	}
}

func TestEntityBadTokensUnauthenticated(t *testing.T) {
	h, wakeKey, _, _ := tb6bHandler(t, "")
	createDirect(t, h, "/agents/a/inbox", "application/json")

	expired := mintWakeFor(t, wakeKey, "agents/a", "", -2*time.Minute)
	good := mintWakeFor(t, wakeKey, "agents/a", "", time.Minute)
	tampered := good[:len(good)-3] + "zzz"

	for name, tok := range map[string]string{"expired": expired, "tampered": tampered} {
		rec := do(h, http.MethodGet, "/agents/a/inbox", map[string]string{"Authorization": "Bearer " + tok}, nil)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s read = %d, want 401", name, rec.Code)
		}
		rec = do(h, http.MethodPost, "/agents/a/inbox", map[string]string{
			"Content-Type": "application/json", "Authorization": "Bearer " + tok,
		}, []byte(`{}`))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s append = %d, want 401", name, rec.Code)
		}
	}
}

// TestEntityNeverMutatesTopology: a wake_token grants no create and no
// delete — the entity acts within its subtree, it does not create or
// destroy entities. Falls to the caller verifier and fails its typ pin.
func TestEntityNeverMutatesTopology(t *testing.T) {
	h, wakeKey, _, _ := tb6bHandler(t, "")
	createDirect(t, h, "/agents/a/inbox", "application/json")
	tok := mintWakeFor(t, wakeKey, "agents/a", "", time.Minute)

	rec := do(h, http.MethodPut, "/agents/a/scratch", map[string]string{
		"Content-Type": "application/json", "Authorization": "Bearer " + tok,
	}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wake-token create = %d, want 401", rec.Code)
	}
	if _, err := h.Store.Get("/agents/a/scratch"); err == nil {
		t.Fatal("stream created despite deny")
	}

	rec = do(h, http.MethodDelete, "/agents/a/inbox", map[string]string{"Authorization": "Bearer " + tok}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wake-token delete = %d, want 401", rec.Code)
	}
	if _, err := h.Store.Get("/agents/a/inbox"); err != nil {
		t.Fatal("stream deleted despite deny")
	}
}

// TestWakeTokenOnClaimHeaderIsNotAWriteToken: presented via the
// electric-claim-token header (not Bearer), the wake_token takes the HMAC
// write-token path and fails — the agent arm reads the Bearer only.
func TestWakeTokenOnClaimHeaderIsNotAWriteToken(t *testing.T) {
	h, wakeKey, _, _ := tb6bHandler(t, "")
	createDirect(t, h, "/agents/a/inbox", "application/json")
	tok := mintWakeFor(t, wakeKey, "agents/a", "", time.Minute)

	rec := do(h, http.MethodPost, "/agents/a/inbox", map[string]string{
		"Content-Type":   "application/json",
		ClaimTokenHeader: tok,
	}, []byte(`{}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wake token on claim header = %d, want 401", rec.Code)
	}
}

// TestEntityAudiencePinned: the gate accepts exactly its configured
// audience — a token minted for another audience (e.g. only the egress
// gateway) does not become a data-plane principal here.
func TestEntityAudiencePinned(t *testing.T) {
	h, wakeKey, _, _ := tb6bHandler(t, "chronicle-dataplane")
	createDirect(t, h, "/agents/a/inbox", "application/json")

	matching := mintWakeFor(t, wakeKey, "agents/a", "chronicle-dataplane", time.Minute)
	rec := do(h, http.MethodGet, "/agents/a/inbox", map[string]string{"Authorization": "Bearer " + matching}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("matching-aud read = %d, want 200", rec.Code)
	}

	for name, tok := range map[string]string{
		"gateway-only audience": mintWakeFor(t, wakeKey, "agents/a", "https://egress-gw", time.Minute),
		"audience-less":         mintWakeFor(t, wakeKey, "agents/a", "", time.Minute),
	} {
		rec := do(h, http.MethodGet, "/agents/a/inbox", map[string]string{"Authorization": "Bearer " + tok}, nil)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s = %d, want 401", name, rec.Code)
		}
	}
}

// TestEntityArmDoesNotShadowOtherFamilies: with the wake gate wired, claim
// -token producer appends and read capabilities keep working unchanged.
func TestEntityArmDoesNotShadowOtherFamilies(t *testing.T) {
	h, _, tokenKey, _ := tb6bHandler(t, "")
	createDirect(t, h, "/events/a", "application/json")

	wtok := mintWriteToken(t, tokenKey, "events/a")
	rec := do(h, http.MethodPost, "/events/a", map[string]string{
		"Content-Type":   "application/json",
		ClaimTokenHeader: wtok,
	}, []byte(`{}`))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("producer append = %d, want 204", rec.Code)
	}
}

// TestEntityInsecureModeUnchanged: the default mode still serves
// uncredentialed base clients with the wake gate wired.
func TestEntityInsecureModeUnchanged(t *testing.T) {
	h, _, _, _ := tb6bHandler(t, "")
	h.AuthMode = auth.ModeInsecure
	mustCreate(t, h, "/agents/a/inbox", "application/json", nil)
	mustAppend(t, h, "/agents/a/inbox", "application/json", []byte(`{}`))
	if rec := do(h, http.MethodGet, "/agents/a/inbox", nil, nil); rec.Code != http.StatusOK {
		t.Fatalf("insecure read = %d", rec.Code)
	}
}

// ---- the woken-entity-writes-home loop (Redis integration) ----

// itestRedis returns a flushed client on an isolated db (10) for the full
// wake→act loop, skipping when Redis is unreachable — the same guard
// convention as the webhook and store/redis suites.
func itestRedis(t *testing.T) goredis.UniversalClient {
	t.Helper()
	url := os.Getenv("CHRONICLE_ITEST_REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/10"
	}
	opt, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	c := goredis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable at %s: %v", url, err)
	}
	if err := c.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestWokenEntityWritesHomeLoop is the Electric Agents wake→act cycle end to
// end against the real mint: a per-entity pull-wake subscription linked to
// exactly agents/a/inbox is claimed, the ClaimResponse carries the
// wake_token from the real Manager mint, and that token — alone — reads and
// appends back to agents/a/inbox through the real enforce-mode handler,
// while agents/b stays forbidden.
func TestWokenEntityWritesHomeLoop(t *testing.T) {
	client := itestRedis(t)
	memStore := store.NewMemoryStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	router, _, authz, err := NewSubscriptions(client, memStore, nil,
		"http://loop.test/v1/stream/", false, SubscriptionTuning{}, logger)
	if err != nil {
		t.Fatal(err)
	}

	// Seed the entity's streams and its per-entity dispatch subscription:
	// exactly one link, so the mint names exactly one entity (#123's honest
	// single-entity rule).
	if _, _, err := memStore.Create("/agents/a/inbox", store.CreateOptions{ContentType: "application/json"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := memStore.Create("/agents/b/inbox", store.CreateOptions{ContentType: "application/json"}); err != nil {
		t.Fatal(err)
	}
	ws := webhook.NewRedisStore(client)
	cfg := webhook.NormalizeConfig(webhook.Config{
		Type: webhook.DispatchPullWake, WakeStream: "agents/a/wake", LeaseTTLMs: 30_000,
	})
	if _, err := ws.CreateOrConfirm("entity-a", cfg, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := ws.Link("entity-a", "agents/a/inbox", webhook.LinkExplicit, store.ZeroOffset.String()); err != nil {
		t.Fatal(err)
	}

	// Claim through the real route (insecure default: claim itself ungated
	// here — TB2's gate is exercised in its own suite).
	req := httptest.NewRequest(http.MethodPost, "/__ds/subscriptions/entity-a/claim",
		strings.NewReader(`{"worker":"w-loop"}`))
	rec := httptest.NewRecorder()
	if !router.HandleRequest(rec, req) {
		t.Fatal("claim not routed")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("claim = %d %q", rec.Code, rec.Body.String())
	}
	var cr webhook.ClaimResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &cr); err != nil {
		t.Fatal(err)
	}
	if cr.WakeToken == "" {
		t.Fatal("claim response missing wake_token")
	}

	// The woken entity acts: the real handler in enforce mode, entity gate
	// wired from the same Manager bundle that minted the token.
	h := &Handler{
		Store:      memStore,
		AuthMode:   auth.ModeEnforce,
		EntityAuth: authz.Entity,
		Logger:     logger,
	}

	rec2 := do(h, http.MethodGet, "/agents/a/inbox", map[string]string{"Authorization": "Bearer " + cr.WakeToken}, nil)
	if rec2.Code != http.StatusOK {
		t.Fatalf("woken entity read = %d %q, want 200", rec2.Code, rec2.Body.String())
	}
	rec2 = do(h, http.MethodPost, "/agents/a/inbox", map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + cr.WakeToken,
	}, []byte(`{"done":"work"}`))
	if rec2.Code != http.StatusNoContent {
		t.Fatalf("woken entity append home = %d %q, want 204", rec2.Code, rec2.Body.String())
	}
	meta, err := memStore.Get("/agents/a/inbox")
	if err != nil || meta.CurrentOffset.ByteOffset == 0 {
		t.Fatalf("append did not land: %v %+v", err, meta)
	}

	// The same real token is useless at another entity.
	rec2 = do(h, http.MethodPost, "/agents/b/inbox", map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + cr.WakeToken,
	}, []byte(`{"forged":1}`))
	if rec2.Code != http.StatusForbidden {
		t.Fatalf("cross-entity append = %d, want 403", rec2.Code)
	}
}
