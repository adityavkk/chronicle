package webhook

import (
	"context"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Integration tests run against live Redis (REDIS_URL or
// redis://localhost:6379/14) and are skipped under -short. The database is
// flushed at setup. They exercise the Lua scripts and the durability paths the
// conformance suite does not: restart survival, lease expiry, and the
// re-scoring due claim.

func newTestStore(t *testing.T) (*RedisStore, goredis.UniversalClient) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Redis integration test in -short mode")
	}
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/14"
	}
	opts, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse REDIS_URL: %v", err)
	}
	client := goredis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unreachable (%s): %v", url, err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisStore(client), client
}

func webhookCfg(url string) Config {
	return Config{Type: DispatchWebhook, Pattern: "events/*", WebhookURL: url, LeaseTTLMs: 1000}
}

func TestStoreCreateConfirmConflict(t *testing.T) {
	s, _ := newTestStore(t)
	now := time.Now()
	cfg := webhookCfg("https://w.example/h")

	if st, err := s.CreateOrConfirm("s1", cfg, nil, now); err != nil || st != CreateCreated {
		t.Fatalf("first create = %v/%v, want CreateCreated", st, err)
	}
	if st, _ := s.CreateOrConfirm("s1", cfg, nil, now); st != CreateMatched {
		t.Fatalf("re-confirm identical = %v, want CreateMatched", st)
	}
	other := cfg
	other.WebhookURL = "https://w.example/other"
	if st, _ := s.CreateOrConfirm("s1", other, nil, now); st != CreateConflict {
		t.Fatalf("different config = %v, want CreateConflict", st)
	}
}

func TestStoreLinkPrecedenceAndGet(t *testing.T) {
	s, _ := newTestStore(t)
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now())

	if err := s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatal(err)
	}
	// Explicit upgrades the glob link, preserving the cursor.
	if err := s.Link("s1", "events/a", LinkExplicit, "ignored"); err != nil {
		t.Fatal(err)
	}
	sub, ok, err := s.Get("s1")
	if err != nil || !ok {
		t.Fatalf("get: %v %v", ok, err)
	}
	if len(sub.Links) != 1 || sub.Links[0].LinkType != LinkExplicit {
		t.Fatalf("expected one explicit link, got %+v", sub.Links)
	}
	if sub.Links[0].AckedOffset != "0000000000000000_0000000000000000" {
		t.Fatalf("explicit upgrade must preserve cursor, got %q", sub.Links[0].AckedOffset)
	}
}

func TestStoreArmWakeSurvivesRestart(t *testing.T) {
	s, client := newTestStore(t)
	now := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	res, err := s.ArmWake("s1", now, 1000, true, "w_first")
	if err != nil || !res.Armed || res.Generation != 1 || res.WakeID != "w_first" {
		t.Fatalf("arm = %+v err=%v", res, err)
	}
	// Coalesce: a second arm while in flight is BUSY, not a new generation.
	if busy, _ := s.ArmWake("s1", now, 1000, true, "w_second"); !busy.Busy || busy.Generation != 1 {
		t.Fatalf("second arm should be BUSY at gen 1, got %+v", busy)
	}

	// Simulate an origin restart: a brand-new store over the same Redis sees the
	// durable wake/lease/cursor — the in-memory map is gone, the state is not.
	restarted := NewRedisStore(client)
	sub, ok, err := restarted.Get("s1")
	if err != nil || !ok {
		t.Fatalf("get after restart: %v %v", ok, err)
	}
	if sub.Phase != PhaseWaking || sub.Generation != 1 || sub.WakeID != "w_first" {
		t.Fatalf("durable wake lost across restart: %+v", sub)
	}
	if len(sub.Links) != 1 || sub.Links[0].AckedOffset != "0000000000000000_0000000000000000" {
		t.Fatalf("durable cursor lost across restart: %+v", sub.Links)
	}
	// The lease deadline is a durable ZSET score, not a goroutine.
	if n, _ := client.ZCard(context.Background(), leaseZKey).Result(); n != 1 {
		t.Fatalf("lease ZSET should hold one durable deadline, got %d", n)
	}
}

func TestStoreClaimCASAckFence(t *testing.T) {
	s, _ := newTestStore(t)
	now := time.Now()
	cfg := webhookCfg("https://w.example/h")
	cfg.Type = DispatchPullWake
	cfg.WakeStream = "wake/pool"
	_, _ = s.CreateOrConfirm("s1", cfg, nil, now)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")

	c1, err := s.Claim("s1", "worker-1", "w_a", now, 1000)
	if err != nil || !c1.Claimed {
		t.Fatalf("claim1 = %+v err=%v", c1, err)
	}
	// Second worker is fenced out while the lease is held.
	if c2, _ := s.Claim("s1", "worker-2", "w_b", now, 1000); !c2.Busy || c2.Holder != "worker-1" {
		t.Fatalf("claim2 should be BUSY held by worker-1, got %+v", c2)
	}
	// Stale-generation ack is fenced.
	if st, _ := s.Ack("s1", c1.Generation+9, c1.WakeID, c1.Generation+9, true, nil, now, 1000); st != "FENCED" {
		t.Fatalf("stale ack should FENCE, got %q", st)
	}
	// Correct ack advances the cursor forward-only and releases.
	acks := []Ack{{Stream: "events/a", Offset: "0000000000000001_0000000000000050"}}
	if st, _ := s.Ack("s1", c1.Generation, c1.WakeID, c1.Generation, true, acks, now, 1000); st != "OK" {
		t.Fatalf("valid ack = %q, want OK", st)
	}
	sub, _, _ := s.Get("s1")
	if sub.Phase != PhaseIdle || sub.Links[0].AckedOffset != "0000000000000001_0000000000000050" {
		t.Fatalf("ack(done) must release and advance cursor: %+v", sub)
	}
	// A replayed ack on the now-cleared wake is fenced (cursor not advanced twice).
	if st, _ := s.Ack("s1", c1.Generation, c1.WakeID, c1.Generation, true, acks, now, 1000); st != "FENCED" {
		t.Fatalf("replayed ack should FENCE, got %q", st)
	}
}

func TestStoreLeaseExpiryAndDueReScore(t *testing.T) {
	s, client := newTestStore(t)
	base := time.Now()
	_, _ = s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, base)
	_ = s.Link("s1", "events/a", LinkGlob, "0000000000000000_0000000000000000")
	_, _ = s.ArmWake("s1", base, 1000, true, "w_a")

	// Before the deadline: not expired.
	if st, _ := s.ExpireLease("s1", base); st != "ACTIVE" {
		t.Fatalf("lease not yet due should be ACTIVE, got %q", st)
	}
	// After the deadline: expired, back to idle.
	after := base.Add(2 * time.Second)
	if st, _ := s.ExpireLease("s1", after); st != "EXPIRED" {
		t.Fatalf("expired lease should be EXPIRED, got %q", st)
	}
	sub, _, _ := s.Get("s1")
	if sub.Phase != PhaseIdle || sub.Holder {
		t.Fatalf("expiry must clear holder and idle the subscription: %+v", sub)
	}

	// Due claim re-scores forward, it does not remove (research 07 §6.1): a
	// crashed worker's item must recur.
	_, _ = s.ArmWake("s1", after, 1000, true, "w_b")
	due, err := s.DueLeases(after.Add(2*time.Second), 16, 30*time.Second)
	if err != nil || len(due) != 1 || due[0] != "s1" {
		t.Fatalf("due = %v err=%v, want [s1]", due, err)
	}
	if n, _ := client.ZCard(context.Background(), leaseZKey).Result(); n != 1 {
		t.Fatalf("claimed member must remain in the ZSET (re-scored, not removed), got %d", n)
	}
	// Immediately re-claiming finds nothing — it was re-scored into the future.
	if again, _ := s.DueLeases(after.Add(2*time.Second), 16, 30*time.Second); len(again) != 0 {
		t.Fatalf("re-scored member must not be due again immediately, got %v", again)
	}
}

func TestStoreSigningKeyPersistence(t *testing.T) {
	s, client := newTestStore(t)
	k1, err := s.LoadSigningKey(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// A new store over the same Redis adopts the same persisted key (stable kid).
	k2, err := NewRedisStore(client).LoadSigningKey(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if k1.Kid != k2.Kid {
		t.Fatalf("kid not stable across restart: %q vs %q", k1.Kid, k2.Kid)
	}
}
