package webhook

import (
	"testing"
	"time"
)

// ownership_store_test.go is the golden table for the {ownership} Lua scripts
// (claim_shard.lua / check_owner.lua) and the membership ZSET, against live Redis
// (skipped under -short). Lease expiry is driven deterministically by passing a
// later `now` rather than sleeping, so the tests are fast and flake-free. These
// match the model_shard.go semantics the jepsen T3 gate checks.

const slotTTL = 1 * time.Second

func TestClaimShardGoldenTable(t *testing.T) {
	s, _ := newTestStore(t)
	key := slotKey(0)
	t0 := time.Unix(1_700_000_000, 0)

	// First claim of an unowned slot: CLAIMED, owner_epoch minted to 1.
	c, err := s.ClaimSlot(key, "A", t0, slotTTL)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if c.Status != SlotClaimed || c.Owner.String() != "A" || c.Epoch.Value() != 1 {
		t.Fatalf("first claim = %+v, want CLAIMED owner=A epoch=1", c)
	}
	if !c.Granted() || !c.Transferred() {
		t.Fatalf("first claim: granted=%v transferred=%v, want true/true", c.Granted(), c.Transferred())
	}

	// Same owner re-claims before expiry: RENEWED, epoch UNCHANGED (1).
	c, err = s.ClaimSlot(key, "A", t0.Add(100*time.Millisecond), slotTTL)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if c.Status != SlotRenewed || c.Epoch.Value() != 1 {
		t.Fatalf("renew = %+v, want RENEWED epoch=1 (bump-on-transfer-only)", c)
	}
	if c.Transferred() {
		t.Fatal("renew must not be a transfer")
	}

	// A foreign claim while A's lease is live: BUSY, reports A as the live owner.
	c, err = s.ClaimSlot(key, "B", t0.Add(200*time.Millisecond), slotTTL)
	if err != nil {
		t.Fatalf("busy claim: %v", err)
	}
	if c.Status != SlotBusy || c.Owner.String() != "A" || c.Epoch.Value() != 1 {
		t.Fatalf("busy claim = %+v, want BUSY owner=A epoch=1", c)
	}
	if c.Granted() {
		t.Fatal("BUSY must not be a grant")
	}

	// After A's lease expires (now past lease_expiry_ns), B takes over: CLAIMED,
	// epoch bumped 1 -> 2 (transfer). This is the rotate-on-takeover that fences A.
	expired := t0.Add(slotTTL + time.Second)
	c, err = s.ClaimSlot(key, "B", expired, slotTTL)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if c.Status != SlotClaimed || c.Owner.String() != "B" || c.Epoch.Value() != 2 {
		t.Fatalf("takeover = %+v, want CLAIMED owner=B epoch=2", c)
	}
	if !c.Transferred() {
		t.Fatal("takeover must be a transfer (fires reconcile)")
	}
}

// The load-bearing property: bump-on-transfer-only means a deposed-then-resumed
// owner carries a STALE epoch and is FENCED by check_owner, while the new owner
// is OWNER. This is the Kleppmann deposed-but-resumed case at the ownership layer.
func TestClaimShardDeposedResumedIsFenced(t *testing.T) {
	s, _ := newTestStore(t)
	key := slotKey(0)
	t0 := time.Unix(1_700_000_000, 0)

	a, _ := s.ClaimSlot(key, "A", t0, slotTTL) // A owns at epoch 1
	expired := t0.Add(slotTTL + time.Second)
	b, _ := s.ClaimSlot(key, "B", expired, slotTTL) // B takes over at epoch 2

	// A resumes after its GC pause still believing it holds epoch 1: FENCED.
	chk, err := s.CheckOwner(key, "A", a.Epoch.String())
	if err != nil {
		t.Fatalf("check A: %v", err)
	}
	if chk != OwnerCheckFenced {
		t.Fatalf("deposed A check = %v, want FENCED", chk)
	}
	// B at the current epoch 2 is OWNER.
	chk, err = s.CheckOwner(key, "B", b.Epoch.String())
	if err != nil {
		t.Fatalf("check B: %v", err)
	}
	if chk != OwnerCheckOwner {
		t.Fatalf("current B check = %v, want OWNER", chk)
	}
}

func TestCheckOwnerStates(t *testing.T) {
	s, _ := newTestStore(t)
	key := slotKey(0)
	t0 := time.Unix(1_700_000_000, 0)

	// UNOWNED: no claim yet.
	if chk, err := s.CheckOwner(key, "A", "1"); err != nil || chk != OwnerCheckUnowned {
		t.Fatalf("fresh slot check = %v/%v, want UNOWNED", chk, err)
	}
	c, _ := s.ClaimSlot(key, "A", t0, slotTTL)
	// OWNER: current owner, current epoch.
	if chk, _ := s.CheckOwner(key, "A", c.Epoch.String()); chk != OwnerCheckOwner {
		t.Fatalf("owner check = %v, want OWNER", chk)
	}
	// FENCED: right owner, wrong epoch.
	if chk, _ := s.CheckOwner(key, "A", "999"); chk != OwnerCheckFenced {
		t.Fatalf("stale-epoch check = %v, want FENCED", chk)
	}
	// FENCED: wrong owner, even at the live epoch.
	if chk, _ := s.CheckOwner(key, "C", c.Epoch.String()); chk != OwnerCheckFenced {
		t.Fatalf("foreign check = %v, want FENCED", chk)
	}
}

func TestMembershipHeartbeatAndLiveMembers(t *testing.T) {
	s, _ := newTestStore(t)
	ttl := 9 * time.Second
	t0 := time.Unix(1_700_000_000, 0)

	if err := s.Heartbeat("r1", t0, ttl); err != nil {
		t.Fatalf("heartbeat r1: %v", err)
	}
	if err := s.Heartbeat("r2", t0, ttl); err != nil {
		t.Fatalf("heartbeat r2: %v", err)
	}
	live, err := s.LiveMembers(t0.Add(time.Second))
	if err != nil {
		t.Fatalf("live members: %v", err)
	}
	if len(live) != 2 {
		t.Fatalf("live = %v, want [r1 r2]", live)
	}

	// r2 keeps heartbeating; r1 goes silent. After r1's lease lapses, a heartbeat
	// from r2 at a later now evicts r1 via ZREMRANGEBYSCORE, and LiveMembers shows
	// only r2 — the dead-member age-out the slot-reconcile loop relies on.
	later := t0.Add(ttl + time.Second)
	if err := s.Heartbeat("r2", later, ttl); err != nil {
		t.Fatalf("heartbeat r2 later: %v", err)
	}
	live, err = s.LiveMembers(later)
	if err != nil {
		t.Fatalf("live members later: %v", err)
	}
	if len(live) != 1 || live[0] != "r2" {
		t.Fatalf("live after r1 aged out = %v, want [r2]", live)
	}
}
