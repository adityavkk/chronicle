package webhook

import (
	"testing"
	"time"
)

// ownership_manager_test.go covers the Manager's slot-ownership shell (issue #14):
// the membership/HRW/slot-reconcile wiring, the ownedSlots() work-sharding gate,
// the new-owner-CAS firing #13's reconcile seam, and the inline OwnerFenced metric.
// Against live Redis (skipped under -short).

func newOwnershipManager(t *testing.T, s *RedisStore, replica string, fm *fakeMetrics) *Manager {
	t.Helper()
	var metrics Metrics
	if fm != nil {
		// The inline owner-epoch fence is recorded store-side (the single place the
		// Lua reply is observed), so wire the store to the same recorder the manager
		// uses — exactly as the binary does (NewRedisStore(...).WithMetrics(...)).
		s.WithMetrics(fm)
		metrics = fm
	}
	opts := ManagerOptions{StreamRootURL: "http://x/v1/stream/", ReplicaID: replica, Metrics: metrics}
	m, err := NewManager(s, &fakeStreams{tails: map[string]string{}}, opts)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

func TestManagerOwnershipDefaultsAndInvariants(t *testing.T) {
	s, _ := newTestStore(t)

	// Zero TTLs default to 9s/3s/9s/3s and the generated replica id is non-empty.
	m := newOwnershipManager(t, s, "", nil)
	if m.memberLeaseTTL != defaultMemberLeaseTTL || m.heartbeatInterval != defaultHeartbeatInterval ||
		m.slotLeaseTTL != defaultSlotLeaseTTL || m.slotReconcileInterval != defaultSlotReconcileInterval {
		t.Fatalf("defaults not applied: %v/%v/%v/%v", m.memberLeaseTTL, m.heartbeatInterval, m.slotLeaseTTL, m.slotReconcileInterval)
	}
	if m.ReplicaID() == "" {
		t.Fatal("generated replica id is empty")
	}

	// An explicit replica id is honored.
	m = newOwnershipManager(t, s, "rA", nil)
	if m.ReplicaID() != "rA" {
		t.Fatalf("replica id = %q, want rA", m.ReplicaID())
	}

	// A timer set violating heartbeatInterval < memberLeaseTTL/2 falls back to ALL
	// defaults rather than failing startup.
	bad, err := NewManager(s, &fakeStreams{tails: map[string]string{}}, ManagerOptions{
		StreamRootURL:     "http://x/v1/stream/",
		MemberLeaseTTL:    4 * time.Second,
		HeartbeatInterval: 3 * time.Second, // 3s >= 4s/2 — violates the invariant
		SlotLeaseTTL:      9 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewManager with bad timers should not fail: %v", err)
	}
	if bad.heartbeatInterval != defaultHeartbeatInterval || bad.memberLeaseTTL != defaultMemberLeaseTTL {
		t.Fatalf("invalid timers not reset to defaults: heartbeat=%v member=%v", bad.heartbeatInterval, bad.memberLeaseTTL)
	}
}

func TestManagerSlotReconcileClaimsOwnsAndFires(t *testing.T) {
	s, _ := newTestStore(t)
	fm := &fakeMetrics{}
	m := newOwnershipManager(t, s, "rA", fm)

	// Seed our membership, then reconcile: at S=1 rA targets the single slot and
	// claim_shard CLAIMS it (first claim is a transfer/epoch bump).
	if err := s.Heartbeat("rA", time.Now(), m.memberLeaseTTL); err != nil {
		t.Fatal(err)
	}
	m.RunSlotReconcile()

	if !m.ownsAnySlot() {
		t.Fatal("rA should own the single slot after reconcile")
	}
	owned := m.ownedSlots()
	if len(owned) != 1 || owned[0].Index() != 0 {
		t.Fatalf("ownedSlots = %v, want [slot 0]", owned)
	}
	if fm.slotOwnership("claimed") < 1 {
		t.Fatalf("SlotOwnership(claimed) not recorded: %v", fm.slotOwn)
	}
	// The new-owner CAS (a transfer) fired #13's reconcile seam.
	select {
	case sc := <-m.reconcileC:
		if sc != scopeNewOwnerCAS {
			t.Fatalf("queued reconcile scope = %v, want scopeNewOwnerCAS", sc)
		}
	default:
		t.Fatal("a new-owner CAS must queue reconcile(scopeNewOwnerCAS)")
	}

	// A second reconcile by the same owner RENEWS (epoch unchanged): no new
	// transfer, so no fresh reconcile is queued.
	m.RunSlotReconcile()
	if fm.slotOwnership("renewed") < 1 {
		t.Fatalf("SlotOwnership(renewed) not recorded on the renew: %v", fm.slotOwn)
	}
	select {
	case sc := <-m.reconcileC:
		t.Fatalf("a renew must NOT queue a reconcile, got %v", sc)
	default:
	}
}

// Work-sharding: with two replicas sharing one Redis, exactly ONE owns the single
// slot (the HRW winner); the other gets BUSY and runs no background work — total
// work is O(total owed) regardless of N.
func TestManagerWorkShardingExactlyOneOwner(t *testing.T) {
	s, _ := newTestStore(t)
	mA := newOwnershipManager(t, s, "rA", &fakeMetrics{})
	mB := newOwnershipManager(t, s, "rB", &fakeMetrics{})

	now := time.Now()
	if err := s.Heartbeat("rA", now, mA.memberLeaseTTL); err != nil {
		t.Fatal(err)
	}
	if err := s.Heartbeat("rB", now, mB.memberLeaseTTL); err != nil {
		t.Fatal(err)
	}
	mA.RunSlotReconcile()
	mB.RunSlotReconcile()

	if mA.ownsAnySlot() == mB.ownsAnySlot() {
		t.Fatalf("exactly one replica must own the slot: A=%v B=%v", mA.ownsAnySlot(), mB.ownsAnySlot())
	}
}

// A deposed owner's lease-worker expiry is FENCED inline and recorded as an inline
// owner fence: rA owns the slot (epoch 1), a foreign replica takes it over (epoch
// 2) after the lease expires, and rA — still holding the stale epoch 1 in its
// snapshot — has its expire_lease FENCED, suppressing its wasted work.
func TestManagerDeposedOwnerExpireFencedInline(t *testing.T) {
	s, _ := newTestStore(t)
	fm := &fakeMetrics{}
	m := newOwnershipManager(t, s, "rA", fm)
	if _, err := s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := s.Heartbeat("rA", time.Now(), m.memberLeaseTTL); err != nil {
		t.Fatal(err)
	}
	m.RunSlotReconcile() // rA owns slot 0 at epoch 1
	scope, ok := m.singleOwnerScope()
	if !ok {
		t.Fatal("rA should hold a slot")
	}

	// A foreign replica takes over the slot after rA's slot lease expires, bumping
	// the epoch — rA's `scope` is now stale (deposed-but-resumed).
	future := time.Now().Add(m.slotLeaseTTL + time.Second)
	tk, err := s.ClaimSlot(slotKey(0), "intruder", future, m.slotLeaseTTL)
	if err != nil || !tk.Transferred() {
		t.Fatalf("takeover = %+v err=%v, want a transfer", tk, err)
	}

	// rA's lease worker would expire a due lease using its stale scope: FENCED
	// inline, recorded as an inline owner fence (its wasted work suppressed).
	status, err := m.expireLease("s1", time.Now(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if status != "FENCED" {
		t.Fatalf("deposed expire = %q, want FENCED", status)
	}
	if fm.ownerFences("inline") < 1 {
		t.Fatalf("OwnerFenced(inline) not recorded: %v", fm.ownerFenced)
	}
}

// The RETRY path threads the owner scope so its inline owner-epoch fence is
// exercised in production (not dead code): recordFailure -> ScheduleRetry and the
// retry-worker auto-ack(done) both FENCE a deposed owner inline. rA owns slot 0
// (epoch 1); a foreign replica takes it over (epoch 2); rA's now-stale scope must
// fence both writes — atomically, above the still-valid (gen,wake_id) fence.
func TestManagerRetryPathFencedInline(t *testing.T) {
	s, _ := newTestStore(t)
	fm := &fakeMetrics{}
	m := newOwnershipManager(t, s, "rA", fm)
	if _, err := s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.Heartbeat("rA", time.Now(), m.memberLeaseTTL); err != nil {
		t.Fatal(err)
	}
	m.RunSlotReconcile() // rA owns slot 0 at epoch 1
	scope, ok := m.singleOwnerScope()
	if !ok {
		t.Fatal("rA should hold a slot")
	}
	now := time.Now()
	arm, err := s.ArmWake("s1", now, 60000, true, "wk-1") // valid gen/wake for the ack test
	if err != nil || !arm.Armed {
		t.Fatalf("arm = %+v err=%v", arm, err)
	}

	// Control: an UNSCOPED recordFailure (the append path) schedules a retry — the
	// sub is schedulable, so a later non-advance is the fence, not a no-op.
	m.recordFailure("s1")
	if sub, _, _ := s.Get("s1"); sub.RetryCount != 1 {
		t.Fatalf("unscoped recordFailure should schedule a retry (count=1), got %d", sub.RetryCount)
	}

	// A foreign replica takes the slot over after the lease expires — rA's `scope`
	// is now stale (deposed-but-resumed).
	tk, err := s.ClaimSlot(slotKey(0), "intruder", now.Add(m.slotLeaseTTL+time.Second), m.slotLeaseTTL)
	if err != nil || !tk.Transferred() {
		t.Fatalf("takeover = %+v err=%v, want a transfer", tk, err)
	}

	// recordFailure with the deposed scope: schedule_retry is FENCED inline, so the
	// retry count does NOT advance and an inline fence is recorded.
	before := fm.ownerFences("inline")
	m.recordFailure("s1", scope)
	if sub, _, _ := s.Get("s1"); sub.RetryCount != 1 {
		t.Fatalf("deposed recordFailure must schedule nothing (count stays 1), got %d", sub.RetryCount)
	}
	if fm.ownerFences("inline") <= before {
		t.Fatalf("recordFailure->schedule_retry inline fence not recorded (was %d, now %d)", before, fm.ownerFences("inline"))
	}

	// The retry-worker auto-ack(done) with the deposed scope: VALID gen/wake (so the
	// (gen,wake_id) fence would pass), yet FENCED — proving the owner-epoch fence is
	// exercised on the retry-path ack, above the gen fence.
	before = fm.ownerFences("inline")
	status, err := m.ack("s1", arm.Generation, arm.WakeID, arm.Generation, true, nil, time.Now(), 60000, scope)
	if err != nil {
		t.Fatal(err)
	}
	if status != "FENCED" {
		t.Fatalf("deposed retry-path ack(done) with valid gen = %q, want FENCED (inline owner fence)", status)
	}
	if fm.ownerFences("inline") <= before {
		t.Fatalf("retry-path ack(done) inline fence not recorded (was %d, now %d)", before, fm.ownerFences("inline"))
	}
}

// A deterministic, timing-free proof that a dead member's slot is reclaimed by a
// survivor: rA holds slot 0; after both the membership lease and the slot lease
// have expired (driven by an explicit later `now`), the survivor's heartbeat
// evicts rA from members and its claim_shard takes the slot over (epoch bump).
func TestSlotReclaimedAfterMemberAndLeaseExpire(t *testing.T) {
	s, _ := newTestStore(t)
	ttl := 9 * time.Second
	t0 := time.Unix(1_700_000_000, 0)

	// rA and rB are both live; rA owns slot 0 at epoch 1.
	if err := s.Heartbeat("rA", t0, ttl); err != nil {
		t.Fatal(err)
	}
	if err := s.Heartbeat("rB", t0, ttl); err != nil {
		t.Fatal(err)
	}
	a, _ := s.ClaimSlot(slotKey(0), "rA", t0, ttl)
	if a.Status != SlotClaimed {
		t.Fatalf("rA claim = %v, want CLAIMED", a.Status)
	}

	// Time advances past both leases; rB's heartbeat at the later now evicts the
	// silent rA from members (ZREMRANGEBYSCORE), so HRW now has only rB to assign.
	later := t0.Add(ttl + time.Second)
	if err := s.Heartbeat("rB", later, ttl); err != nil {
		t.Fatal(err)
	}
	live, _ := s.LiveMembers(later)
	if len(live) != 1 || live[0] != "rB" {
		t.Fatalf("live members after rA aged out = %v, want [rB]", live)
	}
	// rA's slot lease has also expired, so rB's claim_shard takes it over: a
	// transfer, epoch bumped 1 -> 2 (rA is now fenced).
	b, _ := s.ClaimSlot(slotKey(0), "rB", later, ttl)
	if b.Status != SlotClaimed || b.Epoch.Value() != 2 {
		t.Fatalf("rB takeover = %+v, want CLAIMED epoch=2", b)
	}
	// rA, if it resumed, is fenced at its stale epoch 1.
	if chk, _ := s.CheckOwner(slotKey(0), "rA", "1"); chk != OwnerCheckFenced {
		t.Fatalf("resumed rA = %v, want FENCED", chk)
	}
}
