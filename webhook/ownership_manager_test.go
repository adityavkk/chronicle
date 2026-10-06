package webhook

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"maps"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/internal/redistest"
)

// ownership_manager_test.go covers the Manager's slot-ownership shell (issue #14):
// the membership/HRW/slot-reconcile wiring, the held-set work-sharding gate,
// the new-owner-CAS firing #13's reconcile seam, and the inline OwnerFenced metric.
// Against live Redis (skipped under -short).

// ownedSlots is the test's view of the held set: the slots m currently owns.
func ownedSlots(m *Manager) []SlotID {
	scopes := m.ownedScopes()
	out := make([]SlotID, len(scopes))
	for i, o := range scopes {
		out[i] = o.h
	}
	return out
}

// ownerScope is the scope m holds slot h at; ok is false when m does not hold h.
func ownerScope(m *Manager, h SlotID) (OwnerScope, bool) {
	for _, o := range m.ownedScopes() {
		if o.h == h {
			return o.scope, true
		}
	}
	return OwnerScope{}, false
}

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

	// Seed our membership, then reconcile: at S=subSlots a SOLE replica is the HRW
	// target of every slot and claim_shard CLAIMS each (the first claim of each is a
	// transfer/epoch bump). So rA ends up owning all S slots.
	if err := s.Heartbeat("rA", time.Now(), m.memberLeaseTTL); err != nil {
		t.Fatal(err)
	}
	m.RunSlotReconcile()

	owned := ownedSlots(m)
	if len(owned) == 0 {
		t.Fatal("rA should own slots after reconcile")
	}
	if len(owned) != subSlots {
		t.Fatalf("a sole replica should own all %d slots, got %d", subSlots, len(owned))
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

// Work-sharding: with two replicas sharing one Redis, HRW PARTITIONS the S slots
// between them — every slot owned by exactly one replica (no gap, no split-brain),
// and both carry a non-trivial share — so total background work is O(total owed)
// regardless of N (each replica runs the workers only for its owned slots).
func TestManagerWorkShardingPartitionsSlots(t *testing.T) {
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

	ownedA := ownedSlots(mA)
	ownedB := ownedSlots(mB)
	owners := make(map[int]int, subSlots)
	for _, h := range ownedA {
		owners[h.Index()]++
	}
	for _, h := range ownedB {
		owners[h.Index()]++
	}
	if len(owners) != subSlots {
		t.Fatalf("the two replicas must cover all %d slots, covered %d", subSlots, len(owners))
	}
	for h, n := range owners {
		if n != 1 {
			t.Fatalf("slot %d owned by %d replicas, want exactly one (no split-brain)", h, n)
		}
	}
	if len(ownedA) == 0 || len(ownedB) == 0 {
		t.Fatalf("work must be shared across replicas: A owns %d, B owns %d", len(ownedA), len(ownedB))
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
	m.RunSlotReconcile() // rA owns all slots at epoch 1
	// s1's slot is the one whose owner scope its lease worker presents.
	sh, _ := NewSlotID(slotOf("s1"))
	scope, ok := ownerScope(m, sh)
	if !ok {
		t.Fatal("rA should hold s1's slot")
	}

	// A foreign replica takes over THAT slot after rA's slot lease expires, bumping
	// the epoch — rA's `scope` is now stale (deposed-but-resumed).
	future := time.Now().Add(m.slotLeaseTTL + time.Second)
	tk, err := s.ClaimSlot(slotKey(sh.Index()), "intruder", future, m.slotLeaseTTL)
	if err != nil || !tk.Transferred() {
		t.Fatalf("takeover = %+v err=%v, want a transfer", tk, err)
	}

	// rA's lease worker would expire a due lease using its stale scope: FENCED
	// inline, recorded as an inline owner fence (its wasted work suppressed).
	status, err := m.expireLeaseOwned(scope, "s1", time.Now())
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
	m.RunSlotReconcile() // rA owns all slots at epoch 1
	sh, _ := NewSlotID(slotOf("s1"))
	scope, ok := ownerScope(m, sh)
	if !ok {
		t.Fatal("rA should hold s1's slot")
	}
	now := time.Now()
	arm, err := s.ArmWakeUnscoped("s1", now, 60000, true, "wk-1") // valid gen/wake for the ack test
	if err != nil || !arm.Armed {
		t.Fatalf("arm = %+v err=%v", arm, err)
	}

	// Control: an UNSCOPED recordFailure (the append path) schedules a retry — the
	// sub is schedulable, so a later non-advance is the fence, not a no-op.
	m.recordFailure("s1", arm.Generation, arm.WakeID, nil, m.requestIDForWake(arm.WakeID))
	if sub, _, _ := s.Get("s1"); sub.RetryCount != 1 {
		t.Fatalf("unscoped recordFailure should schedule a retry (count=1), got %d", sub.RetryCount)
	}

	// A foreign replica takes the slot over after the lease expires — rA's `scope`
	// is now stale (deposed-but-resumed).
	tk, err := s.ClaimSlot(slotKey(sh.Index()), "intruder", now.Add(m.slotLeaseTTL+time.Second), m.slotLeaseTTL)
	if err != nil || !tk.Transferred() {
		t.Fatalf("takeover = %+v err=%v, want a transfer", tk, err)
	}

	// recordFailure with the deposed scope: schedule_retry is FENCED inline, so the
	// retry count does NOT advance and an inline fence is recorded.
	before := fm.ownerFences("inline")
	m.recordFailure("s1", arm.Generation, arm.WakeID, &scope, m.requestIDForWake(arm.WakeID))
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
	status, err := m.ackOwned(scope, "s1", arm.Generation, arm.WakeID, arm.Generation, true, nil, time.Now(), 60000)
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

// ---- batched passes: slotReconcileOnce / leasePass / duePass / retryPass ----
//
// Member ids chosen so that rA HRW-targets exactly 85 of the 256 slots with
// {rA, rB, rC} and exactly 128 with {rA, rB}: the three-member steady state and
// the share after one member leaves.
const (
	memberB = "rB-27"
	memberC = "rC-19"
)

func heartbeatMembers(t *testing.T, s Store, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := s.Heartbeat(id, time.Now(), 10*time.Minute); err != nil {
			t.Fatal(err)
		}
	}
}

// heldSnapshot copies the manager's held map (slot -> epoch) for comparison.
func heldSnapshot(m *Manager) map[SlotID]OwnerEpoch {
	m.ownMu.RLock()
	defer m.ownMu.RUnlock()
	return maps.Clone(m.held)
}

// queuedReconcile drains the depth-1 reconcileC: the scope a pass queued, or -1.
func queuedReconcile(m *Manager) scope {
	select {
	case sc := <-m.reconcileC:
		return sc
	default:
		return -1
	}
}

// serialClaimStore is the sequential reference: ClaimSlots as one ClaimSlot per key,
// in a shuffled order (the old loop iterated a Go map).
type serialClaimStore struct {
	Store
	rng *rand.Rand
}

func (s *serialClaimStore) ClaimSlots(slotKeys []string, replicaID string, now time.Time, ttl time.Duration) []SlotClaimResult {
	out := make([]SlotClaimResult, len(slotKeys))
	for _, i := range s.rng.Perm(len(slotKeys)) {
		c, err := s.ClaimSlot(slotKeys[i], replicaID, now, ttl)
		out[i] = SlotClaimResult{Claim: c, Err: err}
	}
	return out
}

// TestSlotReconcileHeldMatchesSerialReference replays one random schedule of
// foreign takeovers, lease lapses and member joins/leaves against a fresh DB twice
// — once through the batched ClaimSlots, once through the shuffled per-key serial
// store — and requires the held map (slot -> epoch) and the queued reconcile scope
// to be identical after every pass. Given the same members and foreign state, the
// CAS outcomes are a function of per-slot state alone.
func TestSlotReconcileHeldMatchesSerialReference(t *testing.T) {
	s, client := newTestStore(t)
	ctx := context.Background()
	const passes = 6
	rng := rand.New(rand.NewSource(11))
	type event struct {
		kind   int
		slot   int
		member string
	}
	schedule := make([][]event, passes)
	for p := range schedule {
		for i := 0; i < 12; i++ {
			schedule[p] = append(schedule[p], event{kind: rng.Intn(5), slot: rng.Intn(subSlots), member: []string{memberB, memberC, "rD"}[rng.Intn(3)]})
		}
	}
	apply := func(evs []event) {
		now := time.Now()
		for _, ev := range evs {
			key, legacy := slotKey(ev.slot), legacyOwnershipSlotKey(ev.slot)
			switch ev.kind {
			case 0: // a rival holds the slot live: BUSY for us until it lapses
				exp := strconv.FormatInt(now.Add(time.Hour).UnixNano(), 10)
				for _, k := range []string{key, legacy} {
					if err := client.HSet(ctx, k, "owner_id", "rival", "owner_epoch", "7", "lease_expiry_ns", exp).Err(); err != nil {
						t.Fatal(err)
					}
				}
			case 1: // a rival's lease has lapsed: a takeover (epoch bump) for us
				exp := strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)
				for _, k := range []string{key, legacy} {
					if err := client.HSet(ctx, k, "owner_id", "rival", "owner_epoch", "7", "lease_expiry_ns", exp).Err(); err != nil {
						t.Fatal(err)
					}
				}
			case 2: // our own lease was lost (a stall): the next claim is a transfer back
				if err := client.HSet(ctx, key, "owner_id", "rival", "lease_expiry_ns", "0").Err(); err != nil {
					t.Fatal(err)
				}
			case 3:
				heartbeatMembers(t, s, ev.member)
			case 4:
				if err := client.ZRem(ctx, membersKey, ev.member).Err(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	type snapshot struct {
		held   map[SlotID]OwnerEpoch
		queued scope
	}
	run := func(st Store) []snapshot {
		if err := client.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		m, err := NewManager(st, &fakeStreams{tails: map[string]string{}}, ManagerOptions{StreamRootURL: "http://x/v1/stream/", ReplicaID: "rA"})
		if err != nil {
			t.Fatal(err)
		}
		heartbeatMembers(t, s, "rA", memberB, memberC)
		out := make([]snapshot, 0, passes)
		for p := 0; p < passes; p++ {
			apply(schedule[p])
			m.RunSlotReconcile()
			out = append(out, snapshot{heldSnapshot(m), queuedReconcile(m)})
		}
		return out
	}
	want := run(&serialClaimStore{Store: s, rng: rand.New(rand.NewSource(3))})
	got := run(s)
	for p := range want {
		if want[p].queued != got[p].queued {
			t.Fatalf("pass %d: batched queued scope %v, serial %v", p, got[p].queued, want[p].queued)
		}
		if !reflect.DeepEqual(want[p].held, got[p].held) {
			t.Fatalf("pass %d: held differs\nbatched: %v\nserial:  %v", p, got[p].held, want[p].held)
		}
		if len(want[p].held) == 0 {
			t.Fatalf("pass %d: the schedule left rA owning nothing — not a useful run", p)
		}
	}
}

// TestSlotReconcilePassRoundTripsAreConstant: a pass is one ZRANGEBYSCORE plus three
// pipelines whether it claims 85 slots cold, renews them, or takes over 43 more
// after a member leaves — and the SlotOwnership counts equal the serial reference's.
func TestSlotReconcilePassRoundTripsAreConstant(t *testing.T) {
	s, client, rec := newCountingStore(t)
	warmScripts(t, s, client)
	ctx := context.Background()
	assertPass := func(label string) {
		t.Helper()
		trips := rec.Take()
		if singles, pipes := tripShape(trips); singles != 1 || pipes != 3 {
			t.Fatalf("%s: %d single commands and %d pipelines, want 1 (ZRANGEBYSCORE) and 3: %s", label, singles, pipes, summarize(trips))
		}
	}
	run := func(st Store) map[string]int {
		if err := client.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		fm := &fakeMetrics{}
		m := newOwnershipManager(t, s, "rA", fm)
		m.store = st
		heartbeatMembers(t, s, "rA", memberB, memberC)
		rec.Take()
		m.RunSlotReconcile()
		if n := len(ownedSlots(m)); n != 85 {
			t.Fatalf("three members: rA owns %d slots, want 85", n)
		}
		if _, ok := st.(*serialClaimStore); !ok {
			assertPass("cold pass, 85 slots")
		}
		m.RunSlotReconcile()
		if _, ok := st.(*serialClaimStore); !ok {
			assertPass("steady pass, 85 slots")
		}
		if err := client.ZRem(ctx, membersKey, memberC).Err(); err != nil {
			t.Fatal(err)
		}
		rec.Take()
		m.RunSlotReconcile()
		if n := len(ownedSlots(m)); n != 128 {
			t.Fatalf("after a member left: rA owns %d slots, want 128", n)
		}
		if _, ok := st.(*serialClaimStore); !ok {
			assertPass("member-leaves pass, 43 claimed + 85 renewed")
		}
		fm.mu.Lock()
		defer fm.mu.Unlock()
		return maps.Clone(fm.slotOwn)
	}
	got := run(s)
	want := run(&serialClaimStore{Store: s, rng: rand.New(rand.NewSource(5))})
	if !reflect.DeepEqual(want, got) || got["claimed"] != 85+43 || got["renewed"] != 85+85 {
		t.Fatalf("SlotOwnership counts: batched %v, serial %v, want claimed=128 renewed=170", got, want)
	}
}

// TestWorkerPassRoundTripsAreConstant: each worker pass over 128 owned slots is one
// claim_due pipeline when every slot is empty, and one pipeline plus only the
// per-id work when 22 slots hold one (nonexistent) item each: expire_lease for the
// lease worker; the slot-homed Get (a pipeline), the legacy-tag Get miss and the
// ClearDue ZREM for the due worker; the Get pair for the retry worker.
func TestWorkerPassRoundTripsAreConstant(t *testing.T) {
	s, client, rec := newCountingStore(t)
	warmScripts(t, s, client)
	ctx := context.Background()
	fm := &fakeMetrics{}
	m := newOwnershipManager(t, s, "rA", fm)
	heartbeatMembers(t, s, "rA", memberB)
	m.RunSlotReconcile()
	owned := ownedSlots(m)
	if len(owned) != 128 {
		t.Fatalf("two members: rA owns %d slots, want 128", len(owned))
	}
	var nonEmpty []int
	for i, h := range m.ownedScopes() {
		if i%6 == 0 && len(nonEmpty) < 22 {
			nonEmpty = append(nonEmpty, h.h.Index())
		}
	}
	seed := func() {
		past := float64(time.Now().Add(-time.Second).UnixNano())
		for _, h := range nonEmpty {
			for _, k := range []string{leaseZKey(h), dueZKey(h), retryZKey(h)} {
				if err := client.ZAdd(ctx, k, goredis.Z{Score: past, Member: idInSlot(h)}).Err(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	type pass struct {
		name string
		run  func(time.Time)
	}
	passes := []pass{{"leasePass", m.leasePass}, {"duePass", m.duePass}, {"retryPass", m.retryPass}}
	// Warm the per-id scripts (expire_lease) so the counted runs see no NOSCRIPT.
	seed()
	for _, p := range passes {
		p.run(time.Now())
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	for _, p := range passes {
		rec.Take()
		p.run(time.Now())
		if singles, pipes := tripShape(rec.Take()); singles != 0 || pipes != 1 {
			t.Fatalf("%s over 128 empty slots: %d singles and %d pipelines, want 0 and 1", p.name, singles, pipes)
		}
	}
	want := map[string][2]int{ // singles, pipelines
		"leasePass": {22, 1},      // expire_lease per id
		"duePass":   {44, 1 + 22}, // Get: slot-homed pipeline + legacy HGETALL miss; then ClearDue ZREM
		"retryPass": {22, 1 + 22}, // Get: slot-homed pipeline + legacy HGETALL miss
	}
	dueTicksBefore := fm.dueTicks
	for _, p := range passes {
		if err := client.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		seed()
		rec.Take()
		p.run(time.Now())
		singles, pipes := tripShape(rec.Take())
		if w := want[p.name]; singles != w[0] || pipes != w[1] {
			t.Fatalf("%s over 128 slots with 22 items: %d singles and %d pipelines, want %d and %d", p.name, singles, pipes, w[0], w[1])
		}
	}
	if n := fm.dueTicks - dueTicksBefore; n != 22 {
		t.Fatalf("DueWorkerTick recorded %d times, want once per non-empty slot (22)", n)
	}
}

// slowConn sleeps before every Write: one injected round trip per flush, the model
// under which the sequential pass is N x trips x RTT and the batched one is ~trips x RTT.
type slowConn struct {
	net.Conn
	perWrite time.Duration
}

func (c *slowConn) Write(b []byte) (int, error) {
	time.Sleep(c.perWrite)
	return c.Conn.Write(b)
}

// slowDialer is the newTestStoreWith option that dials slowConns: every
// connection sleeps perWrite before each flush.
func slowDialer(perWrite time.Duration) func(*goredis.Options) {
	return func(o *goredis.Options) {
		o.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &slowConn{Conn: c, perWrite: perWrite}, nil
		}
	}
}

// recordingClaimStore logs when each pass issued its claims and what came back,
// and lets the test act just before a pass issues them.
type recordingClaimStore struct {
	Store
	mu         sync.Mutex
	issuedAt   []time.Time
	results    [][]SlotClaimResult
	beforePass func(pass int) // runs before pass number pass (from 1) issues its claims
}

func (r *recordingClaimStore) ClaimSlots(slotKeys []string, replicaID string, now time.Time, ttl time.Duration) []SlotClaimResult {
	if r.beforePass != nil {
		r.beforePass(r.passes() + 1)
	}
	issued := time.Now()
	out := r.Store.ClaimSlots(slotKeys, replicaID, now, ttl)
	r.mu.Lock()
	r.issuedAt = append(r.issuedAt, issued)
	r.results = append(r.results, out)
	r.mu.Unlock()
	return out
}

func (r *recordingClaimStore) passes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.results)
}

// TestSlotReconcileRenewsWithinLeaseUnderStall is the stability proof with scaled
// timers (slotLeaseTTL 1200 ms, slotReconcileInterval 400 ms — the production 9 s /
// 3 s ratio), an 8 ms round trip and one 400 ms stall on a pipeline of a renewing
// pass: over six passes of the real slotReconcileLoop, every slot's lease from
// pass k is still live when pass k+1 issues its renewal, held stays at 128 and no
// epoch changes. The sequential form cannot pass this: 128 slots x 3 trips x 8 ms
// = 3.07 s per pass against a 1.2 s lease, so every renewal would land after its
// lease lapsed.
func TestSlotReconcileRenewsWithinLeaseUnderStall(t *testing.T) {
	const (
		rtt      = 8 * time.Millisecond
		leaseTTL = 1200 * time.Millisecond
		interval = 400 * time.Millisecond
		passes   = 6
	)
	s, client := newTestStoreWith(t, slowDialer(rtt))
	trips := &redistest.TripLog{}
	client.AddHook(trips)
	rec := &recordingClaimStore{Store: s}
	m, err := NewManager(rec, &fakeStreams{tails: map[string]string{}}, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/", ReplicaID: "rA",
		MemberLeaseTTL: leaseTTL, HeartbeatInterval: interval,
		SlotLeaseTTL: leaseTTL, SlotReconcileInterval: interval,
	})
	if err != nil {
		t.Fatal(err)
	}
	if m.slotLeaseTTL != leaseTTL || m.slotReconcileInterval != interval {
		t.Fatalf("scaled timers rejected: lease=%v interval=%v", m.slotLeaseTTL, m.slotReconcileInterval)
	}
	heartbeatMembers(t, s, "rA", memberB)
	// Stall the first pipeline of pass 2 (the reserve step, while the leases pass 1
	// wrote are aging) by 400 ms: one read-timeout class delay on one round trip.
	var stalled atomic.Bool
	rec.beforePass = func(pass int) {
		if pass == 2 {
			trips.BeforePipeline(func() { time.Sleep(interval); stalled.Store(true) })
		}
	}

	m.wg.Add(1)
	go m.slotReconcileLoop()
	deadline := time.Now().Add(passes*interval + 5*time.Second)
	for rec.passes() < passes && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	m.cancelRun()
	m.wg.Wait()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.results) < passes {
		t.Fatalf("only %d passes ran in time", len(rec.results))
	}
	epochs := map[int]OwnerEpoch{}
	for p := 0; p < passes; p++ {
		if len(rec.results[p]) != 128 {
			t.Fatalf("pass %d claimed %d slots, want 128", p, len(rec.results[p]))
		}
		for i, r := range rec.results[p] {
			if r.Err != nil || !r.Claim.Granted() {
				t.Fatalf("pass %d slot %d: %+v/%v, want a grant", p, i, r.Claim, r.Err)
			}
			if p == 0 {
				epochs[i] = r.Claim.Epoch
			} else if r.Claim.Epoch != epochs[i] || r.Claim.Status != SlotRenewed {
				t.Fatalf("pass %d slot %d: %+v, want RENEWED at epoch %v (no churn)", p, i, r.Claim, epochs[i])
			}
			if p+1 < passes && r.Claim.ExpiryNs <= rec.issuedAt[p+1].UnixNano() {
				t.Fatalf("pass %d slot %d: lease expired %v before pass %d issued its renewal at %v",
					p, i, time.Unix(0, r.Claim.ExpiryNs), p+1, rec.issuedAt[p+1])
			}
		}
	}
	if n := len(ownedSlots(m)); n != 128 {
		t.Fatalf("held %d slots after the run, want 128", n)
	}
	if !stalled.Load() {
		t.Fatal("the injected stall never ran: no pipeline of pass 2 went through the hook")
	}
}

// instantClaimStore answers the two calls a slot-reconcile pass makes in memory
// (the embedded Store serves only NewManager's key custody), so a test about pass
// timing sees only the delays it injects, never Redis or machine load.
type instantClaimStore struct {
	Store
	delay time.Duration // spent inside ClaimSlots, the pass's claim step
	fail  bool          // every claim errors (Redis unreachable): no lease is written
	busy  bool          // every slot is BUSY: a rival holds it
}

func (f *instantClaimStore) LiveMembers(time.Time) ([]string, error) { return []string{"rA"}, nil }

func (f *instantClaimStore) ClaimSlots(slotKeys []string, _ string, now time.Time, ttl time.Duration) []SlotClaimResult {
	time.Sleep(f.delay)
	out := make([]SlotClaimResult, len(slotKeys))
	for i := range out {
		switch {
		case f.fail:
			out[i].Err = errors.New("injected: redis unreachable")
		case f.busy:
			out[i].Claim = SlotClaim{Status: SlotBusy, Epoch: parseOwnerEpoch("2")}
		default:
			out[i].Claim = SlotClaim{Status: SlotRenewed, Epoch: parseOwnerEpoch("1"), ExpiryNs: now.Add(ttl).UnixNano()}
		}
	}
	return out
}

// TestSlotReconcileWarnsOnLapsedLeases: the Warn fires exactly when a slot's
// claim lands more than slotLeaseTTL after the pass that last wrote that slot's
// lease, the moment the lease could have lapsed (Membership.tla's Tick slot gate,
// INV-MEMBER-01), not merely when a pass is slow. Two passes that each take 0.6
// TTL fit the in-interval proxy (neither exceeds slotLeaseTTL -
// slotReconcileInterval = 0.75 TTL), yet the second lands 1.2 TTL after the
// first's start, so it warns once; the instant pass after it does not. Passes
// whose every claim errors write no lease, so they neither warn nor move the
// anchor: the first pass to land after them warns if the leases it finds were
// written more than a TTL before, whether it renews them or finds a rival
// holding them (BUSY), and a rival's lease is not anchored again.
func TestSlotReconcileWarnsOnLapsedLeases(t *testing.T) {
	const ttl = time.Second
	s, _ := newTestStore(t)
	store := &instantClaimStore{Store: s}
	var logs bytes.Buffer
	m, err := NewManager(store, &fakeStreams{tails: map[string]string{}}, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/", ReplicaID: "rA",
		Logger:         slog.New(slog.NewTextHandler(&logs, nil)),
		MemberLeaseTTL: 4 * ttl, HeartbeatInterval: ttl / 4,
		SlotLeaseTTL: ttl, SlotReconcileInterval: ttl / 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	warns := func() int { return strings.Count(logs.String(), "may already have lapsed") }
	m.RunSlotReconcile() // pass 1: no earlier leases to outlive
	store.delay = 6 * ttl / 10
	m.RunSlotReconcile() // pass 2 lands 0.6 TTL after pass 1 started: inside its leases
	if warns() != 0 {
		t.Fatalf("a pass landing inside the previous pass's leases must not warn, got %d in:\n%s", warns(), logs.String())
	}
	m.RunSlotReconcile() // pass 3 lands 1.2 TTL after pass 2 started: pass 2's leases could have lapsed
	if warns() != 1 {
		t.Fatalf("a pass landing after the previous pass's leases could lapse must warn once, got %d in:\n%s", warns(), logs.String())
	}
	store.delay = 0
	m.RunSlotReconcile() // pass 4 lands 0.6 TTL after pass 3 started
	if warns() != 1 {
		t.Fatalf("a normal pass must not warn, got %d in:\n%s", warns(), logs.String())
	}
	store.fail, store.delay = true, 6*ttl/10
	m.RunSlotReconcile() // pass 5: every claim errors, 0.6 TTL after pass 4 started
	m.RunSlotReconcile() // pass 6: every claim errors, 1.2 TTL after pass 4 started
	if warns() != 1 {
		t.Fatalf("a pass that wrote no lease must not warn, got %d in:\n%s", warns(), logs.String())
	}
	if n := len(ownedSlots(m)); n != 0 {
		t.Fatalf("held %d slots after an all-errored pass, want 0", n)
	}
	store.fail, store.delay = false, 0
	m.RunSlotReconcile() // pass 7 renews leases pass 4 wrote more than a TTL ago
	if warns() != 2 {
		t.Fatalf("the first pass to renew after errored passes must warn once, got %d in:\n%s", warns(), logs.String())
	}
	store.busy, store.delay = true, 12*ttl/10
	m.RunSlotReconcile() // pass 8 lands 1.2 TTL after pass 7 and finds a rival on every slot
	if warns() != 3 {
		t.Fatalf("a pass that finds its lapsed leases taken by a rival must warn once, got %d in:\n%s", warns(), logs.String())
	}
	store.delay = 0
	m.RunSlotReconcile() // pass 9: still BUSY, but no lease of ours is left to lapse
	if warns() != 3 {
		t.Fatalf("a rival's lease must not be anchored, got %d warns in:\n%s", warns(), logs.String())
	}
}

// blockingClaimStore answers a slot-reconcile pass only when released, and
// counts the passes inside ClaimSlots at once.
type blockingClaimStore struct {
	Store
	entered  chan struct{} // one send per ClaimSlots call, on entry
	release  chan struct{} // closed to let every ClaimSlots return
	inFlight atomic.Int32
	peak     atomic.Int32
}

func (f *blockingClaimStore) LiveMembers(time.Time) ([]string, error) { return []string{"rA"}, nil }

func (f *blockingClaimStore) ClaimSlots(slotKeys []string, _ string, now time.Time, ttl time.Duration) []SlotClaimResult {
	n := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for p := f.peak.Load(); n > p && !f.peak.CompareAndSwap(p, n); p = f.peak.Load() {
	}
	f.entered <- struct{}{}
	<-f.release
	out := make([]SlotClaimResult, len(slotKeys))
	for i := range out {
		out[i].Claim = SlotClaim{Status: SlotRenewed, Epoch: parseOwnerEpoch("1"), ExpiryNs: now.Add(ttl).UnixNano()}
	}
	return out
}

// TestSlotReconcilePassesAreSerialized: Promote (and RunSlotReconcile) run a pass
// on the caller's goroutine while the loop runs its own, and two passes in flight
// would race on held and let the older pass's leases, anchored at its earlier
// start, land over the newer pass's. A pass started while one is inside its claim
// step waits for it: at most one pass is ever inside ClaimSlots.
func TestSlotReconcilePassesAreSerialized(t *testing.T) {
	s, _ := newTestStore(t)
	store := &blockingClaimStore{Store: s, entered: make(chan struct{}, 2), release: make(chan struct{})}
	m, err := NewManager(store, &fakeStreams{tails: map[string]string{}}, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/", ReplicaID: "rA",
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); m.RunSlotReconcile() }()
	<-store.entered // the first pass is inside its claim step
	go func() { defer wg.Done(); m.Promote() }()
	select {
	case <-store.entered:
		t.Fatal("a second pass entered ClaimSlots while the first was still inside it")
	case <-time.After(200 * time.Millisecond):
	}
	close(store.release)
	<-store.entered // the second pass runs once the first has finished
	wg.Wait()
	if p := store.peak.Load(); p != 1 {
		t.Fatalf("peak passes inside ClaimSlots = %d, want 1", p)
	}
	if n := len(ownedSlots(m)); n != subSlots {
		t.Fatalf("held %d slots after both passes, want %d", n, subSlots)
	}
}
