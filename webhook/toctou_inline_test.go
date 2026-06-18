package webhook

import (
	"testing"
	"time"
)

// toctou_inline_test.go proves the TOCTOU resolution (issue #14, 05:372-385): each
// schedule-/due-mutating script (arm_wake, ack, expire_lease, schedule_retry,
// release) inlines the owner-epoch check and FENCES a stale (deposed) owner-scoped
// caller — atomically with the write, not via a separate round-trip. Against live
// Redis (skipped under -short).
//
// The isolation trick for ack/release: present a VALID (gen, wake_id) so the
// (gen,wake_id) fence cannot fire, then show the call still FENCES under a stale
// slot epoch — the FENCED can only be the inlined owner-epoch fence, proving it is
// layered ABOVE the (gen,wake_id) fence, not the same check. A non-stale scope by
// the current owner is NOT fenced (the control), confirming the gate is the epoch,
// not merely "a scope was passed".

// ownedAndDeposed sets up slot 0 owned by A (epoch e1), then taken over by B
// (epoch e2 > e1 after expiry). It returns A's now-stale scope and B's live scope.
func ownedAndDeposed(t *testing.T, s *RedisStore) (stale, live OwnerScope) {
	t.Helper()
	key := slotKey(0)
	t0 := time.Unix(1_700_000_000, 0)
	a, err := s.ClaimSlot(key, "A", t0, slotTTL)
	if err != nil {
		t.Fatalf("claim A: %v", err)
	}
	b, err := s.ClaimSlot(key, "B", t0.Add(slotTTL+time.Second), slotTTL)
	if err != nil {
		t.Fatalf("takeover B: %v", err)
	}
	if b.Epoch.Value() <= a.Epoch.Value() {
		t.Fatalf("takeover did not bump epoch: %d -> %d", a.Epoch.Value(), b.Epoch.Value())
	}
	return OwnerScope{SlotKey: key, ReplicaID: "A", Epoch: a.Epoch.String()},
		OwnerScope{SlotKey: key, ReplicaID: "B", Epoch: b.Epoch.String()}
}

func TestInlineFence_ArmWake(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	stale, live := ownedAndDeposed(t, s)

	// Current owner (B) is not fenced: a fresh idle sub arms.
	res, err := s.ArmWake("s1", time.Now(), 1000, true, "wk-1", live)
	if err != nil || res.Fenced {
		t.Fatalf("live-owner arm = %+v err=%v, want not fenced", res, err)
	}
	if !res.Armed {
		t.Fatalf("live-owner arm should ARM an idle sub, got %+v", res)
	}
	// Deposed owner (A, stale epoch) is FENCED inline.
	res, err = s.ArmWake("s1", time.Now(), 1000, true, "wk-2", stale)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Fenced {
		t.Fatalf("deposed arm = %+v, want Fenced", res)
	}
}

func TestInlineFence_AckAboveGenFence(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	stale, live := ownedAndDeposed(t, s)

	now := time.Now()
	arm, err := s.ArmWake("s1", now, 60000, true, "wk-1") // webhook: lease armed; gen/wake minted
	if err != nil || !arm.Armed {
		t.Fatalf("arm = %+v err=%v", arm, err)
	}
	// VALID gen/wake → the (gen,wake_id) fence will NOT fire. A heartbeat ack by the
	// current owner is OK (the owner check passes).
	st, err := s.Ack("s1", arm.Generation, arm.WakeID, arm.Generation, false, nil, now, 60000, live)
	if err != nil {
		t.Fatal(err)
	}
	if st != "OK" {
		t.Fatalf("live-owner heartbeat ack = %q, want OK (gen valid, owner current)", st)
	}
	// Same VALID gen/wake, but a STALE slot epoch → FENCED can only be the inlined
	// owner-epoch fence, since the (gen,wake_id) fence would have passed.
	st, err = s.Ack("s1", arm.Generation, arm.WakeID, arm.Generation, false, nil, now, 60000, stale)
	if err != nil {
		t.Fatal(err)
	}
	if st != "FENCED" {
		t.Fatalf("deposed ack with valid gen = %q, want FENCED (inline owner fence above the gen fence)", st)
	}
}

func TestInlineFence_ReleaseAboveGenFence(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	stale, live := ownedAndDeposed(t, s)

	now := time.Now()
	arm, _ := s.ArmWake("s1", now, 60000, true, "wk-1")
	// Valid gen/wake: current owner releases OK.
	if st, err := s.Release("s1", arm.Generation, arm.WakeID, arm.Generation, live); err != nil || st != "OK" {
		t.Fatalf("live-owner release = %q/%v, want OK", st, err)
	}
	// Re-arm and prove the deposed release with a valid gen is still FENCED inline.
	arm, _ = s.ArmWake("s1", now, 60000, true, "wk-2")
	st, err := s.Release("s1", arm.Generation, arm.WakeID, arm.Generation, stale)
	if err != nil {
		t.Fatal(err)
	}
	if st != "FENCED" {
		t.Fatalf("deposed release with valid gen = %q, want FENCED (inline owner fence)", st)
	}
}

func TestInlineFence_ExpireLease(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	stale, live := ownedAndDeposed(t, s)
	now := time.Now()
	s.ArmWake("s1", now, 60000, true, "wk-1") // lease armed, not yet expired

	// Current owner: not fenced (lease still active).
	if st, err := s.ExpireLease("s1", now, live); err != nil || st == "FENCED" {
		t.Fatalf("live-owner expire = %q/%v, want non-FENCED (ACTIVE)", st, err)
	}
	// Deposed owner: FENCED inline, before any ZREM/ZADD.
	st, err := s.ExpireLease("s1", now, stale)
	if err != nil {
		t.Fatal(err)
	}
	if st != "FENCED" {
		t.Fatalf("deposed expire = %q, want FENCED", st)
	}
}

func TestInlineFence_ScheduleRetry(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	stale, live := ownedAndDeposed(t, s)
	now := time.Now()

	// Current owner schedules a retry (count advances).
	n, err := s.ScheduleRetry("s1", now, now.Add(time.Minute), live)
	if err != nil || n < 1 {
		t.Fatalf("live-owner schedule_retry = %d/%v, want count>=1", n, err)
	}
	// Deposed owner: FENCED inline → schedules nothing (count 0) while the sub still
	// exists (so 0 is the fence, not NOSUB).
	n, err = s.ScheduleRetry("s1", now, now.Add(time.Minute), stale)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("deposed schedule_retry = %d, want 0 (FENCED inline)", n)
	}
}

// The external/hot path passes no OwnerScope: the inline check is skipped entirely
// (epoch ”), so behavior is byte-for-byte today's — even when a foreign owner
// holds the slot. This is the load-balanced-ack guarantee.
func TestInlineFence_NoScopeSkipsCheck(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.CreateOrConfirm("s1", webhookCfg("https://w.example/h"), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	// A foreign replica owns the slot.
	if _, err := s.ClaimSlot(slotKey(0), "stranger", time.Unix(1_700_000_000, 0), slotTTL); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	// No OwnerScope → arm proceeds regardless of who owns the slot.
	arm, err := s.ArmWake("s1", now, 60000, true, "wk-1")
	if err != nil || !arm.Armed {
		t.Fatalf("no-scope arm = %+v err=%v, want ARMED (check skipped)", arm, err)
	}
	// No-scope heartbeat ack with valid gen → OK, unaffected by the foreign owner.
	if st, err := s.Ack("s1", arm.Generation, arm.WakeID, arm.Generation, false, nil, now, 60000); err != nil || st != "OK" {
		t.Fatalf("no-scope ack = %q/%v, want OK", st, err)
	}
}
