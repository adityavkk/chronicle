package main

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// Pure unit tests for the gate #5 failover verdict logic (scenario_failover.go) —
// no cluster. The verdict is a total function over frozen inputs, so CI exercises
// the PASS/FAIL + at-least-once-vs-safety framing while the chaos run stays an
// on-demand job. These also pin the correction-#3 discipline: the verdict reads no
// WAIT/WAITAOF count or lease TTL — only the L1 gaps and the FENCED check over the
// monotone fence decide it.

// A clean run: failover injected (RPO >= 0), every stream reached tail, the
// deposed ack was FENCED -> PASS. A positive RPO is NOT a failure.
func TestFailoverVerdict_PassWithPositiveRPO(t *testing.T) {
	r := failoverVerdict(nil, http.StatusConflict, "FENCED", 4096, 3*time.Second, 8)
	if !r.pass {
		t.Fatalf("clean run with a positive RPO must PASS, got %+v", r)
	}
	if r.streamsAtTail != 8 {
		t.Errorf("streamsAtTail = %d, want 8", r.streamsAtTail)
	}
	// A positive RPO is the durability-honest signal the failover dropped writes; the
	// run proves those degraded only to at-least-once. It must surface in the report.
	if !strings.Contains(r.String(), "4096 bytes") {
		t.Errorf("verdict must report the empirical RPO, got:\n%s", r.String())
	}
	if !strings.Contains(r.String(), "NOT a strong-consistency claim") {
		t.Errorf("verdict must state the at-least-once framing explicitly, got:\n%s", r.String())
	}
}

// A zero-RPO run (Tier B WAITAOF 1 1 acked the replica fsync before the kill) still
// PASSes — and is the durability-honest best case.
func TestFailoverVerdict_PassWithZeroRPO(t *testing.T) {
	r := failoverVerdict(nil, http.StatusConflict, "FENCED", 0, 1200*time.Millisecond, 4)
	if !r.pass {
		t.Fatalf("zero-RPO clean run must PASS, got %+v", r)
	}
}

// An L1 gap (a stream never reached tail) is a real at-least-once VIOLATION — a
// dropped fence-write that was never re-fired (a lost update). It must FAIL even if
// the deposed ack was fenced.
func TestFailoverVerdict_GapIsLostUpdate(t *testing.T) {
	gaps := []deliveryGap{{path: "events/fo-2", want: "0000000000000040_0", got: "0000000000000031_0", msgs: 40}}
	r := failoverVerdict(gaps, http.StatusConflict, "FENCED", 0, 2*time.Second, 4)
	if r.pass {
		t.Fatalf("an L1 gap must FAIL (a lost update, not at-least-once), got PASS: %+v", r)
	}
	if r.streamsAtTail != 3 {
		t.Errorf("streamsAtTail = %d, want 3", r.streamsAtTail)
	}
}

// The decisive safety case: the deposed worker's late ack was NOT fenced (it
// returned 200 OK) — a double-grant / cursor regression survived the promotion.
// This is the INV-FENCE-01 violation the whole scenario exists to catch. It must
// FAIL even with zero gaps and zero RPO.
func TestFailoverVerdict_UnfencedDeposedAckIsSafetyViolation(t *testing.T) {
	r := failoverVerdict(nil, http.StatusOK, "", 0, 2*time.Second, 4)
	if r.pass {
		t.Fatal("an unfenced deposed ack (200 OK across the promotion) is a SAFETY violation and must FAIL")
	}
	if r.deposedFenced {
		t.Error("deposedFenced must be false for a 200 OK late ack")
	}
}

// A 409 with a non-FENCED code is not the fence verdict we require.
func TestFailoverVerdict_WrongCodeIsNotFenced(t *testing.T) {
	r := failoverVerdict(nil, http.StatusConflict, "ALREADY_CLAIMED", 0, time.Second, 2)
	if r.pass || r.deposedFenced {
		t.Fatalf("409 ALREADY_CLAIMED is not the FENCED verdict; must FAIL, got %+v", r)
	}
}

// A failed injection (rpoBytes = -1, the single-Redis rig has no replica to
// promote) must FAIL honestly rather than fake a result, and say so in the report.
func TestFailoverVerdict_FailedInjection(t *testing.T) {
	r := failoverVerdict(nil, http.StatusConflict, "FENCED", -1, 0, 4)
	if r.pass {
		t.Fatal("a failed failover injection must FAIL (refuse to fake a cloud result)")
	}
	if !strings.Contains(r.String(), "injection failed") {
		t.Errorf("verdict must report the failed injection honestly, got:\n%s", r.String())
	}
}
