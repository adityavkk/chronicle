package main

import "testing"

// These tests exercise the pure T4 effect checker (check_stalegen.go) directly —
// no cluster, no Redis. A stale-generation op must be inert: a non-granting
// status AND a byte-identical durable snapshot.

func TestNoStaleGenEffect_FencedNoMutationIsClean(t *testing.T) {
	// The canonical T4 case: a deposed worker (reqGen=1) acks after a takeover
	// rotated the fence to 2; the ack is FENCED and the snapshot is unchanged.
	obs := []staleGenObservation{
		{sub: "s", op: "ack", reqGen: 1, curGen: 2, status: statusFenced, before: `{gen:2,acked:5}`, after: `{gen:2,acked:5}`},
	}
	if v := CheckNoStaleGenEffect(obs); len(v) != 0 {
		t.Fatalf("expected no violations, got %v", v)
	}
}

func TestNoStaleGenEffect_CurrentGenMutationIsAllowed(t *testing.T) {
	// A current-generation ack (reqGen == curGen) may legitimately mutate — it is
	// not a stale-gen op, so the before/after difference is fine.
	obs := []staleGenObservation{
		{sub: "s", op: "ack", reqGen: 2, curGen: 2, status: statusOK, before: `{gen:2,acked:5}`, after: `{gen:2,acked:9}`},
	}
	if v := CheckNoStaleGenEffect(obs); len(v) != 0 {
		t.Fatalf("expected no violations for a current-gen op, got %v", v)
	}
}

func TestNoStaleGenEffect_StaleGenAcceptedIsViolation(t *testing.T) {
	// A stale-gen op that returned OK is the T4 violation T1's complement catches:
	// the fence should have rejected it.
	obs := []staleGenObservation{
		{sub: "s", op: "ack", reqGen: 1, curGen: 2, status: statusOK, before: `{gen:2,acked:5}`, after: `{gen:2,acked:5}`},
	}
	v := CheckNoStaleGenEffect(obs)
	if len(v) != 1 {
		t.Fatalf("expected one violation, got %d (%v)", len(v), v)
	}
}

func TestNoStaleGenEffect_StaleGenMutationIsViolation(t *testing.T) {
	// Even FENCED is a violation if the durable snapshot changed — a rejected op
	// must write nothing.
	obs := []staleGenObservation{
		{sub: "s", op: "release", reqGen: 1, curGen: 3, status: statusFenced, before: `{gen:3,acked:5}`, after: `{gen:3,acked:8}`},
	}
	v := CheckNoStaleGenEffect(obs)
	if len(v) != 1 {
		t.Fatalf("expected one violation, got %d (%v)", len(v), v)
	}
}

func TestNoStaleGenEffect_BusyAndNoSubAreInert(t *testing.T) {
	// BUSY (a non-idle arm/claim) and NOSUB (deleted sub) are inert stale-gen
	// statuses, clean as long as they mutate nothing.
	obs := []staleGenObservation{
		{sub: "s", op: "arm", reqGen: 1, curGen: 2, status: statusBusy, before: "x", after: "x"},
		{sub: "s", op: "claim", reqGen: 1, curGen: 2, status: statusNoSub, before: "y", after: "y"},
		{sub: "s", op: "ack", reqGen: 1, curGen: 2, status: statusStale, before: "z", after: "z"},
	}
	if v := CheckNoStaleGenEffect(obs); len(v) != 0 {
		t.Fatalf("expected no violations, got %v", v)
	}
}
