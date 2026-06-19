package main

import (
	"strings"
	"testing"
	"time"
)

func TestCheckSlotIsolationT5ClassifiesFanoutAndCrossSlot(t *testing.T) {
	clean := []slotIsolationObservation{{
		Path:           "events/a",
		Expected:       []string{"sub-b", "sub-a"},
		Actual:         []string{"sub-a", "sub-b"},
		CrossSlotError: true,
	}}
	if got := CheckSlotIsolationT5(clean); len(got) != 0 {
		t.Fatalf("clean T5 observation got violations %v", got)
	}

	bad := []slotIsolationObservation{{
		Path:             "events/a",
		Expected:         []string{"sub-a"},
		Actual:           []string{"sub-a", "foreign"},
		SilentCrossSlot:  true,
		ForeignWakeCount: 1,
	}}
	got := CheckSlotIsolationT5(bad)
	if len(got) != 3 {
		t.Fatalf("bad T5 observation got %d violations, want 3: %v", len(got), got)
	}
}

func TestCheckCoverageRecoveryL2ClassifiesBoundedRecovery(t *testing.T) {
	appendAt := time.Unix(100, 0)
	clean := []coverageRecoveryObservation{{
		SubID: "sub-a", Pending: true, WasUnownedAtAppend: true,
		AppendAt: appendAt, DeliveredAt: appendAt.Add(4 * time.Second), Bound: 9 * time.Second,
	}}
	if got := CheckCoverageRecoveryL2(clean); len(got) != 0 {
		t.Fatalf("clean L2 observation got violations %v", got)
	}

	bad := []coverageRecoveryObservation{{
		SubID: "sub-a", Pending: true, WasUnownedAtAppend: true,
		AppendAt: appendAt, DeliveredAt: appendAt.Add(10 * time.Second), Bound: 9 * time.Second,
	}}
	got := CheckCoverageRecoveryL2(bad)
	if len(got) != 1 || !strings.Contains(got[0].String(), "exceeds") {
		t.Fatalf("bad L2 observation = %v, want exceeds violation", got)
	}
}

func TestCheckOwnershipConvergenceL4ClassifiesQuiescence(t *testing.T) {
	clean := []ownershipConvergenceObservation{{Slot: 7, Owners: []string{"replica-a"}}}
	if got := CheckOwnershipConvergenceL4(clean); len(got) != 0 {
		t.Fatalf("clean L4 observation got violations %v", got)
	}

	bad := []ownershipConvergenceObservation{{
		Slot: 7, Owners: []string{"replica-a", "replica-b"}, Oscillated: true, AcceptedStaleAck: true,
	}}
	got := CheckOwnershipConvergenceL4(bad)
	if len(got) != 3 {
		t.Fatalf("bad L4 observation got %d violations, want 3: %v", len(got), got)
	}
}

func TestCheckNoStarvationL5IgnoresNonPendingAndFlagsPendingGap(t *testing.T) {
	obs := []starvationObservation{
		{SubID: "idle", Pending: false, MaxGap: 10 * time.Second, Bound: 3 * time.Second},
		{SubID: "busy", Pending: true, MaxGap: 2 * time.Second, Bound: 3 * time.Second},
	}
	if got := CheckNoStarvationL5(obs); len(got) != 0 {
		t.Fatalf("clean L5 observation got violations %v", got)
	}

	got := CheckNoStarvationL5([]starvationObservation{{
		SubID: "busy", Pending: true, MaxGap: 10 * time.Second, Bound: 3 * time.Second,
	}})
	if len(got) != 1 || !strings.Contains(got[0].String(), "pending work") {
		t.Fatalf("bad L5 observation = %v, want pending-work violation", got)
	}
}
