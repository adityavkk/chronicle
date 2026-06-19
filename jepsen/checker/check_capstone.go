package main

import (
	"fmt"
	"slices"
	"time"
)

type capstoneViolation struct {
	Property string
	Subject  string
	Reason   string
}

func (v capstoneViolation) String() string {
	if v.Subject == "" {
		return fmt.Sprintf("%s: %s", v.Property, v.Reason)
	}
	return fmt.Sprintf("%s %s: %s", v.Property, v.Subject, v.Reason)
}

type slotIsolationObservation struct {
	Path             string
	Expected         []string
	Actual           []string
	CrossSlotError   bool
	SilentCrossSlot  bool
	ForeignWakeCount int
}

func CheckSlotIsolationT5(obs []slotIsolationObservation) []capstoneViolation {
	var violations []capstoneViolation
	for _, o := range obs {
		expected := sortedCopy(o.Expected)
		actual := sortedCopy(o.Actual)
		if !slices.Equal(expected, actual) {
			violations = append(violations, capstoneViolation{
				Property: "T5", Subject: o.Path,
				Reason: fmt.Sprintf("scatter/gather subscribers %v != reference %v", actual, expected),
			})
		}
		if o.SilentCrossSlot {
			violations = append(violations, capstoneViolation{Property: "T5", Subject: o.Path, Reason: "CROSSSLOT was silent"})
		}
		if o.ForeignWakeCount > 0 {
			violations = append(violations, capstoneViolation{
				Property: "T5", Subject: o.Path,
				Reason: fmt.Sprintf("foreign wakes observed: %d", o.ForeignWakeCount),
			})
		}
	}
	return violations
}

type coverageRecoveryObservation struct {
	SubID              string
	Pending            bool
	WasUnownedAtAppend bool
	AppendAt           time.Time
	DeliveredAt        time.Time
	Bound              time.Duration
}

func CheckCoverageRecoveryL2(obs []coverageRecoveryObservation) []capstoneViolation {
	var violations []capstoneViolation
	for _, o := range obs {
		if !o.Pending || !o.WasUnownedAtAppend {
			continue
		}
		if o.DeliveredAt.IsZero() {
			violations = append(violations, capstoneViolation{Property: "L2", Subject: o.SubID, Reason: "pending append was never delivered"})
			continue
		}
		latency := o.DeliveredAt.Sub(o.AppendAt)
		if latency > o.Bound {
			violations = append(violations, capstoneViolation{
				Property: "L2", Subject: o.SubID,
				Reason: fmt.Sprintf("deliver-append %s exceeds %s", latency, o.Bound),
			})
		}
	}
	return violations
}

type ownershipConvergenceObservation struct {
	Slot             int
	Owners           []string
	AcceptedStaleAck bool
	Oscillated       bool
}

func CheckOwnershipConvergenceL4(obs []ownershipConvergenceObservation) []capstoneViolation {
	var violations []capstoneViolation
	for _, o := range obs {
		subject := fmt.Sprintf("slot=%d", o.Slot)
		if len(o.Owners) != 1 {
			violations = append(violations, capstoneViolation{
				Property: "L4", Subject: subject,
				Reason: fmt.Sprintf("owners after quiescence = %v, want exactly one", o.Owners),
			})
		}
		if o.Oscillated {
			violations = append(violations, capstoneViolation{Property: "L4", Subject: subject, Reason: "owner oscillated after quiescence"})
		}
		if o.AcceptedStaleAck {
			violations = append(violations, capstoneViolation{Property: "L4", Subject: subject, Reason: "stale owner epoch was accepted"})
		}
	}
	return violations
}

type starvationObservation struct {
	SubID   string
	Pending bool
	MaxGap  time.Duration
	Bound   time.Duration
}

func CheckNoStarvationL5(obs []starvationObservation) []capstoneViolation {
	var violations []capstoneViolation
	for _, o := range obs {
		if !o.Pending {
			continue
		}
		if o.MaxGap > o.Bound {
			violations = append(violations, capstoneViolation{
				Property: "L5", Subject: o.SubID,
				Reason: fmt.Sprintf("max inter-delivery gap %s exceeds %s with pending work", o.MaxGap, o.Bound),
			})
		}
	}
	return violations
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	slices.Sort(out)
	return out
}
