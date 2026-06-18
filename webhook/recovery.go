package webhook

import "time"

type recoveryScope uint8

const (
	recoveryScopeBoot recoveryScope = iota + 1
	recoveryScopeReconnect
	recoveryScopeAppendError
	recoveryScopeFloor
	recoveryScopeEpochBump
	recoveryScopeNewOwnerCAS
)

func (s recoveryScope) String() string {
	switch s {
	case recoveryScopeBoot:
		return "boot"
	case recoveryScopeReconnect:
		return "reconnect"
	case recoveryScopeAppendError:
		return "append_error"
	case recoveryScopeFloor:
		return "floor"
	case recoveryScopeEpochBump:
		return "epoch_bump"
	case recoveryScopeNewOwnerCAS:
		return "new_owner_cas"
	default:
		return "unknown"
	}
}

type recoveryPlan struct {
	sweep  bool
	leases bool
}

func (p recoveryPlan) any() bool { return p.sweep || p.leases }

func planRecovery(scope recoveryScope) recoveryPlan {
	switch scope {
	case recoveryScopeBoot, recoveryScopeReconnect, recoveryScopeAppendError, recoveryScopeFloor:
		return recoveryPlan{sweep: true}
	case recoveryScopeEpochBump, recoveryScopeNewOwnerCAS:
		// #14 will provide the ownership event sources and slot scope. Until then,
		// the seam's eager body is the ownership-neutral lease schedule reconcile.
		return recoveryPlan{leases: true}
	default:
		return recoveryPlan{}
	}
}

type leaseReconcileDecision struct {
	reconcile bool
	pending   bool
}

func decideLeaseReconcile(cur ClaimLeaseState, pending bool) leaseReconcileDecision {
	if cur.LeaseUntilNs <= 0 {
		return leaseReconcileDecision{}
	}
	if cur.Phase != PhaseLive && cur.Phase != PhaseWaking {
		return leaseReconcileDecision{}
	}
	return leaseReconcileDecision{reconcile: true, pending: pending}
}

func coverageGapForSweepWake(Subscription, time.Time) (time.Duration, bool) {
	// #14 adds real slot ownership and append-time ownership observations. Until
	// then there is no truthful way to distinguish an unowned-slot coverage gap
	// from an ordinary sweep wake, so this metric wiring is intentionally inert.
	return 0, false
}
