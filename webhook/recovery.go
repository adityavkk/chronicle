package webhook

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
		// Ownership transfer events eagerly repair volatile lease/due schedules;
		// the coarse floor remains the all-slot cursor backstop.
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
