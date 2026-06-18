package webhook

// DueSetEffect is the durable effect a fenced script branch has on the due-set
// outbox. The due set is intentionally tiny: a member is either owed, cleared,
// or untouched by the branch.
type DueSetEffect uint8

const (
	// DueSetNoop leaves the due set unchanged.
	DueSetNoop DueSetEffect = iota
	// DueSetAdd adds or re-scores a subscription as owed.
	DueSetAdd
	// DueSetRemove clears a subscription from the due set.
	DueSetRemove
)

// DueSetMutationDecision is the pure model for ds:{__ds}:due mutations. MetricOp
// is non-empty exactly when the Redis script should have performed the mutation.
type DueSetMutationDecision struct {
	Effect   DueSetEffect
	MetricOp string
}

// Mutates reports whether this branch should touch the due set.
func (d DueSetMutationDecision) Mutates() bool { return d.Effect != DueSetNoop }

func dueSetAdd(op string) DueSetMutationDecision {
	return DueSetMutationDecision{Effect: DueSetAdd, MetricOp: op}
}

func dueSetRemove(op string) DueSetMutationDecision {
	return DueSetMutationDecision{Effect: DueSetRemove, MetricOp: op}
}

// DueSetForArmWake mirrors arm_wake.lua: only a newly ARMED wake owes the sub.
func DueSetForArmWake(status string) DueSetMutationDecision {
	if status == "ARMED" {
		return dueSetAdd("arm")
	}
	return DueSetMutationDecision{}
}

// DueSetForAck mirrors ack.lua: only a successful done ack clears owed work.
// Heartbeats may advance cursors, but the wake remains in flight.
func DueSetForAck(status string, done bool) DueSetMutationDecision {
	if status == "OK" && done {
		return dueSetRemove("ack")
	}
	return DueSetMutationDecision{}
}

// DueSetForExpireLease mirrors expire_lease.lua: an expired lease re-owes the
// subscription when the caller has established pending durable work; otherwise
// it clears the old arm-time due mark because there is no wake left to fire.
func DueSetForExpireLease(status string, pending bool) DueSetMutationDecision {
	if status != "EXPIRED" {
		return DueSetMutationDecision{}
	}
	if pending {
		return dueSetAdd("expire")
	}
	return dueSetRemove("expire")
}

// DueSetForRelease mirrors release.lua: a successful voluntary release clears
// the current in-flight due mark before the caller optionally re-arms if pending.
func DueSetForRelease(status string) DueSetMutationDecision {
	if status == "OK" {
		return dueSetRemove("release")
	}
	return DueSetMutationDecision{}
}

// DueSetForDelete clears any due mark for a deleted subscription. Delete has no
// fenced branch: once the sub hash is gone, future due claims are permanent no-ops.
func DueSetForDelete() DueSetMutationDecision {
	return dueSetRemove("delete")
}
