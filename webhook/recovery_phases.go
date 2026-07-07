package webhook

import "time"

// RecoveryInvariant names the crash-safety invariant pinned by a recovery phase.
type RecoveryInvariant string

const (
	// InvariantLeaseRestore pins lease-tail restoration.
	InvariantLeaseRestore RecoveryInvariant = "INV-LR-01"
	// InvariantRecoverUnstamped pins unstamped pull-wake reemit.
	InvariantRecoverUnstamped RecoveryInvariant = "INV-RECOVER-01"
	// InvariantRecoverStale pins stale stamped pull-wake reemit.
	InvariantRecoverStale RecoveryInvariant = "INV-RECOVER-02"
	// InvariantLeaseExpiry pins due lease expiry.
	InvariantLeaseExpiry RecoveryInvariant = "INV-LEASE-01"
	// InvariantWakeIdlePending pins idle-pending wake recovery.
	InvariantWakeIdlePending RecoveryInvariant = "INV-WAKE-01"
)

// RecoverySourceOfTruth names the durable state a phase trusts.
type RecoverySourceOfTruth string

const (
	// SourceDurableSubscriptionLeaseAndLeaseZSET compares durable sub leases with the lease schedule.
	SourceDurableSubscriptionLeaseAndLeaseZSET RecoverySourceOfTruth = "subscription hash phase/lease_until_ns + lease ZSET membership"
	// SourcePullWakeUnsentFlag trusts the unstamped pull-wake flag.
	SourcePullWakeUnsentFlag RecoverySourceOfTruth = "subscription hash wake_event_sent_ns == 0"
	// SourcePullWakeSentTimestamp trusts the stamped pull-wake timestamp.
	SourcePullWakeSentTimestamp RecoverySourceOfTruth = "subscription hash wake_event_sent_ns timestamp"
	// SourceDurableLeaseDeadline trusts the stored lease deadline.
	SourceDurableLeaseDeadline RecoverySourceOfTruth = "subscription hash lease_until_ns"
	// SourceDurableCursorAndStreamTail compares subscription cursors with stream tails.
	SourceDurableCursorAndStreamTail RecoverySourceOfTruth = "subscription link cursors + stream tails"
)

// RecoveryIdempotence names why re-running a phase is safe.
type RecoveryIdempotence string

const (
	// IdempotentRestoreLeaseReZADD describes restore_lease idempotence.
	IdempotentRestoreLeaseReZADD RecoveryIdempotence = "restore_lease re-ZADDs from durable hash and no-ops unless still live/waking"
	// IdempotentPullWakeFence describes duplicate pull-wake safety.
	IdempotentPullWakeFence RecoveryIdempotence = "same generation/wake_id; duplicate events are claim-fenced"
	// IdempotentExpireLeaseFence describes expire_lease idempotence.
	IdempotentExpireLeaseFence RecoveryIdempotence = "expire_lease only flips an actually expired current lease"
	// IdempotentArmWakeCAS describes arm_wake idempotence.
	IdempotentArmWakeCAS RecoveryIdempotence = "arm_wake CAS only arms idle subscriptions; busy coalesces"
)

// RecoveryDuplicatePolicy names the duplicate side-effect policy of a phase.
type RecoveryDuplicatePolicy string

const (
	// DuplicatePolicyNoExternalDuplicate allows no external duplicate side effect.
	DuplicatePolicyNoExternalDuplicate RecoveryDuplicatePolicy = "no external duplicate; schedule repair only"
	// DuplicatePolicyClaimFenceSafe permits duplicates guarded by the claim fence.
	DuplicatePolicyClaimFenceSafe RecoveryDuplicatePolicy = "duplicates allowed; (generation,wake_id) claim fence coalesces"
	// DuplicatePolicyCASCoalesces permits duplicate attempts that the store CAS coalesces.
	DuplicatePolicyCASCoalesces RecoveryDuplicatePolicy = "duplicates attempted; store CAS coalesces to BUSY/ACTIVE"
)

// RecoveryPhaseKind is the ordered recovery pipeline vocabulary.
type RecoveryPhaseKind string

const (
	// RestoreLeaseTails re-derives lost lease-ZSET members.
	RestoreLeaseTails RecoveryPhaseKind = "RestoreLeaseTails"
	// ReemitUnstampedPullWakes re-appends pull-wake events missing an emit stamp.
	ReemitUnstampedPullWakes RecoveryPhaseKind = "ReemitUnstampedPullWakes"
	// ReemitStalePullWakes re-appends old stamped pull-wake events that were never claimed.
	ReemitStalePullWakes RecoveryPhaseKind = "ReemitStalePullWakes"
	// ExpireDueLeases expires subscriptions whose lease deadline passed.
	ExpireDueLeases RecoveryPhaseKind = "ExpireDueLeases"
	// WakeIdlePending wakes idle subscriptions with pending cursor work.
	WakeIdlePending RecoveryPhaseKind = "WakeIdlePending"
)

// RecoveryPhasePolicy makes each phase's source-of-truth, idempotence, duplicate
// policy, and pinned invariant data rather than a comment.
type RecoveryPhasePolicy struct {
	Invariant       RecoveryInvariant
	SourceOfTruth   RecoverySourceOfTruth
	Idempotence     RecoveryIdempotence
	DuplicatePolicy RecoveryDuplicatePolicy
}

type phasePolicyCarrier struct {
	Kind            RecoveryPhaseKind
	Invariant       RecoveryInvariant
	SourceOfTruth   RecoverySourceOfTruth
	Idempotence     RecoveryIdempotence
	DuplicatePolicy RecoveryDuplicatePolicy
}

func newPhasePolicyCarrier(kind RecoveryPhaseKind, p RecoveryPhasePolicy) phasePolicyCarrier {
	return phasePolicyCarrier{
		Kind:            kind,
		Invariant:       p.Invariant,
		SourceOfTruth:   p.SourceOfTruth,
		Idempotence:     p.Idempotence,
		DuplicatePolicy: p.DuplicatePolicy,
	}
}

func (p phasePolicyCarrier) Policy() RecoveryPhasePolicy {
	return RecoveryPhasePolicy{
		Invariant:       p.Invariant,
		SourceOfTruth:   p.SourceOfTruth,
		Idempotence:     p.Idempotence,
		DuplicatePolicy: p.DuplicatePolicy,
	}
}

func policyForPhase(kind RecoveryPhaseKind) RecoveryPhasePolicy {
	switch kind {
	case RestoreLeaseTails:
		return RecoveryPhasePolicy{
			Invariant:       InvariantLeaseRestore,
			SourceOfTruth:   SourceDurableSubscriptionLeaseAndLeaseZSET,
			Idempotence:     IdempotentRestoreLeaseReZADD,
			DuplicatePolicy: DuplicatePolicyNoExternalDuplicate,
		}
	case ReemitUnstampedPullWakes:
		return RecoveryPhasePolicy{
			Invariant:       InvariantRecoverUnstamped,
			SourceOfTruth:   SourcePullWakeUnsentFlag,
			Idempotence:     IdempotentPullWakeFence,
			DuplicatePolicy: DuplicatePolicyClaimFenceSafe,
		}
	case ReemitStalePullWakes:
		return RecoveryPhasePolicy{
			Invariant:       InvariantRecoverStale,
			SourceOfTruth:   SourcePullWakeSentTimestamp,
			Idempotence:     IdempotentPullWakeFence,
			DuplicatePolicy: DuplicatePolicyClaimFenceSafe,
		}
	case ExpireDueLeases:
		return RecoveryPhasePolicy{
			Invariant:       InvariantLeaseExpiry,
			SourceOfTruth:   SourceDurableLeaseDeadline,
			Idempotence:     IdempotentExpireLeaseFence,
			DuplicatePolicy: DuplicatePolicyCASCoalesces,
		}
	case WakeIdlePending:
		return RecoveryPhasePolicy{
			Invariant:       InvariantWakeIdlePending,
			SourceOfTruth:   SourceDurableCursorAndStreamTail,
			Idempotence:     IdempotentArmWakeCAS,
			DuplicatePolicy: DuplicatePolicyCASCoalesces,
		}
	default:
		panic("unknown recovery phase")
	}
}

// RecoverySnapshot is the immutable input to phase decision functions. handledIDs
// is explicit pipeline data: a phase that re-emits a pull-wake preserves the old
// single-loop continue semantics by marking the subscription handled for later
// phases in this pass.
type RecoverySnapshot struct {
	Subs               []Subscription
	Tails              map[string]string
	Leased             map[string]struct{}
	Now                time.Time
	StalePullWakeAfter time.Duration
	handledIDs         map[string]struct{}
}

func (s RecoverySnapshot) handled(id string) bool {
	_, ok := s.handledIDs[id]
	return ok
}

func (s RecoverySnapshot) withHandled(ids []string) RecoverySnapshot {
	if len(ids) == 0 {
		return s
	}
	next := s
	next.handledIDs = make(map[string]struct{}, len(s.handledIDs)+len(ids))
	for id := range s.handledIDs {
		next.handledIDs[id] = struct{}{}
	}
	for _, id := range ids {
		next.handledIDs[id] = struct{}{}
	}
	return next
}

func (s RecoverySnapshot) withPhase(id string, phase Phase) RecoverySnapshot {
	next := s
	next.Subs = make([]Subscription, len(s.Subs))
	copy(next.Subs, s.Subs)
	for i := range next.Subs {
		if next.Subs[i].ID == id {
			next.Subs[i].Phase = phase
			break
		}
	}
	return next
}

func (s RecoverySnapshot) onlySub(id string) RecoverySnapshot {
	next := s
	next.Subs = nil
	for _, sub := range s.Subs {
		if sub.ID == id {
			next.Subs = []Subscription{sub}
			break
		}
	}
	return next
}

// RecoveryPhaseResult is the common interface for typed phase results.
type RecoveryPhaseResult interface {
	PhaseKind() RecoveryPhaseKind
	Policy() RecoveryPhasePolicy
}

// RestoreLeaseTailDecision requests a RestoreLease call for one subscription.
type RestoreLeaseTailDecision struct {
	SubID string
	Owed  bool
}

// RestoreLeaseTailsResult is the typed output of DecideRestoreLeaseTails.
type RestoreLeaseTailsResult struct {
	phasePolicyCarrier
	Restores []RestoreLeaseTailDecision
}

// PhaseKind returns RestoreLeaseTails.
func (r RestoreLeaseTailsResult) PhaseKind() RecoveryPhaseKind { return r.Kind }

// ReemitPullWakeReason names why a pull-wake is re-emitted.
type ReemitPullWakeReason string

const (
	// ReemitUnstamped means the wake event was never durably stamped.
	ReemitUnstamped ReemitPullWakeReason = "unstamped"
	// ReemitStale means the stamped event aged out unclaimed.
	ReemitStale ReemitPullWakeReason = "stale"
)

// ReemitPullWakeDecision requests a duplicate-safe wake-stream append.
type ReemitPullWakeDecision struct {
	Sub    Subscription
	Reason ReemitPullWakeReason
}

// ReemitUnstampedPullWakesResult is the typed output of DecideReemitUnstampedPullWakes.
type ReemitUnstampedPullWakesResult struct {
	phasePolicyCarrier
	Reemits []ReemitPullWakeDecision
}

// PhaseKind returns ReemitUnstampedPullWakes.
func (r ReemitUnstampedPullWakesResult) PhaseKind() RecoveryPhaseKind { return r.Kind }

// ReemitStalePullWakesResult is the typed output of DecideReemitStalePullWakes.
type ReemitStalePullWakesResult struct {
	phasePolicyCarrier
	Reemits []ReemitPullWakeDecision
}

// PhaseKind returns ReemitStalePullWakes.
func (r ReemitStalePullWakesResult) PhaseKind() RecoveryPhaseKind { return r.Kind }

// ExpireLeaseDecision requests expiry of one due lease.
type ExpireLeaseDecision struct {
	SubID string
}

// ExpireDueLeasesResult is the typed output of DecideExpireDueLeases.
type ExpireDueLeasesResult struct {
	phasePolicyCarrier
	Expires []ExpireLeaseDecision
}

// PhaseKind returns ExpireDueLeases.
func (r ExpireDueLeasesResult) PhaseKind() RecoveryPhaseKind { return r.Kind }

// WakeIdleDecision requests a wake for one idle pending subscription.
type WakeIdleDecision struct {
	Sub Subscription
}

// WakeIdlePendingResult is the typed output of DecideWakeIdlePending.
type WakeIdlePendingResult struct {
	phasePolicyCarrier
	Wakes []WakeIdleDecision
}

// PhaseKind returns WakeIdlePending.
func (r WakeIdlePendingResult) PhaseKind() RecoveryPhaseKind { return r.Kind }

// DecideRestoreLeaseTails is the pure core for restoring lease-ZSET tails lost
// across failover.
func DecideRestoreLeaseTails(s RecoverySnapshot) RestoreLeaseTailsResult {
	res := RestoreLeaseTailsResult{phasePolicyCarrier: newPhasePolicyCarrier(RestoreLeaseTails, policyForPhase(RestoreLeaseTails))}
	for _, sub := range s.Subs {
		if s.handled(sub.ID) {
			continue
		}
		_, inLease := s.Leased[sub.ID]
		if DecideLeaseReconcile(sub.Phase, sub.LeaseUntilNs, inLease) != LeaseStranded {
			continue
		}
		res.Restores = append(res.Restores, RestoreLeaseTailDecision{
			SubID: sub.ID,
			Owed:  HasPendingWorkFrom(sub.Links, s.Tails),
		})
	}
	return res
}

// DecideReemitUnstampedPullWakes finds pull-wakes armed but not stamped as
// durably emitted.
func DecideReemitUnstampedPullWakes(s RecoverySnapshot) ReemitUnstampedPullWakesResult {
	res := ReemitUnstampedPullWakesResult{phasePolicyCarrier: newPhasePolicyCarrier(ReemitUnstampedPullWakes, policyForPhase(ReemitUnstampedPullWakes))}
	for _, sub := range s.Subs {
		if s.handled(sub.ID) {
			continue
		}
		if sub.Config.Type == DispatchPullWake && sub.Phase == PhaseWaking && sub.WakeEventSentNs == 0 {
			res.Reemits = append(res.Reemits, ReemitPullWakeDecision{Sub: sub, Reason: ReemitUnstamped})
		}
	}
	return res
}

// DecideReemitStalePullWakes finds pull-wakes whose durable wake event was sent
// but never claimed before the staleness window elapsed.
func DecideReemitStalePullWakes(s RecoverySnapshot) ReemitStalePullWakesResult {
	res := ReemitStalePullWakesResult{phasePolicyCarrier: newPhasePolicyCarrier(ReemitStalePullWakes, policyForPhase(ReemitStalePullWakes))}
	for _, sub := range s.Subs {
		if s.handled(sub.ID) {
			continue
		}
		if sub.Config.Type == DispatchPullWake && sub.Phase == PhaseWaking && sub.WakeEventSentNs != 0 &&
			s.Now.UnixNano()-sub.WakeEventSentNs > s.StalePullWakeAfter.Nanoseconds() {
			res.Reemits = append(res.Reemits, ReemitPullWakeDecision{Sub: sub, Reason: ReemitStale})
		}
	}
	return res
}

// DecideExpireDueLeases finds non-idle subscriptions whose lease deadline has
// passed.
func DecideExpireDueLeases(s RecoverySnapshot) ExpireDueLeasesResult {
	res := ExpireDueLeasesResult{phasePolicyCarrier: newPhasePolicyCarrier(ExpireDueLeases, policyForPhase(ExpireDueLeases))}
	for _, sub := range s.Subs {
		if s.handled(sub.ID) {
			continue
		}
		if sub.Phase != PhaseIdle && LeaseExpired(sub.LeaseUntilNs, s.Now) {
			res.Expires = append(res.Expires, ExpireLeaseDecision{SubID: sub.ID})
		}
	}
	return res
}

// DecideWakeIdlePending finds idle subscriptions whose durable cursors lag the
// current stream tails.
func DecideWakeIdlePending(s RecoverySnapshot) WakeIdlePendingResult {
	res := WakeIdlePendingResult{phasePolicyCarrier: newPhasePolicyCarrier(WakeIdlePending, policyForPhase(WakeIdlePending))}
	for _, sub := range s.Subs {
		if s.handled(sub.ID) {
			continue
		}
		if sub.Phase == PhaseIdle && HasPendingWorkFrom(sub.Links, s.Tails) {
			res.Wakes = append(res.Wakes, WakeIdleDecision{Sub: sub})
		}
	}
	return res
}

type recoveryPhase struct {
	Kind   RecoveryPhaseKind
	Decide func(RecoverySnapshot) RecoveryPhaseResult
	Apply  func(*Manager, RecoverySnapshot, RecoveryPhaseResult, time.Time) (RecoverySnapshot, int)
}

func recoveryPipeline() []recoveryPhase {
	return []recoveryPhase{
		{Kind: RestoreLeaseTails, Decide: func(s RecoverySnapshot) RecoveryPhaseResult { return DecideRestoreLeaseTails(s) }, Apply: applyRestoreLeaseTails},
	}
}

func perSubscriptionRecoveryPipeline() []recoveryPhase {
	return []recoveryPhase{
		{Kind: ReemitUnstampedPullWakes, Decide: func(s RecoverySnapshot) RecoveryPhaseResult { return DecideReemitUnstampedPullWakes(s) }, Apply: applyReemitUnstampedPullWakes},
		{Kind: ReemitStalePullWakes, Decide: func(s RecoverySnapshot) RecoveryPhaseResult { return DecideReemitStalePullWakes(s) }, Apply: applyReemitStalePullWakes},
		{Kind: ExpireDueLeases, Decide: func(s RecoverySnapshot) RecoveryPhaseResult { return DecideExpireDueLeases(s) }, Apply: applyExpireDueLeases},
		{Kind: WakeIdlePending, Decide: func(s RecoverySnapshot) RecoveryPhaseResult { return DecideWakeIdlePending(s) }, Apply: applyWakeIdlePending},
	}
}

func applyRestoreLeaseTails(m *Manager, s RecoverySnapshot, result RecoveryPhaseResult, _ time.Time) (RecoverySnapshot, int) {
	res := result.(RestoreLeaseTailsResult)
	for _, d := range res.Restores {
		status, err := m.store.RestoreLease(d.SubID, d.Owed, s.Now)
		if err != nil {
			m.log.Warn("webhook: restore stranded lease", "sub", d.SubID, "error", err)
			continue
		}
		_ = status
	}
	return s, 0
}

func applyReemitUnstampedPullWakes(m *Manager, s RecoverySnapshot, result RecoveryPhaseResult, _ time.Time) (RecoverySnapshot, int) {
	res := result.(ReemitUnstampedPullWakesResult)
	return applyPullWakeReemits(m, s, res.Reemits)
}

func applyReemitStalePullWakes(m *Manager, s RecoverySnapshot, result RecoveryPhaseResult, _ time.Time) (RecoverySnapshot, int) {
	res := result.(ReemitStalePullWakesResult)
	return applyPullWakeReemits(m, s, res.Reemits)
}

func (d ReemitPullWakeDecision) externalization() DurableExternalization {
	if d.Reason == ReemitStale {
		return durablePullWakeStampedEmit()
	}
	return durablePullWakeUnstampedEmit()
}

func applyPullWakeReemits(m *Manager, s RecoverySnapshot, reemits []ReemitPullWakeDecision) (RecoverySnapshot, int) {
	if len(reemits) == 0 {
		return s, 0
	}
	handled := make([]string, 0, len(reemits))
	for _, d := range reemits {
		m.writeWakeEventExternalized(d.externalization(), d.Sub, "", d.Sub.Generation, d.Sub.WakeID)
		handled = append(handled, d.Sub.ID)
	}
	return s.withHandled(handled), len(reemits)
}

func applyExpireDueLeases(m *Manager, s RecoverySnapshot, result RecoveryPhaseResult, _ time.Time) (RecoverySnapshot, int) {
	res := result.(ExpireDueLeasesResult)
	for _, d := range res.Expires {
		if status, err := m.expireLeaseUnscoped(d.SubID, s.Now); err == nil && status == "EXPIRED" {
			s = s.withPhase(d.SubID, PhaseIdle)
		}
	}
	return s, 0
}

func applyWakeIdlePending(m *Manager, s RecoverySnapshot, result RecoveryPhaseResult, start time.Time) (RecoverySnapshot, int) {
	res := result.(WakeIdlePendingResult)
	wakes := 0
	for _, d := range res.Wakes {
		if m.issueWake(d.Sub, "") {
			m.metrics.CoverageGap(time.Since(start))
			wakes++
		}
	}
	return s, wakes
}
