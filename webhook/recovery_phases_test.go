package webhook

import (
	"reflect"
	"testing"
	"time"
)

func TestRecoveryPipelineOrderAndPolicies(t *testing.T) {
	wantOrder := []RecoveryPhaseKind{
		RestoreLeaseTails,
		ReemitUnstampedPullWakes,
		ReemitStalePullWakes,
		ExpireDueLeases,
		WakeIdlePending,
	}
	gotPipeline := append(recoveryPipeline(), perSubscriptionRecoveryPipeline()...)
	gotOrder := make([]RecoveryPhaseKind, 0, len(gotPipeline))
	for _, phase := range gotPipeline {
		gotOrder = append(gotOrder, phase.Kind)
	}
	if !reflect.DeepEqual(gotOrder, wantOrder) {
		t.Fatalf("recovery pipeline order = %v, want %v", gotOrder, wantOrder)
	}

	wantPolicies := map[RecoveryPhaseKind]RecoveryPhasePolicy{
		RestoreLeaseTails: {
			Invariant:       InvariantLeaseRestore,
			SourceOfTruth:   SourceDurableSubscriptionLeaseAndLeaseZSET,
			Idempotence:     IdempotentRestoreLeaseReZADD,
			DuplicatePolicy: DuplicatePolicyNoExternalDuplicate,
		},
		ReemitUnstampedPullWakes: {
			Invariant:       InvariantRecoverUnstamped,
			SourceOfTruth:   SourcePullWakeUnsentFlag,
			Idempotence:     IdempotentPullWakeFence,
			DuplicatePolicy: DuplicatePolicyClaimFenceSafe,
		},
		ReemitStalePullWakes: {
			Invariant:       InvariantRecoverStale,
			SourceOfTruth:   SourcePullWakeSentTimestamp,
			Idempotence:     IdempotentPullWakeFence,
			DuplicatePolicy: DuplicatePolicyClaimFenceSafe,
		},
		ExpireDueLeases: {
			Invariant:       InvariantLeaseExpiry,
			SourceOfTruth:   SourceDurableLeaseDeadline,
			Idempotence:     IdempotentExpireLeaseFence,
			DuplicatePolicy: DuplicatePolicyCASCoalesces,
		},
		WakeIdlePending: {
			Invariant:       InvariantWakeIdlePending,
			SourceOfTruth:   SourceDurableCursorAndStreamTail,
			Idempotence:     IdempotentArmWakeCAS,
			DuplicatePolicy: DuplicatePolicyCASCoalesces,
		},
	}
	for _, kind := range wantOrder {
		if got := policyForPhase(kind); got != wantPolicies[kind] {
			t.Fatalf("policyForPhase(%s) = %+v, want %+v", kind, got, wantPolicies[kind])
		}
	}
}

func TestRecoveryPhaseDecisions(t *testing.T) {
	now := time.Unix(100, 0)
	begin := "0000000000000000_0000000000000000"
	tail := "0000000000000001_0000000000000000"
	pullCfg := Config{Type: DispatchPullWake, WakeStream: "events/wake", LeaseTTLMs: 1000}
	webhookCfg := Config{Type: DispatchWebhook, WebhookURL: "http://receiver", LeaseTTLMs: 1000}
	pendingLink := []StreamLink{{Path: "events/a", LinkType: LinkExplicit, AckedOffset: begin}}
	expiredAt := now.Add(-time.Nanosecond).UnixNano()
	staleSentAt := now.Add(-time.Hour).UnixNano()

	t.Run("INV-LR-01 restores absent lease tail and carries owed derived from tails", func(t *testing.T) {
		got := DecideRestoreLeaseTails(RecoverySnapshot{
			Subs:   []Subscription{{ID: "s-live", Config: webhookCfg, Phase: PhaseLive, LeaseUntilNs: now.Add(time.Second).UnixNano(), Links: pendingLink}},
			Tails:  map[string]string{"events/a": tail},
			Leased: map[string]struct{}{},
			Now:    now,
		})
		if got.Policy() != policyForPhase(RestoreLeaseTails) {
			t.Fatalf("policy = %+v", got.Policy())
		}
		want := []RestoreLeaseTailDecision{{SubID: "s-live", Owed: true}}
		if !reflect.DeepEqual(got.Restores, want) {
			t.Fatalf("restores = %+v, want %+v", got.Restores, want)
		}
	})

	t.Run("INV-RECOVER-01 reemits unstamped pull-wake with same fence and reason", func(t *testing.T) {
		got := DecideReemitUnstampedPullWakes(RecoverySnapshot{
			Subs: []Subscription{{ID: "s-unsent", Config: pullCfg, Phase: PhaseWaking, Generation: 1, WakeID: "w1", WakeEventSentNs: 0}},
			Now:  now,
		})
		if got.Policy() != policyForPhase(ReemitUnstampedPullWakes) {
			t.Fatalf("policy = %+v", got.Policy())
		}
		if len(got.Reemits) != 1 {
			t.Fatalf("reemits = %+v, want one", got.Reemits)
		}
		d := got.Reemits[0]
		if d.Sub.ID != "s-unsent" || d.Sub.Generation != 1 || d.Sub.WakeID != "w1" || d.Reason != ReemitUnstamped {
			t.Fatalf("reemit = %+v, want id=s-unsent gen=1 wake=w1 reason=%s", d, ReemitUnstamped)
		}
	})

	t.Run("INV-RECOVER-02 reemits stale stamped pull-wake with same fence and reason", func(t *testing.T) {
		got := DecideReemitStalePullWakes(RecoverySnapshot{
			Subs:               []Subscription{{ID: "s-stale", Config: pullCfg, Phase: PhaseWaking, Generation: 2, WakeID: "w2", WakeEventSentNs: staleSentAt}},
			Now:                now,
			StalePullWakeAfter: time.Minute,
		})
		if got.Policy() != policyForPhase(ReemitStalePullWakes) {
			t.Fatalf("policy = %+v", got.Policy())
		}
		if len(got.Reemits) != 1 {
			t.Fatalf("reemits = %+v, want one", got.Reemits)
		}
		d := got.Reemits[0]
		if d.Sub.ID != "s-stale" || d.Sub.Generation != 2 || d.Sub.WakeID != "w2" || d.Sub.WakeEventSentNs != staleSentAt || d.Reason != ReemitStale {
			t.Fatalf("reemit = %+v, want id=s-stale gen=2 wake=w2 sent=%d reason=%s", d, staleSentAt, ReemitStale)
		}
	})

	t.Run("INV-LEASE-01 expires exact non-idle subscription using durable deadline", func(t *testing.T) {
		got := DecideExpireDueLeases(RecoverySnapshot{
			Subs: []Subscription{{ID: "s-expired", Config: webhookCfg, Phase: PhaseLive, LeaseUntilNs: expiredAt}},
			Now:  now,
		})
		if got.Policy() != policyForPhase(ExpireDueLeases) {
			t.Fatalf("policy = %+v", got.Policy())
		}
		want := []ExpireLeaseDecision{{SubID: "s-expired"}}
		if !reflect.DeepEqual(got.Expires, want) {
			t.Fatalf("expires = %+v, want %+v", got.Expires, want)
		}
	})

	t.Run("INV-WAKE-01 wakes exact idle subscription from cursor-tail truth", func(t *testing.T) {
		got := DecideWakeIdlePending(RecoverySnapshot{
			Subs:  []Subscription{{ID: "s-idle", Config: webhookCfg, Phase: PhaseIdle, Generation: 3, WakeID: "old", Links: pendingLink}},
			Tails: map[string]string{"events/a": tail},
			Now:   now,
		})
		if got.Policy() != policyForPhase(WakeIdlePending) {
			t.Fatalf("policy = %+v", got.Policy())
		}
		if len(got.Wakes) != 1 {
			t.Fatalf("wakes = %+v, want one", got.Wakes)
		}
		if got.Wakes[0].Sub.ID != "s-idle" || got.Wakes[0].Sub.Phase != PhaseIdle || !reflect.DeepEqual(got.Wakes[0].Sub.Links, pendingLink) {
			t.Fatalf("wake = %+v, want idle s-idle with pending link", got.Wakes[0])
		}
	})
}

func TestRecoveryPhaseNegativeDecisions(t *testing.T) {
	now := time.Unix(100, 0)
	pullCfg := Config{Type: DispatchPullWake, WakeStream: "events/wake", LeaseTTLMs: 1000}
	webhookCfg := Config{Type: DispatchWebhook, WebhookURL: "http://receiver", LeaseTTLMs: 1000}
	begin := "0000000000000000_0000000000000000"

	cases := []struct {
		name     string
		kind     RecoveryPhaseKind
		snapshot RecoverySnapshot
	}{
		{
			name: string(RestoreLeaseTails) + " leaves present lease tail intact",
			kind: RestoreLeaseTails,
			snapshot: RecoverySnapshot{
				Subs:   []Subscription{{ID: "s", Config: webhookCfg, Phase: PhaseLive, LeaseUntilNs: now.Add(time.Second).UnixNano()}},
				Leased: map[string]struct{}{"s": {}},
				Now:    now,
			},
		},
		{
			name: string(ReemitUnstampedPullWakes) + " ignores stamped pull-wake",
			kind: ReemitUnstampedPullWakes,
			snapshot: RecoverySnapshot{
				Subs: []Subscription{{ID: "s", Config: pullCfg, Phase: PhaseWaking, WakeEventSentNs: now.UnixNano()}},
				Now:  now,
			},
		},
		{
			name: string(ReemitStalePullWakes) + " ignores young stamped pull-wake",
			kind: ReemitStalePullWakes,
			snapshot: RecoverySnapshot{
				Subs:               []Subscription{{ID: "s", Config: pullCfg, Phase: PhaseWaking, WakeEventSentNs: now.Add(-time.Second).UnixNano()}},
				Now:                now,
				StalePullWakeAfter: time.Minute,
			},
		},
		{
			name: string(ExpireDueLeases) + " ignores idle stale deadline",
			kind: ExpireDueLeases,
			snapshot: RecoverySnapshot{
				Subs: []Subscription{{ID: "s", Config: webhookCfg, Phase: PhaseIdle, LeaseUntilNs: now.Add(-time.Second).UnixNano()}},
				Now:  now,
			},
		},
		{
			name: string(WakeIdlePending) + " ignores caught-up cursor",
			kind: WakeIdlePending,
			snapshot: RecoverySnapshot{
				Subs:  []Subscription{{ID: "s", Config: webhookCfg, Phase: PhaseIdle, Links: []StreamLink{{Path: "events/a", AckedOffset: begin}}}},
				Tails: map[string]string{"events/a": begin},
				Now:   now,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if ids := decisionIDs(decidePhase(tc.kind, tc.snapshot)); len(ids) != 0 {
				t.Fatalf("decision ids = %v, want none", ids)
			}
		})
	}
}

func decidePhase(kind RecoveryPhaseKind, s RecoverySnapshot) RecoveryPhaseResult {
	switch kind {
	case RestoreLeaseTails:
		return DecideRestoreLeaseTails(s)
	case ReemitUnstampedPullWakes:
		return DecideReemitUnstampedPullWakes(s)
	case ReemitStalePullWakes:
		return DecideReemitStalePullWakes(s)
	case ExpireDueLeases:
		return DecideExpireDueLeases(s)
	case WakeIdlePending:
		return DecideWakeIdlePending(s)
	default:
		panic("unknown phase")
	}
}

func decisionIDs(r RecoveryPhaseResult) []string {
	switch res := r.(type) {
	case RestoreLeaseTailsResult:
		ids := make([]string, 0, len(res.Restores))
		for _, d := range res.Restores {
			ids = append(ids, d.SubID)
		}
		return ids
	case ReemitUnstampedPullWakesResult:
		ids := make([]string, 0, len(res.Reemits))
		for _, d := range res.Reemits {
			ids = append(ids, d.Sub.ID)
		}
		return ids
	case ReemitStalePullWakesResult:
		ids := make([]string, 0, len(res.Reemits))
		for _, d := range res.Reemits {
			ids = append(ids, d.Sub.ID)
		}
		return ids
	case ExpireDueLeasesResult:
		ids := make([]string, 0, len(res.Expires))
		for _, d := range res.Expires {
			ids = append(ids, d.SubID)
		}
		return ids
	case WakeIdlePendingResult:
		ids := make([]string, 0, len(res.Wakes))
		for _, d := range res.Wakes {
			ids = append(ids, d.Sub.ID)
		}
		return ids
	default:
		panic("unknown result")
	}
}
