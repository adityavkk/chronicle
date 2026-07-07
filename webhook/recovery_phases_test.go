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
	gotPipeline := recoveryPipeline()
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

	cases := []struct {
		name       string
		kind       RecoveryPhaseKind
		snapshot   RecoverySnapshot
		wantIDs    []string
		wantPolicy RecoveryPhasePolicy
	}{
		{
			name: "INV-LR-01 restores absent lease tail from durable lease hash and marks owed from tails",
			kind: RestoreLeaseTails,
			snapshot: RecoverySnapshot{
				Subs:   []Subscription{{ID: "s-live", Config: webhookCfg, Phase: PhaseLive, LeaseUntilNs: now.Add(time.Second).UnixNano(), Links: pendingLink}},
				Tails:  map[string]string{"events/a": tail},
				Leased: map[string]struct{}{},
				Now:    now,
			},
			wantIDs:    []string{"s-live"},
			wantPolicy: policyForPhase(RestoreLeaseTails),
		},
		{
			name: "INV-RECOVER-01 reemits unstamped pull-wake from wake_event_sent_ns == 0",
			kind: ReemitUnstampedPullWakes,
			snapshot: RecoverySnapshot{
				Subs: []Subscription{{ID: "s-unsent", Config: pullCfg, Phase: PhaseWaking, Generation: 1, WakeID: "w1", WakeEventSentNs: 0}},
				Now:  now,
			},
			wantIDs:    []string{"s-unsent"},
			wantPolicy: policyForPhase(ReemitUnstampedPullWakes),
		},
		{
			name: "INV-RECOVER-02 reemits stale stamped pull-wake with same fence",
			kind: ReemitStalePullWakes,
			snapshot: RecoverySnapshot{
				Subs:               []Subscription{{ID: "s-stale", Config: pullCfg, Phase: PhaseWaking, Generation: 2, WakeID: "w2", WakeEventSentNs: now.Add(-time.Hour).UnixNano()}},
				Now:                now,
				StalePullWakeAfter: time.Minute,
			},
			wantIDs:    []string{"s-stale"},
			wantPolicy: policyForPhase(ReemitStalePullWakes),
		},
		{
			name: "INV-LEASE-01 expires non-idle subscription using durable lease deadline",
			kind: ExpireDueLeases,
			snapshot: RecoverySnapshot{
				Subs: []Subscription{{ID: "s-expired", Config: webhookCfg, Phase: PhaseLive, LeaseUntilNs: now.Add(-time.Nanosecond).UnixNano()}},
				Now:  now,
			},
			wantIDs:    []string{"s-expired"},
			wantPolicy: policyForPhase(ExpireDueLeases),
		},
		{
			name: "INV-WAKE-01 wakes idle subscription from cursor-tail truth",
			kind: WakeIdlePending,
			snapshot: RecoverySnapshot{
				Subs:  []Subscription{{ID: "s-idle", Config: webhookCfg, Phase: PhaseIdle, Links: pendingLink}},
				Tails: map[string]string{"events/a": tail},
				Now:   now,
			},
			wantIDs:    []string{"s-idle"},
			wantPolicy: policyForPhase(WakeIdlePending),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decidePhase(tc.kind, tc.snapshot)
			if got.Policy() != tc.wantPolicy {
				t.Fatalf("policy = %+v, want %+v", got.Policy(), tc.wantPolicy)
			}
			if ids := decisionIDs(got); !reflect.DeepEqual(ids, tc.wantIDs) {
				t.Fatalf("decision ids = %v, want %v", ids, tc.wantIDs)
			}
		})
	}
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
