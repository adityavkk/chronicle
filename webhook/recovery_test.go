package webhook

import (
	"testing"
	"time"
)

func TestRecoveryScopeRouting(t *testing.T) {
	tests := []struct {
		scope      recoveryScope
		wantSweep  bool
		wantLeases bool
	}{
		{scope: recoveryScopeBoot, wantSweep: true},
		{scope: recoveryScopeReconnect, wantSweep: true},
		{scope: recoveryScopeAppendError, wantSweep: true},
		{scope: recoveryScopeFloor, wantSweep: true},
		{scope: recoveryScopeEpochBump, wantLeases: true},
		{scope: recoveryScopeNewOwnerCAS, wantLeases: true},
	}

	for _, tt := range tests {
		t.Run(tt.scope.String(), func(t *testing.T) {
			got := planRecovery(tt.scope)
			if got.sweep != tt.wantSweep || got.leases != tt.wantLeases {
				t.Fatalf("planRecovery(%s) = sweep:%v leases:%v, want sweep:%v leases:%v",
					tt.scope, got.sweep, got.leases, tt.wantSweep, tt.wantLeases)
			}
		})
	}
}

func TestLeaseReconcileDecision(t *testing.T) {
	until := time.Now().Add(time.Minute).UnixNano()
	tests := []struct {
		name        string
		state       ClaimLeaseState
		pending     bool
		wantRepair  bool
		wantPending bool
	}{
		{
			name:        "live lease with pending work",
			state:       ClaimLeaseState{Phase: PhaseLive, LeaseUntilNs: until},
			pending:     true,
			wantRepair:  true,
			wantPending: true,
		},
		{
			name:       "waking lease without pending work",
			state:      ClaimLeaseState{Phase: PhaseWaking, LeaseUntilNs: until},
			wantRepair: true,
		},
		{
			name:  "idle lease state",
			state: ClaimLeaseState{Phase: PhaseIdle, LeaseUntilNs: until},
		},
		{
			name:  "live without a lease deadline",
			state: ClaimLeaseState{Phase: PhaseLive},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decideLeaseReconcile(tt.state, tt.pending)
			if got.reconcile != tt.wantRepair || got.pending != tt.wantPending {
				t.Fatalf("decideLeaseReconcile = reconcile:%v pending:%v, want reconcile:%v pending:%v",
					got.reconcile, got.pending, tt.wantRepair, tt.wantPending)
			}
		})
	}
}

func TestClaimLeasesFromSubscriptionIncludesDefaultAndShards(t *testing.T) {
	shard, _ := NewClaimShard(5)
	sub := Subscription{
		Phase:        PhaseLive,
		Generation:   7,
		WakeID:       "w_default",
		Holder:       true,
		HolderWorker: "default-worker",
		LeaseUntilNs: 100,
		ClaimLeases: []ClaimShardLeaseState{{
			Shard: shard,
			State: ClaimLeaseState{
				Phase:        PhaseWaking,
				Generation:   11,
				WakeID:       "w_shard",
				HolderWorker: "shard-worker",
				LeaseUntilNs: 200,
			},
		}},
	}

	got := ClaimLeasesFromSubscription(sub)
	if len(got) != 2 {
		t.Fatalf("claim lease states = %d, want default plus one shard", len(got))
	}
	if got[0].Shard != DefaultClaimShard || got[0].State.Generation != 7 {
		t.Fatalf("first lease should be legacy shard state, got %+v", got[0])
	}
	if got[1].Shard != shard || got[1].State.Generation != 11 {
		t.Fatalf("second lease should be hydrated shard state, got %+v", got[1])
	}
}
