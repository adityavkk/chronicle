package webhook

import "testing"

func TestDueSetMutationDecisions(t *testing.T) {
	tests := []struct {
		name string
		got  DueSetMutationDecision
		want DueSetMutationDecision
	}{
		{
			name: "arm adds only when armed",
			got:  DueSetForArmWake("ARMED"),
			want: DueSetMutationDecision{Effect: DueSetAdd, MetricOp: "arm"},
		},
		{
			name: "busy arm does not mutate",
			got:  DueSetForArmWake("BUSY"),
			want: DueSetMutationDecision{},
		},
		{
			name: "done ack removes",
			got:  DueSetForAck("OK", true),
			want: DueSetMutationDecision{Effect: DueSetRemove, MetricOp: "ack"},
		},
		{
			name: "heartbeat ack does not mutate",
			got:  DueSetForAck("OK", false),
			want: DueSetMutationDecision{},
		},
		{
			name: "fenced done ack does not mutate",
			got:  DueSetForAck("FENCED", true),
			want: DueSetMutationDecision{},
		},
		{
			name: "expired pending lease re-adds",
			got:  DueSetForExpireLease("EXPIRED", true),
			want: DueSetMutationDecision{Effect: DueSetAdd, MetricOp: "expire"},
		},
		{
			name: "expired non-pending lease clears stale due",
			got:  DueSetForExpireLease("EXPIRED", false),
			want: DueSetMutationDecision{Effect: DueSetRemove, MetricOp: "expire"},
		},
		{
			name: "active lease does not mutate",
			got:  DueSetForExpireLease("ACTIVE", true),
			want: DueSetMutationDecision{},
		},
		{
			name: "release removes",
			got:  DueSetForRelease("OK"),
			want: DueSetMutationDecision{Effect: DueSetRemove, MetricOp: "release"},
		},
		{
			name: "fenced release does not mutate",
			got:  DueSetForRelease("FENCED"),
			want: DueSetMutationDecision{},
		},
		{
			name: "delete removes",
			got:  DueSetForDelete(),
			want: DueSetMutationDecision{Effect: DueSetRemove, MetricOp: "delete"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("decision = %+v, want %+v", tt.got, tt.want)
			}
			if tt.got.Mutates() != (tt.got.Effect != DueSetNoop) {
				t.Fatalf("Mutates disagrees with effect for %+v", tt.got)
			}
		})
	}
}
