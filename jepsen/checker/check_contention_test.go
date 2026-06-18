package main

import "testing"

// These tests exercise the pure contention checkers (check_contention.go)
// directly — no cluster. They encode the empirical 6-clean / 12-collapse
// signature as the regression baseline: a clean ramp passes C1/C2, a fence-storm
// ramp fails them, and C3 gates the granularity fix on the knee moving ~Gx.

var defaultLimits = contentionLimits{MaxFencedRate: 0.05, MaxBusyRate: 0.30}

// A clean ramp: rates stay tiny, no lease lapses, aggregate throughput keeps
// rising with N. This is the 6-replicas-clean baseline.
func cleanRamp() []contentionRound {
	return []contentionRound{
		{claimants: 6, ops: 1000, fenced: 0, alreadyClaimed: 60, throughputPerWorker: 100, wakeP99Ms: 40},
		{claimants: 12, ops: 2000, fenced: 2, alreadyClaimed: 180, throughputPerWorker: 95, wakeP99Ms: 55},
		{claimants: 24, ops: 4000, fenced: 8, alreadyClaimed: 480, throughputPerWorker: 90, wakeP99Ms: 70},
	}
}

func TestBoundedContention_CleanRampPasses(t *testing.T) {
	if v := CheckBoundedContention(cleanRamp(), defaultLimits); len(v) != 0 {
		t.Fatalf("clean ramp should satisfy C1, got %v", v)
	}
}

func TestBoundedContention_FenceStormFails(t *testing.T) {
	// The 12-collapse: at N=12 the FENCED rate explodes past the ceiling and leases
	// lapse under active heartbeats.
	ramp := []contentionRound{
		{claimants: 6, ops: 1000, fenced: 1, alreadyClaimed: 60, throughputPerWorker: 100},
		{claimants: 12, ops: 2000, fenced: 700, alreadyClaimed: 1200, leaseLapsesHeartbeating: 5, throughputPerWorker: 40},
	}
	v := CheckBoundedContention(ramp, defaultLimits)
	if len(v) == 0 {
		t.Fatal("fence storm should violate C1 (rate ceiling and/or lease lapse)")
	}
	// The N=12 round breaches the FENCED ceiling, the BUSY ceiling, and the lapse
	// guard — at least one must name N=12.
	found := false
	for _, x := range v {
		if x.claimants == 12 {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a violation at N=12, got %v", v)
	}
}

func TestNoThroughputCollapse_CleanRampPasses(t *testing.T) {
	if v := CheckNoThroughputCollapse(cleanRamp()); len(v) != 0 {
		t.Fatalf("clean ramp should satisfy C2 (throughput keeps rising), got %v", v)
	}
}

func TestNoThroughputCollapse_KneeFails(t *testing.T) {
	// per-worker throughput collapses 100 -> 45 as claimants rise 6 -> 12 — the knee.
	ramp := []contentionRound{
		{claimants: 6, throughputPerWorker: 100},
		{claimants: 12, throughputPerWorker: 45},
	}
	v := CheckNoThroughputCollapse(ramp)
	if len(v) != 1 || v[0].claimants != 12 {
		t.Fatalf("expected one C2 knee at N=12, got %v", v)
	}
}

// The regression witness for the review fix: the empirical fence-storm signature
// is a PER-WORKER collapse (100 -> 55) as claimants double (6 -> 12) while
// AGGREGATE throughput still RISES (600 -> 660). C2 must flag it — an aggregate
// check would pass exactly the 6-clean/12-collapse the contention suite exists to
// catch (and that #11 gates on).
func TestNoThroughputCollapse_PerWorkerKneeWithRisingAggregate(t *testing.T) {
	ramp := []contentionRound{
		{claimants: 6, throughputPerWorker: 100}, // aggregate 600
		{claimants: 12, throughputPerWorker: 55}, // aggregate 660 — rises, yet per-worker collapsed
	}
	// Guard the premise: the aggregate really does rise, so this is the case an
	// aggregate-based check would wrongly pass.
	if ramp[1].aggregateThroughput() <= ramp[0].aggregateThroughput() {
		t.Fatalf("setup error: aggregate did not rise (%.0f -> %.0f)",
			ramp[0].aggregateThroughput(), ramp[1].aggregateThroughput())
	}
	v := CheckNoThroughputCollapse(ramp)
	if len(v) != 1 || v[0].claimants != 12 {
		t.Fatalf("expected one C2 knee at N=12 (per-worker collapse despite rising aggregate), got %v", v)
	}
}

func TestNoThroughputCollapse_SortsRounds(t *testing.T) {
	// Out-of-order input must not be read as a spurious knee.
	if v := CheckNoThroughputCollapse([]contentionRound{
		{claimants: 24, throughputPerWorker: 90},
		{claimants: 6, throughputPerWorker: 100},
		{claimants: 12, throughputPerWorker: 95},
	}); len(v) != 0 {
		t.Fatalf("ascending throughput (after sort) should pass C2, got %v", v)
	}
}

func TestGranularityMovesKnee_FixPasses(t *testing.T) {
	// G=1 collapses at N=12; G=4 collapses at N=48 (~4x out) — C3 passes.
	g1 := []contentionRound{
		{claimants: 6, throughputPerWorker: 100},
		{claimants: 12, throughputPerWorker: 40}, // knee at 12
	}
	gG := []contentionRound{
		{claimants: 12, throughputPerWorker: 100},
		{claimants: 24, throughputPerWorker: 98},
		{claimants: 48, throughputPerWorker: 45}, // knee at 48
	}
	if v := CheckGranularityMovesKnee(g1, gG, 4, 0.75); len(v) != 0 {
		t.Fatalf("knee 12 -> 48 with G=4 should pass C3, got %v", v)
	}
}

func TestGranularityMovesKnee_NoReliefFails(t *testing.T) {
	// G=4 but the knee barely moved (12 -> 12): the granularity change did not
	// relieve contention.
	g1 := []contentionRound{
		{claimants: 6, throughputPerWorker: 100},
		{claimants: 12, throughputPerWorker: 40},
	}
	gG := []contentionRound{
		{claimants: 6, throughputPerWorker: 100},
		{claimants: 12, throughputPerWorker: 42}, // still collapses at 12
	}
	v := CheckGranularityMovesKnee(g1, gG, 4, 0.75)
	if len(v) != 1 || v[0].property != "C3" {
		t.Fatalf("expected one C3 violation, got %v", v)
	}
}

func TestGranularityMovesKnee_KneeMovedBeyondRangePasses(t *testing.T) {
	// gG never collapses in the measured ramp: the strongest pass.
	g1 := []contentionRound{
		{claimants: 6, throughputPerWorker: 100},
		{claimants: 12, throughputPerWorker: 40},
	}
	gG := []contentionRound{
		{claimants: 12, throughputPerWorker: 100},
		{claimants: 48, throughputPerWorker: 110},
	}
	if v := CheckGranularityMovesKnee(g1, gG, 4, 0.75); len(v) != 0 {
		t.Fatalf("a knee that moved beyond the measured range should pass C3, got %v", v)
	}
}
