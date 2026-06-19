package main

import (
	"fmt"
	"sort"
)

// check_contention.go is the PURE CORE of the contention suite (C1/C2/C3 in
// docs/specs/horizontal-scale/research/07) — the third validation class the
// GKE load test forced: a throughput collapse under claimant concurrency with
// NO fault at all. At 12 agents-server replicas the wake path fence-stormed
// (489–735 FENCED per pod) while every tier sat ≤12% CPU, because all of a
// type's entities and replicas contend for ONE per-type subscription lease.
// That is neither a safety invariant (T1 still holds — no two holders) nor a
// liveness bound (nothing faulted, nothing healed); it is a SATURATION property,
// so the nemesis is claimant COUNT and the checker is a rate/threshold assertion.
//
// Crucially this checker is NOT porcupine: linearizability is NP-hard and caps at
// 3–5 workers (07 honest-gap #1), but the collapse needs 12+ real claimants to
// reproduce. A rate/threshold checker over recorded aggregates has no such cap,
// so it runs at the claimant counts that matter.
//
// SKELETON. C1/C2 are runnable today (the per-type subscription already exists),
// but the live fan-in driver and the ClaimContention metric they read are #11's
// to build — #11 owns gate #6. This file is the pure checker #11 wires onto: the
// types capture the golden signals (BUSY/FENCED/lease-lapse rate, per-busy-worker
// throughput, wake p99/p50) and the three checkers assert C1 (bounded rates), C2
// (no throughput knee), and C3 (the granularity fix moves the knee ~G×).

// contentionRound is one fan-in level: N concurrent claimants on ONE subscription
// under no fault, with the aggregate signals the suite gates on. A run drives a
// ramp of rounds (e.g. N = 6, 12, 24) and feeds the slice to the checkers.
type contentionRound struct {
	claimants               int     // N — the contention nemesis
	ops                     int     // total claim/ack ops observed in the round
	fenced                  int     // FENCED replies (the tipping-point signal)
	alreadyClaimed          int     // ALREADY_CLAIMED / BUSY replies (the earliest signal)
	leaseLapsesHeartbeating int     // leases that lapsed while their holder was heartbeating (C1: must be 0)
	throughputPerWorker     float64 // per busy worker = 1 / round-trip-latency
	wakeP99Ms               float64
	wakeP50Ms               float64
}

// fencedRate is FENCED per op; busyRate is ALREADY_CLAIMED per op. Both are 0 for
// an empty round.
func (r contentionRound) fencedRate() float64 { return ratePerOp(r.fenced, r.ops) }
func (r contentionRound) busyRate() float64   { return ratePerOp(r.alreadyClaimed, r.ops) }

// aggregateThroughput is the whole subscription's throughput at this fan-in:
// per-busy-worker throughput times the number of claimants. A healthy system
// keeps this rising with N; the collapse is where it stops (the knee).
func (r contentionRound) aggregateThroughput() float64 {
	return r.throughputPerWorker * float64(r.claimants)
}

func ratePerOp(n, ops int) float64 {
	if ops == 0 {
		return 0
	}
	return float64(n) / float64(ops)
}

// contentionLimits are the C1 thresholds (the "regression baseline" 07 C2 names):
// the FENCED-per-op and ALREADY_CLAIMED-per-op ceilings the rates must stay under
// as claimants rise.
type contentionLimits struct {
	MaxFencedRate float64
	MaxBusyRate   float64
}

// contentionViolation records one failed assertion, tagged by which C-property it
// belongs to.
type contentionViolation struct {
	property  string // "C1" | "C2" | "C3"
	claimants int
	reason    string
}

func (v contentionViolation) String() string {
	return fmt.Sprintf("%s violated at N=%d claimants: %s", v.property, v.claimants, v.reason)
}

// CheckBoundedContention asserts C1: as claimants rise, the FENCED-per-op and
// ALREADY_CLAIMED-per-op rates stay bounded (under the limits), and no lease ever
// lapses while its holder is heartbeating (the storm's mechanism — a holder's
// heartbeat lands late behind a queue, the lease lapses, a competitor takes over,
// and the deposed holder is FENCED). The rounds need not be sorted. An empty
// result means C1 held.
func CheckBoundedContention(rounds []contentionRound, lim contentionLimits) []contentionViolation {
	var violations []contentionViolation
	for _, r := range rounds {
		if r.leaseLapsesHeartbeating > 0 {
			violations = append(violations, contentionViolation{
				property: "C1", claimants: r.claimants,
				reason: fmt.Sprintf("%d lease(s) lapsed under an active heartbeat", r.leaseLapsesHeartbeating),
			})
		}
		if r.fencedRate() > lim.MaxFencedRate {
			violations = append(violations, contentionViolation{
				property: "C1", claimants: r.claimants,
				reason: fmt.Sprintf("FENCED/op %.3f over ceiling %.3f (fence storm)", r.fencedRate(), lim.MaxFencedRate),
			})
		}
		if r.busyRate() > lim.MaxBusyRate {
			violations = append(violations, contentionViolation{
				property: "C1", claimants: r.claimants,
				reason: fmt.Sprintf("ALREADY_CLAIMED/op %.3f over ceiling %.3f", r.busyRate(), lim.MaxBusyRate),
			})
		}
	}
	return violations
}

// c2KneeTolerance is how far per-busy-worker throughput may dip between adjacent
// claimant rungs before it counts as a collapse knee. A healthy system keeps
// per-worker throughput roughly flat as claimants rise (small declines from
// coordination overhead are fine); a fence storm makes it fall off a cliff. 0.25
// means a >25% per-worker drop from one rung to the next is the knee.
const c2KneeTolerance = 0.25

// collapsed reports whether per-busy-worker throughput fell materially (more than
// c2KneeTolerance) from prev to cur — the C2 knee, where adding claimants stops
// adding throughput PER WORKER. This is deliberately per-worker, not aggregate
// (07 C2): a fence storm can drop per-worker throughput 100 -> 55 while AGGREGATE
// throughput still RISES 600 -> 660 as claimants double, so an aggregate check
// would miss exactly the 6-clean / 12-collapse signature C2 exists to catch.
func collapsed(prev, cur contentionRound) bool {
	if prev.throughputPerWorker <= 0 {
		return false
	}
	return cur.throughputPerWorker < prev.throughputPerWorker*(1-c2KneeTolerance)
}

// CheckNoThroughputCollapse asserts C2: per-busy-worker throughput does not fall
// off as claimants rise. The knee (the empirical 6-clean / 12-collapse) is the
// first round where per-worker throughput collapses versus the previous (lower-N)
// round by more than c2KneeTolerance — even if aggregate throughput is still
// rising. The rounds are sorted by claimant count first. An empty result means no
// knee was observed in the measured range.
func CheckNoThroughputCollapse(rounds []contentionRound) []contentionViolation {
	sorted := sortedByClaimants(rounds)
	var violations []contentionViolation
	for i := 1; i < len(sorted); i++ {
		prev, cur := sorted[i-1], sorted[i]
		if collapsed(prev, cur) {
			violations = append(violations, contentionViolation{
				property: "C2", claimants: cur.claimants,
				reason: fmt.Sprintf("per-worker throughput fell %.1f -> %.1f (%.0f%%) as claimants rose %d -> %d (aggregate %.1f -> %.1f) — the collapse knee",
					prev.throughputPerWorker, cur.throughputPerWorker,
					100*(1-cur.throughputPerWorker/prev.throughputPerWorker),
					prev.claimants, cur.claimants,
					prev.aggregateThroughput(), cur.aggregateThroughput()),
			})
		}
	}
	return violations
}

// CheckGranularityMovesKnee asserts C3, the acceptance gate for the claim-
// granularity fix: sharding the per-type subscription into G per-shard leases
// pushes the collapse knee out ~G× in claimant count. g1 is the C2 ramp at G=1
// (the hot <type>-handler), gG the ramp at the finer <type>-handler:<g>. It
// passes when gG's knee is at least G*tolerance times g1's knee — including the
// case where gG has NO knee in the measured range (the knee moved beyond it),
// which is the strongest pass. It FAILS when g1 had a knee but gG's did not move
// out, i.e. the granularity change did not relieve the contention.
func CheckGranularityMovesKnee(g1, gG []contentionRound, G int, tolerance float64) []contentionViolation {
	knee1, found1 := findKnee(g1)
	if !found1 {
		// No collapse to move — C3 has nothing to prove against; report it so the
		// caller knows the differential was inconclusive rather than passed.
		return []contentionViolation{{
			property: "C3", claimants: 0,
			reason: "inconclusive: the G=1 ramp showed no collapse knee to move (widen the claimant ramp)",
		}}
	}
	kneeG, foundG := findKnee(gG)
	if !foundG {
		return nil // gG never collapsed in the measured range: the knee moved out — pass
	}
	want := float64(knee1) * float64(G) * tolerance
	if float64(kneeG) < want {
		return []contentionViolation{{
			property: "C3", claimants: kneeG,
			reason: fmt.Sprintf("knee moved %d -> %d claimants, want >= %.0f (~%dx) — granularity fix did not relieve contention",
				knee1, kneeG, want, G),
		}}
	}
	return nil
}

// findKnee returns the claimant count of the first throughput-collapse knee in a
// ramp (the first round whose per-worker throughput collapsed versus its
// predecessor — the same per-worker criterion C2 uses, so C3's differential
// compares like for like), and whether one was found.
func findKnee(rounds []contentionRound) (claimants int, found bool) {
	sorted := sortedByClaimants(rounds)
	for i := 1; i < len(sorted); i++ {
		if collapsed(sorted[i-1], sorted[i]) {
			return sorted[i].claimants, true
		}
	}
	return 0, false
}

// sortedByClaimants returns a copy of rounds in ascending claimant order, leaving
// the caller's slice untouched.
func sortedByClaimants(rounds []contentionRound) []contentionRound {
	out := make([]contentionRound, len(rounds))
	copy(out, rounds)
	sort.SliceStable(out, func(i, j int) bool { return out[i].claimants < out[j].claimants })
	return out
}
