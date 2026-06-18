package main

import "fmt"

// check_contention.go is the pure scaffold for the C1/C2/C3 contention suite
// from docs/specs/horizontal-scale/research/07. It is intentionally a
// rate-contract checker, not a porcupine model: the failure class is a
// saturation knee under claimant fan-in, not a finite safety counterexample.

type contentionPoint struct {
	Claimants           int
	Ops                 int
	Busy                int
	Fenced              int
	LeaseLapses         int
	ThroughputPerWorker float64
	WakeP99Ms           float64
	CPUPercent          float64
}

type contentionThresholds struct {
	MaxBusyPerOp          float64
	MaxFencedPerOp        float64
	MaxWakeP99Ms          float64
	MaxThroughputDropFrac float64
}

type contentionViolation struct {
	Property string
	At       int
	Reason   string
}

func (v contentionViolation) String() string {
	return fmt.Sprintf("%s at claimants=%d: %s", v.Property, v.At, v.Reason)
}

func CheckContentionC1C2(points []contentionPoint, th contentionThresholds) []contentionViolation {
	var violations []contentionViolation
	var prev contentionPoint
	for i, p := range points {
		if p.Ops <= 0 {
			violations = append(violations, contentionViolation{Property: "C1", At: p.Claimants, Reason: "ops must be > 0"})
			continue
		}
		if rate := float64(p.Busy) / float64(p.Ops); rate > th.MaxBusyPerOp {
			violations = append(violations, contentionViolation{
				Property: "C1", At: p.Claimants,
				Reason: fmt.Sprintf("BUSY/op %.4f exceeds %.4f", rate, th.MaxBusyPerOp),
			})
		}
		if rate := float64(p.Fenced) / float64(p.Ops); rate > th.MaxFencedPerOp {
			violations = append(violations, contentionViolation{
				Property: "C1", At: p.Claimants,
				Reason: fmt.Sprintf("FENCED/op %.4f exceeds %.4f", rate, th.MaxFencedPerOp),
			})
		}
		if p.LeaseLapses > 0 {
			violations = append(violations, contentionViolation{
				Property: "C1", At: p.Claimants,
				Reason: fmt.Sprintf("lease lapsed %d time(s) while holders were active", p.LeaseLapses),
			})
		}
		if th.MaxWakeP99Ms > 0 && p.WakeP99Ms > th.MaxWakeP99Ms {
			violations = append(violations, contentionViolation{
				Property: "C2", At: p.Claimants,
				Reason: fmt.Sprintf("wake p99 %.1fms exceeds %.1fms", p.WakeP99Ms, th.MaxWakeP99Ms),
			})
		}
		if i > 0 && prev.ThroughputPerWorker > 0 {
			floor := prev.ThroughputPerWorker * (1 - th.MaxThroughputDropFrac)
			if p.ThroughputPerWorker < floor {
				violations = append(violations, contentionViolation{
					Property: "C2", At: p.Claimants,
					Reason: fmt.Sprintf("throughput/worker %.4f fell below %.4f", p.ThroughputPerWorker, floor),
				})
			}
		}
		prev = p
	}
	return violations
}

func CheckContentionC3(baseKneeClaimants, shardedKneeClaimants, granularity int, minMoveFraction float64) []contentionViolation {
	if baseKneeClaimants <= 0 || shardedKneeClaimants <= 0 || granularity <= 0 {
		return []contentionViolation{{Property: "C3", Reason: "knee claimants and granularity must be > 0"}}
	}
	want := float64(baseKneeClaimants*granularity) * minMoveFraction
	if float64(shardedKneeClaimants) < want {
		return []contentionViolation{{
			Property: "C3", At: shardedKneeClaimants,
			Reason: fmt.Sprintf("knee moved to %d claimants; want at least %.1f for G=%d", shardedKneeClaimants, want, granularity),
		}}
	}
	return nil
}

func CheckContentionC3FixedN(base, sharded contentionPoint, granularity int, maxBusyRatio float64) []contentionViolation {
	if base.Ops <= 0 || sharded.Ops <= 0 || granularity <= 0 {
		return []contentionViolation{{Property: "C3", At: sharded.Claimants, Reason: "ops and granularity must be > 0"}}
	}
	baseBusy := float64(base.Busy) / float64(base.Ops)
	shardedBusy := float64(sharded.Busy) / float64(sharded.Ops)
	if baseBusy == 0 {
		if shardedBusy > 0 {
			return []contentionViolation{{
				Property: "C3", At: sharded.Claimants,
				Reason: fmt.Sprintf("baseline BUSY/op is 0 but sharded BUSY/op is %.4f", shardedBusy),
			}}
		}
		return nil
	}
	contendedFraction := 0.0
	if sharded.Claimants > 1 && sharded.Claimants > granularity {
		contendedFraction = float64(sharded.Claimants-granularity) / float64(sharded.Claimants-1)
	}
	limit := baseBusy * contendedFraction * maxBusyRatio
	if shardedBusy > limit {
		return []contentionViolation{{
			Property: "C3", At: sharded.Claimants,
			Reason: fmt.Sprintf("sharded BUSY/op %.4f exceeds %.4f from baseline %.4f and G=%d", shardedBusy, limit, baseBusy, granularity),
		}}
	}
	return nil
}
