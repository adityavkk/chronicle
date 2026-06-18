package main

import "testing"

func TestCheckContentionC1C2_PassesBoundedCurve(t *testing.T) {
	points := []contentionPoint{
		{Claimants: 6, Ops: 1000, Busy: 10, Fenced: 0, ThroughputPerWorker: 20, WakeP99Ms: 120, CPUPercent: 10},
		{Claimants: 12, Ops: 2000, Busy: 25, Fenced: 0, ThroughputPerWorker: 19, WakeP99Ms: 140, CPUPercent: 11},
	}
	th := contentionThresholds{MaxBusyPerOp: 0.02, MaxFencedPerOp: 0.001, MaxWakeP99Ms: 500, MaxThroughputDropFrac: 0.10}
	if got := CheckContentionC1C2(points, th); len(got) != 0 {
		t.Fatalf("expected bounded curve to pass, got %v", got)
	}
}

func TestCheckContentionC1C2_FlagsFenceStormAndThroughputKnee(t *testing.T) {
	points := []contentionPoint{
		{Claimants: 6, Ops: 1000, Busy: 10, Fenced: 0, ThroughputPerWorker: 20, WakeP99Ms: 120, CPUPercent: 10},
		{Claimants: 12, Ops: 1000, Busy: 250, Fenced: 30, LeaseLapses: 2, ThroughputPerWorker: 8, WakeP99Ms: 1200, CPUPercent: 12},
	}
	th := contentionThresholds{MaxBusyPerOp: 0.05, MaxFencedPerOp: 0.001, MaxWakeP99Ms: 500, MaxThroughputDropFrac: 0.10}
	got := CheckContentionC1C2(points, th)
	if len(got) != 5 {
		t.Fatalf("expected BUSY, FENCED, lapse, p99, and throughput violations, got %d: %v", len(got), got)
	}
}

func TestCheckContentionC3_RequiresGranularityScaledKnee(t *testing.T) {
	if got := CheckContentionC3(12, 44, 4, 0.8); len(got) != 0 {
		t.Fatalf("expected 44 claimant knee to satisfy 12*4*0.8, got %v", got)
	}
	got := CheckContentionC3(12, 24, 4, 0.8)
	if len(got) != 1 {
		t.Fatalf("expected insufficient-knee-shift violation, got %v", got)
	}
}
