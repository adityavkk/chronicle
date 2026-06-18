package main

import (
	"sync"
	"testing"
	"time"
)

// These tests exercise the claimant-fan-in DRIVER (scenario_contention.go) without
// Redis: the pure helpers deterministically, and driveRound against an in-memory
// single-holder claimer that faithfully models claim.lua/ack.lua. The integration
// run against real Redis is the `contention` scenario (REDIS_URL-gated); here we
// pin the driver's logic and the 6-clean/12-collapse shape it must surface.

func TestShardOf(t *testing.T) {
	// G<=1 always lands on shard 0 (today's single per-type lease).
	for _, e := range []string{"a", "b", "anything"} {
		if g := shardOf(e, 1); g != 0 {
			t.Fatalf("shardOf(%q,1)=%d, want 0", e, g)
		}
	}
	// G=16: hashing distributes; assert every shard is in range and the spread is
	// not wildly skewed over many entities (the fix relies on uniform-ish hashing).
	const G, N = 16, 4096
	counts := make([]int, G)
	for i := 0; i < N; i++ {
		g := shardOf(itoa(i), G)
		if g < 0 || g >= G {
			t.Fatalf("shard out of range: %d", g)
		}
		counts[g]++
	}
	exp := N / G
	for g, c := range counts {
		if c < exp/2 || c > exp*2 {
			t.Errorf("shard %d got %d entities, expected ~%d (badly skewed hash)", g, c, exp)
		}
	}
}

func TestPercentileMs(t *testing.T) {
	if v := percentileMs(nil, 50); v != 0 {
		t.Fatalf("empty p50 = %v, want 0", v)
	}
	d := []time.Duration{
		1 * time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond,
		4 * time.Millisecond, 100 * time.Millisecond,
	}
	if v := percentileMs(d, 50); v < 2.9 || v > 3.1 {
		t.Errorf("p50 = %v, want ~3", v)
	}
	if v := percentileMs(d, 99); v < 99 {
		t.Errorf("p99 = %v, want ~100", v)
	}
}

func TestParseRamp(t *testing.T) {
	cases := map[string][]int{
		"6,12,24": {6, 12, 24},
		" 6 12 ":  {6, 12},
		"":        {6, 12, 24}, // fallback
		"garbage": {6, 12, 24}, // fallback
		"3":       {3},
	}
	for in, want := range cases {
		got := parseRamp(in)
		if len(got) != len(want) {
			t.Fatalf("parseRamp(%q) = %v, want %v", in, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("parseRamp(%q) = %v, want %v", in, got, want)
			}
		}
	}
}

func TestBuildRound(t *testing.T) {
	stats := []workerStats{
		{completed: 10, busy: 30, fenced: 1, lapses: 1, latencies: durs(5, 6, 7)},
		{completed: 20, busy: 70, fenced: 0, latencies: durs(8, 9)},
	}
	r := buildRound(2, time.Second, stats)
	if r.claimants != 2 {
		t.Errorf("claimants = %d, want 2", r.claimants)
	}
	if r.alreadyClaimed != 100 || r.fenced != 1 || r.leaseLapsesHeartbeating != 1 {
		t.Errorf("aggregation wrong: %+v", r)
	}
	if r.ops != 100+30+1 { // busy + completed + fenced
		t.Errorf("ops = %d, want 131", r.ops)
	}
	// 30 completed cycles over 2 workers in 1s -> 15 cycles/worker/s.
	if r.throughputPerWorker < 14.9 || r.throughputPerWorker > 15.1 {
		t.Errorf("throughputPerWorker = %v, want 15", r.throughputPerWorker)
	}
}

// TestDriveRound_CollapseAtG1_FlatAtG8 is the executable 6-clean/12-collapse
// signature over the driver: against the in-memory single-holder claimer, ONE
// lease (G=1) collapses per-worker throughput as claimants double, while G=8
// spreads the load and stays flat — exactly the differential C3 gates on.
func TestDriveRound_CollapseAtG1_FlatAtG8(t *testing.T) {
	p := contentionParams{
		ttlMs:    30000,
		hold:     1 * time.Millisecond,
		think:    3 * time.Millisecond,
		backoff:  1 * time.Millisecond,
		roundDur: 250 * time.Millisecond,
	}

	g1 := newFakeClaimer(1, p.ttlMs)
	ramp1 := []contentionRound{driveRound(g1, p, 6), driveRound(g1, p, 12)}
	if v := CheckNoThroughputCollapse(ramp1); len(v) == 0 {
		t.Fatalf("G=1 should collapse (per-worker throughput knee), got none: %+v", ramp1)
	}

	g8 := newFakeClaimer(8, p.ttlMs)
	ramp8 := []contentionRound{driveRound(g8, p, 6), driveRound(g8, p, 12)}
	if v := CheckNoThroughputCollapse(ramp8); len(v) != 0 {
		t.Fatalf("G=8 should stay flat (no knee 6->12), got %v (ramp %+v)", v, ramp8)
	}

	// The earliest contention signal drops with granularity: BUSY/op at the top
	// rung is far higher on one lease than spread across eight.
	if ramp1[1].busyRate() <= ramp8[1].busyRate() {
		t.Errorf("expected busy/op to fall with G (G=1 %.3f should exceed G=8 %.3f)",
			ramp1[1].busyRate(), ramp8[1].busyRate())
	}

	// C3 over the two ramps: the knee moved out (G=8 never collapsed in range) — pass.
	if v := CheckGranularityMovesKnee(ramp1, ramp8, 8, 0.75); len(v) != 0 {
		t.Errorf("C3 should pass (knee moved beyond the G=8 range), got %v", v)
	}
}

func TestDriveRound_NoLapseUnderActiveHold(t *testing.T) {
	// With hold << lease_ttl, no lease ever lapses under a holder, so C1's forbidden
	// lapse count stays 0 even on the hot single lease (the storm needs lapses).
	p := contentionParams{ttlMs: 30000, hold: 1 * time.Millisecond, think: 2 * time.Millisecond, backoff: 1 * time.Millisecond, roundDur: 200 * time.Millisecond}
	r := driveRound(newFakeClaimer(1, p.ttlMs), p, 12)
	if r.leaseLapsesHeartbeating != 0 {
		t.Errorf("no lease should lapse under an active hold, got %d", r.leaseLapsesHeartbeating)
	}
	if r.fenced != 0 {
		t.Errorf("no fault + no lapse should yield 0 FENCED, got %d", r.fenced)
	}
}

// ---- in-memory single-holder claimer (a faithful claim.lua/ack.lua model) ----

type fakeShard struct {
	held       bool
	gen        int64
	wake       string
	leaseUntil time.Time
}

type fakeClaimer struct {
	g   int
	ttl int64
	mu  sync.Mutex
	st  map[int]*fakeShard
}

func newFakeClaimer(g int, ttlMs int64) *fakeClaimer {
	if g < 1 {
		g = 1
	}
	return &fakeClaimer{g: g, ttl: ttlMs, st: map[int]*fakeShard{}}
}

func (f *fakeClaimer) label() string { return "fake" }
func (f *fakeClaimer) shards() int   { return f.g }
func (f *fakeClaimer) setup() error  { return nil }
func (f *fakeClaimer) teardown()     {}

func (f *fakeClaimer) shard(g int) *fakeShard {
	s := f.st[g]
	if s == nil {
		s = &fakeShard{}
		f.st[g] = s
	}
	return s
}

// claim models claim.lua's single-holder CAS: BUSY while an unexpired holder
// exists, else a grant that rotates the generation strictly upward.
func (f *fakeClaimer) claim(g int, _, wakeID string, now time.Time) (claimOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.shard(g)
	if s.held && now.Before(s.leaseUntil) {
		return claimOutcome{busy: true, gen: s.gen}, nil
	}
	s.gen++
	s.wake = wakeID
	s.held = true
	s.leaseUntil = now.Add(time.Duration(f.ttl) * time.Millisecond)
	return claimOutcome{granted: true, gen: s.gen, wakeID: s.wake}, nil
}

// ack models ack.lua's fence: a stale (gen,wake) is FENCED; a matching done-ack
// releases the lease.
func (f *fakeClaimer) ack(g int, gen int64, wakeID string, done bool, now time.Time) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.shard(g)
	if gen != s.gen || wakeID == "" || wakeID != s.wake {
		return "FENCED", nil
	}
	if done {
		s.held = false
		s.wake = ""
	} else {
		s.leaseUntil = now.Add(time.Duration(f.ttl) * time.Millisecond)
	}
	return "OK", nil
}

// ---- tiny test helpers ----

func durs(ms ...int) []time.Duration {
	out := make([]time.Duration, len(ms))
	for i, m := range ms {
		out[i] = time.Duration(m) * time.Millisecond
	}
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
