package main

import "testing"

// These tests exercise the pure cursor-monotonicity checker (check_cursor.go)
// directly against crafted sample streams — no cluster. They prove the checker
// passes forward-only cursors and catches every regression, including the
// beginning sentinel and per-(sub, path) isolation.

func sample(sub, path, offset string, atMs int64) cursorSample {
	return cursorSample{sub: sub, path: path, offset: offset, atNs: atMs * 1e6}
}

func TestCheckCursorMonotonic_ForwardOnlyPasses(t *testing.T) {
	samples := []cursorSample{
		sample("s", "events/a", "-1", 0),
		sample("s", "events/a", "0000000000000001_0000000000000010", 1),
		sample("s", "events/a", "0000000000000001_0000000000000010", 2), // repeat: fine
		sample("s", "events/a", "0000000000000001_0000000000000020", 3),
	}
	if v := CheckCursorMonotonic(samples); len(v) != 0 {
		t.Fatalf("forward-only stream reported %d violations: %v", len(v), v)
	}
}

func TestCheckCursorMonotonic_RegressionCaught(t *testing.T) {
	samples := []cursorSample{
		sample("s", "events/a", "0000000000000001_0000000000000020", 0),
		sample("s", "events/a", "0000000000000001_0000000000000010", 1), // backward
	}
	v := CheckCursorMonotonic(samples)
	if len(v) != 1 {
		t.Fatalf("expected 1 violation, got %d: %v", len(v), v)
	}
	if v[0].from != "0000000000000001_0000000000000020" || v[0].to != "0000000000000001_0000000000000010" {
		t.Fatalf("violation has wrong offsets: %+v", v[0])
	}
}

// A cursor that dips and recovers reports exactly one violation: the high-water
// mark is retained across the dip so the recovery is not itself flagged.
func TestCheckCursorMonotonic_DipAndRecoverReportsOnce(t *testing.T) {
	samples := []cursorSample{
		sample("s", "events/a", "0000000000000001_0000000000000020", 0),
		sample("s", "events/a", "0000000000000001_0000000000000005", 1), // dip
		sample("s", "events/a", "0000000000000001_0000000000000021", 2), // recover past the mark
	}
	if v := CheckCursorMonotonic(samples); len(v) != 1 {
		t.Fatalf("expected exactly 1 violation across a dip-and-recover, got %d: %v", len(v), v)
	}
}

// The "-1" beginning sentinel is below any real offset, so advancing off it is
// forward progress, and never returning to it from a real offset is a regression.
func TestCheckCursorMonotonic_BeginningSentinel(t *testing.T) {
	advance := []cursorSample{
		sample("s", "events/a", "-1", 0),
		sample("s", "events/a", "0000000000000001_0000000000000001", 1),
	}
	if v := CheckCursorMonotonic(advance); len(v) != 0 {
		t.Fatalf("advancing off -1 is forward progress, got violations: %v", v)
	}
	regress := []cursorSample{
		sample("s", "events/a", "0000000000000001_0000000000000001", 0),
		sample("s", "events/a", "-1", 1), // back to the beginning
	}
	if v := CheckCursorMonotonic(regress); len(v) != 1 {
		t.Fatalf("returning to -1 from a real offset is a regression, got %d: %v", len(v), v)
	}
}

// Paths and subscriptions are tracked independently: interleaved streams that are
// each monotone produce no violations.
func TestCheckCursorMonotonic_IndependentPerKey(t *testing.T) {
	samples := []cursorSample{
		sample("s1", "events/a", "0000000000000001_0000000000000010", 0),
		sample("s2", "events/a", "0000000000000001_0000000000000002", 1), // different sub, lower: fine
		sample("s1", "events/b", "0000000000000001_0000000000000001", 2), // different path, lower: fine
		sample("s1", "events/a", "0000000000000001_0000000000000011", 3),
	}
	if v := CheckCursorMonotonic(samples); len(v) != 0 {
		t.Fatalf("per-key monotone streams reported violations: %v", v)
	}
}
