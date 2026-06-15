package main

import "fmt"

// check_cursor.go is the PURE CORE of the cursor-monotonicity safety test (T2 in
// docs/specs/horizontal-scale/research/07): per (subscription, stream path), the
// acked offset is forward-only. A replayed or stale ack — the retry worker and
// the recovery sweep both re-firing, or a deposed holder acking late — must be a
// no-op, never a regression and never a phantom advance. This is a cheaper
// invariant than full linearizability (a single backward step is a finite
// counterexample on its own), so it is a direct checker over a time-ordered
// sample stream rather than a porcupine model.
//
// Like model_fence.go this is pure: it takes a slice of observations and returns
// the violations, with no I/O. The shell (a poller in the scenario driver)
// collects the samples from GET .../subscriptions/<id>.

// cursorSample is one observation of a subscription stream's acked offset at a
// point in driver-host time.
type cursorSample struct {
	sub    string
	path   string
	offset string
	atNs   int64
}

// cursorViolation records a non-monotonic cursor: an offset observed below a
// value already seen for the same (sub, path).
type cursorViolation struct {
	sub  string
	path string
	from string // the higher offset seen earlier
	to   string // the lower offset seen later
	atNs int64  // when the regression was observed
}

func (v cursorViolation) String() string {
	return fmt.Sprintf("cursor regressed for sub=%s path=%s: %s -> %s (at +%dms)",
		v.sub, v.path, short(v.from), short(v.to), v.atNs/1e6)
}

// CheckCursorMonotonic asserts that for every (sub, path) the acked offset never
// decreases across the sample stream, in observation order. It returns every
// regression found; an empty result means the property held. Samples may
// interleave across subs and paths — only same-(sub, path) ordering matters — and
// the slice is assumed to be in non-decreasing atNs order (the poller appends in
// real time). A regression keeps the existing high-water mark, so a cursor that
// dips and recovers reports one violation, not a cascade.
func CheckCursorMonotonic(samples []cursorSample) []cursorViolation {
	type key struct{ sub, path string }
	high := map[key]string{} // highest offset seen so far per (sub, path)
	var violations []cursorViolation
	for _, s := range samples {
		k := key{s.sub, s.path}
		prev, seen := high[k]
		if seen && offsetGreater(prev, s.offset) {
			violations = append(violations, cursorViolation{
				sub: s.sub, path: s.path, from: prev, to: s.offset, atNs: s.atNs,
			})
			continue
		}
		if !seen || offsetGreater(s.offset, prev) {
			high[k] = s.offset
		}
	}
	return violations
}

// offsetGreater reports a > b for chronicle's opaque, lexicographically-sortable
// offsets (PROTOCOL §8), treating the "-1"/"" beginning sentinel as less than any
// real offset. A checker-local mirror of webhook.offsetGreater (and common.lua's
// offset_greater) — the harness deliberately stays self-contained, and the
// triple mirror (Go core, Lua, checker) is the existing pattern in this codebase.
func offsetGreater(a, b string) bool {
	if a == b {
		return false
	}
	if b == "-1" || b == "" {
		return a != "-1" && a != ""
	}
	if a == "-1" || a == "" {
		return false
	}
	return a > b
}
