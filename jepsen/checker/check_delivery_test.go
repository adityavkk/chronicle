package main

import "testing"

// Pure unit tests for the L1 at-least-once checker (check_delivery.go) — no
// cluster. Offsets are the protocol's opaque, lexicographically-sortable form.

func TestAtLeastOnce_AllDeliveredPasses(t *testing.T) {
	exp := []deliveryExpectation{
		{path: "events/a", tail: "0000000000000005_0", msgs: 5},
		{path: "events/b", tail: "0000000000000009_0", msgs: 9},
	}
	acked := map[string]string{
		"events/a": "0000000000000005_0", // reached tail
		"events/b": "0000000000000012_0", // past tail (a later append) — still delivered
	}
	if g := CheckAtLeastOnce(exp, acked); len(g) != 0 {
		t.Fatalf("expected no gaps, got %v", g)
	}
}

func TestAtLeastOnce_LaggingStreamIsGap(t *testing.T) {
	exp := []deliveryExpectation{
		{path: "events/a", tail: "0000000000000005_0", msgs: 5},
		{path: "events/b", tail: "0000000000000009_0", msgs: 9},
	}
	acked := map[string]string{
		"events/a": "0000000000000005_0",
		"events/b": "0000000000000003_0", // stuck below tail — undelivered messages
	}
	g := CheckAtLeastOnce(exp, acked)
	if len(g) != 1 || g[0].path != "events/b" {
		t.Fatalf("expected one gap on events/b, got %v", g)
	}
}

func TestAtLeastOnce_MissingCursorIsGap(t *testing.T) {
	// A stream never acked at all (absent from the map) is a gap, not a pass.
	exp := []deliveryExpectation{{path: "events/c", tail: "0000000000000001_0", msgs: 1}}
	g := CheckAtLeastOnce(exp, map[string]string{})
	if len(g) != 1 || g[0].path != "events/c" {
		t.Fatalf("expected one gap on events/c, got %v", g)
	}
}

func TestAtLeastOnce_BeginningSentinelTailIsTriviallyMet(t *testing.T) {
	// A stream with no appended message (tail at the beginning sentinel) is met by
	// any cursor, including the same sentinel.
	exp := []deliveryExpectation{{path: "events/empty", tail: "-1", msgs: 0}}
	if g := CheckAtLeastOnce(exp, map[string]string{"events/empty": "-1"}); len(g) != 0 {
		t.Fatalf("empty stream should be trivially delivered, got %v", g)
	}
}
