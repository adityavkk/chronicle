package main

import (
	"hash/fnv"
	"testing"
)

// check_slot_test.go unit-tests the PURE T5 core (no Redis): the cluster-slot
// oracle, the single-slot precondition, the FNV mirror + g-suffix strip, and the
// differential leakage verdict.

func fnvMod(s string, m int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32() % uint32(m))
}

func TestDsSlotOfMirrorsFNVAndStrips(t *testing.T) {
	for _, id := range []string{"s1", "agent-handler", "sub-with-{braces}", "x:y:z"} {
		if got, want := dsSlotOf(id), fnvMod(id, dsSubSlots); got != want {
			t.Fatalf("dsSlotOf(%q) = %d, want fnv32a%%S = %d", id, got, want)
		}
	}
	// A g>0 shard member homes to its parent sub's slot.
	if dsSlotOf("agent-handler:g:7") != dsSlotOf("agent-handler") {
		t.Fatalf("g-suffix must strip to the base sub's slot")
	}
	// A non-numeric ":g:" suffix is NOT stripped (only the shardMember form).
	if dsSlotOf("foo:g:bar") != fnvMod("foo:g:bar", dsSubSlots) {
		t.Fatalf("a non-numeric :g: suffix must not be treated as a shard suffix")
	}
}

func TestSubKeysOneSlotAndMisTag(t *testing.T) {
	const id = "sub-with-{braces}-and-:colons"
	slot, ok := subKeysOneSlot(id)
	if !ok {
		t.Fatalf("every key for %q must resolve to one cluster slot", id)
	}
	// The fan-out shard in the sub's home slot shares that one slot.
	if clusterSlot(dsStreamSubsKey(dsSlotOf(id), "events/a")) != slot {
		t.Fatalf("the sub's fan-out shard must share its home cluster slot")
	}
	// A mis-tagged key (wrong slot) lands in a DIFFERENT cluster slot — CROSSSLOT is
	// detectable, not silent.
	if clusterSlot(dsStreamSubsKey((dsSlotOf(id)+1)%dsSubSlots, "events/a")) == slot {
		t.Fatalf("a mis-tagged key must change cluster slot (CROSSSLOT detectable)")
	}
}

func TestComputeSlotLeakage(t *testing.T) {
	ref := []string{"a", "b", "c"}

	// Clean: scatter-gather and brute-force both equal the reference.
	if v := computeSlotLeakage(ref, []string{"c", "a", "b"}, []string{"a", "b", "c"}); !v.clean() {
		t.Fatalf("equal sets must be clean, got %+v", v)
	}
	// Foreign wake: a subscriber of another stream is returned.
	if v := computeSlotLeakage(ref, []string{"a", "b", "c", "x"}, []string{"a", "b", "c", "x"}); v.clean() || len(v.Foreign) != 1 || v.Foreign[0] != "x" {
		t.Fatalf("a foreign id must be flagged, got %+v", v)
	}
	// Dropped subscriber: scatter-gather missed one the harness linked.
	if v := computeSlotLeakage(ref, []string{"a", "b"}, []string{"a", "b"}); v.clean() || len(v.Missing) != 1 || v.Missing[0] != "c" {
		t.Fatalf("a missing id must be flagged, got %+v", v)
	}
	// Bitmap missed an occupied slot: brute-force has an id scatter-gather did not.
	if v := computeSlotLeakage(ref, []string{"a", "b", "c"}, []string{"a", "b", "c", "c2"}); v.clean() || len(v.BruteDiffer) != 1 {
		t.Fatalf("a brute-force-only id (bitmap missed a slot) must be flagged, got %+v", v)
	}
}

func TestClusterSlotHashTag(t *testing.T) {
	// Same hash tag => same slot, regardless of the surrounding key.
	if clusterSlot("ds:{__ds:42}:sub:x") != clusterSlot("ds:{__ds:42}:sub:y:links") {
		t.Fatalf("keys sharing a hash tag must share a cluster slot")
	}
	// Empty tag => whole key hashed (different keys differ).
	if clusterSlot("ds:{}:a") == clusterSlot("ds:{}:bbbb") {
		t.Fatalf("an empty hash tag must fall back to hashing the whole key")
	}
}
