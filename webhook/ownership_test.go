package webhook

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"
	"time"
)

// ownership_test.go covers the pure core of work-sharded leased slot ownership
// (ownership.go): the CAS-reply sum types, the HRW assignment (argmax, tie-break,
// ~1/N reassignment), the OwnedSlots set math, the replica-id constructor, and the
// config invariants — all without Redis or a clock, mirroring shard_test.go.

// slotIDs builds n synthetic SlotIDs for the HRW property tests. The HRW math is
// general over slot indices; ownershipSlots only bounds what the manager iterates
// (it is 1 for #14, raised to 256 in #15), so a white-box test constructs the
// wider range directly to exercise the reassignment property.
func slotIDs(n int) []SlotID {
	out := make([]SlotID, n)
	for i := range out {
		out[i] = SlotID{h: i}
	}
	return out
}

func reps(t *testing.T, ids ...string) []ReplicaID {
	t.Helper()
	out := make([]ReplicaID, len(ids))
	for i, id := range ids {
		r, err := NewReplicaID(id)
		if err != nil {
			t.Fatalf("NewReplicaID(%q): %v", id, err)
		}
		out[i] = r
	}
	return out
}

// HRWOwner is a deterministic argmax: the same member set always yields the same
// owner, and every slot resolves to a member in the set.
func TestHRWOwnerDeterministicAndInSet(t *testing.T) {
	members := reps(t, "a", "b", "c", "d")
	inSet := func(r ReplicaID) bool {
		for _, m := range members {
			if m.id == r.id {
				return true
			}
		}
		return false
	}
	for _, h := range slotIDs(256) {
		o1, ok1 := HRWOwner(members, h)
		o2, ok2 := HRWOwner(members, h)
		if !ok1 || !ok2 || o1.id != o2.id {
			t.Fatalf("slot %s: HRW not deterministic (%v/%v, %v/%v)", h, o1, ok1, o2, ok2)
		}
		if !inSet(o1) {
			t.Fatalf("slot %s: owner %q not a live member", h, o1)
		}
	}
}

// No live members → no owner (ok=false). The slot has no target; the full sweep
// covers it.
func TestHRWOwnerNoMembers(t *testing.T) {
	if _, ok := HRWOwner(nil, SlotID{h: 0}); ok {
		t.Fatal("HRWOwner with no members must return ok=false")
	}
}

// A tie on the fnv64a score is broken by the lexicographically greatest
// replica_id. We force a tie by constructing two members whose stored ids compare
// but whose scores we equalize via the same id text under different SlotIDs is not
// a tie; instead assert the documented tie-break rule directly on equal scores.
func TestHRWTieBreakGreatestID(t *testing.T) {
	// Construct a synthetic situation: two members, pick the slot where their
	// scores happen to differ is the common case; to exercise the tie-break branch
	// deterministically, drive HRWOwner through members that include duplicate
	// score behavior by comparing the rule itself.
	a, _ := NewReplicaID("node-1")
	b, _ := NewReplicaID("node-2")
	// Find any slot and confirm the winner is the argmax; if scores were equal the
	// greater id ("node-2") must win. We assert the invariant: the returned owner's
	// (score,id) is >= every other member's (score,id) under the documented order.
	members := []ReplicaID{a, b}
	for _, h := range slotIDs(64) {
		owner, ok := HRWOwner(members, h)
		if !ok {
			t.Fatalf("slot %s: no owner", h)
		}
		os := hrwScore(owner, h)
		for _, m := range members {
			ms := hrwScore(m, h)
			if ms > os || (ms == os && m.id > owner.id) {
				t.Fatalf("slot %s: %q (score %d) beats chosen %q (score %d) under argmax+tie-break", h, m.id, ms, owner.id, os)
			}
		}
	}
}

// Adding one replica to a 3-node set reassigns only ~1/N of slots (HRW's headline
// property: no rebalancing storm). Over 256 slots, the moved fraction should sit
// near 1/4; a generous band guards against the statistical spread.
func TestHRWReassignmentFraction(t *testing.T) {
	slots := slotIDs(256)
	before := reps(t, "r-aaa", "r-bbb", "r-ccc")
	after := reps(t, "r-aaa", "r-bbb", "r-ccc", "r-ddd")

	moved := 0
	for _, h := range slots {
		o1, _ := HRWOwner(before, h)
		o2, _ := HRWOwner(after, h)
		if o1.id != o2.id {
			moved++
		}
	}
	frac := float64(moved) / float64(len(slots))
	// Ideal 1/4 = 0.25; allow [0.12, 0.40] for the hash spread over 256 slots.
	if frac < 0.12 || frac > 0.40 {
		t.Fatalf("reassignment fraction %.3f (%d/256) outside ~1/N band [0.12,0.40]", frac, moved)
	}
	// Every moved slot must now belong to the NEW node (HRW only pulls slots toward
	// the joiner; it never reshuffles among the incumbents).
	for _, h := range slots {
		o1, _ := HRWOwner(before, h)
		o2, _ := HRWOwner(after, h)
		if o1.id != o2.id && o2.id != "r-ddd" {
			t.Fatalf("slot %s moved from %q to %q, but only the joiner r-ddd should gain slots", h, o1.id, o2.id)
		}
	}
}

func TestTargetedAndOwnedSlots(t *testing.T) {
	members := reps(t, "a", "b", "c")
	slots := slotIDs(32)
	me := members[0]
	targeted := TargetedSlots(me, members, slots)
	if len(targeted) == 0 || len(targeted) == len(slots) {
		t.Fatalf("targeted = %d of %d, expected a strict subset", len(targeted), len(slots))
	}
	// held is a subset of targeted (the CAS granted only some); OwnedSlots is the
	// intersection — the CAS is the authority, not the HRW math.
	held := map[SlotID]struct{}{}
	i := 0
	for h := range targeted {
		if i%2 == 0 {
			held[h] = struct{}{}
		}
		i++
	}
	// Add a held slot NOT targeted (a stale lease we no longer target): it must be
	// excluded from OwnedSlots.
	held[SlotID{h: 9999}] = struct{}{}
	owned := OwnedSlots(targeted, held)
	for _, h := range owned {
		if _, ok := targeted[h]; !ok {
			t.Fatalf("owned slot %s not targeted", h)
		}
		if _, ok := held[h]; !ok {
			t.Fatalf("owned slot %s not held", h)
		}
	}
	if len(owned) != len(held)-1 { // minus the un-targeted stale slot
		t.Fatalf("owned = %d, want %d (held ∩ targeted)", len(owned), len(held)-1)
	}
	// Output is sorted by index.
	for i := 1; i < len(owned); i++ {
		if owned[i-1].h > owned[i].h {
			t.Fatal("OwnedSlots not sorted")
		}
	}
}

func TestParseSlotClaim(t *testing.T) {
	cases := []struct {
		reply    []string
		status   SlotClaimStatus
		owner    string
		epoch    int64
		granted  bool
		transfer bool
		wantErr  bool
	}{
		{[]string{"CLAIMED", "A", "1", "1780000000000000000"}, SlotClaimed, "A", 1, true, true, false},
		{[]string{"RENEWED", "A", "1", "1780000000000000000"}, SlotRenewed, "A", 1, true, false, false},
		{[]string{"BUSY", "B", "2", "1780000000000000000"}, SlotBusy, "B", 2, false, false, false},
		{[]string{"WUT"}, 0, "", 0, false, false, true},
		{[]string{}, 0, "", 0, false, false, true},
	}
	for _, c := range cases {
		got, err := parseSlotClaim(c.reply)
		if c.wantErr {
			if err == nil {
				t.Fatalf("parseSlotClaim(%v): want error", c.reply)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseSlotClaim(%v): %v", c.reply, err)
		}
		if got.Status != c.status || got.Owner.id != c.owner || got.Epoch.Value() != c.epoch {
			t.Fatalf("parseSlotClaim(%v) = %+v, want status=%d owner=%q epoch=%d", c.reply, got, c.status, c.owner, c.epoch)
		}
		if got.Granted() != c.granted || got.Transferred() != c.transfer {
			t.Fatalf("parseSlotClaim(%v): granted=%v transfer=%v, want %v/%v", c.reply, got.Granted(), got.Transferred(), c.granted, c.transfer)
		}
	}
}

func TestParseOwnerCheck(t *testing.T) {
	cases := map[string]OwnerCheck{"OWNER": OwnerCheckOwner, "FENCED": OwnerCheckFenced, "UNOWNED": OwnerCheckUnowned}
	for s, want := range cases {
		got, err := parseOwnerCheck([]string{s})
		if err != nil || got != want {
			t.Fatalf("parseOwnerCheck(%q) = %v/%v, want %v", s, got, err, want)
		}
	}
	if _, err := parseOwnerCheck([]string{"NOPE"}); err == nil {
		t.Fatal("parseOwnerCheck unknown: want error")
	}
	if got, _ := parseOwnerCheck([]string{"OWNER"}); !got.OK() {
		t.Fatal("OWNER must report OK()")
	}
	if got, _ := parseOwnerCheck([]string{"FENCED"}); got.OK() {
		t.Fatal("FENCED must not report OK()")
	}
}

func TestGenerateReplicaIDUniqueAndParseable(t *testing.T) {
	seen := map[string]struct{}{}
	for i := 0; i < 100; i++ {
		r, err := GenerateReplicaID("chronicle-7", rand.Reader)
		if err != nil {
			t.Fatalf("GenerateReplicaID: %v", err)
		}
		if _, dup := seen[r.id]; dup {
			t.Fatalf("duplicate replica id %q", r.id)
		}
		seen[r.id] = struct{}{}
		// "<podName>-<32 hex>" — the form the killSlotOwner nemesis parses back.
		if want := "chronicle-7-"; len(r.id) != len(want)+32 || r.id[:len(want)] != want {
			t.Fatalf("replica id %q not of the form chronicle-7-<32hex>", r.id)
		}
	}
	// Empty podName falls back to "local".
	r, _ := GenerateReplicaID("", rand.Reader)
	if r.id[:6] != "local-" {
		t.Fatalf("empty podName fallback = %q, want local-...", r.id)
	}
	// Deterministic given the rand source: same bytes → same id.
	src := bytes.Repeat([]byte{0xab}, 16)
	a, _ := GenerateReplicaID("p", bytes.NewReader(src))
	b, _ := GenerateReplicaID("p", bytes.NewReader(src))
	if a.id != b.id || a.id != "p-"+"abababababababababababababababab" {
		t.Fatalf("not deterministic given rnd: %q vs %q", a.id, b.id)
	}
}

func TestNewReplicaIDRejectsEmpty(t *testing.T) {
	if _, err := NewReplicaID(""); err == nil {
		t.Fatal("NewReplicaID(\"\"): want error")
	}
}

func TestNewSlotIDBounds(t *testing.T) {
	if _, err := NewSlotID(0); err != nil {
		t.Fatalf("NewSlotID(0): %v", err)
	}
	if _, err := NewSlotID(ownershipSlots); err == nil {
		t.Fatalf("NewSlotID(%d): want out-of-range error", ownershipSlots)
	}
	if _, err := NewSlotID(-1); err == nil {
		t.Fatal("NewSlotID(-1): want error")
	}
	if got := AllSlots(); len(got) != ownershipSlots {
		t.Fatalf("AllSlots() = %d, want %d", len(got), ownershipSlots)
	}
}

func TestCheckOwnershipConfigInvariants(t *testing.T) {
	// The documented defaults (05:502-505) satisfy both invariants.
	if err := CheckOwnershipConfig(9*time.Second, 3*time.Second, 9*time.Second, 3*time.Second); err != nil {
		t.Fatalf("defaults must be valid: %v", err)
	}
	cases := []struct {
		name                               string
		member, heartbeat, slot, reconcile time.Duration
		wantErr                            bool
	}{
		{"defaults", 9 * time.Second, 3 * time.Second, 9 * time.Second, 3 * time.Second, false},
		{"heartbeat == ttl/2 (no headroom)", 9 * time.Second, 4500 * time.Millisecond, 9 * time.Second, 3 * time.Second, true},
		{"heartbeat > ttl/2", 9 * time.Second, 5 * time.Second, 9 * time.Second, 3 * time.Second, true},
		{"reconcile > heartbeat", 9 * time.Second, 3 * time.Second, 9 * time.Second, 4 * time.Second, true},
		{"reconcile == heartbeat ok", 9 * time.Second, 3 * time.Second, 9 * time.Second, 3 * time.Second, false},
		{"non-positive member", 0, 3 * time.Second, 9 * time.Second, 3 * time.Second, true},
		{"non-positive slot", 9 * time.Second, 3 * time.Second, 0, 3 * time.Second, true},
	}
	for _, c := range cases {
		err := CheckOwnershipConfig(c.member, c.heartbeat, c.slot, c.reconcile)
		if (err != nil) != c.wantErr {
			t.Fatalf("%s: err=%v, wantErr=%v", c.name, err, c.wantErr)
		}
	}
}

// A compile-time check that the status sum types are usable in a switch (the
// "make invalid states unrepresentable" contract: a caller switches, never tests
// a bool).
func ExampleSlotClaim() {
	c, _ := parseSlotClaim([]string{"CLAIMED", "A", "1", "0"})
	switch c.Status {
	case SlotClaimed:
		fmt.Println("claimed")
	case SlotRenewed:
		fmt.Println("renewed")
	case SlotBusy:
		fmt.Println("busy")
	}
	// Output: claimed
}
