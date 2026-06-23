package webhook

import (
	"context"
	"strconv"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"pgregory.net/rapid"
)

// predicate_differential_test.go is the rapid triple-mirror differential for the
// control-plane fence/ordering/slot predicates that exist in THREE independent
// hand-maintained copies (issue #33, P1.4): the Go pure core (state.go/keys.go),
// the live Lua (scripts/common.lua), and the Jepsen checker reference mirror
// (jepsen/checker). Each predicate is the safety boundary for the wake/lease
// engine — INV-FENCE-01 (single-holder) and INV-FENCE-03 (stale-gen op is inert)
// are stated entirely in terms of `fenced`; INV-CURSOR-01 in terms of
// `offset_greater`; INV-JEP-T5-02 in terms of the home-slot math. Independence is
// the source of value (a single shared helper could not catch a translation bug),
// but independence with no cross-check is exactly the model-vs-implementation gap
// (DESIGN.md §5) — this property pins the copies together over a generated domain.
//
// SPLIT ACROSS TWO PACKAGES, by which package owns the unexported copy:
//   - HERE (package webhook): Go core FenceDecision/offsetGreater vs the LIVE Lua
//     fenced/offset_greater, driven through probe_predicates.lua, which prepends
//     the REAL common.lua (no re-transcription). This is the load-bearing
//     model-vs-implementation edge.
//   - jepsen/checker/predicate_mirror_test.go (package main): the checker's own
//     dsSlotOf/crc16/clusterSlot/`fenced`/offsetGreater mirrors vs webhook's
//     exported core (SlotOf/BaseSubID/FenceDecision/OffsetGreater). The checker is
//     `package main` and cannot be imported, so its mirrors are invoked there
//     through a test-only accessor (export_test.go), never re-implemented here —
//     preserving the independent-copy property the differential depends on.
//
// The slot mirror (TestSlotHomingMirror) is a TWO-Go-copy + live-CRC16
// differential, NOT a three-Lua mirror: slotOf is DELIBERATELY NOT Redis CRC16
// (keys.go) and there is NO Lua copy of the FNV-1a/32 home-slot math — Lua only
// ever sees the CRC16 cluster slot the client computes from the produced
// {__ds:h} tag. So the home-slot FNV is pinned Go-vs-checker (in the checker
// test) and the produced tag is pinned against the table-free CCITT/XMODEM CRC16
// the cluster routes by (clusterSlot, shared with pure_test.go).
//
// Skipped under -short and when Redis is unreachable (probeStore handles both),
// matching the real-Redis-or-skip convention of the equivalence harness. On any
// divergence rapid shrinks to a minimal counterexample and prints the replay
// seed; committed boundary seeds live in fenceBoundaryTuples / offsetBoundaryPairs
// and testdata/predicate_counterexamples.json.

// probeStore loads the test-only probe_predicates.lua (prepended with the SHIPPED
// common.lua via loadScript) and returns it with a live Redis client, or skips.
// loadScript is the exact prelude+body concatenation scripts.go uses for the
// production scripts, so `fenced` / `offset_greater` under test are the shipped
// source, byte-for-byte.
func probeStore(t *testing.T) (*goredis.Script, goredis.UniversalClient) {
	t.Helper()
	_, client := newTestStore(t) // skips under -short / unreachable Redis; flushes DB 14
	return loadScript("probe_predicates.lua"), client
}

// luaFenced drives the LIVE common.lua `fenced` exactly as ack.lua/release.lua do:
// every fence argument is a string (cur_gen from HGET of an HINCRBY'd field;
// req_gen/token_gen from strconv.FormatInt; the wake ids raw), so the probe
// exercises the production string-compare path, not a numeric re-transcription.
func luaFenced(t *rapid.T, script *goredis.Script, client goredis.UniversalClient,
	curGen int64, curWake string, reqGen int64, reqWake string, tokenGen int64,
) bool {
	n, err := script.Run(
		context.Background(), client, nil,
		"fenced",
		strconv.FormatInt(curGen, 10), curWake,
		strconv.FormatInt(reqGen, 10), reqWake,
		strconv.FormatInt(tokenGen, 10),
	).Int()
	if err != nil {
		t.Fatalf("lua fenced probe: %v", err)
	}
	return n == 1
}

// luaOffsetGreater drives the LIVE common.lua `offset_greater(a, b)`.
func luaOffsetGreater(t *rapid.T, script *goredis.Script, client goredis.UniversalClient, a, b string) bool {
	n, err := script.Run(context.Background(), client, nil, "offset", a, b).Int()
	if err != nil {
		t.Fatalf("lua offset_greater probe: %v", err)
	}
	return n == 1
}

// ---- fence-decision triple mirror ----

// fenceBoundaryTuples are the committed boundary seeds the fence generator always
// covers (acceptance criterion: req_wake=="", req_gen==cur_gen ∧ token_gen!=cur_gen,
// all-three-equal — the sole accept case — and off-by-one generations). Drawn
// alongside the random tuples so the accept/reject ladder is never missed even on
// a thin random sample (research/03 pitfall 6: boundary-biased generators).
var fenceBoundaryTuples = []fenceTuple{
	{0, "w", 0, "w", 0}, // all-three-equal: the SOLE accept case
	{5, "w", 5, "w", 5}, // all-three-equal at a nonzero generation
	{5, "w", 5, "w", 4}, // token_gen off-by-one below: fenced
	{5, "w", 5, "w", 6}, // token_gen off-by-one above: fenced
	{5, "w", 4, "w", 5}, // req_gen off-by-one below: fenced
	{5, "w", 6, "w", 5}, // req_gen off-by-one above: fenced
	{5, "w", 5, "", 5},  // req_wake == "" : fenced even with matching generations
	{5, "", 5, "", 5},   // cur_wake == "" and req_wake == "": still fenced (empty req_wake)
	{5, "w", 5, "x", 5}, // wake mismatch: fenced
	{5, "w", 5, "w", 5}, // accept again (idempotent boundary)
	{0, "", 0, "", 0},   // all-zero with empty wakes: fenced (req_wake == "")
	{9223372036854775807, "w", 9223372036854775807, "w", 9223372036854775807}, // int64 max accept
}

// fenceTuple is one generated fence input.
type fenceTuple struct {
	curGen   int64
	curWake  string
	reqGen   int64
	reqWake  string
	tokenGen int64
}

// genGeneration draws a generation biased to the boundary: 0 (first fence), small
// values where off-by-one matters, and the int64 extremes (the fence is a pure
// equality so the magnitude only matters for the Go-int64 / Lua-canonical-decimal
// string-compare agreement).
func genGeneration() *rapid.Generator[int64] {
	return rapid.OneOf(
		rapid.SampledFrom([]int64{0, 1, 2, 5, 6, -1, 9223372036854775807, 9223372036854775806}),
		rapid.Int64Range(0, 20),
	)
}

// genWake draws a wake id biased to the empty string (the req_wake=="" fence
// branch) and a few realistic values that collide often so wake (mis)matches are
// exercised densely.
func genWake() *rapid.Generator[string] {
	return rapid.SampledFrom([]string{"", "w", "w2", "01H8X", "wake-abc"})
}

// genFenceTuple builds a boundary-biased fence tuple. With ~1/3 probability it
// snaps two of (cur_gen, req_gen, token_gen) equal so the "near-accept" region —
// where exactly one field is off — is densely sampled (a uniform draw almost never
// makes three independent int64s equal, so the accept case would otherwise be
// invisible: research/03 pitfall 6).
func genFenceTuple() *rapid.Generator[fenceTuple] {
	return rapid.Custom(func(t *rapid.T) fenceTuple {
		cur := genGeneration().Draw(t, "curGen")
		reqGen := cur
		tokenGen := cur
		// Independently perturb req_gen / token_gen so all of {accept, req off,
		// token off, both off} occur; SampledFrom over small deltas keeps the
		// near-accept region dense.
		reqGen += rapid.SampledFrom([]int64{0, 0, 0, 1, -1, 2}).Draw(t, "reqGenDelta")
		tokenGen += rapid.SampledFrom([]int64{0, 0, 0, 1, -1, 2}).Draw(t, "tokenGenDelta")
		curWake := genWake().Draw(t, "curWake")
		reqWake := curWake
		// Half the time perturb the wake so the wake-(mis)match and empty-wake
		// branches fire independently of the generation deltas.
		if rapid.Bool().Draw(t, "perturbWake") {
			reqWake = genWake().Draw(t, "reqWake")
		}
		return fenceTuple{curGen: cur, curWake: curWake, reqGen: reqGen, reqWake: reqWake, tokenGen: tokenGen}
	})
}

// TestFenceDecisionTripleMirror asserts, over a boundary-biased generated domain
// of (cur_gen, cur_wake, req_gen, req_wake, token_gen) tuples, that the Go core
// FenceDecision and the live Lua `fenced` agree on EVERY tuple:
//
//	FenceDecision(...) == ErrCodeFenced  ⟺  Lua fenced(...) == true
//
// The third mirror — the checker's fence-aware allowed-status classification (a
// fenced op must land in the inert {FENCED,BUSY,STALE,NOSUB} vocabulary and grant
// nothing) and the byte-identical `fenced` formula in model_fence.go — is pinned
// in jepsen/checker/predicate_mirror_test.go against this same FenceDecision (the
// checker is package main and cannot be imported here). Together the two files
// close the triangle: Go⟷Lua here, Go⟷checker there, and Go is the shared apex.
// [INV-FENCE-01, INV-FENCE-03]
func TestFenceDecisionTripleMirror(t *testing.T) {
	script, client := probeStore(t)

	rapid.Check(t, func(rt *rapid.T) {
		var tup fenceTuple
		// Always include the committed boundary seeds in the sampled domain so the
		// accept case and every fence branch are covered on every run.
		if rapid.Bool().Draw(rt, "useBoundarySeed") {
			tup = rapid.SampledFrom(fenceBoundaryTuples).Draw(rt, "boundaryTuple")
		} else {
			tup = genFenceTuple().Draw(rt, "tuple")
		}

		cur := Subscription{Generation: tup.curGen, WakeID: tup.curWake}
		goFenced := FenceDecision(cur, tup.reqGen, tup.reqWake, tup.tokenGen) == ErrCodeFenced
		lua := luaFenced(rt, script, client, tup.curGen, tup.curWake, tup.reqGen, tup.reqWake, tup.tokenGen)

		if goFenced != lua {
			rt.Fatalf("fence divergence on %+v: Go FenceDecision fenced=%v, live Lua fenced=%v", tup, goFenced, lua)
		}
		// The single-holder safety property restated on the boundary: the op may
		// proceed IFF token_gen, req_gen, and cur_gen all match and req_wake is the
		// non-empty current wake. Pin it independently so a both-sides-wrong drift
		// (Go and Lua agreeing on a WRONG answer) is still caught.
		wantProceed := tup.tokenGen == tup.curGen && tup.reqGen == tup.curGen &&
			tup.reqWake != "" && tup.reqWake == tup.curWake
		if goFenced == wantProceed {
			rt.Fatalf("fence semantics wrong on %+v: fenced=%v but wantProceed=%v", tup, goFenced, wantProceed)
		}
	})
}

// ---- offset-greater triple mirror ----

// offsetBoundaryPairs are the committed offset seeds the generator always covers
// (acceptance criterion: the "-1" and "" beginning sentinels, equal pairs,
// fixed-width zero-padded pairs, and non-padded adversarial pairs).
var offsetBoundaryPairs = [][2]string{
	{"-1", "-1"},                             // both sentinel: equal -> not greater
	{"", ""},                                 // both empty sentinel: equal -> not greater
	{"-1", ""},                               // the two sentinel spellings are equivalent: not greater either way
	{"", "-1"},                               // (symmetry of the sentinel equivalence)
	{"5", "-1"},                              // real > beginning sentinel
	{"5", ""},                                // real > empty sentinel
	{"-1", "5"},                              // sentinel < real
	{"", "5"},                                // empty sentinel < real
	{"0000000000000005", "0000000000000005"}, // fixed-width equal
	{"0000000000000006", "0000000000000005"}, // fixed-width greater
	{"0000000000000005", "0000000000000006"}, // fixed-width lesser
	{"10", "9"},                              // ADVERSARIAL non-padded: lexically "10" < "9" (the LB-1/LB-2 footgun)
	{"9", "10"},                              // adversarial reverse
	{"100", "99"},                            // adversarial wider digit width
}

// genOffset draws an offset biased to the sentinels and to fixed-width zero-padded
// values (the protocol's lex-safe shape) plus deliberately non-padded adversarial
// values, so the bytewise `>` is exercised exactly where the digit-width footgun
// lives.
func genOffset() *rapid.Generator[string] {
	return rapid.OneOf(
		rapid.SampledFrom([]string{"-1", "", "9", "10", "99", "100"}),
		rapid.Custom(func(t *rapid.T) string {
			// Fixed-width zero-padded (16 digits): the spec's lex-safe offset shape.
			return zeroPad16(rapid.Uint64Range(0, 1<<20).Draw(t, "padOff"))
		}),
		rapid.Custom(func(t *rapid.T) string {
			// Non-padded decimal across digit widths: the adversarial region.
			return strconv.FormatUint(rapid.Uint64Range(0, 1_000_000).Draw(t, "rawOff"), 10)
		}),
	)
}

func zeroPad16(v uint64) string {
	s := strconv.FormatUint(v, 10)
	for len(s) < 16 {
		s = "0" + s
	}
	return s
}

// TestOffsetGreaterTripleMirror asserts the Go core offsetGreater and the live Lua
// offset_greater agree on EVERY generated (a, b) pair, including both sentinel
// spellings ("-1" and ""), equal pairs, fixed-width zero-padded pairs, and
// non-padded adversarial pairs where bytewise order diverges from numeric order.
// The checker's offsetGreater (check_cursor.go) is the same monotone order the
// cursor-advance guard assumes and is pinned against OffsetGreater in the checker
// test. [INV-CURSOR-01]
func TestOffsetGreaterTripleMirror(t *testing.T) {
	script, client := probeStore(t)

	rapid.Check(t, func(rt *rapid.T) {
		var a, b string
		if rapid.Bool().Draw(rt, "useBoundaryPair") {
			p := rapid.SampledFrom(offsetBoundaryPairs).Draw(rt, "boundaryPair")
			a, b = p[0], p[1]
		} else {
			a = genOffset().Draw(rt, "a")
			b = genOffset().Draw(rt, "b")
		}

		goGt := offsetGreater(a, b)
		luaGt := luaOffsetGreater(rt, script, client, a, b)
		if goGt != luaGt {
			rt.Fatalf("offset_greater divergence on (a=%q, b=%q): Go=%v live Lua=%v", a, b, goGt, luaGt)
		}
		// Order axioms, pinned independently so a both-sides-wrong drift is caught:
		// irreflexive (a > a is false) and asymmetric (not both a>b and b>a).
		if a == b && goGt {
			rt.Fatalf("offset_greater not irreflexive: (%q,%q) -> true", a, b)
		}
		if goGt && offsetGreater(b, a) {
			rt.Fatalf("offset_greater not asymmetric: both (%q>%q) and (%q>%q)", a, b, b, a)
		}
	})
}

// ---- slot-homing mirror (TWO-Go-copy + live-CRC16; NO Lua FNV copy) ----

// slotBoundaryIDs are the committed slot seeds the generator always covers
// (acceptance criterion: valid-digit :g:<n> suffixes, empty suffixes, non-digit
// suffixes, and a real id that coincidentally ends in :g:7).
var slotBoundaryIDs = []string{
	"agent-handler",         // plain id, no suffix
	"sub-with-{braces}",     // id carrying its own braces (must not escape the tag)
	"x:y:z",                 // colons but no :g: suffix
	"sub1:g:0",              // valid-digit g=0 suffix (strips to "sub1")
	"sub1:g:7",              // valid-digit g=7 suffix
	"sub1:g:42",             // multi-digit valid suffix
	"sub1:g:",               // EMPTY suffix: NOT stripped (baseSubID returns whole id)
	"sub1:g:x",              // non-digit suffix: NOT stripped
	"sub1:g:7x",             // mixed suffix: NOT stripped
	"real:id:that:ends:g:7", // a "real" id coincidentally ending :g:7 (strips its tail)
	"::g:1",                 // pathological short id with a valid suffix
}

// genSubID draws a subscription id biased to the :g:<n> claim-granularity suffix
// shapes (valid digits, empty, non-digit, coincidental) so the baseSubID strip
// boundary is densely covered.
func genSubID() *rapid.Generator[string] {
	base := rapid.SampledFrom([]string{"s", "sub1", "agent-handler", "x:y:z", "sub-with-{braces}", "a:b:c:g:9"})
	return rapid.Custom(func(t *rapid.T) string {
		b := base.Draw(t, "base")
		switch rapid.SampledFrom([]string{"none", "digits", "empty", "nondigit", "mixed"}).Draw(t, "suffixKind") {
		case "digits":
			return b + ":g:" + strconv.Itoa(rapid.IntRange(0, 99).Draw(t, "g"))
		case "empty":
			return b + ":g:"
		case "nondigit":
			return b + ":g:" + rapid.SampledFrom([]string{"x", "abc", "-1", "1a"}).Draw(t, "nd")
		case "mixed":
			return b + ":g:" + strconv.Itoa(rapid.IntRange(0, 9).Draw(t, "gm")) + "x"
		default:
			return b
		}
	})
}

// TestSlotHomingMirror asserts the Go core home-slot math agrees with the
// table-free CCITT/XMODEM CRC16 the cluster routes by, for every generated id:
//
//   - SlotOf is the FNV-1a/32 of the (g-suffix-stripped) base id mod SubSlots — an
//     INDEPENDENT, language-stable application hash, DELIBERATELY NOT CRC16.
//   - The PRODUCED {__ds:h} hash tag, routed through the table-free clusterSlot
//     (shared with pure_test.go; the cluster's authority on "same slot"), resolves
//     every key a subscription touches to ONE cluster slot — the INV-JEP-T5-02
//     whole-subscription single-slot-homing precondition.
//
// This is a TWO-Go-copy + live-CRC16 differential, NOT a three-Lua mirror: there
// is NO Lua copy of the FNV home-slot math (keys.go: "DELIBERATELY NOT Redis
// CRC16") — Lua only ever sees the CRC16 cluster slot. The Go-vs-checker FNV edge
// (SlotOf/BaseSubID vs dsSlotOf/allDigits) is pinned in
// jepsen/checker/predicate_mirror_test.go, where the checker mirrors are
// reachable. [INV-JEP-T5-02]
func TestSlotHomingMirror(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		var id string
		if rapid.Bool().Draw(rt, "useBoundaryID") {
			id = rapid.SampledFrom(slotBoundaryIDs).Draw(rt, "boundaryID")
		} else {
			id = genSubID().Draw(rt, "id")
		}

		h := SlotOf(id)
		if h < 0 || h >= SubSlots {
			rt.Fatalf("SlotOf(%q) = %d out of [0,%d)", id, h, SubSlots)
		}
		// The slot is the FNV-1a/32 of the g-stripped base id (independent of CRC16).
		if h != fnv32aMod(BaseSubID(id), SubSlots) {
			rt.Fatalf("SlotOf(%q) = %d, want fnv32a(BaseSubID)%%%d = %d", id, h, SubSlots, fnv32aMod(BaseSubID(id), SubSlots))
		}
		// A g>0 schedule member homes to its parent sub's slot (the #11 split inherits
		// the #15 tag): shardMember derives "<base>:g:<g>" from the BASE id, and SlotOf
		// strips that :g:<digits> suffix so a drained member resolves back to its sub's
		// slot. baseSubID strips only ONE suffix level (it is not recursive — the store
		// only ever forms shardMember from a terminal base id, never from an
		// already-:g:<digits>-suffixed one), so assert the invariant on a TERMINAL base
		// b where BaseSubID(b)==b. This faithfully models how the lease/retry/due
		// workers re-resolve a drained "<base>:g:<g>" member back to subShardKey.
		base := BaseSubID(id)
		if BaseSubID(base) == base {
			if g := SlotOf(shardMember(base, 3)); g != SlotOf(base) {
				rt.Fatalf("shard member %q must home to its sub's slot %d, got %d", shardMember(base, 3), SlotOf(base), g)
			}
		}

		// Every key the whole subscription touches resolves to ONE cluster slot
		// under the table-free CRC16 (the T5 static precondition the atomic scripts
		// rest on). clusterSlot is shared with pure_test.go (the cluster's authority).
		tag := SlotTag(id)
		keys := []string{
			subKey(id), linksKey(id),
			subShardKey(id, 0), subShardKey(id, 1),
			leaseZKey(h), retryZKey(h), dueZKey(h), subsKey(h),
			streamSubsKey(h, "events/a"),
		}
		want := clusterSlot(keys[0])
		for _, k := range keys {
			if got := clusterSlot(k); got != want {
				rt.Fatalf("CROSSSLOT for id=%q: key %q -> slot %d, want %d (tag=%q)", id, k, got, want, tag)
			}
		}
	})
}
