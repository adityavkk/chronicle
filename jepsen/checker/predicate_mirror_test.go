package main

import (
	"strconv"
	"testing"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// predicate_mirror_test.go is the checker side of the triple-mirror differential
// for the control-plane fence/slot/ordering predicates (issue #33, P1.4). It
// closes the triangle the webhook-side property opens:
//
//	webhook/predicate_differential_test.go : Go core  ⟷  LIVE Lua   (model vs impl)
//	THIS FILE                              : Go core  ⟷  checker     (model vs reference)
//
// Go (webhook's exported SlotOf/BaseSubID/FenceDecision/OffsetGreater) is the
// shared apex; transitively all three copies agree. The checker is `package main`
// and cannot be imported by the webhook test, so its independent mirrors
// (dsSlotOf/crc16/clusterSlot/allDigits, checkerFenced, offsetGreater) are reached
// here through the test-only accessor in export_test.go — never re-implemented —
// preserving the independent-copy property the differential depends on.
//
// These sub-properties are PURE (no Redis): the checker mirrors are pure-core
// functions and the Go-core predicates are pure, so the property runs everywhere
// `go test ./...` runs (the same containerized-Redis PBT CI job). The live-Lua
// edge that DOES need Redis is the webhook-side file.
//
// rapid shrinks any divergence to a minimal counterexample and prints the replay
// seed; the boundary seeds the webhook side commits (fence tuples, offset pairs,
// :g: slot ids) are mirrored in the generators here so both sides cover the same
// boundary domain.

// ---- fence: Go FenceDecision ⟷ checker CheckerFenced + stale-gen allowed-status ----

// TestFenceDecisionCheckerMirror asserts the checker's independent fence copy
// (CheckerFenced) agrees with webhook.FenceDecision on every generated tuple, and
// that a fenced tuple is classified into the checker's INERT (non-granting)
// stale-gen vocabulary {FENCED,BUSY,STALE,NOSUB} — the fence-aware allowed-status
// gate (check_stalegen.go) that T4 leans on. [INV-FENCE-01, INV-FENCE-03]
func TestFenceDecisionCheckerMirror(t *testing.T) {
	gen := genGenerationC()
	wake := rapid.SampledFrom([]string{"", "w", "w2", "wake-abc"})

	rapid.Check(t, func(rt *rapid.T) {
		cur := gen.Draw(rt, "curGen")
		// Bias req/token generations to the near-accept region (snap-equal often).
		reqGen := cur + rapid.SampledFrom([]int64{0, 0, 0, 1, -1}).Draw(rt, "reqDelta")
		tokenGen := cur + rapid.SampledFrom([]int64{0, 0, 0, 1, -1}).Draw(rt, "tokDelta")
		curWake := wake.Draw(rt, "curWake")
		reqWake := curWake
		if rapid.Bool().Draw(rt, "perturbWake") {
			reqWake = wake.Draw(rt, "reqWake")
		}

		sub := webhook.Subscription{Generation: cur, WakeID: curWake}
		goFenced := webhook.FenceDecision(sub, reqGen, reqWake, tokenGen) == webhook.ErrCodeFenced
		checkerF := CheckerFenced(cur, reqGen, tokenGen, curWake, reqWake)
		if goFenced != checkerF {
			rt.Fatalf("fence divergence: Go FenceDecision fenced=%v, checker checkerFenced=%v "+
				"(cur=%d req=%d tok=%d curWake=%q reqWake=%q)", goFenced, checkerF, cur, reqGen, tokenGen, curWake, reqWake)
		}

		// A fenced op MUST be classifiable as inert by the checker's allowed-status
		// gate: ack.lua / release.lua fence first (FENCED), and arm/claim refuse a
		// non-idle / held sub (BUSY). So FENCED is always in the inert set — the
		// gate never lets a fenced op grant anything. (statusOK is granting and is
		// deliberately NOT in the set.)
		if goFenced && !CheckerStaleGenInert("FENCED") {
			rt.Fatalf("checker allowed-status gate must classify a fenced op (FENCED) as inert")
		}
		if CheckerStaleGenInert("OK") {
			rt.Fatalf("checker allowed-status gate must NOT classify OK (granting) as inert")
		}
	})
}

func genGenerationC() *rapid.Generator[int64] {
	return rapid.OneOf(
		rapid.SampledFrom([]int64{0, 1, 2, 5, 6, -1, 9223372036854775807, 9223372036854775806}),
		rapid.Int64Range(0, 20),
	)
}

// ---- offset: Go OffsetGreater ⟷ checker CheckerOffsetGreater ----

var offsetBoundaryPairsC = [][2]string{
	{"-1", "-1"},
	{"", ""},
	{"-1", ""},
	{"", "-1"},
	{"5", "-1"},
	{"5", ""},
	{"-1", "5"},
	{"", "5"},
	{"0000000000000005", "0000000000000005"},
	{"0000000000000006", "0000000000000005"},
	{"0000000000000005", "0000000000000006"},
	{"10", "9"},
	{"9", "10"},
	{"100", "99"},
}

// TestOffsetGreaterCheckerMirror asserts webhook.OffsetGreater and the checker's
// offsetGreater (the order the cursor monotonicity checker assumes) agree on every
// generated pair, including both sentinel spellings and the adversarial
// non-padded digit-width region. [INV-CURSOR-01]
func TestOffsetGreaterCheckerMirror(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		var a, b string
		if rapid.Bool().Draw(rt, "useBoundary") {
			p := rapid.SampledFrom(offsetBoundaryPairsC).Draw(rt, "pair")
			a, b = p[0], p[1]
		} else {
			a = genOffsetC().Draw(rt, "a")
			b = genOffsetC().Draw(rt, "b")
		}
		if webhook.OffsetGreater(a, b) != CheckerOffsetGreater(a, b) {
			rt.Fatalf("offset order divergence on (a=%q,b=%q): Go=%v checker=%v",
				a, b, webhook.OffsetGreater(a, b), CheckerOffsetGreater(a, b))
		}
	})
}

func genOffsetC() *rapid.Generator[string] {
	return rapid.OneOf(
		rapid.SampledFrom([]string{"-1", "", "9", "10", "99", "100"}),
		rapid.Custom(func(t *rapid.T) string {
			s := strconv.FormatUint(rapid.Uint64Range(0, 1<<20).Draw(t, "v"), 10)
			for len(s) < 16 {
				s = "0" + s
			}
			return s
		}),
		rapid.Custom(func(t *rapid.T) string {
			return strconv.FormatUint(rapid.Uint64Range(0, 1_000_000).Draw(t, "raw"), 10)
		}),
	)
}

// ---- slot: Go SlotOf/BaseSubID ⟷ checker dsSlotOf/allDigits (the FNV mirror) ----

var slotBoundaryIDsC = []string{
	"agent-handler", "sub-with-{braces}", "x:y:z",
	"sub1:g:0", "sub1:g:7", "sub1:g:42",
	"sub1:g:", "sub1:g:x", "sub1:g:7x",
	"real:id:that:ends:g:7", "::g:1", "a:b:c:g:9:g:3",
}

// TestSlotHomingCheckerMirror is the FNV home-slot edge the webhook side cannot
// reach (the checker is package main): webhook.SlotOf / BaseSubID vs the checker's
// dsSlotOf / allDigits, plus SubSlots == dsSubSlots, plus the produced tag routing
// to ONE cluster slot under the checker's table-free CRC16 (subKeysOneSlot) — the
// INV-JEP-T5-02 precondition. Explicitly a TWO-Go-copy + CRC16 differential: there
// is NO Lua FNV copy to mirror (slotOf is "DELIBERATELY NOT Redis CRC16"; Lua only
// sees the CRC16 cluster slot). [INV-JEP-T5-02]
func TestSlotHomingCheckerMirror(t *testing.T) {
	// The two S constants must match or the home slots cannot agree.
	if webhook.SubSlots != CheckerSubSlots {
		t.Fatalf("S mismatch: webhook.SubSlots=%d checker dsSubSlots=%d", webhook.SubSlots, CheckerSubSlots)
	}

	rapid.Check(t, func(rt *rapid.T) {
		var id string
		if rapid.Bool().Draw(rt, "useBoundary") {
			id = rapid.SampledFrom(slotBoundaryIDsC).Draw(rt, "boundaryID")
		} else {
			id = genSubIDC().Draw(rt, "id")
		}

		// The home-slot FNV math agrees between the two independent Go copies.
		if got, want := webhook.SlotOf(id), CheckerDsSlotOf(id); got != want {
			rt.Fatalf("home-slot divergence on id=%q: webhook.SlotOf=%d checker dsSlotOf=%d", id, got, want)
		}
		// The g-suffix strip agrees: BaseSubID strips iff the checker's allDigits
		// accepts the suffix. (Both strip only ":g:<digits>" with a non-empty digit
		// run, picking the LAST ":g:".)
		if i := lastGColon(id); i >= 0 {
			suf := id[i+3:]
			stripped := webhook.BaseSubID(id) != id
			wantStrip := CheckerAllDigits(suf)
			if stripped != wantStrip {
				rt.Fatalf("g-suffix strip divergence on id=%q (suffix=%q): webhook stripped=%v checker allDigits=%v",
					id, suf, stripped, wantStrip)
			}
		}

		// The produced {__ds:h} tag routes every key the subscription touches to ONE
		// cluster slot under the checker's table-free CRC16 (T5 static precondition).
		if _, ok := CheckerSubKeysOneSlot(id); !ok {
			rt.Fatalf("INV-JEP-T5-02: id=%q does not resolve to a single cluster slot", id)
		}
	})
}

// lastGColon mirrors strings.LastIndex(id, ":g:") for the strip-boundary check.
func lastGColon(id string) int {
	for i := len(id) - 3; i >= 0; i-- {
		if id[i] == ':' && id[i+1] == 'g' && id[i+2] == ':' {
			return i
		}
	}
	return -1
}

func genSubIDC() *rapid.Generator[string] {
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
