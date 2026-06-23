package store

import (
	"testing"

	"pgregory.net/rapid"
)

// readseq_divergence_test.go arms LB-3 (issue #32): MemoryStore.readOwnMessages
// gates the read window on the ByteOffset field ALONE
//
//	if msg.Offset.ByteOffset > offset.ByteOffset { ... }
//
// while the Redis backend range-scans frames with ZRANGEBYLEX over the FULL
// offset string (read.lua: ZRANGEBYLEX KEYS[2] lexLowerBound(offset) '+'), whose
// exclusive lower bound lexLowerBound (store/redis/keys.go) is built from
// offset.String() — the "%016d_%016d" rendering of BOTH ReadSeq and ByteOffset.
// Equivalently, Redis orders by the full Offset.Compare (high-order ReadSeq,
// then ByteOffset), and within the LB-1 width-safe domain (< 10^16) byte-lex
// order of Offset.String() equals Offset.Compare exactly (asserted in
// offset_property_test.go).
//
// The two backends agree TODAY only because ReadSeq is always 0 (it is
// documented "For future log rotation support" in offset.go and never set
// non-zero, because log rotation is unimplemented). The moment rotation lands
// and ReadSeq becomes non-zero, two messages sharing a ByteOffset but differing
// in ReadSeq are ordered correctly by Redis and CONFLATED by readOwnMessages —
// a read-path divergence with the MemoryStore oracle on the WRONG side.
//
// This file pins that boundary NOW, machine-checked and CI-green, in two halves:
//
//   - TestReadSeqAgreesWhileZero asserts the CURRENTLY-TRUE invariant: while
//     every ReadSeq == 0, readOwnMessages's ByteOffset-only selection equals the
//     full-Offset selection. This is the "they agree only because ReadSeq == 0"
//     claim from INV-FRAME-01, asserted rather than assumed.
//
//   - TestReadSeqDivergesWhenNonZero is the RED-ARMED regression: it constructs
//     non-zero-ReadSeq offsets and asserts the divergence is REAL and reproduces
//     as a concrete counterexample (the ByteOffset-only selection differs from
//     the full-Offset selection). It is green today because it asserts the
//     divergence; it documents that readOwnMessages MUST compare the full Offset
//     before log rotation ships. The committed boundary counterexample lives in
//     TestReadSeqDivergenceCounterexampleLB3.
//
// SURFACE + DOCUMENT only — no behavior change to readOwnMessages. The fix
// (compare the full Offset, e.g. Compare(msg.Offset, offset) > 0) lands with the
// future log-rotation feature, which inherits this armed test.
//
//	// LB-3: arms the ReadSeq read-comparison divergence; the readOwnMessages
//	// fix lands with the log-rotation feature, not here.

// readByteOffsetOnly mirrors readOwnMessages's actual selection predicate:
// strictly-greater ByteOffset, ignoring ReadSeq. Returned as the matched
// offsets (the data payloads are irrelevant to the ordering property).
func readByteOffsetOnly(msgs []Message, from Offset) []Offset {
	var out []Offset
	for _, m := range msgs {
		if m.Offset.ByteOffset > from.ByteOffset {
			out = append(out, m.Offset)
		}
	}
	return out
}

// readFullOffset is the faithful model of the Redis read range: select messages
// whose FULL offset sorts strictly after from. Compare orders by ReadSeq then
// ByteOffset, which equals the byte-lex order of Offset.String() that
// ZRANGEBYLEX uses on the LB-1 width-safe domain. This is the order
// readOwnMessages SHOULD use once ReadSeq can be non-zero.
func readFullOffset(msgs []Message, from Offset) []Offset {
	var out []Offset
	for _, m := range msgs {
		if Compare(m.Offset, from) > 0 {
			out = append(out, m.Offset)
		}
	}
	return out
}

func offsetsEqual(a, b []Offset) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

// lb3SafeField draws a uint64 in the LB-1 width-safe domain (< 10^16), so the
// readFullOffset model (numeric Compare) faithfully matches the Redis byte-lex
// order and the LB-3 ReadSeq divergence is isolated from the orthogonal LB-1
// width hazard (issue #27, which is out of scope here). It biases toward small
// values and toward ByteOffset/ReadSeq collisions so same-ByteOffset,
// different-ReadSeq pairs (the divergence trigger) are common.
func lb3SafeField() *rapid.Generator[uint64] {
	return rapid.OneOf(
		rapid.Uint64Range(0, 8),                      // tiny: forces ByteOffset/ReadSeq collisions
		rapid.Uint64Range(0, offsetWidthSafeBound-1), // anywhere in the width-safe domain
		rapid.SampledFrom([]uint64{0, 1, 2, 3, 1000, offsetWidthSafeBound - 1}),
	)
}

// msgsFromOffsets wraps offsets as Messages (payloads are irrelevant here).
func msgsFromOffsets(offs []Offset) []Message {
	msgs := make([]Message, len(offs))
	for i, o := range offs {
		msgs[i] = Message{Offset: o}
	}
	return msgs
}

// TestReadSeqAgreesWhileZero is the CI-green half of INV-FRAME-01 / INV-DIFF-03:
// while every ReadSeq is 0 (today's invariant), readOwnMessages's
// ByteOffset-only read window equals the full-Offset window Redis derives from
// read.lua's lex range. This asserts the currently-true "they agree because
// ReadSeq == 0" claim rather than assuming it. Pure (no Redis); runs on every
// build including -short.
func TestReadSeqAgreesWhileZero(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// All offsets pinned to ReadSeq == 0 (today's invariant). ByteOffsets in
		// the width-safe domain so lex order == numeric order.
		n := rapid.IntRange(0, 8).Draw(t, "n")
		offs := make([]Offset, n)
		for i := range offs {
			offs[i] = Offset{ReadSeq: 0, ByteOffset: lb3SafeField().Draw(t, "byteOffset")}
		}
		from := Offset{ReadSeq: 0, ByteOffset: lb3SafeField().Draw(t, "from")}
		msgs := msgsFromOffsets(offs)

		got := readByteOffsetOnly(msgs, from)
		want := readFullOffset(msgs, from)
		if !offsetsEqual(got, want) {
			t.Fatalf("ReadSeq==0 invariant broken: ByteOffset-only read %v != full-Offset read %v (from %v)",
				got, want, from)
		}
	})
}

// TestReadSeqDivergesWhenNonZero is the RED-ARMED LB-3 regression. It generates
// offset pairs that share a ByteOffset but differ in ReadSeq (and, more broadly,
// inputs that invert under ByteOffset-only vs full-Offset order), and asserts
// the divergence is REAL: readOwnMessages's ByteOffset-only selection differs
// from the full-Offset selection Redis uses. It is green TODAY because it
// asserts the divergence exists (the bug is latent only while ReadSeq stays 0);
// it documents that readOwnMessages MUST switch to a full-Offset comparison
// before log rotation makes ReadSeq non-zero.
//
// Construction guarantees a divergence: a message at {ReadSeq: 1, ByteOffset: B}
// read from {ReadSeq: 0, ByteOffset: B} is INCLUDED by the full-Offset order
// (1_B sorts after 0_B) but EXCLUDED by readOwnMessages (B is not > B). rapid
// shrinks any wider failure to this minimal same-ByteOffset, off-by-one-ReadSeq
// pair (committed in TestReadSeqDivergenceCounterexampleLB3).
//
//	// LB-3: armed divergence; the readOwnMessages fix lands with log rotation.
func TestReadSeqDivergesWhenNonZero(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// A from-offset and a message offset that share a ByteOffset but whose
		// ReadSeq differs, so full-Offset order and ByteOffset-only order split.
		bo := lb3SafeField().Draw(t, "sharedByteOffset")
		fromReadSeq := lb3SafeField().Draw(t, "fromReadSeq")
		// msgReadSeq strictly greater than fromReadSeq -> full-Offset includes,
		// ByteOffset-only excludes (equal ByteOffset is not strictly greater).
		delta := rapid.Uint64Range(1, 8).Draw(t, "readSeqDelta")
		if fromReadSeq > offsetWidthSafeBound-1-delta {
			t.Skip("would leave the width-safe domain")
		}
		msgReadSeq := fromReadSeq + delta

		from := Offset{ReadSeq: fromReadSeq, ByteOffset: bo}
		msg := Offset{ReadSeq: msgReadSeq, ByteOffset: bo}
		msgs := msgsFromOffsets([]Offset{msg})

		got := readByteOffsetOnly(msgs, from)
		want := readFullOffset(msgs, from)

		// Sanity: the full-Offset order (Redis truth) INCLUDES the message...
		if len(want) != 1 {
			t.Fatalf("model error: full-Offset read should include %v from %v, got %v", msg, from, want)
		}
		// ...while readOwnMessages's ByteOffset-only order EXCLUDES it. This
		// inequality IS the LB-3 divergence; asserting it keeps CI green while
		// pinning the bug. If this ever STOPS diverging, readOwnMessages has been
		// fixed to compare the full Offset (the rotation-feature fix) and this
		// armed test must be flipped to assert agreement.
		if offsetsEqual(got, want) {
			t.Fatalf("LB-3 divergence vanished: ByteOffset-only read %v == full-Offset read %v for "+
				"msg=%v from=%v — has readOwnMessages been changed to compare the full Offset? "+
				"un-skip/flip this armed test (the log-rotation fix has landed)", got, want, msg, from)
		}
	})
}

// TestReadSeqDivergenceCounterexampleLB3 is the committed, minimal LB-3
// regression fixture: the boundary counterexample rapid shrinks to in
// TestReadSeqDivergesWhenNonZero. Two messages share ByteOffset 0 but differ in
// ReadSeq; read from {ReadSeq: 0, ByteOffset: 0}:
//
//   - full-Offset order (what Redis ZRANGEBYLEX over Offset.String() returns)
//     includes {ReadSeq: 1, ByteOffset: 0} (offset "...1_...0" sorts after the
//     exclusive bound "...0_...0");
//   - readOwnMessages's ByteOffset-only predicate (0 > 0 is false) EXCLUDES it.
//
// So the two backends would return different messages the instant ReadSeq is
// non-zero. This fixture documents the bug in machine-checked form and keeps CI
// green; the readOwnMessages fix lands with the log-rotation feature, inheriting
// this test. [INV-FRAME-01 / INV-DIFF-03 / LB-3]
//
//	// LB-3: committed boundary counterexample; do not "fix" by editing this
//	// fixture — fix readOwnMessages when log rotation ships, then flip this.
func TestReadSeqDivergenceCounterexampleLB3(t *testing.T) {
	from := Offset{ReadSeq: 0, ByteOffset: 0}
	// Minimal shrunk pair: same ByteOffset, ReadSeq off by one.
	msgs := msgsFromOffsets([]Offset{
		{ReadSeq: 1, ByteOffset: 0}, // higher ReadSeq, same ByteOffset
	})

	byteOnly := readByteOffsetOnly(msgs, from)
	full := readFullOffset(msgs, from)

	// Redis truth (full Offset): the message IS in the read window.
	if len(full) != 1 || !full[0].Equal(Offset{ReadSeq: 1, ByteOffset: 0}) {
		t.Fatalf("full-Offset read should include {1,0}, got %v", full)
	}
	// MemoryStore (ByteOffset-only): the message is WRONGLY excluded.
	if len(byteOnly) != 0 {
		t.Fatalf("expected the LB-3 divergence — readOwnMessages excludes {1,0} when read from {0,0} "+
			"(0 > 0 is false) — but got %v; has readOwnMessages been fixed to compare the full Offset? "+
			"flip this fixture intentionally with the log-rotation feature", byteOnly)
	}

	// And the comparison key parity that root-causes it: full Compare orders the
	// pair (ReadSeq dominates), ByteOffset-only sees them as equal.
	a := Offset{ReadSeq: 0, ByteOffset: 0}
	b := Offset{ReadSeq: 1, ByteOffset: 0}
	if Compare(a, b) != -1 {
		t.Fatalf("full Compare should order {0,0} < {1,0}, got %d", Compare(a, b))
	}
	if a.ByteOffset != b.ByteOffset {
		t.Fatalf("the divergence requires a shared ByteOffset; fixture drifted: %v vs %v", a, b)
	}
}
