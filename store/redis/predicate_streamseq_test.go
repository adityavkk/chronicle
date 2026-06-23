package redis

import (
	"errors"
	"fmt"
	"testing"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// predicate_streamseq_test.go is the LB-2 Stream-Seq digit-width differential
// (issue #33 addendum, INV-DIFF-03). Stream-Seq is a caller-supplied opaque string
// compared BYTEWISE with `<=` on BOTH sides — Go (memory_store.go: a second append
// is rejected iff `opts.Seq <= stream.metadata.LastSeq`) and live Lua
// (append.lua: `stream_seq <= m.lastSeq` -> SEQCONFLICT). A naive client using
// UNPADDED decimal counters breaks at the digit-width boundary ("10" <= "9"
// lexically, so a valid advance "9" -> "10" is wrongly REJECTED) — the same class
// as the %016d offset bug (LB-1).
//
// The property generates Stream-Seq pairs (last, next) — including 9/10, 09/10, and
// leading-zero cases — and drives the REAL append.lua (via Store.Append) against
// the MemoryStore oracle (the real memory_store.go `<=`), asserting both backends
// agree on whether the second append conflicts, for every pair. It is the
// Stream-Seq sibling of the equivalence harness, kept focused so the digit-width
// boundary is the explicit subject.
//
// Skipped under -short / unreachable Redis (newTestStore). On a divergence rapid
// shrinks to a minimal (last, next) pair and prints the replay seed.

// streamSeqBoundaryPairs are the committed seeds the generator always covers: the
// digit-width crossings (9/10), zero-padded equivalents (09/10), leading-zero
// pairs, equal pairs, and a fixed-width pair that is lex-safe.
var streamSeqBoundaryPairs = [][2]string{
	{"9", "10"},      // ADVERSARIAL: numerically advances but lexically "10" < "9" -> wrongly rejected by a naive client
	{"09", "10"},     // zero-padded to equal width: lex-safe, advances ("09" < "10")
	{"9", "9"},       // equal -> conflict (`<=` is true)
	{"09", "9"},      // leading zero vs bare: "09" < "9" lexically (advance), numerically equal
	{"1", "2"},       // trivial advance, single digit
	{"2", "1"},       // trivial regression -> conflict
	{"099", "100"},   // padded width crossing: advances
	{"99", "100"},    // unpadded width crossing: "100" < "99" lexically -> rejected
	{"0001", "0002"}, // fixed-width zero-padded: lex-safe advance
	{"0002", "0001"}, // fixed-width regression -> conflict
	{"", "1"},        // empty last is the no-prior-seq case (never conflicts)
	{"abc", "abd"},   // non-numeric opaque strings still compared bytewise
}

// genStreamSeq draws a Stream-Seq value biased to the digit-width boundary: bare
// decimals across widths (the footgun), zero-padded values, and leading-zero forms.
func genStreamSeq() *rapid.Generator[string] {
	return rapid.OneOf(
		rapid.SampledFrom([]string{"9", "10", "09", "099", "100", "0", "00", "1", "01"}),
		rapid.Custom(func(t *rapid.T) string {
			// Bare unpadded decimal across digit widths (the adversarial region).
			return fmt.Sprintf("%d", rapid.IntRange(0, 1500).Draw(t, "bare"))
		}),
		rapid.Custom(func(t *rapid.T) string {
			// Fixed-width zero-padded (the lex-safe shape the precondition recommends).
			return fmt.Sprintf("%04d", rapid.IntRange(0, 1500).Draw(t, "padded"))
		}),
	)
}

// TestStreamSeqLexMirror asserts that for every generated (last, next) Stream-Seq
// pair the Go `<=` (memory_store.go) and the live-Lua `<=` (append.lua) agree on
// whether the second append is a sequence conflict — including the 9/10 digit-width
// boundary and leading-zero cases. Drives the SHIPPED append.lua against live
// Redis, with MemoryStore as the oracle. [INV-DIFF-03]
func TestStreamSeqLexMirror(t *testing.T) {
	base := newTestStore(t) // skips under -short / unreachable Redis

	rapid.Check(t, func(rt *rapid.T) {
		var last, next string
		if rapid.Bool().Draw(rt, "useBoundary") {
			p := rapid.SampledFrom(streamSeqBoundaryPairs).Draw(rt, "pair")
			last, next = p[0], p[1]
		} else {
			last = genStreamSeq().Draw(rt, "last")
			next = genStreamSeq().Draw(rt, "next")
		}

		// Fresh, globally-unique path on both backends; MemoryStore is the oracle.
		oracle := store.NewMemoryStore()
		subject := New(base.client, Options{})
		path := testPath("streamseq")

		mkStream := func() {
			if _, _, err := oracle.Create(path, store.CreateOptions{}); err != nil {
				rt.Fatalf("oracle Create: %v", err)
			}
			if _, _, err := subject.Create(path, store.CreateOptions{}); err != nil {
				rt.Fatalf("subject Create: %v", err)
			}
		}
		mkStream()

		// First append establishes lastSeq (skipped when last == "", the no-prior
		// case). Both must accept it identically.
		if last != "" {
			oErr := appendSeq(oracle, path, last)
			sErr := appendSeq(subject, path, last)
			if (oErr == nil) != (sErr == nil) {
				rt.Fatalf("seed append(last=%q) presence mismatch: oracle=%v subject=%v", last, oErr, sErr)
			}
			if oErr != nil {
				// A seed that itself conflicts (last <= "" can't happen; lastSeq was
				// empty) is not expected, but stay robust: skip the pair.
				rt.Skip("seed append unexpectedly conflicted")
			}
		}

		// Second append with `next`: the differential subject. Both backends compare
		// `next <= lastSeq` bytewise and must agree on the conflict verdict.
		oConflict := errors.Is(appendSeq(oracle, path, next), store.ErrSequenceConflict)
		sConflict := errors.Is(appendSeq(subject, path, next), store.ErrSequenceConflict)
		if oConflict != sConflict {
			rt.Fatalf("Stream-Seq `<=` divergence on (last=%q, next=%q): Go conflict=%v live-Lua conflict=%v",
				last, next, oConflict, sConflict)
		}

		// Pin the bytewise semantics independently so a both-sides-wrong drift is
		// caught: a conflict occurs IFF there is a prior seq and next <= it bytewise.
		wantConflict := last != "" && next <= last
		if oConflict != wantConflict {
			rt.Fatalf("Stream-Seq semantics wrong on (last=%q, next=%q): conflict=%v want=%v",
				last, next, oConflict, wantConflict)
		}
	})
}

// appendSeq appends a 1-byte payload with the given Stream-Seq and returns the
// error (nil on accept, ErrSequenceConflict on a lex regression). store is the
// minimal interface both backends satisfy.
func appendSeq(s interface {
	Append(path string, data []byte, opts store.AppendOptions) (store.AppendResult, error)
}, path, seq string,
) error {
	_, err := s.Append(path, []byte("x"), store.AppendOptions{Seq: seq})
	return err
}
