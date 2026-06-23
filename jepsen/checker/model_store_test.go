package main

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

// These tests exercise the pure data-plane model (model_store.go) directly
// against crafted histories — no Redis. They are the proof that the oracle
// accepts every legal interleaving of append/read/close/getOffset and rejects
// the torn-write / gap / duplicate / reorder / closed-violation / dirty-EOF
// histories it exists to catch. With no timeout, CheckOperations is definitive:
// true == linearizable (Ok), false == a violation (Illegal). The model is the
// independent oracle; this file is the oracle's OWN spec, so it is trustworthy
// before it ever touches live Redis (scenario_store.go binds it to the SUT).

// ---- terse history-operation builders ----

func sop(client int, in storeInput, out storeOutput, call, ret int64) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret}
}

func tag(client, seq int, n uint64) frameTag {
	return frameTag{clientID: client, opSeq: seq, nbytes: n}
}

func appendIn(path string, t frameTag) storeInput {
	return storeInput{path: path, op: opAppend, tag: t, nbytes: t.nbytes}
}

func appendCloseIn(path string, t frameTag) storeInput {
	return storeInput{path: path, op: opAppend, tag: t, nbytes: t.nbytes, closeWith: true}
}

func indetAppendIn(path string, t frameTag) storeInput {
	return storeInput{path: path, op: opAppend, tag: t, nbytes: t.nbytes, expectIndet: true}
}

// closeIn / getOffIn / readIn are shared with the live driver (scenario_store.go),
// so they live in scenario_store_helpers.go (a non-test file). Only the
// append-shaped builders above stay test-local.

func okAppendOut(off uint64) storeOutput  { return storeOutput{status: stoOK, offset: off} }
func closedErrOut(off uint64) storeOutput { return storeOutput{status: stoClosedErr, offset: off} }
func closedDupOut(off uint64) storeOutput { return storeOutput{status: stoClosedDup, offset: off} }
func indetOut() storeOutput               { return storeOutput{status: stoIndet} }
func freshCloseOut(off uint64) storeOutput {
	return storeOutput{status: stoClosedFresh, offset: off}
}
func alreadyCloseOut(off uint64) storeOutput {
	return storeOutput{status: stoClosedAlrdy, offset: off}
}
func getOffOut(off uint64) storeOutput { return storeOutput{status: stoOK, offset: off} }

func readOut(frames []frameTag, upToDate, closed bool) storeOutput {
	return storeOutput{status: stoOK, readFrames: frames, upToDate: upToDate, readClosed: closed}
}

func checkStore(t *testing.T, name string, h []porcupine.Operation, want bool) {
	t.Helper()
	got := porcupine.CheckOperations(streamModel(), h)
	if got != want {
		t.Fatalf("%s: CheckOperations = %v, want %v", name, got, want)
	}
}

// ---- INV-LIN-01: single-slot append linearization point ----

// A simple sequential append run: tail advances by exactly len(data) each time,
// and getOffset reports the running tail. This is the baseline the whole model
// rests on.
func TestStoreModel_SequentialAppends(t *testing.T) {
	const p = "/s"
	h := []porcupine.Operation{
		sop(0, appendIn(p, tag(0, 0, 5)), okAppendOut(5), 1, 2),
		sop(0, appendIn(p, tag(0, 1, 3)), okAppendOut(8), 3, 4),
		sop(0, getOffIn(p), getOffOut(8), 5, 6),
		sop(0, appendIn(p, tag(0, 2, 4)), okAppendOut(12), 7, 8),
	}
	checkStore(t, "sequential appends", h, true)
}

// Two concurrent appends: either order linearizes, as long as the two returned
// offsets are {first len, first+second len} consistently. Here both clients see
// a self-consistent total order (5 then 8), so it is linearizable.
func TestStoreModel_ConcurrentAppendsLinearize(t *testing.T) {
	const p = "/s"
	h := []porcupine.Operation{
		sop(0, appendIn(p, tag(0, 0, 5)), okAppendOut(5), 1, 4),
		sop(1, appendIn(p, tag(1, 0, 3)), okAppendOut(8), 2, 5),
	}
	checkStore(t, "concurrent appends linearize", h, true)
}

// A byte-offset GAP: two appends of 5 and 3 bytes, but the second returns offset
// 9 (a 1-byte gap). No linearization fits — tail can only be 5+3=8. This is the
// INV-LIN-02 no-gap assertion.
func TestStoreModel_OffsetGapRejected(t *testing.T) {
	const p = "/s"
	h := []porcupine.Operation{
		sop(0, appendIn(p, tag(0, 0, 5)), okAppendOut(5), 1, 2),
		sop(0, appendIn(p, tag(0, 1, 3)), okAppendOut(9), 3, 4), // gap: should be 8
	}
	checkStore(t, "offset gap rejected", h, false)
}

// A TORN / wrong-length write: a 5-byte append that reports tail 4. The offset
// must advance by exactly len(data); 4 != 0+5, so no linearization fits.
func TestStoreModel_TornWriteRejected(t *testing.T) {
	const p = "/s"
	h := []porcupine.Operation{
		sop(0, appendIn(p, tag(0, 0, 5)), okAppendOut(4), 1, 2),
	}
	checkStore(t, "torn write rejected", h, false)
}

// ---- INV-READ-01: exact offset-exclusive suffix with frame identity ----

// Read(0) after two appends returns BOTH frames, in order, upToDate. Read(5)
// returns only the second frame (offset-exclusive). This is the exact-suffix
// contract.
func TestStoreModel_ReadExactSuffix(t *testing.T) {
	const p = "/s"
	f0, f1 := tag(0, 0, 5), tag(0, 1, 3)
	h := []porcupine.Operation{
		sop(0, appendIn(p, f0), okAppendOut(5), 1, 2),
		sop(0, appendIn(p, f1), okAppendOut(8), 3, 4),
		sop(0, readIn(p, 0), readOut([]frameTag{f0, f1}, true, false), 5, 6),
		sop(0, readIn(p, 5), readOut([]frameTag{f1}, true, false), 7, 8),
		sop(0, readIn(p, 8), readOut(nil, true, false), 9, 10), // at tail: empty, upToDate
	}
	checkStore(t, "read exact suffix", h, true)
}

// A read that REORDERS the two frames (returns f1 then f0) has no linearization —
// the (clientId,opSeq) tags pin order; a byte-length-only check would have missed
// this since both length sets match.
func TestStoreModel_ReadReorderRejected(t *testing.T) {
	const p = "/s"
	f0, f1 := tag(0, 0, 5), tag(0, 1, 3)
	h := []porcupine.Operation{
		sop(0, appendIn(p, f0), okAppendOut(5), 1, 2),
		sop(0, appendIn(p, f1), okAppendOut(8), 3, 4),
		sop(0, readIn(p, 0), readOut([]frameTag{f1, f0}, true, false), 5, 6), // reordered
	}
	checkStore(t, "read reorder rejected", h, false)
}

// A read that DUPLICATES a frame (returns f0 twice) is rejected: the suffix is
// exact, never a duplicate. INV-LIN-02 no-duplicate, surfaced at read time.
func TestStoreModel_ReadDuplicateRejected(t *testing.T) {
	const p = "/s"
	f0 := tag(0, 0, 5)
	h := []porcupine.Operation{
		sop(0, appendIn(p, f0), okAppendOut(5), 1, 2),
		sop(0, readIn(p, 0), readOut([]frameTag{f0, f0}, true, false), 3, 4),
	}
	checkStore(t, "read duplicate rejected", h, false)
}

// A read that returns a STALE frame the producer never committed (a different
// (clientId,opSeq) than any append) is rejected.
func TestStoreModel_ReadPhantomFrameRejected(t *testing.T) {
	const p = "/s"
	f0 := tag(0, 0, 5)
	phantom := tag(9, 9, 5) // same size, different identity
	h := []porcupine.Operation{
		sop(0, appendIn(p, f0), okAppendOut(5), 1, 2),
		sop(0, readIn(p, 0), readOut([]frameTag{phantom}, true, false), 3, 4),
	}
	checkStore(t, "read phantom frame rejected", h, false)
}

// ---- INV-CLOSE-01: idempotent close ----

// Close, then close again: the first is a fresh latch flip, the second an
// idempotent already-closed no-op. Both report the same final offset.
func TestStoreModel_IdempotentClose(t *testing.T) {
	const p = "/s"
	h := []porcupine.Operation{
		sop(0, appendIn(p, tag(0, 0, 5)), okAppendOut(5), 1, 2),
		sop(0, closeIn(p), freshCloseOut(5), 3, 4),
		sop(0, closeIn(p), alreadyCloseOut(5), 5, 6), // idempotent
		sop(1, closeIn(p), alreadyCloseOut(5), 7, 8), // a third close, still no-op
	}
	checkStore(t, "idempotent close", h, true)
}

// A close that reports already-closed BEFORE any close happened has no
// linearization (the stream starts open).
func TestStoreModel_AlreadyClosedWithoutCloseRejected(t *testing.T) {
	const p = "/s"
	h := []porcupine.Operation{
		sop(0, closeIn(p), alreadyCloseOut(0), 1, 2),
	}
	checkStore(t, "already-closed without close rejected", h, false)
}

// Append-after-close ⇒ ErrStreamClosed with NO state change: a later read sees
// only the pre-close frame, and getOffset is unmoved.
func TestStoreModel_AppendAfterCloseRejected(t *testing.T) {
	const p = "/s"
	f0 := tag(0, 0, 5)
	h := []porcupine.Operation{
		sop(0, appendIn(p, f0), okAppendOut(5), 1, 2),
		sop(0, closeIn(p), freshCloseOut(5), 3, 4),
		sop(1, appendIn(p, tag(1, 0, 7)), closedErrOut(5), 5, 6), // rejected
		sop(0, readIn(p, 0), readOut([]frameTag{f0}, true, true), 7, 8),
		sop(0, getOffIn(p), getOffOut(5), 9, 10),
	}
	checkStore(t, "append after close rejected", h, true)
}

// A committed append onto a closed stream (status OK after a close) is the
// data-loss-risk violation: it must have NO valid linearization.
func TestStoreModel_CommittedAppendAfterCloseRejected(t *testing.T) {
	const p = "/s"
	h := []porcupine.Operation{
		sop(0, closeIn(p), freshCloseOut(0), 1, 2),
		sop(1, appendIn(p, tag(1, 0, 7)), okAppendOut(7), 3, 4), // committed AFTER close
	}
	checkStore(t, "committed append after close rejected", h, false)
}

// The matching-producer-tuple append-after-close is an idempotent DUP (no new
// frame), per the ClosedBy dedup: a CLOSED_DUP reply on a closed stream is legal.
func TestStoreModel_ClosedTupleDupAccepted(t *testing.T) {
	const p = "/s"
	f0 := tag(0, 0, 5)
	h := []porcupine.Operation{
		sop(0, appendCloseIn(p, f0), okAppendOut(5), 1, 2), // append+close in one op
		sop(0, appendIn(p, tag(0, 1, 0)), closedDupOut(5), 3, 4),
		sop(0, readIn(p, 0), readOut([]frameTag{f0}, true, true), 5, 6),
	}
	checkStore(t, "closed-tuple dup accepted", h, true)
}

// ---- INV-CLOSE / EOF: clean read-past-close ----

// Read at the final offset of a closed stream returns ZERO frames + the closed
// signal — the SSE final-streamClosed / long-poll 204 contract. A read BEFORE
// the tail still returns the remaining frame and is NOT closed (more to drain).
func TestStoreModel_CleanReadPastCloseEOF(t *testing.T) {
	const p = "/s"
	f0, f1 := tag(0, 0, 5), tag(0, 1, 3)
	h := []porcupine.Operation{
		sop(0, appendIn(p, f0), okAppendOut(5), 1, 2),
		sop(0, appendCloseIn(p, f1), okAppendOut(8), 3, 4), // final append delivered WITH close
		// A reader caught up at byte 5 still gets the final frame as DATA, and it is
		// NOT yet EOF for that read because it is not at the tail until it consumes f1.
		sop(1, readIn(p, 5), readOut([]frameTag{f1}, true, true), 5, 6),
		// A reader exactly at the tail of the closed stream: zero frames + closed.
		sop(1, readIn(p, 8), readOut(nil, true, true), 7, 8),
	}
	checkStore(t, "clean read-past-close EOF", h, true)
}

// A read that signals closed/EOF while the stream is still OPEN is a dirty-EOF
// violation (the reader would stop early and miss future appends).
func TestStoreModel_DirtyEOFOnOpenStreamRejected(t *testing.T) {
	const p = "/s"
	f0 := tag(0, 0, 5)
	h := []porcupine.Operation{
		sop(0, appendIn(p, f0), okAppendOut(5), 1, 2),
		sop(0, readIn(p, 5), readOut(nil, true, true), 3, 4), // closed=true but never closed
	}
	checkStore(t, "dirty EOF on open stream rejected", h, false)
}

// A read that signals closed/EOF while frames remain unread (not at tail) is the
// handler_sse.go "drop the final append" race: the closing control event must
// come AFTER the final data, never strand it. Modeled: readClosed=true with a
// non-empty-but-not-at-tail read has no linearization.
func TestStoreModel_EOFBeforeDrainRejected(t *testing.T) {
	const p = "/s"
	f0, f1 := tag(0, 0, 5), tag(0, 1, 3)
	h := []porcupine.Operation{
		sop(0, appendIn(p, f0), okAppendOut(5), 1, 2),
		sop(0, appendCloseIn(p, f1), okAppendOut(8), 3, 4),
		// A reader at byte 0 returns BOTH frames; it IS at tail, so closed is fine.
		sop(1, readIn(p, 0), readOut([]frameTag{f0, f1}, true, true), 5, 6),
		// But a reader at byte 0 that returns ONLY f0 yet signals closed is the
		// dropped-final-append race: not at tail, so EOF is illegal here.
		sop(2, readIn(p, 0), readOut([]frameTag{f0}, false, true), 7, 8),
	}
	checkStore(t, "EOF before drain rejected", h, false)
}

// ---- INV-LIN-02: indeterminate (timed-out) append linearizes both ways ----

// An indeterminate append that LATER turns out to have committed (a subsequent
// read sees its frame) is linearizable: the indeterminate op linearizes as
// committed.
func TestStoreModel_IndeterminateAppendCommitted(t *testing.T) {
	const p = "/s"
	f0 := tag(0, 0, 5)
	h := []porcupine.Operation{
		sop(0, indetAppendIn(p, f0), indetOut(), 1, 4),
		sop(1, readIn(p, 0), readOut([]frameTag{f0}, true, false), 5, 6), // it DID commit
	}
	checkStore(t, "indeterminate append committed", h, true)
}

// The SAME indeterminate append, but a subsequent read shows it did NOT commit
// (empty stream). Also linearizable: the indeterminate op linearizes as
// not-committed. This is the both-ways property: one model accepts both observed
// outcomes.
func TestStoreModel_IndeterminateAppendNotCommitted(t *testing.T) {
	const p = "/s"
	f0 := tag(0, 0, 5)
	h := []porcupine.Operation{
		sop(0, indetAppendIn(p, f0), indetOut(), 1, 4),
		sop(1, readIn(p, 0), readOut(nil, true, false), 5, 6), // it did NOT commit
		sop(1, getOffIn(p), getOffOut(0), 7, 8),
	}
	checkStore(t, "indeterminate append not committed", h, true)
}

// An indeterminate append that, IF committed, would have to produce a DUPLICATE
// frame (a confirmed identical-tag frame already committed) — modeling the
// retry-double-commit hazard. The duplicate is caught at read time: a read that
// returns the tag twice has no linearization regardless of how the indeterminate
// op resolves.
func TestStoreModel_IndeterminateNoDoubleCommit(t *testing.T) {
	const p = "/s"
	f0 := tag(0, 0, 5)
	h := []porcupine.Operation{
		sop(0, appendIn(p, f0), okAppendOut(5), 1, 2),                        // f0 committed
		sop(0, indetAppendIn(p, f0), indetOut(), 3, 6),                       // a retry of the SAME frame
		sop(1, readIn(p, 0), readOut([]frameTag{f0, f0}, true, false), 7, 8), // observed twice = torn retry
	}
	checkStore(t, "indeterminate no double commit", h, false)
}

// ---- Herlihy-Wing locality: two paths are checked independently ----

// Interleaving two independent streams must linearize per path; a violation on
// one path is caught even when the other path's ops are interleaved through it.
func TestStoreModel_PerPathLocality(t *testing.T) {
	const a, b = "/a", "/b"
	fa, fb := tag(0, 0, 4), tag(1, 0, 6)
	good := []porcupine.Operation{
		sop(0, appendIn(a, fa), okAppendOut(4), 1, 2),
		sop(1, appendIn(b, fb), okAppendOut(6), 3, 4),
		sop(0, readIn(a, 0), readOut([]frameTag{fa}, true, false), 5, 6),
		sop(1, readIn(b, 0), readOut([]frameTag{fb}, true, false), 7, 8),
	}
	checkStore(t, "per-path locality OK", good, true)

	bad := []porcupine.Operation{
		sop(0, appendIn(a, fa), okAppendOut(4), 1, 2),
		sop(1, appendIn(b, fb), okAppendOut(6), 3, 4),
		sop(1, readIn(b, 0), readOut([]frameTag{fa}, true, false), 5, 6), // /b read returns /a's frame
	}
	checkStore(t, "per-path locality violation caught", bad, false)
}
