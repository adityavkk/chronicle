package main

import (
	"fmt"
	"strings"

	"github.com/anishathalye/porcupine"
)

// model_store.go is the PURE CORE of the DATA-PLANE linearizability test (the
// Porcupine arm of issue #35): a sequential model of one stream's append log —
// the (frames, closed, tail) register that store/redis/store.go mutates as a
// single per-stream-atomic redis.Script over the {escapePath(path)} hash-tag
// slot (store/redis/keys.go). Like model_fence.go / model_shard.go it has no I/O,
// no clock, and no dependency on the package under test — it is an independent
// oracle, deterministic and unit-testable in isolation (model_store_test.go).
// The imperative shell that drives a live Redis store.Store and feeds histories
// into it lives in scenario_store.go.
//
// PROPERTIES DISCHARGED (docs/specs/formal-verification/INVARIANTS.md):
//   - INV-LIN-01  Per-stream mutation atomicity = single-slot linearization
//     point: for a fixed path the accepted-mutation sequence is totally ordered
//     and each validate+write+tail-update is indivisible. The single EVAL over
//     the path slot is the linearization point this check verifies from the
//     OUTSIDE, under real concurrency — what was prose-only before.
//   - INV-LIN-02  Optimistic re-frame loop preserves linearizability:
//     append.lua's RETRY-on-tail-moved (stRetry) + the bounded Go re-frame loop
//     (maxAppendRetries) must never yield a torn write, a byte-offset gap, or a
//     duplicated frame. Modeled by treating a TIMED-OUT / inconclusive append as
//     INDETERMINATE — the NondeterministicModel linearizes it as committed OR
//     not-committed — so a retry that silently double-committed (a duplicate
//     frame) or left a gap has NO valid linearization and Porcupine surfaces it.
//   - INV-CLOSE-01  Close idempotency / ClosedBy producer-tuple dedup: a repeat
//     close, or a matching-(producer)-tuple append-after-close, is an idempotent
//     no-op; a different tuple ⇒ ErrStreamClosed. Modeled by stepClose (idempotent)
//     and stepAppend's closed-branch (the matching-tuple dup is OK, otherwise
//     rejected with no state change).
//   - INV-READ-01  Read returns exactly the offset-EXCLUSIVE contiguous suffix
//     in linearized append order, and upToDate ⟺ last == tail. Frames are tagged
//     with a unique (clientId, opSeq) — the Elle recoverability idea, the data
//     plane NOT the transactional frame — so the READ step compares exact frame
//     IDENTITY, not just byte length: a reorder, a gap, or a stale/duplicated
//     frame all fail the suffix check.
//   - INV-CLOSE / EOF (handler_sse.go / long-poll): a READ at/after the final
//     offset of a CLOSED stream returns ZERO frames + the closed (end-of-stream)
//     signal — modeling the SSE "final streamClosed control AFTER the final data
//     event" contract. A final append delivered with a close still linearizes as
//     a data frame strictly before the EOF signal because the append's frame is
//     part of the committed suffix any earlier read can observe, while the close
//     only flips the EOF bit a read AT the tail reports.
//   - INV-JEP-REC-01 is inherited by the scenario reusing history.go's
//     host-monotonic recordOp; the model itself is time-free.
//
// Time is deliberately absent (as in model_fence.go / model_shard.go). TTL
// expiry, the sliding-window LastAccessedAt, and lease clocks never change a
// step's verdict here; the model verifies only the time-free log algebra
// (append advances tail by len(data); read returns the exact suffix; close is a
// monotone latch). Durability across async Redis replication is explicitly NOT
// modeled as a checked claim — a lost write appears ONLY as the indeterminate
// append linearizing as not-committed, never as a durability assertion (that is
// the failover chaos rig's job; docs/specs/formal-verification/DESIGN.md §4).

// Data-plane operation statuses, mirroring store.Store's (AppendResult,
// CloseResult, Read) returns and the store sentinels (store/store.go). They are
// redeclared here (not imported) so the harness stays an independent oracle, the
// same way model_fence.go redeclares the webhook phases.
const (
	stoOK          = "OK"            // append committed / read served / getoffset
	stoClosedErr   = "CLOSED"        // append rejected: ErrStreamClosed, no state change
	stoClosedDup   = "CLOSED_DUP"    // append-after-close, matching producer tuple: idempotent no-op
	stoIndet       = "INDETERMINATE" // append timed out / retried: committed-or-not
	stoClosedFresh = "CLOSED_FRESH"  // close: this op flipped the latch
	stoClosedAlrdy = "CLOSED_ALRDY"  // close: already closed (idempotent)
)

// storeOpKind is the operation recorded into a data-plane history.
type storeOpKind int

const (
	opAppend    storeOpKind = iota // Append(path,data,opts) -> AppendResult|ErrStreamClosed
	opClose                        // CloseStream(path)       -> CloseResult (idempotent)
	opRead                         // Read(path,offset)       -> ([]Message, upToDate)
	opGetOffset                    // GetCurrentOffset(path)  -> tail
)

// frameTag is one appended frame's identity: a unique (clientID, opSeq) producer
// coordinate (the Elle-recoverability tag) plus its byte length. nbytes is what
// advances the tail; (clientID, opSeq) is what makes the read step exact —
// length-only comparison would not catch a frame substituted for another of the
// same size, a torn write re-stitched to the right length, or a duplicate.
type frameTag struct {
	clientID int
	opSeq    int
	nbytes   uint64
}

func (f frameTag) String() string {
	return fmt.Sprintf("c%d#%d:%dB", f.clientID, f.opSeq, f.nbytes)
}

// storeState is the sequential model state for one path: the committed frame log
// in linearized append order, the closed latch, and the byte tail. tail is the
// running sum of every committed frame's nbytes (ZeroOffset.ByteOffset == 0), so
// tail is redundant with frames but kept explicitly to mirror the metadata
// CurrentOffset the store actually reports and to make GETOFFSET / read-past-close
// a direct comparison. The fork ReadSeq dimension is out of scope here (single,
// non-forked stream), so only ByteOffset moves — Offset.Add only advances bytes.
type storeState struct {
	frames []frameTag
	closed bool
	tail   uint64
}

// clone returns a deep copy so a Step never mutates its argument (porcupine
// requires pure steps). Only frames needs copying; closed/tail are values.
func (s storeState) clone() storeState {
	cp := make([]frameTag, len(s.frames))
	copy(cp, s.frames)
	return storeState{frames: cp, closed: s.closed, tail: s.tail}
}

// storeInput is the model input: which path, which op, and the op's parameters.
//   - opAppend: tag (the frame identity), nbytes, closeWith (Stream-Closed: true),
//     and prodTuple (the producer (id,epoch,seq) that, on append-after-close,
//     decides the matching-tuple idempotent-dup exception). expectIndet flags a
//     timed-out / inconclusive append.
//   - opClose:  no extra params (close-only; producer dedup is exercised via the
//     append-after-close path, matching store.CloseStream's plain idempotency).
//   - opRead:   readFrom (the exclusive lower-bound byte offset).
//   - opGetOffset: none.
type storeInput struct {
	path        string
	op          storeOpKind
	tag         frameTag
	nbytes      uint64
	closeWith   bool
	prodTuple   string // "" = no producer headers
	expectIndet bool
	readFrom    uint64
}

// storeOutput is the model output: the observed store reply.
//   - opAppend:    status (stoOK|stoClosedErr|stoClosedDup|stoIndet) + offset (the
//     returned tail on OK).
//   - opClose:     status (stoClosedFresh|stoClosedAlrdy) + offset (FinalOffset).
//   - opRead:      readFrames (the exact tagged suffix returned, in order) +
//     upToDate + readClosed (the clean-EOF / Stream-Closed signal).
//   - opGetOffset: offset (the reported tail).
type storeOutput struct {
	status     string
	offset     uint64
	readFrames []frameTag
	upToDate   bool
	readClosed bool
}

// streamModel is the porcupine NondeterministicModel for the data plane,
// partitioned per path (Herlihy–Wing locality: a history is linearizable iff each
// path's sub-history is, so per-stream linearizability composes). It is
// nondeterministic ONLY to admit the indeterminate (timed-out) append both ways;
// every other op is deterministic.
func streamModel() porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Partition: partitionByPath,
		Init: func() []interface{} {
			// A fresh stream: empty log, open, tail at ZeroOffset (byte 0). The
			// scenario creates the stream before any op, so "not found" is never a
			// modeled outcome here.
			return []interface{}{storeState{frames: nil, closed: false, tail: 0}}
		},
		Step:              streamStep,
		Equal:             storeStatesEqual,
		DescribeOperation: describeStoreOp,
		DescribeState:     describeStoreState,
	}
	return nm.ToModel()
}

// streamStep is the heart of the model: given a single concrete pre-state and an
// observed (input, output), it returns the SET of next states consistent with
// the data-plane spec — empty if none (an illegal step). It is a pure function:
// it never mutates its arguments. The set has >1 element only for an
// indeterminate append.
func streamStep(state, input, output interface{}) []interface{} {
	s := state.(storeState)
	in := input.(storeInput)
	out := output.(storeOutput)

	switch in.op {
	case opAppend:
		return stepAppend(s, in, out)
	case opClose:
		return stepClose(s, out)
	case opRead:
		return stepRead(s, in, out)
	case opGetOffset:
		return stepGetOffset(s, out)
	default:
		return nil
	}
}

// stepAppend models store.Append. INV-LIN-01 / INV-LIN-02 / INV-CLOSE-01.
func stepAppend(s storeState, in storeInput, out storeOutput) []interface{} {
	// committed is the post-state if THIS append took effect: the tagged frame is
	// appended to the linearized order and the tail advances by exactly nbytes
	// (Offset.Add). If the append also closed, the latch flips. It returns ok=false
	// when the frame's (clientId, opSeq) tag is ALREADY in the log — committing a
	// duplicate tag is the INV-LIN-02 no-duplicate violation (a retry that
	// double-committed), so it is never an admissible next state.
	committed := func() (storeState, bool) {
		if hasTag(s.frames, in.tag) {
			return storeState{}, false
		}
		ns := s.clone()
		ns.frames = append(ns.frames, in.tag)
		ns.tail = s.tail + in.nbytes
		if in.closeWith {
			ns.closed = true
		}
		return ns, true
	}

	switch out.status {
	case stoOK:
		// A committed append is legal ONLY on an open stream, and the returned
		// offset MUST equal the post-append tail exactly: tail advances by
		// len(data) and not one byte more or fewer (the no-torn-write, no-gap
		// assertion). A frame committed onto a closed stream, a returned offset
		// that disagrees with s.tail+nbytes, or a duplicate (clientId,opSeq) tag
		// has no valid linearization.
		if s.closed {
			return nil
		}
		if out.offset != s.tail+in.nbytes {
			return nil
		}
		ns, ok := committed()
		if !ok {
			return nil
		}
		return []interface{}{ns}

	case stoClosedErr:
		// ErrStreamClosed: rejected, NO state change. Legal only when the stream
		// is already closed in this linearization. The store reports the current
		// (closed) tail; we do not force-match it (a concurrent close may have
		// advanced it), only that nothing was appended.
		if !s.closed {
			return nil
		}
		return []interface{}{s}

	case stoClosedDup:
		// Append-after-close with the SAME producer tuple that closed the stream:
		// store.Append returns ProducerResultDuplicate at the current offset with
		// NO new frame (memory_store.go closed-branch dup, append.lua dedup). Legal
		// only on a closed stream; idempotent no-op.
		if !s.closed {
			return nil
		}
		return []interface{}{s}

	case stoIndet:
		// A timed-out / inconclusive append (context deadline, partition, the
		// bounded-retry exhaustion in store/redis/store.go). It MAY have committed
		// at the slot or not — Redis serialized the EVAL either way, we just did
		// not learn the reply. So BOTH the committed and the unchanged states are
		// admissible (the INV-LIN-02 reframe: the optimistic retry never tears or
		// duplicates, so the only honest uncertainty is committed-vs-not). The
		// commit branch is dropped when it is impossible: the stream is already
		// closed (append.lua would have rejected) OR the tag already committed (a
		// double-commit would be a duplicate). When neither branch is admissible
		// the result is empty and the step is illegal.
		states := []interface{}{s}
		if !s.closed {
			if ns, ok := committed(); ok {
				states = append(states, ns)
			}
		}
		return states

	default:
		return nil
	}
}

// stepClose models store.CloseStream — a monotone, idempotent latch
// (INV-CLOSE-01). The reported FinalOffset must equal the current tail; closing
// never moves the tail.
func stepClose(s storeState, out storeOutput) []interface{} {
	switch out.status {
	case stoClosedFresh:
		// This op flipped the latch: legal only from an open stream, and the
		// returned FinalOffset is the (unchanged) tail.
		if s.closed {
			return nil
		}
		if out.offset != s.tail {
			return nil
		}
		ns := s.clone()
		ns.closed = true
		return []interface{}{ns}
	case stoClosedAlrdy:
		// Already closed (AlreadyClosed=true): an idempotent no-op. Legal only when
		// the stream is closed in this linearization; tail unchanged.
		if !s.closed {
			return nil
		}
		if out.offset != s.tail {
			return nil
		}
		return []interface{}{s}
	default:
		return nil
	}
}

// stepRead models store.Read — the offset-EXCLUSIVE contiguous suffix in
// linearized order, with exact (clientId, opSeq) frame identity (INV-READ-01) and
// the clean read-past-close EOF (INV-CLOSE / EOF). Read never changes state.
func stepRead(s storeState, in storeInput, out storeOutput) []interface{} {
	// Compute the exact suffix the spec says READ(readFrom) must return: every
	// committed frame whose END offset is strictly > readFrom, in append order.
	// Walk the linearized log accumulating end offsets; a frame is in the suffix
	// iff its end > readFrom. Because the log is contiguous (no gaps), the first
	// such frame begins exactly at the suffix's start and the rest follow in order.
	want := suffixFrom(s.frames, in.readFrom)

	// Frame identity must match EXACTLY: same count, same (clientID, opSeq, nbytes)
	// in the same order. A reorder, a gap (missing middle frame), a stale frame, or
	// a duplicate all diverge here. This is what the (clientId,opSeq) tag buys over
	// a byte-length-only check.
	if !framesEqual(out.readFrames, want) {
		return nil
	}

	// upToDate ⟺ the read reached the tail: the last returned frame ends at tail,
	// or (empty read) the caller's offset is already at tail. INV-READ-01's
	// upToDate clause.
	wantUpToDate := readAtTail(s, in.readFrom, want)
	if out.upToDate != wantUpToDate {
		return nil
	}

	// Clean EOF: the closed/end-of-stream signal a read may report is true ONLY
	// when the stream is closed AND the caller is at the tail (no more frames will
	// ever follow) — the handler_sse.go "final streamClosed AFTER final data"
	// contract and the long-poll Stream-Closed-at-tail 204. A closed signal while
	// frames remain unread, or on an open stream, is a violation.
	wantClosedSig := s.closed && wantUpToDate
	if out.readClosed != wantClosedSig {
		return nil
	}
	return []interface{}{s}
}

// stepGetOffset models store.GetCurrentOffset — reports the current tail, read
// only. The reported byte offset MUST equal the linearized tail.
func stepGetOffset(s storeState, out storeOutput) []interface{} {
	if out.offset != s.tail {
		return nil
	}
	return []interface{}{s}
}

// suffixFrom returns the frames whose end offset is strictly greater than from,
// in linearized order — the exact offset-exclusive suffix store.Read promises.
func suffixFrom(frames []frameTag, from uint64) []frameTag {
	var end uint64
	out := make([]frameTag, 0, len(frames))
	for _, f := range frames {
		end += f.nbytes
		if end > from {
			out = append(out, f)
		}
	}
	return out
}

// readAtTail decides the upToDate flag. The offset-exclusive suffix always runs
// to the tail (frames are contiguous: every frame with end > from is included,
// and the last frame ends exactly at the tail), so a NON-EMPTY suffix is always
// up-to-date. An EMPTY suffix is up-to-date iff the caller's offset is already at
// (or past) the tail — i.e. there is nothing beyond from. This mirrors
// memory_store.Read's upToDate computation (last.Offset == CurrentOffset, or
// offset == CurrentOffset when empty).
func readAtTail(s storeState, from uint64, want []frameTag) bool {
	if len(want) == 0 {
		return from >= s.tail
	}
	return true
}

// hasTag reports whether a frame with the same (clientID, opSeq) producer
// coordinate is already committed — the no-duplicate guard. nbytes is excluded
// from the identity match so a retry that re-frames the same logical append to a
// (wrongly) different length is still caught as a duplicate, not a fresh frame.
func hasTag(frames []frameTag, t frameTag) bool {
	for _, f := range frames {
		if f.clientID == t.clientID && f.opSeq == t.opSeq {
			return true
		}
	}
	return false
}

// framesEqual is exact frame-identity equality: same length, same (clientID,
// opSeq, nbytes) in the same order. frameTag is comparable, so == suffices
// element-wise.
func framesEqual(a, b []frameTag) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// storeStatesEqual is porcupine's Equal over the per-partition state. Two states
// are equal iff their closed latch, tail, and full frame log (by identity, in
// order) agree. Used by the NondeterministicModel power-set merge to collapse
// equivalent linearization frontiers — critical for the indeterminate append,
// whose two branches must merge cleanly to keep the search tractable.
func storeStatesEqual(a, b interface{}) bool {
	sa := a.(storeState)
	sb := b.(storeState)
	return sa.closed == sb.closed && sa.tail == sb.tail && framesEqual(sa.frames, sb.frames)
}

// partitionByPath groups a history by stream path so each stream's log is checked
// independently (Herlihy–Wing locality), keeping the per-key search modest. This
// is what makes per-stream linearizability compose to the whole history.
func partitionByPath(history []porcupine.Operation) [][]porcupine.Operation {
	byPath := map[string][]porcupine.Operation{}
	order := []string{}
	for _, o := range history {
		path := o.Input.(storeInput).path
		if _, seen := byPath[path]; !seen {
			order = append(order, path)
		}
		byPath[path] = append(byPath[path], o)
	}
	parts := make([][]porcupine.Operation, 0, len(order))
	for _, path := range order {
		parts = append(parts, byPath[path])
	}
	return parts
}

// describeStoreOp renders one operation for a counterexample timeline.
func describeStoreOp(input, output interface{}) string {
	in := input.(storeInput)
	out := output.(storeOutput)
	switch in.op {
	case opAppend:
		verb := "append"
		if in.closeWith {
			verb = "append+close"
		}
		return fmt.Sprintf("%s(%s,%dB) -> %s(off=%d)", verb, in.tag, in.nbytes, out.status, out.offset)
	case opClose:
		return fmt.Sprintf("close -> %s(final=%d)", out.status, out.offset)
	case opRead:
		return fmt.Sprintf("read(>%d) -> [%s] upToDate=%v closed=%v",
			in.readFrom, describeFrames(out.readFrames), out.upToDate, out.readClosed)
	case opGetOffset:
		return fmt.Sprintf("getOffset -> %d", out.offset)
	}
	return "?"
}

// describeStoreState renders the log register for a counterexample timeline.
func describeStoreState(state interface{}) string {
	s := state.(storeState)
	return fmt.Sprintf("{tail=%d closed=%v frames=[%s]}", s.tail, s.closed, describeFrames(s.frames))
}

func describeFrames(frames []frameTag) string {
	if len(frames) == 0 {
		return ""
	}
	parts := make([]string, len(frames))
	for i, f := range frames {
		parts[i] = f.String()
	}
	return strings.Join(parts, " ")
}
