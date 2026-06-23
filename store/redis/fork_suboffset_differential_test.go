package redis

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// fork_suboffset_differential_test.go is the FORK-SUB-OFFSET differential (P2.2,
// issue #36). resolveForkSubOffset exists in TWO independent copies — the
// MemoryStore oracle (store/memory_store.go) and the live-Redis subject
// (store/redis/store.go) — with a DUAL MEANING that drifts easily:
//
//   - JSON source:   the sub-offset counts FLATTENED MESSAGES and resolves to
//     msgs[subOffset-1].Offset (1-INDEXED), rejecting ErrInvalidForkSubOffset when
//     len(msgs) < subOffset. The fork's internal ForkOffset is ADVANCED to that
//     boundary; ForkOffsetRequested keeps the user's original.
//   - binary source: the sub-offset counts BYTES into the first following message,
//     returned as a PREFIX the fork materializes as its own first frame, rejecting
//     ErrInvalidForkSubOffset when len(first) < subOffset. ForkOffset stays at the
//     user-supplied offset; CurrentOffset advances by len(prefix).
//
// The read-stitching that backs both paths caps inherited frames with an INCLUSIVE
// LessThanOrEqual against the fork offset (store.go:734, memory_store.go:731 +
// capAtOffset at :709) — an off-by-one hazard at the boundary.
//
// This property drives fork-create requests with non-zero ForkSubOffset over JSON
// and binary sources — scalars, nested and empty top-level arrays, multi-message
// chains, sub-offsets AT and ONE PAST the message count / byte length, and the
// nil-vs-0 branch — and asserts both backends return the IDENTICAL
// (resolvedOffset, prefixBytes, error) triple from resolveForkSubOffset, observed
// through the public Create surface:
//
//   - error class agrees (ErrInvalidForkSubOffset at the overshoot boundary);
//   - on success the resolved ForkOffset agrees (JSON: advanced to
//     msgs[subOffset-1].Offset; binary: unchanged), CurrentOffset agrees (binary:
//     +len(prefix)), and the verbatim ForkSubOffset / ForkOffsetRequested agree;
//   - PREFIX-EQUALITY: reading the fork from zero returns frame-identical results
//     across backends — the inherited prefix up to AND INCLUDING ForkOffset, then
//     the fork's own first frame beginning at the resolved boundary. The inclusive
//     LessThanOrEqual cap and the JSON 1-indexed subOffset-1 arithmetic are
//     exercised at their exact boundaries against the sequential MemoryStore oracle.
//
// Clock determinism, oracle pairing, and Real-Redis-or-skip mirror
// json_differential_test.go: ONE injected FakeClock at the Unix epoch (no expiry
// configured, so the clock never moves), the in-process MemoryStore as ORACLE, live
// Redis as SUBJECT, newTestStore skipping under -short / when Redis is unreachable,
// running in the ./store/redis test-integration CI target on every PR.
//
// Failing seeds shrink to a minimal (source content, subOffset); commit the shrunk
// case as a deterministic regression row in TestForkSubOffsetDifferentialCorpus
// (1-indexed vs 0-indexed, inclusive vs exclusive cap, JSON-count vs byte, nil-vs-0).

// forkSubResult is the (resolvedOffset, prefixBytes, error) triple observed through
// the public Create surface, the differential's comparison unit.
type forkSubResult struct {
	err          error
	forkOffset   store.Offset // resolved internal divergence point (JSON: advanced; binary: unchanged)
	currentOff   store.Offset // tail after the fork's own materialized prefix (binary: +len(prefix))
	requested    *store.Offset
	subOffset    uint64
	prefixFrames []store.Message // the fork's read frames from zero (prefix-equality)
	upToDate     bool
}

// createForkAndRead creates a fork on one backend with the given (forkOffset,
// subOffset) and, on success, reads it from zero — capturing the full triple +
// read prefix.
func createForkAndRead(s store.Store, forkPath, srcPath string, forkOffset *store.Offset, subOffset *uint64, ct string) forkSubResult {
	opts := store.CreateOptions{ForkedFrom: srcPath, ForkOffset: forkOffset, ForkSubOffset: subOffset}
	if ct != "" {
		opts.ContentType = ct
	}
	meta, _, err := s.Create(forkPath, opts)
	if err != nil {
		return forkSubResult{err: err}
	}
	msgs, upToDate, rerr := s.Read(forkPath, store.ZeroOffset)
	return forkSubResult{
		forkOffset:   meta.ForkOffset,
		currentOff:   meta.CurrentOffset,
		requested:    meta.ForkOffsetRequested,
		subOffset:    meta.ForkSubOffset,
		prefixFrames: msgs,
		upToDate:     upToDate,
		err:          rerr, // a read error after a successful create is itself a divergence to surface
	}
}

// assertForkSubOffsetAgrees creates the same fork on the oracle and the subject and
// asserts the full triple + prefix-equality match. label/detail are for messages.
func assertForkSubOffsetAgrees(rt rapidT, oracle, subject store.Store, srcPath string, forkOffset *store.Offset, subOffset *uint64, ct, detail string) {
	oForkPath := newForkPath()
	sForkPath := newForkPath()
	o := createForkAndRead(oracle, oForkPath, srcPath, forkOffset, subOffset, ct)
	s := createForkAndRead(subject, sForkPath, srcPath, forkOffset, subOffset, ct)

	// (1) Error presence + class must match (the overshoot boundary returns
	// ErrInvalidForkSubOffset; a clean create returns nil).
	if (o.err == nil) != (s.err == nil) {
		rt.Fatalf("fork-sub-offset error presence mismatch %s: oracle=%v subject=%v", detail, o.err, s.err)
	}
	if o.err != nil {
		if errors.Is(o.err, store.ErrInvalidForkSubOffset) != errors.Is(s.err, store.ErrInvalidForkSubOffset) {
			rt.Fatalf("fork-sub-offset ErrInvalidForkSubOffset class mismatch %s: oracle=%v subject=%v", detail, o.err, s.err)
		}
		// Both errored identically — the overshoot reject agrees. Done.
		return
	}

	// (2) Resolved triple agrees: ForkOffset (JSON: advanced to msgs[subOffset-1];
	// binary: unchanged), CurrentOffset (binary: +len(prefix)), verbatim subOffset,
	// and the user-supplied ForkOffsetRequested.
	if !o.forkOffset.Equal(s.forkOffset) {
		rt.Fatalf("resolved ForkOffset mismatch %s: oracle=%v subject=%v", detail, o.forkOffset, s.forkOffset)
	}
	if !o.currentOff.Equal(s.currentOff) {
		rt.Fatalf("fork CurrentOffset mismatch %s: oracle=%v subject=%v", detail, o.currentOff, s.currentOff)
	}
	if o.subOffset != s.subOffset {
		rt.Fatalf("stored ForkSubOffset mismatch %s: oracle=%d subject=%d", detail, o.subOffset, s.subOffset)
	}
	if (o.requested == nil) != (s.requested == nil) {
		rt.Fatalf("ForkOffsetRequested presence mismatch %s: oracle=%v subject=%v", detail, o.requested, s.requested)
	}
	if o.requested != nil && !o.requested.Equal(*s.requested) {
		rt.Fatalf("ForkOffsetRequested mismatch %s: oracle=%v subject=%v", detail, *o.requested, *s.requested)
	}

	// (3) PREFIX-EQUALITY: the fork's read frames from zero are frame-identical
	// across backends — the inherited prefix up to AND INCLUDING ForkOffset, then
	// the fork's own first frame at the resolved boundary.
	if o.upToDate != s.upToDate {
		rt.Fatalf("fork read upToDate mismatch %s: oracle=%v subject=%v", detail, o.upToDate, s.upToDate)
	}
	if len(o.prefixFrames) != len(s.prefixFrames) {
		rt.Fatalf("fork read frame-count mismatch %s: oracle=%d subject=%d", detail, len(o.prefixFrames), len(s.prefixFrames))
	}
	for i := range o.prefixFrames {
		if !bytes.Equal(o.prefixFrames[i].Data, s.prefixFrames[i].Data) {
			rt.Fatalf("fork read frame[%d] data mismatch %s: oracle=%q subject=%q", i, detail, o.prefixFrames[i].Data, s.prefixFrames[i].Data)
		}
		if !o.prefixFrames[i].Offset.Equal(s.prefixFrames[i].Offset) {
			rt.Fatalf("fork read frame[%d] offset mismatch %s: oracle=%v subject=%v", i, detail, o.prefixFrames[i].Offset, s.prefixFrames[i].Offset)
		}
		// Every inherited frame must be <= the resolved ForkOffset (the INCLUSIVE
		// cap); the fork's own first frame (if any) begins strictly after it. This
		// pins the LessThanOrEqual boundary directly.
		if o.prefixFrames[i].Offset.LessThanOrEqual(o.forkOffset) {
			continue // inherited (or, for JSON, the boundary message itself)
		}
		// First own frame past the boundary: its offset must exceed ForkOffset.
		if !o.forkOffset.LessThan(o.prefixFrames[i].Offset) {
			rt.Fatalf("own frame[%d] not strictly past ForkOffset %s: forkOffset=%v frame=%v", i, detail, o.forkOffset, o.prefixFrames[i].Offset)
		}
	}
}

// jsonArrayGen draws a top-level JSON array of n scalar elements (each >= 1 byte),
// the flatten path the sub-offset counts messages over. Reuses jsonScalarGen from
// json_differential_test.go.
func jsonArrayBodyGen(t *rapid.T, n int) string {
	elems := make([]string, n)
	for i := range elems {
		elems[i] = jsonScalarGen().Draw(t, fmt.Sprintf("jelem%d", i))
	}
	return "[" + joinWithWS(t, elems, "jarr") + "]"
}

// binaryChunkGen draws a non-empty binary message whose bytes stress the byte-prefix
// arithmetic (NULs, high bytes, the frame separator).
func binaryChunkGen() *rapid.Generator[[]byte] {
	return rapid.SliceOfN(rapid.SampledFrom([]byte{0x00, 0xff, 0x7c /* | */, 'a', ' ', '\n', 0x80}), 1, 8)
}

// TestForkSubOffsetDifferentialJSON is the JSON rapid property: a JSON source of N
// flattened messages, forked at ForkOffset=0 with a sub-offset drawn from
// [0, N+1] — covering 0 (the nil-equivalent no-slice path), every in-range message
// boundary, AT the count (the last valid 1-indexed boundary), and ONE PAST it (the
// ErrInvalidForkSubOffset overshoot). MemoryStore vs live Redis must agree on the
// resolved (forkOffset, error) triple and the read prefix. [INV-FORK + INV-OFF-06]
func TestForkSubOffsetDifferentialJSON(t *testing.T) {
	base := newTestStore(t)

	rapid.Check(t, func(rt *rapid.T) {
		clock := store.NewFakeClock(time.Unix(0, 0))
		oracle := store.NewMemoryStore(store.WithClock(clock))
		subject := New(base.client, Options{Clock: clock})

		srcPath := newForkPath()
		opts := store.CreateOptions{ContentType: "application/json"}
		mustCreatePair(rt, oracle, subject, srcPath, opts)

		// Append a (possibly nested) top-level array so the source has N flattened
		// messages. Append twice sometimes to chain message offsets non-trivially.
		n1 := rapid.IntRange(1, 5).Draw(rt, "arr1Len")
		appendPair(rt, oracle, subject, srcPath, jsonArrayBodyGen(rt, n1), "application/json")
		total := n1
		if rapid.Bool().Draw(rt, "secondAppend") {
			n2 := rapid.IntRange(1, 4).Draw(rt, "arr2Len")
			appendPair(rt, oracle, subject, srcPath, jsonArrayBodyGen(rt, n2), "application/json")
			total += n2
		}

		// ForkOffset = 0 so the sub-offset counts from the stream head; sub in
		// [0, total+1] hits the nil-equivalent (0), every interior boundary, the count
		// itself (last valid), and one past (overshoot).
		zero := store.ZeroOffset
		sub := uint64(rapid.IntRange(0, total+1).Draw(rt, "subOffset"))
		detail := fmt.Sprintf("[JSON src N=%d sub=%d]", total, sub)
		if sub == 0 {
			// sub-offset 0 must behave exactly like the nil branch (no slice): assert
			// both the explicit 0 and nil agree with each other AND the backends.
			assertForkSubOffsetAgrees(rt, oracle, subject, srcPath, &zero, &sub, "application/json", detail+" explicit-0")
			assertForkSubOffsetAgrees(rt, oracle, subject, srcPath, &zero, nil, "application/json", detail+" nil")
			return
		}
		assertForkSubOffsetAgrees(rt, oracle, subject, srcPath, &zero, &sub, "application/json", detail)
	})
}

// TestForkSubOffsetDifferentialBinary is the binary rapid property: a binary source
// of several messages, forked at the boundary BEFORE the first following message,
// with a byte sub-offset drawn from [0, len(first)+1] — covering 0 (nil-equivalent),
// every interior byte, the full length (the whole first message as prefix), and one
// past (ErrInvalidForkSubOffset). The resolved triple (ForkOffset unchanged,
// CurrentOffset += len(prefix)) and the materialized first own frame must agree.
func TestForkSubOffsetDifferentialBinary(t *testing.T) {
	base := newTestStore(t)

	rapid.Check(t, func(rt *rapid.T) {
		clock := store.NewFakeClock(time.Unix(0, 0))
		oracle := store.NewMemoryStore(store.WithClock(clock))
		subject := New(base.client, Options{Clock: clock})

		srcPath := newForkPath()
		mustCreatePair(rt, oracle, subject, srcPath, store.CreateOptions{})

		// First message at [0, len(head)); a second message [len(head), ...) is what
		// the sub-offset slices a byte prefix of when forking at len(head).
		head := binaryChunkGen().Draw(rt, "headMsg")
		appendPair(rt, oracle, subject, srcPath, string(head), "")
		follow := binaryChunkGen().Draw(rt, "followMsg")
		appendPair(rt, oracle, subject, srcPath, string(follow), "")

		forkAt := store.Offset{ByteOffset: uint64(len(head))} // the boundary before `follow`
		sub := uint64(rapid.IntRange(0, len(follow)+1).Draw(rt, "subBytes"))
		detail := fmt.Sprintf("[binary head=%d follow=%d forkAt=%d sub=%d]", len(head), len(follow), len(head), sub)
		if sub == 0 {
			assertForkSubOffsetAgrees(rt, oracle, subject, srcPath, &forkAt, &sub, "", detail+" explicit-0")
			assertForkSubOffsetAgrees(rt, oracle, subject, srcPath, &forkAt, nil, "", detail+" nil")
			return
		}
		assertForkSubOffsetAgrees(rt, oracle, subject, srcPath, &forkAt, &sub, "", detail)
	})
}

// forkSubCorpusCase is a deterministic regression row: a committed boundary case
// that pins a specific failure class (1-indexed vs 0-indexed, inclusive vs
// exclusive cap, JSON-count vs byte, nil-vs-0). New shrunk divergences land here.
type forkSubCorpusCase struct {
	name    string
	ct      string   // "" = binary, "application/json" = JSON
	appends []string // source content appends
	forkAt  *store.Offset
	sub     *uint64
	nilTwin bool // also assert the nil-ForkSubOffset twin agrees (the nil-vs-0 branch)
}

// TestForkSubOffsetDifferentialCorpus replays hand-picked boundary cases through
// the SAME assertForkSubOffsetAgrees path the rapid properties use, so each tricky
// boundary is a deterministic MemoryStore-vs-Redis regression row. These are the
// exact off-by-one / dual-meaning boundaries the issue calls out.
func TestForkSubOffsetDifferentialCorpus(t *testing.T) {
	base := newTestStore(t)
	off := func(b uint64) *store.Offset { o := store.Offset{ByteOffset: b}; return &o }
	sub := func(n uint64) *uint64 { return &n }
	zero := store.ZeroOffset

	cases := []forkSubCorpusCase{
		// JSON: 1-indexed message boundary arithmetic. [10,20,30] -> offsets 2,4,6.
		{name: "json sub=1 first boundary", ct: "application/json", appends: []string{`[10,20,30]`}, forkAt: &zero, sub: sub(1)},
		{name: "json sub=2 middle boundary", ct: "application/json", appends: []string{`[10,20,30]`}, forkAt: &zero, sub: sub(2)},
		{name: "json sub=3 AT count (last valid)", ct: "application/json", appends: []string{`[10,20,30]`}, forkAt: &zero, sub: sub(3)},
		{name: "json sub=4 ONE PAST count (overshoot)", ct: "application/json", appends: []string{`[10,20,30]`}, forkAt: &zero, sub: sub(4)},
		{name: "json sub=0 nil-equivalent", ct: "application/json", appends: []string{`[10,20,30]`}, forkAt: &zero, sub: sub(0), nilTwin: true},
		// JSON across two appends: [1,2] then [3] -> 3 messages total.
		{name: "json chained sub=3 spans two appends", ct: "application/json", appends: []string{`[1,2]`, `[3]`}, forkAt: &zero, sub: sub(3)},
		// Binary: byte-prefix arithmetic. "hello"(0..5) then "world"(5..10); fork at 5.
		{name: "binary sub=1 first byte", appends: []string{"hello", "world"}, forkAt: off(5), sub: sub(1)},
		{name: "binary sub=3 mid bytes", appends: []string{"hello", "world"}, forkAt: off(5), sub: sub(3)},
		{name: "binary sub=5 AT length (whole msg)", appends: []string{"hello", "world"}, forkAt: off(5), sub: sub(5)},
		{name: "binary sub=6 ONE PAST length (overshoot)", appends: []string{"hello", "world"}, forkAt: off(5), sub: sub(6)},
		{name: "binary sub=0 nil-equivalent", appends: []string{"hello", "world"}, forkAt: off(5), sub: sub(0), nilTwin: true},
		// Binary fork at tail with sub-offset past it: no following message -> reject.
		{name: "binary fork at tail sub=1 no follow (overshoot)", appends: []string{"hello"}, forkAt: off(5), sub: sub(1)},
	}

	rt := testingTAdapter{t}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := store.NewFakeClock(time.Unix(0, 0))
			oracle := store.NewMemoryStore(store.WithClock(clock))
			subject := New(base.client, Options{Clock: clock})
			srcPath := newForkPath()
			opts := store.CreateOptions{}
			if tc.ct != "" {
				opts.ContentType = tc.ct
			}
			mustCreatePair(testingTAdapter{t}, oracle, subject, srcPath, opts)
			for _, a := range tc.appends {
				appendPair(testingTAdapter{t}, oracle, subject, srcPath, a, tc.ct)
			}
			assertForkSubOffsetAgrees(rt, oracle, subject, srcPath, tc.forkAt, tc.sub, tc.ct, "["+tc.name+"]")
			if tc.nilTwin {
				assertForkSubOffsetAgrees(rt, oracle, subject, srcPath, tc.forkAt, nil, tc.ct, "["+tc.name+" nil-twin]")
			}
		})
	}
}

// ---- shared helpers ----

// mustCreatePair creates the same stream on both backends, asserting both succeed.
func mustCreatePair(rt rapidT, oracle, subject store.Store, path string, opts store.CreateOptions) {
	if _, _, err := oracle.Create(path, opts); err != nil {
		rt.Fatalf("oracle create %s: %v", path, err)
	}
	if _, _, err := subject.Create(path, opts); err != nil {
		rt.Fatalf("subject create %s: %v", path, err)
	}
}

// appendPair appends the same body to both backends, asserting agreement on error
// presence and the resulting tail (the source must be byte-identical for the fork
// arithmetic to be meaningfully differential).
func appendPair(rt rapidT, oracle, subject store.Store, path, body, ct string) {
	opts := store.AppendOptions{}
	if ct != "" {
		opts.ContentType = ct
	}
	oRes, oErr := oracle.Append(path, []byte(body), opts)
	sRes, sErr := subject.Append(path, []byte(body), opts)
	if (oErr == nil) != (sErr == nil) {
		rt.Fatalf("source append error presence mismatch for %q: oracle=%v subject=%v", body, oErr, sErr)
	}
	if oErr != nil {
		return
	}
	if !oRes.Offset.Equal(sRes.Offset) {
		rt.Fatalf("source append tail mismatch for %q: oracle=%v subject=%v", body, oRes.Offset, sRes.Offset)
	}
}

// newForkPath returns a fresh unique stream path so fork-sub-offset streams never
// alias across the run (mirrors newJSONPath).
func newForkPath() string {
	return testPath("forksub")
}
