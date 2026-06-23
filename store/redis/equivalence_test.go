package redis

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// equivalence_test.go is the MemoryStore-vs-Redis model-based equivalence
// harness (issue #26). It generates random, shrinkable sequences of store
// operations and runs each op against BOTH the in-process store.MemoryStore
// (the ORACLE / model) and the live Redis backend (the SUBJECT) on the same
// path, asserting they agree on (result, error, tail offset, key metadata,
// read payload) after every step.
//
// Soundness of single-threaded driving: Redis serializes a whole mutation per
// hash-tag slot, so one stream's mutations are already linearized; the model
// only needs to match that per-stream order, which a single goroutine provides
// (INV-LIN-01).
//
// Clock determinism: both stores share ONE injected store.FakeClock, so
// lazy-expiry / sliding-TTL / is_expired decisions are reproducible at a frozen
// now and never depend on independently-sampled wall clocks (the AdvanceClock
// action moves the shared clock; INV-DIFF-06, INV-EXP-01).
//
// Scope: NON-JSON content only. JSON-mode flattening (ProcessJSONAppend,
// fork-sub-offset arithmetic) is a separate issue (#44) and is deliberately
// excluded here so the generator never emits application/json streams.
//
// Failing seeds: rapid auto-persists a replayable failfile under
// store/redis/testdata/rapid/<TestName>/ on failure; committed minimized seeds
// live under store/redis/testdata/equivalence_seeds/ as regression fixtures
// (see that directory's README).

// eqPathCounter gives every generated stream a unique, collision-free path
// across the whole rapid run (paths are never reused, so a Delete + same-name
// Create cannot alias a stale slot).
var eqPathCounter atomic.Int64

// eqClockStart anchors the shared FakeClock at the Unix epoch so all UnixNano
// timestamps stay below 2^53 and are exact as Lua doubles (see
// TestEquivalenceMemoryVsRedis for why).
var eqClockStart = time.Unix(0, 0)

// boundaryEpochSeq seeds the generator's producer (epoch, seq) space with the
// interesting rungs from differential_test.go's table so the accept/reject
// ladder (INV-DIFF-02) is always exercised: first-contact seq 0, first-contact
// gap, epoch bump at seq 0, epoch bump at seq>0, duplicate, in-order, gap,
// stale epoch.
var boundaryEpochSeq = [][2]int64{
	{0, 0}, // new producer seq 0 accepted
	{7, 0}, // new producer any epoch at seq 0 accepted
	{0, 3}, // new producer nonzero seq is a gap
	{4, 0}, // (after epoch 5) stale epoch fenced
	{5, 0}, // in-order / first contact
	{5, 1}, // next seq
	{6, 0}, // epoch bump at seq 0 accepted
	{6, 1}, // epoch bump must start at seq 0
	{2, 4}, // duplicate seq
	{2, 7}, // seq gap
}

// chronicleModel is the rapid state machine. The oracle and subject share one
// FakeClock; paths records every stream created so far (for fork sources and
// the cross-stream Check).
type chronicleModel struct {
	oracle  *store.MemoryStore
	subject *Store
	clock   *store.FakeClock

	paths []string // every path ever created (may be deleted/expired)
}

// Check is the after-every-action invariant: for every known path, the oracle
// and subject must agree on observable metadata (Get), the full read payload
// from the zero offset (Read), and — for streams that are observably live —
// the tail offset. (Per-op result/error agreement is asserted inside each
// action.)
//
// Tail agreement is gated on observable liveness on purpose. GetCurrentOffset
// is documented on BOTH backends as expiry-blind (it reads the raw tail without
// checking expiry). But the two backends reap an expired stream's storage at
// different moments: Redis lazy-expiry physically DELs the meta hash (so the
// tail field vanishes -> ErrStreamNotFound) during the next expiry-aware op,
// while MemoryStore leaves the expired entry in its map until a Create/Delete
// removes it. That cleanup-timing gap is the documented MemoryStore-vs-Redis
// asymmetry (INV-EXP-01) and is NOT an observable divergence: Get/Has/Read all
// agree (they are expiry-aware and clock-driven), and those ARE asserted here.
func (m *chronicleModel) Check(t *rapid.T) {
	for _, p := range m.paths {
		// Direct Read agrees on every path: both backends hide expired and
		// soft-deleted streams as ErrStreamNotFound on the direct read path.
		m.assertReadAgrees(t, p, store.ZeroOffset)

		live := m.oracle.Has(p) && m.subject.Has(p)
		if live {
			// Observable metadata and the raw tail are only meaningfully
			// comparable while the stream is live on both backends. For a dead
			// stream, Get's error class is the documented asymmetry: an expired
			// fork SOURCE is reported ErrStreamNotFound by the MemoryStore
			// (expired, map entry not yet flipped) but ErrStreamSoftDeleted by
			// Redis (expire_cleanup eagerly set softDel=1 to preserve fork
			// readability). Both mean "not directly visible"; Has agrees (false
			// on both) and fork readability is preserved on both (INV-EXP-01).
			m.assertMetaAgrees(t, p)
			m.assertTailAgrees(t, p)
		}
	}
}

func (m *chronicleModel) newPath() string {
	n := eqPathCounter.Add(1)
	return fmt.Sprintf("/eq%d%s/%d", testRunStamp, eqWorkerTag, n)
}

// eqWorkerTag namespaces every generated path with a per-PROCESS suffix. It is
// empty for the in-process property runner (TestEquivalenceMemoryVsRedis, where
// a single process owns the test DB after the one-time flush), so that test's
// paths are byte-for-byte unchanged. The fuzz target (FuzzStoreEquivalence)
// sets it to a per-process token because `go test -fuzz` spawns MULTIPLE worker
// PROCESSES that all share the same live Redis DB; without a per-process suffix
// two workers whose testRunStamp collided (both = time.Now().UnixNano() at
// package init) could alias the same key, and one worker's create/delete would
// corrupt the other's oracle-vs-subject comparison. Combined with the
// non-flushing fuzz setup (a worker must NOT FlushDB out from under its peers),
// this keeps concurrent fuzz workers fully isolated on one shared DB. See the
// FINDING note in equivalence_fuzz_test.go.
var eqWorkerTag = ""

// pickPath draws an existing path (or skips if none). Heavily biases toward
// recently-created paths to keep sequences focused on live streams.
func (m *chronicleModel) pickPath(t *rapid.T) string {
	if len(m.paths) == 0 {
		t.Skip("no streams yet")
	}
	return rapid.SampledFrom(m.paths).Draw(t, "path")
}

// contentTypeGen draws a NON-JSON content type. application/json is excluded
// (JSON-mode flattening is issue #44).
func contentTypeGen() *rapid.Generator[string] {
	return rapid.SampledFrom([]string{
		"",
		"application/octet-stream",
		"text/plain",
		"text/plain; charset=utf-8",
		"application/x-binary",
	})
}

// dataGen draws a non-empty payload (Redis rejects empty non-close appends with
// ErrEmptyBody; MemoryStore relies on the handler to enforce that, so empty
// appends are a backend asymmetry the harness avoids by construction). Includes
// the frame separator '|', 0x00 and 0xff to exercise framing.
func dataGen() *rapid.Generator[[]byte] {
	return rapid.SliceOfN(rapid.Byte(), 1, 24)
}

// Create makes a plain stream with a generated content type and optional
// TTL/ExpiresAt/Closed, on a fresh unique path.
func (m *chronicleModel) Create(t *rapid.T) {
	path := m.newPath()
	opts := m.drawCreateOpts(t)
	m.applyCreate(t, path, opts)
}

// ForkCreate forks an existing stream via CreateOptions (there is no Fork
// method; forking is a Create). Uses a fork offset at the source's current tail
// or a generated earlier offset, plus an optional binary sub-offset.
func (m *chronicleModel) ForkCreate(t *rapid.T) {
	src := m.pickPath(t)
	path := m.newPath()
	opts := store.CreateOptions{ForkedFrom: src}

	// Optionally pin a fork offset <= the oracle's view of the source tail.
	if rapid.Bool().Draw(t, "forkOffsetPinned") {
		tail, err := m.oracle.GetCurrentOffset(src)
		if err == nil && tail.ByteOffset > 0 {
			bo := rapid.Uint64Range(0, tail.ByteOffset).Draw(t, "forkByteOffset")
			fo := store.Offset{ReadSeq: tail.ReadSeq, ByteOffset: bo}
			opts.ForkOffset = &fo
		}
	}
	// Optionally request a binary sub-offset (bytes into the first message
	// after the fork point). Small values keep the resolution path realistic.
	if rapid.Bool().Draw(t, "forkSubOffset") {
		sub := rapid.Uint64Range(0, 8).Draw(t, "forkSub")
		opts.ForkSubOffset = &sub
	}
	m.applyCreate(t, path, opts)
}

func (m *chronicleModel) drawCreateOpts(t *rapid.T) store.CreateOptions {
	opts := store.CreateOptions{
		ContentType: contentTypeGen().Draw(t, "contentType"),
		Closed:      rapid.Bool().Draw(t, "createClosed"),
	}
	switch rapid.SampledFrom([]string{"none", "ttl", "expiresAt"}).Draw(t, "expiryKind") {
	case "ttl":
		ttl := rapid.Int64Range(1, 120).Draw(t, "ttlSeconds")
		opts.TTLSeconds = &ttl
	case "expiresAt":
		// Relative to the shared clock so expiry is reproducible.
		dt := rapid.Int64Range(1, 120).Draw(t, "expiresInSeconds")
		exp := m.clock.Now().Add(time.Duration(dt) * time.Second)
		opts.ExpiresAt = &exp
	}
	return opts
}

// applyCreate runs Create on both backends and diffs (created, error). On
// success the path is recorded for later actions and the cross-stream Check.
func (m *chronicleModel) applyCreate(t *rapid.T, path string, opts store.CreateOptions) {
	_, oCreated, oErr := m.oracle.Create(path, opts)
	_, sCreated, sErr := m.subject.Create(path, opts)
	if m.diffErr(t, "Create", path, oErr, sErr) && oCreated != sCreated {
		t.Fatalf("Create(%s) created mismatch: oracle=%v subject=%v", path, oCreated, sCreated)
	}
	if oErr == nil {
		m.paths = append(m.paths, path)
	}
}

// Append appends generated data with optional Stream-Seq, producer epoch/seq,
// and Close, diffing the full AppendResult and error.
func (m *chronicleModel) Append(t *rapid.T) {
	path := m.pickPath(t)
	data := dataGen().Draw(t, "data")
	opts := store.AppendOptions{
		ContentType: contentTypeGen().Draw(t, "appendContentType"),
		Close:       rapid.Bool().Draw(t, "appendClose"),
	}
	if rapid.Bool().Draw(t, "withStreamSeq") {
		// Stream-Seq is compared lexicographically; small zero-padded strings
		// keep the comparison meaningful without tripping LB-2 here.
		opts.Seq = fmt.Sprintf("%04d", rapid.IntRange(0, 50).Draw(t, "streamSeq"))
	}
	if rapid.Bool().Draw(t, "withProducer") {
		es := rapid.SampledFrom(boundaryEpochSeq).Draw(t, "epochSeq")
		epoch, seq := es[0], es[1]
		opts.ProducerId = rapid.SampledFrom([]string{"p", "q"}).Draw(t, "producerId")
		opts.ProducerEpoch = &epoch
		opts.ProducerSeq = &seq
	}

	oRes, oErr := m.oracle.Append(path, data, opts)
	sRes, sErr := m.subject.Append(path, data, opts)
	if m.diffErr(t, "Append", path, oErr, sErr) {
		m.diffAppendResult(t, path, oRes, sRes)
	}
}

// Read draws a starting offset (zero, the current tail, or a generated earlier
// byte offset) and diffs the returned messages, upToDate flag, and error.
func (m *chronicleModel) Read(t *rapid.T) {
	path := m.pickPath(t)
	off := m.drawReadOffset(t, path)
	m.assertReadAgrees(t, path, off)
}

func (m *chronicleModel) drawReadOffset(t *rapid.T, path string) store.Offset {
	switch rapid.SampledFrom([]string{"zero", "tail", "earlier"}).Draw(t, "readFrom") {
	case "tail":
		if tail, err := m.oracle.GetCurrentOffset(path); err == nil {
			return tail
		}
		return store.ZeroOffset
	case "earlier":
		if tail, err := m.oracle.GetCurrentOffset(path); err == nil && tail.ByteOffset > 0 {
			bo := rapid.Uint64Range(0, tail.ByteOffset).Draw(t, "readByteOffset")
			return store.Offset{ReadSeq: tail.ReadSeq, ByteOffset: bo}
		}
		return store.ZeroOffset
	default:
		return store.ZeroOffset
	}
}

// CloseStream closes (idempotent) and diffs the CloseResult + error.
func (m *chronicleModel) CloseStream(t *rapid.T) {
	path := m.pickPath(t)
	oRes, oErr := m.oracle.CloseStream(path)
	sRes, sErr := m.subject.CloseStream(path)
	if m.diffErr(t, "CloseStream", path, oErr, sErr) && oErr == nil {
		if oRes.AlreadyClosed != sRes.AlreadyClosed {
			t.Fatalf("CloseStream(%s) alreadyClosed mismatch: oracle=%v subject=%v", path, oRes.AlreadyClosed, sRes.AlreadyClosed)
		}
		if !oRes.FinalOffset.Equal(sRes.FinalOffset) {
			t.Fatalf("CloseStream(%s) finalOffset mismatch: oracle=%v subject=%v", path, oRes.FinalOffset, sRes.FinalOffset)
		}
	}
}

// CloseStreamWithProducer closes with producer headers (closedBy tuple dedup)
// and diffs the full CloseProducerResult + error.
func (m *chronicleModel) CloseStreamWithProducer(t *rapid.T) {
	path := m.pickPath(t)
	es := rapid.SampledFrom(boundaryEpochSeq).Draw(t, "closeEpochSeq")
	opts := store.CloseProducerOptions{
		ProducerId:    rapid.SampledFrom([]string{"p", "q"}).Draw(t, "closeProducerId"),
		ProducerEpoch: es[0],
		ProducerSeq:   es[1],
	}
	oRes, oErr := m.oracle.CloseStreamWithProducer(path, opts)
	sRes, sErr := m.subject.CloseStreamWithProducer(path, opts)
	if m.diffErr(t, "CloseStreamWithProducer", path, oErr, sErr) {
		m.diffCloseProducerResult(t, path, oRes, sRes)
	}
}

// Delete deletes a stream (soft-delete when forks reference it, hard delete
// otherwise). The error is diffed only when the stream was observably live on
// both backends before the call: MemoryStore.Delete is expiry-blind (it deletes
// an expired-but-not-yet-reaped map entry and returns nil), while Redis's
// delete.lua is expiry-aware (it returns ErrStreamNotFound and reaps). That is
// the documented expiry-cleanup-timing asymmetry (see Check); the observable
// end state — the stream is gone — agrees either way, which the post-step Check
// verifies via Get/Read/Has.
func (m *chronicleModel) Delete(t *rapid.T) {
	path := m.pickPath(t)
	live := m.oracle.Has(path) && m.subject.Has(path)
	oErr := m.oracle.Delete(path)
	sErr := m.subject.Delete(path)
	if live {
		m.diffErr(t, "Delete", path, oErr, sErr)
	}
}

// GetCurrentOffset diffs the tail offset for a stream that is observably live
// on both backends. For an expired stream the raw tail may have been reaped on
// one side but not the other (the documented expiry-cleanup-timing asymmetry,
// see Check); GetCurrentOffset is expiry-blind on both backends, so its raw
// result is only meaningful while the stream is live.
func (m *chronicleModel) GetCurrentOffset(t *rapid.T) {
	path := m.pickPath(t)
	if m.oracle.Has(path) && m.subject.Has(path) {
		m.assertTailAgrees(t, path)
	}
}

// AdvanceClock moves the shared FakeClock forward, driving lazy-expiry and
// sliding-TTL deterministically on BOTH backends at once (INV-DIFF-06).
func (m *chronicleModel) AdvanceClock(t *rapid.T) {
	secs := rapid.Int64Range(0, 90).Draw(t, "advanceSeconds")
	m.clock.Advance(time.Duration(secs) * time.Second)
}

// ---- diff helpers ----

// diffErr asserts the two backends returned errors that are equivalent in the
// store's sentinel-error sense (both nil, or both wrap the same sentinel).
//
// ErrStreamNotFound and ErrStreamSoftDeleted are collapsed into one
// "inaccessible" equivalence class. They denote the same client-observable
// outcome (the stream is not directly usable) and the two backends legitimately
// disagree on WHICH of the two a dead-but-forked stream reports: Redis's
// expire_cleanup eagerly flips an expired fork SOURCE to soft-deleted (to keep
// fork reads working), while the MemoryStore reports the still-expired map
// entry as not-found until a later op flips it. That is the documented
// expiry-cleanup asymmetry (INV-EXP-01). A genuine live-vs-dead divergence is
// still caught: nil-vs-error never collapses, and the post-step Check asserts
// observable agreement (Has/Get/Read) on every live stream.
// diffErr returns resultsComparable=true when the two outcomes are an exact
// match (both nil, or both the same sentinel class), so the caller may go on to
// diff the success/error result payloads. It returns false when the outcomes
// were collapsed via the inaccessible equivalence class (NotFound <-> SoftDel),
// in which case the result payloads are not meaningfully comparable.
func (m *chronicleModel) diffErr(t *rapid.T, op, path string, oErr, sErr error) (resultsComparable bool) {
	if (oErr == nil) != (sErr == nil) {
		t.Fatalf("%s(%s) error presence mismatch: oracle=%v subject=%v", op, path, oErr, sErr)
	}
	if oErr == nil {
		return true
	}
	if inaccessible(oErr) && inaccessible(sErr) {
		return errors.Is(oErr, store.ErrStreamNotFound) == errors.Is(sErr, store.ErrStreamNotFound)
	}
	for _, sentinel := range storeSentinels {
		if errors.Is(oErr, sentinel) != errors.Is(sErr, sentinel) {
			t.Fatalf("%s(%s) error class mismatch on %v: oracle=%v subject=%v", op, path, sentinel, oErr, sErr)
		}
	}
	return true
}

// inaccessible reports whether err is one of the "stream not directly usable"
// sentinels that the two backends may legitimately swap at the expiry-cleanup
// boundary (see diffErr).
func inaccessible(err error) bool {
	return errors.Is(err, store.ErrStreamNotFound) || errors.Is(err, store.ErrStreamSoftDeleted)
}

func (m *chronicleModel) diffAppendResult(t *rapid.T, path string, o, s store.AppendResult) {
	if !o.Offset.Equal(s.Offset) {
		t.Fatalf("Append(%s) offset mismatch: oracle=%v subject=%v", path, o.Offset, s.Offset)
	}
	if o.ProducerResult != s.ProducerResult {
		t.Fatalf("Append(%s) producerResult mismatch: oracle=%v subject=%v", path, o.ProducerResult, s.ProducerResult)
	}
	if o.CurrentEpoch != s.CurrentEpoch || o.ExpectedSeq != s.ExpectedSeq ||
		o.ReceivedSeq != s.ReceivedSeq || o.LastSeq != s.LastSeq {
		t.Fatalf("Append(%s) producer detail mismatch: oracle=%+v subject=%+v", path, o, s)
	}
	if o.StreamClosed != s.StreamClosed {
		t.Fatalf("Append(%s) streamClosed mismatch: oracle=%v subject=%v", path, o.StreamClosed, s.StreamClosed)
	}
}

func (m *chronicleModel) diffCloseProducerResult(t *rapid.T, path string, o, s *store.CloseProducerResult) {
	if (o == nil) != (s == nil) {
		t.Fatalf("CloseStreamWithProducer(%s) nil-result mismatch: oracle=%v subject=%v", path, o, s)
	}
	if o == nil {
		return
	}
	if !o.FinalOffset.Equal(s.FinalOffset) {
		t.Fatalf("CloseStreamWithProducer(%s) finalOffset mismatch: oracle=%v subject=%v", path, o.FinalOffset, s.FinalOffset)
	}
	if o.ProducerResult != s.ProducerResult || o.LastSeq != s.LastSeq ||
		o.CurrentEpoch != s.CurrentEpoch || o.ExpectedSeq != s.ExpectedSeq || o.ReceivedSeq != s.ReceivedSeq {
		t.Fatalf("CloseStreamWithProducer(%s) producer detail mismatch: oracle=%+v subject=%+v", path, o, s)
	}
	if o.StreamClosed != s.StreamClosed || o.AlreadyClosed != s.AlreadyClosed {
		t.Fatalf("CloseStreamWithProducer(%s) close-flag mismatch: oracle=%+v subject=%+v", path, o, s)
	}
}

func (m *chronicleModel) assertTailAgrees(t *rapid.T, path string) {
	oTail, oErr := m.oracle.GetCurrentOffset(path)
	sTail, sErr := m.subject.GetCurrentOffset(path)
	m.diffErr(t, "GetCurrentOffset", path, oErr, sErr)
	if oErr == nil && !oTail.Equal(sTail) {
		t.Fatalf("GetCurrentOffset(%s) mismatch: oracle=%v subject=%v", path, oTail, sTail)
	}
}

func (m *chronicleModel) assertReadAgrees(t *rapid.T, path string, off store.Offset) {
	oMsgs, oUp, oErr := m.oracle.Read(path, off)
	sMsgs, sUp, sErr := m.subject.Read(path, off)
	m.diffErr(t, "Read", path, oErr, sErr)
	if oErr != nil {
		return
	}
	if oUp != sUp {
		t.Fatalf("Read(%s, %v) upToDate mismatch: oracle=%v subject=%v", path, off, oUp, sUp)
	}
	if len(oMsgs) != len(sMsgs) {
		t.Fatalf("Read(%s, %v) message count mismatch: oracle=%d subject=%d", path, off, len(oMsgs), len(sMsgs))
	}
	for i := range oMsgs {
		if !oMsgs[i].Offset.Equal(sMsgs[i].Offset) {
			t.Fatalf("Read(%s, %v) msg[%d] offset mismatch: oracle=%v subject=%v", path, off, i, oMsgs[i].Offset, sMsgs[i].Offset)
		}
		if string(oMsgs[i].Data) != string(sMsgs[i].Data) {
			t.Fatalf("Read(%s, %v) msg[%d] payload mismatch: oracle=%q subject=%q", path, off, i, oMsgs[i].Data, sMsgs[i].Data)
		}
	}
}

// assertMetaAgrees diffs the observable metadata fields that both backends
// persist. Get hides expired/soft-deleted streams identically on both sides, so
// the error class is part of the comparison.
func (m *chronicleModel) assertMetaAgrees(t *rapid.T, path string) {
	oMeta, oErr := m.oracle.Get(path)
	sMeta, sErr := m.subject.Get(path)
	m.diffErr(t, "Get", path, oErr, sErr)
	if oErr != nil {
		return
	}
	if oMeta.Closed != sMeta.Closed {
		t.Fatalf("Get(%s) closed mismatch: oracle=%v subject=%v", path, oMeta.Closed, sMeta.Closed)
	}
	if !oMeta.CurrentOffset.Equal(sMeta.CurrentOffset) {
		t.Fatalf("Get(%s) currentOffset mismatch: oracle=%v subject=%v", path, oMeta.CurrentOffset, sMeta.CurrentOffset)
	}
	if oMeta.LastSeq != sMeta.LastSeq {
		t.Fatalf("Get(%s) lastSeq mismatch: oracle=%q subject=%q", path, oMeta.LastSeq, sMeta.LastSeq)
	}
	if oMeta.RefCount != sMeta.RefCount {
		t.Fatalf("Get(%s) refCount mismatch: oracle=%d subject=%d", path, oMeta.RefCount, sMeta.RefCount)
	}
	if oMeta.ForkedFrom != sMeta.ForkedFrom {
		t.Fatalf("Get(%s) forkedFrom mismatch: oracle=%q subject=%q", path, oMeta.ForkedFrom, sMeta.ForkedFrom)
	}
	if !normContentType(oMeta.ContentType).equal(normContentType(sMeta.ContentType)) {
		t.Fatalf("Get(%s) contentType mismatch: oracle=%q subject=%q", path, oMeta.ContentType, sMeta.ContentType)
	}
	if (oMeta.ClosedBy == nil) != (sMeta.ClosedBy == nil) {
		t.Fatalf("Get(%s) closedBy presence mismatch: oracle=%v subject=%v", path, oMeta.ClosedBy, sMeta.ClosedBy)
	}
	if oMeta.ClosedBy != nil && *oMeta.ClosedBy != *sMeta.ClosedBy {
		t.Fatalf("Get(%s) closedBy mismatch: oracle=%+v subject=%+v", path, *oMeta.ClosedBy, *sMeta.ClosedBy)
	}
}

// normContentType applies the same normalization both backends use for
// matching (empty -> octet-stream, parameters stripped, ASCII-lowered) so a
// metadata diff doesn't trip on a cosmetic content-type echo difference.
type normalizedCT string

func (a normalizedCT) equal(b normalizedCT) bool { return a == b }

func normContentType(ct string) normalizedCT {
	return normalizedCT(normalizeCT(ct))
}

// storeSentinels is the set of sentinel errors the two backends classify
// against. Equality means "both wrap the same sentinel".
var storeSentinels = []error{
	store.ErrStreamNotFound,
	store.ErrStreamExpired,
	store.ErrStreamExists,
	store.ErrConfigMismatch,
	store.ErrSequenceConflict,
	store.ErrContentTypeMismatch,
	store.ErrEmptyBody,
	store.ErrInvalidOffset,
	store.ErrStreamClosed,
	store.ErrStaleEpoch,
	store.ErrInvalidEpochSeq,
	store.ErrProducerSeqGap,
	store.ErrPartialProducer,
	store.ErrStreamSoftDeleted,
	store.ErrInvalidForkOffset,
	store.ErrInvalidForkSubOffset,
	store.ErrRefCountUnderflow,
}

// TestEquivalenceMemoryVsRedis is the rapid state-machine harness. It is
// skipped under -short and when Redis is unreachable (newTestStore handles
// both). Each generated sequence gets a fresh FakeClock and a fresh oracle; the
// subject reuses the shared Redis client (paths are globally unique so streams
// never alias).
func TestEquivalenceMemoryVsRedis(t *testing.T) {
	base := newTestStore(t) // skips under -short / unreachable Redis

	rapid.Check(t, func(t *rapid.T) {
		runEquivalenceModel(t, base)
	})
}

// runEquivalenceModel is the ONE state-machine property body driven by both the
// PR-gate property runner (TestEquivalenceMemoryVsRedis above, via rapid.Check)
// and the nightly coverage-guided fuzz target (FuzzStoreEquivalence in
// equivalence_fuzz_test.go, via rapid.MakeFuzz). Both consume the identical
// chronicleModel, the identical StateMachineActions, and the identical
// after-every-step Check oracle — the only difference is who feeds the
// bitstream rapid draws from (uniform PRNG for Check, the coverage-guided fuzz
// input bytes for MakeFuzz). Keeping a single body is the whole point of the
// MakeFuzz bridge (issue #42): one model serves both regimes, so a fuzz-found
// divergence is byte-for-byte the same failure the property runner would report.
func runEquivalenceModel(t *rapid.T, base *Store) {
	// Anchor the shared clock at the Unix epoch so every UnixNano timestamp
	// the harness produces stays well below 2^53 and is therefore EXACTLY
	// representable as a Lua double. Redis's is_expired runs in Lua (doubles)
	// while the MemoryStore oracle compares int64s; at multi-billion-second
	// wall-clock magnitudes a double loses ~256ns, which could make the two
	// backends disagree inside a sub-microsecond expiry window. Keeping now
	// small removes that rounding entirely, so is_expired is bit-identical
	// across backends at every generated instant (INV-DIFF-06).
	clock := store.NewFakeClock(eqClockStart)
	oracle := store.NewMemoryStore(store.WithClock(clock))
	subject := New(base.client, Options{Clock: clock})

	m := &chronicleModel{
		oracle:  oracle,
		subject: subject,
		clock:   clock,
	}

	// Bootstrap one baseline stream so the model's initial state is non-degenerate
	// (paths is never empty). Every path-requiring action (Append/Read/Delete/
	// Close*/Fork*/GetCurrentOffset) calls pickPath, which t.Skip()s the action
	// when no stream exists yet. Under rapid.Check the uniform action draw almost
	// always reaches Create early, so this corner never bit. Under rapid.MakeFuzz
	// a degenerate fuzz bitstream (e.g. a long run of one byte) can draw the SAME
	// path-requiring action on every one of rapid's 100 executeAction tries; with
	// no stream to act on, all 100 skip and rapid aborts the whole property with
	// "can't find a valid (non-skipped) action" — a fuzz crasher that is a harness
	// robustness gap, NOT a backend divergence (FINDING, issue #42). Seeding a
	// baseline stream guarantees at least one applicable action from step 1 (a real
	// server likewise always has a stream available once one is created), and is
	// behavior-identical for rapid.Check (which created a stream that early anyway).
	// This is the standard rapid idiom for an action precondition that some entity
	// exists; it adds no new action, generator, or invariant.
	m.applyCreate(t, m.newPath(), store.CreateOptions{})

	t.Repeat(rapid.StateMachineActions(m))
}

// TestEquivalenceExpiryBoundary is the focused, deterministic frozen-clock test
// for INV-DIFF-06: at a fixed now the MemoryStore lazy-expiry (IsExpiredAt) and
// the Redis Lua is_expired must agree, INCLUDING the boundary now == expiry =>
// NOT expired (a strict ">" on both sides). It covers both the TTLSeconds and
// the ExpiresAt configs and the sliding-TTL touch (a renewing Read pushes the
// expiry forward identically on both backends).
//
// Liveness is observed through Has, which is expiry-aware and clock-driven on
// both backends, so it reflects exactly the is_expired decision under test.
func TestEquivalenceExpiryBoundary(t *testing.T) {
	base := newTestStore(t)
	// Anchor at the Unix epoch so the nanosecond boundary is exact in Lua
	// doubles (see TestEquivalenceMemoryVsRedis).
	start := eqClockStart

	// freshStores returns a clock-sharing oracle+subject pair anchored at start.
	freshStores := func() (*store.FakeClock, *store.MemoryStore, *Store) {
		clock := store.NewFakeClock(start)
		return clock, store.NewMemoryStore(store.WithClock(clock)), New(base.client, Options{Clock: clock})
	}

	bothHas := func(t *testing.T, oracle *store.MemoryStore, subject *Store, path string) (bool, bool) {
		t.Helper()
		oh, sh := oracle.Has(path), subject.Has(path)
		if oh != sh {
			t.Fatalf("Has(%s) mismatch: oracle=%v subject=%v", path, oh, sh)
		}
		return oh, sh
	}

	t.Run("TTLSeconds boundary now==expiry is not expired", func(t *testing.T) {
		clock, oracle, subject := freshStores()
		path := testPath("exp-ttl-boundary")
		ttl := int64(10)
		opts := store.CreateOptions{ContentType: "text/plain", TTLSeconds: &ttl}
		mustCreate(t, subject, path, opts)
		if _, _, err := oracle.Create(path, opts); err != nil {
			t.Fatal(err)
		}

		// now == LastAccessedAt + ttl exactly: strict ">" => NOT expired on both.
		clock.Set(start.Add(time.Duration(ttl) * time.Second))
		if oh, _ := bothHas(t, oracle, subject, path); !oh {
			t.Fatal("at now == expiry the stream must NOT be expired (strict >)")
		}

		// One nanosecond past expiry: expired on both.
		clock.Set(start.Add(time.Duration(ttl)*time.Second + time.Nanosecond))
		if oh, _ := bothHas(t, oracle, subject, path); oh {
			t.Fatal("one ns past expiry the stream must be expired on both backends")
		}
	})

	t.Run("ExpiresAt boundary now==expiry is not expired", func(t *testing.T) {
		clock, oracle, subject := freshStores()
		path := testPath("exp-at-boundary")
		expAt := start.Add(30 * time.Second)
		opts := store.CreateOptions{ContentType: "text/plain", ExpiresAt: &expAt}
		mustCreate(t, subject, path, opts)
		if _, _, err := oracle.Create(path, opts); err != nil {
			t.Fatal(err)
		}

		clock.Set(expAt) // now == ExpiresAt: NOT expired (After is strict)
		if oh, _ := bothHas(t, oracle, subject, path); !oh {
			t.Fatal("at now == ExpiresAt the stream must NOT be expired")
		}
		clock.Set(expAt.Add(time.Nanosecond))
		if oh, _ := bothHas(t, oracle, subject, path); oh {
			t.Fatal("one ns past ExpiresAt the stream must be expired on both backends")
		}
	})

	t.Run("sliding TTL touch on Read renews the window identically", func(t *testing.T) {
		clock, oracle, subject := freshStores()
		path := testPath("exp-ttl-slide")
		ttl := int64(10)
		opts := store.CreateOptions{ContentType: "text/plain", TTLSeconds: &ttl}
		mustCreate(t, subject, path, opts)
		if _, _, err := oracle.Create(path, opts); err != nil {
			t.Fatal(err)
		}
		mustAppend(t, subject, path, []byte("x"), store.AppendOptions{})
		if _, err := oracle.Append(path, []byte("x"), store.AppendOptions{}); err != nil {
			t.Fatal(err)
		}

		// Advance 6s (< ttl), then Read on both: the touch resets LastAccessedAt
		// to now, sliding the deadline to now+ttl.
		clock.Set(start.Add(6 * time.Second))
		if _, _, err := subject.Read(path, store.ZeroOffset); err != nil {
			t.Fatalf("subject Read at 6s: %v", err)
		}
		if _, _, err := oracle.Read(path, store.ZeroOffset); err != nil {
			t.Fatalf("oracle Read at 6s: %v", err)
		}

		// At 6s + ttl exactly (16s): boundary of the renewed window, NOT expired.
		clock.Set(start.Add(6*time.Second + time.Duration(ttl)*time.Second))
		if oh, _ := bothHas(t, oracle, subject, path); !oh {
			t.Fatal("renewed window: at now == renewed expiry must NOT be expired")
		}
		// Past the renewed window: expired on both.
		clock.Set(start.Add(6*time.Second + time.Duration(ttl)*time.Second + time.Nanosecond))
		if oh, _ := bothHas(t, oracle, subject, path); oh {
			t.Fatal("renewed window: one ns past renewed expiry must be expired on both")
		}
	})
}
