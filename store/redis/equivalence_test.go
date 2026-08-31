package redis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
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
// Write fence (#183): the model also drives the WriteFenceStore capability —
// grant / renew / revoke / seal of claim markers for a pool of two authorities
// (one subscription and its recreated incarnation), fenced-class appends and
// closes presenting those claims, and fenced creates — so the fence rung
// (INV-DIFF-02 rung 3b), the per-authority seal and the producer binding are
// diffed between the Go oracle and the Lua mirror step for step. Marker
// retention is the one reaper the shared FakeClock does not drive: on Redis it
// is a wall-clock key TTL, on the MemoryStore a wall-clock expiresAt, both two
// minutes past any lease and so far beyond one run that neither backend reaps
// a marker mid-sequence.
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
// the cross-stream Check); claims records every claim fence granted on a path
// per pooled authority, so later ops can present a claim the stream slot has
// actually seen (accepted, renewed, sealed or superseded) rather than only
// fresh draws, and claimed lists those paths in a deterministic order.
type chronicleModel struct {
	oracle  *store.MemoryStore
	subject *Store
	clock   *store.FakeClock
	tally   *fenceTally // outcomes reached; nil records nothing

	paths   []string                              // every path ever created (may be deleted/expired)
	claims  map[string]map[int][]auth.AppendFence // path -> claimPool index -> granted fences
	claimed []string                              // every path with a granted claim, in grant order
}

// fenceTally counts the outcomes a property run reached, by key, so a probe
// can refuse a corpus fixture or a generator that no longer reaches the
// branch it is meant to — the vacuity a fence generator drawing every input
// independently falls into, since the accept shape is a conjunction of them.
type fenceTally struct {
	mu   sync.Mutex
	seen map[string]int
}

func newFenceTally() *fenceTally { return &fenceTally{seen: make(map[string]int)} }

// hit records one occurrence of key; a nil tally records nothing.
func (c *fenceTally) hit(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen[key]++
}

// reached asserts every key was hit at least once and reports the full tally
// on failure.
func (c *fenceTally) reached(t testing.TB, keys ...string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		if c.seen[key] == 0 {
			t.Errorf("outcome %q never reached; tally: %v", key, c.seen)
		}
	}
}

// String renders the tally with its keys sorted.
func (c *fenceTally) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, 0, len(c.seen))
	for k := range c.seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	for _, k := range keys {
		b = fmt.Appendf(b, "%s=%d ", k, c.seen[k])
	}
	return string(b)
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
	// A fork never inherits the write fence; it declares its own (C.1).
	opts.WriteFence = rapid.Bool().Draw(t, "forkWriteFence")
	m.applyCreate(t, path, opts)
}

func (m *chronicleModel) drawCreateOpts(t *rapid.T) store.CreateOptions {
	opts := store.CreateOptions{
		ContentType: contentTypeGen().Draw(t, "contentType"),
		Closed:      rapid.Bool().Draw(t, "createClosed"),
		WriteFence:  rapid.Bool().Draw(t, "createWriteFence"),
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
	if errors.Is(oErr, store.ErrInvalidForkSubOffset) {
		m.tally.hit("fork_suboffset_overshoot")
	}
}

// Append appends generated data with optional Stream-Seq and Close as a drawn
// write — open or fenced class, with or without a producer tuple — and diffs
// the full AppendResult, the fence disclosure included, and error.
func (m *chronicleModel) Append(t *rapid.T) {
	path := m.pickFencePath(t)
	data := dataGen().Draw(t, "data")
	w := m.drawWrite(t, path, false)
	opts := store.AppendOptions{
		Close:         rapid.Bool().Draw(t, "appendClose"),
		ProducerId:    w.producerID,
		ProducerEpoch: w.epoch,
		ProducerSeq:   w.seq,
		Fence:         w.fence,
	}
	if !w.acceptShape {
		opts.ContentType = contentTypeGen().Draw(t, "appendContentType")
	}
	if rapid.Bool().Draw(t, "withStreamSeq") {
		// Stream-Seq is compared lexicographically; small zero-padded strings
		// keep the comparison meaningful without tripping LB-2 here.
		opts.Seq = fmt.Sprintf("%04d", rapid.IntRange(0, 50).Draw(t, "streamSeq"))
	}

	oRes, oErr := m.oracle.Append(path, data, opts)
	sRes, sErr := m.subject.Append(path, data, opts)
	if m.diffErr(t, "Append", path, oErr, sErr) {
		m.diffAppendResult(t, path, oRes, sRes)
	}
	m.tallyWrite(path, opts.Fence, oRes.FenceReason, oErr)
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
	if m.diffErr(t, "CloseStream", path, oErr, sErr) {
		m.diffCloseResult(t, "CloseStream", path, oRes, sRes)
	}
}

// CloseStreamFenced closes through the claim rung (FencedCloser) with a drawn
// claim and diffs the CloseResult — the fence disclosure on refusal included —
// and error.
func (m *chronicleModel) CloseStreamFenced(t *rapid.T) {
	path := m.pickFencePath(t)
	_, fence := m.drawClaim(t, path)
	oRes, oErr := m.oracle.CloseStreamFenced(path, fence)
	sRes, sErr := m.subject.CloseStreamFenced(path, fence)
	if m.diffErr(t, "CloseStreamFenced", path, oErr, sErr) {
		m.diffCloseResult(t, "CloseStreamFenced", path, oRes, sRes)
	}
	var reason store.FenceReason
	if oRes != nil {
		reason = oRes.FenceReason
	}
	m.tallyWrite(path, &fence, reason, oErr)
}

// CloseStreamWithProducer closes with a drawn producer tuple (closedBy tuple
// dedup) of either class and diffs the full CloseProducerResult + error.
func (m *chronicleModel) CloseStreamWithProducer(t *rapid.T) {
	path := m.pickFencePath(t)
	w := m.drawWrite(t, path, true)
	opts := store.CloseProducerOptions{
		ProducerId:    w.producerID,
		ProducerEpoch: *w.epoch,
		ProducerSeq:   *w.seq,
		Fence:         w.fence,
	}
	oRes, oErr := m.oracle.CloseStreamWithProducer(path, opts)
	sRes, sErr := m.subject.CloseStreamWithProducer(path, opts)
	if m.diffErr(t, "CloseStreamWithProducer", path, oErr, sErr) {
		m.diffCloseProducerResult(t, path, oRes, sRes)
	}
	var reason store.FenceReason
	if oRes != nil {
		reason = oRes.FenceReason
		if oRes.ProducerResult == store.ProducerResultDuplicate {
			m.tally.hit("close_duplicate")
		}
	}
	m.tallyWrite(path, opts.Fence, reason, oErr)
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

// ---- write fence actions (#183) ----

// claimPool is the two fence authorities every fence op draws from: one
// subscription and its recreated incarnation, so per-authority seal isolation
// is exercised next to generation takeover within one authority. Generation,
// wake id and holder are drawn per op from small sets, so a fresh draw
// collides with an installed claim often enough to renew or supersede it, and
// misses it often enough to be refused as another claim.
var claimPool = []auth.AppendFence{
	{SubscriptionID: "sub", SubscriptionIncarnation: "inc-1"},
	{SubscriptionID: "sub", SubscriptionIncarnation: "inc-2"},
}

// pickFencePath draws a path for an op that presents or meets a claim,
// preferring one a claim has been granted on (three draws in four, when any)
// so the fenced class finds a marker the stream slot has actually installed
// and the open class finds a producer it has actually bound.
func (m *chronicleModel) pickFencePath(t *rapid.T) string {
	if len(m.claimed) > 0 && rapid.IntRange(0, 3).Draw(t, "claimedPath") != 0 {
		return rapid.SampledFrom(m.claimed).Draw(t, "claimed")
	}
	return m.pickPath(t)
}

// drawFreshClaim draws a complete claim fence of one pooled authority with a
// fresh generation / wake / holder. The lease is the caller's to set; the
// rung never reads it.
func drawFreshClaim(t *rapid.T) (int, auth.AppendFence) {
	id := rapid.IntRange(0, len(claimPool)-1).Draw(t, "claim")
	f := claimPool[id]
	f.Generation = rapid.Int64Range(1, 3).Draw(t, "generation")
	f.WakeID = rapid.SampledFrom([]string{"w_1", "w_2"}).Draw(t, "wakeID")
	f.Holder = rapid.SampledFrom([]string{"worker-a", "worker-b"}).Draw(t, "holder")
	return id, f
}

// drawClaim draws a claim fence for path: one granted there before (three
// draws in four, when any — the accept, renewal, sealed and superseded
// paths), else a fresh one.
func (m *chronicleModel) drawClaim(t *rapid.T, path string) (int, auth.AppendFence) {
	id, fresh := drawFreshClaim(t)
	if granted := m.claims[path][id]; len(granted) > 0 && rapid.IntRange(0, 3).Draw(t, "grantedClaim") != 0 {
		return id, rapid.SampledFrom(granted).Draw(t, "grantedFence")
	}
	return id, fresh
}

// writeDraw is one drawn write: its class (fence nil = open) and its producer
// tuple (epoch nil = no producer headers). acceptShape marks a fenced write
// left in the accept shape, which presents no content type so the rung, not
// the media type, decides it.
type writeDraw struct {
	fence       *auth.AppendFence
	producerID  string
	epoch, seq  *int64
	acceptShape bool
}

// drawWrite draws the class and producer tuple of an append or close on
// path. The open class (two draws in three, so the base ladder keeps its
// coverage) carries the boundary (epoch, seq) table on half of its writes, or
// on all of them when the op requires headers. The fenced class starts from
// the accept shape — a drawn claim, producer "p" at the claim generation and
// the sequence the oracle expects next — and disturbs at most one input: the
// headers dropped (producer_required on a fenced stream), the epoch off by
// one (epoch), or the boundary table instead (the producer ladder behind the
// rung). The claim itself supplies the marker, seal and lease outcomes.
func (m *chronicleModel) drawWrite(t *rapid.T, path string, headersRequired bool) writeDraw {
	var w writeDraw
	tuple := func(id string, epoch, seq int64) { w.producerID, w.epoch, w.seq = id, &epoch, &seq }
	boundary := func(id string) {
		es := rapid.SampledFrom(boundaryEpochSeq).Draw(t, "epochSeq")
		tuple(id, es[0], es[1])
	}
	if rapid.IntRange(0, 2).Draw(t, "writeClass") != 0 {
		if headersRequired || rapid.Bool().Draw(t, "withProducer") {
			boundary(rapid.SampledFrom([]string{"p", "q"}).Draw(t, "producerId"))
		}
		return w
	}
	_, fence := m.drawClaim(t, path)
	w.fence = &fence
	// The fenced class draws its producer id too, so a run can bind a second
	// id and exercise the per-id binding map on both backends.
	id := rapid.SampledFrom([]string{"p", "q"}).Draw(t, "fencedProducerId")
	shapes := []string{"accept", "accept", "epoch off by one", "boundary", "no producer"}
	if headersRequired {
		shapes = shapes[:4]
	}
	switch rapid.SampledFrom(shapes).Draw(t, "fencedShape") {
	case "accept":
		w.acceptShape = true
		tuple(id, fence.Generation, m.nextSeq(path, id, fence.Generation))
	case "epoch off by one":
		tuple(id, fence.Generation+1, 0)
	case "boundary":
		boundary(id)
	}
	return w
}

// nextSeq is the sequence the oracle accepts next from producer at epoch: the
// last accepted plus one at the producer's current epoch, else 0 (a first
// contact, an epoch bump, or a stale epoch the ladder refuses).
func (m *chronicleModel) nextSeq(path, producer string, epoch int64) int64 {
	meta, err := m.oracle.Get(path)
	if err != nil {
		return 0
	}
	if st := meta.Producers[producer]; st != nil && st.Epoch == epoch {
		return st.LastSeq + 1
	}
	return 0
}

// fenced reports whether the oracle sees path as a live write-fenced stream.
func (m *chronicleModel) fenced(path string) bool {
	meta, err := m.oracle.Get(path)
	return err == nil && meta.WriteFence
}

// sealedGeneration is the oracle's HEAD seal summary for path (0 when none or
// not live).
func (m *chronicleModel) sealedGeneration(path string) int64 {
	meta, err := m.oracle.Get(path)
	if err != nil {
		return 0
	}
	return meta.SealedGeneration
}

// tallyWrite records a write's outcome: a fence refusal by reason, the base
// producer refusals the corpus fixtures are named for, or the acceptance of a
// fenced-class write on a write-fenced stream (the rung's accept path, which
// binds the producer and fixes the class's last offset).
func (m *chronicleModel) tallyWrite(path string, fence *auth.AppendFence, reason store.FenceReason, err error) {
	switch {
	case errors.Is(err, store.ErrAppendFenced):
		m.tally.hit("fence:" + string(reason))
	case errors.Is(err, store.ErrInvalidEpochSeq):
		m.tally.hit("epoch_seq")
	case errors.Is(err, store.ErrProducerSeqGap):
		m.tally.hit("seq_gap")
	case err == nil && fence != nil && m.fenced(path):
		m.tally.hit("fence:accept")
	}
}

// drawLease draws a marker lease relative to the shared clock: already lapsed
// (the grant refuses it), the boundary now itself, or live for a window a
// later AdvanceClock may or may not outrun.
func (m *chronicleModel) drawLease(t *rapid.T) int64 {
	secs := rapid.SampledFrom([]int64{-1, 0, 1, 45, 100}).Draw(t, "leaseSeconds")
	return m.clock.Now().Add(time.Duration(secs) * time.Second).UnixNano()
}

// recordClaim remembers a fence granted on path for its authority, once per
// (generation, wake, holder): a renewal changes only the lease.
func (m *chronicleModel) recordClaim(path string, id int, fence auth.AppendFence) {
	if m.claims[path] == nil {
		m.claims[path] = make(map[int][]auth.AppendFence)
		m.claimed = append(m.claimed, path)
	}
	for _, g := range m.claims[path][id] {
		if g.Generation == fence.Generation && g.WakeID == fence.WakeID && g.Holder == fence.Holder {
			return
		}
	}
	m.claims[path][id] = append(m.claims[path][id], fence)
}

// applyGrant runs GrantAppendFence on both backends and diffs (installed,
// error). Like Delete, the diff is gated on observable liveness:
// grant_append_fence.lua consults the meta hash as it stands, so on an expired
// stream the two backends see the documented expiry-cleanup-timing asymmetry
// (see Check). A claim both backends installed is recorded for later ops.
func (m *chronicleModel) applyGrant(t *rapid.T, path string, id int, fence auth.AppendFence) {
	live := m.oracle.Has(path) && m.subject.Has(path)
	sealedBefore := m.sealedGeneration(path)
	oOK, oErr := m.oracle.GrantAppendFence(path, fence)
	sOK, sErr := m.subject.GrantAppendFence(path, fence)
	if live && m.diffErr(t, "GrantAppendFence", path, oErr, sErr) && oOK != sOK {
		t.Fatalf("GrantAppendFence(%s) installed mismatch: oracle=%v subject=%v", path, oOK, sOK)
	}
	switch {
	case oOK && sOK:
		m.recordClaim(path, id, fence)
		m.tally.hit("grant:installed")
		if m.sealedGeneration(path) != sealedBefore {
			m.tally.hit("grant:superseded")
		}
	case errors.Is(oErr, store.ErrAppendFenced):
		m.tally.hit("grant:fenced")
	}
}

// GrantFence installs a fresh claim marker on a drawn path: an install, a
// takeover with the supersession seal, a renewal when the draw lands on the
// installed claim, or a refusal as an older, sealed or foreign claim.
func (m *chronicleModel) GrantFence(t *rapid.T) {
	path := m.pickPath(t)
	id, fence := drawFreshClaim(t)
	fence.LeaseUntilNs = m.drawLease(t)
	m.applyGrant(t, path, id, fence)
}

// RenewFence re-grants a claim granted on a claimed path before with a new
// lease: the heartbeat renewal, which never shortens the marker lease and is
// refused once the claim is revoked, sealed or superseded.
func (m *chronicleModel) RenewFence(t *rapid.T) {
	if len(m.claimed) == 0 {
		t.Skip("no claim granted yet")
	}
	path := rapid.SampledFrom(m.claimed).Draw(t, "claimed")
	id := rapid.IntRange(0, len(claimPool)-1).Draw(t, "claim")
	granted := m.claims[path][id]
	if len(granted) == 0 {
		t.Skip("no claim of this authority here")
	}
	fence := rapid.SampledFrom(granted).Draw(t, "grantedFence")
	fence.LeaseUntilNs = m.drawLease(t)
	m.applyGrant(t, path, id, fence)
}

// RevokeFence tombstones a drawn claim's marker without sealing and diffs the
// error. The marker lives beside the stream on both backends, so the diff
// needs no liveness gate.
func (m *chronicleModel) RevokeFence(t *rapid.T) {
	path := m.pickFencePath(t)
	_, fence := m.drawClaim(t, path)
	m.diffErr(t, "RevokeAppendFence", path,
		m.oracle.RevokeAppendFence(path, fence), m.subject.RevokeAppendFence(path, fence))
}

// SealFence tombstones a drawn claim's marker and, on a fenced stream, seals
// its generation for its authority; it diffs the SealResult and error,
// liveness-gated as applyGrant is.
func (m *chronicleModel) SealFence(t *rapid.T) {
	path := m.pickFencePath(t)
	_, fence := m.drawClaim(t, path)
	live := m.oracle.Has(path) && m.subject.Has(path)
	oRes, oErr := m.oracle.SealAppendFence(path, fence)
	sRes, sErr := m.subject.SealAppendFence(path, fence)
	if live && m.diffErr(t, "SealAppendFence", path, oErr, sErr) &&
		(oRes.Outcome != sRes.Outcome || oRes.Generation != sRes.Generation || !oRes.FinalOffset.Equal(sRes.FinalOffset)) {
		t.Fatalf("SealAppendFence(%s) result mismatch: oracle=%+v subject=%+v", path, oRes, sRes)
	}
	if oErr == nil {
		m.tally.hit("seal:" + string(oRes.Outcome))
	}
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
	if o.FenceReason != s.FenceReason || o.FenceGeneration != s.FenceGeneration || o.FenceHolder != s.FenceHolder {
		t.Fatalf("Append(%s) fence disclosure mismatch: oracle=%+v subject=%+v", path, o, s)
	}
}

// diffCloseResult diffs a CloseResult: nil-ness, the final offset and
// already-closed flag on success, the fence disclosure on refusal.
func (m *chronicleModel) diffCloseResult(t *rapid.T, op, path string, o, s *store.CloseResult) {
	if (o == nil) != (s == nil) {
		t.Fatalf("%s(%s) nil-result mismatch: oracle=%v subject=%v", op, path, o, s)
	}
	if o == nil {
		return
	}
	if o.AlreadyClosed != s.AlreadyClosed {
		t.Fatalf("%s(%s) alreadyClosed mismatch: oracle=%v subject=%v", op, path, o.AlreadyClosed, s.AlreadyClosed)
	}
	if !o.FinalOffset.Equal(s.FinalOffset) {
		t.Fatalf("%s(%s) finalOffset mismatch: oracle=%v subject=%v", op, path, o.FinalOffset, s.FinalOffset)
	}
	if o.FenceReason != s.FenceReason || o.FenceGeneration != s.FenceGeneration || o.FenceHolder != s.FenceHolder {
		t.Fatalf("%s(%s) fence disclosure mismatch: oracle=%+v subject=%+v", op, path, o, s)
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
	if o.FenceReason != s.FenceReason || o.FenceGeneration != s.FenceGeneration || o.FenceHolder != s.FenceHolder {
		t.Fatalf("CloseStreamWithProducer(%s) fence disclosure mismatch: oracle=%+v subject=%+v", path, o, s)
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
	if oMeta.WriteFence != sMeta.WriteFence {
		t.Fatalf("Get(%s) writeFence mismatch: oracle=%v subject=%v", path, oMeta.WriteFence, sMeta.WriteFence)
	}
	if oMeta.SealedGeneration != sMeta.SealedGeneration {
		t.Fatalf("Get(%s) sealedGeneration mismatch: oracle=%d subject=%d", path, oMeta.SealedGeneration, sMeta.SealedGeneration)
	}
	if (oMeta.SealedOffset == nil) != (sMeta.SealedOffset == nil) ||
		(oMeta.SealedOffset != nil && !oMeta.SealedOffset.Equal(*sMeta.SealedOffset)) {
		t.Fatalf("Get(%s) sealedOffset mismatch: oracle=%v subject=%v", path, oMeta.SealedOffset, sMeta.SealedOffset)
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
	store.ErrAppendFenced,
	store.ErrWriteFenceUnsupported,
}

// TestEquivalenceMemoryVsRedis is the rapid state-machine harness. It is
// skipped under -short and when Redis is unreachable (newTestStore handles
// both). Each generated sequence gets a fresh FakeClock and a fresh oracle; the
// subject reuses the shared Redis client (paths are globally unique so streams
// never alias).
func TestEquivalenceMemoryVsRedis(t *testing.T) {
	base := newTestStore(t) // skips under -short / unreachable Redis

	tally := newFenceTally()
	rapid.Check(t, func(t *rapid.T) {
		runEquivalenceModelWith(t, base, tally)
	})
	t.Logf("outcomes reached: %v", tally)
	// The fence outcomes the generators reach in every default run by a wide
	// margin; the rarer ones are pinned deterministically by the corpus probe
	// (TestFuzzStoreEquivalenceCorpusReachesBranches).
	tally.reached(t, "grant:installed", "grant:fenced", "fence:marker", "seal:unfenced", "seal:sealed")
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
	runEquivalenceModelWith(t, base, nil)
}

// runEquivalenceModelWith is runEquivalenceModel recording the outcomes the
// sequence reaches in tally (nil records nothing).
func runEquivalenceModelWith(t *rapid.T, base *Store, tally *fenceTally) {
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
		tally:   tally,
		claims:  make(map[string]map[int][]auth.AppendFence),
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

	t.Run("internal bounded page does not renew sliding TTL", func(t *testing.T) {
		clock, oracle, subject := freshStores()
		path := testPath("exp-ttl-no-touch")
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

		clock.Set(start.Add(6 * time.Second))
		subjectPage, err := subject.ReadPage(
			context.Background(),
			path,
			store.ZeroOffset,
			store.ReadPageOptions{NoTouch: true},
		)
		if err != nil || len(subjectPage.Messages) != 1 {
			t.Fatalf("subject no-touch page: messages=%d err=%v", len(subjectPage.Messages), err)
		}
		oraclePage, err := oracle.ReadPage(
			context.Background(),
			path,
			store.ZeroOffset,
			store.ReadPageOptions{NoTouch: true},
		)
		if err != nil || len(oraclePage.Messages) != 1 {
			t.Fatalf("oracle no-touch page: messages=%d err=%v", len(oraclePage.Messages), err)
		}

		clock.Set(start.Add(time.Duration(ttl)*time.Second + time.Nanosecond))
		if oh, _ := bothHas(t, oracle, subject, path); oh {
			t.Fatal("internal snapshot extended the sliding TTL")
		}
	})
}
