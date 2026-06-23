package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
)

// scenario_store.go is the imperative SHELL of the data-plane linearizability
// check (the Porcupine arm of issue #35): the live driver that binds the pure
// streamModel() (model_store.go) to the shipped Redis backend (store/redis). It
// is the model_fence.go ↔ scenario_lease.go / model_shard.go ↔ scenario_ownership.go
// pattern, applied to APPEND / READ / CLOSE / GETOFFSET on ONE stream.
//
// K concurrent clients hammer a single freshly-created stream. Each client
// brackets every op via history.go's recordOp (host-monotonic clock, so
// INV-JEP-REC-01 real-time soundness holds even while the clock-skew nemesis
// runs), then the recorded history is checked against streamModel() with
// porcupine.CheckOperationsVerbose; an Illegal verdict writes a VisualizePath
// witness. A PASS validates INV-LIN-01 (the single-slot Lua EVAL over the
// {escapePath(path)} hash-tag slot is the linearization point) from the OUTSIDE,
// under real concurrency + faults — what was prose-only before.
//
// Indeterminate appends (INV-LIN-02): each append runs under a short
// context deadline. A deadline-exceeded / contention-exhausted append is
// recorded as stoIndet — the streamModel linearizes it committed-OR-not — so a
// retry that silently double-committed (a duplicate frame) or left a byte gap
// has no valid linearization and Porcupine surfaces it; an honest
// maybe-committed append does not produce a false Illegal.
//
// Nemeses: this driver talks to Redis directly (no cluster), so it applies the
// in-process / proxy-level faults that perturb the single Redis slot — gcPause
// (a worker stalls mid-cycle) and, when -toxiproxy is supplied, a Redis
// partition / added latency (nemesis.go). The pod-kill / kubectl faults are
// cluster-only and not applicable here. The recorder clock stays on the driver
// host throughout (history.go), so no fault can corrupt the [Call,Return] order.

// runStoreLinz is the store-linz scenario entry point. It creates one stream,
// runs K clients doing append/read/close/getOffset against live Redis under the
// gcPause (+ optional toxiproxy) nemeses, and checks the history for
// linearizability against streamModel().
func runStoreLinz(c config) error {
	client, err := storeRedisClient(c)
	if err != nil {
		return err
	}
	defer client.Close()
	st := redisstore.New(client, redisstore.Options{})

	path := fmt.Sprintf("/store-linz/%d", time.Now().UnixNano())
	if _, _, err := st.Create(path, store.CreateOptions{ContentType: "application/octet-stream"}); err != nil {
		return fmt.Errorf("create stream %s: %w", path, err)
	}
	defer func() { _ = st.Delete(path) }()

	K := c.workers
	if K < 1 {
		K = 1
	}
	fmt.Printf("== store-linz: path=%s clients=%d toxiproxy=%q for %dms ==\n",
		path, K, c.toxiproxy, c.workloadMs)

	rec := newRecorder()
	deadline := time.Now().Add(time.Duration(c.workloadMs) * time.Millisecond)

	// Optional Redis partition/latency nemesis on a randomized window. The
	// recorder clock is host-monotonic, so even a partition that makes appends
	// time out (-> indeterminate) cannot corrupt the history ordering.
	stop := make(chan struct{})
	var nemWG sync.WaitGroup
	if c.toxiproxy != "" {
		n := &nemesis{}
		tp := newToxiproxy(c.toxiproxy, c.redisProxy)
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		nemWG.Add(1)
		go func() {
			defer nemWG.Done()
			n.churn(stop, rng, 300*time.Millisecond, 900*time.Millisecond, func() {
				_ = tp.partition()
				sleep(nemesisWindow(rng, 80*time.Millisecond, 220*time.Millisecond))
				_ = tp.heal()
			})
		}()
	}

	var wg sync.WaitGroup
	for w := 0; w < K; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			storeClient(st, path, wid, K, deadline, rec)
		}(w)
	}
	wg.Wait()
	close(stop)
	nemWG.Wait()

	history := rec.history()
	parts := len(partitionByPath(history))
	fmt.Printf("operations: %d across %d path partition(s)\n", len(history), parts)

	result, info := porcupine.CheckOperationsVerbose(streamModel(), history, 30*time.Second)
	switch result {
	case porcupine.Ok:
		fmt.Println("PASS: data-plane linearizable — append/read/close on the single-slot EVAL are a linearization point (INV-LIN-01/02, INV-CLOSE-01, INV-READ-01)")
		return nil
	case porcupine.Illegal:
		const witness = "store-linz-counterexample.html"
		if verr := porcupine.VisualizePath(streamModel(), info, witness); verr == nil {
			fmt.Printf("counterexample: %s\n", witness)
		}
		return fmt.Errorf("NOT linearizable: the recorded append/read/close history has no valid linearization against streamModel() — a torn write, byte-offset gap, duplicate/reordered frame, or dirty close/EOF (INV-LIN-01/02/CLOSE-01/READ-01 violated)")
	default:
		return fmt.Errorf("linearizability UNKNOWN: history too concurrent for the timeout (reduce -workers/-workload-ms)")
	}
}

// storeClient runs one client's append/read/close/getOffset loop until the
// deadline, recording every op into the shared porcupine history. Each client
// owns a private opSeq counter so its frames carry a unique (clientId, opSeq)
// tag — the exact-read identity the model checks (INV-READ-01).
func storeClient(st *redisstore.Store, path string, clientID, K int, deadline time.Time, rec *recorder) {
	rng := rand.New(rand.NewSource(int64(clientID)*1_000_003 + time.Now().UnixNano()))
	opSeq := 0
	// Exactly one client (the last) is the closer, late in the run, so most ops
	// race on the OPEN stream (the contended slot) and the close/append-after-close
	// /read-past-close transitions are still exercised. closedOnce guards a single
	// fresh close across the run.
	isCloser := clientID == K-1

	for time.Now().Before(deadline) {
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4, 5:
			storeDoAppend(st, path, clientID, &opSeq, rng, rec)
		case 6, 7, 8:
			storeDoRead(st, path, clientID, rng, rec)
		case 9:
			storeDoGetOffset(st, path, clientID, rec)
		}
		// gcPause nemesis: occasionally a client stalls, widening the interleavings
		// the single Redis slot must serialize (the in-process T1/T3 nemesis).
		if rng.Intn(40) == 0 {
			sleep(gcPause(40 * time.Millisecond))
		}
		// The closer flips the latch once, past the half-way mark so reads/appends
		// keep racing the open stream before and the closed stream after.
		if isCloser && opSeq >= 6 && !storeAlreadyClosed.Load() {
			storeDoClose(st, path, clientID, rec)
		}
	}
	// Guarantee the stream ends CLOSED so the read-past-close / append-after-close
	// (EOF) transitions are recorded even if the closer never tripped its condition.
	if isCloser && !storeAlreadyClosed.Load() {
		storeDoClose(st, path, clientID, rec)
	}
}

// storeAlreadyClosed guards the single fresh close per run so most close ops
// observe AlreadyClosed (idempotency) while exactly one trips the latch.
var storeAlreadyClosed atomic.Bool

// storeDoAppend issues one append under a short deadline. A timeout / contention
// exhaustion is recorded as INDETERMINATE (the maybe-committed honesty); a clean
// success/closed/dup reply is recorded exactly.
func storeDoAppend(st *redisstore.Store, path string, clientID int, opSeq *int, rng *rand.Rand, rec *recorder) {
	seq := *opSeq
	*opSeq++
	// The payload ENCODES the (clientId, opSeq) tag (plus padding to a random
	// length) so a reader recovers the exact frame identity the writer stamped —
	// the Elle-recoverability idea that makes the model's read step compare
	// identity, not just byte length. The content type is octet-stream so the
	// store writes the bytes verbatim (no JSON flatten).
	data := encodeFrame(clientID, seq, 1+rng.Intn(16))
	n := uint64(len(data))
	t := frameTag{clientID: clientID, opSeq: seq, nbytes: n}

	callNs := rec.now()
	// store.Store.Append takes no context (it runs context.Background() internally),
	// so the commit-indeterminate window here is the store's OWN bounded optimistic
	// re-frame loop giving up under contention ("too much contention" after
	// maxAppendRetries) — isIndeterminate classifies that, and a partition that makes
	// the underlying go-redis call fail surfaces as the default (also indeterminate)
	// branch. Either way a maybe-committed append is recorded as stoIndet, never as a
	// fabricated OK/closed verdict.
	res, err := st.Append(path, data, store.AppendOptions{})

	in := storeInput{path: path, op: opAppend, tag: t, nbytes: n}
	var out storeOutput
	switch {
	case err == nil:
		out = storeOutput{status: stoOK, offset: res.Offset.ByteOffset}
	case errors.Is(err, store.ErrStreamClosed):
		out = storeOutput{status: stoClosedErr, offset: res.Offset.ByteOffset}
	case isIndeterminate(err):
		// Timed out / too-much-contention: the EVAL may or may not have committed.
		in.expectIndet = true
		out = storeOutput{status: stoIndet}
	default:
		// An unexpected error (e.g. ErrStreamNotFound from a teardown race): treat
		// it as indeterminate rather than fabricate a verdict — the honest stance.
		in.expectIndet = true
		out = storeOutput{status: stoIndet}
	}
	rec.recordOp(clientID, in, out, callNs)
}

// storeDoRead reads from a randomly chosen lower-bound offset and records the
// EXACT tagged suffix returned (frame identity, not just length), the upToDate
// flag, and the clean-EOF closed signal — the INV-READ-01 / INV-CLOSE-EOF checks.
func storeDoRead(st *redisstore.Store, path string, clientID int, rng *rand.Rand, rec *recorder) {
	// Read from the tail occasionally (to exercise read-past-close EOF) and from
	// the beginning or a small offset otherwise.
	from := store.ZeroOffset
	if cur, err := st.GetCurrentOffset(path); err == nil {
		switch rng.Intn(3) {
		case 0:
			from = cur // at tail
		case 1:
			if cur.ByteOffset > 0 {
				from = store.Offset{ByteOffset: uint64(rng.Intn(int(cur.ByteOffset) + 1))}
			}
		default:
			from = store.ZeroOffset
		}
	}

	callNs := rec.now()
	msgs, upToDate, err := st.Read(path, from)
	if err != nil {
		// A read error (teardown race) is not recordable as a deterministic read
		// outcome; skip it rather than fabricate frames.
		return
	}
	// The closed/EOF signal mirrors the handler: closed AND the read reached the
	// tail. We recompute it from metadata exactly as handler.go / handler_sse.go do
	// (currentMeta.Closed && upToDate at the post-read offset).
	closedSig := false
	if meta, mErr := st.Get(path); mErr == nil && meta.Closed {
		end := from
		if len(msgs) > 0 {
			end = msgs[len(msgs)-1].Offset
		}
		closedSig = end.Equal(meta.CurrentOffset)
	}

	frames := framesFromMessages(msgs)
	out := storeOutput{status: stoOK, readFrames: frames, upToDate: upToDate, readClosed: closedSig}
	rec.recordOp(clientID, readIn(path, from.ByteOffset), out, callNs)
}

// storeDoGetOffset records GetCurrentOffset -> tail (INV-LIN-01 tail read).
func storeDoGetOffset(st *redisstore.Store, path string, clientID int, rec *recorder) {
	callNs := rec.now()
	cur, err := st.GetCurrentOffset(path)
	if err != nil {
		return
	}
	rec.recordOp(clientID, getOffIn(path), storeOutput{status: stoOK, offset: cur.ByteOffset}, callNs)
}

// storeDoClose records CloseStream -> fresh-flip or already-closed (INV-CLOSE-01).
func storeDoClose(st *redisstore.Store, path string, clientID int, rec *recorder) {
	callNs := rec.now()
	res, err := st.CloseStream(path)
	if err != nil {
		return
	}
	storeAlreadyClosed.Store(true)
	status := stoClosedFresh
	if res.AlreadyClosed {
		status = stoClosedAlrdy
	}
	rec.recordOp(clientID, closeIn(path), storeOutput{status: status, offset: res.FinalOffset.ByteOffset}, callNs)
}

// framesFromMessages reconstructs the (clientId, opSeq) tags from the returned
// message bytes. The driver encodes each frame's identity in its payload so the
// reader recovers the exact tag the writer stamped — the Elle-recoverability
// idea that makes the read step compare frame IDENTITY, not just length.
func framesFromMessages(msgs []store.Message) []frameTag {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]frameTag, len(msgs))
	for i, m := range msgs {
		out[i] = frameTag{clientID: decodeClient(m.Data), opSeq: decodeSeq(m.Data), nbytes: uint64(len(m.Data))}
	}
	return out
}

// isIndeterminate reports whether an append error leaves the commit outcome
// unknown: a context deadline (partition/timeout) or the bounded-retry
// contention exhaustion in store/redis/store.go.
func isIndeterminate(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		isContentionExhausted(err)
}
