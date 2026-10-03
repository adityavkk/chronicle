/*
 * FanoutIndex.als -- INV-RECOVER-04 (issue #40, Alloy relational model).
 *
 * The per-stream fan-out index (streamSubsKey SET) is a CACHE repairable from
 * the canonical links (linksKey HASH, the SOURCE OF TRUTH). ReconcileIndexes
 * (webhook/redis_store.go) rebuilds the SET from the links HASH: it re-adds
 * any membership a crash dropped, and NEVER invents membership absent from
 * links. The implementation applies the transpose as pipelined, chunked
 * SADD/SETBIT batches (indexStreams), and a pass may stop after any chunk (a
 * failed Exec ends it); the second half of this file models one chunk as an
 * additive step and checks that the cut and the order of the chunks do not
 * matter (ChunkIsMonotoneAndJustified, ChunksCommute,
 * FullPassCoversAndIsIdempotent). The catalogued invariant (INVARIANTS.md
 * INV-RECOVER-04):
 *
 *     forall (sub,path): path in links(sub)  =>  sub in streamSubs(path)
 *       (after reconcile -- the index is a SUPERSET of the link projection)
 *   AND
 *     streamSubs only ever holds (path,sub) pairs that links justifies
 *       (reconcile never INVENTS membership) -- modeled as: every streamSubs
 *       tuple either mirrors a current link OR is a STALE bit that deindex left
 *       (bits are never cleared on deindex; a stale set bit only costs one empty
 *       SMEMBERS, deindexStream). We therefore distinguish the two cleanly.
 *
 * We model the canonical links as a relation Sub->Path and the index as a
 * relation Path->Sub (its natural transpose). A crash DROPS index tuples
 * (deindex / lost SADD); a Reconcile reconstructs the index as EXACTLY the
 * transpose of the current links. We check, over ALL configurations up to a
 * scope, that:
 *   (1) AFTER reconcile the index projection EQUALS the link projection
 *       (RebuildIsExactTranspose) -- nothing dropped survives, nothing invented.
 *   (2) AFTER reconcile the index is a SUPERSET of every current link
 *       (ReconcileCoversAllLinks, the catalogued forward direction).
 *   (3) Reconcile NEVER INVENTS membership: every post-reconcile index tuple is
 *       justified by a current link (NeverInventsMembership).
 *
 * The headline result is an ALWAYS-VALID assertion (check => UNSAT => holds for
 * every configuration in scope), not a single instance. We also `run` a witness
 * so the model is shown non-vacuous (a real drop-then-reconcile repair exists).
 */
module FanoutIndex

sig Sub {}
sig Path {}

/*
 * A State is a snapshot of the two relations:
 *   links      : the canonical Sub->Path membership (source of truth).
 *   streamSubs : the fan-out index Path->Sub (the cache / transpose).
 * Modeling each as a field of an explicit State lets us relate a PRE state
 * (possibly with a dropped index tuple) to its reconciled POST state.
 */
sig State {
  links      : Sub -> Path,
  streamSubs : Path -> Sub
}

// The index tuple (p,s) MIRRORS a link iff the canonical links has (s,p).
pred mirrors[st: State, p: Path, s: Sub] { s -> p in st.links }

/*
 * reconcile[pre, post]: post is pre after ReconcileIndexes -- the index is
 * rebuilt to be EXACTLY the transpose of the (unchanged) canonical links. This
 * is the relational meaning of "for each id, for each path in links(id),
 * SADD streamSubs(slot,path) id" run to completion over the whole keyspace,
 * with stale tuples cleaned (we model the correctness-critical re-add AND assert
 * the never-invent direction separately so a deferred stale-bit cleanup is
 * visible, not hidden).
 */
pred reconcile[pre, post: State] {
  post.links = pre.links                       // links unchanged (read-only source)
  post.streamSubs = ~(pre.links)               // index := transpose of links
}

/*
 * drop[pre, post]: a crash / lost SADD drops one or more index tuples while the
 * canonical links survive (the INV-RECOVER-04 fault). The post index is any
 * SUBSET of the pre index (membership can only be LOST, never invented, by a
 * drop); links are untouched.
 */
pred drop[pre, post: State] {
  post.links = pre.links
  post.streamSubs in pre.streamSubs            // a subset: tuples removed
}

// ---- the headline assertions (check => holds for ALL configs in scope) ----

// (1) After reconcile, the index is EXACTLY the transpose of the links: every
// dropped membership is restored and nothing extra survives.
assert RebuildIsExactTranspose {
  all pre, post: State | reconcile[pre, post] =>
     post.streamSubs = ~(post.links)
}

// (2) The catalogued forward direction: after reconcile, every canonical link
// has its index tuple (the SUPERSET property the low-latency wake path needs).
assert ReconcileCoversAllLinks {
  all pre, post: State | reconcile[pre, post] =>
     (all s: Sub, p: Path | s -> p in post.links => p -> s in post.streamSubs)
}

// (3) Reconcile NEVER invents membership: every post-reconcile index tuple is
// justified by a current canonical link.
assert NeverInventsMembership {
  all pre, post: State | reconcile[pre, post] =>
     (all p: Path, s: Sub | p -> s in post.streamSubs => mirrors[post, p, s])
}

// (4) The end-to-end self-heal: drop any subset of index tuples, then reconcile,
// and the index is fully repaired to the link projection -- a dropped entry
// self-heals via the reconcile (latency cost only).
assert DropThenReconcileSelfHeals {
  all s0, s1, s2: State |
     (drop[s0, s1] and reconcile[s1, s2]) =>
        (all sub: Sub, p: Path | sub -> p in s2.links => p -> sub in s2.streamSubs)
}

check RebuildIsExactTranspose   for 5
check ReconcileCoversAllLinks   for 5
check NeverInventsMembership    for 5
check DropThenReconcileSelfHeals for 5

// Non-vacuity witness: a genuine drop-then-reconcile repair really exists --
// a pre state with some link, a dropped index tuple, and a reconcile that
// restores it. (run => SAT => the modeled fault+repair is reachable.)
pred RepairWitness {
  some s0, s1, s2: State |
     some s0.links                       // there is canonical membership
     and drop[s0, s1]                     // a tuple is dropped
     and s1.streamSubs != ~(s1.links)     // the index is genuinely degraded
     and reconcile[s1, s2]                // reconcile runs
     and s2.streamSubs = ~(s2.links)      // and fully repairs the index
}
run RepairWitness for 5

/*
 * ---- the batched form (webhook/redis_store.go ReconcileIndexes/indexStreams) --
 *
 * The code never clears the index. ReconcileIndexes reads the links of a chunk
 * of subscriptions and ADDs their transpose in one pipeline (SADD + SETBIT per
 * link, idempotent), chunk after chunk, and a failed Exec ends the pass after
 * the chunks already landed. A Chunk is one such pipeline: `done` is the set of
 * links it carries (any subset of the links -- the cut is arbitrary, and on a
 * cluster the commands of one pipeline land per node in parallel, so within a
 * chunk no order exists either). indexChunk[pre, post, c] is its effect. A
 * pipeline that fails part-way lands a subset of its entries, which is itself a
 * Chunk. One entry's SADD and SETBIT are a single tuple here: a pair torn
 * between them (member without bit, or bit without member) is below this
 * model's atomicity -- the reader treats either as "not yet visible"
 * (StreamSubscribers) and the next chunk that carries the link re-asserts both
 * (indexStreams), so the tear is a transient the Go tests cover, not a state
 * these assertions speak to.
 */
sig Chunk { done: Sub -> Path }

pred indexChunk[pre, post: State, c: Chunk] {
  c.done in pre.links                          // a chunk carries current links only
  post.links = pre.links                       // links are read, never written
  post.streamSubs = pre.streamSubs + ~(c.done) // SADD/SETBIT: additive, idempotent
}

// (5) A chunk only adds, and every tuple it adds mirrors a current link: a pass
// that stops after any chunk leaves the index between the pre-state and the
// transpose, never with invented membership (the partial-failure safety).
assert ChunkIsMonotoneAndJustified {
  all pre, post: State, c: Chunk | indexChunk[pre, post, c] =>
     (pre.streamSubs in post.streamSubs
      and all p: Path, s: Sub | p -> s in post.streamSubs - pre.streamSubs => mirrors[post, p, s])
}

// (6) Chunks commute: landing c1 then c2 reaches the same index as c2 then c1,
// so neither the cut nor the arrival order of the pipelines is load-bearing.
assert ChunksCommute {
  all s0, s1, s2, t1, t2: State, c1, c2: Chunk |
     (indexChunk[s0, s1, c1] and indexChunk[s1, s2, c2]
      and indexChunk[s0, t1, c2] and indexChunk[t1, t2, c1]) =>
        s2.streamSubs = t2.streamSubs
}

// (7) A full pass -- chunks that together carry every link -- lands exactly on
// the pre-index plus the transpose (reconcile's post-state, plus whatever stale
// tuples the pre-index held: the deferred cleanup the header describes), so it
// covers every link; and a second full pass is a no-op.
assert FullPassCoversAndIsIdempotent {
  all s0, s1, s2, s3: State, c1, c2, c3: Chunk |
     (indexChunk[s0, s1, c1] and indexChunk[s1, s2, c2] and c1.done + c2.done = s0.links
      and c3.done = s2.links and indexChunk[s2, s3, c3]) =>
        (s2.streamSubs = s0.streamSubs + ~(s0.links)
         and (all s: Sub, p: Path | s -> p in s2.links => p -> s in s2.streamSubs)
         and s3.streamSubs = s2.streamSubs)
}

check ChunkIsMonotoneAndJustified   for 5
check ChunksCommute                 for 5
check FullPassCoversAndIsIdempotent for 5

// Non-vacuity witness: a pass really can stop after a chunk with a link still
// uncovered, and the next pass really covers it. (run => SAT.)
pred InterruptedPassWitness {
  some s0, s1, s2: State, c1, c2: Chunk |
     some c1.done and c1.done != s0.links      // a pass stopped after a partial chunk
     and indexChunk[s0, s1, c1]
     and some (~(s1.links) - s1.streamSubs)     // some link is still uncovered
     and c2.done = s1.links and indexChunk[s1, s2, c2]
     and ~(s2.links) in s2.streamSubs           // the next pass covers every link
}
run InterruptedPassWitness for 5
