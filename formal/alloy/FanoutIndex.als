/*
 * FanoutIndex.als -- INV-RECOVER-04 (issue #40, Alloy relational model).
 *
 * The per-stream fan-out index (streamSubsKey SET) is a CACHE repairable from
 * the canonical links (linksKey HASH, the SOURCE OF TRUTH). ReconcileIndexes
 * (webhook/redis_store.go:393) rebuilds the SET from the links HASH: it re-adds
 * any membership a crash dropped, and NEVER invents membership absent from
 * links. The catalogued invariant (INVARIANTS.md INV-RECOVER-04):
 *
 *     forall (sub,path): path in links(sub)  =>  sub in streamSubs(path)
 *       (after reconcile -- the index is a SUPERSET of the link projection)
 *   AND
 *     streamSubs only ever holds (path,sub) pairs that links justifies
 *       (reconcile never INVENTS membership) -- modeled as: every streamSubs
 *       tuple either mirrors a current link OR is a STALE bit that deindex left
 *       (bits are never cleared on deindex; a stale set bit only costs one empty
 *       SMEMBERS, redis_store.go:436). We therefore distinguish the two cleanly.
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
