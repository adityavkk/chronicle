# Chronicle formal-first contract

This directory is executable design input for the future Rust implementation; no Rust
code depends on it yet. `tla/` model-checks bounded distributed traces and `lean/` proves
unbounded facts about the deterministic apply function. Run `make check` (Java, curl,
and Lean 4.31.0 via elan are required).

## Abstraction mapping

The Raft log supplies the only durable total order. Its **term and committed membership
are the ownership fence**: `owner` is the current leader, `term` is its term, and
`members`/`learners` are committed configuration state. There is deliberately no second
lease or ownership authority. An accepted authority operation records issuing node/term
and the then-current node/term; `AcceptedAuthorityCurrent` compares that history, so
turning `OwnerFence` off has a real counterexample. Learners record an exact caught-up
log index and promotion requires it to cover the commit index at promotion.

`log` is committed Raft entries and may contain duplicate proposals. `events` is the
successfully applied visible stream and must have unique producer identities. `completed`
and `results` model durable client outcomes (including deterministic rejection), distinct
from successful effects. Exact retries join/return the original result in the faithful
configuration; the apply layer still deterministically suppresses duplicate committed
entries. Producer rules reject lower sequences and epoch regressions.

Payload lengths are modeled explicitly, and visible offsets are cumulative bytes rather
than Raft indexes. Lifecycle application prevents Close from changing Deleted and Create
requires exactly `incarnation + 1`. Expiry is an ordinary committed Delete-like command
carrying the expected incarnation; snapshot state includes `expiryInc`, so delayed expiry
cannot delete a recreated stream.

## Checked configurations

* `Safety.cfg`: all bounded protocol actions, 1 node/producer and 1 committed entry.
* `Placement.cfg`: 2 nodes, owner history, indexed catch-up and promotion.
* `Scenario.cfg`: a constrained feasible trace with **4 committed writes** (3 unique plus
  a duplicate), byte lengths 1/2/3, offsets 0/1/3/6, and 2 producers; checks eventual
  application and exactly 3 visible effects.
* `Liveness.cfg`: weak fairness of apply.
* `BadAck.cfg`, `BadOwnerFence.cfg`, and `BadPromotion.cfg`: intentional mutations. The
  Makefile requires TLC exit code 12 **and the named invariant violation**, rather than a
  generic nonzero result that could conceal parser/configuration errors.

Lean proves cumulative payload-byte offsets, prefix replay, stale-incarnation and closed
writes, exact/lower-sequence retries and epoch regressions as no-ops, Close-after-Delete,
delayed expiry, and an exact retry after another producer's interleaved write. The proofs
contain no `sorry`, `admit`, or custom axioms.

This does not prove Raft, storage fsync correctness, serialization, integer overflow, or
refinement to future Rust. Implementations must atomically persist applied state, producer
and outcome dedup records, lifecycle/expiry metadata, and `last_applied`; snapshots must
capture exactly that committed boundary before log truncation.
