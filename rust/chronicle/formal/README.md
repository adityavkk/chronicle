# Chronicle formal-first contract

This directory began as executable design input before the Rust implementation.
`tla/` model-checks bounded distributed traces and `lean/` proves
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

## Committed file projection — checked before adding the read path

`Projection.tla` explores two incarnations, up to two committed bytes chosen from
two values, partial copying, publication, reads, crashes and restart: 3,168 states.
`authority` maps to the durable applied SQLite stream, never a local file length.
`BeginCopy`/`CopyByte`/`Publish` map to blocking-actor materialization followed by
publishing a usable file range. The actor serializes this with state changes.
`Read` captures bytes and authoritative frontier together; an already opened range
must remain bounded even if later appends extend the file. Lifecycle/snapshot
replacement must use a new inode, never truncate a file held by an existing reader.

Restart discards projection metadata and reconstructs from SQLite; a file is not
proof of committed content. `BadProjectionRestart` trusts a corrupt file's size;
`BadProjectionIncarnation` reuses a published generation after recreation. Both
produce retained `DataFromAuthority` counterexamples. These are bounded publication
checks, not a proof of OS inode semantics, power-loss behavior, or Rust refinement.
The required implementation tests cover partial files, restart corruption,
incarnation/snapshot replacement, concurrent range lifetimes, JSON boundaries,
and truncation during response delivery. Liveness still depends on responsive
local storage and eventual quorum; this cache model asserts safety only.

The implemented cache in `src/projection.rs` is bounded and rebuildable. Range
opening checks captured incarnation/frontier and a process-local snapshot token
against current authority; intervening lifecycle/snapshot changes return a
retryable read error instead of substituting another view. `tests/projection.rs`
checks that fence, old-reader lifetimes, independent seeks, restart reconstruction,
JSON boundaries, truncation errors and cancellation with a queued blocking read.
Raw binary and comma-delimited JSON bytes follow pinned Electric 0.1.5. Axum uses
bounded file reads with explicit unexpected-EOF errors; its body API does not
expose Electric's raw plaintext socket for `sendfile`, so this is not zero-copy.

## Implementation mapping and remaining gaps

`src/model.rs::State::apply` is the deterministic state machine. `storage.rs` commits
SQLite WAL/FULL transactions before publishing cached state or calling `LogFlushed`;
`main.rs` awaits OpenRaft `client_write` before success and `ensure_linearizable`
before strict reads. Snapshot install atomically replaces streams, producer results,
membership and applied index. These are reviewed correspondences, **not mechanized
refinement**. The formal model is smaller than the implementation:

* The original `Chronicle.tla` outcome cache includes rejections. The retrospective
  `Retention.tla` refinement instead models the implemented policy: incarnation and
  epoch fences precede lookup; only successes are retained; each old sequence returns
  its original byte frontier despite later writes and payload changes; a gap consumes
  nothing; a new epoch and delete/recreate clear old results; and snapshot/recovery
  carries the success table. `RetentionScenario.tla` runs a deterministic trace of 14
  transitions (15 states), including a retry immediately after recovery that must
  return the original frontier. `BadRetention.cfg` changes duplicate
  replies to the latest frontier and reaches a six-state
  `OriginalFrontierReplies` counterexample. This is a retrospective policy refinement,
  not evidence that the refinement preceded the implementation.
* `bridge/retention_trace.tsv` is the tabular projection of those scenario phases and
  `tests/formal_retry.rs` replays it through the real `State::apply`, including a serde
  state round trip for snapshot/recovery. The fixture and TLA scenario are reviewed
  side-by-side rather than generated from one common executable AST, so this catches
  Rust behavior drift but not transcription drift between those two files. Lean proves
  original-frontier insertion, preservation across later successes, and incarnation/
  epoch-before-cache ordering without `sorry`, `admit`, or custom axioms.
* Production permits 100,000 retained successes per stream and rejects the next one
  without eviction. The bounded model's `CacheBound` checks its three-sequence domain;
  it does not enumerate 100,001 writes or prove Rust allocation/overflow behavior.
* Learners catch up to an observed committed read-barrier boundary before OpenRaft
  performs safe membership change. The abstract model's instantaneous current-commit
  promotion condition is stronger. OpenRaft, not that simplified predicate, supplies
  membership safety while writes continue.
* `Ownership.tla` was added **after** the live overlap failure. It explicitly models
  two processes loading and later publishing cached state through one durable node
  identity. Twenty reachable states preserve acknowledged retention with exclusive
  ownership; `BadOwnership.cfg` reproduces lost acknowledgement without it. It is a
  retrospective ownership-assumption check, not a consensus proof.

The deployment assumption is one running process per node identity, one unmodified
local volume, and no cloned identity/volume. `Worker` locks the same persistent inode
before opening SQLite; the binary additionally locks the data directory and validates
a local identity file and all five stores before starting any Raft group. Locks are
never unlinked. They **cannot fence independent volume copies or hostile filesystem
mutation**. Normal restart fails closed on missing stores; explicit genesis and
fresh learner admission are separate operations. Admission durably rejects reused IDs
and addresses. The identity record is local and never installed from a Raft snapshot.
Each RPC carries the intended cluster and node ID; the receiver checks its persisted
identity before passing append/vote/snapshot to Raft. A DNS alias for another replica
therefore cannot count as an independent voter. This is routing validation on a trusted
network, not authentication against a malicious sender.

## Leadership policy (specified before implementation)

This is an opt-in experiment (`CHRONICLE_EXPERIMENTAL_CAMPAIGNS=1`), default off.
It does not qualify as the requested resource-informed leadership balancing.
Automatic replica placement and repair operate independently of this switch.

OpenRaft 0.9.25 has no directed transfer API. The controller will use its native
`trigger().elect()` on the preferred voter, not alter terms or remove/re-add
members. This can interrupt availability; it is not a zero-downtime handoff.
For each completed placement, the preferred voter is the sorted voter at
`shard % voter_count`. This derives the intent from replicated placement state
without another mutable authority. It balances leader counts, not measured load.

Campaign only while applied membership is the uniform intended voter set, this
node is the preferred voter and not draining, and another leader is known. Require the same
placement generation, term and leader for 30 monotonic seconds first. Reset the
cooldown **before** calling the native trigger, including unknown outcomes.
Restart, ineligibility or observation changes require a fresh interval. This
does not wait for a particular log index: Raft's native voting/log-up-to-date
rules remain authoritative, including a membership change racing the check.
The interval bounds trigger submissions, not election starts: `elect()` enqueues
a command and does not report election success. Cancellation cannot retract a
queued command. A delayed command may execute after eligibility changes.

`Campaign.tla` checks this cooldown in 711 bounded states. Its `clock` maps to
Rust `Instant`, `view` to `(generation, term, leader)`, `observed/since` to the
controller's per-group observation, and `Campaign` to resetting `since` before
awaiting the trigger. `Restart` forgets the observation; ineligibility uses the
same reset. `BadCampaign.cfg` omits that pre-trigger reset and violates
`CampaignSpacing`. Unit tests must cross the interval boundary, unknown/repeated
attempts, changed views and restart. There is no mechanized refinement or claim
that this policy proves election safety/liveness; those rely on OpenRaft and
eventually connected healthy voters. No resource-informed balancing claim follows
from this count-based preference.
