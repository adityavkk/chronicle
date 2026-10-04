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

The producer map now distinguishes absence from a committed epoch-zero/sequence-zero
entry. The original total-map default conflated them: it rejected the first sequence
zero and admitted sequence one. This retrospective model correction adds proofs for
fresh sequence-zero acceptance at any epoch, fresh gap rejection, initial epoch-zero
acceptance, and absence after recreation. `initial` represents a successfully created
empty, open incarnation-one stream, not Rust's pre-create state. The existing Rust
retention fixture crosses the initial gap/zero boundary and recreation. No-op means
no effect on state, not a rejected HTTP result: a duplicate may succeed without an
effect. Write-offset monotonicity is non-strict and does not cover recreation, which
resets the offset. The effect and retained-result models remain separately proved;
their composition and Rust's capacity/finite-integer limits are not proved here.

This does not prove Raft, storage fsync correctness, serialization, integer overflow, or
refinement to Rust. Implementations must atomically persist applied state, producer
and outcome dedup records, lifecycle/expiry metadata, and `last_applied`; snapshots must
capture exactly that committed boundary before log truncation.

Permanent-volume-loss qualification treats the lost identity as stopped, not as
a Raft voter permitted to forget its durable state. The planned local schedule
stops one k3d agent, withholds its Chronicle PVC contents, and starts it with an
empty directory. Startup must refuse to serve or recreate databases. Two original
voters retain quorum; an existing, previously verified drained spare is made
eligible, and automatic learner catch-up/membership replacement must complete.
The withheld contents remain inaccessible to every Chronicle process through the
final strict read/history check. This exercises a volume-unavailability simulation
and the no-empty-store-restart boundary, not physical disk/power loss. Any later
restoration is a separately reported cleanup step, never evidence for retention.
No new consensus theorem is claimed: this relies on the existing crash-stop,
surviving quorum and safe membership assumptions, plus tested startup fencing.

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

`IncarnationAdmission.tla` precedes implicit HTTP recreation compatibility.
Requests with no `Stream-Incarnation` target the current lifetime at admission;
Create after a tombstone chooses its checked next incarnation. HTTP resolves
this once and submits an explicit incarnation in every command. Apply never
refreshes that binding. Explicit stale caller headers stay stale, even across
delete/recreate; the negative model refreshes at apply and violates the binding.
The existing Lean `stale_incarnation_write_noop` theorem proves the corresponding
unbounded deterministic write fence. Rust legacy commands with absent Create
incarnation still mean one on replay, rather than acquiring new semantics.

An implicit retry arriving after recreation is indistinguishable from a new
operation on that URL. Clients needing unknown-outcome retry safety across
lifetimes must retain and resend the original explicit incarnation (and producer
tuple for append). This is a necessary API distinction, not cross-lifetime dedup
or a claim that the server can infer intent from an identical headerless request.
Tests must cover normal implicit recreation, fixed queued-command fencing,
explicit stale retries, producer reset and legacy replay. TTL expiration is
the same tombstone transition, not a bypass of the incarnation guard.

HTTP metadata compatibility preserves the existing committed-command model:
config equality compares the ASCII-case-insensitive media type before `;`,
expiry policy and closure, while preserving the original content-type header.
This follows Chronicle's Go `ContentTypeMatches`, not a Unicode case fold.
JSON framing is selected by exact base media type, not a string prefix;
`application/jsonp` is not JSON. New configs persist the framing choice explicitly.
Legacy configs without it retain the original case-sensitive prefix rule: changing
classification on replay would reinterpret already acknowledged bytes. Config
equivalence also requires matching effective framing; a legacy misclassified
stream must not silently acquire a different framing through idempotent creation.
This is an upgrade boundary, not a payload migration or repair. Header decoding, required nonempty producer IDs
and POST content type are validation before the append proposal, not replicated
append attempts. Existing expiry maintenance remains a separate committed action.
Successful creation supplies Location from the request authority and path; it
does not read newer stream state to construct the response. Existing safety
proofs abstract payload/config classification: parser correctness and this
classification-to-code refinement require explicit Rust/live tests, not a claim
that TLA/Lean prove HTTP parsing. No consensus/lifecycle transition changes are
needed for this compatibility family.

`Expiry.tla` and `ChronicleFormal/Expiry.lean` precede sliding-TTL implementation.
Immutable policy (duration or absolute instant) is distinct from last access.
Only committed initial strict GET touches and append attempts renew sliding TTL;
HEAD, live-read continuations, idempotent PUT and explicitly stale reads do not.
Pre-admission rejections do not renew; append rejections after incarnation and
expiry checks may renew, so they are not no-effect operations for TTL histories.
Absolute instants never slide. Expiry commands recheck incarnation, observed
access and expiry at their supplied time; a delayed command cannot delete a
renewed/recreated stream. Clock samples are replicated inputs, not apply-time I/O.
Access uses max(previous, sample), and expired touch cannot revive a stream.
Snapshot/replay must preserve policy/access together; the TLA recovery stutter
assumes that contract, rather than proving serialization or Raft recovery.
Lean proves monotonic sliding deadlines, absolute stability and rejection of
stale expiry/access. Real wall-clock accuracy, drift bounds and Rust refinement
are not proved. Legacy `expires_ms` decodes as its original fixed absolute
deadline; its lost original TTL duration cannot be reconstructed or invented.

`RetryBackoff.tla` records the replication retry policy before its adapter fix.
Unavailable transport/HTTP/decoding endpoints map to OpenRaft `Unreachable`,
whose default replication-worker backoff is 500 ms, rather than `Network`, which
may immediately reschedule pending replication. The negative mutation permits
two failures without elapsed backoff. This is a two-state policy check, not a
proof of the library scheduler: a real-Raft HTTP failure-count regression must
bridge it to the adapter. Read-quorum probes and elections have separate caller
timeouts and are not rate-limited by this replication-worker policy. Snapshot
chunk retries use the pinned library's separate bounded-attempt policy.

`LeaderRetirement.tla` specifies the retained-leader cleanup fix before code changes.
OpenRaft 0.9.25 deliberately keeps a demoted leader leading while its node record
remains. `DeliverDemotion` maps to observing locally applied uniform membership
at or beyond the replicated placement's demotion boundary. `RemoveSelf` maps to
the controller's vote-fenced `RemoveNodes(self)`, serialized with movement and
revalidated against the completed placement generation. `ApplyRemoval` and `Tick`
are the library's committed removal application and subsequent step-down. Final
verified retirement still requires runtime Learner; eligibility to remove self
does not. The negative mutation waits for Learner before removing its node record
and violates eventual step-down. Liveness assumes a stable completed intent,
surviving voter quorum, responsive storage and fair reconciliation/Raft ticks;
it does not assert progress during sustained membership churn or quorum loss.
This focused five-state model neither proves Raft nor mechanizes Rust refinement.

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

## Long-poll response boundary (specified before implementation)

`LiveRead.tla` preceded the implemented strict long-poll API (678 reachable states).
It assumes OpenRaft supplies committed application state and a safe strict read
barrier. `Observe` maps to a barrier followed by `read_info`; `from` is the fixed
resolved byte offset, including initial resolution of `offset=now` and clamping
a future numeric live offset to the initial tail, following pinned Electric.
`requestInc` binds the request to that initial incarnation. A later incarnation
must fail the pending request, never silently substitute a recreated stream.

`Reply` uses one captured frontier for both payload coverage and next offset.
An empty timeout must not advertise a newer frontier sampled after that capture:
the client could otherwise skip committed bytes. The timeout mutation deliberately
makes this mistake. The incarnation mutation permits a response from a replacement
stream. Positive checks cover close, recreate, append and connectivity changes
interleaved between capture and response. The existing projection model separately
checks that captured ranges come from the committed incarnation.

The implementation uses bounded admission and a five-second long-poll wait,
with apply notifications used only to wake readers. Every successful response,
including timeout/EOF, requires a strict barrier and a captured view; notifications
are not authority. A deadline wake must recheck data before returning an empty
response. Cursor/framing conventions follow the pinned Electric source;
stored-byte offsets exclude JSON response wrapper lengths.
Explicit stale-prefix mode remains a finite read, not a freshness promise.

This safety model does not prove notification delivery, wall-clock expiration or
bounded HTTP completion. Progress requires eventual connectivity, a responsive
storage actor and executor fairness. There is no mechanized model-to-Rust
refinement. Unit tests and the [retained k3d HTTP observations](../evidence/LONGPOLL.md)
cross timeout/append races, incarnation replacement, EOF and loss of quorum before
a response. These observations are not input to the general Porcupine adapter.

## SSE publication (specified before implementation)

`SseRead.tla` specifies repeated strict observations, data delivery and control
publication, including appends, close, recreation, connectivity and cancellation.
Raft and the projection model supply committed range authority. `Observe` maps
to a strict barrier and captured incarnation/frontier. `Data` finishes encoding
that captured range; only then may `Control` expose its next stored-byte offset.
Wakes and 15-second heartbeats request another observation, not an unguarded tail
sample. `BadSseCoverage` samples a later tail for control; `BadSseIncarnation`
substitutes a recreated stream. Checks are bounded and do not prove client receipt,
HTTP encoding, notification delivery, executor fairness or Rust refinement.

The HTTP implementation in `src/sse.rs` follows the pinned Electric SSE event format:
data then control, JSON arrays/text/base64, initial caught-up control, closed
control without cursor, and a 60-second connection lifetime. It retains the initial
incarnation and resolves `now`/future offsets once. A post-header read error or
lost quorum aborts the body without a new control offset. `upToDate` describes the
captured strict view, not freshness after another concurrent write. Idle lifetime
expiry needs no new barrier because it emits no new data or cursor.

`src/sse_wire.rs` streams bounded chunks, preserving UTF-8/base64 boundaries and
escaping CR/LF SSE field injection. It deliberately emits `data: ` rather than
upstream's `data:` so an SSE parser does not strip a payload's leading space.
A slow consumer does not buffer an entire backlog or spawn a producer queue.
General and live admission remain held through delivery and any detached blocking
work, on the leader and forwarding ingress. Unit tests cover encoding boundaries;
`tests/sse.py` and `tests/sse_faults.py` exercise real HTTP and storage boundaries.
The lifetime is an application deadline, not a transport shutdown guarantee for a
consumer whose socket stops polling the body. These tests do not close the formal
model-to-Rust refinement gap.

## Unsupported HTTP semantics are pre-admission rejection

Until implemented, PUT fork/absolute-expiry headers and POST `Stream-Seq` must
return 501 before body extraction, forwarding, a read barrier or any store call.
The model mapping is a stuttering step: no command is submitted and no lifecycle,
payload or producer state changes. Silently dropping such headers and submitting
a different command is not a refinement of the requested operation. This gate
does not make those features conformant; their state transitions remain missing.
Middleware tests must distinguish rejection from request admission and prove a
missing request body cannot delay it. HTTP parsing itself is not formally proved.

## Pending writes retain admission after HTTP cancellation

`Admission.tla` separates the HTTP lifetime (`active`) from outstanding local
Raft proposal tracking (`pending`). `Finish` includes timeout, disconnect and
normal response completion; it cannot release the last permit while the proposal
is pending. `Complete` means the local Raft completion waiter terminates, not
necessarily that the operation was rejected or even that its durable log entry
can never apply. Timeout remains an unknown outcome in the stream history model.
The negative mutation releases admission at timeout and violates `PendingOwned`.

Implementation mapping, specified before the fix: a detached completion task
owns a clone of the request admission guard and awaits `client_write` without an
HTTP deadline. The caller may time out or disconnect without cancelling that task.
The same ownership is needed for expiry proposals initiated by reads. Bodies and
commands remain size-bounded. This bounds outstanding proposals from the public
stream endpoint, not trusted administrative RPCs, library-internal queues, total
disk usage or a cluster's globally aggregated requests. Runtime shutdown ends local
tracking without asserting a no-effect result. No fairness or eventual quorum is
assumed for this safety check; without quorum all slots may remain occupied.
This is a bounded resource model, not a mechanized refinement proof.

Review identified the same cancellation boundary for `ensure_linearizable`:
it submits a barrier to the core queue before awaiting a response. The strict
read path now retains guards in a detached barrier waiter too. `Submit` and
`Complete` can also represent this local waiter; no application mutation is
asserted for a read barrier. The forwarding metadata actor queue remains separately
bounded, and this model does not claim every internal Raft allocation is covered.

## Replica retirement is distinct from quorum repair

`Retirement.tla` specifies the cleanup boundary before implementation. Raft is
assumed to supply committed uniform replacement and durable membership delivery.
Repair must not require the removed node to be reachable. Temporarily retained
learners can receive their nonvoter configuration; unreachable learners can be
pruned without claiming that their local process learned its demotion. Registered
identities outside desired voters remain a durable, derivable cleanup obligation:
on return an obsolete voter can be added only as a learner, demoted, then pruned.
The existing placement record retains per-identity history: possible voter, or
the committed demotion boundary after its last possible promotion. New intents
mark their target identities as possible voters before consensus membership work.
Completion records fill demotion boundaries without advancing already-retired
identities unnecessarily. Legacy records conservatively include every registered
identity; the registry never deletes identities. Missing identities in known
history are genuinely untouched, unlike identities missing from legacy history.
This metadata change requires an offline upgrade; mixed revisions are unsupported.

Implementation mapping: `change_membership(target, true)` implements Repair;
replication/snapshot install supplies Deliver; a peer's atomic Raft metrics with
uniform nonvoter membership at or beyond the recorded demotion boundary, and
last-applied covering that membership log entry, supplies Observe under this
store's durable-apply contract. Version 0 in the model is an older learner state,
1 a delayed promotion, and 2 the required demotion. Separate negative mutations
incorrectly accept an unreachable peer or stale learner metadata. An untouched
group additionally requires recovery-confirmed empty state; that implementation
case is not modeled by this single previously-assigned-replica episode.
`RemoveNodes` prunes
nonvoters, never active voters. Probe/catch-up failures do not undo completed voter
replacement. Completion of placement alone still does not certify graceful node
retirement. Reconciliation must serialize membership work, check current intent
and leadership, and limit returning-replica probes/additions rather than starting
full-data learners for every historical node at once.

The model fixes the desired voter set for one cleanup episode. A changed placement
supersedes that episode; OpenRaft rejects removal of a current voter. No formal
claim is made about concurrent controller refinement, transport honesty, or
retirement without eventual connectivity and durable storage progress. Copies of
an old volume under an already-live identity remain unsupported.
The positive configuration separately checks eventual voter repair under weak
fairness of Repair and the stated available-quorum assumption. It does not claim
eventual retirement while the old replica remains unreachable.

`Drain.tla` separately models a controller's cached eligible-node observation
racing a committed draining registration. New `Place` commands check eligibility
at deterministic apply, before changing placement/history. Thus an earlier intent
blocks retirement, while a later stale intent is rejected. The negative mutation
omits that guard and reassigns a node after retirement. The model excludes an
explicit operator undrain, which deliberately permits future assignment.
The command's default-false `eligible_only` field preserves old committed-log
replay; new controllers always set it. This is not a configurable placement policy.

## Membership admission must fence the originating vote

`MembershipAdmission.tla` precedes the next implementation change. It separates
two races not covered by the earlier fixed-episode retirement model:

* A paused controller may resume after its process loses and regains leadership.
  A separate metrics check cannot prevent its old target being admitted in the
  new term, even after another leader completed a successor placement. An atomic
  expected-**Vote** check at RaftCore admission must cover both membership phases,
  AddNodes and pruning, without altering upstream quorum/commit rules.
* Cancellation drops the operation future, not an already-enqueued membership
  entry. Applied voters may still equal a replacement target while an older joint
  entry is outstanding. OpenRaft's strict-read barrier covers committed/noop state,
  not that outstanding tail. A completed target-membership operation is required
  even when applied voters already match; InProgress prevents premature completion.

The bounded positive check has 28 states. Separate negative mutations remove each
boundary and violate `CompletedAuthority`. The delayed-admission action represents
either phase's common core gate; it is not a full model of the consensus engine.
The completion action assumes upstream serialized membership admission and durable
commit, and must be bridged by deterministic library/controller regression tests.

With a single timeout-owned controller per process, dropping a future prevents
unsent phases; queued phases precede the next membership barrier. Other-leader
requests have different votes. This permits older work to settle after a newer
intent, but not to reassert itself after the newer placement completes. It does
not promise immediate cross-group revocation at intent commit, recovery without
the applicable joint quorums, or safe additional concurrent membership writers.

## Replacing a stalled intent must preserve authority history

`PlacementIntent.tla` specifies the next control-state change before implementation.
It explores two shards, four registered identities, three-voter configurations and
three generations. `Place` maps to `State::apply(Command::Place)`: generation CAS,
only one pending shard, and union of old/new possible-voter identities. A new
default-false `repair_pending` command field will permit replacing the *same*
pending shard without changing replay of previously committed commands. `Complete`
maps to generation-checked `Placed`, assuming an applied uniform membership
response obtained through the separately checked membership-admission boundary.

Four negative configurations remove repair availability, single-movement gating,
completion-generation checking or old-identity retention. Health observations,
cooldown and failure-domain preferences choose targets; none authorizes quorum
reduction or forgetting a possibly promoted replica. The controller will replace
a pending target only when one of its members is unavailable/draining, a distinct
three-node eligible target exists, and cooldown has elapsed. Other moves wait.

The temporal check assumes weak fairness of membership completion and bounded
intent changes. It does not infer eventual quorum, disk or network progress from
health checks. In particular, loss of an applicable joint quorum can still block
completion even after a successor intent is accepted. The models are separate
abstractions, not a mechanized composition or end-to-end Rust refinement proof.

## A terminal storage failure stops the whole node

Specified before implementation: any group's OpenRaft `Fatal::StorageError`
causes process exit, including learners and control group 0. All five groups
share a node/PVC failure domain. Keeping the other groups alive can strand their
controller behind the failed local control group and advertise a permanently
broken learner as healthy. Loss of quorum, normal elections, uninitialized
learners, application rejections and `Fatal::Stopped` do not meet this predicate.

This maps to the existing crash transition, not a new consensus operation:
unacknowledged operations remain unknown, persistent state is unchanged by the
monitor, and restart uses the existing identity/stores without bootstrap, deletion
or reset. SQLite transactions and quorum persistence, not destructors, supply
recovery. Other groups may have in-flight operations when the process exits.
The refinement assumption is that the executor eventually observes a published
storage-fatal metric; a storage call that hangs forever need not publish one.
No bounded failure-detection or eventual repair theorem follows from this policy.

Each group watcher inspects its initial and changed metrics, without a storage
read or quorum barrier. The fatal path logs best effort and exits without waiting
for blocking file readers, store shutdown or telemetry flush. A normal async-main
return is insufficient: Tokio runtime teardown can wait forever for a blocked
file-read task. Health checks use the same predicate during the interval before
exit. Tests must distinguish typed storage failure from other terminal states,
cross the actual core-to-metrics boundary, and verify subprocess exit while an
unrelated blocking task remains gated. This is a reviewed model-to-code mapping,
not mechanized refinement or evidence of power-loss durability.

## Successful POST status is a projection of a committed result

Specified before the status correction: ordinary POST and empty close-only POST
return 204, while a new producer POST with a body returns 200; duplicates return
204. PUT's 201/200 and DELETE's 204 are unchanged. This is only HTTP encoding of
the already committed outcome, per protocol sections 5.2 and 5.2.1: no new
transition, read of later mutable state or pre-commit success is permitted.
Tests distinguish non-producer data, new producer data, duplicate producer data,
and empty close-only with and without a producer. Body bytes, frontiers and
retained producer effects must remain unchanged.

## Closure response metadata belongs to the apply boundary

`WriteReply.tla` precedes adding closure response metadata. `Apply` abstracts
Raft's committed deterministic application; `boundary` maps to a closure field
captured in `Outcome`, not a later store read. A duplicate producer tuple retains
its old effect even when a retry changes the requested close flag. Its offset
remains the original cached frontier, while its closure metadata describes the
state at duplicate application. Existing incarnation/epoch fences still precede
lookup. Historical success retention is not narrowed to the closing tuple.

The two negative controls echo request intent or sample mutable state after
application; both must violate `ClosureFromApply`. Concurrent closure/recreation
can change current state before HTTP encoding. The bounded model covers this
publication boundary, not the full producer/lifecycle composition or Rust
refinement. Existing Lean no-effect/lifecycle proofs remain scoped as before.

The accompanying closure-policy corrections are deterministic: idempotent PUT
must compare current closure as part of config matching; an empty non-producer
close on an already closed stream succeeds without effect; other new appends to
closed streams still fail. Successful stream outcomes and closed errors capture
the applied closure and relevant byte frontier. Case-insensitive `true` controls
the request flag; other values are ignored. Snapshot/reopen and HTTP tests must
cross changed-flag duplicate retries, old-frontier retries after closure,
ordinary repeated close, and create closure mismatch, without weakening fences.

Review exposed an early HTTP-validation gap: a retained empty producer close
retried with false/omitted closure must still reach result lookup. Before fixing
that path, the command contract adds default-false `empty_body`, meaning original
HTTP-body emptiness before JSON framing. Missing fields in old logs must preserve
replay of zero-byte commands. POST JSON `[]` remains rejected by the HTTP encoder
before Raft proposal (after HTTP admission); PUT JSON `[]` is allowed. Neither
policy changes here.
New commands reject empty/non-closing bodies only after existing fences and
retained-success lookup, before any state/producer mutation. Fresh rejection
does not consume a tuple; cached retries still succeed. Malformed-body errors
emit no closure metadata and are outside this publication model. Tests must
separate old zero-byte commands, rejected empty JSON appends, invalid fresh empty
requests and retained empty retries, including false/omitted close flags.

### Browser response boundary

All stream-route responses must include `X-Content-Type-Options: nosniff` and
`Cross-Origin-Resource-Policy: cross-origin`, matching Chronicle's existing Go
HTTP contract. The wrapper runs outside admission and body extraction so errors,
empty responses and streaming responses receive the same headers. It must not
poll or buffer a response body, release its admission guard or change status.
These are response decorations, not replicated state transitions; the state
models and durability assumptions are unchanged. HTTP tests, not TLC, verify
this boundary. The headers neither authenticate requests nor make the private
admin/Raft listener safe for public access; no CORS authorization is added.

### Producer response position

`WriteReply` now also checks `ProducerFromApply`: the highest accepted sequence
in a producer response is captured at apply. A retained sequence-zero retry
after sequence one therefore reports sequence one while retaining sequence
zero's original byte frontier. Concurrent appends or recreation before HTTP
encoding cannot rewrite that position. The bounded model abstracts epoch and
validation; two dedicated negative controls echo the request or sample later
state. Existing deterministic safety proofs are unchanged, and this is not a
mechanized refinement of the HTTP implementation.

The code mapping adds an optional epoch/sequence position to `Outcome`, populated
from committed producer state on duplicate success and fencing/gap errors, and
from the newly applied position on fresh success. No producer ID or payload is
copied into reply metadata. HTTP uses this captured position for producer headers
and sequence-gap diagnostics. Fenced epochs map to 403; a higher epoch starting
at nonzero sequence maps to 400; ordinary gaps remain 409. Expected sequence is
zero for a new epoch/producer or the current successor otherwise; received
sequence comes from the immutable submitted command. Rejections still change no
producer state and consume no tuple. Incarnation/epoch precedence, original
frontier retention, snapshot format and command replay remain unchanged.

Tests must distinguish an old retry from the current tail and highest sequence,
preserve a captured outcome across a subsequent epoch change, cross snapshot /
reopen, and fill a rejected gap before retrying it. The offline checker still
checks the same effects and domain errors; HTTP/schema-3 tests additionally
check metadata, since the effects checker does not certify response headers.

### Stream-wide ordering token

`Stream-Seq` is optional, per stream incarnation, and compared lexicographically
by bytes: `"10" < "2"`. Present-empty differs from absent. Absence leaves the
last token unchanged; presence must strictly advance it unless none was stored.
Producer/incarnation fences and cached-success lookup keep existing precedence.
A cached retry ignores a changed token and returns its original effect/frontier.
A token conflict returns 409 with the applied frontier and consumes neither
producer sequence nor epoch. Empty close-only acceptance updates the token;
idempotent re-close does not. Delete/recreate clears it. Successful PUT retry
does not change it. These follow pinned Electric's optional-string policy while
preserving this implementation's stronger historical-result retention.

Mapping, established before implementation: `Command::Append.stream_seq`
contains the supplied token; `Stream.last_seq` contains the committed token.
Both default to absent for legacy JSON replay. The deterministic apply guard
runs after retry/closure checks but before any payload, producer or token update.
Token storage counts toward the shard metadata bound, charging only growth on
replacement. SQLite apply and serialized snapshots include it atomically with
the rest of `Stream`; no sidecar or HTTP-local token is authoritative. HTTP must
not silently ignore an undecodable supplied token. Repeated `Stream-Seq` fields
are rejected, including equal values; a comma inside one value remains opaque.

`StreamOrder.tla` checks bounded token/dedup composition with four token ranks,
including empty and the asymmetric numeric spellings above. Negative mutations
parse numeric spellings or update the token during a duplicate. Lean proves
lexicographic transitivity over arbitrary natural-number lists (a superset of
byte lists), strict advancement on change, and preservation for absent,
duplicate and rejected tokens. These are application-level properties, not a
new proof of Raft, snapshot transport, or mechanized refinement of Rust string
comparison. Tests must cross snapshot/install/reopen, rejected epoch changes,
old duplicates, token absence/emptiness and metadata capacity. Existing offline
histories do not submit tokens; until extended, their checker does not certify
token ordering. Keep live token fixtures separate and explicitly scoped.

### Conditional read validators

Before implementation: ordinary GET validators bind the same committed view
used by `Store::read_file`, never a later metadata sample. The descriptor binds
resource identity, incarnation, start/end byte offsets, closure and framing;
append-only bytes within an incarnation make that descriptor immutable. A
SHA-256 digest is an opaque HTTP validator, not an integrity proof. Strict reads
still cross the leadership barrier, TTL renewal and range validation before
returning 304. Live reads do not use conditional replies. Cache-Control remains
no-store; validators support explicit client revalidation, not shared caching.
`CacheView.tla` checks capture/respond interleavings with append and recreation;
the negative mutation samples the tag after capture and must fail. Its scope is
descriptor/body correspondence, assuming committed-prefix and incarnation
safety already established separately. Hash correctness and Rust refinement
are tested/reviewed, not mechanized here.

### SSE wire compatibility refinement

Before changing encoding: `SseRead.tla` continues to require complete successful
data encoding before a control can advance the frontier. Coalescing bounded
data chunks with that following control does not change the transition order;
source errors must never emit the control. Flush once buffered output reaches
16 KiB (plus one bounded source chunk), rather than buffering a whole stream.
No transport-level atomic delivery guarantee follows from this coalescing.
Text fields omit the optional separator except when the payload itself starts
with a space, which needs an extra space to survive SSE parsing. The line-start
state crosses arbitrary UTF-8/source chunk boundaries. Existing split-boundary
and property tests check parser-equivalent output; the TLA model does not prove
byte encoding. SSE Cache-Control becomes no-cache; strict barriers are unchanged.

### Cross-shard fork commit: initial model and mapping

`ForkCommit.tla` assumes each action is a durable application of the owning Raft
group. Source/target availability can toggle independently. `Begin` maps to a
persisted source preparation that locks conflicting mutations and captures its
prefix; `Prepare` maps to unpublished target bytes and a capacity/identity
reservation. `Commit` records the irreversible source decision and retained
relationship atomically. `Finalize` publishes only from that decision. Timeout
is not abort. Deletion and idempotent parent release are separate transitions.
Both prepared records and decisions belong in SQLite state and snapshots, not
HTTP task memory. Apply persistence must include every modified record and its
applied index in one transaction. This is a pre-implementation mapping.

Positive safety checks cover 1,212 reachable states for one transaction and
three tail lengths. Negative controls remove the mutation lock, publish a target
before decision, or lose prepared state during recovery. Each must violate its
named invariant, not merely fail parsing. `ForkLiveness` separately assumes
eventual sustained availability of both groups and weakly fair reconciliation;
it checks settlement/publication, not availability during a partition. Lean
proves irreversible/idempotent decisions, copied prefix, exact boundary offset
and fresh writer state for arbitrary lists/natural offsets. It does not verify
the cross-shard protocol or byte framing. The initial model excludes competing
transactions, expiry, incarnation reuse and chained releases; those remain
required refinement/test work. No fork implementation is certified by this model.
