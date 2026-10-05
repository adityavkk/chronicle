# Checkpoints: accelerate folds without changing the event log

Research date: 2026-09-26. Implementation update: 2026-10-03. Chronicle now has
opt-in HTTP/storage support; the upstream protocol extension remains proposed.
Companion [Electric/SDK patches](../../experimental/electric-checkpoints/README.md)
implement opt-in integration against pinned sources. They are not merged or
released upstream, and these local results are not a production deployment.

## Recommendation

Add **versioned, application-produced state images bound to an exact stream
incarnation and consumed offset**, discoverable without reading the event prefix.
Restore an image, then consume the canonical stream from its saved offset. Keep
the log. Keep reducers out of the transport server. Keep subscription progress
independent of materialization progress.

The wire contract and rollout instructions are [SNAPSHOTS.md](../spec/SNAPSHOTS.md).
Chronicle implements it behind `--enable-snapshots` with a 1 MiB image limit,
same-slot atomic source/boundary/CAS validation, lifecycle cleanup, guarded reads,
and dedicated publication authority. Fork cuts must be root-owned boundaries;
experimental segment wrappers are not yet supported. Redis failover still needs
prefix-preservation guarantees or an explicit invalidation procedure.
The original folding experiment is [experimental/checkpoint](../../experimental/checkpoint/),
which the HTTP implementation does not use. The [ADR](../adr/0012-projection-checkpoints.md)
records the boundary. The [integration guide](../ELECTRIC-AGENTS.md#projection-snapshots-opt-in-upstream-patches)
separates the implemented bounded-input contract from application-specific rollout
and historical-observer work.
The vendored Durable Streams `PROTOCOL.md` remains byte-for-byte unchanged.

Four different operations are often called “checkpointing”:

| Operation | Saves | Solves | Does not establish |
|---|---|---|---|
| Projection snapshot | Materialized state + exact input position | Cold replay cost | Smaller historical log, exactly-once external effects |
| Consumer checkpoint | Position successfully handled/committed | Delivery recovery | Recoverable application state |
| Log compaction/retention | Selected history, or a retained suffix | Storage growth | Equivalence for every possible reducer |
| LLM context compaction | Summary + prompt watermark | Context-window/token cost | Exact agent state or faster StreamDB bootstrap |

The unpatched Electric revision inspected below has consumer progress and
model-context compaction, but its cold-start path does not load a materialization
snapshot.

## Prior art and what to borrow

These are primary documentation/source references, not assertions that all
systems provide the same feature. Documentation is live; the Electric source
findings below are pinned to revisions.

### Event-sourced aggregates: Akka, Axon, Kurrent, Marten

**[Akka Persistence snapshotting](https://doc.akka.io/libraries/akka-core/current/typed/persistence-snapshot.html)**
loads the latest selected snapshot and replays later events. Snapshots can be
triggered every N events or by a predicate. Akka stashes incoming commands while
saving mutable state, so asynchronous serialization cannot capture a moving
object. Snapshot saving failure need not stop the actor. Snapshot-load failure
can optionally fall back to replay, with an explicit warning that this is unsafe
after events have been deleted. Deletion occurs only after a successful save;
projections may still need the old events.

**Borrow:** complete-state serialization barrier, independently configurable
cadence, optional acceleration while history remains, no implicit retention.
For a browser/runtime that must keep consuming, freeze/copy state rather than
hold the agent's command path for an object upload.

**[Axon event snapshots](https://docs.axoniq.io/axon-framework-reference/5.0/tuning/event-snapshots/)**
describes event-count triggers, background snapshot creation concurrent with
appends, and aggregate snapshots. `RevisionSnapshotFilter` excludes snapshots
whose aggregate revision no longer matches; upcasting is an alternative that
requires appropriate filtering configuration. The referenced 5.0 page explicitly
says the feature is unavailable in 5.0 and planned for reintroduction in 5.1:
this is established design prior art, not a claim about 5.0 feature availability.
The [older implemented contract](https://legacy-docs.axoniq.io/reference-guide/v/2.4/repositories-and-event-stores.html)
states that a snapshot's sequence number equals the last event included.

**Borrow:** projection/serializer revision is part of snapshot identity. A
concurrent append need not invalidate a correctly captured older prefix. A
revision mismatch is a cache miss, not an invitation to deserialize permissively.

**[Kurrent/EventStoreDB's snapshotting study](https://kurrentdb.kurrent.io/blog/snapshots-in-event-sourcing/)**
compares a separate snapshot stream, snapshots in the source stream, external
stores/caches, and asynchronous subscription-driven production. Reading the last
event in a separate snapshot stream makes discovery cheap. It discusses the
write amplification of frequent snapshots, rebuilding after cache expiry, and
domain alternatives such as closing an accounting period and starting another.
Its examples use EventStore revisions; their arithmetic must **not** be copied
onto opaque Durable Streams offsets.

**Borrow:** snapshot as a replaceable optimization and an application-owned fold.
**Do not borrow unchanged:** a latest-snapshot index implemented as an ever-growing
ordinary Durable Stream. DS has no generic reverse-read/latest-record API; it
would make snapshot discovery another unbounded replay. Chronicle needs a direct
latest lookup. Appending a reset/snapshot event to the source alone also leaves
the entire byte prefix on the download path.

**[Marten live aggregation](https://martendb.io/events/projections/live-aggregates)**
supports folding into existing state with `fromVersion`, as well as aggregation
at a historical version/time. Its snapshot example writes at a domain boundary
rather than on every event. Marten distinguishes live aggregation from persisted
projections, which may be maintained inline or asynchronously.

**Borrow:** one reducer for full replay and snapshot-plus-tail, with explicit
version boundaries. Choose write-side maintenance only when its latency and
transaction coupling are justified. A read-side checkpoint worker is a better
first fit for Chronicle's byte-oriented append server.

### Stream processing: Kafka and Flink

**[Kafka log compaction](https://docs.confluent.io/kafka/design/log_compaction.html)**
retains at least the latest value per key, preserving surviving record offsets
and ordering. It is asynchronous, not a guarantee of exactly one row per key at
all times. Tombstones eventually disappear; reconstructing a correct full view
requires completing the scan within the deletion-retention window.

**Borrow:** separate the hot event tail from an economical recovery representation.
**Do not equate with a generic fold:** for `balance += delta`, retaining only the
last delta loses money. For State Protocol full replacement rows, latest-by-key
can reconstruct current values, but cannot necessarily reconstruct Electric's
original row pointers, stable first-insertion ordering, raw wakes, or historical
forks. Token deltas with unique keys barely shrink under key compaction.

**[Flink checkpointing](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/)**
captures operator state together with source positions. Barriers define a
consistent cut. A completed checkpoint, not an in-progress copy, is recoverable.
Asynchronous snapshots require immutable/copy-on-write state. End-to-end
exactly-once effects additionally need cooperating transactional sinks.

**Borrow:** state and cursor are one logical commit; never publish a cursor ahead
of serialized state. **Limit v1 to one source stream.** A multi-stream projection
needs a vector of positions and an application consistency contract, possibly
barrier alignment. Taking unrelated HEADs is not a transactionally consistent
snapshot. Chronicle snapshots do not make an LLM call or payment exactly once.

### Long-lived workflows and agents: Temporal and LangGraph

**[Temporal Continue-As-New](https://docs.temporal.io/workflow-execution/continue-as-new)**
passes relevant state as arguments to a new execution with the same Workflow ID,
a new Run ID, and a fresh event history. This bounds replay and helps long-lived
workflows adopt new code versions. It is a lifecycle transition, not transparent
replacement of an arbitrary log prefix.

**Borrow:** explicit epochs/sessions are an alternative when the domain permits
them. For an Electric entity, a new session with a carried summary could bound
active state, but changes addressing, observation, history, and fork semantics.
Do not disguise that as a lossless snapshot optimization.

**[LangGraph checkpointers](https://docs.langchain.com/oss/python/langgraph/checkpointers)**
persist graph state at super-step boundaries, including continuation information,
parent checkpoints, channel versions and per-task pending writes. Completed
parallel tasks can survive a sibling failure without rerunning. Its `sync`,
`async`, and `exit` modes have different crash-loss windows. Time travel reruns
steps *after* the chosen checkpoint, including their external calls.

The same documentation describes beta `DeltaChannel` support (requires
`langgraph>=1.2`): periodic full seeds plus ancestor deltas avoid repeatedly
storing a growing channel in every checkpoint. Recovery, pruning, and copying
must preserve the seed/ancestor dependency chain. Broken specific-ID lookup can
silently lose the reconstructed state.

**Borrow:** include hidden execution/projection metadata, not just user-visible
values; test restore semantics directly. **Defer:** incremental checkpoint chains
until measurements justify their GC/dependency complexity. Start with standalone
full images. Consider structurally shared immutable collection chunks later,
rather than unbounded delta chains.

### Electric's closest implementation is Yjs-specific

The inspected Durable Streams Yjs implementation stores a complete Y.Doc in a
separate snapshot stream, publishes its position to an index, restores the image,
then tails updates. It also has a `snapshot.available` subscription facade.
This is application-specific, not a generic State/Agents bootstrap protocol.

- [Compaction and publication](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/y-durable-streams/src/server/compaction.ts#L99-L190)
- [Client bootstrap](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/y-durable-streams/src/yjs-provider.ts#L400-L430)
- [Snapshot notifications](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/y-durable-streams/src/server/snapshot-subscriptions.ts#L3-L17)

[Issue #397](https://github.com/durable-streams/durable-streams/issues/397) reports
an offset arithmetic bug: snapshot code increments an offset component and can
resume inside a record. The inspected
[code](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/y-durable-streams/src/server/compaction.ts#L126-L145)
does perform that increment. **Use the exact returned next-read offset. Do not
add one, reconstruct it from event counts, or substitute a later HEAD.**

[Issue #404](https://github.com/durable-streams/durable-streams/issues/404) requests
retention/key compaction, noting that reset/snapshot markers alone do not bound
bytes downloaded. [Issue #349](https://github.com/durable-streams/durable-streams/issues/349)
asks about persisting materialized state to Postgres. The original
[State RFC #31](https://github.com/durable-streams/durable-streams/issues/31)
leaves persistence decoupled. These are evidence of the gap, not an exhaustive
guarantee that no other proposal exists.

## Electric runtime: source-level findings

Inspected revisions:

- Electric: [bb39742](https://github.com/electric-sql/electric/tree/bb397424db0e1c153dc356713fd3dfd40315470c)
- Durable Streams: [461b402](https://github.com/durable-streams/durable-streams/tree/461b40267aabd644558f9b19dbb9507dd5f691cf)

These are research pins, not changes to Chronicle's conformance or integration
version pins. The repository's older `0.6.3` deployment runbook is not assumed to
have every behavior in these newer sources.

### Cold activation replays; existing “checkpoints” compact prompts

`processWake` creates a new DurableStream, producer and EntityStreamDB per wake
([construction](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/process-wake.ts#L514-L573)),
then claims and calls `db.preload()` concurrently
([preload](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/process-wake.ts#L1164-L1214)).
StreamDB's `startConsumer()` calls `stream.stream({live, json: true})` without a
resume offset and applies the complete stream
([implementation](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/state/src/stream-db.ts#L653-L738)).
`observe(entity)` also builds/preloads a fresh DB
([client](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/agents-client.ts#L48-L58)).
Connection reuse within a wake is not durable cross-wake caching.

StreamDB exposes `offset`, `preload()` and `close()`, but no supported import/export
or resume-state option. `MaterializedState` is an in-memory map with `apply`,
`applyBatch`, `get`, `getType` and `clear`
([source](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/state/src/materialized-state.ts#L3-L93)).
Its `update` replaces the entire row; it is not a patch merge.

The [State Protocol](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/state/STATE-PROTOCOL.md#L203-L254)
defines `snapshot-start`, `snapshot-end`, and `reset`. StreamDB treats the first
two as hints; reset clears collections/key sets but does not seek the connection
to `reset.headers.offset`
([dispatcher](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/state/src/stream-db.ts#L353-L377)).
They do not provide atomic external image discovery or restoration.

Agent context compaction selects completed `context_inserted` markers, drops
model-history items at/below a watermark, and substitutes a summary. Running or
failed markers are ignored
([context projection](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/timeline-context.ts#L475-L560)).
`maybeStartBackgroundCompaction`, `writeBackgroundCheckpoint`, and
`failBackgroundCheckpoint` are its APIs
([signatures](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/context-factory.ts#L172-L188)).
This is downstream of complete materialization, so it does not solve cold replay.

The default LLM projection reconstructs user messages from processed inbox/wakes,
assistant text from deltas, and tool-call/result history from final call state.
Its `projection(item)` customization runs after materialization
([projection and ordering](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/timeline-context.ts#L201-L284),
[processed inbox](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/timeline-context.ts#L341-L394)).
Saving only this rendered message list loses custom state, manifests, inbox
status, replay watermarks, pending tools and fork information.

### Rows and a cursor are insufficient for exact hydration

The [dispatcher](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/state/src/stream-db.ts#L263-L341)
stamps `_seq = this.seq++` for recognized changes, including deletes. Unknown
types and control events do not increment it. Updates overwrite row `_seq`.
Therefore neither row count nor maximum surviving `_seq` determines `nextSeq`
after trailing deletions. Existing-key sets also determine upsert/replayed-insert
normalization. Transaction-ID state matters if restoring outstanding `awaitTxId`
semantics; for fresh activations, explicitly start with no outstanding local
optimistic transactions instead of persisting an unbounded seen-ID set.

EntityStreamDB maintains row EventPointers and stable `_timeline_order` side
tables. Its batch callback groups items by `headers.offset`, counts `subOffset`
from 1, and anchors each group at the preceding entry offset. After a batch it
saves `previousBatchOffset = batch.offset`
([pointer calculation](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/entity-stream-db.ts#L355-L433)).
The [EventPointer contract](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/event-pointer.ts#L1-L45)
warns about counting from arbitrary partial-entry starts. Preserve these tables
and anchor state; do not regenerate them by replaying synthetic snapshot inserts.

`JsonBatchMeta` contains `offset`, `upToDate`, optional `cursor`, and `streamClosed`
([type](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/client/src/types.ts#L213-L247)).
The stream sets `lastConsumedOffset` before dispatch, runs callbacks, then commits
collection writes
([ordering](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/state/src/stream-db.ts#L698-L725)).
Thus polling `db.offset` and serializing collections is not an atomic cut. Add an
after-commit hook. In SSE, wait for the control event covering its preceding data
events, not just a data callback
([client batching](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/client/src/response.ts#L541-L643)).
`upToDate` is historical, not permission to skip catch-up after restore; cursor is
a cache optimization, not the correctness position. Reestablish live readiness.

### Raw wake events have a separate recovery lifetime

During preload, `catchUpEvents` receives raw changes
([collector](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/process-wake.ts#L1129-L1161)).
Consumers include:

- Latest-per-key signal handling, dispatched after claim
  ([side effects](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/process-wake.ts#L724-L748),
  [post-claim call](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/process-wake.ts#L1223-L1228)).
- Wake combination, including multiplicity of raw changes rather than merely
  latest row values
  ([combineWakeEvents](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/process-wake.ts#L340-L374)).
- Inbox selection, cancellation and notification/fork reconciliation filters
  ([selection](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/process-wake.ts#L1608-L1685)).
- Handler-visible `ctx.events`
  ([context construction](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/process-wake.ts#L2195-L2226)).
- Setup/self-observation
  ([setup](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/process-wake.ts#L1720-L1735)).

Manifest rows suffice for parts of session/shared-state restoration
([wake-session](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/wake-session.ts#L329-L342)),
but not for every callback that observes raw history. `safeAckOffset` can advance
after batch acceptance or producer flush; it is **not** a materialized-state
serialization barrier
([offset handling](https://github.com/electric-sql/electric/blob/bb397424db0e1c153dc356713fd3dfd40315470c/packages/agents-runtime/src/process-wake.ts#L640-L697)).
Snapshot publication must never itself advance it.

## Proposed Electric integration

The corrected companion patches implement both default
`rawInputRecovery: 'full-replay'` and explicit `bounded-checkpoint-v1` recovery
under [ADR 0013](../adr/0013-bounded-electric-runtime-recovery.md). Bounded mode
restores durable completed progress and state, then reads only the entity-source
suffix on an aligned image hit. Setup, `ctx.events`, and self-observation receive
the post-completion delta; handlers must consume that entire delta. This is not
transparent equivalence for handlers that inspect arbitrary history. The
[integration guide](../ELECTRIC-AGENTS.md#projection-snapshots-opt-in-upstream-patches)
documents trust, export, migration and teardown requirements.

The earlier full-raw-replay bundle remains immutable measurement evidence, not
a deployment recommendation: its SDK update mode discards partial inbox fields,
including without snapshot recovery. The corrected patches preserve partial
updates. Historical observer bootstrap remains separate.

Implement in layers, in the Electric/DS repositories, rather than monkey-patching
private TanStack collection internals from Chronicle:

1. **`@durable-streams/state`: atomic bootstrap/export.** Add an optional bootstrap
   image and an after-committed-batch export hook to `createStreamDB`. Hydrate
   committed rows and dispatcher internals before opening the canonical stream at
   the supplied offset. Validate the complete image before changing live state;
   malformed hydration must discard the temporary DB and fall back to full replay.
   No optimistic actions, network writes, side effects, or readiness notification
   may occur while hydrating. `markReady` happens only after tail catch-up. Public
   export requires explicit committed capture and rejects synchronous uncommitted
   batch hooks; otherwise queued rows or advanced indexes can create a mixed-cut
   image. Default legacy sync visibility remains unchanged.
2. **`EntityStreamDB`: extended image codec.** Persist every built-in/custom
   collection with `_seq`, `_timeline_order`, row pointers, next sequence counter,
   existing-key state and previous-batch anchor. Version the codec together with
   schema, reducer behavior, and ordering algorithm. For a new activation there
   are no resumable in-process transactions. Keep any transaction-history API
   promise explicit rather than accidentally weakening it.
3. **`processWake`: state recovery plus event recovery.** Keep claim acquisition
   concurrent with preload; no handlers or signal effects before a valid claim.
   Bounded mode uses one committed state reader for both materialized state and
   raw delta. Select unfinished inputs by durable progress, never notification
   tail hints. Certify only a fully drained prefix after successful tracked
   effects; pending B prevents A's later output from acknowledging B. An image
   must align with that completion marker. Legacy full replay still supplies
   historical raw inputs independently; do not apply that prefix twice to state.
4. **Resolve all raw-history consumers before enabling fast preload.** A simple
   “resume from ack” is not proven sufficient for the pre-filter signal/setup
   paths above. The bounded contract uses a canonical completed-progress marker,
   rejects opaque explicit wake payloads without source identity, and requires
   write-token mediation for untrusted inputs. Keep full replay for applications
   that cannot meet those constraints. Materialized-state speedups alone do not
   prove end-to-end activation equivalence.
5. **Publish outside the append critical path.** Freeze the committed image at a
   complete batch boundary. Optionally flush producer writes and wait until the
   consumer has actually applied them before taking an end-of-activation image.
   Upload under a dedicated snapshot-publisher service principal, never a browser
   read token or ordinary append token. A failed save leaves the old image/log
   usable and does not determine wake completion.
6. **Agents-server proxy and apps.** Forward discovery, snapshot media type,
   conditional headers and incarnation guards; do not let the append proxy or
   credential overwrite accidentally authorize snapshot replacement. Later reuse
   the bootstrap in `observe(entity)`; establish event callback semantics there
   too, since hydrating rows does not replay historical callback events.

Original v1 design sketch, retained to explain the required data rather than as a
copy-paste API. The corrected bundle uses codec `electric-entity-image/v3`,
`projectionVersion`, tuple-array pointer indexes, all-item source positions, and
durable completed progress for bounded recovery:

```ts
type EntityImageV1 = {
  codec: "electric-entity-image/v1"
  schemaFingerprint: string
  nextSeq: number
  rowsByCollection: Record<string, unknown[]>
  rowPointers: Record<string, Record<string, {
    offset: string | null
    subOffset: number
  }>>
  timelineOrders: Record<string, Record<string, string>>
  previousBatchOffset: string | null
  // Existing-key indexes can be rebuilt from committed rows if schema-safe.
  // Pending raw inputs require their own codec/progress contract, described above.
}

// Design pseudocode; see the integration guide for the implemented API.
const image = await snapshots.load(entityURL, projectionVersion)
const db = createEntityStreamDB({
  stream,
  bootstrap: image && validateImage(image),
  afterCommittedBatch(cut) {
    // Synchronously freeze state and cut.offset before yielding to live input.
    maybePublish(cut.exportImage())
  },
})
await db.preload() // hydrate, then canonical suffix; still waits for up-to-date
// Recover pending raw wake inputs independently before invoking the handler.
```

The server stores opaque bytes, so it cannot check that the application fold is
correct. Snapshot publisher authority is correspondingly powerful. A read-only
principal must not be able to poison a shared projection.

### Forks, versions, and failures

- Key by source identity **and** incarnation **and** projection version. A fork
  gets its own identity. V1 does not automatically inherit a parent's snapshot.
  Inheriting later requires proof that its full cut precedes the exact fork point,
  including partial-entry sub-offsets and branch ordering metadata.
- Keep old and new projection versions side-by-side during rolling upgrades.
  Unknown version: full replay. Never silently reinterpret a summary as exact
  state. Data/schema migration is explicit and must preserve the cut.
- Writer crashes before publish: old snapshot remains. Lost publication reply:
  reload and compare, then retry conditionally. Slow writer: cannot overwrite a
  later boundary. Snapshot corruption: full replay, because v1 retains the log.
- Delete/recreate between snapshot GET and tail GET: incarnation precondition
  rejects the tail. No merging two different entities at the same URL.
- Multi-stream observations are separate views, not one consistent global cut.
- A snapshot doesn't contain a JavaScript stack, mesh identity, claim lease,
  producer epoch, bearer token, or permission to re-execute an external effect.

## Expected benefit and what to measure

Let \(N\) be historical events, \(K\) events since the image, \(S\) serialized
live state, and \(R\) pending raw inputs still needed by the runtime. Full replay
costs roughly \(O(N)\) event decoding/folding; restoration costs \(O(S + K + R)\).
This is **not** \(O(1)\) when \(S\) or \(R\) grows with \(N\). Image writes cost
\(O(S)\) per publication, assuming bounded per-event decode/apply work.

For a fixed-size aggregate and a checkpoint every \(K\) events, uniform activation
arrival sees approximately \(K/2\) replay events. With activation rate \(a\),
event rate \(e\), per-event fold cost \(f\), and image write cost \(w\), a simple
cost model is \(afK/2 + ew/K\), minimized at \(K = \sqrt{2ew/(af)}\).
Its square-root balance can guide experiments, but not set a universal default:
images grow, hot entities differ, and recovery SLOs impose an upper replay bound.
Use replay bytes/time plus minimum publication spacing, not event count alone.

Electric workloads should be measured separately:

- **Update-heavy state:** many updates to few keys; full-row images should win.
- **Token-heavy sessions:** mostly unique text-delta keys; state remains large.
  A future runtime projection can prejoin completed text and retain tool/inbox
  state, while loading audit/timeline data lazily. That changes the projection
  contract and cannot replace arbitrary `ctx.db` collections transparently.
- **Large pending wakes/offline consumers:** R may dominate; a snapshot cannot
  erase inputs the handler still needs.

Measure cold wake p50/p95/p99, source bytes, image bytes, fold CPU, deserialization,
heap, Redis commands/CPU, publication duration, hit/mismatch/corruption rate,
checkpoint age, and redundant writers. Compare full replay, exact collection
image, and (later) a versioned compact runtime projection. Include image creation,
read/write amplification, failed uploads and concurrent appends in the cost.
Do not report the small aggregate microbenchmark as an Electric activation gain.

### HTTP/Redis end-to-end measurements

The [2026-10-02 campaign](../../benchmarks/snapshots/README.md) measures the
implemented API using a Node client, separate Chronicle process and isolated
Redis. It covers 14 scenarios and 15,663 verified recoveries, retaining raw
latencies, CPU counters, transfer sizes and publication costs. These are synthetic
State-shaped workloads on one 8-CPU orb, not Electric runtime activations.

With 100,000 updates over 100 keys and a 100-event suffix, p50 was
**448.34 ms full replay versus 4.29 ms snapshot recovery**; body transfer fell
from 34,120,781 to 69,736 bytes. But 2,500 append-only rows regressed from
**11.56 to 18.08 ms**, and needing 50,000 raw inputs reduced the gain to
**454.61 versus 211.75 ms**. A roughly 808 KiB image cost **20.95 ms p50** to
publish. Short streams, stale snapshots, oversized-image fallback, version
misses, concurrent readers, append pressure and added request delay are all
reported, including the losing cases. See the campaign for p95 and methodology.

This supports workload-based eligibility and publication cadence, not universal
snapshotting or a promised Electric speedup. The SDK should check image size
before upload; the runtime must still establish its raw-input recovery contract.

The [2026-10-03 fixed-arrival campaign](../../benchmarks/snapshots/README.md#fixed-arrival-contention-campaign-large-image-publication-alongside-recovery)
adds simultaneous advancing 808 KiB publications, source/unrelated appends, bounded
recovery queues, captured-cut validation and sampled memory. All 848 completed
recoveries, 4,302 appends and 293 advancing publications passed. At one offered
recovery/s, both strategies sustained all offered work: scheduled-arrival p50/p95
fell from **553.50/588.29 ms to 21.16/26.56 ms**, with publication p95 around
18–20 ms. At eight/s, full replay overloaded the shared Node client pipeline;
missed arrivals and reduced achieved append load are reported, not hidden or
presented as Chronicle's maximum capacity. These remain synthetic projection
measurements, not `processWake` activation timings.

The raw-binary Redis layout also reduced measured `MEMORY USAGE` for the same
808 KiB image from **1,310,976 to 918,096 bytes** (about 30%). This is an
allocator-dependent representation comparison, not a latency A/B. Quotas now
bound each source to eight images and 4 MiB of bodies, with conditional retirement
to reclaim capacity. See the campaign for the raw evidence and reproduction.

### Electric runtime evidence is separate from projection-only speedups

The historical [full-raw-replay bundle](../../experimental/electric-snapshots/README.md#real-processwake-e2e-and-cold-activation-campaign)
exercises the actual signed-webhook → agents-server → `processWake` →
Chronicle callback path, with real Postgres and Electric shapes. It compares
replay and snapshot recovery with exact handler-visible state, raw inbox inputs,
persisted replies and acknowledgments. A real SIGINT cancels a running handler;
an injected final-callback 503 causes automatic Chronicle redelivery in both
modes. That is not a process-kill test or proof of exactly-once external effects.

Runtime measurements and their scope are recorded in the
[benchmark guide](../../benchmarks/snapshots/README.md#electric-runtime-measurements-use-the-actual-activation-path).
All 360 measured activations passed, but the tested full-raw-replay policy with
forced publication showed no median activation win. The 10k-update case reduced
preload p50 from 43.45 to 36.60 ms, yet total activation was 74.26 → 76.08 ms;
3k append-only rows regressed from 63.56 to 143.90 ms. Images can reduce folding
without reducing raw transfer, and restoring an old image with a large pending
inbox reads that suffix in both the state and raw readers. Publication eligibility
and cadence therefore need application measurements; neither a successful
snapshot GET nor the Chronicle-only 100k-update speedup establishes a runtime win.

The corrected [bounded-input campaigns](../../benchmarks/snapshots/README.md#bounded-electric-recovery-avoids-prefix-reads)
use the same explicit input contract on both paths and verify every admitted
entity-source GET against the aligned image cut and incarnation. All 360 measured
activations pass, including 180 checkpoint hits with no prefix reads. At 10k
updates over 16 keys, activation p50/p95 improves from 107.25/136.66 to
60.18/72.42 ms; combined Node CPU p50 falls from 89.06 to 48.39 ms, and captured
request-plus-response bodies fall from 1,610,749 to 18,475 bytes, including
publication. This is an observed 44% p50 reduction, not the projection-only
experiment's two-orders-of-magnitude gain.

The same corrected tree is slower for default-size workloads, 3k append-only
rows (155.12 → 184.13 ms p50), and 100 pending 8 KiB inputs. Image transfer,
hydration and forced successor publication remain real costs. Both campaigns
measure fresh application state in warm processes with synthetic handlers and
zeroed publication gates, not LLM work, cold process startup, default cadence or
production capacity. The source freeze, complete samples, independent reporter,
resource scope and qualifications are linked from the campaign table.

### Local prototype evidence

Executed with Go 1.26.2, linux/amd64, Intel Xeon 2.60 GHz in an orb on 2026-09-26.
Redis integration used local Redis 7.0.15 with `noeviction`, not the production
Redis 8 target. This verifies ordinary Lua/GET/SET behavior, not HA durability.

```bash
env -u REDIS_URL go test -race -count=1 ./...       # all packages passed
make lint                                        # 0 issues
go test -race -short ./experimental/checkpoint -rapid.checks=1000
go test -race -count=10 ./experimental/checkpoint -run TestRedisCheckpoint
go test ./experimental/checkpoint -run '^$' -bench BenchmarkCapture -benchmem -count=3
```

All listed tests passed. The microbenchmark's three samples were:

| Path | Applied frames/op | Time/op | Allocated bytes/op |
|---|---:|---:|---:|
| Full replay | 10,010 | 2.498–3.101 ms | about 2,419,500 |
| Snapshot + tail | 10 | 58.565–61.035 µs | 4,899 |

Both include state serialization; the snapshot path also includes validation and
deserialization. Neither includes Redis/network I/O or publication. The state is
a tiny fixed-size order-sensitive aggregate, not an agent's collections. Some
checks ran concurrently in this shared orb, so these are illustrative samples,
not an isolated latency study. MemoryStore still scans its message slice;
10 frames/op is **reducer work**, not a claim of constant-time source lookup.

## Acceptance tests before rollout

The Chronicle prototype exercises arbitrary prefix splits with an order-sensitive
fold, exact suffix counts, corrupt/versioned images, complete JSON messages,
append races, delete/recreate even at tail, failures, concurrent Redis publication,
equal-offset conflicts, fresh-client restoration and fork identity isolation.

The patch bundle records executed integration tests. The following is the wider
**application/rollout acceptance matrix**, not a claim that every case is covered:

1. Full replay vs hydrate+tail: compare all collection rows, explicit next `_seq`
   after trailing deletes, row pointers, stable timeline order and model messages.
2. Preserve each collection's configured update semantics: partial inbox promotion
   must retain payload/mode; full-replacement updates must remove absent fields.
3. Multiple JSON items/entries per batch and multiple SSE data events per control;
   checkpoint only after commit; compare canonical all-item pointers, timeline
   order and equal-cut bytes, including optimistic echoes and control messages.
4. Pending wake multiplicity, cancellation spanning the cut, signals already
   handled vs pending, and equivalent `ctx.events`, manifests and self-observation
   under the same selected input contract, not legacy-versus-bounded equivalence.
5. No ack advancement or side effects from hydration/publication; crash before
   handler completion; redelivery under a new fenced activation.
6. Rollback from incompatible codec, schema and runtime builds; no partially
   hydrated DB visible after validation failure.
7. Forks before/at/after a checkpoint including sub-offset boundaries; source
   deletion with retained fork prefixes; no parent-future state in a child.
8. HTTP authorization/CORS/cache variation/incarnation preconditions; mixed old
   and new replicas; oversized or malicious snapshot uploads; lifecycle erasure.
9. Replica failover and restore: an independently surviving image must not mask
   a rolled-back/replaced source prefix. Prove source-prefix durability or
   invalidate checkpoints after uncertain rollback; a checksum alone cannot.
