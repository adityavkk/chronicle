# Axon Framework & Marten — the projection spectrum

> Two systems chosen because they **bracket** the design space. Axon (JVM) is a full CQRS/DDD/ES
> framework that **bundles command + event handlers inside the aggregate** and treats projections as
> async event processors. Marten (.NET on Postgres) is a **library** that keeps handlers out of the
> store and makes the "*when do you materialize the read model*" decision a first-class, named
> setting: `ProjectionLifecycle.Live` / `.Inline` / `.Async`. That live-vs-inline-vs-async axis is
> exactly the decision Chronicle would face if extended toward event-sourced entities, so it is the
> spine of this document.

Primary sources: [Axon Aggregate ref](https://docs.axoniq.io/axon-framework-reference/4.13/axon-framework-commands/modeling/aggregate/),
[Axon Event infrastructure ref](https://docs.axoniq.io/axon-framework-reference/5.1/events/infrastructure/),
[Axon Event snapshots ref](https://docs.axoniq.io/axon-framework-reference/5.0/tuning/event-snapshots/),
[Axon Streaming processor ref](https://docs.axoniq.io/axon-framework-reference/4.13/events/event-processors/streaming/),
[Marten Projections](https://martendb.io/events/projections/),
[Marten Command-handler workflow / `FetchForWriting`](https://martendb.io/scenarios/command_handler_workflow.html),
[Marten Async daemon](https://martendb.io/events/projections/async-daemon.html),
[Marten Single-stream projections & snapshots](https://martendb.io/events/projections/single-stream-projections).

---

## Model

### Axon — the aggregate *is* the write model, handlers live inside it

An Axon aggregate is a plain Java object that holds state and the methods that change it. One
aggregate **instance** maps to one event sequence, keyed by `@AggregateIdentifier`. Events are typed
POJOs; the aggregate publishes them with the static `AggregateLifecycle.apply(...)`. Crucially, both
the *decision* logic and the *state-folding* logic are annotated methods **inside the aggregate
class** ([ref](https://docs.axoniq.io/axon-framework-reference/4.13/axon-framework-commands/modeling/aggregate/)):

```java
public class GiftCard {
    @AggregateIdentifier private String id;          // external reference into this aggregate

    @CommandHandler                                  // DECIDE: business logic, may reject
    public GiftCard(IssueCardCommand cmd) {
        apply(new CardIssuedEvent(cmd.getCardId(), cmd.getAmount()));  // publish event
    }

    @EventSourcingHandler                            // EVOLVE: mutate state from the event
    public void on(CardIssuedEvent evt) { id = evt.getCardId(); }

    protected GiftCard() { }                          // no-arg ctor required: empty shell, then replay
}
```

- `@CommandHandler` = "the place where you would put your decision-making/business logic." It may
  reject the command or call `apply(...)`.
- `@EventSourcingHandler` = "called when the Aggregate is *sourced from its events*… this is where
  all the state changes happen." The aggregate identifier **must** be set in the handler of the first
  event.
- To load an aggregate, Axon constructs the empty shell via the no-arg constructor and **replays its
  events** through the `@EventSourcingHandler`s.

This is the canonical "**handlers bundled with the entity**" model: command model (the aggregate) and
query model (projections) are split by [CQRS](https://www.axoniq.io/concepts/cqrs-and-event-sourcing),
but command-side decide+evolve are welded together in one class that is also the consistency boundary.

**Ordering / identity.** Aggregate-based storage engines organize events "by aggregate identifier and
sequence number, with a global index for streaming"
([infra ref](https://docs.axoniq.io/axon-framework-reference/5.1/events/infrastructure/)). So an event
is identified by `(aggregateIdentifier, sequenceNumber)` (per-entity, 0,1,2,…) plus a global index
that gives processors a single totally-ordered stream to subscribe to. Axon 5 adds **Dynamic
Consistency Boundaries (DCB)**: events carry *tags*, are queried by *criteria*, and the consistency
boundary is no longer forced to be exactly one aggregate — the default `AxonServerEventStorageEngine`
and a `PostgresqlEventStorageEngine` (PG 16+) support it via `tags` / `consistency_tags` tables.

### Marten — the stream is dumb; the "aggregate" is a fold

A Marten event is a JSON document in the `mt_events` table; `mt_streams` tracks the current state of
each stream ([appending ref](https://martendb.io/events/appending)). A **stream** has an id (`Guid`
or `string`) and an *optional* .NET "aggregate type" marker `T` that today is "nothing more than
metadata." Each event carries a per-stream `IEvent.Version` and a store-global `IEvent.Sequence`.

The defining contrast with Axon: in Marten the **aggregate is not a write-side object with bundled
behavior — it is a projection**, i.e. a fold over the stream expressed as `Create`/`Apply` methods
([single-stream ref](https://martendb.io/events/projections/single-stream-projections)):

```csharp
public sealed record QuestParty(Guid Id, List<string> Members) {
    public static QuestParty Create(QuestStarted s) => new(s.QuestId, []);
    public static QuestParty Apply(MembersJoined j, QuestParty p) =>
        p with { Members = p.Members.Union(j.Members).ToList() };
}
```

These `Apply` methods are *pure functions*: `(State, Event) -> State`. Whether that fold runs at read
time, at write time, or in the background is a **separate registration decision** (next sections) —
not a property of the type. Command handling (the "decide" half that Axon nails into the aggregate)
is **your** code in Marten, typically via `FetchForWriting` (below) or Wolverine's higher-level
[Aggregate Handler Workflow](https://wolverinefx.net/guide/durability/marten/event-sourcing.html).

| | Axon | Marten |
|---|---|---|
| Stream/aggregate id | `@AggregateIdentifier` | stream id (`Guid`/`string`) + optional type `T` |
| Per-entity order | sequence number 0,1,2… | `IEvent.Version` |
| Global order | global index | `IEvent.Sequence` |
| Events | typed Java classes | typed .NET classes, stored as JSON |
| Decide logic | `@CommandHandler` **inside** aggregate | app code / Wolverine (outside) |
| Evolve logic | `@EventSourcingHandler` inside aggregate | `Create`/`Apply` on a projection type |

---

## Optimistic concurrency

Both give **single-writer-per-entity correctness via a per-entity version + a uniqueness/CAS check**,
no application-level locks. They differ in granularity wording (Axon: aggregate; Marten: stream) but
an Axon aggregate instance *is* one stream, so the mechanisms coincide.

### Axon — uniqueness on `(aggregateIdentifier, sequenceNumber)`

The event table "only allows a single event for a given aggregate identifier and sequence number.
Therefore, inserting a second event for an existing aggregate with an existing sequence number will
result in an error… [the engine] can detect this error and translate it to a `ConcurrencyException`"
([infra ref](https://docs.axoniq.io/axon-framework-reference/5.1/events/infrastructure/)). Within one
JVM Axon also serializes access to a given aggregate (a lock); across JVMs the **database key
constraint is the backstop**. The error literally surfaces as *"An event for aggregate [id] at
sequence [n] was already inserted"*
([issue #1194](https://github.com/AxonFramework/AxonFramework/issues/1194)). Storage engine choices:
`AxonServerEventStorageEngine` (default, DCB), `AggregateBasedAxonServerEventStorageEngine`,
`PostgresqlEventStorageEngine`, `AggregateBasedJpaEventStorageEngine` (JPA/RDBMS) — the aggregate-based
ones key on `(aggregateId, sequenceNumber)`; DCB ones use a `consistency_tags` table for
concurrent-write safety.

### Marten — expected stream version on `FetchForWriting` / `SaveChangesAsync`

The recommended command path captures the stream version at read time and checks it at commit
([command-handler ref](https://martendb.io/scenarios/command_handler_workflow.html)):

```csharp
var stream = await session.Events.FetchForWriting<Order>(command.OrderId);  // captures current version
// ...decide against stream.Aggregate, then:
stream.AppendOne(new ItemReady(...));
await session.SaveChangesAsync();   // ConcurrencyException if another writer advanced the stream meanwhile
```

Three explicit flavors:
- **Implicit** — `FetchForWriting<Order>(id)`: throws `ConcurrencyException` if the stream moved
  between fetch and save.
- **Explicit expected version** — `FetchForWriting<Order>(id, command.Version)`: caller asserts the
  starting version; checked both at fetch and at `SaveChangesAsync`.
- **Exclusive** — takes a Postgres **row-level lock** on the stream and waits, for when the decision
  depends only on the initial state.

Lower-level primitives: `StartStream<T>(...)` (throws `ExistingStreamIdCollisionException` if the
stream exists), `Append(id, events)`, `Append(id, expectedVersion, events)` (needs `Rich` append
mode), and `AppendOptimistic(id, events)` (auto-looks-up the version). The docs **strongly recommend
`FetchForWriting` over hand-rolling `Append(id, expectedVersion, …)`** because it works in both append
modes and "gives you the optimistic-concurrency guard for free." A subtlety worth noting for any
imitator: if a handler appends **no** events, Marten by default does **no** concurrency check at
all — you must opt in (an `AssertStreamVersion` read) to detect a racing writer on a no-op decision.

---

## Snapshots

This is where the two systems diverge most sharply, and the divergence is instructive.

### Axon — a built-in, separate snapshot primitive (an optimization)

Loading a long-lived aggregate by replaying thousands of events is slow, so Axon snapshots
([snapshot ref](https://docs.axoniq.io/axon-framework-reference/5.0/tuning/event-snapshots/)):

- A `SnapshotTriggerDefinition` decides *when*. The shipped one is
  `EventCountSnapshotTriggerDefinition(snapshotter, 500)` — snapshot once loading would need > N
  events. Configured per aggregate: `@Aggregate(snapshotTriggerDefinition = "giftCardSnapshotTrigger")`.
- A `Snapshotter` (recommended to run on its own thread) creates the snapshot. Axon's
  `AggregateSnapshotter` produces an `AggregateSnapshot` — "a special type of snapshot, since it
  contains the actual aggregate instance within it."
- **Recovery folds snapshot + tail**: the repository "will extract the aggregate from
  [the snapshot]… All events loaded after the snapshot events are streamed to the extracted aggregate
  instance." So load = latest snapshot → then replay only events after it.
- Events stay the **source of truth**: "Snapshotted events will never be read in again… However, if
  you want to be able to reconstruct an aggregate state prior to the snapshot, you must keep the
  events." A snapshot is a cache, never authoritative.

### Marten — there is no snapshot primitive; a "snapshot" *is* an inline single-stream projection

Marten makes the connection the brief asks for **explicit in its own API**: a "snapshot" is just a
self-aggregating single-stream projection given a lifecycle
([single-stream ref](https://martendb.io/events/projections/single-stream-projections)):

```csharp
opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Inline);  // "Snapshot now means a version of the projection from the events"
opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);   // same doc, maintained in the background
opts.Projections.LiveStreamAggregation<QuestParty>();             // == ProjectionLifecycle.Live (no stored doc)
```

So in Marten the "snapshot" is **a continuously maintained document**, not a periodic checkpoint of a
write model. Its freshness is *the same live/inline/async dial* used for every other read model:
- `Inline` snapshot → the document is rewritten in the **same transaction** as the append (always
  exactly current).
- `Async` snapshot → the background daemon keeps it eventually-current.
- No registration → `FetchLatest<T>()` / `AggregateStreamAsync<T>()` treat `T` as a **Live** snapshot
  and fold the events in memory on every call ("appropriate for short streams, maybe a performance
  issue in longer event streams").

The two philosophies: **Axon snapshots the *write* model periodically to speed up command handling;
Marten "snapshots" by choosing how eagerly to materialize a *read* model.** Same word, opposite side
of CQRS.

---

## Projections / read models — the centerpiece

The question "**when is the read model computed?**" has three answers, and Marten is unusual in
naming all three as a single enum. Axon supports two of them (sync/inline-ish vs async) but not an
on-read fold for projections (its on-read fold is the aggregate, on the command side).

### Marten — `ProjectionLifecycle` is the whole story

Registered up front, the lifecycle decides everything downstream
([projections ref](https://martendb.io/events/projections/)):

```csharp
opts.Projections.Add<MyProjection>(ProjectionLifecycle.Live);    // read-time
opts.Projections.Add<MyProjection>(ProjectionLifecycle.Inline);  // write-time, strongly consistent
opts.Projections.Add<MyProjection>(ProjectionLifecycle.Async);   // background, eventually consistent
```

**(a) Live / on-read aggregation** — `AggregateStreamAsync<T>(streamId)`. Nothing is stored; events
are loaded and folded in memory at query time. Supports point-in-time: `AggregateStreamAsync<T>(id,
version: 3)` or `…(id, timestamp: …)`. Cheapest to maintain (zero write cost), most expensive per
read, scales badly with stream length.

**(b) Inline projections** — the projected document is updated "*at the time of event capture and in
the same unit of work to persist the projected documents*." **Strongly consistent**: after
`SaveChangesAsync` returns, the read model already reflects the new events, in the same Postgres
transaction. This is what makes `FetchForWriting` + inline snapshots an efficient
read-decide-append loop (Marten can reuse the fetched aggregate from the identity map instead of
re-reading; `UseIdentityMapForAggregates` is `true` by default in Marten 9). Cost: every write pays
the projection cost synchronously, and inline projections under "Quick" append mode don't yet know
`IEvent.Version`/`Sequence`.

**(c) Async projections via the Async Daemon** — a background agent
([async ref](https://martendb.io/events/projections/async-daemon.html)). Two definitions matter:

- **High Water Mark** — "the furthest known event sequence that the daemon 'knows' that all events
  with that sequence or lower can be safely processed in order." It deliberately lags the highest
  assigned sequence when there are **gaps** in the sequence (uncommitted/rolled-back inserts), to
  preserve ordering.
- **Projection progression** — "the async daemon always knows what the current progression by event
  sequence number is for each individual asynchronous projection." Each projection (shard) has its
  own persisted checkpoint; it advances from its progression toward the high-water mark.

Deployment modes: **Solo** (one node, daemon runs everything) vs **HotCold** (built-in leader
election that "ensure[s] that each projection is running on **exactly one** running process"). So
within a single projection Marten is **not** a competing-consumers pool — parallelism is *across*
projections/shards, and ordering within a shard is preserved by the single owner.

**Projection shapes** (orthogonal to lifecycle): Single-stream (one stream → one view),
Multi-stream (aggregate across arbitrary stream groupings; **must be Async** and cannot use
`FetchForWriting`), Event projections (one event → create/delete docs),
[Flat-table](https://martendb.io/events/projections/flat) (project into relational tables), and
Custom.

**Rebuild / replay** = reset the progression to zero and re-run:
`daemon.RebuildProjectionAsync<TProjection>(...)` or the CLI `dotnet run -- projections --rebuild
-p <shard>`. Because the events are the source of truth, a projection is disposable and can be
reshaped and rebuilt at will. (Continuous processing skips apply/serialization errors by default;
rebuilds enforce them and stop.)

### Axon — projections are event processors; lifecycle = which processor

Axon read models are built by **event processors** consuming the global event stream, distinct from
the aggregates. The "when" is chosen by processor *type*
([streaming ref](https://docs.axoniq.io/axon-framework-reference/4.13/events/event-processors/streaming/),
[subscribing ref](https://docs.axoniq.io/axon-framework-reference/5.1/events/event-processors/subscribing/)):

- **Subscribing event processor** — handles events **in the publishing thread / transaction**
  (push). This is the closest Axon analog to Marten **Inline**: synchronous, same transaction as the
  command, no independent checkpoint. It "always work[s] on a sequential-per-aggregate basis."
- **Tracking / Streaming event processor** (the default when an event store is present) — runs in
  its **own thread**, pulls from the stream, and **persists its position** as a `TrackingToken` in a
  `TokenStore`. This is Marten **Async**: decoupled, resilient, replayable, eventually consistent.

The checkpoint is the `TrackingToken`: "the progress is kept by updating and saving the
`TrackingToken` after handling batches of events… for which the Streaming Processor uses the
`TokenStore`." `TokenStore` implementations: `JpaTokenStore`, `JdbcTokenStore`, `InMemoryTokenStore`
(non-production). **Replay/rebuild** a projection by resetting the token —
`StreamingEventProcessor.resetTokens()` (or `resetTokens(startPosition)`); the processor must be
stopped and must claim all its segments first.

**Parallelism / competing consumers.** A processor's stream is split into numbered **segments**, one
`TrackingToken` per segment (`initialSegmentCount`; default 1 for Tracking, 16 for Pooled). Workers
**claim** segment tokens; a stalled claim can be **stolen** after `claimTimeout` (default 10s) so work
is never blocked forever, and the token-update-is-required-to-commit rule prevents double processing
on a steal. Ordering within parallelism is governed by the `SequencingPolicy` — default
`SequentialPerAggregatePolicy` keeps events from one aggregate on one segment in order. This is
**at-least-once**: on crash, processing resumes from the last saved token, so events after it run
again (handlers must tolerate replay).

**Sagas / process managers** (Axon's long-running coordinators) sit alongside: a saga is started by a
`@StartSaga @SagaEventHandler(associationProperty = "orderId")` handler and correlated to domain
concepts by **association** `(key, value)` pairs, adding more via
`SagaLifecycle.associateWith("shipmentId", id)` and ending with `SagaLifecycle.end()`
([saga ref](https://docs.axoniq.io/axon-framework-reference/5.1/sagas/associations/)). A saga instance
is never invoked concurrently, so it sidesteps the sequencing-policy problem entirely.

---

## Handlers & consumers

### Are command/event handlers bundled with the entity?

- **Axon: yes, emphatically.** `@CommandHandler` (decide) and `@EventSourcingHandler` (evolve) live
  inside the aggregate; the aggregate is simultaneously the entity, the consistency boundary, the
  single writer, and the home of its behavior. Projections and sagas are the *only* things modeled as
  external consumers.
- **Marten: no.** Marten is a store, not an actor runtime. The aggregate type carries only the
  `Create`/`Apply` fold; the *decide* step is ordinary application code that calls `FetchForWriting`
  (the "**decider pattern**": handler returns events, the inline projection rebuilds state from those
  events on save). Wolverine layers a bundled-handler *feel* on top, but the store itself stays
  handler-free.

### The subscription / consumer model

| | Axon | Marten |
|---|---|---|
| Push vs pull | Subscribing = push (publisher thread); Tracking = pull (own thread off the store) | Async daemon **pulls** by polling Postgres for new `Sequence`; inline = synchronous in the write txn |
| Checkpoint | `TrackingToken` in `TokenStore`, per segment | progression row per projection shard; `HighWaterMark` gates it |
| Competing consumers | segments + token **claim/steal**; many workers, ordered per `SequencingPolicy` | **HotCold leader election: exactly one node per projection**; parallelism is across shards, not within |
| Delivery | at-least-once (resume from last token → replay) | inline ≈ exactly-once (same txn); async ≈ at-least-once during catch-up/rebuild |
| Rebuild | `resetTokens()` | reset progression → `RebuildProjectionAsync` / `--rebuild` |

The shared, load-bearing idea: **the read-model offset is a consumer concern, kept in a separate,
resettable store — never derived from or stored in the event log itself.** Axon's `TokenStore` and
Marten's progression table are the same artifact under different names, and "rebuild a projection" is
"reset that offset and replay" in both.

---

## Lessons for Chronicle

1. **"When do you materialize" is the single decision to expose — and the answer is a per-read-model
   dial, not a global choice.** Marten's `Live` / `Inline` / `Async` enum
   ([ref](https://martendb.io/events/projections/)) is precisely the menu Chronicle's apps face today
   — except Chronicle only offers **Live**: fold the stream yourself on read. If Chronicle grows
   projections, copy Marten's posture (name the three lifecycles, let each read model pick) rather
   than Axon's (lifecycle implied by processor type). Don't pick one and bake it in.

2. **The cheapest, highest-value, most-provable extension is a per-stream numeric expected-version on
   append.** Both systems get single-writer-per-entity from a per-entity version + uniqueness/CAS
   (Axon `(aggregateId, sequenceNumber)` key constraint; Marten `FetchForWriting` version check).
   Chronicle's `Stream-Seq` is *lexicographic and stream-scoped*, not a per-entity numeric revision.
   Adding a numeric "expected version N" to `POST /v1/stream/{path}` (reject with 409 if the stream's
   event count ≠ N) reproduces Marten's `FetchForWriting` guarantee. Chronicle **already serializes
   per stream inside one Lua script on one shard**, so this CAS is essentially free and folds straight
   into the existing Lean-proven append state machine — no new distributed machinery.

3. **Inline (write-time) projections are the dangerous one for Chronicle's architecture; keep any
   server-side fold single-stream and shard-aligned.** Marten's inline model runs the fold *in the
   append transaction* — fine when the projection is a single-stream aggregate (it stays inside the
   one-shard-per-stream Lua boundary that already gives Chronicle atomicity). But **multi-stream
   projections force Async in Marten and can't use `FetchForWriting`** for a reason: cross-entity
   materialization can't be both synchronous and scalable. Given Chronicle's known constraint that
   the single Durable-Streams hub **serializes the wake path under concurrency**, any inline
   *cross-stream* fold would inherit that exact serialization stall. Rule: inline ⇒ single-stream
   only; everything cross-entity goes async.

4. **Don't build a bespoke snapshot primitive — snapshots fall out of single-stream projections.**
   Marten proves the equivalence in its API: `Snapshot<T>(SnapshotLifecycle.Inline)` *is* a
   continuously-maintained inline aggregate
   ([ref](https://martendb.io/events/projections/single-stream-projections)). If Chronicle adds
   (a) single-stream fold-on-read and (b) an optional materialized document per stream, it gets
   snapshots for free as the inline/async variant of (a). Keep events as source of truth (Axon's
   archive rule: a snapshot is a cache you can always discard and rebuild), so the pure-core proofs
   still bound correctness.

5. **Projections are just fenced consumers with a resettable cursor — reuse the subscription plumbing,
   don't invent a second checkpoint store.** Axon `TokenStore` and Marten's progression table are the
   same artifact: a per-consumer offset, separate from the log, resettable to replay. Chronicle
   *already* has this shape in its pull-wake subscription path (lease + ack'd offset + generation/epoch
   fencing). A projection consumer is one more fenced subscriber whose cursor can be reset to `-1` to
   rebuild. Crucially, keep the offset **out of the log** (Chronicle's offset is the read cursor;
   the projection checkpoint must be a separate, per-projection record) so rebuilds never mutate
   history.

6. **Bundled server-side handlers (the Axon move) fight Chronicle's two non-negotiables; prefer the
   Marten/decider posture.** Axon's aggregate-resident `@CommandHandler` requires the server to host
   and serialize per-entity behavior — which is exactly the polyglot-breaking, hub-serializing design
   Chronicle must avoid (handlers in one JVM; one writer per aggregate routed through a node). Marten
   keeps decide-logic in app code and offers only `FetchForWriting` (read-current-state → app decides
   → append-with-expected-version). That read-decide-append contract is HTTP-shaped, language-agnostic,
   and maps onto Chronicle's existing primitives with one addition (lesson 2). If Chronicle ever wants
   "handlers," expose the *contract*, not a server-resident aggregate runtime — and if cross-entity
   reactions are needed, model them as async consumers **sharded by entity/partition key** so the fold
   parallelizes instead of fencing on the single hub.
