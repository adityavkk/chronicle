# Akka Persistence (Typed) — Event-Sourced Entities, Sharding, Query, Projections

> System under study: **Akka Persistence Typed** (`EventSourcedBehavior`), **Cluster Sharding**,
> **Persistence Query**, and **Akka Projection**. Versions cited: akka-core 2.10.x, akka-projection
> 1.6.x. Licensing moved to BSL/BUSL-1.1; this note ignores licensing and studies the architecture.
> Every non-obvious claim is linked to Akka's official docs. Where Akka's mechanism differs from the
> "expected version" / "projection" vocabulary the user used, that is called out explicitly.

The one-sentence framing: Akka is the opposite end of the spectrum from Chronicle. Chronicle is a
storage primitive (a durable log) with no compute. Akka is a **programming model + distributed
runtime** in which the event log is almost an implementation detail — the entity, its command/event
handlers, the single-writer guarantee, and recovery are all the actual product. Reading Akka tells
Chronicle which of those pieces are *storage* concerns (worth absorbing) and which are *runtime*
concerns (worth refusing).

## Model

An event-sourced entity is an [`EventSourcedBehavior[Command, Event, State]`](https://doc.akka.io/api/akka-core/current/akka/persistence/typed/scaladsl/EventSourcedBehavior.html). The
[minimum definition](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html) is four
things:

```scala
EventSourcedBehavior[Command, Event, State](
  persistenceId = PersistenceId.ofUniqueId("abc"),
  emptyState    = State(),
  commandHandler = (state, cmd) => /* Effect: maybe persist events */,
  eventHandler   = (state, evt) => /* new State */)
```

- **`persistenceId`** — "the stable unique identifier for the persistent actor in the backend event
  journal and snapshot store" ([docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html#persistenceid)).
- **`emptyState`** — the `State` before any event (a counter starts at 0).
- **`commandHandler: (State, Command) => Effect[Event, State]`** — validates the command against
  current state and returns an `Effect` directive — typically `Effect.persist(event)` (persists one
  or several events atomically) or `Effect.none` (read-only command).
- **`eventHandler: (State, Event) => State`** — applies a *persisted* event to produce the next
  state. The same handler runs both for live events and during recovery.

The core invariant: **only events are stored, never the state.** "The events are persisted by
appending to storage (nothing is ever mutated) ... A stateful actor is recovered by replaying the
stored events" ([docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html#introduction)).
The split between the two handlers is deliberate and load-bearing: a command **can** fail (validation,
external calls), but "events cannot fail when being replayed to a persistent actor" — so the event
handler must be pure state-folding with no side effects (those would re-run on every recovery)
([docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html#event-handler)).

How the model maps to the user's vocabulary:

| Concept | Akka realization |
| --- | --- |
| "event" | a typed value (`sealed trait Event`); identified by **(persistenceId, sequenceNr)** |
| "stream" | the per-`persistenceId` event log in the journal (append-only) |
| "aggregate" | the entity = one `persistenceId` = one in-memory actor holding `State` |
| event type | the JVM type of the `Event` (serialized by a configured serializer) |
| ordering | strict per-`persistenceId` monotonic sequence number |

`persistenceId` is usually built from the Cluster Sharding `EntityTypeKey` plus the business
`entityId`, e.g. `PersistenceId("ShoppingCart", entityId)` — the type prefix disambiguates entities
that share an `entityId`. The default separator is `|`
([docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html#persistenceid)). There
is **no global `$all` stream and no by-type stream for free** — cross-entity views require explicit
tagging (see Projections).

## Optimistic concurrency

Akka does **not** use a compare-and-swap on an "expected version" in the event-sourced path. Its
concurrency control is structural: the **single-writer principle**.

> "Akka Persistence is based on the single-writer principle, for a particular `PersistenceId` only
> one persistent actor instance should be active. If multiple instances were to persist events at the
> same time, the events would be interleaved and might not be interpreted correctly on replay.
> Cluster Sharding is typically used together with persistence to ensure that there is only one
> active entity for each `PersistenceId`."
> — [Cluster Sharding docs](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html)
> (restated nearly verbatim in the [Persistence docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html#running-the-actor)).

So the mechanism is: each entity carries **per-`persistenceId` monotonically increasing sequence
numbers** in the journal (the journal row schema is literally `(persistence_id, seq_nr, payload, ...)`,
visible in the [Persistence Query plugin example](https://doc.akka.io/libraries/akka-core/current/persistence-query.html#readjournal-plugin-api)).
Because exactly one writer is live and it holds the current `State` (hence the current highest
sequence number) in memory, the next event is simply `seq = highest + 1`. There is no append-time
version check to lose, because there is never a second writer to race. The "expected version" is
implicit and always satisfied.

This shifts the whole problem from *detecting* conflicts to *preventing* them. Enforcement is a
stack of runtime guarantees:

- **Cluster Sharding** routes all messages for an `entityId` to the single node currently hosting it;
  the entity "lives on one node" so reads can even be served from memory
  ([durable-state docs](https://doc.akka.io/libraries/akka-core/current/typed/durable-state/persistence.html#cluster-sharding-and-durablestatebehavior)).
- **Passivation** stops idle entities to save memory; Sharding buffers in-flight messages across the
  passivate→stop→reincarnate window so nothing is lost
  ([passivation docs](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html#passivation)).
  Default strategy passivates entities idle for 2 minutes.
- **`rememberEntities`** re-creates entities after a rebalance/restart and disables automatic
  passivation ([docs](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html#automatic-passivation)).

The pathological case — two writers somehow journaling the *same* sequence number (a split brain /
corruption) — is handled at recovery by a configurable **replay filter** that detects and discards
events from a foreign writer-UUID at a duplicate sequence number
([docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html#replay-filter)). That
is a safety net, not the primary mechanism.

**Recovery** is the dual of the write path: "An event sourced actor is automatically recovered on
start and on restart by replaying journaled events. New messages ... during recovery ... are stashed
and received ... after the recovery phase completes"
([docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html#recovery)). The number
of concurrent recoveries is capped so a thundering herd of restarts can't overload the journal. A
`RecoveryCompleted` signal always fires (even for a brand-new `persistenceId`), and is the correct
place for post-recovery side effects.

## Snapshots

Snapshots are a recovery-time optimization layered on a **separate** `SnapshotStore` plugin
(`akka.persistence.snapshot-store.plugin`), distinct from the journal. They are triggered two ways
([Snapshotting docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence-snapshot.html)):

```scala
EventSourcedBehavior[Command, Event, State](...)
  .snapshotWhen { case (state, BookingCompleted(_), seqNr) => true; case _ => false }
  .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100, keepNSnapshots = 2))
```

- **`snapshotWhen(predicate)`** — snapshot when a `(State, Event, sequenceNr)` predicate holds
  (e.g. on a terminal event).
- **`RetentionCriteria.snapshotEvery(numberOfEvents = N, keepNSnapshots = K)`** — snapshot every N
  events and keep K of them; older snapshots (seqNr below `latest − K*N`) are auto-deleted after a
  successful save.

Recovery folds **snapshot + tail**: "During recovery, the persistent actor is using the latest saved
snapshot to initialize the state. Thereafter the events after the snapshot are replayed using the
event handler to recover ... to its current state"
([docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence-snapshot.html#snapshots)).

The non-negotiable property: **snapshots are never the source of truth — events are.**
`SnapshotSelectionCriteria.none` disables snapshot-based recovery entirely and replays *all* events,
which is the documented escape hatch when a snapshot serialization format changed incompatibly
([docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence-snapshot.html#snapshots)).
A snapshot store need not even be configured; Akka only warns and keeps running until something tries
to snapshot. Optional event deletion (`withDeleteEventsOnSnapshot`, `deleteEventsOnSnapshot = true`)
exists but is discouraged — deleting events throws away the history that is the point of event
sourcing, and is forbidden under Replicated Event Sourcing
([docs](https://doc.akka.io/libraries/akka-core/current/typed/persistence-snapshot.html#event-deletion)).

## Projections / read models

The journal is **per-entity**. A read model that spans entities (all completed orders, a popularity
count across all carts) cannot be served by `eventsByPersistenceId`. So Akka's read side is built in
two layers: **Persistence Query** (the raw event streams) and **Akka Projection** (a managed,
checkpointed consumer that writes a read model).

### Persistence Query — the raw streams

[Persistence Query](https://doc.akka.io/libraries/akka-core/current/persistence-query.html)
"complements Event Sourcing by providing a universal asynchronous stream based query interface."
The predefined queries (each journal documents which it supports):

- **`eventsByPersistenceId(pid, fromSeq, toSeq)`** — "a query equivalent to replaying an event
  sourced actor," but live: it keeps emitting new events. `currentEventsByPersistenceId` is the
  finite variant.
- **`eventsByTag(tag, offset)` / `currentEventsByTag`** — "allows querying events **regardless of
  which `persistenceId`** they are associated with ... query all domain events of an Aggregate Root
  type."
- **`eventsBySlices(entityType, minSlice, maxSlice, offset)`** — slices are deterministically derived
  from the `persistenceId` to evenly partition all ids; the modern, shard-friendly alternative to
  tags.
- `persistenceIds` / `currentPersistenceIds`.

**Why you tag** is the whole point: the per-entity journal has no cross-entity index, so the
**write side opts events into a cross-cutting stream** via `.withTagger`:

```scala
EventSourcedBehavior[Command, Event, State](...)
  .withTagger(event => event match {
    case _: OrderCompleted => Set(entityGroup, "order-completed")
    case _                 => Set(entityGroup)
  })
```

([tagging example](https://doc.akka.io/libraries/akka-core/current/persistence-query.html#eventsbytag-and-currenteventsbytag)).
Each delivered `EventEnvelope` carries `(offset, persistenceId, sequenceNr, event)`. **Offsets** are
opaque resumption tokens of type `Offset` — `Offset.noOffset`, `Sequence(Long)` (numeric/ordinal
journals), or `TimeBasedUUID` (Cassandra's time-ordered UUID). The offset is **exclusive**: feed the
last delivered offset back in to resume without re-reading it. Critically, the docs warn that for
multi-`persistenceId` queries like `eventsByTag`, "the order of events ... rarely is guaranteed (or
stable between materializations)" unless a journal explicitly promises it.

### Akka Projection — the managed consumer

[Akka Projection](https://doc.akka.io/libraries/akka-projection/current/overview.html) wraps a query
stream into a restartable, checkpointed consumer: "you process a stream of events ... Each event is
associated with an offset ... used for resuming the stream from that position when the projection is
restarted." A projection is assembled from a **`SourceProvider`** (e.g.
`EventSourcedProvider.eventsByTag` or `.eventsBySlices`) plus an **offset store**. Offset-store
backends: [Cassandra](https://doc.akka.io/libraries/akka-projection/current/cassandra.html),
[JDBC](https://doc.akka.io/libraries/akka-projection/current/jdbc.html),
[R2DBC](https://doc.akka.io/libraries/akka-projection/current/r2dbc.html),
[Slick](https://doc.akka.io/libraries/akka-projection/current/slick.html), or Kafka.

The offset store is **per-projection**, keyed by a `ProjectionId(name, key)` — e.g.
`ProjectionId("ShoppingCarts", "carts-0-255")` for one slice range. **WHEN** is read models are
computed: **asynchronously, in a separate process, in the background** — this is the CQRS read side,
eventually consistent with the write side. It is *not* computed inline at write time and *not* folded
on-the-fly at read time. **Replay/rebuild** a read model by clearing or resetting the stored offset
(projection management) and letting it re-consume from `noOffset`.

Projection instances are distributed and load-balanced across the cluster with
**`ShardedDaemonProcess`**, one instance per slice range:

```scala
ShardedDaemonProcess(system).initWithContext(
  name = "ShoppingCartProjection", initialNumberOfInstances = 4,
  behaviorFactory = ctx => {
    val range = EventSourcedProvider.sliceRanges(system, R2dbcReadJournal.Identifier, ctx.totalProcesses)(ctx.processNumber)
    ProjectionBehavior(R2dbcProjection.exactlyOnce(
      ProjectionId("ShoppingCarts", s"carts-${range.min}-${range.max}"),
      None, sourceProvider(range), () => new ShoppingCartHandler))
  }, ...)
```

([R2DBC example](https://doc.akka.io/libraries/akka-projection/current/r2dbc.html)). Slices partition
the work; this is closer to **partitioned consumers** (each instance owns a disjoint slice range)
than to a competing-consumer pool fighting over the same items.

## Handlers & consumers

This is the section that answers the user's "are handlers bundled with the entity?" question directly.

**Write-side handlers are bundled INSIDE the entity.** The `commandHandler` and `eventHandler` are
constructor arguments of the `EventSourcedBehavior` and are co-located with the `persistenceId` and
`State`:

```scala
val commandHandler: (State, Command) => Effect[Event, State] = { (state, command) =>
  command match {
    case Add(data) => Effect.persist(Added(data))   // decide: validate + emit events
    case Clear     => Effect.persist(Cleared)
  }
}
val eventHandler: (State, Event) => State = { (state, event) =>
  event match {
    case Added(data) => state.addItem(data)         // evolve: fold event into state
    case Cleared     => State()
  }
}
```

The command handler is the **decide** function (Command + State → events, may reject); the event
handler is the **evolve** function (State + Event → State, total and pure). This is exactly the
"handlers bundled with the stream/entity" model the user is comparing against — and it is a
*programming model backed by a runtime* (actors, dispatchers, sharding, passivation), not a storage
feature.

**Read-side handlers are deliberately NOT bundled.** The projection `Handler` lives in a separate
process and consumes the tagged/sliced stream. Akka keeps the cross-entity fold out of the entity.

**Consumer / subscription model:**

- **Pull, not push.** The query stream is an Akka Stream `Source` that the journal drives — for
  stores without native push it polls on a `refresh-interval`; stores that can subscribe to a live
  feed expose the same `Source` API ([query design overview](https://doc.akka.io/libraries/akka-core/current/persistence-query.html#design-overview)).
- **Checkpointing** = the per-projection offset store, advanced as envelopes are processed.
- **Delivery semantics** are an explicit choice at projection construction
  ([R2DBC docs](https://doc.akka.io/libraries/akka-projection/current/r2dbc.html)):

  | Mode | Constructor | Guarantee |
  | --- | --- | --- |
  | **exactly-once** | `R2dbcProjection.exactlyOnce(...)` | "The offset is stored in the **same transaction** used for the user defined handler" — read-model write and offset commit succeed or fail atomically. |
  | **at-least-once** | `.atLeastOnce(...).withSaveOffset(afterEnvelopes = 100, afterDuration = 500.millis)` | offset stored *after* processing, batched; on restart some envelopes reprocess → **handler must be idempotent**. |
  | **at-most-once** | `.atMostOnce(...)` | offset stored *before* processing; on restart, in-flight envelopes are skipped. |
  | **grouped** | `.groupedWithin(...).withGroup(20, 500.millis)` | handler receives a `Seq[EventEnvelope]` for batch writes; offset committed in the same transaction → exactly-once over the batch. |

  The transactional "**update read model + commit offset in one DB transaction**" pattern is the
  crux of exactly-once: the handler receives an `R2dbcSession` (an open connection), and both its
  `INSERT`/`UPDATE` and the offset upsert ride the same commit. There are also `...Async` and
  `...Flow` variants for handlers that write to a non-transactional sink (e.g. **Kafka**), which can
  only offer at-least-once.

## Contrast: DurableState, and a note on Replicated Event Sourcing

**`DurableStateBehavior` — the CRUD alternative.** Akka ships a second persistence model that stores
**only the latest state**, no event log:

```scala
DurableStateBehavior[Command, State](
  persistenceId, emptyState = State(0),
  commandHandler = (state, command) => command match {
    case Increment       => Effect.persist(state.copy(value = state.value + 1)) // upsert latest state
    case GetValue(reply) => Effect.reply(reply)(state)
    case Delete          => Effect.delete[State]()
  })
```

"Only the latest state is stored ... Very much like a CRUD based operation"
([docs](https://doc.akka.io/libraries/akka-core/current/typed/durable-state/persistence.html#durable-state)).
There is **no `eventHandler`** — the command handler returns the *next state* directly, replacing the
row. No history, no replay, no projections-from-events (there's a separate state-change query
interface instead).

Crucially, **this is where Akka's only real "expected version" lives.** The DurableState write is an
optimistic CAS on a **revision number**:

> "In case of an existing persistence id, the record will be **updated only if the revision number of
> the incoming record is 1 more than the already existing record. Otherwise `persist` will fail.**"
> — [DurableState docs](https://doc.akka.io/libraries/akka-core/current/typed/durable-state/persistence.html#command-handler)

So the two models use two different concurrency mechanisms: **event-sourced ⇒ single-writer** (no
CAS), **durable-state ⇒ revision-number CAS** (real expected-version check). This distinction is
exactly the kind of thing that gets muddled when people say "optimistic concurrency."

**Replicated Event Sourcing (active-active), in brief.** Akka can run **multiple replicas of each
entity** (e.g. one per region) with "automatic replication of every event persisted to all replicas"
([docs](https://doc.akka.io/libraries/akka-core/current/typed/replicated-eventsourcing.html)). The
price: "when replication is enabled **the single-writer guarantee is not maintained**" — concurrent
writes at different replicas are possible, so the **event handler must tolerate concurrent events**.
The recommended discipline is to model `State` as an **operation-based CRDT** (events are the
operations and must be commutative), using built-ins like `ORSet`, `Counter`, `LwwTime`, or an
app-specific last-writer-wins on timestamps. Conflicts are auto-merged by construction rather than
rejected. This is Akka's answer to multi-master event sourcing — and it is a substantial step up in
complexity from the single-writer default.

## Lessons for Chronicle

Chronicle today is a durable append-only HTTP stream over Redis — no entities, projections,
snapshots, per-entity expected-version, or bundled handlers (per `00-chronicle-ground-truth.md`).
Akka sits at the far "full framework" end. The useful pressure it exerts:

- **Single-writer is a better OCC story than CAS — and Chronicle already half-has it.** Akka's
  event-sourced concurrency control is *not* an append-time compare-and-swap; it's "exactly one live
  writer per `persistenceId`," enforced by Cluster Sharding. Chronicle already serializes appends per
  stream (Lua-atomic, single shard via `{path}` hash-tag) — Redis *is* the single writer, with no
  cluster or in-memory actor needed. That is strictly simpler than Akka's machinery and is a strength
  to keep. If Chronicle grows per-entity sequencing, it should extend its existing **idempotent-producer
  `ProducerSeq`** (already a proven per-`(stream, producer)` monotonic counter) toward "per-entity
  expected revision," rather than bolt on a general CAS it doesn't need.

- **Decide the cross-cutting stream at append time, or pay a full replay later.** Akka's biggest
  ergonomic tax is that the journal is per-`persistenceId`, so *every* cross-entity read model
  requires `.withTagger` at write time and a separate projection over `eventsByTag`/`eventsBySlices`.
  Chronicle has no `$all`, no category streams, no server-side secondary index. The lesson is not
  "add an index" — it's that the highest-leverage primitive is an **append-time fan-out / slice key**
  (tag-on-write into a category stream, or a deterministic slice derived from the entity key), because
  retrofitting cross-entity order after the fact means re-reading everything. This is the one
  write-side addition worth considering.

- **Keep projections async, external, and checkpointed — never a server-side inline fold.** Akka
  never computes read models at write time or on read; always a background consumer with a persisted
  per-projection offset store and an explicit at-least-once/exactly-once choice. This matches
  Chronicle's existing pull-wake subscription (lease + ack offsets) far better than a server-side
  projection engine would — and a server-side fold would inherit the **single-DS-hub serialization
  bound** noted in ground truth. The concrete gap: Chronicle's cursors are *delivery-only*; it lacks a
  **per-consumer offset-store abstraction for user read models**. Exposing "give me events for
  tag/slice T from offset X, and let me commit my read-model write + new offset together" would let an
  external projector get exactly-once the same way `R2dbcProjection.exactlyOnce` does — without
  Chronicle running any user compute.

- **If snapshots are added, make them a derivable cache, never truth.** Akka loads the latest snapshot
  then replays the tail, and `SnapshotSelectionCriteria.none` always recovers correctly from events
  alone. A Chronicle snapshot should be exactly this shape: "state as of offset X," keyed by
  `(streamPath, offset)`, foldable with a `ZRANGEBYLEX` of the tail from X, and **deletable with zero
  data loss**. Chronicle's offset is already `(ReadSeq, ByteOffset)` — a snapshot adds *no new
  invariant*, which is precisely what its proven-pure-core philosophy wants.

- **Bundled write-handlers mean shipping a compute runtime — refuse it; expose hooks instead.** Akka's
  `commandHandler`/`eventHandler` live inside the entity, but that bundling is inseparable from a
  runtime: actors, dispatchers, sharding, passivation, `rememberEntities`. For Chronicle to offer
  "bundled handlers" would mean co-locating compute with storage — the heavyweight-framework direction
  the synthesis question warns against. Note that even Akka keeps the *read-side* fold (the projection
  handler) outside the entity. The Chronicle-shaped move is to keep all handlers out of the core and,
  if anything, extend the existing optional control plane (webhook / pull-wake) so an **external**
  runtime can be the single writer per entity and own the fold.

- **Name the concurrency model explicitly — revision-CAS and single-writer are different things.**
  Akka proves a system can ship two: event-sourced (single-writer, no CAS) and durable-state (numeric
  revision CAS: "incoming revision must be existing + 1 or `persist` fails"), behind one API shape,
  plus Replicated ES (CRDT auto-merge) for active-active. The "expected version" the user asked about
  exists in Akka **only** in the durable-state/CRUD path. Chronicle's `Stream-Seq` header gestures at
  a *lexicographic, stream-scoped* guard — which is neither a per-entity numeric revision nor a
  single-writer sequence. If Chronicle adds per-entity optimistic concurrency, it should pick one
  model deliberately and document which, because conflating revision-CAS with single-writer sequencing
  is the classic event-sourcing confusion.
