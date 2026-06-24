# Event sourcing and Chronicle: answering the question

You asked four things. How were event sourced systems like EventStoreDB, Akka, and Axon built, and
how do they handle the parts you are used to seeing, such as snapshots, projections, and Akka
bundling handlers with the stream. Whether Chronicle's durable streams are the same kind of thing as
an event sourcing backend like EventStoreDB, and where they are similar and where they differ. Where
TanStack DB fits for projections. And whether Chronicle can and should grow toward true event sourced
entities.

This document answers those questions in order. The detailed design recommendation, with an API
sketch and the proof obligations, is in [`LANDSCAPE.md`](LANDSCAPE.md). The grounded, source cited
research for each system is in [`research/`](research/).

The short answer up front. Chronicle is not behind an event store. It is the durable log layer that
an event store is built on top of. It already matches the log family on idempotent producers, and it
is stronger than EventStoreDB on delivery because its pull-wake path is exactly once. The features you
associate with event sourcing, such as snapshots and projections, are application concerns in most of
these systems, not storage features. So the right move is to add a small amount of substrate to
Chronicle and keep the entity logic outside it, rather than turn Chronicle into a framework.

## How these systems are built

All of these systems share one idea. They store the events, not the current state. To get an
entity's current state you read its events in order and fold them into a value. A bank account is the
sum of its deposits and withdrawals, computed by replaying them.

**One stream per entity.** EventStoreDB names a stream after the entity, so the stream `order-123` is
the order with id 123, and its events folded in order are its state. Akka gives each entity a
`persistenceId` and stores one event log per id. Axon stores one event sequence per aggregate id.
Marten stores one stream per id in Postgres. In every case the entity is the unit, and the per entity
event count gives a version number that starts at zero and goes up by one.

**Snapshots exist because folding a long history on every load is slow.** The pattern is the same
everywhere. You save the folded state every so often, and on load you read the latest saved state and
then replay only the events after it. EventStoreDB has no built in snapshot at all, and its own docs
tell you not to add one early, because reads are fast and a snapshot adds a write on every change. The
EventStoreDB pattern is to write your snapshots into a separate stream and keep only the last one.
Akka and Axon ship a built in snapshot trigger, for example "snapshot every 100 events." Marten does
not have a separate snapshot feature at all. In Marten a snapshot is just a read model that it keeps
up to date for you, which is a useful way to see what a snapshot really is. The rule all of them share
is that the snapshot is a cache you can delete at any time, and the events are the source of truth.

**Projections are read models, and the one real decision is when you compute them.** A projection
reads events and folds them into a shape that is good for queries, such as a table of order totals.
There are three choices for when this happens, and Marten is unusual because it names all three as one
setting.

- Compute on read. You fold the events every time you query. Nothing is stored. This costs nothing to
  maintain but gets slower as the stream grows.
- Compute on write. You update the read model in the same transaction as the event append, so the
  read model is always current. This is safe only when the projection covers a single stream. The cost
  is that every write now pays for the projection.
- Compute in the background. A separate process reads the events and updates the read model after the
  fact. The read model is a little behind, which is the cost, but this scales and is the normal choice
  for anything that spans many entities.

A projection keeps a checkpoint, which is the position in the log it has processed so far. The
checkpoint lets the projection resume after a restart, and it lets you rebuild the read model from
scratch by resetting the checkpoint to the beginning and replaying. Axon calls this checkpoint a
tracking token, Akka calls it an offset store, and Marten calls it a projection progression. They are
the same thing under three names, and in all of them the checkpoint is kept outside the event log so
that rebuilding never changes history.

**Bundling handlers with the entity is an Akka and Axon choice, and it has a cost.** In Akka an event
sourced entity is an `EventSourcedBehavior` that holds two functions. The command handler decides what
to do with a command and may reject it or produce events. The event handler takes an event and folds
it into the new state. Both live inside the entity. Axon does the same with annotations inside the
aggregate class. This is convenient to write. The cost is that it only works inside a runtime that
hosts those entities and routes each command to the one place the entity lives. Akka needs cluster
sharding, passivation, and serializers to make this work, all on the JVM. EventStoreDB and Marten do
not bundle handlers. In those systems your own application code does the deciding, and the store only
keeps events. Even Akka keeps the read side fold outside the entity. Only the
write side decide and evolve steps are bundled.

## The two design decisions that actually divide these systems

Most of the differences come down to two choices.

**How do you stop two writers from corrupting one entity.** There are two answers, and people use the
single phrase "optimistic concurrency" for both, which hides the difference.

- Expected version. When you append, you say which version you think the stream is at, for example
  "append only if this stream has exactly 42 events." The store rejects the write if someone else
  wrote first, and it tells you the real version so you can read again and retry. EventStoreDB, Axon,
  and Marten work this way. They need this check because they allow several clients to write the same
  stream.
- Single writer. You guarantee that only one writer is ever active for an entity, so there is no
  conflict to detect and the next version is just the last one plus one. Akka's event sourced path
  works this way, using cluster sharding to keep exactly one live writer per entity. Restate and
  Pulsar's key based subscriptions work this way too.

The detail that clears up the confusion: Akka's event sourced path uses single writer and has no
version check. The numeric version check in Akka exists only in its separate state storing path, the
one that keeps the latest value instead of an event log. So expected version is not the same as event
sourcing. It is one of two ways to make a safe single writer entity. The log systems, Kafka and
Pulsar, have neither check. They only remove duplicate retries from a single producer, which is not
the same as detecting that two writers raced.

**Where does the projection run.** EventStoreDB can run projections inside the server as JavaScript,
but its own docs discourage this in production for two reasons. The projections multiply the number of
writes, because each one emits more events, and they run only on the leader node, so they cannot
scale out. Kafka and ksqlDB run projections on the server tier and keep them up to date one record at
a time. TanStack DB runs the projection on the client instead, which the next section covers.

## Is Chronicle an event sourcing backend like EventStoreDB

It is the layer underneath one. The honest comparison is that Chronicle is a single Kafka partition
with HTTP semantics, not an EventStoreDB.

Where they are the same:

- Both are append only logs that you read by position.
- Both have idempotent producers that drop duplicate retries.
- Both order events within a stream.

Where they differ, and these are the gaps:

- EventStoreDB makes one stream per entity the convention and gives a per entity numeric version with
  an expected version check on append. Chronicle's stream is any path you choose, and its only version
  guard is the `Stream-Seq` header, which is per stream and compares strings rather than a per entity
  number. So Chronicle does not yet give you the load, decide, append loop with conflict detection.
- EventStoreDB builds index streams for you, so you can subscribe to "all orders" or "all events of
  this type." Chronicle has no such index and no global stream of everything.
- EventStoreDB and the others have a snapshot pattern. Chronicle has no snapshots.

Where Chronicle is ahead:

- Chronicle's idempotent producer is proven correct in Lean. Kafka and Pulsar only test theirs.
- Chronicle's pull-wake delivery is exactly once, enforced by fencing that is checked in TLA+.
  EventStoreDB's competing consumer subscriptions are only at least once, so they can deliver an event
  more than once.
- Chronicle gets single writer for free. Every key of a stream lives on one Redis shard, and Redis runs
  one command at a time, so Redis is the single writer. Akka builds a whole cluster sharding system to
  get the same guarantee.

So the gap is narrow and specific. Chronicle is missing the entity boundary, snapshots, and read
models. The research says two of those three should be conventions and contracts, not server features.

## Where TanStack DB fits

TanStack DB answers your intuition that projections are interesting and that you should not recompute
the aggregate on every read. It moves the projection to the client.

The client holds a local set of rows, called a collection, that is kept in sync with the server. A
live query over that collection updates only the parts that changed when new data arrives, using a
technique called differential dataflow, implemented in a library called d2ts. So a grouped count or a
join does not refold the whole set on each change. It applies the change as a small update. You can
also stack one query on another, which gives you a read model built from another read model.

This affects Chronicle directly, for one concrete reason. A projection that runs on the server would run
through the single durable streams hub, which is the part of Chronicle that already serializes the
wake path and leaves the other tiers idle under load. A server side fold would inherit that
bottleneck. If the projection runs on the client instead, the server only has to ship the log, which
Chronicle already does well with long polling, server sent events, and offset cursors. The cost of
the client side approach is that each client now runs the projection and you need a sync protocol to
feed it, so it is more work on the client and more moving parts than a single server view.

## Can and should Chronicle be extended

Yes, but as a small amount of substrate, and not as a framework. Keep the entity logic, meaning the
handlers and the single writer ownership, in a process outside Chronicle that uses Chronicle's
existing lease and fencing primitives. Chronicle should provide the durable log, the concurrency
contract, the read model checkpoint store, and the fan in key. It should not host your handlers or run
your folds.

There are two coherent ways to get an event sourced entity, and a deployment can use either one per
entity.

- The expected version path. Add a numeric expected version header to the append. Reject a stale write
  with a 409 that echoes the real version. This gives the classic load, decide, append loop for the
  case where many independent clients write the same entity. It is cheap because writes already run in
  one Redis script, so the check is a small addition to logic that Chronicle already proves in Lean.
  The cost is a new numeric version that you store and check, and you must decide and document that it
  is a per entity number and not the existing string compare.
- The single writer path. Put a single holder lease in front of an entity stream, so one external
  worker is the only writer and owns the fold. This is the Restate and Akka model, expressed with
  primitives Chronicle already has and has proven. It needs little new code. The cost is that you now
  run an external worker per active entity and depend on the lease being correct, which Chronicle's
  fencing already covers.

The single writer path fits Chronicle's architecture better, because Chronicle already serializes per
stream. It is the one to lead with.

The supporting pieces, which are the same on both paths:

- Snapshots. Do not build a snapshot subsystem. Document the snapshot stream pattern, which already
  works today using a stream and offsets, where a snapshot is the state as of a given offset and you
  load it and replay the tail after it. You can also add optional key compaction, meaning the log keeps
  only the latest value per key, which is how Kafka and Pulsar bound their storage. The cost of
  compaction is that it is best effort and not a point in time snapshot, so it is a latest value cache
  and nothing more.
- Projections. Prefer the client side approach above. The one server side primitive worth adding is a
  per consumer checkpoint store for an external projector, so the projector can commit its read model
  write and its new checkpoint together and get exactly once. Reuse the existing fenced cursor, keep
  the checkpoint out of the log, and allow a reset to the beginning to rebuild. Do not build a server
  side projection engine, because that is the EventStoreDB trap of leader only execution made worse by
  Chronicle's single hub.
- Cross entity reads. Only if you adopt one stream per entity, add a category or slice key on append
  that also writes into a derived index stream, so you can read "all orders" without knowing every id.
  Keep that index as derived data that you can rebuild, not as primary storage. The cost is the extra
  writes for the index, which is why it should be optional.

What to refuse, and why. A server side projection engine, because it inherits the single hub
serialization bound. Server resident command and event handlers, because they require hosting a
compute runtime and break the property that any language can talk to Chronicle over HTTP. A bespoke
snapshot subsystem, because a snapshot adds no new guarantee and can be a convention. Cross stream
transactions, because no system in this family offers them and they would be a large new burden of
proof.

The test for whether any addition belongs in Chronicle is simple. It must be provable as a small
tightening of an existing proof, as expected version is, or it must add no new guarantee at all, as
snapshots do. And no addition may introduce a fold that runs on the server across many streams,
because that is the one shape Chronicle is built to avoid.

## What to read next

- [`LANDSCAPE.md`](LANDSCAPE.md): the design reference, with a comparison table of nine systems, the
  five design axes in detail, the scoped gaps, a phased extension plan, an API sketch, and the Lean
  and TLA+ proof obligations.
- [`TANSTACK-DB.md`](TANSTACK-DB.md): a from-first-principles guide to the client-side materialization
  stack: the State Protocol, MaterializedState, TanStack DB, and the d2ts differential-dataflow engine.
- [`research/01-eventstoredb.md`](research/01-eventstoredb.md): EventStoreDB and KurrentDB.
- [`research/02-akka.md`](research/02-akka.md): Akka Persistence, sharding, query, and projections.
- [`research/03-axon-marten.md`](research/03-axon-marten.md): Axon and Marten, the projection spectrum.
- [`research/04-log-tanstack-durable.md`](research/04-log-tanstack-durable.md): the Kafka and Pulsar
  log family, TanStack DB as a client side projection, and durable execution such as Restate and
  Temporal.
- [`research/00-chronicle-ground-truth.md`](research/00-chronicle-ground-truth.md): what Chronicle is
  today, the fixed reference the research compares against.
