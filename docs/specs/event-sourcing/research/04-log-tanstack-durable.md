# The Log Family, Client-Side Projections, and Durable Execution

> Three adjacent worlds, none of which is an "event store," but which together describe the
> design space Chronicle actually lives in. **(A)** the log-as-source-of-truth (Kafka, Pulsar) —
> the family Chronicle most resembles; **(B)** TanStack DB — a projection engine that runs *on the
> client*; **(C)** durable execution (Restate, Temporal, DBOS) — the "keyed single-writer handler
> over a log" pattern. The recurring lesson: the log is primitive, and *aggregates, expected-version,
> and projections are things you build on top of it* — exactly Chronicle's situation today.

---

## Model

**The conceptual frame.** Two maintainer essays define this family. Jay Kreps' [*The Log: What
every software engineer should know about real-time data's unifying abstraction*](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)
argues the append-only, totally-ordered log is the universal primitive: an ordered sequence where
"the order of the entries defines a notion of 'time'," and any consumer is just a deterministic
state machine replaying it. Martin Kleppmann's [*Turning the database inside out*](https://martin.kleppmann.com/2015/03/04/turning-the-database-inside-out.html)
completes the argument: a database is *already* a log plus a materialized view over it (the
replication log, the WAL). If you make the log the **source of truth** and treat every index,
cache, and read model as a **derived, recomputable view**, you have "turned the database inside
out" — *"At its core is a distributed, durable commit log… Layered on top are simple but powerful
tools for joining streams and managing large amounts of data."* This is the whole thesis: **the
event log is authoritative; derived views are disposable functions of the log.**

**Kafka.** A [topic](https://kafka.apache.org/43/streams/core-concepts/) is a named feed split into
**partitions**; each partition is "an ordered, replayable, and fault-tolerant sequence of immutable
data records, where a data record is defined as a key-value pair." A record's identity is
`(topic, partition, offset)` — the **offset** is a monotonic per-partition integer cursor. Ordering
is *only* guaranteed within a partition; the partition key decides placement. There is no notion of
an "event type" beyond the bytes and an optional schema (via Schema Registry); typing is a
convention, not a primitive.

**Pulsar.** A [topic](https://pulsar.apache.org/docs/next/concepts-messaging/) carries **messages**
whose components include a `Key` ("useful for features like topic compaction"), a producer-assigned
`Sequence ID` ("indicating its order in that sequence… can be used for message deduplication"), and
a bookie-assigned `Message ID` that "indicates a message's specific position in a ledger and is
unique within a Pulsar cluster." So Pulsar has *two* identifiers: the producer's logical sequence
number and the storage-layer position — a cleaner separation than Kafka's single offset.

**The crisp point — a log is NOT an event store.** Neither Kafka nor Pulsar has:
- **per-entity streams** — you get topics/partitions, not one stream per aggregate instance. To
  read "all events for order #4173" you must filter a partition or maintain your own index.
- **per-entity expected-version** — there is no "append iff this entity is at revision N" (see next
  section). The unit of optimistic control does not exist.
- **an aggregate boundary** — nothing enforces a consistency/transaction boundary around a logical
  entity. CQRS, aggregates, and invariants are things *you bolt on top yourself*.

This is **Chronicle's exact situation.** Chronicle's stream is a URL-addressable append-only byte
sequence with a `{ReadSeq, ByteOffset}` offset — structurally a single Kafka partition with HTTP
semantics. Like Kafka/Pulsar it moves bytes, orders within a stream, and stops there. Everything
event-sourced (entities, folds, projections) is left to the application.

**Where TanStack DB and durable execution sit in the model.** TanStack DB's unit is a **collection**
(a typed row-set) and the source of truth is whatever syncs into it; the log is implicit (an
ElectricSQL shape, a query result). Durable-execution engines make the log *explicit and internal*:
Temporal's **Event History** is literally a per-workflow event log ("the Temporal Service tracks the
progress of each Workflow Execution by appending information about Events… to the Event History"
[[Temporal: Events and Event History](https://docs.temporal.io/workflow-execution/event)]), and
Restate journals each handler invocation. In all three, **state = fold(log)** — the event-sourcing
identity — even when the log is hidden.

---

## Optimistic concurrency

**The log family has no per-entity expected-version. Full stop.** This is the single sharpest gap
between a log and an event store (EventStoreDB's `ExpectedRevision`, Akka's per-persistence-id
sequence). Kafka's producer API offers no "append iff partition is at offset N" conditional write;
appends are unconditional. The closest mechanisms are about **dedup**, not **conflict detection**:

- **Kafka idempotent producer.** A producer gets a PID + monotonic per-partition sequence number;
  the broker rejects duplicates and out-of-order sequences, giving exactly-once *append* semantics.
  This deduplicates retries — it does **not** let two writers detect that they raced on the same
  logical entity.
- **Pulsar message deduplication.** With `brokerDeduplicationEnabled=true`, "the sequence ID of each
  message is unique within a producer of a topic… can be used for message deduplication"
  [[Pulsar messaging](https://pulsar.apache.org/docs/next/concepts-messaging/)]. Same story: per-
  *producer* idempotence, not per-*entity* OCC. Pulsar additionally enforces that "only one producer
  with the same name can be publishing on a topic at any given time" — a *producer*-fencing rule, not
  an entity-revision check.

**Chronicle is already at this exact tier and slightly past it.** Chronicle's idempotent-producer
oracle `(ProducerId, ProducerEpoch, ProducerSeq)` — first seq 0, increment-by-one, stale-epoch
fenced (409), duplicate → 204 — is *the same mechanism as Kafka's idempotent producer and Pulsar's
producer dedup*, and Chronicle's is **Lean-proven**. Chronicle's optional `Stream-Seq` lexicographic
guard is a *stream-scoped* expected-order check — strictly more than Kafka/Pulsar offer, but still
not per-*entity* numeric revision. **Conclusion: copying the log family buys Chronicle nothing new
for OCC; it already matches them. Per-entity expected-version is the thing none of them have, and is
precisely what distinguishes an event store from a log.**

**The sideways escape used by durable execution.** Restate's **Virtual Object** sidesteps OCC
entirely: "At most one handler with write access can run at a time per object key. Mimicks a queue
per object key" [[Restate: Services](https://docs.restate.dev/foundations/services)]. If there is a
**single writer per key**, there is no concurrency to be optimistic about — serialize at the key and
the conflict cannot arise. This is a different answer to the same problem and is highly relevant to
Chronicle (see Lessons).

**TanStack DB's "optimistic" is a different word.** Its [optimistic mutations](https://tanstack.com/db/latest/docs/guides/mutations)
mean *apply the write locally immediately, sync in the background, roll back on server rejection* —
optimistic *UI*, not optimistic *concurrency control*. Conflict resolution against the authoritative
server is the server's job; the client just reconciles. Worth flagging so the two senses don't blur.

---

## Snapshots

**Log compaction is the log family's snapshot.** This is the key insight. Kafka
[log compaction](https://docs.confluent.io/kafka/design/log_compaction.html) (`cleanup.policy=compact`)
retains "the latest value for each message key in a topic, while discarding older values," so that
"the log contains a full snapshot of the final value for every key, not just keys that changed
recently." A tombstone (null value for a key) deletes it. The doc names the use case directly:
*"Event sourcing. While not enabled by compaction, compaction does ensure you always know the latest
state of each key, which is important for event sourcing."* A compacted topic **is** a continuously-
maintained snapshot keyed by entity id — recovery = read the compacted head, then tail the rest.
Pulsar has the same primitive as [topic compaction](https://pulsar.apache.org/docs/5.0.x/concepts-topic-compaction/),
producing a compacted ledger consumers can read instead of replaying full history.

Caveat from the source: compaction is **best-effort and non-deterministic** — *"Compaction in Kafka
does not guarantee there is only one record with the same key at any one time… compaction timing is
non-deterministic"* — so it is a space-reclamation/latest-value optimization, **not** a transactional
point-in-time snapshot. It also requires keys: "Compacted topics must have records with keys."

**Kafka Streams: changelog-backed state-store snapshots.** A stateful operator keeps local state in a
RocksDB store; that store is *itself* backed by a compacted **changelog topic** in Kafka. On failure,
the store is rebuilt by replaying the changelog (and, for fast restarts, restored from a local
checkpoint). The compacted changelog is the snapshot; the offset is the recovery cursor. This is the
"journaling for high-availability" use case the compaction doc calls out: "a process that does local
computation can be made fault-tolerant by logging out changes… so another process can reload these
changes and carry on."

**Durable execution: replay, with continue-as-new as the snapshot.** Temporal recovers a workflow by
**replaying its entire Event History** through the deterministic workflow code — no separate snapshot;
the log *is* the state. Because unbounded histories are costly ("the Temporal Service places limits on
both the number and size of items in the Event History"), the pattern is **continue-as-new**: close
the current history and start a fresh execution carrying forward only the distilled state — a manual
snapshot-and-truncate. Restate Virtual Objects keep **K/V state "retained indefinitely and shared
across requests,"** so the materialized state lives beside the journal rather than being re-folded
each time.

**TanStack DB: the collection is the snapshot.** A synced collection *is* the materialized current
state on the client; it is hydrated once and then kept live by deltas, so there is no fold-from-zero
on each read. Local-storage/local-only collections persist that snapshot across reloads.

**Chronicle today has none of this.** No compaction (the ZSET keeps every frame until TTL), no
latest-value-per-key view, no state-store changelog. The log-family lesson is concrete: **the cheapest
snapshot Chronicle could add is key-compaction of a stream** (or a sibling "latest" stream), not a
bespoke aggregate-snapshot subsystem.

---

## Projections / read models

This is the center of gravity for the whole research, because the log family and TanStack DB give two
*opposite* answers to "where and when is the read model computed."

### Kafka / Pulsar: continuous, write-time, incremental — on the server tier

**Stream–table duality** is the foundational idea: a stream of keyed updates and a table (latest value
per key) are two views of the same data, and you can convert between them losslessly — replaying a
changelog *builds* a table; observing a table's changes *emits* a stream. Kafka Streams encodes this
directly as **`KStream`** (event stream) vs **`KTable`** (changelog/materialized table), where a
[`KTable`](https://docs.confluent.io/platform/current/streams/concepts.html) is a materialized view
kept continuously up to date as records arrive.

- **When computed:** *continuously, at write/ingest time*, one record at a time ("one-record-at-a-time
  processing to achieve millisecond processing latency" [[Kafka Streams core concepts](https://kafka.apache.org/43/streams/core-concepts/)]).
  The projection is **incremental** — each event updates the aggregate; you never refold from zero.
- **Where it lives:** in **state stores** (RocksDB) local to each stream-processing instance, queryable
  in-process via [**Interactive Queries**](https://kafka.apache.org/43/streams/developer-guide/interactive-queries/)
  — "let you leverage the state of your application from outside your application… treat the application
  itself as a database." **ksqlDB** exposes the same as SQL `CREATE TABLE … AS SELECT` materialized
  views over topics.
- **Checkpoints / offsets:** the projection's progress *is* the consumer's committed **offset**.
- **Replay / rebuild:** reset the consumer group's offset to 0 (or to a snapshot offset) and reprocess.
  Because the projection is a pure function of the log, rebuilds are routine — Kleppmann's "derived data
  is recomputable" made operational.

### TanStack DB: the same incrementality, moved to the CLIENT

TanStack DB is the direct answer to the intuition *"projections are interesting, and you shouldn't
recompute the aggregate each time."* It is, in its own words, "the reactive client store for your API"
with "sub-millisecond live queries" [[overview](https://tanstack.com/db/latest/docs/overview.md)]. The
mechanism is the important part:

- **Live queries are incremental view maintenance.** "The query builder… composes your query into an
  optimal incremental pipeline that gets compiled and executed efficiently" and results "automatically
  update when the underlying data changes" [[live queries](https://tanstack.com/db/latest/docs/guides/live-queries)].
- **The engine is differential dataflow.** TanStack DB's query engine is **d2ts**, "a TypeScript
  implementation of [differential dataflow]… process data as it comes in, and only recompute the parts
  that have changed" [[electric-sql/d2ts](https://github.com/electric-sql/d2ts)]. The ElectricSQL
  maintainers state it plainly: *"Enter Sam Willis' work on d2ts, a TypeScript implementation of
  differential dataflow that can handle even the most complex reactive queries in microseconds"*
  [[Electric: Super-fast apps on sync with TanStack DB](https://electric.ax/blog/2025/07/29/super-fast-apps-on-sync-with-tanstack-db)].
  A `groupBy`/`join`/aggregate over a collection updates by **delta**, never by refolding the whole set
  — exactly "don't recompute the aggregate each time," implemented as a dataflow graph.
- **Derived collections = materialized views.** A live query "resolves to collections that automatically
  update," and the docs frame derived collections as "materialised views" — you can stack a projection on
  a projection.
- **Where it lives:** entirely **in the browser**. Collections sync from the server (ElectricSQL/PowerSync
  **shapes**, TanStack Query, or any source); the *read model is computed and maintained on the client*,
  not on a server projection tier.
- **Optimistic local writes:** a mutation "instantly applies optimistic state," then syncs; rows expose
  `$synced` (confirmed by sync vs still optimistic) and `$origin` (local vs remote) virtual columns so the
  UI can distinguish pending from durable.

**The framing that matters for Chronicle:** Kafka Streams and TanStack DB run the *same algorithm*
(incremental, delta-based view maintenance) at *opposite ends of the wire*. Kafka materializes on the
server and exposes interactive queries; TanStack materializes on the client and exposes live queries. If
the projection can live on the client and stay incrementally correct from a synced log, **the server may
not need a projection subsystem at all** — it only needs to ship the log (or a compacted view of it)
efficiently.

---

## Handlers & consumers

### Are handlers bundled with the stream/entity?

- **Kafka: no.** Consumers are external. A **consumer group** gives competing consumers: partitions are
  distributed across group members (one partition → one consumer in the group at a time), and progress is
  a **committed offset** stored in the internal `__consumer_offsets` topic
  [[consumer design](https://docs.confluent.io/kafka/design/consumer-design.html)]. Delivery is
  **at-least-once** by default (process, then commit); exactly-once needs the transactional
  read-process-write protocol. Pull-based: consumers poll. Kafka Streams *embeds* handler logic in your
  app process, but the topic itself carries no code.

- **Pulsar: yes — handlers can be attached to topics.** This is the distinctive contrast.
  **[Pulsar Functions](https://pulsar.apache.org/docs/next/functions-concepts/)** are lightweight compute
  units that the broker runs: "A function instance… [is] a collection of consumers consuming messages from
  different input topics, an executor that invokes the function, [and] a producer that sends the result…
  to an output topic." Each function "has a separate state store with FQFN… to persist intermediate
  results in BookKeeper," and other clients can query that state. **Pulsar IO** connectors (sources/sinks)
  are the same idea for external systems. So Pulsar offers **bundled handlers at *topic* granularity** —
  the analog of Akka's per-entity handlers, but coarser (per topic, not per aggregate instance).

- **Subscription modes (Pulsar) decide the consumer topology** [[SubscriptionType](https://pulsar.apache.org/api/client/4.2.x/org/apache/pulsar/client/api/SubscriptionType.html)]:
  - **Exclusive** — "only 1 consumer on the same topic with the same subscription name."
  - **Failover** — multiple consumers share the subscription, "but only 1 consumer will receive the
    messages" (the rest are hot standbys).
  - **Shared** — "messages dispatched according to a round-robin rotation between the connected consumers"
    (competing consumers; order not guaranteed).
  - **Key_Shared** — "all messages with the same key will be dispatched to only one consumer" — i.e.
    **per-key ordered, competing across keys**. This is the messaging-layer version of "single consumer
    per entity," and the direct analog of Chronicle's per-`{path}` shard placement.

- **Durable execution: yes — the handler *is* the entity.** Restate **Virtual Objects** are "stateful
  entities identified by a unique key" with built-in K/V state and "single writer per key (+ concurrent
  readers)" [[Restate Services](https://docs.restate.dev/foundations/services)]. A Virtual Object is
  essentially an **event-sourced actor fronting a durable log**: keyed, single-writer, state =
  fold(journal), with execution itself journaled and replayed on crash. Temporal **Workers** poll task
  queues, execute workflow/activity code, and the **Event Loop** drives the workflow by feeding new
  history events and replaying deterministically; "this information… is essential for providing Durable
  Execution, since it enables the Workflow Execution to recover from a crash and continue making
  progress." DBOS does the same with Postgres as the journal (durable workflow steps recorded in a
  table, replayed on restart). In this family the boundary between "consumer," "handler," and "entity"
  collapses: **one keyed, single-writer, replayable handler per id.**

### Push vs pull, checkpointing, delivery guarantees — summary

| System | Delivery model | Checkpoint | Guarantee |
|---|---|---|---|
| Kafka consumer group | pull | committed offset (`__consumer_offsets`) | at-least-once; exactly-once via txns |
| Kafka Streams | pull + local state | offset + changelog topic | exactly-once (EOS) |
| Pulsar subscription | push (broker dispatches) | per-subscription ack cursor | at-least-once; effectively-once for Functions |
| Restate Virtual Object | invoke (single-writer queue per key) | journaled steps + K/V state | exactly-once per key |
| Temporal Workflow | worker polls task queue | Event History | exactly-once workflow semantics via replay |
| **Chronicle pull-wake** | **pull (tail `__wake/{subId}`)** | **acked offset (cursor)** | **exactly-once via fencing (epoch/gen)** |

**Chronicle's pull-wake subscriber already lands in this table as a peer.** Leased slots + a private wake
stream + acked offsets + epoch fencing is *structurally* Pulsar's Failover/Key_Shared subscription plus
Restate's single-writer-per-key — built from Chronicle's own primitives and TLA+-checked. Chronicle does
**not** today bundle handler *code* with a stream (Pulsar Functions / Restate Virtual Objects do); it ships
*wake signals* to external workers instead.

---

## Lessons for Chronicle

1. **Chronicle is squarely in the log family (A), and that is a defensible identity — not a deficiency.**
   It is a single Kafka partition with HTTP semantics, idempotent producers, and a proven core. Per the
   Kreps/Kleppmann thesis, the log is the *right* primitive to be; "event-sourced entities" are an
   application layer, and EventStoreDB-style per-entity streams are one option, **not** the only path.
   Don't reflexively grow Chronicle into an event store; first decide whether the missing pieces belong
   on the server at all.

2. **TanStack DB (B) is the strongest argument that Chronicle may never need a server-side projection
   tier.** The same incremental, delta-based view maintenance (differential dataflow / `KTable`) that
   Kafka runs on the server, TanStack DB runs *on the client* in microseconds — "only recompute the parts
   that have changed." Chronicle's job then shrinks to **shipping the log efficiently** (it already has
   long-poll + SSE tailing and offset cursors); the client folds it into whatever read model it needs and
   keeps it live. This also dodges Chronicle's known **single-hub serialization** constraint: a server-side
   fold would inherit that bottleneck, whereas client-side projection puts the compute on N clients. *The
   projection question may be answered by "push it to the edge," not "build it into the server."*

3. **If Chronicle wants cheap server-side snapshots, the log family says: add key-compaction, not an
   aggregate-snapshot subsystem.** A compacted stream (`cleanup.policy=compact` semantics — latest value
   per key, tombstone deletes) gives "a full snapshot of the final value for every key" for free, and is
   exactly how Kafka Streams state stores and Pulsar topic compaction bound their storage. This fits
   Chronicle's ZSET-per-stream model (a compacted sibling keyed by an app-supplied message key) far better
   than bolting on EventStoreDB-style snapshot events. Note the honest caveat: compaction is best-effort
   and non-deterministic, a latest-value cache, not a transactional point-in-time snapshot.

4. **The cleanest route to "event-sourced entities" is the durable-execution route (C): one keyed,
   single-writer handler over the log — not per-entity optimistic concurrency.** Restate's Virtual Object
   ("single writer per key… mimics a queue per object key") gets the *result* event sourcing wants (an
   entity whose state is fold(log), with no write conflicts) by **serializing at the key** instead of by
   detecting version clashes. Chronicle already has the ingredients: per-`{path}` shard placement (a key →
   one shard → Lua-serialized writes) and epoch-fenced single-holder leases. A "Chronicle Virtual Object"
   would be a stream whose appends are serialized per key with an optional bundled fold — far closer to
   Chronicle's grain than importing `ExpectedRevision`.

5. **Idempotent producers are Chronicle's hidden head-start; don't re-solve what Kafka/Pulsar only
   approximate.** Chronicle's `(ProducerId, Epoch, Seq)` oracle is the *same* mechanism as Kafka's
   idempotent producer and Pulsar's `brokerDeduplicationEnabled`, and Chronicle's is Lean-proven. This is
   exactly the substrate durable execution needs for exactly-once handler invocation. The gap to close for
   event-sourced entities is **per-entity expected-version** — the one thing *no* system in this entire
   family (log or durable-execution) provides via OCC; they all either skip it (logs) or sidestep it via
   single-writer-per-key (durable execution). That strongly suggests Chronicle should reach for
   **single-writer-per-key**, which it can already express, rather than per-entity numeric revisions, which
   it cannot cheaply enforce across shards.

6. **Bundled handlers are a real fork in the road — and the log family shows both prongs.** Kafka keeps
   handlers *out* of the broker (external consumers, pull, at-least-once); Pulsar pulls them *in* (Functions/
   IO attached to topics, per-topic state in BookKeeper). Chronicle today is Kafka-shaped (wake signals to
   external workers). Moving toward Pulsar-shaped (a fold/handler attached to a stream, run server-side)
   would buy ergonomics but **re-import the single-hub serialization risk unless the handler is sharded by
   the stream's `{path}` key** — which, conveniently, is how Chronicle already shards storage. If handlers
   are ever bundled, bundle them **per key, single-writer**, mirroring Pulsar Key_Shared + Restate Virtual
   Objects, so the proven per-shard serialization is the concurrency model rather than a new global lock.
