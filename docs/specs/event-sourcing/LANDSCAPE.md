# Event sourcing & durable streams — the landscape, the gaps, and how far to extend Chronicle

> Synthesis of the four grounded research files in [`research/`](research/) against Chronicle's
> [ground truth](research/00-chronicle-ground-truth.md). Sources: EventStoreDB/KurrentDB
> ([research/01](research/01-eventstoredb.md)), Akka Persistence
> ([research/02](research/02-akka.md)), Axon + Marten ([research/03](research/03-axon-marten.md)),
> the Kafka/Pulsar log family + TanStack DB + durable execution
> ([research/04](research/04-log-tanstack-durable.md)). Every primary-source citation lives in those
> files; this document reasons over them.

---

## TL;DR

Chronicle is **not behind** an event store — it is a **durable log primitive** (a single Kafka
partition with HTTP semantics) that already matches the log family on idempotent producers (and is
*Lean-proven* where they are not) and **beats** EventStoreDB on subscription delivery (exactly-once
via fencing vs at-least-once persistent subscriptions). "Event-sourced entities" are an *application
layer*, and there are two legitimate ways to get them. The research converges hard on five points:

1. **The only real gap is the entity boundary**: one-stream-per-entity + a way to control
   concurrency on it. There are **two** answers, and they are *alternatives*, not add-ons:
   **(A) per-entity expected-version CAS** (EventStoreDB, Axon, Marten) — needed *because* those
   systems let multiple clients write one stream; **(B) single-writer-per-key** (Akka event-sourced,
   Restate Virtual Objects, Pulsar Key_Shared) — no CAS because conflicts can't arise. **Chronicle
   already enforces single-writer per stream** (Redis is single-threaded per shard; all of a stream's
   keys hash-tag to one shard; appends are Lua-atomic), so it sits structurally in camp **B**.
2. **Don't build a snapshot subsystem.** Snapshots fall out of *(single-stream fold + an optional
   materialized doc)* or *key-compaction*. A Chronicle snapshot is just "state as of offset X,"
   foldable from a `ZRANGEBYLEX` tail and deletable with zero data loss — **it adds no new invariant**.
3. **Keep projections off the server.** EventStoreDB's own docs warn server-side projections cause
   write-amplification and run leader-only; that would compound Chronicle's known **single-DS-hub
   serialization** bound into a double bottleneck. The modern answer (TanStack DB) is **client-side
   incremental view maintenance** — the server may need *no* projection tier, only efficient log
   shipping (which Chronicle already has: SSE/long-poll + offsets).
4. **Projection checkpoints are a reuse opportunity, not a new subsystem.** Axon's `TokenStore` and
   Marten's progression table are one artifact: a per-consumer offset kept *out of the log*, resettable
   to replay. Chronicle already has this exact shape in its fenced pull-wake cursor.
5. **Refuse server-resident handlers.** Bundling command/event handlers in the entity (Axon, Akka) is
   inseparable from hosting a compute runtime (actors, sharding, passivation). Even Akka keeps the
   *read-side* fold external. Expose the **contract** (Marten's `FetchForWriting` decider:
   read-current → app decides → append) — which is HTTP-shaped and polyglot — not a runtime.

**Recommendation in one line:** extend Chronicle toward event-sourced entities **only** along the
axes that are *storage* concerns and provable as "numeric tightening of an existing proof" or "no new
invariant"; put the *entity runtime* (ownership, handlers, folds) in an **external** process built on
Chronicle's existing lease/fence primitives. Concretely: ship a per-stream `expected-version` header,
a documented single-writer-via-lease pattern, an exactly-once **projection offset-store** contract,
and append-time **category/slice keys** — and refuse a server-side projection engine, server-resident
handlers, a bespoke snapshot primitive, and cross-stream transactions.

---

## The landscape at a glance

| | **Unit of storage** | **Concurrency control** | **Snapshots** | **Projection computed…** | **Handlers bundled?** | **Consumer model** |
|---|---|---|---|---|---|---|
| **EventStoreDB** | stream = aggregate; gap-free per-stream `revision`; global `$all` | **expected-version CAS** (`NoStream`/`StreamExists`/exact-N → `WrongExpectedVersion`, echoes actual) | none built-in; app-level `snapshot-{id}` stream, `$maxCount=1` | server-side JS, **async/write-time** (leader-only, write-amplifying — discouraged) | no | catch-up (client checkpoint, ordered) **+** persistent (server checkpoint, competing, at-least-once, unordered) |
| **Akka (event-sourced)** | `persistenceId` journal; per-pid `sequenceNr` | **single-writer** (Cluster Sharding: one live writer per pid; *no CAS*) | separate `SnapshotStore`; `snapshotEvery(N)`; recover = snapshot + tail | **async**, external (Akka Projection over `eventsByTag`/slices) | **yes** (`commandHandler`+`eventHandler` inside the entity) — needs a runtime | pull stream + per-projection offset store; exactly-once = offset committed in the read-model txn |
| **Akka (durable-state)** | latest state row | **revision CAS** ("incoming revision = existing+1 or fail") | n/a (state is the row) | n/a | yes | state-change query |
| **Axon** | aggregate; `(aggregateId, seqNr)` | **CAS** via unique key on `(aggregateId, seqNr)` → `ConcurrencyException` | built-in `Snapshotter` + `SnapshotTriggerDefinition(N)` | external event processors; **subscribing** (inline) **or** tracking (async, `TrackingToken`) | **yes** (`@CommandHandler`+`@EventSourcingHandler`) | tracking processors; segments + claim/steal; at-least-once |
| **Marten** | stream + events (Postgres); `IEvent.Version` | **expected-version** via `FetchForWriting` → `ConcurrencyException` | "snapshot" *is* an inline single-stream projection | **`Live` / `Inline` / `Async`** (the lifecycle dial) | **no** (decider in app code; Wolverine adds the feel) | async daemon; **HotCold leader = one node per projection**; inline ≈ exactly-once |
| **Kafka** | topic/partition; `offset` | **none** (idempotent producer = dedup, not OCC) | **log compaction** (latest-value-per-key) | continuous/incremental on the server (`KTable`/ksqlDB, RocksDB state stores) | no (Streams embeds in *your* app) | consumer groups (pull, competing, committed offset); at-least-once |
| **Pulsar** | topic; `Message ID` + producer `Sequence ID` | none (producer dedup; one named producer per topic) | topic compaction | server-side (Functions) | **yes** (Functions/IO attached to topics, per-topic state) | Exclusive/Failover/Shared/**Key_Shared** subscriptions |
| **Restate** | Virtual Object (keyed) | **single-writer-per-key** ("a queue per object key") | K/V state retained beside the journal | n/a (state = fold(journal)) | **yes** (the handler *is* the entity) | invoke; journaled+replayed; exactly-once per key |
| **TanStack DB** | **collection** (client) | optimistic *UI* (local apply → sync → rollback), not OCC | the synced collection *is* the snapshot | **client-side incremental** (differential dataflow / d2ts); live queries = materialized views | n/a | sync from server shapes; reactive |
| **Chronicle (today)** | URL stream; `{ReadSeq,ByteOffset}` offset | **single-writer per shard** (Lua-atomic) + idempotent producer `(Id,Epoch,Seq)` (Lean-proven) + stream-scoped *lexicographic* `Stream-Seq` | **none** | **Live only** (apps fold on read) | **no** (wake signals to external workers) | catch-up (long-poll/SSE, client offset) **+** pull-wake (leased, acked cursor, **exactly-once via fencing**) |

Read the last row against the rest: Chronicle is **already a peer** on storage, producers, and
delivery. It is missing exactly three things — an *entity-level* concurrency story, *snapshots*, and
*read models* — and the research says two of those should be **conventions/contracts, not server
features**.

---

## The five axes

### 1. Identity & ordering — the entity boundary, and cross-entity fan-in

Every event store makes **one entity == one stream**, with a per-stream gap-free counter
(EventStoreDB `revision`, Akka `sequenceNr`, Axon `seqNr`, Marten `IEvent.Version`). The log family
does **not**: you get topics/partitions, and "all events for entity X" is your problem to index
([research/04](research/04-log-tanstack-durable.md)). Chronicle is in the log family — a stream is
whatever path you choose; nothing makes `order-123` an entity except convention.

The non-obvious lesson is about **cross-entity reads**. The moment you adopt one-stream-per-entity,
"subscribe to all orders" requires knowing every `order-*` id. EventStoreDB solves this with **system
projections** that emit link events into `$ce-{category}` / `$et-{type}`
([research/01](research/01-eventstoredb.md)); Akka solves it with **`.withTagger`** at write time +
`eventsByTag`/`eventsBySlices` ([research/02](research/02-akka.md)). Both make the same point:
**the fan-in key must be decided at append time** — "retrofitting cross-entity order after the fact
means re-reading everything." These indexes are always **derived, replayable** data, never primary.

> **Chronicle:** if it adopts per-entity streams, the highest-leverage *write-side* primitive is an
> append-time **category/slice/tag key** that fans an append into a derived index stream
> (`{category}` → member streams, or a deterministic slice of the entity key). Maintain it with the
> **existing** webhook/pull-wake machinery, not a new indexing subsystem. Keep it derived so it stays
> outside the proven core.

### 2. Optimistic concurrency — the central fork (and Chronicle is already on one side of it)

This is the heart of the matter, and the four files resolve a confusion that the phrase "optimistic
concurrency" usually hides. There are **two distinct mechanisms**:

- **(A) Expected-version CAS.** Append "iff the stream is at revision N"; the store rejects on
  mismatch and echoes the actual revision so you reload-and-retry. EventStoreDB, Axon, Marten. This
  exists **because those systems allow multiple clients to write the same stream** — CAS is how they
  detect a read-decide-append race without locks.
- **(B) Single-writer-per-key.** Guarantee exactly one writer for an entity; then the sequence number
  is just "highest + 1" and there is **no race to detect**. Akka *event-sourced* (Cluster Sharding),
  Restate Virtual Objects ("a queue per object key"), Pulsar Key_Shared.

The decisive datum from [research/02](research/02-akka.md): **Akka's event-sourced path uses (B), not
(A).** Its numeric "expected version" (`revision = existing + 1 or fail`) lives **only** in the
*DurableState/CRUD* path. So "expected version" is not synonymous with event sourcing — it is one of
two ways to make a single-writer entity, and the heavyweight ES frameworks split on it.

And [research/04](research/04-log-tanstack-durable.md) lands the punchline: **no system in the log /
durable-execution family offers per-entity CAS at all** — they *all* either skip it (logs) or
sidestep it with single-writer-per-key. Per-entity CAS is the one thing that distinguishes an event
store from a log; single-writer-per-key is the thing that distinguishes durable execution from a log.

> **Chronicle is already in camp (B).** Redis is single-threaded per shard; every key of a stream
> hash-tags to one shard; appends run in one Lua script. "Redis *is* the single writer, with no
> cluster or in-memory actor needed" ([research/02](research/02-akka.md)) — strictly simpler than
> Akka's sharding. Two consequences:
>
> - For the **single-writer entity** model, Chronicle needs *nothing new* for concurrency: front the
>   stream with a pull-wake **single-holder lease** (already TLA+-proven) and that worker is the one
>   writer. This is the "Restate Virtual Object" shape, expressed in primitives Chronicle already has.
> - A per-stream **expected-version** is still worth adding for the *other* model — many independent
>   clients appending to one entity stream and wanting OCC without electing an owner. It is **cheap and
>   provable**: writes already serialize in one Lua script, so the CAS is a numeric tightening of the
>   existing Lean-proven append state machine. Return `409` echoing expected-vs-actual (EventStore's
>   PR #2679 lesson). Note Chronicle's current `Stream-Seq` is *lexicographic and stream-scoped* — a
>   gesture at this, but not a per-entity numeric revision; pick one model and document which.

### 3. Snapshots — a cache that falls out of other features, never a subsystem

Unanimous across the files: **a snapshot is a derivable cache, never the source of truth.**

- EventStoreDB ships **none** and tells you not to reach for one early; the pattern is a
  `snapshot-{id}` stream with `$maxCount=1`, read-latest + replay-tail
  ([research/01](research/01-eventstoredb.md)).
- Akka loads the latest snapshot then replays the tail; `SnapshotSelectionCriteria.none` always
  recovers from events alone ([research/02](research/02-akka.md)).
- Marten makes the equivalence explicit in its API: a "snapshot" **is** an inline single-stream
  projection ([research/03](research/03-axon-marten.md)).
- The log family's snapshot is **log compaction** — latest-value-per-key, best-effort, keyed
  ([research/04](research/04-log-tanstack-durable.md)).

> **Chronicle:** do **not** build a snapshot primitive. Two no-new-invariant options:
> (a) document the app-level snapshot-stream pattern (expressible *today* with TTL + offsets — a
> snapshot is "state as of offset X," foldable from a `ZRANGEBYLEX` tail, deletable with zero data
> loss); and/or (b) add **optional key-compaction** of a stream (a compacted sibling keyed by an
> app-supplied message key) — the log-family-native "latest value per key," which fits the
> ZSET-per-stream model far better than EventStore-style snapshot events. Honest caveat from the
> source: compaction is best-effort and non-deterministic — a latest-value cache, not a transactional
> point-in-time snapshot.

### 4. Projections / read models — *where* and *when* to compute, and why mostly "not on the server"

Two questions: **when** is the read model computed, and **where** does it live.

**When** has three answers, and Marten is unusual in naming all three as one enum
([research/03](research/03-axon-marten.md)):

- **Live / on-read** — fold events at query time, nothing stored (Marten `AggregateStreamAsync`;
  EventStoreDB transient query). Zero write cost, expensive per read, scales badly with stream length.
  **This is the only mode Chronicle offers today.**
- **Inline / write-time** — read model updated in the *same transaction* as the append; strongly
  consistent. Safe only when the projection is **single-stream / shard-aligned**; Marten *forces*
  multi-stream projections to be async for exactly this reason.
- **Async / background** — a checkpointed consumer; eventually consistent; the default for anything
  cross-entity (Akka Projection, Marten async daemon, Axon tracking processors).

**Where** is the modern twist. EventStoreDB's server-side JS projections are explicitly discouraged in
its own docs — **write-amplification** (one event → three+ writes) and **leader-only** execution
([research/01](research/01-eventstoredb.md)). Kafka/ksqlDB materialize on the server tier with
incremental `KTable`s. But TanStack DB runs the **same incremental algorithm** (differential dataflow,
d2ts) **on the client** in microseconds — "only recompute the parts that have changed"
([research/04](research/04-log-tanstack-durable.md)). If the projection can live on the client and stay
correct from a synced log, **the server may need no projection tier at all.**

> **Chronicle:** this is the most important architectural call. A **server-side fold would inherit the
> single-DS-hub serialization bound** — exactly the EventStoreDB leader-only trap, doubled. So:
> - **Never** build a server-side projection engine (JS-in-engine). It is the one feature whose costs
>   Chronicle is structurally primed to amplify.
> - **Default posture: push projections to the client** (TanStack DB / differential dataflow).
>   Chronicle's job shrinks to *shipping the log efficiently* — which it already does (SSE/long-poll +
>   offset cursors). N clients fold; the hub never folds.
> - For server-needed read models, expose an **exactly-once projection offset-store contract** for an
>   **external** projector: "give me events for tag/slice T from offset X, and let me commit my
>   read-model write + the new offset **together**" — the `R2dbcProjection.exactlyOnce` pattern
>   ([research/02](research/02-akka.md)). This is the **one genuine gap**: Chronicle's cursors are
>   *delivery-only*; it lacks a per-consumer offset-store for user read models. Reuse the fenced
>   pull-wake cursor, keep the offset **out of the log**, reset to `-1` to rebuild. If any *inline*
>   fold is ever offered, constrain it to **single-stream** so it stays inside the one-shard Lua
>   boundary.

### 5. Handlers & consumers — bundling is a runtime, not a storage feature

"Are handlers bundled with the entity?" splits the field cleanly:

- **Bundled (Axon, Akka, Restate, Pulsar Functions):** `@CommandHandler`/`@EventSourcingHandler`
  inside the aggregate, or the Virtual Object handler *is* the entity. Ergonomic — and **inseparable
  from a compute runtime** (actors, dispatchers, sharding, passivation, `rememberEntities`). Tellingly,
  **even Akka keeps the read-side fold outside the entity** ([research/02](research/02-akka.md)).
- **Not bundled (EventStoreDB, Marten, Kafka):** the store moves events; *decide*, *evolve*, and read
  models are your code. Marten's **decider contract** — `FetchForWriting` (read-current → app decides →
  append-with-expected-version) — gives the bundled *feel* with none of the runtime, and is
  HTTP-shaped and language-agnostic ([research/03](research/03-axon-marten.md)).

On the **consumer** side, Chronicle's fenced pull-wake subscriber already sits in the durable-execution
table as a peer — leased slots + private wake stream + acked offsets + epoch fencing is *structurally*
Pulsar Failover/Key_Shared + Restate single-writer-per-key, and it is **exactly-once via fencing**,
stronger than EventStoreDB persistent subscriptions' at-least-once
([research/04](research/04-log-tanstack-durable.md), [research/01](research/01-eventstoredb.md)).

> **Chronicle:** refuse server-resident handlers — they are the heavyweight-framework direction the
> whole exercise is meant to avoid, and they break polyglot/HTTP-native. Expose the **decider
> contract** instead (read-current-state → app decides → append, optionally with expected-version).
> If you want "the handler is the entity" ergonomics, that is **Path B** (an external worker holding a
> single-holder lease owns the entity and its fold) — built on existing primitives, with the compute
> *outside* the storage core.

---

## Where Chronicle already wins (do not regress these)

1. **Lean-proven idempotent producers.** `(ProducerId, Epoch, Seq)` is the *same* mechanism as Kafka's
   idempotent producer and Pulsar dedup — but proven, not just tested. This is exactly the substrate
   durable execution needs for exactly-once handler invocation.
2. **Exactly-once subscription delivery via fencing** — stronger than EventStoreDB's at-least-once
   persistent subscriptions; on par with Restate/Temporal, and TLA+-checked.
3. **Single-writer for free, per shard** — no cluster, no in-memory actor; Redis serializes. The thing
   Akka builds Cluster Sharding to achieve, Chronicle gets from `{path}` hash-tagging.
4. **A small provable core** (Lean pure core + TLA+ distributed core). This is the asset every
   extension must protect — it is why "adds no new invariant" / "numeric tightening of an existing
   proof" are the acceptance tests below.
5. **HTTP-native, polyglot, thin-client.** Anyone can `POST`/`GET`; the UI is a client, not a runtime.

## The gaps (precisely scoped)

| Gap | Severity | Right-sized fix |
|---|---|---|
| No per-entity concurrency contract beyond lexicographic `Stream-Seq` | **medium** | Path A: numeric `expected-version` header (CAS). Path B: single-holder-lease ownership (already possible). |
| No snapshots | **low** | Convention (snapshot-stream) and/or optional key-compaction. No subsystem. |
| No user-facing read-model offset store (cursors are delivery-only) | **medium — the one real new primitive** | Exactly-once projection offset-store contract over the fenced cursor. |
| No cross-entity fan-in (`$all`/category/by-type) | **medium** (only once per-entity streams are adopted) | Append-time category/slice key → derived, replayable index stream. |
| No typed events / schema | **low** | Out of scope; stays an app concern (Chronicle moves bytes). |

## Recommendation — yes, extend, but as substrate, not a framework

**Should Chronicle become an event-sourcing backend? No. Should it grow the *substrate* on which
event-sourced entities are cheaply built? Yes — along four narrow, provable axes.** The entity
*runtime* (ownership, handlers, folds) belongs in an external process built on Chronicle's lease/fence
primitives; Chronicle provides the durable log + concurrency contract + offset store + fan-in key.

**Two coherent paths to an event-sourced entity (a deployment can use either, per entity):**

- **Path A — "EventStore-lite" (multi-writer + OCC).** Add a per-stream numeric `expected-version`
  append header → `409` echoing actual. Gives the classic load → decide → append loop over HTTP, for
  many independent clients on one entity. Cheap (folds into the Lua append) and provable.
- **Path B — "Virtual Object" (single-writer-per-key).** Front the entity stream with a pull-wake
  single-holder lease; that worker is the one writer and owns the fold. No CAS needed. This is the
  Akka/Restate model in primitives Chronicle already has and has proven. **This is the more natural
  fit** for Chronicle's architecture and the one to lead with.

**Common substrate (both paths, all derivable / no-new-invariant):**

1. **Snapshots** = documented snapshot-stream pattern, optionally + key-compaction. (no subsystem)
2. **Projections** = client-side by default (TanStack-style); for server-needed read models, an
   **exactly-once offset-store contract** for external projectors, reusing the fenced cursor, offset
   kept out of the log, resettable to `-1`. **Never** a server-side projection engine.
3. **Cross-entity fan-in** = append-time category/slice key → derived index stream, maintained by the
   existing subscription machinery.
4. **Decider contract**, not bundled handlers: read-current → app decides → append(+expected-version).

**Phasing (each phase is independently shippable and independently provable):**

- **Phase 0 (docs only):** write the recipes that already work today — single-writer-via-lease entity
  (Path B), app-level snapshot stream, client-side projection over SSE. Zero code; closes most of the
  perceived gap immediately.
- **Phase 1:** per-stream numeric `expected-version` header (Path A). One numeric tightening of the
  proven append state machine; Lean obligation below.
- **Phase 2:** the projection offset-store contract (the one genuinely new primitive). TLA+ obligation
  below — it must compose with the existing membership/ownership/fence specs.
- **Phase 3 (optional):** append-time category/slice key + key-compaction, as derived data.

**Refuse:** a server-side projection/fold engine; server-resident command/event handlers; a bespoke
snapshot subsystem; cross-stream atomic transactions. Each violates either the single-hub-serialization
guardrail or the polyglot/HTTP-native/small-core mandate.

## Proposed minimal surface (sketch — to be specced, not final)

```
# Path A — per-entity optimistic concurrency (numeric, per-stream)
POST /v1/stream/{path}
Expected-Version: 41            # append iff stream has exactly 42 events (revision 41); else 409
→ 409 Conflict
  Stream-Actual-Version: 43     # echo actual so the client can reload+retry (EventStore PR #2679)

# Snapshot = pure convention (works today; document it)
#   snapshot-{path} stream, keep-last-1; body = {state, asOfOffset}; load = read last + ZRANGEBYLEX tail

# Phase 2 — exactly-once projection offset store (the one new primitive)
GET    /__proj/{projId}/cursor                 → { offset }
POST   /__proj/{projId}/commit                 # advance cursor; caller has already written its read
       { fromOffset, toOffset }                #   model; contract = "your write + this commit are
                                               #   the unit you make atomic on your side"
DELETE /__proj/{projId}/cursor                 # reset → replay from -1 (rebuild)

# Phase 3 — append-time fan-in (derived index; optional)
POST /v1/stream/{path}
Stream-Category: order          # also append a link into the derived `$cat/order` index stream
Compact-Key: order-123          # (optional) key-compaction: keep latest frame per key
```

## Proof obligations (the acceptance test for "is this Chronicle-shaped?")

- **`expected-version`** must be expressible as a **numeric tightening of the existing Lean producer /
  append state machine** — same shape as `INV-PROD-*`. If it needs a new distributed invariant, it is
  in the wrong layer.
- **Projection offset store** must **compose with** the existing TLA+ `Membership`/`Ownership`/
  `SubscriptionFence`/`Composed` specs (it is another fenced, leased cursor) and must prove the offset
  is *never folded back into the log* (rebuild must not mutate history).
- **Snapshots / compaction** must add **no new invariant**: a snapshot is `fold(ZRANGEBYLEX tail from
  X)`, deletable with zero data loss; compaction is a retention policy, not a correctness primitive.
- **No addition may introduce a global server-side fold** — anything cross-stream is client-side or
  sharded-by-`{path}`, so it never serializes on the single hub.

---

*Provenance: orca-orchestrated, `/search`-grounded. Per-system findings and all primary-source links
are in [`research/01`](research/01-eventstoredb.md)–[`research/04`](research/04-log-tanstack-durable.md);
the worker briefs are under [`research/_prompts/`](research/_prompts/).*
