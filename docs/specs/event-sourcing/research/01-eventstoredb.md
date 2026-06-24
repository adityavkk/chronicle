# EventStoreDB / KurrentDB — the event store as a database

> The canonical "event store as a database." Originally **EventStoreDB** (Greg Young et al.);
> rebranded **KurrentDB** in 2024. Docs now live at `docs.kurrent.io`; the engine, stream model,
> and APIs are continuous across the rename. This file uses "ESDB" for the product and quotes the
> current Kurrent docs. It is the closest existing analog to a Chronicle-built event-sourcing
> backend: a purpose-built append-only log whose *unit of storage is the stream*, with optimistic
> concurrency, subscriptions, and projections layered on top.

---

## Model

**Streams are the physical unit; events are the records.** ESDB is "purpose-built for event
storage. Unlike traditional state-based databases, which retain only the most recent entity state,
KurrentDB allows you to store each state alteration as an independent event. These **events** are
logically organized into **streams**, typically only one stream per entity"
([Event streams](https://docs.kurrent.io/server/v26.0/features/streams)). That last clause is the
whole event-sourcing convention: **one aggregate instance == one stream**. A stream named
`order-123` *is* the order with id 123; its events, folded in order, *are* its state. There is no
separate "aggregate" object in the database — the aggregate is a client-side fold over a stream.

**Stream naming convention** is `{category}-{id}`, e.g. `account-9E763770-...`, `shopping-cart-1`.
The hyphen separator is not enforced by the store — it's a convention the *category* system
projections key off of (see below). Pick a separator and stick to it.

**An event** carries:

| Field | Meaning |
| --- | --- |
| `eventId` | A client-supplied `UUID` uniquely identifying the event. "If two events with the same `UUID` are appended to the same stream in quick succession, EventStoreDB will only append one" — i.e. **idempotent writes keyed on eventId** ([Appending events](https://docs.kurrent.io/clients/golang/legacy/v4.2/appending-events.html)). |
| `eventType` | A string type name, e.g. `OrderPlaced`. Docs **recommend against** using the language class name, to avoid coupling storage to code and to ease versioning. |
| `data` | The payload, bytes. JSON recommended — "This allows you to take advantage of all of EventStoreDB's functionality, such as projections" (projections require JSON). |
| `metadata` | A separate byte array for cross-cutting info (correlation/causation ids, timestamps, auth). Kept physically separate from `data`. |
| `eventNumber` / **stream revision** | A gap-free monotonic integer, per stream, starting at 0. `0@test-stream`, `1@test-stream`, … This is the position *within a stream* and the basis for optimistic concurrency. |

**ContentType** is `Json` or `Binary`; only JSON events are visible to projections.

**The global `$all` stream.** Beyond per-stream ordering, ESDB maintains one append-only log of
*everything*: "The append-only log is called the *$all stream*… a logical representation of the
memory (disk) position where the event is located. The position is monotonic, and may have gaps. To
be precise, it is actually composed of two such pointers: a *commit* and a *prepare*. They are
remainings from when transactions were supported… (it is to be unified into one number in upcoming
versions). For now, if you're not using transactions, you can treat the commit position as the
single relevant one" ([Oskar Dudycz, *Let's talk about positions*](https://event-driven.io/en/lets_talk_about_positions_in_event_stores/)).
Crucially: "in EventStoreDB, the physical structures are streams. The *$all stream* is the secondary
structure." So there are **two coordinate systems**: per-stream `revision` (gap-free, for
concurrency) and global `Position{commit,prepare}` (gappy, for global subscriptions/checkpoints).

**Category & by-type streams are built by SYSTEM projections** — they are *not* primary storage,
they are link-event indexes ([System projections](https://docs.kurrent.io/server/v26.0/features/projections/system)).
Five ship built-in (present but **disabled** on a fresh DB; you POST `/command/enable`):

| Projection | Produces | Use |
| --- | --- | --- |
| `$by_category` | `$ce-{category}` — splits stream id by separator (`first`/`last` + char), e.g. `account-9E76…` → `$ce-account` | "subscribing to all events within a category" |
| `$by_event_type` | `$et-{eventType}` — every `PaymentProcessed` event, regardless of source stream, gets a link in `$et-PaymentProcessed`. **Not configurable.** | subscribe to all events of one type |
| `$by_correlation_id` | `$bc-{correlationId}` — keyed on a configurable `correlationIdProperty` | tie together a saga/process |
| `$stream_by_category` | `$category-{category}` — links *stream names* (not events) into a category stream | "subscribing to all stream instances of a category" |
| `$streams` | `$streams` — one link per new stream. **Not configurable.** | enumerate all streams |

These produce **link events** (pointers `eventNumber@sourceStream`), resolved on read via
`resolveLinkTos`. They cost writes (see Projections → write amplification).

---

## Optimistic concurrency

**The expected-version check is the entire concurrency-control story** — there are no locks.
On append you pass the stream state/revision you believe the stream is in; the server rejects the
write if reality differs. From the .NET client signature:

```csharp
Task<WriteResult> AppendToStreamAsync(
    string stream, long expectedVersion, params EventData[] events);
```

> "It is possible to make an optimistic concurrency check during the append by specifying the
> version at which you expect the stream to be."
> ([Appending events](https://docs.kurrent.io/clients/tcp/dotnet/21.2/appending.html))

**The expected-revision options** (modern gRPC clients model these as a sum type rather than a
magic integer):

- **`Any`** — no concurrency check (also disables idempotency dedup beyond eventId).
- **`NoStream`** — the stream must not yet exist (first write of a new aggregate).
- **`StreamExists`** — the stream must exist (any revision).
- **`StreamRevision(n)`** — the stream's last event must be exactly revision `n`.

([Handling concurrency](https://docs.kurrent.io/clients/golang/legacy/v4.2/appending-events.html#handling-concurrency);
the legacy TCP constants were `ExpectedVersion.NoStream` (-1), `EmptyStream`, `Any`.)

**The conflict surfaces as `WrongExpectedVersionException`** (gRPC: a `WrongExpectedVersion`
error result). The error **echoes back the expected vs. actual revision** so the caller can
decide to retry/rebase ([EventStore PR #2679 "Echo Back Expected Revision / State to Client"](https://github.com/EventStore/EventStore/pull/2679)).

**The load → decide → append loop** is the standard command path:

```
read stream (note last revision r)  →  fold events into state
decide (run business logic, produce new events)
appendToStream(stream, expectedRevision = r, newEvents)
   → success, or WrongExpectedVersionException → reload & retry
```

Because `revision` is **per-stream and gap-free**, and a single aggregate maps to a single stream,
this delivers **per-aggregate single-writer correctness without locks**: two concurrent commands
that read revision 7 will both try to append "expecting 7"; the log serializes them, the first wins
at 8, the second gets `WrongExpectedVersion` and must rebase. This is the property Chronicle's
stream-level lexicographic `Stream-Seq` guard *approximates* but does not provide at entity
granularity.

Two adjacent guarantees worth noting: **idempotent retries** (same `eventId` + same expected version
is a safe no-op, so a timed-out client can resend) and a hard **batch limit** — a single append's
`events` "length must not be greater than 4095" and is written atomically (all-or-nothing). A
transaction may span multiple appends but **only to one stream** — "Transactions across multiple
streams are not supported." There is no cross-aggregate atomic write.

---

## Snapshots

**ESDB has no built-in aggregate snapshot.** The engine stores events; folding them into state and
deciding when that fold is too expensive is the application's job. Kurrent's own guidance leads with
"don't bother yet": "isn't loading more than one event a performance issue? Frankly, it's not…
EventStoreDB is optimised for such operations, and the reads scale well" — snapshots are a
**later optimization**, not a default ([Snapshots in Event Sourcing](https://www.kurrent.io/blog/snapshots-in-event-sourcing/)).

**The standard app-level pattern is a snapshot stream.** You persist the folded state periodically;
on load you read the latest snapshot, then replay only the tail of events after it:

```
load(aggregateId):
  snap = read last event of  snapshot-{aggregateId}   # {state, version=V}
  tail = read order-{aggregateId} from revision V+1 .. end
  state = fold(snap.state, tail)
```

**Where to store it** is open: "events in the same or separate stream, in a separate database,
in-memory (popular in actor-based systems), in cache such as Redis." A separate
`snapshot-{id}` stream with `$maxCount = 1` (keep only the latest) is the common ESDB-native choice.

**When to snapshot** — the blog enumerates the tactics, each a write/staleness trade:

1. **After each event** — load is always one read, but you pay an extra write per command (and "if
   we're doing snapshots as the performance optimisation, then additional write each time can
   degrade it even more"). Sync write = slower writes; async write = possible stale snapshot.
2. **Every *N* events** — read snapshot + up to *N* tail events. The common middle ground.
3. **On a marker event** (e.g. `CashierShiftEnded`) — "closing the books."
4. **Every period** (hourly/daily) — risks spikes between snapshots.

**Caveats the docs stress:** snapshots reintroduce the versioning/migration problem they were
meant to avoid (the snapshot schema must evolve with the model), and "the need to use snapshots may
hint to the model's design flaw" — often the better fix is **shorter streams** (lifecycle the
aggregate so streams stay small), not snapshotting a long one.

---

## Projections / read models

ESDB has a **server-side projection subsystem**, which is unusual — most event stores push read-model
building entirely to the consumer. It "lets you append new events or link existing events to streams
in a reactive manner" and is good at one thing: **temporal-correlation queries** ("how many users
said 'happy' within 5 minutes of 'coffee'") ([Projections intro](https://docs.kurrent.io/server/v26.0/features/projections/)).
Two kinds:

**SYSTEM projections** — the five built-ins above. Indexing only (`linkTo`), fixed behavior.

**USER projections** — JavaScript, registered via API or admin UI, run **inside the server**:

```javascript
fromCategory('order')                       // selector: fromStream / fromCategory / fromAll / fromStreams
  .when({
     $init:        () => ({ count: 0 }),    // initial running state
     OrderPlaced:  (s, e) => { s.count += 1 },
     $any:         (s, e) => { /* … */ }
  })
  .transformBy(s => ({ Total: s.count }))
  .outputState()                            // writes $projections-{name}-result
```

Key surface ([User-defined projections](https://docs.kurrent.io/server/v26.0/features/projections/custom)):

- **Selectors**: `fromStream(id)`, `fromCategory(cat)` (reads `$ce-cat`), `fromAll()`,
  `fromStreams([...])`. (`fromAll` with ≥2 event-type handlers auto-optimizes to read the relevant
  `$et-*` streams until caught up, then switches to `$all`.)
- **`when(handlers)`** — per-event-type or `$any`/`$init`/`$deleted` handlers that mutate a
  **stateful "running state"** object.
- **`partitionBy(fn)` / `foreachStream()`** — partition the running state (per key, per stream).
- **`emit(stream, type, body, meta)`** — append a **brand-new event** to another stream.
- **`linkTo(stream, event, meta)`** — write a **link** (index pointer), not a copy.
- **`transformBy` / `filterBy` / `outputState`** — shape and publish the result; result lands in a
  `$projections-{name}-result` stream you can subscribe to like any other.
- **Transient vs continuous**: a projection can run once to drain current results (a one-time
  query) or run **continuously**, "finding new results as they happen and updating its result set."

**When are projections computed?** **Asynchronously, write-time-triggered, server-side.** They run
reactively as events land in the log, maintaining their own **checkpoints** so they can resume; on
restart "the projection… first goes through all the events after that checkpoint and checks them
against the emitted stream… the projection can understand if it is up to the last event and can
continue from where it left off." Resetting a projection soft-deletes its output streams and rewinds
the checkpoint to replay from the beginning.

**Why heavy user projections are discouraged in production** — this is explicit in the docs, not
folklore:

- **Write amplification.** "All projections emit events as a reaction to events that they process.
  We call this effect *write amplification*… If all those three [`$by_category`, `$by_event_type`,
  `$by_correlation_id`] projections are enabled… adding one event to the database will, in fact,
  produce three additional events and, therefore, **quadruples the number of write operations**.
  Custom projections create the most significant write amplification."
- **Leader-only, single-node.** "Projections only run on a leader node of the cluster due to
  consistency concerns. It creates more CPU and IO load on the leader node." You cannot scale
  projection work horizontally.
- **They own their output streams.** "Streams where projections emit events cannot be used to append
  events from applications. When this happens, the projection will detect events not produced by the
  projection itself and it will break."

The docs' own recommendation: "Many problems are not a good fit for projections and are better
served by **hosting another read model populated by a catchup subscription**" — i.e. compute read
models *in your own service*, off a subscription, not in the database.

---

## Handlers & consumers

**Handlers are not bundled with the stream.** ESDB stores events; it does not host your command
handlers or event handlers (the JS projection subsystem is the *only* server-side compute, and it's
for indexing/correlation, not domain logic). Command handling, the fold, and event handling all live
in **your** application. The store's job is durable ordered storage + the subscription firehose.

**Two subscription models** deliver events to consumers:

### Catch-up (and volatile) subscriptions — client-checkpointed, ordered
> "catch-up subscriptions must keep the last known position on the subscriber side… [they] are
> client-driven, always receive and process events sequentially and can only be load-balanced on the
> client side" ([Persistent subscriptions, Concepts](https://docs.kurrent.io/server/v26.0/features/persistent-subscriptions)).

You subscribe from a position (a stream `revision`, or a global `Position`), the server **reads
history from there and then transitions to live** as the tail catches up — one continuous stream
of past-then-present. The **client owns the checkpoint** (persist the last processed
`revision`/`Position` yourself). This is the recommended way to build a read model: ordered,
exactly-once-ish *if you checkpoint atomically with your write*, fully under your control. A
**volatile** subscription is the degenerate case: live-only, no history, no checkpoint.

**`$all` subscriptions with server-side filtering.** You can subscribe to the whole log and filter
**on the server** by event type or stream name, via prefix or regex, and exclude system events —
so you don't ship every event to the client just to drop most of them
([Filtering subscriptions by event type](https://event-driven.io/en/filtering_eventstoredb_subscriptions_by_event_types/);
Rust [`SubscriptionFilter`](https://docs.rs/eventstore/latest/eventstore/struct.SubscriptionFilter.html)).
Filtered `$all` subscriptions checkpoint on a configurable interval even when events are filtered out,
so the checkpoint keeps advancing.

### Persistent subscriptions — server-checkpointed, competing consumers
> "Persistent subscriptions run on the Leader node and are not dropped when the connection is
> closed… this subscription type supports the 'competing consumers' messaging pattern… KurrentDB
> saves the subscription state server-side and allows for **at-least-once** delivery guarantees
> across multiple consumers."

This is ESDB acting like a message broker. The **server owns the checkpoint** and load-balances a
**consumer group** across many workers:

- **Ack / nack.** "Clients must acknowledge (or not acknowledge) messages as they are handled. If
  messages aren't acknowledged before they time out on the server, the server will retry them."
- **Retry → park.** "If a message has been retried more than the `maxRetryCount`… then the message
  will be **parked**" in `$persistentsubscription-{stream}::{group}-parked`. You can `Replay` parked
  messages later (optionally `stopAt=N`).
- **Checkpointing.** "Once a persistent subscription has handled enough events, it will write a
  checkpoint" to `$persistentsubscription-{stream}::{group}-checkpoint` (event type
  `$SubscriptionCheckpoint`). On leader change it resumes there, so **"some events may be received
  multiple times"** — at-least-once, not exactly-once.
- **Buffer / checkpoint tuning** — `bufferSize`, `maxRetryCount`, min/max checkpoint counts trade
  throughput against redelivery and checkpoint write load.
- **Consumer strategies** — `RoundRobin` (default, even spread), `DispatchToSingle` (fill one
  consumer's buffer, then next — HA fallback), `Pinned` (hash source-stream-id into 1024 buckets per
  consumer, to *reduce* same-stream concurrency — "for use with an indexing projection such as
  `$by_category`").

**The ordering trade-off is the headline cost.** "Processing events in a group of consumers running
in parallel processes will **most likely get events out of order** within a specific window… there
is no guarantee of events coming in order with any strategy." So: **catch-up = ordered + you keep
the checkpoint + you scale on the client; persistent = unordered + server keeps the checkpoint +
competing-consumer scale-out.** You pick per consumer based on whether ordering or throughput
matters.

---

## Lifecycle

**Retention is per-stream metadata**, written to the `$${stream}` metadata stream
([Event streams](https://docs.kurrent.io/server/v26.0/features/streams)):

- **`$maxAge`** — sliding time window (seconds ≥ 1); events older than this disappear from reads and
  become scavengeable.
- **`$maxCount`** — sliding length window; only the last N events are kept.
- **`$tb` / `TruncateBefore`** — every event with `eventNumber < $tb` is treated as deleted.
- (Both `$maxAge` and `$maxCount` together → eligible "when *either* condition is met.")

These affect *reads immediately* (the index consults metadata) but reclaim disk only on **scavenge**.

**Deletion has two flavors:**

- **Soft delete** sets `$tb` to `Int64.MaxValue` (everything truncated). Reads return
  `StreamNotFound`/404, but **the stream can be reopened** by appending — new events resume from the
  prior last revision + 1. Soft-deleted events vanish on the next scavenge.
- **Hard delete (tombstone)** appends a `$streamDeleted` tombstone and **permanently** closes the
  stream: any further append or read returns `StreamDeleted`/**410**. "Hard deletion of a stream is
  permanent; you cannot append or recreate it." The tombstone survives scavenge even though the
  events don't. Recommended only when you truly need to burn the stream name.
- **You cannot delete one event from the middle** — "It only allows truncating the stream." (For
  true erasure of sensitive data, the pattern is hard-delete or crypto-shredding, not surgical
  removal.)

**Scavenging** is the offline GC pass that physically reclaims space from deleted/expired events and
rewrites chunk files; until it runs, "deleted events within [`$all`] remain readable"
([Scavenging](https://docs.kurrent.io/server/v25.0/operations/scavenge.md)). Note the index-bypass
asymmetry: per-stream reads honor `$maxAge`/`$tb` instantly, but a `fromAll`/`$all` subscriber **sees
deleted-but-not-yet-scavenged events and tombstones** and must tolerate them.

---

## Lessons for Chronicle

1. **The single most important borrowed idea is "one aggregate == one stream + per-stream gap-free
   revision + expected-version append."** That four-part bundle is what makes lockless
   single-writer-per-entity correctness fall out for free. Chronicle today has streams and a
   *stream-level, lexicographic* `Stream-Seq` guard — close, but not per-entity and not a numeric
   revision. The smallest high-value extension is a **per-stream monotonic `revision` plus an
   `expected-revision` append header** (`NoStream` / `StreamExists` / exact-N), returning a
   `409` that **echoes expected-vs-actual** (ESDB's PR #2679 lesson). This is a pure, provable
   addition to the existing Lua-atomic append and fits the "small provable core" mandate — it's
   essentially a numeric tightening of a guard Chronicle already proves.

2. **Don't build snapshots into the store; build the *hooks* that make app-level snapshots cheap.**
   ESDB ships **no** aggregate snapshot and tells users not to reach for one early. The pattern is
   just "a `snapshot-{id}` stream with `$maxCount=1`, read-latest + replay-tail." Chronicle already
   has streams, TTL, and offsets — the entire pattern is expressible *today* with zero new server
   features, **provided** there's a per-entity stream and a cheap "read last frame" + "read from
   offset" (Chronicle has both). Resist a built-in snapshot primitive; document the pattern instead.

3. **Server-side projections are a trap Chronicle is structurally primed to fall into — avoid the
   leader-bound version.** ESDB's own docs warn that user projections cause **write amplification**,
   run **leader-only / single-node**, and should often be replaced by "a read model populated by a
   catchup subscription." This maps *exactly* onto Chronicle's known constraint: the wake path
   **already serializes through a single Durable-Streams hub**. A naive "run folds server-side"
   feature would inherit ESDB's leader bottleneck *and* Chronicle's hub bottleneck at once. The
   defensible move is **read models computed in the consumer** off a catch-up subscription, with the
   server providing only ordered delivery + checkpoints — never a JS-in-the-server engine.

4. **Category / by-type indexes are the feature that makes per-entity streams *usable at scale* —
   and they are "just" derived link streams.** Without `$ce-{category}` / `$et-{type}`, "subscribe
   to all orders" means knowing every `order-*` id. ESDB solves this with system projections that
   emit link events. If Chronicle adopts per-entity streams, it will immediately need a way to
   **fan-in across an entity category** without server-side fold. The cheap design is a
   **convention-named index stream** (`{category}` → member streams) maintained by the *existing*
   webhook/pull-wake subscription machinery, not a new indexing subsystem — keeping indexes as
   *derived, replayable* data, never primary.

5. **Copy the two-subscription split deliberately, because Chronicle already has both halves.**
   ESDB's "catch-up (ordered, client checkpoint) vs persistent (competing consumers, server
   checkpoint, at-least-once, *out of order*)" is precisely Chronicle's "long-poll/SSE catch-up read
   (client holds the offset)" vs "pull-wake lease workers (server-tracked cursor, exactly-once via
   fencing)." Chronicle is actually **stronger here**: its pull-wake path targets *exactly-once via
   fencing*, where ESDB persistent subs settle for at-least-once. The lesson is to **keep the
   ordered/client-checkpointed read as the read-model path** and not contaminate it with
   competing-consumer reordering — and to market the fencing-based exactly-once as a genuine edge
   over the ESDB baseline.

6. **Retention/lifecycle is metadata, not deletion — and the `$all`/index asymmetry is a real
   correctness hazard to design around now.** ESDB makes `$maxAge`/`$maxCount`/`$tb` reads honor
   instantly but reclaim only on scavenge, and `$all`/projection readers see *unscavenged deleted
   events and tombstones*. Chronicle's TTL + forks already imply the same class of "logically gone,
   physically present" states. If Chronicle grows category/index streams or any global-order reader,
   it must decide **up front** whether those readers see TTL-expired-but-unreaped frames, and prove
   the invariant — exactly the kind of thing its Lean/TLA+ core is built to pin down before the
   feature ships, rather than discovering the asymmetry in production as ESDB users do.
