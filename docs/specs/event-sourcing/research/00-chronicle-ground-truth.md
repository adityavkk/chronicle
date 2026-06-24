# Chronicle — Ground Truth (what it is *today*)

> This is the fixed reference the synthesis compares everything against. Sourced from a
> read-only architectural sweep of `/Users/auk000v/dev/chronicle` (Go server + Redis 8 +
> Lean/TLA+ proofs + Preact UI). Do not treat external research as overriding this.

## One-line
Chronicle is a **durable append-only stream primitive** spoken over HTTP. It is *not* an
event-sourcing framework. It is the layer **underneath** one — closer to "Kafka-the-log
with HTTP semantics and idempotent producers" than to EventStoreDB-the-event-store.

## Core model
- **Stream** = URL-addressable, append-only byte sequence: `POST/GET /v1/stream/{path}`.
  Single content-type for the whole stream. Explicit monotonic closure (EOF). TTL + forks.
- **Message boundaries**: binary content-type → whole body is one message; JSON content-type
  → body must be a JSON array, flattened one level into individual messages.
- **Offset** (`store/offset.go`): `{ReadSeq, ByteOffset}` rendered `"%016d_%016d"` —
  lexicographically sortable, fixed width. The offset **is** the cursor.
- **Storage** (Redis, `store/redis/`): one **ZSET per stream** of frames `"<endOffset>|<bytes>"`
  (score 0), read via `ZRANGEBYLEX`. The ZSET is the index; **no segments, no secondary index**.
  Keys hash-tagged by `{path}` so all of a stream's keys live on one shard (multi-key Lua stays
  cluster-legal). `meta` HASH (contentType, currentOffset, closed, ttl, fork/refcount),
  `prod` HASH (producer dedup state), `ds:notify:{path}` pub/sub channel for tail wakeups.

## Append / read API
- **Append** `POST /v1/stream/{path}`. Atomic: validate → write frame → bump tail → publish
  notify, all in **one Lua script** per op (Redis single-thread serializes per shard).
- **Read** `GET ...?offset=`. Offsets: `-1` (start), explicit, or `now` (tail). Paged catch-up
  via `Stream-Next-Offset`. Live modes: `live=long-poll` (block ≤30s) and `live=sse`.
- **Idempotent producers** (`store/producer.go`, the spec §5.2.1 oracle, *proven in Lean*):
  `(ProducerId, ProducerEpoch, ProducerSeq)`. First seq must be 0; must increment by exactly 1;
  gaps rejected (400 echoing expected/received); stale epoch fenced (409, zombie fencing);
  duplicate → 204 no-op. State kept per `(streamPath, producerId)`.
- **`Stream-Seq` header**: optional *lexicographic* expected-order guard at the **stream** level
  (rejects appends whose Stream-Seq ≤ previous). This is the nearest thing to "expected version,"
  but it is stream-scoped and lexicographic, not a per-entity numeric revision.

## Subscriptions (reserved `__ds/*` control plane — NOT the core protocol)
- **Webhook**: glob/explicit stream link → on append, Manager fires signed HTTP POST with the
  offset range; at-least-once with retries. (`webhook/manager.go`)
- **Pull-wake**: workers **claim a lease** on a subscription "slot"; server appends wake events
  to a private `__wake/{subId}` stream; worker tails it, reads the named streams, **acks offsets**
  (cursor advances). Exactly-once via **fencing** (generation/epoch). Lease TTLs +
  membership/ownership reconciliation. (`webhook/ownership.go`, `webhook/state.go`)
- Cursors/checkpoints exist **only** for subscription delivery — not for user read models.

## What Chronicle does NOT have (the whole point of this research)
- ❌ Projections / materialized read models / fold-on-read
- ❌ Snapshots of aggregate state
- ❌ Aggregates / entities / per-entity optimistic concurrency (only stream-level `Stream-Seq`)
- ❌ Command handlers or event handlers bundled with a stream/entity
- ❌ `$all` / category / by-type streams; no server-side secondary indexing of events
- ❌ Typed events (it moves bytes; JSON is just framing, not a schema/event-type system)
- Applications building event sourcing on Chronicle must do all of this themselves.

## Distinctive strengths (the things a redesign must NOT lose)
- **Formal verification**: Lean 4 proofs for the pure core (producer state machine, offset
  arithmetic, cursor monotonicity, fence single-holder, webhook ack-merge monotonicity) mapped
  to `INV-*` invariants; **TLA+/TLC** for the distributed parts (Membership, Ownership,
  SubscriptionFence, Composed). Any extension should preserve a *small, provable pure core*.
- **HTTP-native, polyglot, language-agnostic** (anyone can POST/GET; the UI is a thin client).
- **Single-shard-per-stream** simplicity; Lua-atomic writes.

## Known architectural constraint (from operator memory, verify against code)
- The **wake path fences under concurrency**: a **single Durable-Streams hub serializes** it, so
  tiers sit idle under load (a *saturation/architecture* bound, not a hardware bound). Relevant
  to any "projections/handlers run server-side" proposal — server-side fold could inherit the
  same serialization unless sharded by entity/partition key.

## The synthesis question (what the user actually asked)
1. Map the landscape: EventStoreDB, Akka Persistence/event-sourced actors, Axon — how they do
   snapshots, projections, bundled handlers, optimistic concurrency.
2. Durable streams vs an event-sourcing backend (EventStoreDB): similarities and **gaps**.
3. TanStack DB as a projection engine (client-side incremental views).
4. **Should/can Chronicle be extended toward "true event-sourced entities"** — and if so, what is
   the smallest set of additions (entity/category streams, per-entity expected-version,
   snapshots, projection checkpoints, optional bundled handlers) that fits its proven-core,
   HTTP-native philosophy without becoming a heavyweight framework.
