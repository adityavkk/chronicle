# 02 — Cloudflare Durable Objects as a scaling primitive

Even though Electric *doesn't* use Durable Objects, they're the cleanest reference
design for the problem chronicle has (per-entity coordination without a central hot
spot), so it's worth knowing exactly what they buy — and what they cost.

## The model

A Durable Object is an **addressable actor = compute + private storage**:

- **Identity.** A globally-unique `DurableObjectId` from a logical name
  (`idFromName("room-42")`, a deterministic global mapping) or a random id. "Each
  object exists in only one location in the whole world at a time." Any Worker that
  knows the id reaches the *same* instance.
- **Concurrency.** Each DO is **single-threaded**, cooperatively scheduled — operations
  serialize naturally, no locks, no consensus. *Input gates* pause new requests during
  `await`; *output gates* hold responses until pending writes are confirmed, so clients
  never see unpersisted data.
- **Storage.** Private, transactional, strongly-consistent, **SQLite in the same thread
  as the code** (synchronous SQL, no network hop), up to 10 GB/object. Each op is
  implicitly a transaction. Durability: the SQLite WAL is replicated to **5 followers in
  5 datacenters**, the write acked after **≥3 confirm**; 30-day point-in-time recovery
  via bookmarks.
- **Hibernation.** Idle DOs are evicted from memory (in-memory state lost; constructor
  re-runs next event) while WebSocket clients stay connected at the edge; **no duration
  billing during hibernation.**
- **Alarms.** `setAlarm(timeMs)` schedules *one* future wake per object; the `alarm()`
  handler fires at that time — at-least-once with exponential backoff. This is the
  per-object timer primitive.

## How it scales

Sharding *is* the model. You mint **one DO per logical resource** (per room, session,
subscription, queue shard). Objects are unlimited per namespace, each independently
addressed *and located*, so the data plane is handled by the object holding that
resource's data — no central index every request passes through, no global lock.

The explicit anti-pattern is a **single "global" DO** funneling all traffic: a single
object has a soft cap of **~1,000 req/s**. Cloudflare's own Queues rewrite is the
proof: v1 used one DO per queue and bottlenecked; v2 sharded into many regional
"Storage Shard" DOs, taking P50 200 ms → 60 ms and 400 → 5,000 msg/s (10×). **Capacity
grows by adding objects, not by resizing a cluster** — the opposite of chronicle's
single `{__ds}` slot.

The alarm is the headline for chronicle: instead of an `O(K)` scan to find who needs
waking, **each entity schedules its own wake** and the platform fires only that object.
Wake cost is `O(1)` per entity; there is no scan loop to run, shard, or fall behind.

## Regional / DR

- **Placement** is near the first `get()` and fixed thereafter (it can migrate among
  healthy servers on failure, but its home region doesn't move). `locationHint` steers
  initial placement to one of 9 regions.
- **Jurisdictions** (`"eu"`, `"fedramp"`) hard-pin where a DO runs and persists for data
  residency; the id is still logged out-of-jurisdiction for billing.
- **Durability/failover** comes from the synchronous 5-datacenter WAL replication above —
  on host loss the object reactivates elsewhere with storage intact, single-active-instance
  avoiding split-brain.
- **Read replicas:** automatic global read-replica fan-out is a **D1 feature** (D1 is a
  SQLite DO that streams its WAL to per-region replica DOs with monotonic bookmark
  timestamps), **not** a generic knob on every user DO. A plain DO is single-region for
  reads and writes.

## The tradeoffs

| | Durable Objects | Managed Redis (chronicle today) |
|---|---|---|
| Hot-path latency | In-thread SQLite, no network hop | Network round-trip per op |
| Consistency | Strong, serializable, single-writer, lock-free | Single-threaded per shard; cross-slot needs care |
| Scale | Add objects; no central bottleneck | Add shards; control plane pinned to one slot |
| Idle cost | Hibernation zeroes duration billing | Always-on provisioned memory |
| Ops | No servers/sharding/failover to run | Cluster sizing, replication, eviction tuning |
| **Lock-in** | **Cloudflare-only; runs only in Workers; no portable equivalent** | **Portable across clouds / on-prem** |

## Verdict for chronicle

Great fit *technically* — the DO-per-resource + per-object alarm model is exactly the
shape that removes chronicle's central-sweep and single-slot bottleneck. But DOs are
**Cloudflare-proprietary and run only inside Workers**, which is incompatible with
chronicle's no-AWS / cloud-agnostic / managed-Redis-8 posture. **The takeaway is to copy
the patterns** — per-entity actor with private state; per-entity self-scheduled wake
instead of a central scan; single-writer-per-key to avoid locks; "don't ack until
persisted" — **onto the managed-Redis + object-storage stack, not to adopt DOs.** Note
the ~1,000 req/s per-object ceiling means very hot entities still need app-level
sub-sharding even on DOs.

## Sources

Cloudflare docs/blog: *What are Durable Objects?*, *Rules of Durable Objects*,
*Zero-latency SQLite storage in every Durable Object* (5-follower WAL replication, PITR),
*Durable Objects Alarms* + Alarms API reference, *WebSockets/Hibernation*,
*Data location* (location hints, jurisdictions), *D1 global read replication*,
*How we built Cloudflare Queues* (10× via sharding), *Limits*.

**Verified** from primary Cloudflare sources. **Nuance:** the "5 followers / ≥3 ack" and
10 GB figures are from the SQLite-in-DO GA blog and may have moved — check the live
limits page. Generic per-DO read-replication is **not** GA (D1-only today).
