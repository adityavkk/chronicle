# 03 — Prior art: horizontal scale + regional DR

How stateful streaming / subscription / queue systems scale out and survive a region
loss. The point is to extract patterns chronicle can apply on managed Redis, not to
adopt any one system.

## The one move everyone makes

**Partition the keyspace, assign each partition exactly one owner, keep no central
coordinator in the request path.**

| System | Partition unit | Owner | Routing |
|---|---|---|---|
| Redis Cluster | 1 of 16,384 CRC16 hash slots | a primary node | client redirected (MOVED/ASK) |
| Kafka | topic partition | one consumer per group | group coordinator assigns |
| NATS JetStream | a stream (its own RAFT group) | the RAFT leader | per-stream consensus |
| Pulsar | a topic | one owner broker (stateless) | metadata ownership; storage in BookKeeper |
| Orleans | a grain | one activation (at-most-once) | grain directory: `GrainId → (silo, activation)` |
| Akka Cluster Sharding | an entity (→ shard) | a `ShardRegion` node | singleton `ShardCoordinator` allocates |
| Cloudflare DO | a logical object | one instance worldwide | id → instance |

**The universal failure mode is the hot partition** — one slot/key/partition/grain all
traffic funnels to, pinning load to a single core while the rest idles. Redis makes it
sharp: it is **single-threaded per shard** and has **no cross-slot transactions**
(`CROSSSLOT` error) — exactly chronicle's `{__ds}`-slot situation. The unit of
parallelism is one key; a hot one can't be split by adding nodes, only by sharding the
entity itself.

## Two patterns that matter most for chronicle

**1. Per-entity durable timers replace a central sweep.** Orleans *reminders* and DO
*alarms* let each entity persist its own next wake-up (renewal, expiry, retry),
at-least-once, surviving restarts. There is no `O(all entities)` scan to run, shard, or
fall behind — a sweep remains only as a coarse safety-net reconciler. Orleans' guidance
that reminders are *minutes*-grained (not sub-second) is a useful boundary on what a
durable-timer design can promise.

**2. Ownership can live in Redis.** Orleans ships a `RedisGrainDirectory`: per-entity
placement with the ownership map in Redis (so it survives a full-cluster restart),
*not* requiring a bespoke consensus layer. This is the managed-Redis-friendly middle
path — consistent-hash ownership of subscriptions with a Redis-backed directory, no
Cloudflare/Postgres dependency.

**Rebalancing should be cooperative, not stop-the-world.** Kafka's KIP-848 moves
assignment to a broker-side coordinator and hands over only the delta (GA in 4.0);
Akka buffers messages for a shard during handoff, stops the entities, and re-creates
them from persistence at the new owner ("state is not migrated" — stop here, resume
there). Both keep unaffected owners running while capacity changes.

## Reliable delivery without distributed transactions

The **transactional outbox + CDC** pattern: write business state and an outbox row in
one local transaction, then a relay (e.g. Debezium tailing the WAL/binlog) ships outbox
rows to the broker — atomic with the state change, at-least-once, requiring idempotent
consumers. It's how you get reliable event emission out of a control plane without 2PC.
(chronicle's doc-10 "outbox" slice is the same idea applied to wake emission.)

## Regional DR: two families

**Active-passive / async** — low local latency, simple, but **RPO > 0** (you can lose
the unreplicated tail) and failover is an orchestrated promotion:
- Kafka MirrorMaker 2 (unidirectional, offset translation for consumer failover).
- NATS *mirror*/*source* streams (store-and-forward, eventual, survives WAN partitions).
- Pulsar asynchronous geo-replication (per-namespace broker forwarding).
- **Redis** OSS replica in another region + `WAIT` (bound how many replicas acked before
  returning) and `WAITAOF` (7.2+, also require AOF fsync). **`WAIT` is not strong
  consistency** — it only narrows the failover loss window.

**Active-active / synchronous / stretched** — RPO ≈ 0 or local-write-anywhere, at higher
cost:
- **Redis Enterprise Active-Active (CRDB)** uses CRDTs (OR-Set for sets, counter CRDTs
  for `INCR`, LWW for strings): every region takes local reads/writes and converges with
  type-aware conflict resolution. **Strong-*eventual*, not linearizable**, and only
  operations that map to a CRDT are safe.
- NATS stretched RAFT super-cluster (immediate consistency, but every write pays
  inter-region RTT; tolerates `floor((N-1)/2)` region loss).
- Pulsar synchronous geo-replication (BookKeeper quorum across DCs — strongest, slowest).
- Bidirectional Kafka MM2 active-active is widely cautioned against over distance
  (offset-translation complexity, no cross-region ordering).

Actor frameworks (Orleans/Akka) are usually single-cluster-per-region; their DR is
"rebuild entities from persistence in the surviving region," so DR quality is the
backing store's replication, not the framework's.

## Distilled for chronicle's subscription control plane

1. **Consistent-hash ownership** of subscriptions (`hash(subId) → owner`), with the
   ownership map in **Redis** (the Orleans `RedisGrainDirectory` precedent). A request
   routes to a subscription's single owner — no cross-node locks, no `CROSSSLOT`.
2. **Cooperative rebalancing** on node join/leave: move only affected ranges; keep
   subscription state persistent so handoff is stop-here-resume-there.
3. **Per-subscription durable timers** (its next wake/expiry/retry) replacing the `O(K)`
   sweep; keep a slow sweep as a reconciler only.
4. **Outbox** for reliable wake emission (doc-10).
5. **DR active-passive first** (`WAIT`/`WAITAOF` to bound RPO); active-active CRDT only
   if local writes in every region are required and the data is CRDT-friendly.

## Sources

Redis: *Scalability/Clustering* (16,384 slots), Percona *hash tags & hot spots*, Redis
KB *Hot Key Imbalance*, *CROSSSLOT best practices*, *WAIT/WAITAOF*, *Active-Active CRDTs*.
Kafka: Confluent *Consumer Group Protocol*, *KIP-848*, StreamNative *multi-region/MM2*.
NATS: Synadia *Multi-Region Consistency Models*. Pulsar: *Geo-Replication concepts*.
Orleans: MS Learn *Grain Directory* + *Timers & Reminders* (+ `RedisGrainDirectory` via
the dotnet/orleans repo). Akka: *Cluster Sharding concepts*. Cloudflare: *DO Alarms*.
Outbox: Debezium WAL/binlog tailing.

**Verified** from vendor/project docs and the dotnet/orleans repo. The
*distilled-for-chronicle* section is **synthesis**, not published guidance.
