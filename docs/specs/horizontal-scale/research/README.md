# Horizontal-scaling research

**Question.** chronicle is one stateless Go binary doing two jobs: durable-streams
serving (append/read/long-poll/SSE) and the `__ds` subscription control plane (the
recovery sweep + lease/retry workers). The load test (issue #2) showed the control
plane is pinned to a single `{__ds}` Redis slot and the sweep is `O(N·K)` across N
replicas — so "spin up more replicas" scales serving but not subscriptions. This
folder researches how comparable systems scale horizontally and survive a region
loss, and proposes options for chronicle.

## Headline: the Cloudflare-DO hypothesis is wrong

We suspected Electric Agents' managed Durable Streams runs subscription management
on Cloudflare Durable Objects (one DO per entity). **It does not.** Verified against
the actual source: Electric splits into a **Postgres-backed control plane**
(per-entity claim rows with epoch + lease + heartbeat) and a **closed-source streams
store fronted by a Cloudflare *CDN*** for read fan-out. Durable Objects appear only
(a) as a *sync target* you can sync shapes into and (b) as the proprietary pattern
Electric's blog positions *against*. So the relevant prior art isn't "use DOs" — it's
their **per-entity claim-row** coordination and **CDN-cached, offset-keyed reads**.
Details in [01-electric-agents.md](01-electric-agents.md).

## The pattern every system converges on

Across Redis Cluster, Kafka, NATS JetStream, Pulsar, Orleans, Akka, and Cloudflare
DOs, horizontal scale is the same move: **partition the keyspace, give each partition
exactly one owner, and keep no central coordinator in the request path.** Two
corollaries matter most for chronicle:

- **Per-entity durable timers replace a central scan.** Orleans reminders and DO
  alarms let each entity schedule its own next wake — there is no `O(all entities)`
  sweep to run, shard, or fall behind. A sweep survives only as a safety-net
  reconciler. ([03](03-prior-art-redis-and-beyond.md))
- **Ownership can live in Redis.** Orleans' `RedisGrainDirectory` proves you can do
  per-entity placement with a Redis-backed ownership map — a managed-Redis-friendly
  middle path that fits chronicle's no-AWS / managed-Redis-8 constraint without a
  platform bet on DOs.

## What it means for chronicle

The work-vs-state distinction from issue #2 holds and sharpens:

1. **Shard the sweep *work*** across replicas (consistent-hash by subscription id) →
   `O(K)` regardless of N. Cheap, fence-safe.
2. **Shard the `{__ds}` *state*** across hash-tag slots → distributes capacity.
3. **Per-subscription timers / an outbox** instead of the `O(K)` sweep → removes the
   scan entirely; the sweep becomes a reconciler.
4. **Split serving from the control plane** only if their load profiles diverge — the
   one-binary design is simpler and currently adequate.
5. **DR: active-passive first** (async cross-region replica, `WAIT`/`WAITAOF` to bound
   RPO). Active-active (Redis Enterprise CRDT) only if you need local writes in every
   region and can accept strong-*eventual* semantics.

The full proposal, grounded in chronicle's code, is in
[04-options-for-chronicle.md](04-options-for-chronicle.md).

## Documents

| | |
|---|---|
| [01-electric-agents.md](01-electric-agents.md) | What Electric Agents actually does (the DO hypothesis, busted) |
| [02-cloudflare-durable-objects.md](02-cloudflare-durable-objects.md) | The DO model as a scaling primitive — and the lock-in catch |
| [03-prior-art-redis-and-beyond.md](03-prior-art-redis-and-beyond.md) | Redis Cluster, Kafka, NATS, Pulsar, Orleans, Akka, outbox/CDC, DR |
| [04-options-for-chronicle.md](04-options-for-chronicle.md) | Proposed options for chronicle, with tradeoffs and a recommendation |

## On verification

The external-system findings (01–03) are sourced to vendor docs, engineering blogs,
and — for Electric — the actual `electric-sql/electric` repo schema. Each doc marks
what is **confirmed** vs **inferred**. The chronicle-specific proposal (04) is design
synthesis from this prior art applied to chronicle's code, **not** anyone's published
guidance — it is a starting point for a design doc, not a decision.
