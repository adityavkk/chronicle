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

The work-vs-state distinction from issue #2 holds and sharpens — and a **later load test added
a third axis** (the Electric agents runtime against chronicle@main collapsed at 12 replicas on
per-type claim contention, ≤12% CPU on every tier; see
[05](05-proposed-architecture.md#a-third-axis-per-type-claim-contention-from-the-load-test)):

0. **Refine claim *granularity*.** The runtime registers one subscription per entity *type*
   (`<typeName>-handler`), so all of a type's entities and replicas share **one** single-holder
   lease. Neither sharding work nor state relieves it (one hot subId stays one slot, one lease).
   This was the *actual* collapse — sequence it **first**. Fix: per-entity / per-shard-of-type leases.
1. **Shard the sweep *work*** across replicas (consistent-hash by subscription id) →
   `O(K)` regardless of N. Cheap, fence-safe. *(Hardens the recovery sweep — not the hot-path contention above.)*
2. **Shard the `{__ds}` *state*** across hash-tag slots → distributes capacity.
3. **Per-subscription timers / an outbox** instead of the `O(K)` sweep → removes the
   scan entirely; the sweep becomes a reconciler.
4. **Split serving from the control plane** only if their load profiles diverge — the
   one-binary design is simpler and currently adequate.
5. **DR: active-passive first** (async cross-region replica, `WAIT`/`WAITAOF` to bound
   RPO). Active-active (Redis Enterprise CRDT) only if you need local writes in every
   region and can accept strong-*eventual* semantics.

The hardened proposal and implementer handoff is
[05-proposed-architecture.md](05-proposed-architecture.md); start there. The
adversarial review that produced it is [06](06-adversarial-review.md). The corrected
framing: the fence is *safety, not liveness*; "tunable consistency" on Redis is a
*durability + freshness* knob, not a CAP knob; and the standing decision is **Option A**
(one binary, slot-home whole subscriptions, per-shard schedules, a per-subscription
due-set, work-sharded leased ownership) — splitting into the Option B coordinator tier
only if the measurements force it, not as an open post-measurement choice. 05 carries the
full build spec for Option A.

## Documents

| | |
|---|---|
| [01-electric-agents.md](01-electric-agents.md) | What Electric Agents actually does (the DO hypothesis, busted) |
| [02-cloudflare-durable-objects.md](02-cloudflare-durable-objects.md) | The DO model as a scaling primitive — and the lock-in catch |
| [03-prior-art-redis-and-beyond.md](03-prior-art-redis-and-beyond.md) | Redis Cluster, Kafka, NATS, Pulsar, Orleans, Akka, outbox/CDC, DR |
| [04-options-for-chronicle.md](04-options-for-chronicle.md) | First-pass options (superseded by 05; see 06 for the corrections) |
| [05-proposed-architecture.md](05-proposed-architecture.md) | ⭐ The decision (Option A) + the full build spec — slot addressing, the new Lua scripts, membership/HRW, due-set, loop change-map, config, metrics; Option B kept as the deferred fallback |
| [06-adversarial-review.md](06-adversarial-review.md) | Adversarial review of 01–04 + the corrections folded into 05 |
| [07-jepsen-style-verification.md](07-jepsen-style-verification.md) | Jepsen-style safety + liveness test plan for 05, in Go (porcupine + the existing `jepsen/` harness) |

## On verification

The external-system findings (01–03) are sourced to vendor docs, engineering blogs,
and — for Electric — the actual `electric-sql/electric` repo schema. Each doc marks
what is **confirmed** vs **inferred**. The chronicle-specific proposal — first drafted in
04, superseded by the hardened [05](05-proposed-architecture.md) — is design synthesis from
this prior art applied to chronicle's code, **not** anyone's published guidance. The
direction (Option A) is now a decision; the *unmeasured numbers* it rests on are not — see
05's gating experiments.
