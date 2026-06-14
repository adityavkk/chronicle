# 04 — Horizontal-scaling options for chronicle

Design synthesis from the prior art ([01](01-electric-agents.md)–[03](03-prior-art-redis-and-beyond.md))
applied to chronicle's code. This is input to a design doc, not a decision. It assumes
the constraint: **no AWS, managed Redis 8, cloud-agnostic.**

## Where we are

One stateless Go binary runs both concerns:

- **Serving** — append/read/long-poll/SSE. Stateless handlers; per-stream data keyed by
  `ds:{path}:…`, so different streams land on different Redis-cluster slots. Long-poll/SSE
  wake cross-replica via Redis pub/sub (`ds:notify:{path}`). **This already scales out —
  given a Redis Cluster.**
- **Control plane** (`subscriptions.go`, `webhook/`) — the recovery sweep + lease/retry
  workers, all in **one `{__ds}` hash-tag slot** (`ds:{__ds}:subs`, `:sub:<id>`,
  `:sub:<id>:links`, `:stream:<path>`, `:sched:lease`, `:sched:retry`). The sweep is
  `O(K)` per replica and `O(N·K)` across N replicas (`SMEMBERS subs` then a batched read
  of all K — `webhook/manager.go`). The lease/retry workers already de-dup across replicas
  by re-scoring due items (`claim_due.lua`); the sweep does **not**.

The work-vs-state framing from issue #2: **sharding the work** (H1) stops adding replicas
from hurting; **sharding the state** (H2) adds capacity; you need both for the control
plane to scale, and replicas alone do neither.

## The options

### O1 — Shard the sweep *work* across replicas (the cheap first step)

Partition K by `hash(subId) % N == ordinal` (or claim id-ranges from a ZSET, reusing the
`claim_due.lua` re-score pattern). Each replica sweeps only its slice → total work `O(K)`
regardless of N. The `generation`/`wake_id` **fence already makes mis-sharding harmless**
(a sub swept twice just coalesces at `arm_wake`), so this is a low-risk efficiency change.

- **Needs:** replica identity/count — a StatefulSet ordinal, or a Redis-backed membership
  lease (the prior art's lesson: keep the directory in Redis, à la Orleans
  `RedisGrainDirectory`).
- **Limit:** every slice still reads the *same one* `{__ds}` node. **O1 stops the bleeding;
  it does not add capacity.**

### O2 — Shard the `{__ds}` *state* across slots (the capacity lever)

Spread subscriptions across `S` hash-tag slots: `ds:{__ds:0}:…` … `ds:{__ds:S}:…` by
`hash(subId) % S`. Control-plane reads/writes distribute across Redis-cluster nodes,
lifting the single-node ceiling.

- **Cost:** cross-slot operations get harder — the global sub-list and the per-stream
  fan-out index must route by sub-slot (`OnStreamAppend` for stream `p` must find
  subscribers possibly spread across slots), and the sweep iterates `S` slots. No
  cross-slot transactions (`CROSSSLOT`), so per-slot Lua + app-level coordination.
- **Pairs with O1:** O1 shards the work across replicas, O2 shards the state across nodes —
  together the control plane scales horizontally.

### O3 — Per-subscription durable timers / outbox (remove the `O(K)` scan)

The biggest structural lever, and the clearest lesson from Orleans reminders / DO alarms:
**each subscription schedules its own next wake** instead of a central loop scanning all K.

- On the **outbox** form (doc-10 slice-1): a due-scored ZSET of subscriptions that *have
  owed work*; a worker processes only the due set (`O(owed)`, not `O(K)`), and sub-sweep
  recovery latency becomes possible (a due score fires before the next tick). Arm on
  append, clear on ack — the transactional-outbox pattern from [03](03-prior-art-redis-and-beyond.md).
- The sweep survives only as a **coarse reconciler** (the safety net the prior art always
  keeps), run rarely.
- **At-least-once + idempotent** is the contract (already true via the fence). Orleans'
  boundary applies: durable timers are coarse (seconds), fine for recovery, not a
  sub-millisecond hot path — which matches the sweep's role.

### O4 — Owner-replica / actor model (the end state)

Consistent-hash **ownership** of subscriptions: each replica *owns* a slice — its sweep,
its fast-path wakes (`OnStreamAppend`), and its claim loop — with a **Redis-backed
ownership directory** and cooperative handoff on churn (Akka/Kafka-KIP-848 style: stop
here, resume there, state stays in Redis). This is O1 + the missing cross-instance fast
path (today `OnStreamAppend` is in-process, so only the appending replica fires the fast
wake; other replicas rely on the sweep). It also lets serving and the control plane scale
independently.

- **Cost:** a real membership + rebalancing protocol (the hardest piece). Reserve until
  O1 + O3 are not enough.

### O5 — Split serving from the control plane (separate deployables)

Run the `__ds` control plane (sweep/workers/coordinator) as its own service, separate
from the stateless serving tier.

**For:** independent scaling (serving load and subscription load have different shapes);
CPU isolation (today the sweep/workers are goroutines competing with request-serving on
the same node — a heavy sweep degrades serving p99 and vice versa); a smaller blast radius.

**Against:** the one-binary design is operationally simpler and currently adequate (the
load test held at K=10k on a single small replica). A split adds a deployment, a network
hop, and a second thing to run for no benefit until the two loads actually diverge.

**Verdict:** **don't split yet.** Do O1 (and instrument the contention — the new
`/metrics` already expose sweep tick time and worker backlog). Split when the metrics show
the sweep/workers and serving genuinely competing, or when you adopt O4 (the coordinator
tier is the natural split point).

### Serving (for completeness)

Serving already scales out: stateless replicas + per-`{path}` data sharding. The
prerequisite to *use* that is a **Redis Cluster** (a single instance is the ceiling
regardless of replica count). The one structural gap is the **in-process subscription
fast-path wake** — `OnStreamAppend` fires only on the appending replica; cross-replica it
falls back to the sweep. O4 fixes this; a lighter fix is to route the subscription fast
wake through the existing `ds:notify` pub/sub the direct-consumer path already uses.

## Regional DR

| Posture | Mechanism | RPO / RTO | When |
|---|---|---|---|
| **Active-passive** (recommended first) | async cross-region Redis replica; `WAIT`/`WAITAOF` to bound the unsynced tail; orchestrated failover | RPO > 0 (small), RTO = promotion time | Default — control planes tolerate small RPO better than stretched-consensus latency |
| Active-active | Redis Enterprise CRDT (CRDTs: OR-Set/counters/LWW) | RPO ≈ 0, strong-*eventual* | Only if local writes in every region are required and the data is CRDT-friendly |

chronicle's **at-least-once + `generation`/`wake_id` fence** behaves correctly under
async-replica failover: a lost-tail wake is re-derived by the sweep/timer on the surviving
region, and the fence rejects stale acks from a deposed holder — so DR doesn't compromise
correctness, only the recovery-latency/RPO window. `WAIT` does **not** make this
linearizable; it only narrows the loss window (see [03](03-prior-art-redis-and-beyond.md)).

## Recommended sequencing

1. **Load-test the ceilings** (the rig does this): ramp K → 100k for the per-tick cliff;
   ramp N replicas and read managed-Redis CPU to quantify `O(N·K)`; push the `{__ds}` slot
   to saturation; measure recovery latency under failover.
2. **O1 — shard the sweep work.** Cheap, fence-safe; stops scale-out from degrading.
3. **O3 — outbox / per-subscription due-set.** Removes the `O(K)` scan; the lever for
   large K and sub-sweep latency.
4. **O2 — shard the `{__ds}` state.** The capacity lever; only when the single slot
   saturates (heaviest change — measure first).
5. **O4/O5 — owner-replica + split.** The end state for independent scaling and a
   cross-region fast path; reserve until 1–4 aren't enough.

## What to prototype + measure next

- A `BenchmarkSweepOnce`-style and rig measurement of **O2** at S=2,4,8 slots to confirm
  the capacity gain and the cross-slot fan-out cost.
- A spike of **O3** (the due-set outbox) to measure recovery latency vs the sweep, and the
  arm/clear write amplification on append/ack.
- The **O1** sharding key (`hash(subId) % N`) against real replica churn — does the fence
  truly make handoff races harmless under load? (the rig's pod-kill scenario).

These three are the cheapest experiments that de-risk the structural decisions before
committing to a rebuild.
