# 07 — Subscription, wake & lease durability

**Purpose:** design a crash-resilient, Redis-backed `__ds` subscription / wake /
lease layer for chronicle. The Durable Streams protocol defines subscriptions as
*durable cursors* and **MUST**s that their state survive a server restart
(PROTOCOL.md §6–7). Both reference implementations — the TS dev server and the Go
Caddy plugin — keep this state in an in-memory map and lose it on restart. That
is the durability gap chronicle exists to close: it already persists the *log* to
Redis; this study designs the subscription layer to persist there too.
Written 2026-06-13. Builds on [03-caddy-store.md](03-caddy-store.md) (the store
contract) and [05-redis-design.md](05-redis-design.md) (the Redis data model).

**Recommendation (TL;DR):** the `__ds` layer is **not implemented today** —
chronicle is the core stream store only, and `/v1/stream/__ds/*` returns `501`.
Build the layer as a faithful Redis translation of the Caddy webhook engine:

- Per-consumer **HASH** `ds:{cid}:cons` holds the durable cursor map, `state`,
  `generation` (the fencing token), `wake_id`, `holder`, and the retry/failure
  bookkeeping — every field the Caddy `ConsumerInstance` keeps in RAM.
- The two Caddy goroutine timers become two **due-scored ZSETs**:
  `ds:sched:lease` (the 45 s liveness lease) and `ds:sched:retry` (the
  exponential backoff). They *are* the timers, now durable.
- The `streamConsumers` fan-out map becomes a **SET** `ds:{path}:subs`, read
  inside `append.lua` so an append atomically enqueues wakes — the same Lua-over-
  hash-tag idiom the core store already uses.
- Claims, acks, releases, and the cursor advance go through one **Lua CAS**
  (`fence.lua`) gated on `generation` + `wake_id`; stale callers get `409 FENCED`.
- **The claim re-scores the lease, it never `ZREM`s it** (§6.1) — otherwise a
  worker crash mid-delivery loses the wake.
- Durability is `appendfsync always` + `WAITAOF` + one replica for the protocol's
  MUSTs; delivery is **at-least-once**, with correctness from the generation
  fence making duplicates safe (§9).
- **A durable cursor is necessary but not sufficient** to close the restart gap.
  Wake creation is event-driven with no scanner, so an idle subscription is never
  re-evaluated after a restart. Chronicle must also run a **recovery sweep** on
  boot and on an interval that recomputes `HasPendingWork` against the durable
  cursors and re-fires (§8). This is the `dispatchRecoveryIntervalMs` the
  upstream coordinator declares but never implements.

A Redis Streams–native design (consumer-group PEL as the lease) is documented in
§4 as the considered alternative; it is elegant but a poor fit for chronicle (one
`min-idle-time` per stream cannot express per-subscription `lease_ttl_ms`, and it
still needs a companion ZSET for backoff). The ZSET-scheduler is recommended.

---

## 1. What chronicle implements today (and what it does not)

Chronicle implements the **core stream store** on Redis and nothing of the
subscription layer. The `store.Store` interface has exactly 11 methods — `Create`,
`Get`, `Has`, `Delete`, `Append`, `CloseStream`, `CloseStreamWithProducer`,
`Read`, `WaitForMessages`, `GetCurrentOffset`, `Close` — and not one names a
subscription, wake, consumer, lease, or webhook (`store/store.go:98-152`). The
reserved control route `/v1/stream/__ds/*` is mounted but returns `501`
(`mount.go:44`, `handler.go:67-69`).

The Redis data model it *does* have is the toolkit for building the rest. Per
stream, under one `{path}` hash tag: a `:meta` HASH, a `:prod` HASH, a `:msg`
ZSET of lex-ordered frames, a `:forks` SET, and a `ds:notify:` pub/sub channel
(`store/redis/keys.go:19-108`). Every mutation is a single atomic Lua `EVALSHA`
over seven scripts sharing `common.lua` (`scripts.go:11-109`). `WaitForMessages`
is pub/sub + subscribe-recheck + a 1 s defensive poll (`notify.go:13-110`).

The subscription layer reuses all of it: HASHes for records, a ZSET for
scheduling, SET indexes for fan-out, Lua for atomicity, the `{...}` hash tag for
cluster-safety, and the pub/sub-plus-poll pattern for low-latency-but-reliable
wakeups.

## 2. The thing to port — the Caddy in-memory wake/lease engine

The Caddy plugin's `webhook` package is a complete subscription engine in four
files, and **every byte of its state is in RAM** — lost on restart. The porting
checklist is its field list.

**The `Store` holds four maps** (`webhook/store.go:11-28`): `subscriptions`
(id → `Subscription`), `consumers` (cid → `ConsumerInstance`), and two reverse
indexes — `subscriptionConsumers` (sub → consumer set) and `streamConsumers`
(stream path → consumer set, the **wake fan-out index**).

A **`Subscription`** is config only — `SubscriptionID`, `Pattern` (glob),
`Webhook` (URL), `Description` (`types.go:18-23`).

A **`ConsumerInstance`** carries all runtime state (`types.go:26-46`):

| Field | Meaning |
| --- | --- |
| `Streams map[path]offset` | the **durable cursor** — last acked offset per stream, init `"-1"` |
| `State` | `IDLE` / `WAKING` / `LIVE` |
| `Epoch int` | the **fencing/generation token**, bumped on every wake |
| `WakeID`, `WakeIDClaimed` | the single-use claim token for the current wake |
| `LastCallbackAt` | liveness timestamp (the lease clock) |
| `First/LastWebhookFailureAt`, `RetryCount` | backoff input + the 3-day GC window |

Note Caddy has **no separate `generation` and no `holder` field**: `Epoch` *is*
the generation, and the "lease" is purely the liveness timer plus
`LastCallbackAt` (`store.go:152-158`). The protocol adds an explicit `holder`;
the port introduces it as a HASH field.

**The lifecycle**, all driven by two goroutine timers:

- `OnStreamAppend → wakeConsumer → deliverWebhook` (async goroutine).
  `HasPendingWork = ∃ stream where tail > acked` (`store.go:225-235`); a wake
  bumps `Epoch`, mints a `WakeID`, builds a signed Ed25519 webhook with a
  callback URL + JWT, and POSTs (`manager.go:65-153`).
- `resetLivenessTimeout` = the **lease**: a 45 s (`livenessTimeoutMS`)
  `time.NewTimer` in a goroutine; if no callback arrives while `LIVE`, it forces
  `IDLE` and re-wakes if work remains (`manager.go:19,387-409`).
- `scheduleRetry` = the **backoff**: `min(2ⁿ·100ms, 30000)+jitter`, switching to
  a steady `60000+jitter` after 10 attempts; a 3-day first-failure window GCs the
  dead webhook (`manager.go:21-23,212-269`).
- Callbacks are **generation-fenced**: a stale epoch, a bad/expired token, or a
  stale `wake_id` are all rejected (`manager.go:272-333`, `store.go:161-172`).

**What a restart loses:** every subscription, consumer, cursor, state, epoch,
`wake_id`, both timers, and the failure history. The bytes in the log survive
(they are on disk); the knowledge of *where each subscriber is and what is owed*
does not. That is crash window 6 / "the sharp edge."

## 3. What the protocol demands of a durable subscription layer

From `PROTOCOL.md` §6–7 — the bar the Redis layer must clear:

- **Subscriptions are durable cursors.** L820: *"Subscriptions are durable
  cursors that wake workers when one or more streams have pending events."* The
  per-stream state is `{path, link_type, acked_offset}` (L836-844).
- **The cursor MUST be persistent.** `acked_offset` is *"Last processed offset,
  inclusive"* (L842) and offsets *"remain valid for the lifetime of the stream"*
  (L1190).
- **The retry schedule MUST survive a restart.** L1079: *"Retry metadata,
  including `next_attempt_at`, MUST be persisted across Durable Object eviction
  so a freshly-loaded object honors the prior retry schedule."* This is the
  hardest MUST and the one both reference servers fail.
- **Lease expiry MUST reschedule.** L1182: *"When a lease expires, the server
  MUST clear the holder and wake token; if pending work remains, it MUST schedule
  another wake."* `lease_ttl_ms` is bounded 1 s – 10 min, default 30 s (L872).
- **Fencing.** Stale `generation` / `wake_id` callbacks are rejected `409 FENCED`;
  stale producer epochs on the append path are `403`.
- **Signing keys SHOULD persist.** Ed25519 private keys, `kid` from the JWK
  thumbprint, old public keys published until past the ~5 min replay window
  (L1002).

The subscription conformance suite is gated behind `options.subscriptions`, so it
runs only against a server that advertises the layer — chronicle opts in once the
layer exists.

## 4. Two Redis designs considered

Both keep all durable facts in the `ds:` keyspace under per-subscription /
per-stream hash tags, mutated by atomic Lua, exactly like the core store. They
differ in how they model the **wake queue and the lease**.

### 4.1 Option A — Redis Streams-native (the PEL is the lease)

A wake is an `XADD` to `ds:{sub}:wake`. Chronicle replicas form one consumer
group; `XREADGROUP` delivers each wake to exactly one replica and books it into
that replica's PEL — **the pending entry is the lease, its idle-time is
`lease_ttl_ms`**. On a `{done}` 2xx the worker advances the durable cursor and
`XACK`s. A crash leaves the entry pending; `XAUTOCLAIM` (or 8.4 `XREADGROUP
CLAIM`) past `min-idle-time` reassigns it to another replica. Subscription
config, cursors, generation, `wake_id`, holder, and failure window live in
HASHes; retry backoff still needs a companion `ds:due` ZSET swept into the stream
(Streams have no native delayed delivery).

**Why it loses for chronicle:** `min-idle-time` is one value per stream, but
`lease_ttl_ms` is **per subscription** (1 s – 10 min). Mixing subscriptions with
different TTLs in one wake stream breaks the lease semantics, forcing one stream
per subscription — potentially thousands of `XREADGROUP` targets. It is elegant
and gives at-least-once redelivery "for free," but it introduces a primitive
chronicle does not otherwise use *and still needs a ZSET for backoff*. The PEL
also redelivers only *already-delivered-then-orphaned* entries; an `XADD`'d but
never-claimed wake recovers via the group's `>` cursor, not `XAUTOCLAIM` — two
recovery paths to reason about.

### 4.2 Option B — ZSET-scheduler + HASH-state (recommended)

A line-for-line translation of the Caddy package. Each consumer is a HASH; the
two goroutine timers become two **due-scored ZSETs**. One uniform mechanism, the
exact idioms `05-redis-design.md` already chose, and per-subscription
`lease_ttl_ms` falls out naturally because the score is per member.

### 4.3 In-memory → Redis mapping (Option B)

| Caddy in-memory | Redis (durable) | Survives restart because… |
| --- | --- | --- |
| `consumers[cid]` instance | HASH `ds:{cid}:cons` (all fields below) | a HASH on the AOF-persisted cluster |
| `Streams[path]` cursor | HASH field `cur:<escPath> = acked_offset` | the core durability MUST (L1190); advanced only forward by Lua CAS |
| `Epoch` (fencing token) | HASH field `generation`, `HINCRBY` on wake | monotonic integer is replayed from the AOF |
| `WakeID` / `WakeIDClaimed` | HASH fields `wake_id`, `wake_claimed`, `holder` | claim is a Lua CAS; a pre-crash claim keeps `holder` set |
| `State` IDLE/WAKING/LIVE | HASH field `state` (written with `generation`) | reconciled from the lease ZSET entry on boot |
| `resetLivenessTimeout` 45 s | ZSET `ds:sched:lease`, score = `lease_expiry_ns` | the deadline is a durable score, not a goroutine |
| `scheduleRetry` backoff | ZSET `ds:sched:retry`, score = `next_attempt_at_ns` | satisfies L1079 — the schedule is in Redis |
| `First/LastWebhookFailureAt`, `RetryCount` | HASH fields `first_fail_ns`, `last_fail_ns`, `retry_count` | the 3-day GC clock is anchored in Redis |
| `streamConsumers[path]` fan-out | SET `ds:{path}:subs`, read in `append.lua` | a durable SET; post-restart appends still wake |
| `subscriptionConsumers[sub]` | SET `ds:{sub}:cons` (cascade delete) | durable SET |
| Ed25519 key + JWKS | HASH `ds:jwks` `kid → {priv, pub, status}` | L1002 SHOULD-persist; same `kid` signs after restart |
| both goroutine timers + the map | **gone** — replaced by durable Redis structures | nothing in RAM to lose |

## 5. Recommendation — ZSET-scheduler + HASH-state

Adopt Option B. It is the smallest, most faithful port; it matches chronicle's
existing HASH/ZSET/Lua/`{hash-tag}` idiom; it expresses per-subscription lease
TTLs natively; and it needs no primitive the core store does not already use.
The four moving parts:

1. **State in HASHes**, mutated only by Lua scripts that also touch the schedule
   ZSETs in the same call (single-slot atomic via the consumer's hash tag).
2. **Two ZSETs as the timers** — `lease` and `retry` — scored by due-timestamp.
3. **A worker tick** on every replica: one Lua `claim` script pops due members
   atomically (§7), then the worker performs the side effect (fire webhook, or
   run lease-expiry).
4. **`fence.lua`** on every callback: reject unless `generation` (and `wake_id`
   for the claim) match the stored values → `409 FENCED`; else compare-and-
   advance the cursor.

## 6. Three corrections the adversarial pass forced

These are not optional polish; each is a place a naïve port is silently wrong.

### 6.1 The claim re-scores the lease — it never `ZREM`s

`ZRANGEBYSCORE … LIMIT` then `ZREM` is the textbook Redis delayed-queue claim,
and it is **wrong for a lease**: if the worker dies after the `ZREM` but before
finishing the delivery, the item is gone — there is no lease left to expire, and
the wake is lost. The claim must instead **re-score the member forward to
`now + visibility`** (a short in-flight lease) inside the same Lua script. A
crashed worker's item simply falls due again and another replica reclaims it.
This is at-least-once: a crash *after* the side effect but before the final ack
redelivers and re-fires — which is exactly why delivery must be idempotent (§9).
Set the visibility window from `lease_ttl_ms`, not a constant, so it always
exceeds the worst-case webhook timeout (~30 s) — otherwise a slow-but-alive
delivery is reclaimed and double-fired.

### 6.2 At-least-once, not exactly-once; correctness is the fence, not the lease

Redis Streams consumer groups and ZSET leases are both **at-least-once** — reclaim
is purely time-based, with no liveness check on the original owner, so a slow GC
pause can let two workers fire the same wake concurrently. Lua atomicity covers
only the *in-Redis* transition, never the worker's external webhook POST. So the
design never promises exactly-once delivery; it promises *at-least-once delivery
+ a fence that makes the duplicate harmless*. The `generation` + `wake_id` CAS in
`fence.lua` is the actual correctness mechanism: a duplicate or late callback is
`409 FENCED` and cannot advance the cursor twice. Handlers dedupe on
`(consumer_id, generation, wake_id)`.

### 6.3 A durable cursor alone does NOT close window 6 — you also need a sweep

This is the load-bearing one. The restart gap is weak on **two** independent
counts, and persisting the cursor fixes only the first:

1. the cursor was lost → a durable HASH field fixes this; and
2. **wake creation is purely event-driven — there is no scanner.** A wake is
   minted only by a new append or by an in-flight wake's lifecycle ending. After
   a restart, an *idle* subscription has no in-flight wake and (if its streams
   are quiet) no new append, so its `HasPendingWork` is never evaluated and
   nothing re-fires — even though the durable cursor now makes the answer
   recomputable.

So the durable cursor is necessary but not sufficient. Chronicle must run a
**recovery sweep** (§8) that actively recomputes `HasPendingWork` against the
durable cursors and re-fires. The cursor makes the question answerable; the sweep
asks it.

## 7. Keyspace and the worker loop

```
-- KEYSPACE (ds:, hash-tagged for single-slot atomic Lua) --
ds:{sub:<id>}:sub        HASH  pattern, webhook, lease_ttl_ms, cfg_hash
ds:{sub:<id>}:cons       SET   member cids (cascade delete)
ds:{<cid>}:cons          HASH  state, generation, wake_id, wake_claimed, holder,
                               retry_count, first_fail_ns, last_fail_ns,
                               cur:<escPath> = <acked_offset>
ds:{<path>}:subs         SET   wake fan-out index (read inside append.lua)
ds:sched:lease           ZSET  cid -> lease_expiry_ns    (the 45s liveness lease)
ds:sched:retry           ZSET  cid -> next_attempt_at_ns (exponential backoff)
ds:jwks                  HASH  kid -> ed25519 key material

-- WAKE on append (folded into append.lua, atomic with the write):
for cid in SMEMBERS ds:{path}:subs:
  if HGET cur:<path> < new_tail:                 -- HasPendingWork
    gen = HINCRBY ds:{cid}:cons generation 1
    HSET ds:{cid}:cons wake_id=<sha(gen,rand)> holder='' state='WAKING'
    ZADD ds:sched:lease <now+lease_ttl> cid      -- arm the lease

-- claim.lua  (KEYS = a sched ZSET; ARGV = now_ns, limit, visibility_ns)
-- ATOMIC: read due members, re-score them forward to an in-flight lease,
-- and return them. Re-score (never ZREM) so a crashed worker's item recurs.
local due = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, ARGV[2])
for _, cid in ipairs(due) do
  redis.call('ZADD', KEYS[1], ARGV[1] + ARGV[3], cid)   -- lease forward
end
return due

-- worker tick (every replica; pub/sub ds:wake is a latency hint only):
for cid in claim(ds:sched:retry):  redeliver_webhook(cid)
for cid in claim(ds:sched:lease):  lease_expiry(cid)   -- clear holder; re-wake if pending

-- fence.lua on a callback (claim / ack / release):
--   reject unless ARGV.generation == HGET generation  (+ wake_id for claim) -> 409 FENCED
--   else advance cursor: HSET cur:<path> = max(stored, acked)  (forward-only)
--   on done: ZREM both sched ZSETs; on heartbeat: re-ZADD lease forward
```

## 8. Closing window 6 — the recovery sweep

The piece the upstream coordinator declares (`dispatchRecoveryIntervalMs`,
`staleOutstandingWakeAfterMs`) and never implements. Chronicle implements it,
and it is small because all the state it needs is now durable:

- **On boot**, and on an interval (say every `lease_ttl/2`), a sweeper scans
  subscriptions and, for each consumer in `IDLE`/`WAKING`, recomputes
  `HasPendingWork` from the durable cursor (`HGET cur:<path>`) versus the durable
  tail (`HGET ds:{path}:meta current_offset`). If pending, it mints a wake — the
  same `wake.lua` the append path uses.
- The sweep is **idempotent** under the generation fence: if it races a real
  append's wake, both bump `generation`, one supersedes, and the stale one is
  fenced. Worst case is one extra webhook the idempotent handler absorbs.
- It is the same scan that backfills a newly-created subscription's cursors at
  the current tail, so the two share code.

With durable state **and** the sweep, an origin restart re-evaluates every
subscription and re-fires anything owed — closing window 6 for idle entities, not
just busy ones. The lease and retry ZSETs additionally re-drive anything that was
mid-flight, because their due-scores are already in Redis.

## 9. Durability honesty

In the spirit of [PLAN.md §4.7](../PLAN.md):

- **The guarantee is at-least-once, fenced.** A wake delivered may be delivered
  again (lease reclaim, retry, sweep); the `generation`/`wake_id` fence makes the
  duplicate safe. Exactly-once is not offered and not needed — the protocol
  itself is at-least-once.
- **The fsync policy sets the loss window.** `appendfsync everysec` (the Redis
  default) can lose ~1–2 s of writes on a hard crash or an un-fsynced failover; an
  acked-cursor advance written just before a crash may be lost and re-delivered.
  To honor the protocol's MUSTs, gate the durability-critical writes on
  `appendfsync always` + `WAITAOF` to a replica. Even then `WAITAOF` *"does not
  make Redis a strongly-consistent store"* — a failover can drop the last
  unsynced write — which is precisely why the design is at-least-once and leans on
  the fence rather than on never losing a write.
- **Leases are not write-safety.** A Redis TTL lease alone cannot fence a stalled
  worker (the Kleppmann result); correctness comes from the monotonic
  `generation` token, not from lease mutual exclusion. The lease only bounds
  *redelivery latency*, never *correctness*.
- **Multi-replica without a leader.** N stateless chronicle replicas run the same
  tick. The claim is one Lua script — Redis is single-threaded, so exactly one
  replica's re-score wins a given due member per tick; peers see it already
  re-scored and skip. No leader election for the workers. The only near-singleton
  is the recovery sweep, kept idempotent (the fence) so concurrent sweepers are
  harmless. Shard the ZSETs by `crc16(cid) % N` to cut contention; correctness
  does not depend on the sharding.

## 10. Decision log

| # | Decision | Why |
| --- | --- | --- |
| 1 | Port the `__ds` layer to Redis rather than copy Caddy's in-memory map | The whole point of chronicle: close the restart gap the in-memory servers have (PROTOCOL §6–7 MUSTs) |
| 2 | ZSET-scheduler + HASH-state over Streams-native | One uniform mechanism, matches the core store's idioms, expresses per-subscription `lease_ttl_ms`; Streams force one stream per sub and still need a ZSET |
| 3 | Claim **re-scores** the lease, never `ZREM`s | `ZREM`-on-claim loses the item on a mid-delivery crash; re-score makes it recur (§6.1) |
| 4 | At-least-once + `generation` fence, not exactly-once | Reclaim is time-based and Lua can't fence the external POST; the fence makes duplicates safe (§6.2) |
| 5 | Durable cursor **and** a recovery sweep | A durable cursor alone is never re-evaluated for an idle sub after restart — wake creation is event-driven with no scanner (§6.3, §8) |
| 6 | `appendfsync always` + `WAITAOF` + replica for durability-critical writes | The protocol MUSTs vs the `everysec` ~1–2 s loss window (§9) |
| 7 | Workers leaderless via single-script atomic claim; sweep idempotent | Single-threaded Redis makes the claim a CAS; the fence absorbs sweep races (§9) |

---

*Next: a `08`-style implementation plan (the `store.Store`-parallel
`SubscriptionStore` contract, the Lua script set, and the conformance opt-in)
once this design is ratified.*
