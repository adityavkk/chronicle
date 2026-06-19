# 08 — Claim granularity: per-shard-of-type leases (the third axis)

> **Status:** design + chronicle build (issue #11, P0 · the #1 priority). This is
> the load-bearing design [05](05-proposed-architecture.md#a-third-axis-per-type-claim-contention-from-the-load-test)
> flagged as *"the load-bearing design neither this doc nor 04's O1–O5 yet
> covers."* It owns the contention suite ([07](07-jepsen-style-verification.md)
> **C1/C2/C3**) and doc-05 **gate #6**.

## 1. The problem (recap)

A GKE load test (the Electric agents runtime against chronicle@main) proved the
binding constraint at 12 replicas is **per-type claim contention**, not the
`O(N·K)` sweep or `{__ds}` keyspace capacity. The Electric runtime registers one
subscription per entity *type* (`subscriptionId = "<typeName>-handler"`,
`entity-manager.ts:685`); chronicle holds exactly **one** single-holder
`(generation, lease)` per subscription (`ds:{__ds}:sub:<id>`). So every entity of
a type **and** every replica hosting one serialize on one lease: at 6 replicas the
wake path was clean, at 12 it fence-stormed (489–735 `FENCED`/pod, ~40% of
entities woke) while **every tier sat ≤12% CPU**. Slot-homing (#15) and leased
ownership (#14) shard *keyspace and ownership* — neither splits a hot
subscription's one lease, so neither touches this.

**The fix (05's third axis): make claim granularity match dispatch granularity.**
Let one logical type map to *many* leases — per-shard-of-type
(`"<type>-handler:<g>"`, `g = hash(entityId) % G`) — so concurrent claimants on
different entity-shards do not serialize. It is **cross-repo**: a **client** half
(the agents runtime choosing the finer granularity) and a **chronicle** half (one
subscription claimable by multiple concurrent holders over disjoint stream
subsets). This issue owns the chronicle half + the client contract.

## 2. The executable baseline (C1/C2 — reproduced today, no rebuild)

The `contention` scenario (`jepsen/checker`, `scenario_contention.go`) reproduces
the collapse with no rebuild: N workers claim/ack one logical type through the
real `claim.lua`/`ack.lua` against a local Redis, ramping 6→12→24, no fault.
Scaled timers (hold 5 ms, think 25 ms → ~6-claimant per-lease capacity) reproduce
the empirical 6-clean/12-collapse signature in seconds; the **ratio** (and thus
the knee location and its ~`G×` scaling), not the absolute ms, is what is faithful.

**G=1 (today's hot per-type lease) — the collapse:**

| N | busy/op | fenced/op | thru/worker | aggregate | p99 ms |
|---|---------|-----------|-------------|-----------|--------|
| 6  | 0.78 | 0.00 | 21.7 | 130 | 85  |
| 12 | 0.94 | 0.00 | 10.4 | 125 | 337 |
| 24 | 0.97 | 0.00 | 5.2  | 124 | 727 |

Aggregate throughput is **pinned flat (~125/s)** while per-worker throughput
halves at every rung — adding claimants adds *zero* throughput because they all
serialize on one lease. C2 flags the knee at N=12 (−49%) and N=24 (−69%); C1 flags
the runaway `ALREADY_CLAIMED` rate. `fenced/op` stays 0 locally: a literal *fence
storm* additionally needs lease lapses (queueing past the 30 s TTL), which a
sub-ms local Redis does not produce — the local suite reproduces the
**BUSY-contention throughput knee** (the executable C2 signature), and the literal
fence storm is the GKE campaign (§8). Full numbers: `docs/jepsen/results.md`.

## 3. Design decision: per-shard-of-type leases, fixed **G = 16**

Two candidate granularities (05): **per-entity** (one lease per entity) or
**per-shard-of-type** (a small fixed `G`). We choose **per-shard-of-type with
G = 16**:

- **Per-entity is unbounded.** A type can have millions of entities; one lease
  record (and one schedule member, one due mark) per entity multiplies the control
  plane by the entity count and defeats wake coalescing (a wake per entity, not per
  type). Per-shard-of-type bounds the fan-out to a small constant `G` per type.
- **G = 16 moves the knee well past the failure point.** The collapse is at ~6–12
  concurrent claimants per lease; `G=16` lifts the per-type capacity to ~16× that
  (~96–192 claimants) — comfortably beyond the 12-replica failure and the
  foreseeable replica count, while keeping per-type control-plane state at 16
  fence registers, not millions. It is a power of two (cheap `& (G-1)` if ever
  needed) and small enough that the recovery sweep enumerating `G` shards per
  subscription stays negligible.
- **G is fixed, not adaptive.** An adaptive/auto-scaling `G` would re-introduce a
  coordination problem (who decides, how is it migrated). A fixed small `G` is a
  deployment constant; raising it later is a config change, not a redesign.

`g = hash(entityId) % G` (FNV-1a, 32-bit), the same hash on both sides of the
contract (`webhook.ShardIndex`).

## 4. The chronicle capability: per-`(subId, g)` fences

**The model.** A claim **names a shard** `g`. The single-holder
`(generation, wake_id)` fence is **per `(subId, g)`**, so concurrent claimants on
different entity-shards of the same type do not serialize. A subscription is thus
claimable by up to `G` concurrent holders over disjoint stream subsets; the
holder of shard `g` cannot disturb shard `g'`'s lease (independent registers).

**Keyspace** (`webhook/keys.go`). The config and the per-stream cursors stay
**shared** (one record per subscription); only the runtime fence shards:

| What | Key | Sharded? |
|---|---|---|
| Immutable config + NOSUB existence | `ds:{__ds}:sub:<id>` | shared (also holds shard 0's fence) |
| Per-stream cursors (links) | `ds:{__ds}:sub:<id>:links` | shared (forward-only watermarks) |
| **Per-shard fence** (`generation`, `wake_id`, `phase`, `holder`, `lease_until_ns`) | shard 0 → `ds:{__ds}:sub:<id>` · shard g>0 → `ds:{__ds}:sub:<id>:g:<g>` | **per `(subId, g)`** |
| Lease/retry/due ZSET member | shard 0 → `<id>` · shard g>0 → `<id>:g:<g>` | **per `(subId, g)`** |

Shard 0's fence deliberately lives in the **main** `sub` hash, and its schedule
member is the bare `<id>`. So at **G=1 (only shard 0) the keyspace, the scripts,
and the schedule members are byte-for-byte identical to today** — the granularity
change is purely additive (`g>0` adds new hashes/members; `g=0` is unchanged).
This also means the existing `expire_lease.lua` and the manager's lease worker
operate on a `g>0` shard unchanged, because a shard's fence hash is just
`subKey(<id>:g:<g>)` and its member is that derived id.

**`claim.lua`** gains one key. It now takes `KEYS: 1=sub(config) 2=shardstate
3=lease_zset` and `ARGV: 1=member …`: the **NOSUB** check is `EXISTS(sub config)`
(so a fresh, never-claimed `g>0` shard is *not* NOSUB — its fence starts at idle
and is minted on first claim), while the **fence** read/rotate/arm and the `ZADD`
operate on `shardstate` with `member`. When `sub == shardstate` and `member ==
<id>` (shard 0 / G=1) the behavior is identical to the prior single-key script.

**`ack.lua`** is unchanged in shape: `KEYS[1]` is the **shardstate** hash (the
fence the ack checks and the done-branch idles) and `ARGV[1]` is the schedule
**member** for the lease/retry `ZREM`/`ZADD`. For shard 0 / G=1 these are
`subKey(<id>)` and `<id>` — identical to today.

**Single-holder is preserved *within* each shard** (07 T1): every grant to a new
holder of `(subId, g)` rotates that shard's generation strictly upward; a deposed
holder's later ack carries the old generation and is `FENCED`. `T1` is run per
`(subId, g)` (§7).

**The cursor subset is a partitioning convention, not a fence.** Each shard `g`
processes the streams whose entity hashes to `g`; the ack for shard `g` advances
only those cursors. Cursors are **forward-only watermarks**, so even an
overlapping advance is idempotent and safe (at-least-once, fenced) — the
disjointness is a work-partitioning optimization, and "a holder of `g` cannot ack
`g'`" means it cannot affect `g'`'s **lease/fence** (guaranteed by independent
registers), proven by the fence-isolation test (§7).

**Composition with the Move spine** (the shared `claim.lua`/`ack.lua` conflict
surface):
- **#12 (due `ZREM`).** `ack.lua`'s done branch `ZREM`s the schedule member; #12
  adds `ZREM <due_zset> member` there. Our change makes the member shard-scoped
  (`ARGV[1]`), so #12's due `ZREM` uses the *same* `member` and shards with no
  further change — a per-shard due mark is removed by its own shard's ack.
- **#14 (TOCTOU inline owner check).** #14's inline owner-epoch check is a layer
  *above* the `(gen, wake_id)` fence and is keyed by slot ownership, orthogonal to
  the shard index; it composes by checking ownership of the slot homing `<id>`,
  unaffected by which shard `g` the claim names.

## 5. The client contract (cross-repo — the agents-runtime half)

chronicle exposes the capability; the **client chooses the granularity**. The
contract:

1. **The client picks `G` (= 16) and computes `g = ShardIndex(entityId, G)`** =
   `fnv32a(entityId) % G`. chronicle does **not** infer `g` — a claim/ack names
   its shard explicitly. (`G` is a client/deployment constant; chronicle stores no
   `claim_shards` field, so no config-schema or `create_sub.lua` change is needed
   to use the capability.)
2. **The agents runtime claims `(subscriptionId, g)`** for the entity it is
   processing, instead of the bare `subscriptionId`. Two equivalent realizations:
   - *server-side shard arg* (this build): one subscription, `claim`/`ack` carry
     `g` → `RedisStore.ClaimShard(id, g, …)` / `AckShard(id, g, …)`.
   - *client-side finer subscriptionId*: the runtime registers
     `"<type>-handler:<g>"` as `G` subscriptions (works on today's code with **no
     chronicle change** — this is what the C1/C2/C3 baseline run used). The
     server-side arg is preferred because it keeps one config + one cursor set per
     type (coalescing, listing, and GC stay per-type).
3. **A holder of shard `g` acks only the streams whose entity hashes to `g`.**
4. **`heartbeat 10 s · lease_ttl_ms 30 s · idleTimeout 10 s` are unchanged** — the
   granularity fix does not touch the per-subscription lease timers (the new
   slot-ownership TTLs from #14, 9 s, are a different layer).

See §9 for the Electric-repo tracking issue.

## 6. Build surface (this issue, chronicle half)

- `webhook/shard.go` — the domain types: `ShardCount` (a validated `G ≥ 1`),
  `ShardKey{SubID, Index}` (invalid states unrepresentable; built only via
  `ShardCount`), and the pure `ShardIndex(entityId, G)` hash. `+ shard_test.go`.
- `webhook/keys.go` — `subShardKey(id, g)`, `shardMember(id, g)` (both reduce to
  today's `subKey(id)` / `<id>` at `g==0`).
- `webhook/scripts/claim.lua` — the extra config key + per-shard member.
- `webhook/scripts/ack.lua` — shardstate `KEYS[1]` + per-shard member doc (no
  behavioral change at `g==0`).
- `webhook/redis_store.go` — `ClaimShard(id, g, …)` / `AckShard(id, g, …)`;
  `Claim`/`Ack` delegate at `g==0`, so the `Store` interface is unchanged.
- `jepsen/checker` — `shardedSubClaimer` (the C3 differential against the
  capability) + `TestShardSingleHolderLinz` (T1 per `(subId, g)`) +
  the fence-isolation test (a holder of `g` cannot ack `g'`).

## 7. C3 — the acceptance gate (gate #6)

**To run** (one Redis container, no cluster):

```
docker run -d -p 6380:6379 --name hs11-redis-claude redis:7
REDIS_URL=redis://localhost:6380/14 go run ./jepsen/checker \
  -scenario contention -c3 -G 16 -sharded
```

`-sharded` drives the chronicle per-`(subId, g)` capability (one subscription,
`ClaimShard`/`AckShard`) rather than client-side `G` subscriptions; C3 then
compares G=1 vs G=16 on the *same* server-side mechanism.

**C3 result — server-side per-`(subId,g)`, G=16 — PASS (gate #6 holds):**

| topology | N | busy/op | fenced/op | thru/worker | aggregate | p99 ms |
|---|---|---------|-----------|-------------|-----------|--------|
| **G=16** sharded-sub | 6  | 0.00 | 0.00 | 26.3 | 158 | 32 |
| **G=16** sharded-sub | 12 | 0.13 | 0.00 | 28.0 | 336 | 27 |
| **G=16** sharded-sub | 24 | 0.12 | 0.00 | 27.1 | 650 | 53 |
| **G=1** baseline | 6  | 0.80 | 0.00 | 18.7 | 112 | 109 |
| **G=1** baseline | 12 | 0.94 | 0.00 | 10.3 | 123 | 309 |
| **G=1** baseline | 24 | 0.97 | 0.00 | 4.3  | 104 | 866 |

On the **chronicle per-`(subId,g)` capability** (not client-side sharding), G=16
holds per-worker throughput flat (~27) and scales aggregate 158→336→650 with N,
while G=1 collapses per-worker 18.7→10.3→4.3 with aggregate pinned ~110. The
`CheckGranularityMovesKnee` differential is clean: the knee that collapsed G=1 at
N=12 moved beyond the entire G=16 ramp — **gate #6 holds**. (`fenced/op` is 0 at
every rung, so the single-holder fence per `(subId,g)` never spuriously fired.)

**T1 per `(subId, g)`** — `TestShardSingleHolderLinz` drives contending workers
across `G` shards via `ClaimShard`/`AckShard`, records each op into a porcupine
history partitioned per `(subId, g)`, and checks it against the unchanged
`leaseModel` (07's single-holder model). The fence-isolation test asserts a
shard-`g` token is `FENCED` against shard `g'` and `OK` against `g`.

## 8. What runs locally vs the GKE campaign

- **Local (this issue):** C1/C2/C3 + T1-per-`(subId,g)` against a single Redis
  container — reproduces the **throughput-knee** collapse and proves the
  granularity fix moves it ~`G×`. No k3d, no GKE (the shared-VM constraint).
- **GKE campaign (orchestrator-run, documented here):** the literal-timer
  (heartbeat 10 s, `lease_ttl_ms` 30 s, `idleTimeout` 10 s) 12+-replica run that
  reproduces the literal **fence storm** (lease lapses → takeover → `FENCED`).
  Command/spec: ramp the agents-server replicas 6→12+ against a single hot
  per-type subscription, read `ALREADY_CLAIMED`/`FENCED`/lease-lapse rates + wake
  p99/p50 + per-busy-agent throughput from `chronicle_claim_contention_total`,
  then repeat with the client choosing `G=16` shards and confirm the knee moves
  ~`G×` at CPU-bound (not lock-bound) utilization.

## 9. Cross-repo client tracking issue (to open)

The client half lands in the **Electric** repo (the agents runtime), not
chronicle. File this there (do **not** open it against `adityavkk/chronicle`):

> **Title:** Agents runtime: claim per-shard-of-type (`g = hash(entityId) % 16`)
> to end the per-type claim-contention collapse
>
> **Body:**
> The GKE load test proved the 12-replica collapse is per-type claim contention:
> the runtime registers one subscription per entity type
> (`subscriptionId = "<typeName>-handler"`, `entity-manager.ts:685`), so every
> entity of a type and every replica serialize on chronicle's one
> `(generation, lease)` per subscription — a fence storm at ≤12% CPU on every
> tier.
>
> chronicle now provides the **server half** (chronicle#11): a claim names a
> shard `g` and the single-holder `(generation, wake_id)` fence is per
> `(subscriptionId, g)`, so one type is claimable by up to `G` concurrent holders
> over disjoint entity-shards. Contract (chronicle `08-claim-granularity.md` §5):
> - Pick **G = 16**; compute `g = fnv1a32(entityId) % G` (matches
>   `webhook.ShardIndex`).
> - Claim/ack `(subscriptionId, g)` for the entity being processed — either via
>   the server-side shard argument (preferred: one config + one cursor set per
>   type) or by registering `"<type>-handler:<g>"` as `G` subscriptions (works
>   today, no chronicle change).
> - A holder of shard `g` acks only the streams whose entity hashes to `g`.
> - Leave the lease timers unchanged (heartbeat 10 s / `lease_ttl_ms` 30 s /
>   `idleTimeout` 10 s).
>
> **Acceptance:** the contention suite's C3 (chronicle#11) moves the throughput
> knee out ~`G×`; on the GKE rig, `ALREADY_CLAIMED`/`FENCED` at fixed replica
> count drop ~`G×` and the system holds CPU-bound, not lock-bound.

## 10. Deferred (out of scope for #11, documented)

- **Manager lease-worker / recovery-sweep per-shard enumeration.** The full sweep
  and the lease worker still enumerate subscriptions per `<id>`; for `g>0` shards
  the inline expired-lease takeover in `claim.lua` covers liveness for the
  contention suite (active claimants always re-take an expired shard lease). Wiring
  the sweep to enumerate all `G` shards per subscription (a still-correct full
  backstop) is mechanical follow-up; the **safety** fence is complete and
  T1-proven per `(subId, g)`. (Per the epic invariant: work-sharding is an
  optimization over a still-correct full-sweep baseline.)
- **Config-persisted `G` + `delete_sub.lua` shard cleanup.** `G` is a client
  constant here; persisting `claim_shards` in the config (and having
  `delete_sub.lua` drop the `g>0` shard hashes) is a follow-up if chronicle ever
  needs to enforce/enumerate `G` server-side. The driver cleans shard hashes on
  teardown.
