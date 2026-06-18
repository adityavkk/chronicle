# 05 — Proposed scalability + DR architecture

**The design is Option A** — one binary, slot-homed, work-sharded — with the full build
spec below. Option B (split the tiers) is documented as the deferred fallback, built only
if measurements force it. Both are grounded in chronicle's actual mechanisms and hardened
against the adversarial review ([06](06-adversarial-review.md)); they share a foundation
and differ only in **how far you split the system**. Read [06] first if you want the
critique that shaped these; the corrections it produced are baked in here.

## Fresh implementer handoff

Use this document as the starting point for the horizontal-scaling work. It is the
canonical proposal; [04](04-options-for-chronicle.md) is a superseded first pass, and
[06](06-adversarial-review.md) records the corrections that shaped this version.

**The design is Option A.** Keep one Chronicle binary, slot-home the subscription control
plane, add per-slot schedules and a per-subscription due-set, and shard background work by
leased slot ownership. The complete buildable contract — slot addressing, the two new Lua
scripts, the membership/HRW protocol, the due-set, the loop change-map, config defaults,
and metrics — is in ["Option A — build spec"](#option-a--build-spec-everything-an-implementer-needs)
below; build from that. [Option B](#option-b--the-deferred-fallback-split-the-tiers-only-if-measured)
is the **deferred fallback**: build it *only* if the gate measurements show the one-binary
design can't deliver the cross-instance fast wake, CPU isolation, or independent
control-plane scaling. Do **not** remove the sweep; demote it to a reconciler.

Minimum supporting material:

| Need | Read |
|---|---|
| Why this work exists | GitHub issue [#2](https://github.com/adityavkk/chronicle/issues/2) and [loadtest/RESULTS-gke.md](../../../../loadtest/RESULTS-gke.md) |
| How to run the GKE rig | [loadtest/README.md](../../../../loadtest/README.md) and [loadtest/AGENTS.md](../../../../loadtest/AGENTS.md) |
| Existing subscription implementation | [webhook/manager.go](../../../../webhook/manager.go), [webhook/keys.go](../../../../webhook/keys.go), [webhook/redis_store.go](../../../../webhook/redis_store.go), and [webhook/scripts/](../../../../webhook/scripts) |
| As-built durability behavior | [docs/research/11-subscription-hardening-implemented.md](../../../research/11-subscription-hardening-implemented.md) |
| Prior-art basis | [01](01-electric-agents.md), [02](02-cloudflare-durable-objects.md), and [03](03-prior-art-redis-and-beyond.md) |
| What was corrected | [06-adversarial-review.md](06-adversarial-review.md) |
| Acceptance tests for the redesign | [07-jepsen-style-verification.md](07-jepsen-style-verification.md) |

Implementation order (this is the same sequence as Option A's **Migration** slices below,
with the verification baseline and DR called out):

0. **Characterize the [per-type claim-contention axis](#a-third-axis-per-type-claim-contention-from-the-load-test) first** (experiment 6 below). It is the failure that collapsed the system at 12 replicas, and neither slot-homing nor the coordinator split addresses it. The slices below harden the sweep/keyspace axes that were *already clean* at 6 replicas — do them, but not before understanding the claim-granularity fix.
1. Run the gating measurements in ["Measure before building"](#measure-before-building-the-gating-experiments).
2. Build the no-rebuild safety baseline from [07](07-jepsen-style-verification.md):
   start with T1 (single-holder lease), T2 (cursor monotonicity), and L3 (lease-tail-drop
   recovery) — 07's recommended first three; T4 and L1 also run today and should follow.
3. Add leased slot ownership (the membership/HRW protocol and `claim_shard`/`check_owner`
   from the build spec) while the existing full sweep still covers every subscription.
4. Add the per-subscription due-set in the current single slot and measure its append/ack
   write amplification (gate #3).
5. Slot-home subscription state into `{__ds:h}` and shard lease/retry/due schedules with
   the subscription. Migrate lazily or by shadow writes; never split one subscription's
   Lua key set across Redis slots.
6. Add the cross-region active-passive standby (Option A's **Regional DR** section below).
7. Add the Option B coordinator split **only** if the measured bottleneck is outside
   Option A's reach.

## Three corrections that frame everything

The review forced three precision fixes that every design below respects:

1. **The fence is the safety boundary; it is *not* liveness.** The
   `generation`/`wake_id` fence makes a *duplicate* wake harmless (coalesce at
   `arm_wake.lua` BUSY, or `FENCED` at the ack). It does **nothing** for a *coverage
   gap* — a slice of subscriptions briefly owned by no replica during a rebalance is
   simply not swept until ownership re-converges. So any work-sharding scheme must
   supply liveness *separately*, as an optimization over a still-correct full-sweep
   baseline — never as a correctness dependency.
2. **A cursor reconcile is irreducible — but a *perpetual* one is not.** Only re-deriving
   owed work from *durable cursors* (links vs stream tail) recovers a wake whose schedule
   entry a failover dropped while keeping the `sub` hash — the lease worker can't, because
   its `ZADD` is gone. But that argues for a cursor reconcile *triggered* by the recovery
   events (boot, epoch bump, new-owner CAS, reconnect, append error) plus a coarse periodic
   floor for the one undetectable case — an owed-mark lost on a slot that is unowned and
   quiet, where the cross-slot append/arm gap means no atomic owed-mark was ever written. It
   does **not** justify a perpetual 2s scan. So "remove the `O(K)` scan" is true for the
   *hot path*; the reconcile is demoted to event-triggered plus a rare floor, not deleted.
3. **"Tunable consistency" on Redis is a durability + freshness knob, not a CAP knob.**
   Redis is AP. `WAIT`/`WAITAOF` buy replica-acked / fsync durability — *not*
   linearizability (Redis docs say so explicitly). The genuinely strong guarantee is
   the monotonic generation fence enforced by compare-and-set + reject-if-stale at the
   resource — the Kleppmann fencing-token result, which chronicle already implements.
   Build the knob on durability + read-freshness, and never put a correctness-critical
   single-holder lease on a CRDB LWW register (it silently drops a concurrent writer).

## A third axis: per-type claim contention (from the load test)

The three corrections above came from the *review*. A subsequent GKE load test — the
Electric agents runtime driven against chronicle@main as its Durable-Streams backend, ramping
the agents-server replicas that are the system under test — forced a fourth, and it **reorders
the priorities below**. At **6 replicas the wake path was clean** (~566–630 wakes/s, `FENCED`→0
at steady state); at **12 it collapsed** — the load generators' warmup fence-stormed (489–735
`FENCED` *per pod*, only ~40% of entities ever woke) while **every tier sat ≤12% CPU**
(chronicle 4%, Redis 4%, Postgres 12%). Giving chronicle 2.5× the CPU made it *worse*. So the
binding constraint at that scale is **neither the `O(N·K)` sweep nor `{__ds}` keyspace
capacity** — it is lock contention on a single subscription's lease. (Full results live in the
Electric rig: `electric/loadtest/docs/RESULTS-scale12.md` and `RESULTS-chronicle-main.md`.)

**The contention unit is one subscription per entity *type*.** The Electric runtime registers
`subscriptionId = "<typeName>-handler"` — one subscription shared by *all* entities of a type
(agents-server `entity-manager.ts:685`). chronicle holds exactly **one** single-holder lease +
generation per subscription (`ds:{__ds}:sub:<id>`). So every entity of a type *and* every
replica hosting one contends for that one lease. The runaway is the claim path itself: a
competitor on a *live* lease gets `409 ALREADY_CLAIMED` (`claim.lua` BUSY, no generation bump);
when the holder's heartbeat lands late — because the single `{__ds}` slot's ~12 control-plane
ops/wake are queued — the 30 s `lease_ttl_ms` lapses, a competitor *takes over* (`HINCRBY
generation`), and the deposed holder's in-flight done/heartbeat is `FENCED`; the loser retries,
adding ops, lengthening the queue. More replicas ⇒ more contenders per type-lease ⇒ a
thundering herd on one lock. (Source-traced both sides; the exact per-wake interleaving at 12
replicas is inferred from the proven halves plus the collapse counts.)

**Neither Option A nor Option B fixes this as written.** `h = fnv32a(subId) % S` slot-homes
*different* subscription ids across slots; a single hot `<type>-handler` is still one subId ⇒
one slot ⇒ one lease, for any `S`. And Option A's claim/ack hot path is **load-balanced across
replicas** (["Any Chronicle replica can handle a … claim/ack request"](#option-a--slot-homed-evolve-in-place)),
so all of a type's claimants still serialize on that one lease. Option B's coordinator-owns-a-sub
model narrows *who* drives a sub but leaves it one lease per type. Slot-homing and coordinator
ownership shard **ownership and keyspace**; they do not refine **claim granularity**, which is
what collapsed.

**The fix is a third axis: make claim granularity match dispatch granularity.** Let one logical
type map to *many* leases — per-entity, or per-shard-of-type subscriptions
(`"<typeName>-handler:<g>"`, `g = hash(entityId) % G`) — so concurrent claimants on different
entities do not serialize through one lease. This is partly a **client** decision (the agents
runtime choosing a finer `subscriptionId`) and partly a **chronicle capability** (letting one
subscription's linked streams be claimed by *multiple concurrent holders* over disjoint stream
subsets). It is **cross-repo** and is the load-bearing design neither this doc nor 04's O1–O5
yet covers. Because the collapse was hot-path, **this axis sequences before** the sweep
work-sharding and the state shard below: those harden axes that were *already clean* at 6
replicas (whose ceiling was wake **round-trip latency** — which Option B's cross-instance fast
wake targets directly, a reason to revisit its deferred priority).

**Golden signals to instrument and gate on** (none exist today): `ALREADY_CLAIMED`/BUSY rate
(the *earliest* indicator — contenders bouncing off live leases), `FENCED` rate (the tipping
point — leases lapsing into takeover), lease-lapse rate, wake p99/p50, and per-busy-agent
throughput = `1 / round-trip-latency`. On timers: the runtime heartbeats every **10 s** against
a **30 s** `lease_ttl_ms` with a **10 s** `idleTimeout` hold; `idleTimeout` only suppresses
*one* claimant's self-release churn — it does nothing about cross-replica contention. The new
slot-ownership TTLs (`slotLeaseTTL`/`memberLeaseTTL` = 9 s) added below are a *different* lease
layer; **none of them touch the per-subscription `lease_ttl_ms` whose lapse drives the storm.**

## The shared foundation (build this first)

Independent of which option you pick, the same substrate is step one — and it is the
fix for the review's "O2 breaks the atomic Lua" critique:

- **Slot-home whole subscriptions, never split one across slots.** Give every key a
  subscription touches — `sub` hash, `links` hash, its `lease`/`retry` ZSETs, its
  due-set, and its fan-out membership — the *same* tag `{__ds:h}`, `h = hash(subId) %
  S`. Then `ack.lua` (4 keys), `delete_sub.lua` (5 keys), `arm_wake.lua`,
  `claim.lua` stay **byte-for-byte single-slot and atomic**. Sharding becomes "compute
  the tag," not "rewrite the atomicity contract." This is the load-bearing move the
  original O2 missed.
- **Per-slot schedules, not one global ZSET.** The lease/retry/due ZSETs shard *with*
  the subs (`ds:{__ds:h}:sched:lease`, etc.), so `claim_due.lua` (1 key, the schedule
  ZSET) runs unchanged, once per slot. This is the real capacity lift — the original O2
  left a single global schedule that *was* the surviving ceiling.
- **Per-subscription due-set replaces the `O(K)` scan.** `arm_wake.lua` (reached via
  `OnStreamAppend`→`maybeWake`→`issueWake`→`ArmWake`) `ZADD`s the sub into
  `ds:{__ds:h}:due` inside the same atomic script (score = `now_ns`, a pure "needs a
  wake" outbox — see the build spec); `ack` `ZREM`s it (one line in the already-atomic
  script). A per-slot due-worker fires owed subs — `O(owed)`, not `O(K)`. **Honest
  cost:** this *relocates* the existing `arm→emit→record` dual-write and *adds* a
  `ZADD`/`ZREM` to the hot path; measure it (below). The full sweep stays the ~2s
  backstop covering every slot; only the `O(streams)` fan-out-index reconcile is the
  ~30s loop.
- **The fence is untouched.** Generation/wake_id stay in the `sub` hash in `{__ds:h}`;
  every fenced script is still single-slot.

Where the options diverge: **Option A** keeps this in the one binary and shards the
*work* across replicas by leased slot ownership. **Option B** turns each subscription
into an actor with a Redis grain directory, splits the control plane from serving, and
adds a cross-instance fast wake.

---

## Option A — Slot-homed, evolve in place

**Bet:** the single-binary design is fine *for the sweep and keyspace axes* — those are the
single slot and the redundant scan, fixed by sharding state into `{__ds:h}` slots and sharding
sweep *work* across replicas, with a leased-membership protocol supplying the liveness the fence
lacks. **Caveat (load test):** this bet does **not** cover the
[per-type claim-contention axis](#a-third-axis-per-type-claim-contention-from-the-load-test) —
slot-homing gives it zero relief — so Option A is necessary but not sufficient by itself.

**Keyspace** (additive to today):

```
ds:{__ds:h}:subs               SET   ids homed in slot h
ds:{__ds:h}:sched:lease        ZSET  id -> lease_expiry_ns   (claim_due.lua, unchanged)
ds:{__ds:h}:sched:retry        ZSET  id -> next_attempt_ns
ds:{__ds:h}:due                ZSET  id -> earliest_owed_ns   (the outbox; new)
ds:{__ds:h}:sub:<id>           HASH  config + runtime (gen, wake_id, phase, …)
ds:{__ds:h}:sub:<id>:links     HASH  path -> "<linktype>:<acked_offset>"
ds:{__ds:h}:stream:<path>      SET   subscribers of <path> homed in slot h (fan-out shard)
ds:{ownership}:members         ZSET  replicaId -> lease_expiry_ns
ds:{ownership}:slot:<h>        HASH  owner_id, owner_epoch, lease_expiry_ns
```

**Horizontal scale.** State capacity scales with `S` across all cluster nodes (the
genuine ceiling-lift, because the schedule shards *with* the subs). Sweep/due work is
sharded by leased slot ownership: each replica runs the workers + reconciler only for
its owned slots, so total work is `O(total owed)` regardless of N — the `O(N·K)`
redundancy is gone. **The honest cost:** `OnStreamAppend` can no longer be one
`SMEMBERS`; it becomes `S` *parallel pipelined* `SMEMBERS` (one per slot) — ~max-node-RTT
wall-clock (the slots span cluster nodes, so go-redis groups the pipeline per node), not
`S` serial, but a real, measured regression. Mitigation for sparse wide streams: a
per-stream "occupied-slots" bitmap so you probe only slots that actually have a
subscriber (the bitmap spec is in the build spec). This is the number gate #2 measures.

**Membership and shard ownership protocol.** The public Chronicle endpoint remains one
ordinary load-balanced service. The load balancer is not shard-aware, and Option A does
not require Chronicle-to-Chronicle request forwarding. Any Chronicle replica can handle
a create/get/delete/claim/ack/release request by computing `h = hash(subId) % S` and
running the single-slot Lua script against `ds:{__ds:h}:...`. Ownership applies to
autonomous background work and external side effects, not to every Redis mutation.

The app-cluster membership protocol is Redis-backed:

1. Each Chronicle process starts with a stable `replica_id` for its current pod
   lifetime. A StatefulSet ordinal works; a Deployment can use the pod UID plus a
   process-start generation. The value distinguishes process incarnations that reuse a
   pod name; `owner_epoch` handles a live process that resumes after a long pause.
2. Every process heartbeats with `ZADD ds:{ownership}:members <now+ttl> <replica_id>`.
   Every process may also remove expired members. Kubernetes decides which pods receive
   HTTP traffic; this Redis ZSET decides which pods are eligible to own subscription
   slots.
3. From the live member set, every process computes the same target owner for each
   fixed virtual slot `h`. Use rendezvous hashing or another deterministic assignment
   that moves only the affected slots on join/leave. Do not tie `S` to the current pod
   count; pick many more virtual slots than expected replicas so adding replicas
   reassigns existing slots instead of requiring a key migration.
4. A would-be owner claims `ds:{ownership}:slot:<h>` with `claim_shard.lua`. The script
   is a CAS: it succeeds only when the current owner is expired, missing, or already the
   caller, and it increments `owner_epoch` on every ownership transfer. The slot record
   stores `owner_id`, `owner_epoch`, and `lease_expiry_ns`.
5. Workers process only slots they currently own. The `(owner_id, owner_epoch)` check is
   an owner-epoch fence layered *above* the per-subscription `(gen, wake_id)` fence — it
   suppresses a deposed owner's wasted work, but the `(gen, wake_id)` fence remains the
   safety boundary that makes any leaked duplicate harmless. Schedule and due-set writes
   **inline** the check atomically (a separate `check_owner.lua` round-trip would not
   fence a GC pause between check and write); the standalone `check_owner.lua` is only for
   the external webhook POST. A deposed owner that resumes after a GC pause or network
   partition gets `FENCED` and stops. (Contracts and the TOCTOU resolution are in the
   build spec.)
6. When a new replica joins, its heartbeat changes the deterministic assignment. Old
   owners stop renewing slots they no longer target and may release them early; expiry is
   the authoritative handoff. New owners CAS the slot records and start the workers for
   those slots. When a replica dies, its member lease and slot leases expire, and the
   remaining replicas claim its slots.

This protocol is separate from Redis Cluster membership. Redis Cluster decides which
Redis node stores the `ds:{__ds:h}:...` keys and may move hash slots during a managed
reshard. Chronicle decides which Chronicle process performs the side effects for
subscription slot `h`. The Redis Cluster client follows `MOVED`/`ASK` redirects; the
Chronicle ownership protocol uses the `ds:{ownership}:...` keys above.

**Liveness (the review's #1 fix).** Slot ownership is an *optimization over a correct
baseline*: a still-correct cursor reconcile covers every sub regardless of ownership. A
rebalance coverage gap closes when the new owner claims the slot and reconciles it (an
*event*, not a tick); the residual periodic floor covers only the one eventless case, in the
seconds-to-minutes band — see ["Recovery"](#recovery-triggered-not-perpetual-refines-07-honest-gap-4).
A crash-lost wake is recovered at its trigger, or at worst within the floor interval, never
indefinitely. Split-brain on a slot (two owners) is *safe* (double-wake coalesces).

**Regional DR.** Active-passive: one home region per slot, async cross-region replica.
Not CRDB (an LWW string fence silently drops a writer). `WAIT 1`/`WAITAOF 1 1` on the
fence-minting writes (`arm_wake`/`claim` generation `HINCRBY`) bounds RPO to ~the AOF
fsync interval; the client must check the returned count. On promotion (epoch bump),
each owner runs a **failover-aware eager reconcile** that re-derives schedule entries
from the durable `sub` hash — recovering the stranded-webhook-wake case. RPO = async
lag + fsync (~1s) + link latency; RTO = promotion time.

**Migration** (reversible, slice by slice): (0) extend the loadgen rig (chaos/pod-kill,
fault-injection, the new metrics, a managed-Redis CPU reader) and run the unmeasured load
tests — the SUT is already instrumented and the K-sweep baseline is in RESULTS-gke.md →
(1) leased slot ownership, work-sharded sweep, full-sweep fallback intact → (2) the due-set
outbox in the single slot → (3) the `S`-slot state shard behind a shadow write + lazy
per-sub migration → (4) cross-region standby → (5) split the binary *only* if metrics force
it.

---

## Option A — build spec (everything an implementer needs)

Everything above is the rationale. This is the buildable contract — the detail an
implementer hits on step 1, grounded in the current code (`webhook/scripts/*.lua`,
`webhook/manager.go`, `webhook/keys.go`, `webhook/redis_store.go`, `webhook/scripts.go`).
Each subsection closes a specific gap; nothing here is left to "use rendezvous hashing or
something."

### Slot addressing — fix `S`, define `h`, re-tag the keys

- **`S = 256`, a compile-time constant** (`const subSlots = 256` in `keys.go`). It is
  immutable for the life of a keyspace: changing it re-tags every key, so a change is a
  dual-write migration (read old tag, write new tag, flip), never a config edit. 256
  comfortably exceeds any expected replica count and is the upper bound gate #2 measures.
- **`h = fnv32a(subId) % S`** using Go's `hash/fnv` (FNV-1a, 32-bit) — **not** Redis
  CRC16. The Redis client already CRC16-hashes the `{…}` tag to a cluster slot; `h` must
  be an independent, language-stable application choice, or you re-introduce the
  `CROSSSLOT` the slot-homing is meant to kill. `slotTag(id) = "{__ds:" +
  itoa(fnv32a(id)%S) + "}"`.
- **Re-tag `keys.go`.** Today every key uses the one fixed `dsTag = "{__ds}"`
  (`keys.go:10`). The per-sub keys (`subKey`, `linksKey`, `streamSubsKey`) already take an
  id/path — derive `h` and build the tag from it. The schedule/index keys that are
  *constants* today (`subsKey`, `leaseZKey`, `retryZKey`, plus the new `dueKey`) become
  `func(h int) string`. A store method computes `h` **once** from the id and builds the
  sub's whole key set from that single `h`, so `ack.lua` (4 keys), `delete_sub.lua` (5
  keys), `arm_wake.lua`, and `claim.lua` stay byte-for-byte single-slot.
  `streamSubsKey(path)` moves under the slot tag too — `ds:{__ds:h}:stream:<path>` —
  matching the keyspace block above.
- **Ownership keys use their own literal tag** `{ownership}` (cross-slot membership
  metadata, deliberately not slot-homed).
- **Add a guard test**: `subKey(id)`, `linksKey(id)`, and the sched/due keys for one id
  all resolve to one Redis cluster slot (the precondition T5 checks).

### The two new Lua scripts

Both follow house style: a `common.lua` prelude (concatenated by `loadScript` in
`scripts.go`), a `{status, …}` string-array reply decoded by `evalStrings`, single-slot
under the `{ownership}` tag, and a package var in `scripts.go`
(`claimShardScript = loadScript("claim_shard.lua")`, `checkOwnerScript = …`).

`claim_shard.lua` — the slot-ownership CAS (07 T3's acceptance gate):

```lua
-- claim_shard.lua — CAS takeover of a slot-ownership lease. Grants only when the
-- current owner is expired, missing, or the caller itself; bumps owner_epoch on
-- every *transfer* (never on a same-owner renew) so a deposed-but-resumed owner
-- carries a stale epoch and is fenced. The {ownership}-tagged analogue of
-- claim.lua's expired-lease takeover.
-- KEYS: 1=slot (ds:{ownership}:slot:<h>)
-- ARGV: 1=replica_id 2=now_ns 3=lease_ttl_ms
-- Reply: {status, owner_id, owner_epoch, lease_expiry_ns} ; CLAIMED | RENEWED | BUSY
local slot, me = KEYS[1], ARGV[1]
local now  = tonumber(ARGV[2])
local owner = redis.call('HGET', slot, 'owner_id')
local exp   = tonumber(redis.call('HGET', slot, 'lease_expiry_ns')) or 0
if owner ~= false and owner ~= me and exp > now then
  return { 'BUSY', owner, redis.call('HGET', slot, 'owner_epoch'), tostring(exp) }
end
local epoch
if owner == me then
  epoch = redis.call('HGET', slot, 'owner_epoch')                  -- renew: keep epoch
else
  epoch = tostring(redis.call('HINCRBY', slot, 'owner_epoch', 1))  -- transfer: bump (1 on first claim)
end
local until_ns = now + tonumber(ARGV[3]) * 1000000
redis.call('HSET', slot, 'owner_id', me, 'lease_expiry_ns', tostring(until_ns))
return { (owner == me) and 'RENEWED' or 'CLAIMED', me, epoch, tostring(until_ns) }
```

`check_owner.lua` — the owner-epoch fence for the one write that *cannot* be inlined:

```lua
-- check_owner.lua — verify the caller still owns slot h at the expected epoch,
-- before an EXTERNAL side effect (the webhook POST) where an atomic inline check
-- is impossible. Schedule/due writes do NOT call this — they inline the same
-- check (see below). This is an owner-epoch fence ABOVE the (gen,wake_id) fence:
-- it suppresses a deposed owner's wasted work, but the (gen,wake_id) fence stays
-- the safety boundary that makes any leaked duplicate harmless.
-- KEYS: 1=slot (ds:{ownership}:slot:<h>)
-- ARGV: 1=replica_id 2=expected_epoch
-- Reply: {status} ; OWNER | FENCED | UNOWNED
local owner = redis.call('HGET', KEYS[1], 'owner_id')
if owner == false then return { 'UNOWNED' } end
if owner ~= ARGV[1] or redis.call('HGET', KEYS[1], 'owner_epoch') ~= ARGV[2] then
  return { 'FENCED' }
end
return { 'OWNER' }
```

**Resolve the TOCTOU explicitly.** A separate `check_owner` round-trip *before* a Go-side
write does **not** fence a GC pause between the two. So every schedule-/due-mutating
script (`arm_wake`, `ack`, `expire_lease`, `release`, `schedule_retry`, the due
`ZADD`/`ZREM`) takes the slot key as an **extra KEY** and inlines the owner+epoch check
at the top — the check and the write are then one atomic script. Standalone
`check_owner.lua` is reserved for the external webhook POST, where atomicity across the
network is impossible anyway and the `(gen,wake_id)` fence on the returned ack is the
real guard.

### Membership and ownership — the concrete protocol

- **`replica_id`** = `POD_NAME` (Kubernetes Downward API) + a per-process-start nonce
  (`crypto/rand`, 16 bytes hex): `replicaID = podName + "-" + startNonce`. Fall back to a
  generated UUID when `POD_NAME` is unset (local/dev). It only needs to be unique per live
  process — `owner_epoch` (bumped on transfer), **not** `replica_id`, is what fences a
  paused-then-resumed same incarnation. Add `ReplicaID string` to `ManagerOptions`, default
  to the generated form in `NewManager`, thread it from `subscriptions.go`.
- **Heartbeat** (every `heartbeatInterval`): `ZADD ds:{ownership}:members <now+memberLeaseTTL>
  <replica_id>`, then `ZREMRANGEBYSCORE ds:{ownership}:members -inf (now` to evict expired
  members. Both are idempotent under single-threaded Redis; run them from every replica. No
  slot-GC loop is needed — an expired `slot:<h>` record is simply claimable, and a never-
  reclaimed slot is harmless because the full sweep still covers it.
- **HRW assignment.** Read the live set once per reconcile tick:
  `ZRANGEBYSCORE ds:{ownership}:members (now +inf` (unexpired members only). For each slot
  `h ∈ [0,S)`, `score(r,h) = fnv64a(r + ":" + itoa(h))`; the owner is the `argmax` score,
  tie-broken by the lexicographically greatest `replica_id`. Adding/removing one replica
  reassigns only ~`1/N` of slots.
- **The CAS is the authority, not the HRW math.** A replica runs work for slot `h` only if
  it *both* targets `h` (HRW) *and* holds `h`'s lease (`claim_shard` returned
  `CLAIMED`/`RENEWED`). A brief disagreement during a stale-member-read window is safe:
  two would-be owners produce a double-wake that coalesces, and a zero-owner gap is covered
  by the full sweep until `claim_shard` resolves it.
- **Reconcile loop** (new, every `slotReconcileInterval`): for each `h` where
  `owner(h) == me`, run `claim_shard`; for each `h` this replica holds but no longer
  targets, stop its workers and release early (or let the lease expire — expiry is the
  authoritative handoff).

### The due-set outbox

- `ds:{__ds:h}:due` ZSET, **score = `now_ns`** at arm/append time — a pure "needs a wake"
  outbox, not a deadline queue (the lease ZSET already carries in-flight visibility). A
  sub re-armed after a `FENCED` re-`ZADD`s at the new `now_ns`.
- **Script change-map** (each gains the due key; all stay single-slot under `{__ds:h}`):

  | Script | Change |
  |---|---|
  | `arm_wake.lua` | add `KEYS[3]=due_zset`; in the `ARMED` branch `ZADD due now_ns id` |
  | `ack.lua` | add the `due_zset` KEY; in the `done='1'` branch `ZREM due id` (alongside the existing lease/retry `ZREM`) |
  | `expire_lease.lua` | in the `EXPIRED` branch, `ZADD due now_ns id` when pending work remains (re-owe) |
  | `release.lua` | add the `due_zset` KEY; in the idle-reset branch `ZREM due id` (GAP3: mirrors `ack`'s done branch) |
  | `delete_sub.lua` | include the due key in tombstone cleanup so a deleted in-flight sub cannot leave a permanent due member |

  Update the matching `redis_store.go` call sites (`ArmWake`, `Ack`, `ExpireLease`,
  `Release`, and delete cleanup) to compute `h` and pass the new per-slot key.
- **New `dueWorker()` loop**, modeled on `leaseWorker`: for each *owned* slot, run
  `claim_due.lua` against `ds:{__ds:h}:due` and `maybeWake` each returned id. `claim_due.lua`
  (1 key, the ZSET) is **unchanged** — it just runs once per owned slot.
  The full cursor-reading recovery sweep remains enabled as the correctness backstop; the
  due worker removes the low-latency path's dependence on a full `O(K)` re-evaluation.

### Background-loop change map (`manager.go`)

| Loop / func | Change |
|---|---|
| `leaseWorker` | for `h` in `ownedSlots()`: `DueLeases(h, …)` → `ExpireLease`; the `ExpireLease` script inlines the owner+epoch check for slot `h` |
| `retryWorker` | for `h` in `ownedSlots()`: `DueRetries(h, …)`; `deliverWebhook` only after `check_owner` returns `OWNER` |
| `dueWorker` (new) | for `h` in `ownedSlots()`: `claim_due(dueKey(h))` → `maybeWake` |
| `recoverySweeper` / `sweepOnce` | iterate **all** `h ∈ [0,S)` (not just owned) reading `subsKey(h)`; this is the unowned-slot backstop. **No owner guard** — it issues only fence-safe wakes (a duplicate coalesces) |
| `OnStreamAppend` | `S` parallel `SMEMBERS` over `streamSubsKey(h, path)` for occupied `h` (below) |
| `issueWake` / `maybeWake` | unchanged except keys now derive from `slotTag(id)` |

`ownedSlots()` is recomputed each reconcile tick from the HRW result intersected with the
held slot-leases. `sweepOnce` deliberately covers unowned slots — that is the liveness
backstop, and it resolves [07](07-jepsen-style-verification.md)'s honest-gap #4.

### Recovery: triggered, not perpetual (refines 07 honest-gap #4)

Recovery is not one job on one timer. It is a bundle of cases that split by whether anything
*observable* signals them:

- **Event-triggered — every detectable case, no timer.** Run a cursor reconcile at the
  moment a recovery-relevant event fires: process boot, a failover / `owner_epoch` bump, a
  new-owner `claim_shard` CAS, a Redis reconnect, or an `OnStreamAppend` / delivery error.
  The [failover-aware eager reconcile](#failover-aware-eager-reconcile-makes-07-l3-pass) *is*
  one of these — promote it to the **primary** recovery path. A rebalance coverage gap closes
  when the new owner CASes the slot and reconciles it, not on a clock tick.
- **A coarse periodic floor — one undetectable case only.** Exactly one case has no trigger:
  an owed-mark dropped on a slot that is unowned (mid-rebalance) *and* quiet (no later
  append), where neither a crash, an ownership change, nor a future append exists to react
  to, because the cross-slot append/arm gap means no durable owed-mark was written atomically
  with the append. Only a periodic cursor re-derivation finds it. The conjunction is rare and
  not latency-sensitive, so the floor runs in the **seconds-to-minutes** band — align it with
  the existing 30s index reconcile — *not* at 2s.

`sweepInterval` was 2s because the sweep was the catch-all for every case, including the
latency-sensitive ones. Once those are event-triggered, **2s is a tunable latency floor for
one narrow case, not a correctness requirement** — nothing in the cursor-durability argument
demands it. 07's L2 asserts the detectable churn case recovers at the takeover trigger
(`deliver − append ≤ membership-lease TTL + RTT`); the floor bounds only the eventless case.

This matches the field: event-driven hot paths with periodic scans reserved for rare safety
nets (Orleans' minutes-scale reminder refresh, Cloudflare DO per-object alarms, Kafka offset
resume, Electric's recovery-only reconcile). Chronicle still needs *a* cursor backstop those
log-native systems avoid — its Redis substrate (AP, cross-slot, no per-entity durable alarm)
cannot co-locate a durable owed-mark atomically with the append — but that justifies a coarse
floor, not a 2s primary guarantee.

### Failover-aware eager reconcile (makes 07 L3 pass)

Today `sweepOnce` re-wakes only *idle* subs with pending work; a sub stuck in
`phase ∈ {live,waking}` whose lease-ZSET tail was dropped by a failover (the exact L3 fault)
is **not** recovered — the lease worker never sees it as due. Add a `reconcileLeases` pass
(run in `sweepOnce` and eagerly on every epoch bump / new-owner CAS): for each sub in a
slot, if `phase ∈ {live,waking}` and `lease_until_ns > 0` but the id is **absent** from the
lease ZSET, re-`ZADD` it at `lease_until_ns` — re-deriving the schedule entry from the
durable `sub` hash. Re-derive the due entry from pending-work state the same way. This is
the "failover-aware eager reconcile" the Regional-DR section names; without it, dropping the
lease tail strands the sub and L3 fails against current code.

### `OnStreamAppend` fan-out + occupied-slots bitmap

- `StreamSubscribers(path)` issues `S` `SMEMBERS` over `streamSubsKey(h, path)` for
  `h ∈ occupiedSlots(path)`, pipelined. go-redis groups pipelined commands by cluster node,
  so the wall-clock is **~max-node-RTT** (the slots span nodes) — *not* the "~1 RTT" a
  single-node pipeline would give. This is the regression gate #2 measures.
- **Occupied-slots bitmap**: `ds:{__ds-occ}:streamslots:<path>` (its own tag — cross-slot
  metadata), a 256-bit string. `SETBIT h 1` on `indexStream`; repaired by the reconcile
  loop, **never cleared on deindex** (a stale set bit only costs one empty `SMEMBERS`, so
  it is race-safe). `OnStreamAppend` probes only the set bits, collapsing the sparse-wide-
  stream cost from `S` to occupied-slots-per-stream.

### New config defaults (`ManagerOptions`)

| Tunable | Default | Role |
|---|---|---|
| `memberLeaseTTL` | 9s | member ZSET entry TTL; a missed heartbeat past this drops the replica |
| `heartbeatInterval` | 3s | how often a replica re-`ZADD`s its membership |
| `slotLeaseTTL` | 9s | `claim_shard` lease TTL on a `slot:<h>` record |
| `slotReconcileInterval` | 3s | how often HRW is recomputed and owned slots are (re)claimed |

Invariants: `heartbeatInterval < memberLeaseTTL/2` (renew with headroom) and
`slotReconcileInterval ≤ heartbeatInterval`. These are unrelated to the per-subscription
webhook `lease_ttl_ms` (already in `Config`). Add the four fields to `ManagerOptions`,
default them in `NewManager` like the existing knobs, and thread them via
`subscriptions.go`.

### New metrics (`Metrics` interface)

The gating experiments and L-series tests all lean on measurement, but the current
`Metrics` interface (`SweepTick`, `WakeDelivery`, `WakeEvent`, `WorkerTick`) has no signal
for the new mechanisms. Add (with `NopMetrics` no-ops):

| Method | Wired at | Feeds |
|---|---|---|
| `FanOut(dur, slotsProbed, subs int)` | `OnStreamAppend` | gate #2 (fan-out p99) |
| `DueSetMutation(op string)` / `DueWorkerTick(dur, fired int)` | arm/ack + `dueWorker` | gate #3 (write amplification) |
| `SlotOwnership(event string, slot int)` | `claim_shard` / reconcile | gate #4 (churn, double-grant) |
| `CoverageGap(dur)` | `sweepOnce`, when it wakes a sub whose slot was unowned at append | gate #4 (coverage-gap latency) |
| `OwnerFenced(scope string)` | `check_owner` / inlined checks | gate #4/#5 (fence firing) |
| `ClaimContention(status, subId string)` | `claim.lua` / `ack.lua` call sites (per subscription) | gate #6 — the per-type contention SLIs: `ALREADY_CLAIMED`/BUSY rate, `FENCED` rate, lease-lapse rate |

---

## Option B — the deferred fallback (split the tiers, only if measured)

**Not chosen.** Option A is the design. This option is documented as the escape hatch and
the criteria that would trigger it — build it *only* if the gate measurements prove Option
A cannot deliver the cross-instance fast wake, CPU isolation, or independent scaling below.
It reuses Option A's substrate (slot-homing, the due-set, `claim_shard`/`check_owner`), so
nothing here is wasted if you do cross the fork.

**Bet:** you also need the things Option A can't give — a **cross-instance fast wake**
(today `OnStreamAppend` is in-process, so only the appending replica fires it), CPU
isolation between serving and the control plane, and independent scaling. Make each
subscription an *actor* owned by exactly one coordinator, recorded in a Redis grain
directory (the Orleans `RedisGrainDirectory` pattern), and split the control plane out.

**Components.** A **stateless serving tier** (append/read/long-poll/SSE; unchanged) and
a **coordinator tier** (the `webhook.Manager` loops, extracted). Both share the managed
Redis — no second datastore.

**Ownership.** Partition subs into `P` (e.g. 4096) virtual shards by **rendezvous (HRW)
hashing** over the live coordinator set, so adding/removing one instance reassigns only
~`1/N` of shards (no rebalancing storm). A Redis directory keyed `dir[shardId]` holds the
same `{owner_id, owner_epoch, lease_expiry_ns}` HASH record as Option A's
`ds:{ownership}:slot:<h>` (just at shard rather than slot granularity), claimed by the same
`claim_shard.lua` — a CAS in exactly the shape of `claim.lua`'s expired-lease takeover. An
**owner-epoch fence** layers above the wake fence: a deposed-but-resumed owner (GC pause /
partition, the Kleppmann case) finds `dir[shardId].owner_id != me` and self-evicts;
`check_owner.lua` rejects its stale-epoch schedule writes.

**Fast wake (the gap nothing else closes).** After a durable append, the serving
replica `PUBLISH`es to a `ds:wake:{__ds}:<shardId>` control channel (parallel to the
`ds:notify:{path}` it already publishes for long-poll). The owning coordinator is
subscribed and fires `maybeWake` immediately. Fire-and-forget, so the per-entity due
timer is the backstop and the reconciler is the last resort — and the fence makes the
timer+pub/sub duplicate harmless.

**Liveness.** The per-entity due timers *survive in the shard's ZSET*, so during a
crash→takeover window recovery latency is bounded by the membership-lease TTL (e.g.
3–9 s), not "until membership re-converges." A bounded reconciler scans
expired-owner shards specifically.

**Regional DR.** Active-passive first (coordinators only in the active region → no
ownership split-brain). A defined path to **home-region-per-shard active-active**:
every shard has one home region that owns all its fence writes (single-writer, so the
fence stays a real CAS), other regions reject fence writes for it — active-active at the
*fleet* level, single-writer *per shard*, sidestepping CRDB's LWW trap entirely. CRDB
counters/sets are fine for non-critical fan-out metrics, never for the lease.

**Migration** re-sequences A so the fast path + liveness come *before* raw capacity: (0)
measure → (1) membership + directory in shadow mode → (2) per-shard schedules +
per-entity timers (still S=1) → (3) cross-instance fast wake → (4) split the deployable
→ (5) raise `S>1` for capacity only if measured → (6) DR.

---

## Tunable consistency (both options)

A per-subscription / per-deployment tier in config, each a concrete durability +
freshness setting (the fence, not the tier, is the safety boundary):

| Tier | Mechanism | RPO on failover | Cost |
|---|---|---|---|
| **A — at-least-once, fast** (default) | no `WAIT`; wake on local primary | full async lag (re-fired post-failover, fence-deduped) | best latency |
| **B — durable wake** | `WAITAOF 1 1` on the fence-minting write before dispatch; **client checks the returned pair** | ~AOF fsync interval | one write round-trip per arm; needs AOF + in-region replica |
| **C — read-your-writes** | carry a D1-bookmark-style freshness token; replica read blocks until it applied that generation, else read primary | n/a (read path) | per-read token plumbing |

`WAIT`/`WAITAOF` are durability, **not** linearizability. Bounded-staleness has no
native Redis primitive — synthesize it as "read primary if replica lag > X." Only Tier
B touches the hot path; schedules and fan-out reads are deliberately eventual because
the fence + reconciler make staleness self-healing. **Verify the managed Redis 8 SKU
first:** a plain offering likely exposes only single-primary + async replica +
`WAIT`/`WAITAOF` (so DR is active-passive with lag-bounded RPO > 0), *not* Enterprise
Active-Active — design for that floor.

## Recommendation

**Build Option A.** Slot-home whole subscriptions, per-slot schedules, the per-entity
due-set, and work-sharded leased ownership — the [build spec](#option-a--build-spec-everything-an-implementer-needs)
is the contract. That addresses both the `O(N·K)` scan and the single-slot capacity
ceiling while staying in one binary, which already held at K=10k. **But it does not address
the [per-type claim-contention axis](#a-third-axis-per-type-claim-contention-from-the-load-test)** — the failure that actually collapsed the system at 12 replicas. Pair Option A with a
claim-granularity fix (finer per-shard-of-type leases, partly client-side); sequence that
*first*, since the sweep/keyspace work hardens a path that was already clean at 6 replicas.

The A-vs-B fork is **not** an open decision you make after the measurements — A is chosen.
The measurements decide only the *later, optional* question of whether the split to Option
B is ever needed (the cross-instance fast wake, CPU isolation, or independent scaling), not
whether to start. Do not split on intuition; do not wait on the measurements to begin
Option A.

## Measure before building (the gating experiments)

The review proved several load-bearing numbers are still **unmeasured** (RESULTS-gke.md
lists them as not-yet-done). The SUT is already instrumented (`chronicle_sweep_*`,
`chronicle_wake_*`, `chronicle_worker_due_items` in package `metrics`) and the K-sweep
baseline is in RESULTS-gke.md — so the gap is the **loadgen rig** (chaos/pod-kill,
fault-injection, the new metrics above, a managed-Redis CPU reader), not the SUT. Only
experiment 1 runs on the rig as it stands today; 2–5 need a foundation step and/or new
tooling, so they are sequenced against the build, not all "run first":

1. **`O(N·K)` redundancy** *(runnable now, + a Redis-CPU reader)* — ramp replicas 1→4 at K=10k, read managed-Redis CPU. Confirms the scan multiplies (the whole premise). Pass: CPU scales with N at fixed K.
2. **`OnStreamAppend` fan-out regression** *(after step 5: the `S`-slot shard)* — append→wake p99 with 1 vs `S` parallel `SMEMBERS` at S=2/4/8/256 on a real cluster. *The* number that decides whether slot-homing is viable. Pass: p99 regression within budget; with the occupied-slots bitmap it tracks occupied-slots-per-stream, not `S`.
3. **Due-set write amplification** *(after step 4: the due-set)* — the added `ZADD`/`ZREM` QPS on append/ack vs the recovery-latency win (uses the new `DueSetMutation` metric). Sizes the cost of the due-set step; informs, does not gate. Local #12 evidence records the per-branch Redis-op delta: `arm_wake` `ARMED` adds one `ZADD`; `arm_wake` `BUSY` adds zero; `ack` `done='1'` adds one `ZREM`; `ack` heartbeat adds zero; `expire_lease` `EXPIRED` adds one `ZADD` when pending remains or one `ZREM` when the old due mark is stale; `release` `OK` adds one `ZREM`; `delete_sub` adds one cleanup `ZREM`. Real GKE `DueSetMutation` QPS is an orchestrator-run sizing campaign, not a worker-local gate.
4. **Membership churn window** *(after step 3: leased ownership; needs a chaos step + ≥2 replicas)* — pod-kill a slot owner; measure the coverage-gap latency (the detectable case recovers at the new owner's `claim_shard` + eager reconcile, so **≤ membership-lease TTL + RTT**, not a sweep tick) and confirm zero lost wakes / zero double-grants. This is [07](07-jepsen-style-verification.md)'s L2/L4 as a rig run.
5. **Failover fence drill** *(after step 4; needs `STANDARD_HA` Redis + fault-injection + a webhook receiver)* — drop the lease-ZSET tail mid-lease; confirm only the cursor-reading reconciler (the failover-aware eager reconcile) recovers a stranded webhook sub, and a deposed ack returns `FENCED` (HTTP 409, not silent success). This is [07](07-jepsen-style-verification.md)'s L3, and the basic-tier Redis `ltctl` provisions has no failover — provision `STANDARD_HA`.

6. **Per-type claim-contention collapse** *(runnable now on the Electric agents rig — the experiment that actually reproduced the failure)* — ramp agents-server replicas 6→12+ against a **single hot per-type subscription** and read `ALREADY_CLAIMED`/`FENCED`/lease-lapse rates, wake p99/p50, and per-busy-agent throughput (`1/round-trip-latency`). **Falsifies any CPU-scaling pass criterion** — the empirical collapse sat at ≤12% CPU on every tier. Pass: BUSY/FENCED rates stay bounded and per-busy-agent throughput does not fall off as replicas rise; FAIL is the observed fence storm. This is the gate the claim-granularity fix must move, and it maps to [07](07-jepsen-style-verification.md)'s contention tier.

**Experiment 6 reproduces the actual 12-replica collapse (claim contention) and should be characterized first; experiment 1 gates the premise (sweep); 2 gates the rebuild (slot-homing viability); 3 sizes the
due-set step; 4–5 prove the liveness and DR claims this doc rests on** (and share their
tooling with 07's L-series).
