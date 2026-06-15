# 05 — Proposed scalability + DR architecture

Two concrete options, both grounded in chronicle's actual mechanisms and hardened
against the adversarial review ([06](06-adversarial-review.md)). They share a
foundation and differ only in **how far you split the system**. Read [06] first if
you want the critique that shaped these; the corrections it produced are baked in
here.

## Three corrections that frame everything

The review forced three precision fixes that every design below respects:

1. **The fence is the safety boundary; it is *not* liveness.** The
   `generation`/`wake_id` fence makes a *duplicate* wake harmless (coalesce at
   `arm_wake.lua` BUSY, or `FENCED` at the ack). It does **nothing** for a *coverage
   gap* — a slice of subscriptions briefly owned by no replica during a rebalance is
   simply not swept until ownership re-converges. So any work-sharding scheme must
   supply liveness *separately*, as an optimization over a still-correct full-sweep
   baseline — never as a correctness dependency.
2. **The sweep can never be fully retired.** Only the sweep re-derives owed work from
   *durable cursors* (links vs stream tail). After a failover that loses a webhook
   subscription's lease-ZSET tail but keeps its `sub` hash, the lease worker can't
   recover it (its `ZADD` is gone) — only the cursor-reading reconciler can. So "remove
   the `O(K)` scan" is true for the *hot path*, false for the *DR backstop*: the sweep
   is demoted to a rare reconciler, not deleted.
3. **"Tunable consistency" on Redis is a durability + freshness knob, not a CAP knob.**
   Redis is AP. `WAIT`/`WAITAOF` buy replica-acked / fsync durability — *not*
   linearizability (Redis docs say so explicitly). The genuinely strong guarantee is
   the monotonic generation fence enforced by compare-and-set + reject-if-stale at the
   resource — the Kleppmann fencing-token result, which chronicle already implements.
   Build the knob on durability + read-freshness, and never put a correctness-critical
   single-holder lease on a CRDB LWW register (it silently drops a concurrent writer).

## The shared foundation (both options need this first)

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
  the subs (`ds:{__ds:h}:sched:lease`, etc.), so `claim_due.lua` runs unchanged, once
  per slot. This is the real capacity lift — the original O2 left a single global
  schedule that *was* the surviving ceiling.
- **Per-subscription due-set replaces the `O(K)` scan.** `arm_wake` and `OnStreamAppend`
  `ZADD` the sub into `ds:{__ds:h}:due` (score = earliest-owed deadline); `ack`
  `ZREM`s it (one line in the already-atomic script). A per-slot due-worker fires
  owed subs — `O(owed)`, not `O(K)`. **Honest cost:** this *relocates* the existing
  `arm→emit→record` dual-write and *adds* a `ZADD`/`ZREM` to the hot path; measure it
  (below). The full sweep demotes to a ~30s reconciler.
- **The fence is untouched.** Generation/wake_id stay in the `sub` hash in `{__ds:h}`;
  every fenced script is still single-slot.

Where the options diverge: **Option A** keeps this in the one binary and shards the
*work* across replicas by leased slot ownership. **Option B** turns each subscription
into an actor with a Redis grain directory, splits the control plane from serving, and
adds a cross-instance fast wake.

---

## Option A — Slot-homed, evolve in place

**Bet:** the single-binary design is fine; the problem is purely the single slot and
the redundant scan, and both are fixed by sharding state into `{__ds:h}` slots and
sharding sweep *work* across replicas — with a leased-membership protocol supplying the
liveness the fence lacks.

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
ds:{ownership}:slot:<h>        STR   owner replicaId + owner_epoch (CAS-guarded)
```

**Horizontal scale.** State capacity scales with `S` across all cluster nodes (the
genuine ceiling-lift, because the schedule shards *with* the subs). Sweep/due work is
sharded by leased slot ownership: each replica runs the workers + reconciler only for
its owned slots, so total work is `O(total owed)` regardless of N — the `O(N·K)`
redundancy is gone. **The honest cost:** `OnStreamAppend` can no longer be one
`SMEMBERS`; it becomes `S` *parallel pipelined* `SMEMBERS` (one per slot) — ~1 RTT
wall-clock, not S serial, but a real, measured regression. Mitigation for sparse wide
streams: a per-stream "occupied-slots" bitmap so you probe only slots that actually
have a subscriber.

**Liveness (the review's #1 fix).** Slot ownership is an *optimization over a correct
baseline*: the coarse full-sweep still covers every sub regardless of ownership, so an
unowned slot during a rebalance is swept (slower, `O(K)`) until ownership
re-converges — a crash-lost wake waits at most one reconcile interval, never
indefinitely. Split-brain on a slot (two owners) is *safe* (double-wake coalesces).

**Regional DR.** Active-passive: one home region per slot, async cross-region replica.
Not CRDB (an LWW string fence silently drops a writer). `WAIT 1`/`WAITAOF 1 1` on the
fence-minting writes (`arm_wake`/`claim` generation `HINCRBY`) bounds RPO to ~the AOF
fsync interval; the client must check the returned count. On promotion (epoch bump),
each owner runs a **failover-aware eager reconcile** that re-derives schedule entries
from the durable `sub` hash — recovering the stranded-webhook-wake case. RPO = async
lag + fsync (~1s) + link latency; RTO = promotion time.

**Migration** (reversible, slice by slice): (0) instrument + run the unmeasured load
tests → (1) leased slot ownership, work-sharded sweep, full-sweep fallback intact → (2)
the due-set outbox in the single slot → (3) the `S`-slot state shard behind a shadow
write + lazy per-sub migration → (4) cross-region standby → (5) split the binary *only*
if metrics force it.

---

## Option B — Owner-replica coordinator (split the tiers)

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
~`1/N` of shards (no rebalancing storm). A Redis `dir` hash maps `shardId →
owner:epoch`, claimed by `claim_shard.lua` — a CAS in exactly the shape of
`claim.lua`'s expired-lease takeover, reused at shard granularity. An **owner-epoch
fence** layers above the wake fence: a deposed-but-resumed owner (GC pause / partition,
the Kleppmann case) finds `dir[G] != me` and self-evicts; `check_owner.lua` rejects its
stale-epoch schedule writes.

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

**The first three migration steps are identical for A and B** — slot-home the subs,
per-shard schedules, the per-entity due-set, work-sharded ownership. Do those, and the
`O(N·K)` scan and the single-slot capacity ceiling are both addressed while staying in
one binary. **The real fork is later:** split into a coordinator tier (Option B) *only*
if the measurements show you need the cross-instance fast wake, CPU isolation, or
independent scaling — Option A's "don't split yet" is the right default until proven
otherwise (the one binary held at K=10k).

So: **build the shared foundation, measure, then choose A or B.** Do not pick the split
on intuition.

## Measure before building (the gating experiments)

The review proved several load-bearing numbers are still **unmeasured** (RESULTS-gke.md
lists them as not-yet-done). Run these on the rig first — cheapest, in order:

1. **`O(N·K)` redundancy** — ramp replicas 1→4 at K=10k, read managed-Redis CPU. Confirms the scan multiplies (the whole premise).
2. **`OnStreamAppend` fan-out regression** — append→wake p99 with 1 vs `S` parallel `SMEMBERS` at S=2/4/8/256 on a real cluster. *The* number that decides whether slot-homing is viable.
3. **Due-set write amplification** — the added `ZADD`/`ZREM` QPS on append/ack vs the recovery-latency win.
4. **Membership churn window** — pod-kill an owner; measure the coverage-gap latency (must be ≤ one reconcile interval) and confirm zero lost wakes / zero double-grants.
5. **Failover fence drill** — drop the lease-ZSET tail mid-lease; confirm only the cursor-reading reconciler recovers a stranded webhook sub, and a deposed ack returns `FENCED`.

Runs 1–2 gate the rebuild; 4–5 prove the liveness and DR claims this doc rests on.
