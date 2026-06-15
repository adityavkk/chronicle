# 07 — Jepsen-style safety + liveness verification

A test plan that proves the proposed architectures ([05](05-proposed-architecture.md))
actually hold their safety and liveness properties under faults — in Go, building on
chronicle's existing `jepsen/` harness, with `porcupine` as the linearizability checker.

## Why this shape

Jepsen drives a deployed system with three parts — a **generator** (client op
workload), a **nemesis** (fault injection), and a **checker** (verifies the recorded
**history** against a model) — and the **safety/liveness distinction dictates how each
property is tested** (Alpern–Schneider):

- **Safety** = "nothing bad ever happens." A violation has a **finite counterexample**
  (a bad prefix no continuation can fix). Test it as an **invariant** over a recorded
  concurrent history; `porcupine` (the Go-native Knossos) finds the witness.
- **Liveness** = "something good eventually happens." A violation has **no finite
  counterexample** — any prefix can still be extended into a good trace. So liveness is
  only testable against a **time bound**, asserted **relative to quiescence** (after the
  last fault heals — during a sustained partition the system is *correctly* unavailable,
  CAP).

chronicle already ships the triad in Go (`jepsen/checker/main.go`): a generator that
appends a known message set, a `kubectl`/`redis-cli` nemesis (`killOneOrigin`,
`killRedis`, `deleteStreamIndex`) on a `time.Ticker`, and a checker (`verify()`) that
asserts every cursor reached its tail within `-settle` (a bounded-liveness property).
What's missing: a real **per-op linearizability** checker (today it checks only the
*final* state), and the safety/liveness properties of the *proposed* mechanisms.

The Go-native tool is **`porcupine`** (`anishathalye/porcupine`): you give it a
sequential `Model{Init, Step(state,input,output)→(ok,newState), Equal}` plus a history
of `Operation{ClientId, Input, Output, Call, Return}`, and `CheckOperations` proves
linearizability or `VisualizePath` renders the counterexample. It's ~1000–10000× faster
than Knossos and is what etcd, TiKV, and MemoryDB use. **The modeling insight:** a *wake*
is not a linearizable read/write — model the **lease ownership** (and shard ownership)
as the linearizable register; the fence is exactly the single-holder invariant.

## Safety suite — invariants (finite counterexample)

| # | Property | Invariant | Nemesis | Checker |
|---|---|---|---|---|
| **T1** | Single-holder lease | At no instant do two workers hold a token `ack.lua` will accept for one sub (the `(gen,wake_id)` fence; a deposed holder's ack is `FENCED`) | holder pod-kill mid-lease + **GC-pause sim** (hold a claimed token past `lease_ttl_ms`) + redis-failover + clock skew | **porcupine** lease-register model: state `{gen,wake,holder,phase}`; `Step` encodes `claim.lua`'s rotate-vs-coalesce + `ack.lua`'s fence; partition per `subId`. Witness = two `OK` acks under one `gen` window |
| **T2** | Cursor monotonicity | Per `(sub,path)`, `acked_offset` is forward-only; a replayed/stale ack is a no-op, never a regression or phantom advance | origin churn (retry-worker + sweep both re-fire) + redis-failover + clock skew | Custom non-decreasing checker (cheaper than porcupine): assert `acked_offset` never regresses and every advance is one fence-valid strictly-greater ack |
| **T3** | Ownership exclusivity | Exactly one current owner per slot/shard; a deposed owner's schedule writes are `FENCED` (`check_owner.lua`) | slot-owner churn + GC-pause sim + partition (toxiproxy/iptables) + clock skew on the membership TTL | **porcupine** CAS-register model of `ds:{ownership}:slot:<h> = {owner_id, owner_epoch, lease_expiry_ns}`. **Acceptance gate** for the proposed `claim_shard.lua` — proves it's a real CAS, not a silently-dropping LWW |
| **T4** | No stale-generation effect | A wake/ack/release/record carrying an old generation **never** mutates durable state | expired-lease-takeover + re-arm churn; a "ghost" worker replays a stale token | Custom effect checker: for every op whose gen ≠ then-current, assert status ∈ {FENCED,BUSY,STALE,NOSUB} **and** the before/after durable snapshot is byte-identical |
| **T5** | No cross-subscriber leakage (slot-homing) | A wake for path `p` reaches exactly `p`'s subscribers; an ack for `s` touches only `s`'s `{__ds:h}` keys | slot-owner churn during the S-parallel fan-out + a deliberately mis-tagged sub (assert CROSSSLOT is *detected*, not silent) | Differential checker: the S-slot scatter-gather subscriber set **equals** the unsharded single-`SMEMBERS` reference; no foreign wake; every sub whole-homed in one slot |

**T1 is the highest-ROI test and needs no rebuild:** it generalizes the existing
`runExpiredLeaseTakeover` (one hand-built sequence) into a **model-checked concurrent
history**, proving the fence holds the single-holder invariant under arbitrary
interleavings + faults. This is the etcd-lock bug class (Jepsen found etcd double-granted
a lock and lost ~18% of acked updates) — modeled directly.

## Liveness suite — bounded, quiescence-relative

| # | Property | Bound | Nemesis | Checker |
|---|---|---|---|---|
| **L1** | At-least-once delivery (sharded + owner-replica) | per-message ≤ one sweep tick under no fault; ≤ `max(sweep,reconcile)+RTT` after the last fault heals | continuous origin churn + kill-all after the final append; redis-restart (AOF replay) | Extend `verify()` to a **per-message** delivery checker over the S-slot keyspace; porcupine monotone-cursor model per stream |
| **L2** | Bounded recovery under churn (coverage gap) | ≤ **one reconcile interval** `R` from the membership change to the wake firing | continuous **kill-slot-owner** (read `ds:{ownership}:slot:<h>`, kill that pod) on a randomized 2–8 s window | Per-message `deliver−append` ≤ `R+RTT` for any message whose slot was unowned at append; histogram + worst case (the measure-before-build #4 number) |
| **L3** | Failover recovery of a stranded wake | ≤ `R` from the lease-tail drop to the cursor reaching tail | **drop-the-lease-tail**: `ZREM` the lease/due entry while leaving the `sub` hash intact (simulates a failover that lost the schedule tail) | Assert the cursor reaches tail and only the **cursor-reading reconciler** could have done it (run a `-sweep=0` variant); a deposed ack still returns `FENCED` |
| **L4** | Eventual ownership re-convergence | every slot single-owned within **membership-TTL + R** after the last churn, stable thereafter | hard membership churn (`kubectl scale 2→1→3→2` + force-deletes), then quiesce | **Ownership-timeline sampler** polls `members` + every `slot:<h>` ~500 ms; after quiescence assert exactly one unexpired owner per slot, no oscillation; flag 0 owners (gap), >1 effective owners (split-brain), or stale epochs still accepted by `check_owner.lua` |
| **L5** | No permanent starvation under continuous churn | per-sub max inter-delivery gap ≤ `3R` (statistical, over a ~5 min run) | never-quiescing **combined** nemesis (kill-slot-owner + partition+heal + redis-failover + lease-tail-drop), churn period near `R` to try to trap one sub | Per-sub starvation checker; FAIL only on a sub stalled > `3R` *with pending work* |

**L2/L3 are the load-bearing claims of [05].** Doc 05's whole "work-sharding is an
optimization over a correct full-sweep baseline" argument rests on L2 (the coverage gap
is bounded by the reconciler, not unbounded), and "the sweep is demoted, not deleted"
rests on L3 (only the cursor-reading reconciler recovers a stranded webhook wake). These
are the measure-before-build experiments #4 and #5 from [05], as executable assertions.

## The Go harness — build on `jepsen/`

Reuse the existing harness wholesale (the k3d lifecycle `up.sh`/`run.sh`, the nemesis
primitives, the HTTP client, `waitReady`). Add `github.com/anishathalye/porcupine` to the
root `go.mod` (`jepsen/checker` inherits it). Then:

1. **`history.go`** — a recorder that brackets each client op into a
   `porcupine.Operation` with `time.Now().UnixNano()` (call before the request, return
   after the response) and stores `(status,gen,wake)` for fence ops.
2. **`models.go`** — `leaseModel` (T1), `shardModel` (T3, the ownership CAS register), each
   `Partition`-ed by `subId`/`shardId` so the NP-hard search stays per-key; cursor and
   isolation use cheaper custom checkers.
3. **New scenarios** in the `-scenario` switch: `single-holder-linz`, `cursor-monotonic`,
   `ownership-exclusivity`, `stale-gen-noop`, `slot-isolation`, plus the five liveness
   ones. On `CheckResult != Ok`, `porcupine.VisualizePath` emits the counterexample
   timeline next to `docs/jepsen/results.md`.
4. **Enrich the nemesis** (the prior-art gap): a **`gcPause`** that holds a claimed token
   past `lease_ttl_ms` *in-process* (no infra, highest ROI for T1/T3); **clock skew**;
   **`toxiproxy`** in front of Redis for partition/latency without killing pods;
   **`killSlotOwner`** / **`dropLeaseTail`**; and **randomized** start/stop windows
   instead of the fixed ticker.
5. **Recorders**: the ownership-timeline sampler (L4) and per-message `deliver_time`
   stamping (L1/L2/L3/L5).
6. **Make the time bound explicit per scenario** (`-settle`/`-sweep`/`-reconcile`) — a
   liveness verdict is only meaningful relative to its bound.

## What runs today vs what gates the rebuild

- **Runs against today's code** (the fence, cursor, and at-least-once already exist in
  the single-`{__ds}` design): **T1, T2, T4** and **L1, L3** (via `ZREM` lease-tail drop).
  These should be built first — they prove the *existing* fence and recovery under
  concurrency + faults, generalizing the current hand-built scenarios, with **no rebuild
  required.**
- **Acceptance gates for the proposed mechanisms** (`claim_shard.lua`, `check_owner.lua`,
  `ds:{ownership}:*`, the S-slot `{__ds:h}` tagging, the per-sub due-set): **T3, T5** and
  **L2, L4, L5**. They can't run until that code exists — so they *are* the executable
  contract each migration step must satisfy. Write them as the spec, red until the step
  lands.

## Honest gaps

1. **Linearizability is NP-hard.** `porcupine` blows up on highly-concurrent histories —
   `Partition` strictly per sub/shard and keep per-worker op counts modest (3–5 workers,
   tens of ops). A deep failover history can still time out (`Unknown`, not `Illegal`).
2. **No in-process failpoint seam.** The harness drives from outside, so it can't kill
   "exactly between `arm_wake.lua` and `record_wake_sent.lua`" or between the shard CAS
   and the first due-tick. Adopt **`gofail`** (etcd's failpoint library) for surgical
   windows; until then, approximate with coarse pod-kill + many seeds.
3. **No real failover substrate.** `deploy.yaml` is one Redis (`Recreate`); `killRedis`
   only tests AOF replay. L3 is tested more directly by the `ZREM` lease-tail drop; a true
   failover (Sentinel/cluster, or the managed Redis 8 SKU + WAIT/WAITAOF RPO) is out of
   scope of the current k3d substrate and is its own rig.
4. **Recovery bound — resolved in [05](05-proposed-architecture.md).** The full sweep
   keeps the ~2 s `sweepInterval` and covers **all** `S` slots (owned or not); only the
   `O(streams)` fan-out-index reconcile runs at 30 s. So the unowned-slot coverage gap is
   one **sweep** interval, not one reconcile interval: L2/L3 assert
   `deliver − append ≤ sweepInterval + RTT` (~2 s), the tighter bound.
5. **Clock-skew nemesis blurs real-time edges.** Pin the `porcupine` recorder's clock to
   the *driver host*, never a skewed node, so `[Call,Return]` ordering stays sound.

## Recommendation

Build **T1 + the in-process `gcPause` nemesis first** — it proves chronicle's *existing*
fence holds the single-holder invariant under concurrency and the Kleppmann
deposed-but-resumed case, with no rebuild, generalizing `runExpiredLeaseTakeover` from
one sequence to a model-checked history. Then **T2** (cursor) and **L3** (lease-tail-drop
recovery) — also runnable today. The proposed-mechanism tests (T3/T5, L2/L4/L5) become
the **acceptance gates** wired into the migration so each step ships green or not at all.

## Sources

Jepsen: jepsen.io, the consistency taxonomy, the generator/nemesis/checker tutorials,
Knossos (`knossos.competition` = `linear` + `wgl`), Elle (VLDB 2020). Go: `porcupine`
(pkg.go.dev API; "Faster linearizability checking via P-compositionality"), etcd
robustness tests + `gofail`, Antithesis DST, `Shopify/toxiproxy`, `chaos-mesh`, TiPocket
(Jepsen/Elle ported to Go), the DST landscape (FoundationDB/TigerBeetle/WarpStream).
chronicle's own harness: `jepsen/checker/main.go` (generator `:123`, nemesis `:779`,
`verify()` `:399`, `runExpiredLeaseTakeover` `:223`).
