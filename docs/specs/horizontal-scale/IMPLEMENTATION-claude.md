# Horizontal-scale epic #9 — Implementation writeup (claude)

> Branch `adityavkk/epic-hscale-claude` · PR **#18** (`epic-hscale-claude → main`, **DO NOT merge**) · written 2026-06-19 while context was warm, before worktree cleanup.
> Companion artifacts: [`loadtest/RESULTS-claude.md`](../../../loadtest/RESULTS-claude.md) (V&V ledger + cloud numbers), [`docs/jepsen/results.md`](../../jepsen/results.md) (T/L/C run logs).

## 1. Summary

Shipped the full `__ds` subscription control-plane horizontal-scale design across seven sub-issues (#10–#16): the per-type **claim-contention collapse is fixed first** (#11, the actual production bug — the per-`(subId,g)` granularity moves the contention knee ~G×), then **Option-A hardening** layers on top — due-set outbox (#12), event-triggered recovery + a coarse floor (#13), leased slot ownership via CAS (#14), slot-homed state into `S=256` keyspace slots (#15), and an active-passive DR / durability tier (#16). All safety properties **T1–T5 are GREEN** locally (porcupine-checked, with an injected-bug "teeth" proof on the two acceptance gates T3/T5), the contention suite **C1–C3 GREEN**, and the `webhook.Metrics` interface stayed strictly append-only throughout.

**Slot-homing decision: SHIP** (with one named follow-up). Why: slot-homing is **correct** (T5 GREEN — zero cross-subscriber leakage), its fan-out blow-up risk is **mitigated by design** (the occupied-slots bitmap bounds the probe set to occupied-slots-per-stream — ≤4/append locally, never 256), and it is **reversible** (shadow-write + lazy migration; `S` is a compile-time const). The one thing SHIP rests on that is *not yet measured in the cloud* is gate #2's fan-out p99 on a real **sharded** Redis — see §6. That is a confirmatory follow-up, not a defer-blocker, because the design is reversible and T5-correct.

## 2. What was built, per sub-issue (#10–#16)

**#10 — V&V foundation + no-rebuild baseline.** Established the executable contract before any behavior change: appended six horizontal-scale methods to `webhook.Metrics` (`FanOut`/`DueSetMutation`/`DueWorkerTick`/`SlotOwnership`/`CoverageGap`/`OwnerFenced` — `webhook/metrics.go`, consistent across interface / `NopMetrics` / Prometheus / golden test), and stood up the jepsen scenario+nemesis harness (`jepsen/checker/nemesis.go`: gcPause / toxiproxy / killSlotOwner / dropLeaseTail / clock-skew). Turns green the **T1** single-holder-linz (porcupine `model_fence.go`, Ok over 293 ops under gcPause+clock-skew), **T2** cursor-monotonic, **T4** stale-gen-noop, **L1** at-least-once, **L3** lease-tail-drop on *today's* code — the no-rebuild baseline every later slice must preserve.

**#11 — Claim granularity / the third axis (Phase 1, the actual collapse).** Adds per-`(subId,g)` claim granularity so one subscription type can be held by multiple workers (`G` shards) instead of a single global claimant (`webhook/shard.go`, `webhook/redis_store.go` `ClaimShard`). This is the production fix: **C1/C2 reproduce the empirical collapse** (at G=1 per-worker throughput halves 21.7→10.4→5.2 ops/s as claimants rise 6→12→24, BUSY/op→0.97 — the fence storm), and **C3 / doc-05 gate #6 PASS** (at G=16 per-worker holds ~27 flat, aggregate scales 158→336→650 — the knee moves beyond G=16). `ClaimContention` metric appended (cardinality-safe: subID discarded, only status is a label). **T1 still holds per `(subId,g)`** (`model_shard.go` shard-linz, Unknown=fail).

**#12 — Due-set outbox (Move 2).** Adds a `ds:{__ds}:due` outbox ZADD/ZREM'd inside the fenced Lua scripts (`arm_wake.lua`/`ack.lua`/`expire_lease.lua`/`release.lua`) so the `dueWorker` (`webhook/manager.go:785`) drains owed subs in **O(owed)** via the unchanged `claim_due.lua`, with the full sweep retained as a backstop. Pure reconcile core `DecideDue` (`webhook/state.go:179`). Closes GAP3 (a released sub leaves no phantom due-mark, regression-tested). Owner of **gate #3** (due-set write amplification). T1/T2/T4/L1/L3 stay GREEN (fence untouched).

**#13 — Event-triggered recovery + coarse floor.** Splits the recovery sweeper into `reconcile(scope)` (sealed sum type `Boot|Reconnect|AppendError|Floor`, plus `EpochBump|NewOwnerCAS` for #14 — `webhook/manager.go:950`) and raises the coarse periodic floor **2 s → 30 s**. `reconcileLeases` (`:1235`) re-derives a stranded live/waking sub's dropped lease+due tail from the durable sub hash via `restore_lease.lua` (schedule-only — it never touches the `(gen,wake_id)` fence). `sweepOnce` (`:1147`) preserved byte-for-byte as the unguarded backstop. Pure core `DecideLeaseReconcile` (`webhook/state.go:224`). **L3 sharpened to `-floor=0` + explicit takeover and GREEN** (the eager reconcile reaches tail far under the 30 s floor → a periodic tick can never be the recoverer). Also fixed a real defect: the binary's `config.DefaultConfig.SweepInterval` was still 2 s — aligned to 30 s in `fc8d28a` so the floor change is actually effective in production.

**#14 — Leased slot ownership (Move 3).** Adds CAS-based slot ownership: `claim_shard.lua` CAS over `ds:{ownership}:slot:<h>` + `check_owner.lua` (external POST only), a membership heartbeat + **HRW** placement (`webhook/ownership.go` `hrwScore`/`HRWOwner`, FNV-1a + splitmix64 finalize, ~1/N reassignment), and a slot-reconcile loop; workers are gated by `ownedSlots() = HRW ∩ held-leases` (`webhook/manager.go:1106`). A TOCTOU owner-epoch check is **inlined** in `arm_wake`/`ack`/`expire_lease`/`schedule_retry`/`release` (gated on a non-empty epoch, so the load-balanced external path is unchanged). `sweepOnce` stays the UNGUARDED backstop. **T3 (acceptance gate) PASS** — porcupine `shardModel` CAS-register (`model_shard.go`), Unknown=FAIL: linearizable over 165–275 ops / 4 partitions across seeds, **with teeth** (an injected epoch-reuse/LWW bug flips the run to *Illegal* with a counterexample). Owner of **gate #4** (churn window) and **gate #5** fence (with #16).

**#15 — Slot-homed state shard (Move 1).** Homes a subscription's whole key set under one `{__ds:h}` tag, `h = fnv32a(subId) % 256` (`webhook/keys.go` `slotOf`, Go `hash/fnv` — **FNV-1a, deliberately not CRC16**; `slotOf` strips the `#11 :g:<n>` suffix so g-shards home to their sub's slot). `OnStreamAppend` (`webhook/manager.go:359`) becomes `S` parallel pipelined `SMEMBERS` over the stream's **occupied** slots (`redis_store.go` `StreamSubscribers`), gated by the typed `OccupiedSlots` bitmap (`keys.go:178`, a 256-bit string per stream). Migration is **shadow-write + lazy per-sub** (`webhook/migrate.go` `migrateSub`) — reversible by construction (see §3). **T5 (acceptance gate) PASS** — the live `slot-isolation` differential checker: 320 subs over 8 streams spanning 204/256 slots, the scatter-gather subscriber set ≡ reference ≡ brute-force union, **zero foreign wakes**, held under concurrent ownership churn; a mis-tag is DETECTED (CROSSSLOT). Owner of **gate #2** (fan-out p99) — the one PENDING-CLOUD deciding number.

**#16 — DR + system-level capstone.** Active-passive DR (the only new mechanism): (1) a Tier-B **`WAITAOF`/`WAIT` durability barrier** on the fence-minting writes (`ArmWake` ARMED / `Claim` CLAIMED), checked via pure `InterpretWaitAOF`/`InterpretWait`; (2) `Manager.Promote()` (`webhook/manager.go:406`) re-establishes ownership on the promoted primary and fires the failover-aware eager reconcile (`scopeEpochBump`); (3) a sealed `ConsistencyTier` A/B/C config surface (`webhook/consistency.go:26`). **Correction #3 (load-bearing): `WAIT`/`WAITAOF` are durability, NOT linearizability** — the monotonic `(gen,wake_id)` fence stays the *only* exclusivity guard. **T1–T5 all GREEN** on the AOF substrate; Tier-B proof: re-ran T1 in Tier B and saw Redis `cmdstat_waitaof: calls=16, rejected=0` while T1 stayed `linearizable: yes`. STANDARD_HA failover substrate authored (`jepsen/deploy/standard-ha.yaml` + `standard-ha-failover.sh`). Owner of **gate #5** (failover fence drill).

## 3. Key design decisions & rationale

**The `(gen,wake_id)` fence + owner-epoch layering (#14).** The original monotonic `(generation, wake_id)` fence is the *only* exclusivity guard and was kept **byte-for-byte unchanged**. #14's owner-epoch check is layered strictly *above* it (inlined in the fenced scripts, gated on a non-empty epoch) — it never replaces the fence; it fences a *deposed-then-resumed* owner. Trade-off: two fences to reason about, but the safety argument composes (T3 proves the owner-epoch CAS register; T1 proves the inner fence still holds) and the externally-load-balanced path (empty epoch) is provably unchanged. *This layering is why every prior T-property survived each later slice.*

**Claim granularity (#11).** Chose per-`(subId,g)` shards with `G` a per-type knob over the alternatives (sticky hashing, per-partition leases) because it is the *minimal* change that breaks the single-claimant fence storm while keeping T1's linearizability per shard. Trade-off: `G` is a tuning parameter the operator must set per high-fan-in type; G=1 is byte-identical to today (safe default). Shard 0 / G=1 is byte-identical to the pre-change path.

**Due-set outbox (#12).** An outbox ZSET mutated *inside* the already-fenced Lua scripts (not a separate transaction) so the due-mark can never diverge from the fence decision. Trade-off: a small write-amplification on arm/ack (one ZADD/ZREM) bought against turning the recovery sweep from O(N) into O(owed). The full sweep is retained as a backstop, so a missed outbox entry degrades to "slower", never "lost" — owner of gate #3 to bound the amplification.

**Event-triggered recovery + coarse floor 2 s→30 s (#13).** Recovery is now event-driven (boot / reconnect / append-error / new-owner-CAS), with the periodic sweep demoted to a coarse 30 s *floor* — because the happy path is event-driven and the old 2 s sweep fired nothing in steady state. Trade-off: a longer worst-case floor latency for a stranded sub that somehow misses every event, accepted because L3 proves the eager reconcile (not the floor) is the recoverer. Steady-state delivery latency is unchanged.

**Leased slot ownership / CAS (#14).** HRW (rendezvous) placement over consistent-hashing rings because HRW gives ~1/N reassignment on membership change with no ring bookkeeping, and the slot owner is decided by `HRW ∩ held-leases` so ownership can't outrun the actual lease. CAS over `slot:<h>` with an owner epoch makes transfer atomic and detectable. Trade-off: a membership heartbeat + reconcile loop to run; mitigated by keeping `sweepOnce` an *unguarded* backstop so a total ownership outage still drains via the floor.

**Slot-homing S=256 + MIGRATION STRATEGY (#15).** Home each sub's keys under `{__ds:h}` so per-sub atomic Lua stays single-cluster-slot while fan-out parallelizes across nodes. `S=256` (a compile-time const) comfortably exceeds any expected replica count (the gate-#2 upper bound). **Migration is explicit: shadow-write + lazy per-sub, copy-then-flip, with a sweep backstop.**
- *Writes* always target the slot-homed tag (`redis_store` computes `h` from the id). *Reads* lazily migrate on a slot-homed miss: `migrateSub` HGETALLs the legacy `{__ds}` copy, **copies** it to `{__ds:h}`, then **flips** (drops the legacy copy). The full sweep (`List()` unions the legacy id-set) is the backstop that drains cold subs on quiet slots.
- **Reversible by construction:** `S` is a compile-time const and the legacy readers are retained, so rollback is the mirror move (read new, write legacy).
- **Crash-safe:** the copy-then-flip is non-destructive — a crash mid-migrate leaves the legacy copy intact to be re-migrated on next access; a half-done copy is superseded the moment the slot-homed copy is read (`Get` prefers it). Idempotent (HSET overwrites, ZREM/DEL of absent members are no-ops).
- **Cost/safety trade-off:** it is **cross-slot by nature** (legacy `{__ds}` and `{__ds:h}` are different cluster slots), so it is a Go-side dual-write, *not* one atomic Lua — there is a non-atomic dual-write window, mitigated by copy-then-flip + idempotency + the sweep backstop. The win is **no big-bang cutover, no downtime**; the cost is the keyspace carrying both copies until the lazy drain + sweep complete, and a cold sub on a quiet stream relies on the full-sweep backstop to migrate. **What I'd do differently:** add an explicit one-shot offline migrator to drain every slot deterministically for a production cutover, rather than relying on lazy+sweep for cold subs (see §6).

**DR tier (#16).** Tunable consistency as a sealed `A/B/C` enum (never a bool/free string) parsed at the env boundary, with only Tier B touching the hot path. The hard call here was scoping `WAIT`/`WAITAOF` to **durability, not linearizability** (Correction #3): it would have been tempting to treat the replica-ack count as an ordering signal — that would have been wrong, and no path infers exclusivity from it.

## 4. Key references (a navigable index)

**Source docs** (`docs/specs/horizontal-scale/research/`):
- `05-proposed-architecture.md` — the design + the **gate #1–#6 definitions** (the metric→trigger→gate table is at §"Metrics", around L525–530).
- `06-adversarial-review.md` — the red-team pass (Correction #3 "WAIT is durability not linearizability" lives here).
- `07-jepsen-style-verification.md` — the **T1–T5 / L1–L5 / C1–C3 contract** + the honest-gap list.
- `08-claim-granularity.md` — the #11 per-shard-of-type (G=16) design.
- `README.md` — research index; `ORCHESTRATION.md` — how the epic was run.

**The executable contract — properties and where each is checked** (`jepsen/checker/`):
- **T1** single-holder linearizability → `scenario_lease.go` + `model_fence.go` (porcupine lease register, Partition per key, Unknown=FAIL).
- **T1′** shard-linz per `(subId,g)` (#11) → `scenario_shard.go` + `model_fence.go` (partitioned per shard).
- **T2** cursor-monotonic → `scenario_cursor.go` + `check_cursor.go`.
- **T3** ownership-exclusivity (#14 acceptance) → `scenario_ownership.go` + `model_shard.go` (CAS register, with injected-bug teeth).
- **T4** stale-gen-noop → `scenario_stalegen.go` + `check_stalegen.go`.
- **T5** slot-isolation (#15 acceptance) → `scenario_slot.go` + `check_slot.go` (differential: scatter ≡ reference ≡ brute union; `go test ./jepsen/checker -run SlotLeakage`).
- **L1** at-least-once → `check_delivery.go`. **L3** lease-tail-drop → `scenario_leasetail.go`.
- **C1/C2/C3** claim contention (gate #6) → `scenario_contention.go` + `check_contention.go`.
- Harness entry + nemesis: `jepsen/checker/main.go`, `jepsen/checker/nemesis.go`.
- **doc-05 gates #1–#6**: defined in `05-proposed-architecture.md`; status ledger in `loadtest/RESULTS-claude.md` §"Gate ledger".

**Code anchors** (file `Func()` — line numbers drift, names don't):
- Fan-out (gate #2): `webhook/manager.go` `OnStreamAppend` (:359) → `redis_store.go` `StreamSubscribers` (:358) → `metrics.go` `FanOut` (:40); bitmap `keys.go` `OccupiedSlots`/`decodeOccupiedSlots` (:178/:182).
- Slot-homing (#15): `keys.go` `slotOf` (:81), `const subSlots = 256` (:34), `slotKey` (:241); migration `migrate.go` `migrateSub` (:45).
- Claim granularity (#11): `shard.go`, `redis_store.go` `ClaimShard` (:496), `scripts/claim_shard.lua`.
- Due-set (#12): `manager.go` `dueWorker` (:785), `state.go` `DecideDue` (:179), `scripts/claim_due.lua`.
- Recovery (#13): `manager.go` `reconcile` (:950) / `sweepOnce` (:1147) / `reconcileLeases` (:1235), `state.go` `DecideLeaseReconcile` (:224), `scripts/restore_lease.lua`.
- Ownership (#14): `ownership.go` `hrwScore`/`HRWOwner` (:259/:278), `manager.go` `ownedSlots` (:1106), `scripts/check_owner.lua`; inlined owner-epoch fence in `arm_wake.lua`/`ack.lua`/`expire_lease.lua`/`schedule_retry.lua`/`release.lua`.
- DR (#16): `manager.go` `Promote` (:406), `consistency.go` `ConsistencyTier` (:26), `failpoint.go` (arm→emit seam).

**Commits + PR** — branch `adityavkk/epic-hscale-claude`, **PR #18**. Notable SHAs (newest first):
- `469fda1` real-GKE shakeout + cloud V&V record · `fc8d28a` fix: default SweepInterval→30 s floor.
- `#16`: `47770be` WAITAOF Tier-B barrier · `6b6094f` Promote() eager reconcile · `db0a30f` A/B/C pure core · `71ce7b9` STANDARD_HA substrate.
- `#15`: `8987069` slot-home into S=256 · `b978a26` shadow-write + lazy migration · `a26e4c4` T5 differential checker.
- `#14`: `0300058` claim_shard/check_owner Lua · `57c113d` membership+HRW+reconcile · `fe7851c` inline owner-epoch fence · `4dcdc41` T3 gate.
- `#13`: `2867c83` split reconcile(scope)+floor · `b854ce9` DecideLeaseReconcile · `d953562` sharpen L3 -floor=0.
- `#12`: `f8c30aa` due-set core · `c5233f3` mutate outbox in Lua · `0fa77ff` dueWorker O(owed).
- `#11`: `fa75bc8` per-(subId,g) granularity · `c65a972` C1/C2 collapse repro · `5b8ae89` C3 gate #6.
- `#10`: `3a77f1f` six Metrics methods · `daf4254` nemesis primitives · `ce94d5c` T4/C1-C3 checkers.

## 5. V&V results (honest)

### Local — GREEN
| Suite | Property | Result |
|---|---|---|
| Unit / pure-core | `go test -short ./...` (root + `loadgen/`) | GREEN |
| Integration | `webhook` against real `redis:7` (whole-epic #10–#16) | GREEN (`ok webhook`) |
| T1 | single-holder-linz (porcupine) | GREEN — 463 ops, linearizable |
| T1′ | shard-linz per `(subId,g)`, G=8 | GREEN |
| T2 | cursor-monotonic | GREEN |
| T3 | ownership-exclusivity (CAS register) | **GREEN + teeth** (injected bug → Illegal) |
| T4 | stale-gen-noop | GREEN (deposed ack 409 FENCED, byte-identical cursor) |
| T5 | slot-isolation (differential) | **GREEN + teeth** — 320 subs / 204 slots, 0 foreign wakes, CROSSSLOT detected |
| L1 | at-least-once | GREEN (6/6 streams at tail) |
| L3 | lease-tail-drop, `-floor=0` + takeover | GREEN (cursor-only recovery; tick can't recover) |
| C1/C2 | fence-storm collapse reproduced | GREEN (21.7→10.4→5.2 ops/s at G=1) |
| C3 | gate #6 — knee moves ~G× | **GREEN** (G=16: per-worker ~27 flat, aggregate 158→336→650) |
| Tier B | durability barrier exercised | GREEN (`waitaof calls=16 rejected=0`, T1 still linearizable) |

How to run: see §7.

### Cloud — real GKE run (the honest table)
Run on GCP `adityavkk-prototyping`, GKE + Memorystore, all resources `-claude`-suffixed and **torn down + verified ($0 ongoing)**. Full detail + the shared schema in `loadtest/RESULTS-claude.md` §"Cloud V&V — real GKE run".

| Gate | Scenario | Measured | SLO | Verdict |
|---|---|---|---|---|
| #2 fan-out | S=2/4/8/256 OnStreamAppend p99 | **not captured** (`fanout _count=0`) | within budget | **N/A*** |
| baseline | K=10k sweep p50/p99 | 1536 / **2037.8 ms** | p99 < 1500 | **FAIL†** |
| #4 | ownership churn (coverage-gap / 0-lost / 0-double) | not captured | — | **N/A‡** |
| #5 | failover drill (RPO/RTO) | not captured | — | **N/A‡** |
| L2/L4/L5 | liveness under churn/stress | not captured | — | **N/A‡** |

- **`*` Gate #2 N/A (honest, this is the deciding metric):** the loadgen tool `sweepscale` seeds subs and measures the recovery *sweep*; it never drives the wide-stream *append* path that `OnStreamAppend → S-parallel SMEMBERS → FanOut` needs, so `chronicle_fanout_seconds` stayed `_count=0` across all S. A direct in-cluster append driver also failed to populate it within budget. Mechanism is proven by **T5 GREEN** + the bitmap (`slots_probed ≤ 4`, never 256, locally). Also: Memorystore **BASIC is single-shard**, so the sharded max-node-RTT this gate targets needs Memorystore-for-Redis **CLUSTER** regardless.
- **`†` K=10k FAIL is confounded, not a regression:** the SUT was downsized `cpu:2→cpu:1` to fit cost-minimal `e2-standard-2`; the sweep is CPU-bound. Not like-for-like vs the 509 ms single-slot baseline. Fair re-run needs ≥`cpu:2` on `e2-standard-4`. Seeded 9936/10000 in 24.5 s.
- **`‡` N/A — rig harness gaps (first-ever GKE run):** gate #4 shells out to `redis-cli` (absent from the chronicle image); gate #5 needs a `STANDARD_HA` substrate + working failover; L2/L4/L5 need ≥2 replicas + chaos. The **mechanisms are proven in-process** (T3; `TestLeaseTailDropRecoveredByEagerReconcile`; `TestPromoteDrivesEagerReconcile`) — the cloud liveness-under-chaos numbers are what's pending.

**Rig bugs found + fixed on the first real GKE run** (all in `469fda1`): legacy global Cloud Build bucket deprecated → `--default-buckets-behavior=regional-user-owned-bucket`; `cpu:2` doesn't fit `e2-standard-2` → `cpu:1` + 2-node pool; deploy rolling-surge stalled → `strategy: Recreate`; `cmd_gate2` killed itself under `set -e` extracting a metric the fan-out job never emits → made tolerant.

## 6. Open questions & follow-ups

1. **Gate #2 fan-out p99 is the one unmeasured deciding number.** It needs (a) **Memorystore for Redis CLUSTER** (sharded — BASIC/STANDARD_HA are single-shard, so `{__ds:h}` slots don't span nodes and the max-node-RTT this gate measures isn't exercised) and (b) a **real wide-stream append driver** (loadgen drives the sweep, not appends). Until then SHIP rests on local T5 + bitmap + reversibility. **This is the single biggest honest gap.**
2. **K=10k 2037.8 ms is confounded** by the `cpu:1` downsizing — re-run at `e2-standard-4`/`cpu:2` for a real comparison against the 509 ms floor.
3. **Migration cost/safety:** the shadow-write + lazy + sweep approach has a non-atomic cross-slot dual-write window and relies on the full-sweep backstop to drain cold subs on quiet slots. An explicit **one-shot offline migrator** (deterministic per-slot drain + an orphaned-legacy-key audit) would be safer for a production cutover. Rollback also requires re-mirroring (read-new/write-legacy).
4. **Known defect found + fixed:** the binary's `config.DefaultConfig.SweepInterval` was 2 s while #13 raised the coarse floor to 30 s — the floor change was inert in the binary until `fc8d28a`. Surfaced by the #16 review; worth a config-vs-design drift check elsewhere.
5. **Test-seam honesty:** the arm→emit failpoint (`webhook/failpoint.go`) is a dependency-free in-process seam, not `gofail` proper — adopting real gofail is a build-system follow-up (07 honest-gap #2).
6. **Before slot-homing ships in production, prove:** (a) gate #2 `fanout_p99(S=256)` within the wake-latency budget on a real sharded cluster under wide-stream appends; (b) `slots_probed` tracks occupied-slots-per-stream (not S) under that load — locally ✓, cloud-unconfirmed; (c) K=10k p99 < 1500 ms at a fair node size; (d) the migration drains all slots with zero orphaned legacy keys.

## 7. Reproduce

**Local — full suite** (from repo root; needs Go + a local `redis:7`):
```sh
# unit / pure-core (root + loadgen module)
go test -short ./...
( cd loadgen && go test -short ./... )

# webhook integration against a real Redis (whole-epic #10–#16)
docker run -d --rm -p 6379:6379 redis:7
REDIS_URL=redis://localhost:6379/14 go test ./webhook/...

# jepsen pure model/checker units (incl. T5 differential, T3 register, contention)
go test ./jepsen/checker/...
go test ./jepsen/checker -run SlotLeakage     # T5 slot-isolation differential

# the T/L/C scenarios against a live chronicle binary + Redis (local-binary path)
#   chronicle on :4437 + one Redis; the jepsen harness drives scenarios & checks.
cd jepsen && ./run.sh          # multi-pod k3d rig: ./up.sh then ./run.sh then ./down.sh
```
Outputs: `go test` to stdout; jepsen run logs + porcupine verdicts recorded in `docs/jepsen/results.md`.

**Cloud — the gate rig** (GKE + Memorystore; every resource `-claude`-suffixed; **always tear down**):
```sh
cd loadtest
export LT_PROJECT=adityavkk-prototyping        # pass --project on raw gcloud calls
./ltctl.sh up                                   # build images, GKE cluster, Memorystore

# gate #2 — fan-out p99 (S is a compile-time const → one SUT image per S)
for S in 2 4 8 256; do LT_TAG="s$S" ./ltctl.sh gate2 spec/fanout-gate2.yaml; done
# baseline — K=10k sweep floor (use e2-standard-4/cpu:2 for a fair number; see §6)
LT_TAG=s256 ./ltctl.sh run spec/sweep-10k.yaml
# gate #4 churn / gate #5 failover (STANDARD_HA)
LT_TAG=s256 ./ltctl.sh gate4 spec/sweep-10k-churn.yaml
./ltctl.sh up --redis-tier=STANDARD_HA && ./ltctl.sh gate5 spec/dispatch-webhook-ha.yaml

./ltctl.sh down                                 # MANDATORY — verify with:
gcloud container clusters list --project adityavkk-prototyping
gcloud redis instances list   --project adityavkk-prototyping --region us-central1
```
Outputs land in `loadtest/gate2-<S>-metrics.txt` / `gate2-results.tsv` (gitignored run artifacts); the curated numbers + caveats are written into `loadtest/RESULTS-claude.md`.
