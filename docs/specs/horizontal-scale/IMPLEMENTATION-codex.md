# Horizontal-scale epic #9 — Implementation writeup (codex)

## 1. Summary

This branch implements the horizontal-scale `__ds` subscription control plane from #10-#16: first the claim-contention fix, then the Option-A hardening path from the research docs. The implementation includes sharded pull-wake claims, a due-set outbox, event-triggered recovery, leased slot ownership, slot-homed subscription keys, and DR consistency tiers. The slot-homing decision for this PR is **DEFER** because real GKE gate #2 failed at S=256 and the K=10k sweep floor regressed; the root cause is a fixable hot-path migration/synchronous fan-out defect, not a green production-ready slot-homing result.

## 2. What was built, per sub-issue (#10-#16)

**#10: harness and executable contract.** Added the horizontal-scale checker baseline under `jepsen/checker/`, load-gate scaffolding under `loadtest/` and `loadgen/`, Prometheus/report hooks, and `loadtest/RESULTS-codex.md` for measured evidence. This established the T1/T2/T4/L1/L3 baseline, the C1-C3 contention contract, and the doc-05 gate #1 / gate #2 reporting shape that later issues extend.

**#11: bounded claim granularity.** Added typed claim shards in `webhook/claim_shard.go`, request parsing in `webhook/routes.go`, Redis state/script support in `webhook/redis_store.go` and `webhook/scripts/{claim,ack,release}.lua`, and contention models in `jepsen/checker/check_contention.go`. This changes replicas from competing for one global pull-wake lease to bounded shard work, preserving legacy inference for old leases, and is the direct fix for the 12-replica claim collapse captured by C1-C3.

**#12: due-set outbox.** Added pure due-set transition helpers in `webhook/due_set.go`, Redis schedule keys and operations in `webhook/redis_store.go`, due-worker loops in `webhook/manager.go`, and script updates across `arm_wake.lua`, `ack.lua`, `expire_lease.lua`, `release.lua`, and `delete_sub.lua`. This moves delayed retries and stranded work into a distributed due set so recovery can claim due work instead of redundantly sweeping every subscription on every replica.

**#13: event-triggered recovery plus coarse floor.** Added the pure recovery planner in `webhook/recovery.go` and manager paths for append errors, Redis reconnects, and owner/reconcile loops in `webhook/manager.go`. This keeps the slow floor as a last-resort liveness net while moving normal recovery to targeted events, tightening L3 without making the entire cluster depend on redundant global sweeps.

**#14: leased slot ownership.** Added strongly typed owner identity, ownership slots, epochs, HRW target selection, and timing validation in `webhook/ownership.go`; Redis ownership operations in `webhook/redis_store.go`; script-level owner checks in `webhook/scripts/{claim_shard,check_owner}.lua`; and ownership checkers in `jepsen/checker/model_shard.go` and `check_capstone.go`. This turns T3/L2/L4 from prose into executable checks: one live owner per slot, stale-owner side effects rejected, and dead owners aged out by epoch-bearing leases.

**#15: slot-homed subscription state.** Added 256 slot-homed subscription key tags in `webhook/keys.go`, scatter/gather reads and lazy migration in `webhook/redis_store.go`, occupied-stream-slot fan-out, and S-parallel load specs in `loadgen/sweep/sweep.go` and `loadtest/spec/gate2-fanout-s*-codex.yaml`. This locally covers T5 slot isolation, but the real GKE run found a steady-state migration hot-path regression; same-PR fixes only addressed the concurrent migration replay race and cluster-local owner fence keys, not the performance blocker.

**#16: DR consistency tier capstone.** Added typed consistency tiers A/B/C in `webhook/consistency.go`, Tier B `WAIT 1` plus `WAITAOF 1 1` durability for generation-minting `ArmWake` and `Claim` writes in `webhook/redis_store.go`, config/render plumbing in `webhook/config.go` and `loadgen/experiment/experiment.go`, and the DR/load spec `loadtest/spec/dr-ha-webhook-codex.yaml`. This provides the DR durability shell and capstone checker classifiers, while explicitly not treating Redis `WAIT`/`WAITAOF` as strong consistency.

## 3. Key design decisions & rationale

**Generation/wake fence plus owner epoch layering.** The original `(gen, wake_id)` fence remains the per-subscription correctness boundary: stale acks/releases are no-ops, new claims mint a new generation, and wake IDs bind external side effects to a concrete claim. Owner epochs are layered outside that fence as a cluster-control guard so a dead or displaced owner cannot continue claiming or delivering work for a slot. This keeps protocol safety local to a subscription while giving horizontal ownership a separately typed, revocable authority.

**Claim granularity before slot-homing.** #11 lands first because it fixes the measured 12-replica collapse without depending on the riskier key migration in #15. The trade-off is one more axis in claim state (`ClaimShard` and `ClaimMode`), but it removes the global lease bottleneck and gives later owner slots smaller work units.

**Due-set outbox over pure sweep reduction.** The due set keeps retry and lease-expiry work materialized in Redis, which lets workers claim known-due items and makes recovery event-driven. This adds write amplification around wake lifecycle mutations, but it is bounded, observable, and safer than relying on every replica to rediscover due work by full scans.

**Event-triggered recovery plus a coarse floor.** Reconnects, append failures, and ownership changes now trigger targeted recovery, while a coarse sweep remains as a safety net. The trade-off is more manager paths and more reconciliation states, but the pure `planRecovery`/`decideLeaseReconcile` core keeps the behavior testable without Redis.

**Leased slot ownership with CAS.** HRW ownership assigns target slots from live membership, and Redis CAS scripts mint owner epochs only when the observed lease is still current. The trade-off is eventual convergence rather than instantaneous perfect balance, but stale owners are fenced before side effects and ownership can be checked independently of subscription lease state.

**Slot-homing migration strategy.** The migration approach is rolling and lazy: `fnv32a(subscription_id) % 256` chooses a home slot, all new per-subscription keys use the matching `{__ds:h}` hash tag, legacy records are copied under a migration lock, and a `_slot_migration_complete` marker prevents concurrent replay from clobbering newer slot-homed state. This is safe for mixed-version rollout because old keys remain readable during migration, but the current implementation paid an unsafe performance cost by checking legacy keys in hot steady-state paths after migration completed. I would change that before production: completed slot-homed records should never issue per-operation legacy `EXISTS` checks; legacy cleanup belongs in an explicit background migration/cleanup phase or a low-frequency reconcile path.

**DR tiering, not pretending durability is consistency.** Tier A is the default single-primary behavior, Tier B adds Redis acknowledgement durability for generation-minting writes, and Tier C is reserved for stronger deployment models. The important constraint from the adversarial review is preserved: `WAIT`/`WAITAOF` reduce acknowledged-data-loss windows, but they do not make Redis linearizable across failover.

## 4. Key references (a navigable index)

### Source docs

- `docs/specs/horizontal-scale/research/05-proposed-architecture.md`: Option A source of truth.
- `docs/specs/horizontal-scale/research/06-adversarial-review.md`: three corrections: slot-home whole subscriptions, keep the outbox, and treat `WAIT`/`WAITAOF` as durability only.
- `docs/specs/horizontal-scale/research/07-jepsen-style-verification.md`: executable T/L/C contract and doc-05 gates #1-#6.
- `docs/specs/horizontal-scale/ORCHESTRATION.md`: issue order, worker lifecycle, gate mapping, and stop conditions.
- `README.md`: repository-level usage, server, Redis, and test entry points.
- `docs/adr/0001-lua-scripts-for-atomic-grouped-redis-operations.md`: why Chronicle keeps Redis Lua scripts and mirrors pure Go validation logic.
- `loadtest/README.md`, `loadtest/AGENTS.md`, and `loadtest/RESULTS-codex.md`: cloud rig contract, operational hazards, and this fleet's measured results.

### Executable contract and where it is checked

| Contract / gate | Meaning | Primary checks |
| --- | --- | --- |
| T1 | single-holder lease | `jepsen/checker/model_fence.go`, `scenario_lease.go`, `model_fence_test.go` |
| T2 | cursor monotonicity | `jepsen/checker/check_cursor.go`, `scenario_cursor.go`, `check_cursor_test.go` |
| T3 | ownership exclusivity | `jepsen/checker/model_shard.go`, `check_capstone.go`, `webhook/scripts/claim_shard.lua`, `check_owner.lua` |
| T4 | stale generation no-op | `jepsen/checker/check_stale_generation.go`, `webhook/redis_store_test.go` |
| T5 | slot isolation | `jepsen/checker/check_capstone.go`, `webhook/keys.go`, `webhook/redis_store.go`, `loadgen/sweep/sweep.go` |
| L1 | at-least-once delivery | `jepsen/checker/scenario_baseline_ext.go`, `model_fence.go` |
| L2 | bounded recovery under churn | `jepsen/checker/check_capstone.go`, `webhook/recovery.go` |
| L3 | failover recovery of stranded wake | `jepsen/checker/scenario_baseline_ext.go`, `webhook/recovery.go`, `webhook/manager.go` |
| L4 | ownership convergence | `jepsen/checker/check_capstone.go`, `webhook/ownership.go` |
| L5 | no starvation under churn | `jepsen/checker/check_capstone.go` |
| C1-C3 | claim contention and knee movement | `jepsen/checker/check_contention.go`, `scenario_contention.go`, `webhook/claim_shard.go` |
| Gate #1 | K=10k sweep floor | `loadtest/spec/gate1-replicas-*-codex.yaml`, `loadgen/sweep/sweep.go`, `loadtest/RESULTS-codex.md` |
| Gate #2 | S=2/4/8/256 fan-out p99 | `loadtest/spec/gate2-fanout-s*-codex.yaml`, `loadgen/sweep/sweep.go` |
| Gate #3 | due-set write amplification | `webhook/due_set.go`, `webhook/metrics.go`, `webhook/metrics_test.go` |
| Gate #4 | ownership churn | `jepsen/checker/check_capstone.go`, `webhook/ownership.go` |
| Gate #5 | failover drill | `loadtest/spec/dr-ha-webhook-codex.yaml`, `webhook/consistency.go` |
| Gate #6 | contention floor | `jepsen/checker/check_contention.go`, `loadtest/` contention scaffolding |

### Code anchors

| Mechanism | Anchors |
| --- | --- |
| Metrics append-only surface | `webhook/metrics.go`, `metrics/metrics.go`, `metrics/metrics_test.go` |
| Sharded pull-wake claims | `webhook/claim_shard.go`, `webhook/routes.go`, `webhook/redis_store.go`, `webhook/scripts/{claim,ack,release}.lua` |
| Due-set outbox | `webhook/due_set.go`, `webhook/manager.go`, `webhook/scripts/{claim_due,schedule_retry,record_success}.lua` |
| Recovery planner | `webhook/recovery.go`, `webhook/manager.go`, `webhook/recovery_test.go` |
| Slot ownership | `webhook/ownership.go`, `webhook/scripts/{claim_shard,check_owner}.lua`, `webhook/redis_store.go` |
| Slot-homed keys and migration | `webhook/keys.go`, `webhook/redis_store.go`, `webhook/redis_store_test.go` |
| S-parallel fan-out load | `loadgen/sweep/sweep.go`, `loadtest/spec/gate2-fanout-s*-codex.yaml` |
| DR tiering | `webhook/consistency.go`, `webhook/config.go`, `loadgen/experiment/experiment.go`, `loadtest/spec/dr-ha-webhook-codex.yaml` |

### Commits and PR

- Branch: `adityavkk/epic-hscale-codex`.
- PR: `https://github.com/adityavkk/chronicle/pull/19`.
- Notable commits:
  - `1181a15` `test(jepsen): add hscale contract models`
  - `aab8124` `test(jepsen): wire baseline hscale scenarios`
  - `c5fa022` `feat(metrics): add hscale metric scaffolds`
  - `40067e2` `perf(loadtest): add gate one reporting hooks`
  - `af6e117` `feat(webhook): add sharded pull-wake claims`
  - `c320679` `feat(webhook): add due-set outbox worker`
  - `3d812ae` `feat(webhook): add event-triggered recovery floor`
  - `52281d0` `feat(webhook): add leased slot ownership`
  - `08be300` `feat(webhook): slot-home subscription state`
  - `8ba030a` `Fix concurrent subscription migration replay`
  - `8c06bf5` `Implement DR consistency tier capstone`
  - `e573d5a` `Fix Jepsen slot owner nemesis key`
  - `30ddd1f` `Add cloud fan-out gate specs`
  - `a550b44` `Record cloud gate results`
  - `b94a61e` `Document cloud gate root causes`

## 5. V&V results (honest)

### Local

The final code branch before this writeup passed:

```sh
go test -count=1 -short ./...
go test -race -count=1 -short ./...
go test -count=1 ./webhook ./metrics ./jepsen/checker
(cd loadgen && go test -count=1 ./...)
make lint
make conformance
git diff --check main..HEAD
```

Observed results:

- `go test -count=1 -short ./...`: PASS.
- `go test -race -count=1 -short ./...`: PASS.
- `go test -count=1 ./webhook ./metrics ./jepsen/checker` against Redis: PASS.
- `(cd loadgen && go test -count=1 ./...)`: PASS.
- `make lint`: PASS.
- `make conformance`: PASS on rerun, 332/332 in 46.75s.
- `git diff --check main..HEAD`: PASS.
- Commit trailers checked: no `Co-Authored-By` or generated trailers.
- PR CI for commit `b94a61e`: `lint`, `test`, and `conformance` all PASS.

### Cloud

Full details are in `loadtest/RESULTS-codex.md`.

Environment: project `adityavkk-prototyping` | commit `24afd50` tested | run `2026-06-19 11:18 UTC` | wall `36m`

| Gate | Scenario | Metric | Measured | Budget/SLO | Verdict |
| --- | --- | --- | --- | --- | --- |
| #2 fan-out | S=2 | OnStreamAppend p99 | 4.95 ms; 100/100 appends, 0 errors | p99 < 100 ms; 0 errors | PASS |
| #2 fan-out | S=4 | OnStreamAppend p99 | 4.95 ms; 100/100 appends, 0 errors | p99 < 100 ms; 0 errors | PASS |
| #2 fan-out | S=8 | OnStreamAppend p99 | 4.95 ms; 100/100 appends, 0 errors | p99 < 100 ms; 0 errors | PASS |
| #2 fan-out | S=256 | OnStreamAppend p99 | 9.94 ms from 20 observations; 19/100 appends, 2 errors; sweep p99 2037.8 ms | p99 < 100 ms; 0 errors; sweep p99 < 1500 ms | FAIL |
| baseline | K=10k sweep | sweep p50 / p99 | committed P=5 gate: 16384 / 16384 ms; one-link control: 16384 / 16384 ms | p99 < 1500 ms | FAIL |
| #4 | ownership churn | live churn convergence | not run; required #2 S=256 and K=10k baseline failed first | run after required gates green | NOT RUN |
| #5 | failover drill | RPO / recovery | not run; BASIC Redis used for cost-bounded required gates | STANDARD_HA failover evidence | NOT RUN |
| L2 | required-gate liveness | liveness | not run; stopped after required gate failures | PASS/FAIL | NOT RUN |
| L4 | ownership churn | single-owner under churn | not run; stopped after required gate failures | PASS/FAIL | NOT RUN |
| L5 | combined-nemesis stress | liveness under stress | not run; stopped after required gate failures | PASS/FAIL | NOT RUN |

The real GKE system under test was:

- GKE cluster: `chronicle-hscale-codex`, zone `us-central1-a`, `2 x e2-standard-2`.
- Chronicle: `1` replica, image `us-central1-docker.pkg.dev/adityavkk-prototyping/chronicle-codex/chronicle:codex-24afd50`, metrics enabled.
- Redis: `Memorystore BASIC 1GB`, persistence `none`.
- Load: `sweepscale`, specs `gate2-fanout-s2/4/8/256-codex.yaml`, `gate1-replicas-1-codex.yaml`, and a one-link K=10k floor control.

### Rig gaps and caveats

- S=256 did not produce a valid 100-append p99. The run stopped at 19 successful appends, 2 errors, and 20 metric observations inside a 20s measure window.
- The two exact S=256 append errors are not recoverable from saved artifacts because `appendFanOut` only counted errors; it did not log status, response body, or error text before teardown.
- `chronicle_fanout_seconds` currently measures the occupied-slot index probe in `Manager.OnStreamAppend`, not the full synchronous wake loop that happens before the HTTP append response is returned.
- The K=10k `16384/16384 ms` p50/p99 is partly a histogram-ceiling artifact: the sweep histogram's top finite bucket is 16.384s. The regression is still real because histogram sum/count reported mean sweep ticks around 26s.
- Cloud Monitoring CPU capture failed because this local gcloud install did not expose `gcloud monitoring time-series list`; the CPU JSON files contain an error rather than Redis CPU samples.
- The required cost-bounded run used BASIC Redis. Gate #5 still needs STANDARD_HA or the production-equivalent managed Redis target.

## 6. Open questions & follow-ups

- **Do not ship slot-homing from this PR state.** The migration fast path must be fixed first: completed slot-homed subscriptions should skip legacy-key probes in `GetMany`, `Get`, `ArmWake`, `Claim`, `Ack`, and related hot operations.
- Add a command-count regression test or benchmark that seeds already-migrated slot-homed subscriptions and proves sweep/GetMany stays batched instead of issuing per-id legacy checks.
- Add full append latency and first-error samples to the fan-out load report. The gate should distinguish index-probe latency from end-to-end HTTP append latency.
- Rerun gate #2 at S=2/4/8/256 after the migration fast path is fixed. The observed S=256 index probe was 9.94 ms p99, so the architecture may still be viable, but this run does not prove it.
- Rerun the K=10k sweep floor after the migration fix. The historical GKE baseline in `loadtest/RESULTS-gke.md` was 509 ms p99 for K=10k, one link per subscription; that is the expected recovery target, not a substitute for a new measurement.
- Run #4 ownership churn, #5 failover, L2, L4, and L5 only after the required fan-out and K=10k gates are green.
- For production migration, prefer an explicit background slot migration plus cleanup phase over indefinite lazy checks in request/sweep paths. The safety trade-off is more rollout choreography; the cost win is removing hidden O(K) round trips from steady state.

## 7. Reproduce

### Local suite

Run from the repository root with no global `REDIS_URL` override:

```sh
make redis-up
go test -count=1 -short ./...
go test -race -count=1 -short ./...
go test -count=1 ./webhook ./metrics ./jepsen/checker
(cd loadgen && go test -count=1 ./...)
make lint
make conformance
git diff --check main..HEAD
```

### Cloud gates

Run from an environment allowed to create GKE, Artifact Registry, Memorystore, Cloud Build, Compute, and Monitoring resources in project `adityavkk-prototyping`. Keep every resource suffixed with `-codex`, and keep the teardown trap installed until `down` has completed.

```sh
cd loadtest

export LT_PROJECT=adityavkk-prototyping
export LT_CLUSTER=chronicle-hscale-codex
export LT_AR_REPO=chronicle-codex
export LT_ZONE=us-central1-a
export LT_REGION=us-central1
export LT_MACHINE=e2-standard-2
export LT_DISK_GB=30
export LT_REDIS_SIZE_GB=1
export LT_REDIS_TIER=basic
export LT_REDIS_VERSION=redis_7_2
export LT_TAG=codex-$(git rev-parse --short HEAD)

trap './ltctl.sh --project "$LT_PROJECT" down' EXIT INT TERM

./ltctl.sh --project "$LT_PROJECT" up
./ltctl.sh --project "$LT_PROJECT" run spec/gate2-fanout-s2-codex.yaml
./ltctl.sh --project "$LT_PROJECT" run spec/gate2-fanout-s4-codex.yaml
./ltctl.sh --project "$LT_PROJECT" run spec/gate2-fanout-s8-codex.yaml
./ltctl.sh --project "$LT_PROJECT" run spec/gate2-fanout-s256-codex.yaml
./ltctl.sh --project "$LT_PROJECT" run spec/gate1-replicas-1-codex.yaml
./ltctl.sh --project "$LT_PROJECT" down

trap - EXIT INT TERM
```

For the DR/failover path, use a STANDARD_HA Redis target and the DR spec after the required gates are green:

```sh
cd loadtest
export LT_PROJECT=adityavkk-prototyping
export LT_CLUSTER=chronicle-hscale-codex
export LT_AR_REPO=chronicle-codex
export LT_REDIS_TIER=standard
export LT_REDIS_SIZE_GB=5
trap './ltctl.sh --project "$LT_PROJECT" down' EXIT INT TERM
./ltctl.sh --project "$LT_PROJECT" up
./ltctl.sh --project "$LT_PROJECT" run spec/dr-ha-webhook-codex.yaml
./ltctl.sh --project "$LT_PROJECT" down
trap - EXIT INT TERM
```

Outputs land under `loadtest/out/reports/*` and should be summarized into `loadtest/RESULTS-codex.md`.

After any cloud run, verify no Codex resources are left running:

```sh
gcloud container clusters list \
  --project adityavkk-prototyping \
  --filter='name~codex' \
  --format='table(name,location,status)'

gcloud redis instances list \
  --project adityavkk-prototyping \
  --region us-central1 \
  --filter='name~codex' \
  --format='table(name,tier,memorySizeGb,state)'
```
