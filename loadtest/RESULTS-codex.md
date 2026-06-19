# Results — Codex gate runs for horizontal-scale #9

## Cloud V&V — real GKE run
Environment: project adityavkk-prototyping | commit `24afd50` tested (`codex-24afd50`; later PR commits add harness-only fixes and this results record) | run 2026-06-19 11:18 UTC | wall 36m
### SUT (System Under Test)
- GKE cluster: `chronicle-hscale-codex` | zone `us-central1-a` | `2` x `e2-standard-2`
- Chronicle: `1` replica | image `us-central1-docker.pkg.dev/adityavkk-prototyping/chronicle-codex/chronicle:codex-24afd50` | metrics enabled
- Redis: `Memorystore BASIC 1GB` | persistence `none`
- Load: sweepscale | scenarios `spec/gate2-fanout-s2-codex.yaml`, `spec/gate2-fanout-s4-codex.yaml`, `spec/gate2-fanout-s8-codex.yaml`, `spec/gate2-fanout-s256-codex.yaml`, `spec/gate1-replicas-1-codex.yaml`, `/tmp/sweep-10k-floor-codex.yaml`
### Gate results (REAL numbers — no PENDING rows)
| Gate | Scenario | Metric | Measured | Budget/SLO | Verdict |
|------|----------|--------|----------|-----------|---------|
| #2 fan-out | S=2 | OnStreamAppend p99 (ms) | 4.95 ms; 100/100 appends, 0 errors | p99 < 100 ms; 0 errors | PASS |
| #2 fan-out | S=4 | p99 (ms) | 4.95 ms; 100/100 appends, 0 errors | p99 < 100 ms; 0 errors | PASS |
| #2 fan-out | S=8 | p99 (ms) | 4.95 ms; 100/100 appends, 0 errors | p99 < 100 ms; 0 errors | PASS |
| #2 fan-out | S=256 | p99 (ms) | 9.94 ms from 20 observations; 19/100 appends, 2 errors; sweep p99 2037.8 ms | p99 < 100 ms; 0 errors; sweep p99 < 1500 ms | FAIL |
| baseline | K=10k sweep | sweep p50 / p99 (ms) | committed P=5 gate: 16384 / 16384; one-link control: 16384 / 16384 | p99 < 1500 | FAIL |
| #4 | ownership churn | live churn convergence | not run; required #2 S=256 and K=10k baseline failed first | run only after required gates are green | NOT RUN |
| #5 | failover drill | RPO / recovery (s) | not run; BASIC Redis used for cost-bounded required gates | STANDARD_HA failover evidence | NOT RUN |
| L2 | required-gate liveness | liveness | not run; stopped after required gate failures | PASS/FAIL | NOT RUN |
| L4 | ownership churn | single-owner under churn | not run; stopped after required gate failures | PASS/FAIL | NOT RUN |
| L5 | combined-nemesis stress | liveness under stress | not run; stopped after required gate failures | PASS/FAIL | NOT RUN |
### #15 slot-homing decision
DEFER — S=256 kept sub-10 ms p99 for the samples it completed, but it failed the zero-error fan-out gate (`19/100` appends, `2` errors) and breached the sweep floor (`2037.8 ms` p99).
### Teardown confirmation
clusters = none, Memorystore = none, $0 ongoing.

Notes:

- Exact teardown checks after the run returned `chronicle-hscale-codex: none` and `chronicle-hscale-codex-redis: none`.
- The K=10k regression floor failed even on the one-link historical control (`seeded 10000/10000`, sweep mean `26183.9 ms`, p50/p99 `16384/16384 ms`, `2` ticks over `40s`), so this is a real blocker.
- Cloud Monitoring CPU capture did not work in this local gcloud install: `gcloud monitoring time-series list` is not available, so `*-redis-cpu.json` contains `{"error":"gcloud monitoring time-series list failed"}`.
- The run exposed harness hazards that are now fixed in this branch: per-cluster `LT_KUBECONFIG`, prompt failed-job detection, Job metadata labels, and Recreate SUT rollouts for one-node test clusters.

### Root-cause analysis — 2026-06-19 follow-up

#### Failure 1: Gate #2 fan-out S=256

Classification: **REAL Chronicle hot-path defect in the current branch, plus a rig observability gap**.

Evidence:

- The S=2/S=4/S=8 reports all completed `100/100` appends with `0` errors and fan-out p99 `4.95 ms`. The S=8 report, for example, records `fanout_appended: 100`, `fanout_append_errors: 0`, `fanout_count: 100`, `mean_fanout_slots_probed: 8`, and `mean_fanout_subscribers: 8`.
- The S=256 pod log showed `seeded 256/256`, `fanout stream "loadtest/fanout/s256" occupied_slots=256 | appends 19/100 errors=2 | fanout count=20 mean=6.70ms p50=7.06ms p99=9.94ms | slots=256.0 subs=256.0`, followed by `SLO FAIL: fanout append errors 2`.
- The two exact client errors are **not recoverable from the saved artifacts**. `appendFanOut` only increments `errs` when `cli.Append` returns an error or HTTP status `>=400`; it does not log the error, status, or response body. The failed pod was deleted during teardown before richer diagnostics were collected. Given the code cancels the append context at the end of the 20s measure window, the most likely class is request context cancellation or an HTTP response arriving after cancellation, but the exact text cannot be pasted from existing logs.
- The run was cut off by the 20s measure window: `appendFanOut` is sequential and stops when its context is canceled. With only `19` successes, `2` errors, and `20` observed `chronicle_fanout_seconds` samples, the p99 from 20 observations is **not a valid 100-append gate result**.
- The reported `chronicle_fanout_seconds` p99 is not full HTTP append latency. `Manager.OnStreamAppend` observes `FanOut` immediately after `store.StreamSubscribers`, then loops through every returned subscription and calls `maybeWake`. `handler.handleAppend` calls `h.onStreamAppend(path)` before sending response headers, so the loadgen append completion includes the whole synchronous wake loop, while the metric only covers the index probe.
- At S=256, `RedisStore.StreamSubscribers` probes all occupied stream slots with one pipelined `SMEMBERS` per slot plus the legacy set. That index probe was still sub-10 ms in the measured sample, so the occupied-slot index itself is not the observed cliff.
- The expensive part is after the index probe: `OnStreamAppend` synchronously calls `maybeWake` for every subscriber. `maybeWake` calls `store.Get`, and `Get` calls `migrateSubscription`. For already slot-homed subscriptions, `migrateSubscription` still does a migration-complete `HGET` and then an `EXISTS` against the old legacy key before returning. First wake also calls `ArmWake`, which repeats `migrateSubscription`. This turns one S=256 append into hundreds of per-subscription Redis round trips before the HTTP response.

Root cause:

The S=256 failure is not primarily a cluster-sizing artifact. The small GKE shape exposed it, but the current Chronicle hot path is doing synchronous per-subscriber work that includes unnecessary lazy-migration checks on already slot-homed subscriptions. The rig also under-reported the true append cost because it timed only `StreamSubscribers` and discarded append error details.

Fix:

- Chronicle fix: make `migrateSubscription` a true fast path for completed slot-homed subscriptions. Once `slotMigrationCompleteField` is present, return without checking the legacy key on every per-subscription operation; move legacy cleanup to `List`, explicit migration/reconcile, or a low-frequency cleanup path. Then audit `GetMany`, `Get`, and `ArmWake` so steady-state slot-homed operations do not pay legacy migration round trips.
- Chronicle fix: either make the append-time wake loop bounded/asynchronous through the due-set path, or at least add a full `OnStreamAppend`/append-handler latency metric in addition to the existing index-probe metric.
- Harness fix: record append latency and the first few append error samples/status codes in the sweepscale report. The current `fanout_append_errors` count is insufficient for root cause once pods are torn down.

Gate expectation:

The occupied-slot index portion likely passes after the metric is scoped correctly: the observed S=256 index-probe p99 was `9.94 ms`. The full `OnStreamAppend` gate should **not** be marked green from this run; after the migration fast path and metric/logging fixes, rerun S=256 before deciding SHIP. Until then, slot-homing should remain **DEFER for this PR state**, but the failure points to a fixable implementation/harness issue rather than proof that slot-homing is architecturally infeasible.

#### Failure 2: K=10k sweep baseline

Classification: **REAL Chronicle steady-state regression in the current branch; the identical p50/p99 value is a histogram-ceiling artifact, not the root cause**.

Evidence:

- The committed K=10k/P=5 report records `seeded 10000/10000`, `sweep_ticks: 5`, `sweep_mean_ms: 26368.402570800004`, `sweep_p50_ms: 16384`, `sweep_p99_ms: 16384`, `mean_subs_evaluated: 10000`, and `mean_tails_batched: 50000`.
- A one-link historical control run, captured from the failed pod log before teardown, also seeded `10000/10000` and reported `sweep tick over 40s: mean=26183.9ms p50=16384.0ms p99=16384.0ms | subs/tick=10000 tails/tick=10000 | 2 ticks`.
- `16384` comes from the Prometheus histogram bucket definition, not from a timeout or parse fallback. `chronicle_sweep_tick_seconds` uses `prometheus.ExponentialBuckets(0.0005, 2, 16)`, whose top finite bucket is `0.0005 * 2^15 = 16.384s`. The loadgen quantile code returns the previous finite bucket when the quantile falls in `+Inf`, so any sweep tick above the top finite bucket can report p50/p99 as exactly `16384 ms`.
- The mean is computed from histogram sum/count, not bucket interpolation. A `26.18s` mean with non-zero tick count confirms the SUT recorded genuinely slow sweep ticks; this is not a missing-metric parse fallback.
- The regression mechanism is visible in the code. `sweepOnce` still calls `store.GetMany(ids)` intending a batched read. `RedisStore.GetMany` chunks the HGETALL pipeline, but first loops every id and calls `migrateSubscription(id)`. For already slot-homed records, `migrateSubscription` checks `slotMigrationCompleteField` and then performs an `EXISTS` against the old legacy subscription key before returning. That serial per-subscription migration check reintroduces O(K) round trips before the batched reads.

Root cause:

The K=10k failure is a real slot-homing implementation regression: lazy migration was inserted into hot steady-state paths in a way that defeats the batched sweep. The round `16384` p50/p99 only shows that the histogram bucket range is too low to distinguish 16s from 26s ticks; the underlying `sweep_mean_ms` proves the sweep itself was slow.

Fix:

- Same primary Chronicle fix as above: completed slot-homed subscriptions must not check legacy keys on every `GetMany`, `Get`, `ArmWake`, `Claim`, `Ack`, etc. Legacy cleanup belongs in a cold path.
- Add a regression test or benchmark that seeds slot-homed subscriptions with migration-complete markers and asserts `GetMany`/sweep does not perform per-id legacy checks in steady state. A Redis hook or fake store can count commands without requiring cloud.
- Expand the sweep duration histogram buckets or have the loadgen report `+Inf`/overflow counts so future results cannot collapse all over-budget ticks to `16384 ms`.

Gate expectation:

After removing the steady-state migration round trips, the K=10k one-link control is expected to return near the historical GKE baseline (`509 ms` p99 in `loadtest/RESULTS-gke.md`) and should likely pass. That expectation is code-based, not a substitute for the gate: rerun the K=10k floor after the fix before marking the epic green.

This file records the #10 worker's load-gate attempt. Numbers are recorded only
when measured; blocked runs list the exact command and blocker.

## Gate #1 — 2026-06-18, blocked before provisioning

Goal: run K=10k at SUT replicas 1, 2, and 4, collect the SUT sweep p99 and
Memorystore CPU (`redis.googleapis.com/stats/cpu_utilization`) to confirm the
`O(N*K)` redundancy premise.

Preflight command run:

```sh
gcloud compute regions describe us-central1 --flatten=quotas \
  --format="csv[no-heading](quotas.metric,quotas.limit,quotas.usage)" \
  | grep -E "^(CPUS|E2_CPUS|N2_CPUS|SSD_TOTAL_GB),"
```

Result:

```text
ERROR: (gcloud.compute.regions.describe) Could not fetch resource:
 - Request is prohibited by organization's policy. vpcServiceControlsUniqueIdentifier: 83zaZ3n3mh1mCrYlZb-QOWcV6-4_zUoB4QV99O2-XAQbM4xwrLhJUGrbSIwIuRfhI_OoprK4vRWnB5I
```

No cloud resources were created. Because the blocker occurs before quota
inspection, I did not attempt `make up` or any provisioning command.

## Coordinator Runbook

Run from an environment allowed to call GCP Compute, Container, Redis, Cloud
Build, Artifact Registry, and Monitoring APIs:

```sh
cd loadtest
trap 'make down' EXIT INT TERM
make up
make run SPEC=spec/gate1-replicas-1-codex.yaml
make run SPEC=spec/gate1-replicas-2-codex.yaml
make run SPEC=spec/gate1-replicas-4-codex.yaml
make down
trap - EXIT INT TERM
```

The rig defaults suffix external resources with `-codex`:

| resource | default |
| --- | --- |
| GKE cluster / namespace | `chronicle-loadtest-codex` |
| Artifact Registry repo | `chronicle-codex` |
| Redis instance | `chronicle-loadtest-codex-redis` |

Expected report paths:

```text
loadtest/out/reports/gate1-replicas-1-codex-<timestamp>-sweepscale.log
loadtest/out/reports/gate1-replicas-1-codex-<timestamp>-redis-cpu.json
loadtest/out/reports/gate1-replicas-2-codex-<timestamp>-sweepscale.log
loadtest/out/reports/gate1-replicas-2-codex-<timestamp>-redis-cpu.json
loadtest/out/reports/gate1-replicas-4-codex-<timestamp>-sweepscale.log
loadtest/out/reports/gate1-replicas-4-codex-<timestamp>-redis-cpu.json
```

Pass criteria remain:

| Gate | Pass criterion |
| --- | --- |
| K=10k sweep floor | sweep p99 `< 1500 ms` |
| `O(N*K)` premise | managed Redis CPU increases with replicas at fixed K |

If Cloud Monitoring returns zero CPU samples, keep the sweepscale result and
rerun only the Monitoring query over the same window after the metric ingestion
lag clears.

## Gate #2-#5 / DR capstone attempt — 2026-06-18, blocked before provisioning

Goal: run the #16 active-passive DR/load path with `sut.replicas >= 2`,
`consistency_tier: B`, dispatch `webhook`, and a STANDARD_HA / managed Redis 8
endpoint; record fan-out p99 at S=2/4/8/256, due-set write amplification, churn
ownership convergence, and real failover RPO/RTO.

Local validation completed:

```sh
cd /Users/auk000v/orca/workspaces/chronicle/hs-16-dr-capstone/loadgen
go test -count=1 ./...
go run ./cmd/render -spec ../loadtest/spec/dr-ha-webhook-codex.yaml \
  -image example.com/chronicle:codex \
  -loadgen-image example.com/chronicle-loadgen:codex \
  -redis-url redis://standard-ha-codex.example:6379/0
```

Render evidence from the generated manifest:

```text
namespace: chronicle-loadtest-codex
replicas: 2
CHRONICLE_REDIS_URL=redis://standard-ha-codex.example:6379/0
CHRONICLE_CONSISTENCY_TIER=B
-webhook-url=http://webhook-receiver-codex.chronicle-loadtest-codex.svc.cluster.local/hook
cluster_name = "chronicle-loadtest-codex"
```

The new spec is `loadtest/spec/dr-ha-webhook-codex.yaml`. It is a renderable
STANDARD_HA/Tier B webhook variant, but it still requires an in-cluster
`webhook-receiver-codex` service and a real managed Redis endpoint before it is a
load result.

Cloud preflight commands run:

```sh
gcloud compute regions describe us-central1 --flatten=quotas \
  --format="csv[no-heading](quotas.metric,quotas.limit,quotas.usage)"

gcloud redis instances list --region=us-central1 \
  --format="table(name,tier,memorySizeGb,redisVersion,host)"
```

Results:

```text
ERROR: (gcloud.compute.regions.describe) Could not fetch resource:
 - Request is prohibited by organization's policy. vpcServiceControlsUniqueIdentifier: J6OZ1osBAlaRnbh5admsDmQq4dVPl9nCgUI4StWAyLr9AMW2Oi6rsT_vY7wvK8mJnqz_Uwfwz4dCxGA

ERROR: (gcloud.redis.instances.list) PERMISSION_DENIED: Google Cloud Memorystore for Redis API has not been used in project adityavkk-prototyping before or it is disabled.
reason: SERVICE_DISABLED
service: redis.googleapis.com
```

No cloud resources were created.

Gate status:

| Gate | Status |
| --- | --- |
| K=10k regression floor | **NO NEW MEASUREMENT**. The last real GKE baseline remains `loadtest/RESULTS-gke.md`: K=10k pull-wake, 1 replica, 1 link/sub, sweep p99 **509 ms**. |
| Gate #2 fan-out p99 S=2/4/8/256 | **BLOCKED** by GCP policy/API access before provisioning. |
| Gate #3 due-set write amplification | **LOCAL ONLY** through unit/integration tests; no load number recorded. |
| Gate #4 churn window | **LOCAL ONLY** through ownership/reconcile tests; no live churn window recorded. |
| Gate #5 real failover fence drill | **BLOCKED**. No STANDARD_HA / managed Redis 8 failover was run, so no RPO/RTO is recorded. The single Redis Recreate harness must not be treated as this gate. |
| Stress/load dispatch:webhook S=256 replicas>=2 | **BLOCKED** by the same cloud preflight failures. |

Runbook once GCP access is available:

```sh
cd loadtest
trap 'make down' EXIT INT TERM
LT_REDIS_TIER=standard LT_REDIS_SIZE_GB=5 make up
make run SPEC=spec/dr-ha-webhook-codex.yaml
make down
trap - EXIT INT TERM
```

For a production managed Redis 8 endpoint, pass it through the render/ltctl
`-redis-url` path rather than editing the spec. Keep every resource name on the
`-codex` defaults and tear down immediately after the run.
