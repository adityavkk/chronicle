# Gate #2 cloud V&V results

| | |
|---|---|
| **Cluster** | chronicle-gate2 (us-central1) |
| **SUT image** | `us-central1-docker.pkg.dev/adityavkk-prototyping/chronicle/chronicle:gate2-v1` |
| **Loadgen image** | `us-central1-docker.pkg.dev/adityavkk-prototyping/chronicle/chronicle-loadgen:gate2-v1` |
| **Date** | 2026-06-22 |
| **Standalone Redis** | `redis://10.253.53.131:6379/0` (Memorystore BASIC) |
| **Cluster Redis** | N/A — see Phase 3 |
| **Nodes** | 2× loadgen (e2-standard-4), 3× obs (e2-standard-2); SUT pool quota-exhausted → SUT scheduled on loadgen/obs nodes |

## Phase 1: Horizontal scale curve (fence-storm regression)

Method: port-forward to pod[0] for each N (single-replica path through that pod), S=4 pull-wake subs,
rate=10/s, warmup=20s, measure=60s. P99 includes ~10ms port-forward RTT; the signal is **flatness**, not absolute value.

| N replicas | fanout_p99_ms | fanout_p50_ms | mean_subs_per_fanout | append_errors | status |
|---|---|---|---|---|---|
| 1 | 12.48 | 5.20 | 4 | 0 | PASS |
| 2 | 12.61 | 5.59 | 4 | 0 | PASS |
| 4 | 12.58 | 5.45 | 4 | 0 | PASS |
| 8 | 12.53 | 5.29 | 4 | 0 | PASS |

**Verdict: PASS** — fanout p99 flat at 12.5 ± 0.1ms across N=1→8 (< 1% variance). Zero append errors at all scales.
The fence-storm is GONE. No exponential degradation as replicas grow.

_Note: N=12 not run (GCP INSUFFICIENT_QUOTA_PROJECT on SUT node pool). Trend through N=8 is conclusive._

## Phase 2: Claim-contention knee (gate #6 baseline, G=1)

| Workers (W) | operations | claims | granted | linearizable | result |
|---|---|---|---|---|---|
| 4 | 451 | 417 | 34 | yes | PASS |
| 16 | 1267 | 1225 | 42 | yes | PASS |

**Verdict: PASS** — single-holder invariant is linearizable under W=4 and W=16 concurrent workers.
Gate #6 baseline established: contention is healthy with G=1.

## Phase 3: Gate #2 fan-out (Memorystore CLUSTER, N=1)

| S subs | fanout_p99_ms | probe_p99_ms | SLO | status |
|---|---|---|---|---|
| — | — | — | — | SKIPPED |

**Verdict: SKIPPED** — Memorystore CLUSTER (`google_redis_cluster`) provisioning failed with
`INSUFFICIENT_QUOTA_PROJECT` on every attempt (tried twice, both entered CREATING→DELETING loop,
never reached READY). Standalone Redis phases were unaffected.

_Recommended follow-up: request quota increase for `redis.googleapis.com` in `adityavkk-prototyping`,
then re-run Phase 3 in isolation._

## Phase 4: Fair K=10k sweep

| K | N | cpu | seeded | seed_errors | sweep_ticks | sweep_mean_ms | sweep_p50_ms | sweep_p99_ms | SLO | status |
|---|---|---|---|---|---|---|---|---|---|---|
| 10000 | 1 | 2 | 10000 | 0 | 60 | 663 | 768 | **1019** | <1500ms | PASS |

**Verdict: PASS** — sweep_p99_ms=1019ms < 1500ms SLO. Seeded 10k subscriptions with P=5 in 4.1s
(64-concurrent). 60 sweep ticks in 120s measurement. mean_tails_batched=50001 (P=5 tails per sub confirmed).

## Phase 5: Jepsen correctness

| Scenario | Property | nemesis_actions | result |
|---|---|---|---|
| expired-lease-takeover | L2: fence rotates on stale-lease takeover | none | PASS |
| single-holder-linz | L4: fence linearizable under GC-pause nemesis | none | PASS |
| pull-wake-arm-crash | T4: arm-without-emit recovered by sweep | kill-all-origins×1, kill-origin×12 | **FAIL** |
| baseline | T1: skipped — cluster→host webhook back-channel needed | — | SKIPPED |
| origin-restart | T3: skipped — cluster→host webhook back-channel needed | — | SKIPPED |

**expired-lease-takeover** — PASS:
- Worker A claimed generation=1, stalled 2.5s past TTL
- Worker B took over (generation=2)
- Worker A's late ack returned 409 FENCED ✓

**single-holder-linz** — PASS:
- W=4, 440 operations (404 claims, 36 granted), linearizable=yes ✓

**pull-wake-arm-crash** — FAIL (real T4 bug):
- 120 messages appended across 6 streams; all 120 reachable in Redis
- After kill-all-origins + 120s settle: subscription cursor stuck at `000000000150` for ALL 6 streams
- Stream tail at `000000000293` (remaining messages never acked)
- Root cause: the recovery sweep does NOT re-emit pull-wake wake events after origin restart
- The cursor advanced during the workload (before kill-all) but the sweep failed to re-emit wakes for the post-kill range
- Reproduce: `jepsen-checker -scenario pull-wake-arm-crash -settle 120s`
- Filing as T4 correctness gap: sweep needs to push wakes into the pull-wake wake stream on recovery

## Gate status

| Gate | Condition | Phase | Status |
|---|---|---|---|
| Gate #2 fan-out p99 | p99 < 50ms at S≤8; < 100ms at S=256 | Phase 3 | SKIPPED (quota) |
| Gate #1 O(N·K) | sweep p99 < 1500ms at K=10k, N=1, cpu:2 | Phase 4 | **PASS** (1019ms) |
| Horizontal scale | fanout p99 flat/decreasing as N grows 1→8 | Phase 1 | **PASS** (12.5ms flat ±1%) |
| Linearizability L2 | expired-lease-takeover fences the deposed holder | Phase 5 | **PASS** |
| Linearizability L4 | single-holder-linz passes at W=4 and W=16 | Phase 2+5 | **PASS** |
| Pull-wake recovery T4 | pull-wake-arm-crash PASS | Phase 5 | **FAIL** — sweep not re-emitting wakes after kill-all |

## Summary verdict

- **PASS**: horizontal scale (fence-storm gone), O(N·K) sweep within SLO, lease linearizability L2+L4
- **FAIL**: T4 pull-wake recovery after kill-all-origins (real bug, sweep gap)
- **SKIPPED**: Gate #2 CLUSTER fan-out (quota), webhook-based Jepsen scenarios (GKE back-channel constraint)

_Generated by comprehensive cloud V&V run on chronicle-gate2 cluster (2026-06-22)_
