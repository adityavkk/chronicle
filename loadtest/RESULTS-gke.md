# Results — chronicle sweep-scale on GKE + managed Redis

First real-cloud run of the rig: the instrumented SUT on GKE, driven by the
in-cluster `sweepscale` Job, measured through `chronicle_sweep_*` — not a local
microbenchmark. It is the "a live run finds what local validation can't" check.

## Run 1 — 2026-06-14, sweep-smoke (K=10k)

**Setup.** GKE zonal cluster `chronicle-loadtest` in `us-central1-a`: a `role=sut`
pool (1× `e2-standard-2`) and a `role=loadgen` pool (1× `e2-standard-2`),
`pd-standard` 50 GB disks. Managed Redis: Memorystore for Redis (basic tier,
1 GB, `REDIS_7_2`, `maxmemory-policy=noeviction`). Images built amd64 in Cloud
Build → Artifact Registry. chronicle: 1 replica, `--sweep-interval=2s`,
`--sweep-batch=0`, 1 CPU / 1 Gi.

**Workload.** 10,000 pull-wake subscriptions, 1 link each; 20 s warmup, 40 s
measure window.

| metric | value |
| --- | --- |
| seeded | 10,000 / 10,000 in 4.3 s, 0 errors |
| sweep tick mean | **316 ms** |
| sweep tick p50 | 384 ms |
| sweep tick p99 | **509 ms** |
| subs / tick | 10,000 |
| tails / tick | 10,000 |
| ticks sampled | 20 (over 40 s) |
| SLO (`p99 < 1500 ms`) | **PASS** (Job exit 0) |

**Reading it.** The batched sweep holds at K=10k **well under the 2 s interval**
on real infrastructure. The cloud tick (~316 ms mean) is ~2.5× the local
microbench at the same K (~122 ms): the difference is the VPC round-trip latency
to managed Redis (vs loopback) plus a small `e2-standard-2` SUT. Per-tick the
sweep batches ~`⌈10000/512⌉ + ⌈10000/512⌉ ≈ 40` pipelined round trips, so the
cost is Redis-throughput- and RTT-bound, exactly as designed — not the
per-link-round-trip cliff the pre-batching sweep would have hit (that form would
have been ~20,000 sequential round trips, seconds per tick).

**What this validated end to end.** Cloud Build amd64 images (no QEMU); chronicle
reaching Memorystore from a VPC-native pod (`/readyz` green, no eviction warning);
kubectl direct to the control plane (Connect Gateway not needed on this network);
the in-cluster Job seeding + scraping the SUT; and the SLO gate as a pass/fail.

**Cost / teardown.** Cluster + Memorystore deleted immediately after the run
(Artifact Registry images kept for re-runs). Total spend for the ~25-minute
provision-run-destroy cycle was well under $1.

## Gate #1 — O(N·K) premise (ramp replicas 1→4, read Memorystore CPU) — TO RUN

This is the rig extension issue #10 adds; **it has not been run** (the orchestrator
owns all cloud campaigns — the worktree builds the rig + spec + command only, never
spends on GKE). The hypothesis: at a **fixed K=10k**, every replica runs the
recovery sweep over all K identically, so the control-plane `{__ds}` slot's
**Redis CPU grows ~N×** while the per-replica `chronicle_sweep_*` histogram stays
flat. That rising Redis CPU is the `O(N·K)` redundancy the epic exists to relieve
(the inverse number — CPU flat as N rises after work-sharding — lands in #14's
gate #4).

**Exact command** (after authenticating gcloud — `! gcloud auth login`):

```sh
cd loadtest
make up                                 # provision cluster + Memorystore + images (once)
make gate1                              # ramp replicas 1→4 at K=10k, read Memorystore CPU
make down                               # ALWAYS tear down — stop the meter
```

`make gate1` (≡ `./ltctl.sh gate1 spec/sweep-10k-scale.yaml`) renders the
[`spec/sweep-10k-scale.yaml`](spec/sweep-10k-scale.yaml) variant at replicas
`1,2,3,4` (via `render -replicas N`), deploys the SUT, runs the SLO-gated
`sweepscale` Job at each N, then reads the Memorystore CPU over the just-finished
measure window with the new `rediscpu` reader
(`loadgen/cmd/rediscpu`, backed by `loadgen/redismon` → Cloud Monitoring
`redis.googleapis.com/stats/cpu_utilization`). It writes a CPU-vs-N table to
`gate1-results.tsv`:

```
N   sweep_p99_ms   redis_cpu_max   redis_cpu_mean
1   ...            ...             ...
2   ...            ...             ...
3   ...            ...             ...
4   ...            ...             ...
```

**Pass:** `redis_cpu_*` scales with N at fixed K (confirms the premise) **AND**
the K=10k sweep p99 stays under the 1500 ms SLO at every N (the
[Run 1](#run-1--2026-06-14-sweep-smoke-k10k) 509 ms floor reproduces — every
replica's sweep tick still fits the interval; only the shared Redis pays the
redundancy). Read CPU directly from Cloud Monitoring / the Memorystore console as
a cross-check; the chronicle sweep histogram is per-replica, so it cannot show the
multi-replica effect (it's read at `replicas: 1`).

## Gate #4 — membership churn window (the inverse of gate #1) — TO RUN

The rig extension issue #14 adds; **it has not been run** (the orchestrator owns
all cloud campaigns — the worktree builds the rig + spec + command only). It is the
**inverse** of gate #1: with work-sharded leased slot ownership, run the SUT at
**replicas≥2** and pod-kill the slot owner mid-window. The hypothesis: total
background work is now **O(total owed) regardless of N** (only the slot owner runs
the lease/retry/due workers, so Redis CPU does NOT scale with N the way gate #1's
full-sweep redundancy does), and a rebalance coverage gap recovers at the
takeover **trigger** (the new owner's `claim_shard` CAS + eager reconcile) within
**membership-lease TTL + RTT (~9 s)** — NOT a sweep tick — with **zero lost wakes**
and **zero double-grants**.

**Exact command** (after authenticating gcloud — `! gcloud auth login`):

```sh
cd loadtest
./ltctl.sh up                                 # provision cluster + Memorystore + images (once)
./ltctl.sh gate4 spec/sweep-10k-churn.yaml    # deploy >=2 replicas, run the job, kill the slot owner mid-window
./ltctl.sh down                               # ALWAYS tear down — stop the meter
```

`gate4` renders [`spec/sweep-10k-churn.yaml`](spec/sweep-10k-churn.yaml) at
`replicas: 2`, launches the SLO-gated `sweepscale` Job, sleeps ~45 s for the
workload to warm up and ownership to settle, then reads `owner_id` from
`ds:{ownership}:slot:0` and force-deletes that owner's pod (the `killSlotOwner`
nemesis; falls back to the coarse `chaos` pod-kill if the owner pod cannot be
resolved). After the job completes it scrapes the ownership metrics from a
surviving pod's `/metrics`.

**Pass:** `chronicle_coverage_gap_seconds` p99 ≤ membership-lease TTL + RTT (~9 s,
NOT the 30 s floor); the `sweepscale` tail check shows every appended message
delivered (**zero lost wakes**); `chronicle_slot_ownership_events_total` never
shows two live owners for one slot (**zero double-grants** — confirmed against the
L4 ownership-timeline sampler); `chronicle_owner_fenced_total` rises at the
takeover (the deposed owner is fenced); and the K=10k sweep p99 stays under the
1500 ms SLO (the [Run 1](#run-1--2026-06-14-sweep-smoke-k10k) 509 ms floor
reproduces). T3 (the ownership-CAS linearizability gate) is GREEN locally already
(see `docs/jepsen/results.md`); gate #4 is its at-scale rig form.

## Other rig extensions (issue #10)

- **Chaos / pod-kill** — `make chaos` (≡ `./ltctl.sh chaos`) force-deletes the SUT
  pods mid-run (the rig analogue of the jepsen nemesis; `loadgen/chaos` is the
  unit-tested command builder). Used to observe the membership churn window at
  scale once leased ownership lands (#14, gate #4 / 07 L2/L4).
- **New SUT golden signals** — `chronicle_fanout_seconds` (gate #2),
  `chronicle_due_set_mutations_total` / `chronicle_due_worker_tick_seconds`
  (gate #3, wired in #12), `chronicle_slot_ownership_events_total` /
  `chronicle_coverage_gap_seconds` / `chronicle_owner_fenced_total` (gate #4/#5,
  **wired in #14** — `slot_ownership` per `claim_shard` CAS, `coverage_gap` at the
  backstop sweep, `owner_fenced` at `check_owner`/the inlined checks). Cross-read
  them from the SUT `/metrics` during `gate4`; `fanout` stays a `NopMetrics` no-op
  until #15 wires `OnStreamAppend`.

## Next

- Sweep K (1k → 100k) for the full curve.
- Bigger SUT + HA Redis to push the `{__ds}`-slot ceiling.
- `dispatch: webhook` with a receiver to add append→delivery latency.
