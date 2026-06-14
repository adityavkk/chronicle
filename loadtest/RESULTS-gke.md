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

## Next

- Sweep K (1k → 100k) for the full curve; raise `sut.replicas` and read
  Memorystore CPU to quantify the `O(N·K)` redundancy.
- Bigger SUT + HA Redis to push the `{__ds}`-slot ceiling.
- `dispatch: webhook` with a receiver to add append→delivery latency.
