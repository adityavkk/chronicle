# Chronicle load-test rig

An on-demand rig for finding the subscription layer's real fault lines on a
Kubernetes cluster with managed Redis 8 — not a single laptop, where the SUT,
Redis, and load generator fight over one set of cores and loopback erases the
round-trip latency the sweep is bound by.

One **declarative experiment spec** (`spec/*.yaml`) pins the system under test
(image, flags, replicas, resources, Redis) plus the workload and the SLOs, and
renders to the Kubernetes manifests and Terraform vars for one reproducible run.

## Pieces

| Path | What |
| --- | --- |
| `terraform/` | GKE cluster + managed Redis 8 (Memorystore) + three isolated node pools (`sut`, `loadgen`, `obs`). Ephemeral: apply → run → destroy. |
| `spec/` | The declarative experiment specs. |
| `../loadgen/cmd/render` | `spec.yaml` → `sut.yaml` + `job.yaml` + `terraform.auto.tfvars`. `-replicas N` overrides `sut.replicas` for the gate-#1 ramp. |
| `../loadgen/cmd/sweepscale` | Seeds K subscriptions, scrapes `chronicle_sweep_*`, reports per-tick mean/p50/p99. SLO-gated (non-zero exit on breach). |
| `../loadgen/cmd/rediscpu` | Reads managed-Redis (Memorystore) CPU from Cloud Monitoring — the gate-#1 signal (`../loadgen/redismon`). |
| `../loadgen/chaos` | The pod-kill nemesis command builder (`make chaos`). |
| `../loadgen/Dockerfile` | The load-generator image (sweepscale + dsload). |
| `../metrics` | The SUT's `/metrics` + `/healthz` + `/readyz` (enabled with `-metrics-listen`). |

The load generators and observability run on their own node pools so the rig
**never shares a node with the SUT** — co-locating them is the classic way to
get fake numbers.

## Fast local loop (no cloud)

Validate the whole pipeline against a local Redis before spending on a cluster:

```sh
# 1. instrumented SUT
go run ./cmd/chronicle --store=redis --redis-url=redis://localhost:6379/13 \
  --metrics-listen=:9090 --listen=:4437 --public-url=http://localhost:4437

# 2. drive it
cd loadgen
go run ./cmd/sweepscale -base-url http://localhost:4437 \
  -metrics-url http://localhost:9090/metrics \
  -subscriptions 5000 -links-per-sub 5 -warmup 5s -measure 20s -slo-p99-ms 1500
```

## Cloud run (GKE + managed Redis), one command

`ltctl.sh` does the whole thing — enable APIs, Cloud Build the amd64 images,
provision the cluster + managed Redis, render the spec with the live Redis URL,
deploy, run the SLO-gated job, print the report. Prereqs: `gcloud`
(authenticated — `! gcloud auth login`), `kubectl`, `go`. **Skim `AGENTS.md`** for
the pre-flight quota checks and the corp-net Connect-Gateway fix.

```sh
cd loadtest

make all SPEC=spec/sweep-10k.yaml   # provision → run → ALWAYS tear down
```

`all` tears down on success, failure, **or Ctrl-C** (a trap), so the meter is
never left running by accident. Edit the spec (K, P, replicas, SLO) and re-run;
override the target with env vars (`LT_PROJECT`, `LT_ZONE`, `LT_MACHINE`, …).

Granular, when iterating (the cluster + Redis stay up between `run`s):

```sh
make up                          # provision once (idempotent)
make run SPEC=spec/sweep-50k.yaml   # render + deploy + run + report
make run SPEC=spec/sweep-10k.yaml   # ... again, no re-provision
make down                        # delete cluster + Redis (keeps AR images)
```

A non-zero Job exit (and `make run` exit) means an SLO breach (sweep p99 over
budget, or seed errors). For a worked run and its numbers see `RESULTS-gke.md`.

The Terraform under `terraform/` is the equivalent IaC for those who prefer it
(`terraform apply` the cluster + Memorystore, then `make run`); `ltctl.sh` uses
`gcloud` directly so no Terraform install is needed.

## Finding the fault lines

- **Sweep-scale curve** — re-render with rising `workload.subscriptions`
  (1k → 10k → 100k) and plot the reported `sweep_p99_ms`. The cliff is where it
  approaches `sweep_interval`.
- **Per-replica redundancy (`O(N·K)`) — gate #1** — `make up && make gate1` ramps
  `sut.replicas` 1→4 at a fixed K=10k and reads Memorystore CPU at each N with the
  `rediscpu` reader, writing a CPU-vs-N table to `gate1-results.tsv`. Every replica
  sweeps all K, so the control-plane slot's Redis load grows ~N×. The chronicle
  sweep histogram stays per-replica (read it at `replicas: 1` for a crisp tick); use
  the Redis CPU for the redundancy effect. The exact command + reading guide is in
  [`RESULTS-gke.md`](RESULTS-gke.md) "Gate #1".
- **Chaos / pod-kill** — `make chaos` force-deletes the SUT pods mid-run (the rig
  nemesis), to observe the membership churn window once leased ownership lands (#14).
- **Cap relief** — set `sut.sweep_batch` > 0 to bound per-tick cost on a large
  keyspace (trades recovery latency).

## Gating

On-demand by design — no CI wiring. For cheap per-change regression detection
without a cluster, run the `BenchmarkSweepOnce` microbenchmark with `benchstat`
against a `main` baseline; it catches round-trip regressions in seconds. The
cloud rig is for validating absolute scale, not every commit.

## Constraint

chronicle's module path is `gecgithub01.walmart.com/...`: no AWS, the managed
Redis 8 offering, cloud-agnostic IaC. GKE satisfies no-AWS; point `redis_url` at
the same managed Redis 8 production runs (set `provision_redis = false`) so the
numbers transfer, and keep the Terraform provider swap-able.
