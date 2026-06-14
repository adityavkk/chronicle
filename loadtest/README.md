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
| `../loadgen/cmd/render` | `spec.yaml` → `sut.yaml` + `job.yaml` + `terraform.auto.tfvars`. |
| `../loadgen/cmd/sweepscale` | Seeds K subscriptions, scrapes `chronicle_sweep_*`, reports per-tick mean/p50/p99. SLO-gated (non-zero exit on breach). |
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

## Cloud run (GKE + managed Redis 8)

Prereqs: `gcloud` (authenticated — run `! gcloud auth login` in your shell),
`terraform`, `kubectl`, `docker`, and an Artifact Registry repo.

```sh
# 1. Build + push images (chronicle from the repo root, loadgen from loadgen/).
docker build -t $REG/chronicle:$TAG .
docker build -t $REG/chronicle-loadgen:$TAG -f loadgen/Dockerfile loadgen
docker push $REG/chronicle:$TAG && docker push $REG/chronicle-loadgen:$TAG

# 2. Point the spec at those images, then render.
$EDITOR loadtest/spec/sweep-10k.yaml          # set sut.image + loadgen_image
( cd loadgen && go run ./cmd/render -spec ../loadtest/spec/sweep-10k.yaml -out ../loadtest/out )

# 3. Provision the cluster + managed Redis. Set project_id in the tfvars first.
cp loadtest/out/terraform.auto.tfvars loadtest/terraform/
$EDITOR loadtest/terraform/terraform.auto.tfvars   # project_id, and redis_version = your managed Redis 8
( cd loadtest/terraform && terraform init && terraform apply )

# 4. Wire the SUT to Redis, re-render, and point kubectl at the cluster.
#    Put `terraform output redis_url` into the spec's sut.redis_url, then:
( cd loadgen && go run ./cmd/render -spec ../loadtest/spec/sweep-10k.yaml -out ../loadtest/out )
eval "$( cd loadtest/terraform && terraform output -raw kubeconfig_command )"

# 5. Deploy the SUT and run the SLO-gated experiment.
kubectl apply -f loadtest/out/sut.yaml
kubectl -n chronicle-loadtest rollout status deploy/chronicle
kubectl apply -f loadtest/out/job.yaml
kubectl -n chronicle-loadtest wait --for=condition=complete --timeout=15m job -l app=sweepscale
kubectl -n chronicle-loadtest logs -l app=sweepscale --tail=-1   # the JSON report

# 6. Tear it all down.
( cd loadtest/terraform && terraform destroy )
```

A non-zero Job exit means an SLO breach (sweep p99 over budget, or seed errors).

## Finding the fault lines

- **Sweep-scale curve** — re-render with rising `workload.subscriptions`
  (1k → 10k → 100k) and plot the reported `sweep_p99_ms`. The cliff is where it
  approaches `sweep_interval`.
- **Per-replica redundancy (`O(N·K)`)** — raise `sut.replicas` and watch the
  managed-Redis CPU (Memorystore metrics): every replica sweeps all K, so the
  control-plane slot's load grows ~N×. The chronicle sweep histogram stays
  per-replica, so read it at `replicas: 1` for a crisp tick; use Redis metrics
  for the redundancy effect.
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
