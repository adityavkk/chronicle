# Implementer notes — running chronicle subscription load tests on GKE

For the repo-wide map (codebase layout, other runbooks, the open work) see the
root [`AGENTS.md`](../AGENTS.md). This file is the rig specifically.

Read this before you touch the rig. It is the distilled "don't repeat my
mistakes" for running it on GKE, adapted from the Electric Agents rig. For *what*
the rig is, see `README.md`; for the design, the spec format, and source-verified
deployment contract, see that and the spec under `spec/`.

The whole thing is one principle: **one declarative experiment spec is the source
of truth; `render` turns it into the SUT manifest + load Job + tfvars, and you
reconcile a namespace to it.** Tuning = copy a spec, change numbers, re-render —
never hand-edit live k8s objects.

What this rig measures that the Electric one couldn't: chronicle now exposes
`/metrics` (`chronicle_sweep_*`), so the **sweep tick duration, subs/tails per
tick, and wake-delivery latency are app-level metrics**, not inferred. You still
want cAdvisor (CPU) and the Memorystore metrics (the per-replica `O(N·K)` Redis
load), but the headline signal — does a sweep tick fit its interval at K — comes
straight from the SUT.

---

## The repeatable recipe

```bash
export REG=us-central1-docker.pkg.dev/$PROJECT/chronicle TAG=v1

# 1. amd64 images in Cloud Build (NOT QEMU on an arm Mac).
gcloud builds submit --config loadtest/cloudbuild.yaml \
  --substitutions=_REG=$REG,_TAG=$TAG .

# 2. render the spec, provision, deploy, run, tear down.
( cd loadgen && go run ./cmd/render -spec ../loadtest/spec/sweep-10k.yaml -out ../loadtest/out )
( cd loadtest/terraform && terraform init && terraform apply )   # or gcloud, if no terraform
kubectl apply -f loadtest/out/sut.yaml
kubectl -n chronicle-loadtest rollout status deploy/chronicle
kubectl apply -f loadtest/out/job.yaml
kubectl -n chronicle-loadtest logs -f -l app=sweepscale   # the JSON report; exit code = SLO verdict
( cd loadtest/terraform && terraform destroy )            # STOP THE METER
```

For Codex issue-slice runs, keep the defaults suffixed with `-codex`
(`LT_CLUSTER=chronicle-loadtest-codex`, `LT_AR_REPO=chronicle-codex`,
namespace `chronicle-loadtest-codex`) unless the coordinator explicitly assigns
a different isolated name.

---

## Pre-flight (do these FIRST — each one is a failed ~6-min cluster create)

- **Per-family CPU quota, not the generic one.** GKE bills the machine family's
  quota. A fresh project often has `E2_CPUS=24` while `CPUS=200`. Check the family
  you'll use:
  ```bash
  gcloud compute regions describe us-central1 --flatten=quotas \
    --format="csv[no-heading](quotas.metric,quotas.limit,quotas.usage)" \
    | grep -E "^(CPUS|E2_CPUS|N2_CPUS|SSD_TOTAL_GB),"
  ```
- **`SSD_TOTAL_GB` quota (default 500, often partly used).** `nodes × disk` must
  fit *remaining* quota or the cluster lands in **ERROR state** (some nodes up,
  rest fail `GCE_QUOTA_EXCEEDED`) — not a clean failure. Use small disks
  (`--disk-size=50`) or `pd-standard` (counts against `DISKS_TOTAL_GB`).
- **Billing enabled:** `gcloud beta billing projects describe $PROJECT`.

---

## Gotchas, by layer (Symptom → Cause → Fix)

### GCP / Cloud Build / kubectl
| Symptom | Cause | Fix |
|---|---|---|
| `gcloud builds submit` → `storage.objects.get denied` | New projects run Cloud Build as the **Compute Engine default SA**, which lacks roles | Grant the `<num>-compute@…` SA `roles/cloudbuild.builds.builder` + `artifactregistry.writer` + `storage.objectAdmin` + `logging.logWriter` |
| Build dies at first `RUN --mount=type=cache` | Cloud Build's classic docker builder has **BuildKit off** | `env: ['DOCKER_BUILDKIT=1']` on the step (cloudbuild.yaml sets this). Build amd64 in Cloud Build, **not** QEMU on arm |
| `kubectl` → `gke-gcloud-auth-plugin not found` | brew gcloud doesn't expose the plugin on PATH | It's at `$(gcloud info --format='value(installation.sdk_root)')/bin`; set `USE_GKE_GCLOUD_AUTH_PLUGIN=True` |
| `kubectl` → `dial tcp <ip>:443: i/o timeout` (gcloud works) | Network reaches `*.googleapis.com` but **not raw GKE control-plane IPs** (corp net) | Create with `--fleet-project=$PROJECT`, then `gcloud container fleet memberships get-credentials <cluster>` → routes via `connectgateway.googleapis.com`. **port-forward works over Connect Gateway** |
| Backgrounded `gcloud … \| tail; echo $?` "succeeds" but nothing happened | The pipe masks gcloud's real exit code | Treat `gcloud builds list` / AR contents / cluster `status` as ground truth |

### chronicle (deployment contract — source-verified)
| Symptom | Cause | Fix |
|---|---|---|
| Subscriptions 400 on create / never wake | chronicle's SSRF guard drops webhooks to private ClusterIPs; or `--public-url` left as localhost | `--webhook-allow-private` (`CHRONICLE_WEBHOOK_ALLOW_PRIVATE=true`) + `--public-url=http://chronicle.<ns>:4437` + `CHRONICLE_SUBSCRIPTIONS=true` |
| Streams silently truncate under memory pressure | Redis eviction drops stream data; chronicle warns at boot | Memorystore **`maxmemory-policy=noeviction`** (redis.tf sets it). chronicle needs only **Redis 6.0+** — no HEXPIRE — so REDIS_7_2 works; target Valkey 8.0 to match prod |
| `/readyz` flaps | readiness pings Redis; Memorystore not reachable from the SUT pool | Authorized network / same VPC; `/readyz` returns 503 until Redis is up (by design) |
| Sweep histogram looks "mixed" / counts jump around with replicas>1 | the Job scrapes the Service, which load-balances across replicas, each with its own histogram | Scrape **one** replica. Run the sweep-scale measurement at `sut.replicas: 1` (every replica sweeps all K identically); use Redis metrics for the multi-replica `O(N·K)` effect |
| Env pollution `CHRONICLE_*_PORT` in the pod | k8s service links inject them from the Service named `chronicle` (harmless — chronicle reads `CHRONICLE_LISTEN`, not `CHRONICLE_PORT`) | `enableServiceLinks: false` (sut template sets it) — best practice, not required |
| Redis CPU report has zero samples | Cloud Monitoring metrics can lag several minutes, or the query window was shorter than the sampling period | Re-run only the CPU query over the same window if the cluster is still up, or record the zero-sample CPU file as the collection blocker |

Not applicable here (Electric-stack only): Node V8 heap OOM, Postgres pool /
`max_connections`, Drizzle migration races, embedded-DS replica split-brain.
chronicle has none of these — the load generator is Go (`sweepscale`), and the
store is Redis.

---

## Methodology make-sure-tos

- **The sweep is the SUT signal.** `chronicle_sweep_tick_seconds` p99 vs
  `sweep_interval` is the question. Past `K·(1+P)` where the tick stops fitting
  the interval, recovery falls behind. sweepscale reports it and the SLO gate
  fails the run on a breach.
- **Sweep K, find the cliff.** Re-render with rising `workload.subscriptions`
  (1k → 10k → 100k) and plot `sweep_p99_ms`. Local validation already showed the
  batched sweep at 31 ms (K=1k·P1) / 176 ms (K=5k·P5); the cloud run extends the
  curve with real managed-Redis round trips.
- **Attribute the bottleneck with evidence.** Cross-read the SUT `/metrics`
  (`sweep_tails_batched`, `worker_due_items`), cAdvisor CPU (GKE **Standard** —
  Autopilot blocks the node-proxy scrape), and Memorystore CPU/ops (the
  control-plane `{__ds}` slot is the shared ceiling). Don't guess.
- **Gate #1 is three specs, not an edited live object.** Run
  `spec/gate1-replicas-1-codex.yaml`, `spec/gate1-replicas-2-codex.yaml`, and
  `spec/gate1-replicas-4-codex.yaml` through `make run`; each run writes
  sweepscale plus Redis CPU summaries under `loadtest/out/reports/`.
- **Pod-kill chaos is opt-in.** Set `LT_CHAOS=pod-kill` (and optionally
  `LT_CHAOS_PERIOD=15s`) to kill one Chronicle pod repeatedly while the Job
  runs. Leave it off for the clean CPU-vs-replicas gate unless measuring churn.
- **Right-size so the SUT gets its core.** Pin `sut.cpu` limits so node
  contention doesn't masquerade as the sweep's ceiling, and keep the load
  generator on its own node pool — a driver co-located with the SUT gives fake
  numbers.
- **A live run finds what local validation can't.** `go build`/`go test` and
  `render` were green, yet a real deploy is where the SSRF/public-url, the
  managed-Redis round-trip latency, and the multi-replica histogram-mixing show
  up. Do one real end-to-end run before trusting numbers.
- **Stop the meter.** `terraform destroy` (or delete the cluster) when done; keep
  the Artifact Registry images so re-runs skip the rebuild.
