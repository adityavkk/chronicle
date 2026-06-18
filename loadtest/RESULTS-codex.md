# Results — Codex gate runs for horizontal-scale #10

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
