# Jepsen-style durability tests for the `__ds` subscription layer

A fault-injection harness that runs chronicle on a local Kubernetes cluster and
verifies that the subscription layer delivers every durably-appended message even
when origins and Redis are killed mid-flight. It is the empirical counterpart to
the crash-window analysis in [docs/research/07](../docs/research/07-subscription-wake-lease-durability.md);
results and interpretation live in [docs/jepsen/results.md](../docs/jepsen/results.md).

## Prerequisites

`docker`, `k3d`, `kubectl`, and the Go toolchain. The harness builds a static
chronicle binary on the host and bakes it into a minimal image, so the image build
needs no network.

## Run

```sh
jepsen/up.sh                       # create the k3d cluster + deploy chronicle ×2 + Redis
jepsen/run.sh                      # run baseline, origin-restart, redis-restart
jepsen/run.sh origin-restart       # or a single scenario
jepsen/down.sh                     # tear down the cluster
```

`up.sh` maps `localhost:4438` to the chronicle NodePort through the k3d
loadbalancer, so the host driver keeps reaching chronicle while individual pods
die. Override `CLUSTER`, `STREAMS`, `MSGS` via env.

## What it does

`jepsen/checker` (the host driver):

1. Starts a webhook receiver on the host (`:8099`), reachable from pods via
   `host.k3d.internal`; it returns `{"done":true}` so each wake auto-acks.
2. Creates a webhook subscription over `events/*`.
3. Appends a known set of messages across many streams, retrying through origin
   churn, recording each stream's final tail.
4. Injects the scenario's faults with `kubectl delete pod --force`:
   - `baseline` — none.
   - `origin-restart` — kill one origin every 3 s during the workload, then kill
     **all** origins after the final append (the final wake can then only come
     from the recovery sweep on a restarted origin).
   - `redis-restart` — delete the Redis pod mid-workload; it recreates and replays
     its PVC-backed AOF.
5. After the faults settle, asserts every stream's `acked_offset` equals its tail,
   and reports the delivered-wake count and the duplicate factor (at-least-once).

A non-zero exit means a stream never reached its tail — a wake was lost and not
recovered.

## Files

- `deploy/deploy.yaml` — Namespace, Redis (AOF on a PVC), chronicle ×2, Services.
- `Dockerfile` — wraps the host-built `bin/chronicle-linux`.
- `checker/main.go` — receiver + workload + nemesis + checker.
- `up.sh` / `run.sh` / `down.sh` — lifecycle.
