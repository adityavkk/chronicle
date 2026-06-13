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
jepsen/run.sh expired-lease-takeover glob-create-crash  # the hardening scenarios
jepsen/down.sh                     # tear down the cluster
```

The hardening scenarios (`pull-wake-arm-crash`, `expired-lease-takeover`,
`glob-create-crash`, `index-repair`) are not in the default `run.sh` set yet;
pass them by name.

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
4. Injects the scenario's faults with `kubectl delete pod --force` (and, for
   `index-repair`, `redis-cli del` inside the Redis pod):
   - `baseline` — none.
   - `origin-restart` — kill one origin every 3 s during the workload, then kill
     **all** origins after the final append (the final wake can then only come
     from the recovery sweep on a restarted origin).
   - `redis-restart` — delete the Redis pod mid-workload; it recreates and replays
     its PVC-backed AOF.

   Hardening scenarios (one per slice, docs/research/10):
   - `pull-wake-arm-crash` (slice 1) — a pull-wake subscription drained by a
     worker loop while origins are killed aggressively and then all killed after
     the final append; asserts every stream still reaches tail because the sweep
     re-emits any wake stranded between arm and event-emit. **Approximation:** the
     true "after arm, before emit" window is a few µs inside `issueWake` and
     cannot be hit precisely from the host, so the harness drives the strictly
     stronger end-to-end property (see the comment on `runPullWakeArmCrash`).
   - `expired-lease-takeover` (slice 2) — worker A claims a pull-wake sub and
     stalls past `lease_ttl_ms`; worker B claims (lease takeover) and acks;
     asserts A's later ack returns **409 FENCED** and B's generation rotated. A
     deterministic claim/ack property; no pod kill needed.
   - `glob-create-crash` (slice 3) — create matching streams while killing all
     origins the instant each is created (before the best-effort
     `OnStreamCreated`/backfill links it); asserts the slow reconcile loop
     re-matches the glob and every stream reaches tail.
   - `index-repair` (slice 4) — `redis-cli del` selected
     `ds:{__ds}:stream:<path>` fan-out index SETs during a webhook workload, then
     append past the gap; asserts `ReconcileIndexes` rebuilds the index from the
     canonical links and the later appends still wake.
5. After the faults settle, asserts every stream's `acked_offset` equals its tail
   (or, for `expired-lease-takeover`, asserts the FENCED status directly), and
   reports the delivered-wake count and the duplicate factor (at-least-once).

A non-zero exit means a stream never reached its tail — a wake was lost and not
recovered — or, for `expired-lease-takeover`, that the deposed worker was not
fenced.

## Files

- `deploy/deploy.yaml` — Namespace, Redis (AOF on a PVC), chronicle ×2, Services.
- `Dockerfile` — wraps the host-built `bin/chronicle-linux`.
- `checker/main.go` — receiver + workload + nemesis + checker.
- `up.sh` / `run.sh` / `down.sh` — lifecycle.
