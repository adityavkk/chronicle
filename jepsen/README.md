# Jepsen-style durability tests for the `__ds` subscription layer

A fault-injection harness that runs chronicle on a local Kubernetes cluster and
verifies that the subscription layer delivers every durably-appended message even
when origins and Redis are killed mid-flight. It is the empirical counterpart to
the safety + liveness verification plan in
[docs/specs/horizontal-scale/research/07-jepsen-style-verification.md](../docs/specs/horizontal-scale/research/07-jepsen-style-verification.md);
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
jepsen/run.sh single-holder-linz cursor-monotonic       # the safety scenarios (07: T1, T2)
jepsen/run.sh stale-gen-noop lease-tail-drop at-least-once  # the no-rebuild baseline (07: T4, L3, L1)
jepsen/down.sh                     # tear down the cluster
```

The hardening scenarios (`pull-wake-arm-crash`, `expired-lease-takeover`,
`glob-create-crash`, `index-repair`), the safety scenarios (`single-holder-linz`,
`cursor-monotonic`, `stale-gen-noop`), the liveness scenarios (`lease-tail-drop`,
`at-least-once`), and the gated acceptance-gate scaffolds (`ownership-exclusivity`,
`slot-isolation`) are not in the default `run.sh` set yet; pass them by name.

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

## Safety scenarios: linearizability checking (research/07)

The durability scenarios above check only the *final* state. The two safety
scenarios check a property over the whole concurrent **history**, with
[`porcupine`](https://github.com/anishathalye/porcupine) (the Go-native Knossos)
as the linearizability checker. They implement T1 and T2 of the
[verification plan](../docs/specs/horizontal-scale/research/07-jepsen-style-verification.md).
The code is split **pure core / imperative shell**: the models and checkers are
deterministic, I/O-free, and unit-tested without a cluster (`go test ./jepsen/checker/`);
the scenario drivers and the recorder are the shell.

- **`single-holder-linz` (T1, single-holder lease).** `N` workers (`-workers`,
  default 4) contend on one pull-wake subscription's lease for `-workload-ms`
  (default 8000), with an in-process **gcPause** nemesis: a worker that has
  claimed stalls past `lease_ttl_ms` before acking, so a peer takes over
  (rotating the fence) and the stalled worker's ack races in stale — Kleppmann's
  deposed-but-resumed process. Every claim/ack is recorded into a
  `porcupine.Operation` history and checked against the pure **lease-fence model**
  (`model_fence.go`). The insight: a *wake* is not a linearizable read/write, but
  the `(generation, wake_id)` fence is — the single-holder guarantee is "every
  grant to a new holder rotates the generation strictly upward, and every accepted
  ack carries the current fence." A violation (a non-rotating takeover, or an OK on
  a stale token) has no valid linearization; `porcupine.VisualizePath` writes the
  counterexample to `linz-counterexample.html`. Generalizes the hand-built
  `expired-lease-takeover` to a model-checked concurrent history.
- **`cursor-monotonic` (T2, cursor monotonicity).** Drives the webhook delivery
  workload under origin churn (sweep + retry worker both re-fire) while a poller
  samples each subscription cursor on a ticker, then checks the samples with the
  pure forward-only checker (`check_cursor.go`): an acked offset never regresses
  and never phantom-advances.
- **`stale-gen-noop` (T4, no stale-generation effect).** Generalizes
  `expired-lease-takeover`: after a takeover rotates the fence, the deposed worker
  tries to advance the cursor with its now-stale token. Asserts the attempt is
  `409 FENCED` **and** the durable cursor snapshot is byte-identical before/after,
  with a positive control — the same offset ack succeeds under the current
  generation — so the no-op is meaningful, not vacuous (`check_stalegen.go`).

## No-rebuild liveness baseline (research/07: L1, L3)

These run against today's code as the regression floor — no production rebuild.

- **`lease-tail-drop` (L3, failover recovery of a stranded wake).** Claims a
  pull-wake subscription, then `dropLeaseTail` ZREMs its lease-schedule entry while
  leaving the sub hash intact (the exact L3 fault). The lease worker is now blind
  to the lease, so only the cursor-reading recovery sweep can recover it: the test
  asserts the cursor reaches tail (recovered) and the deposed worker's late ack is
  `409 FENCED`. The bound is `lease_ttl_ms + -sweep + RTT`. The sharper
  `-sweep=0`/`-floor=0` variant (proving *only* the failover-aware eager reconcile
  recovers it) needs the `reconcileLeases` pass that lands in #13/#14.
- **`at-least-once` (L1).** The no-fault webhook delivery path through the
  per-message L1 checker (`check_delivery.go` `CheckAtLeastOnce`): every linked
  stream's appended range is covered by its final cursor within ≤ one sweep tick
  (`-sweep`).

## Acceptance-gate scaffolds (research/07: T3, T5 — red until the mechanism lands)

`ownership-exclusivity` (T3) and `slot-isolation` (T5) are the executable contract
for mechanisms that do not exist on today's code — `claim_shard.lua` /
`ds:{ownership}` (#14) and the S-slot `{__ds:h}` tagging (#15). They reach the
cluster, exercise the matching nemesis seam (e.g. `killSlotOwner` cleanly no-ops
against the mechanism-less keyspace), and report **GATED** (exit 0 — there is
nothing to fail yet). Their pure models/checkers are unit-tested without a cluster:
`go test ./jepsen/checker/ -run TestShardModel` (the T3 ownership-CAS oracle).

**Modeling note (a real subtlety).** The fence model is *time-free*: lease expiry
governs only grant-vs-BUSY (an observed output), not the safety algebra. One
consequence — `expire_lease.lua` clears an expired lease's `wake_id` **without**
rotating the generation, a server-side event with no client op. So the model can
believe a token is current after the server has already fenced it, which is why a
`FENCED` is treated as an unconditional legal no-op (it grants nothing and mutates
nothing, so it can never be half of a two-holder violation; the OK branch is the
sole safety gate). A `porcupine.Unknown` result (linearizability is NP-hard) means
the history was too concurrent to decide in the timeout — reduce `-workers` or
`-workload-ms`; the scenario fails closed.

## Files

- `deploy/deploy.yaml` — Namespace, Redis (AOF on a PVC), chronicle ×2, Services.
- `Dockerfile` — wraps the host-built `bin/chronicle-linux`.
- `checker/main.go` — receiver + workload + nemesis + durability/L1 checker + the `-scenario` switch.
- `checker/model_fence.go` — pure porcupine lease-fence model (T1).
- `checker/model_shard.go` — pure porcupine ownership-CAS model (T3 scaffold, #14).
- `checker/check_cursor.go` — pure cursor-monotonicity checker (T2).
- `checker/check_stalegen.go` — pure no-stale-generation-effect checker (T4).
- `checker/check_delivery.go` — pure at-least-once / per-message delivery checker (L1).
- `checker/check_contention.go` — pure C1/C2/C3 contention skeleton (gate #6, #11).
- `checker/nemesis.go` — gcPause/toxiproxy/killSlotOwner/dropLeaseTail/clock-skew/randomized-window primitives.
- `checker/history.go` — the recorder seam (driver-host monotonic clock).
- `checker/scenario_lease.go` — `single-holder-linz` driver + gcPause nemesis.
- `checker/scenario_cursor.go` — `cursor-monotonic` driver + cursor poller.
- `checker/scenario_stalegen.go` — `stale-gen-noop` driver (T4).
- `checker/scenario_leasetail.go` — `lease-tail-drop` driver (L3).
- `checker/scenario_gated.go` — `ownership-exclusivity` / `slot-isolation` gate scaffolds (T3/T5).
- `checker/*_test.go` — unit tests for the pure models/checkers/nemesis (no cluster).
- `up.sh` / `run.sh` / `down.sh` — lifecycle.
