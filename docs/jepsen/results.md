# Jepsen-style durability results — the `__ds` subscription layer

**What this checks.** The subscription layer's central claim (docs/research/07) is
that chronicle closes the restart gap the in-memory reference servers have: the
durable cursor plus a recovery sweep guarantee that every durably-appended
message is eventually delivered, even when the origin that accepted the append
dies before the wake fires. These tests assert that property empirically against
a real Kubernetes deployment under fault injection.

**The property.** After the workload and the faults settle, every linked stream's
subscription cursor (`acked_offset`) has advanced to that stream's tail — i.e.
every message that earned a `2xx` was eventually delivered and acked. Delivery is
at-least-once, so duplicate deliveries are reported (the duplicate factor) but are
not failures: the `generation`/`wake_id` fence makes a duplicate harmless.

**Topology.** k3d single node; chronicle ×2 (so killing one origin leaves a peer
to sweep); Redis ×1 with `appendonly yes`, `appendfsync always`, AOF on a
PersistentVolumeClaim (so the log and the subscription control plane replay after
a pod restart). The harness (`jepsen/checker`) embeds the webhook receiver on the
host, reachable from pods via `host.k3d.internal`; the receiver returns
`{"done":true}` so each wake auto-acks its snapshot. Faults are injected with
`kubectl delete pod --force`.

Reproduce: `jepsen/up.sh && jepsen/run.sh` (`jepsen/down.sh` to tear down).

## #10 harness baseline — 2026-06-18

Commands run from `/Users/auk000v/orca/workspaces/chronicle/hs-10-harness`:

```sh
jepsen/up.sh
jepsen/run.sh single-holder-linz cursor-monotonic stale-gen-noop at-least-once lease-tail-drop-recovery
kubectl --context k3d-chronicle-jepsen -n chronicle-jepsen exec deploy/redis -- redis-cli -n 0 flushdb
jepsen/bin/jepsen-checker -base http://localhost:4438 -cluster chronicle-jepsen \
  -scenario cursor-monotonic -streams 8 -msgs 120 \
  -nemesis-window-min=100ms -nemesis-window-max=200ms -settle=25s
for i in 1 2 3; do
  kubectl --context k3d-chronicle-jepsen -n chronicle-jepsen exec deploy/redis -- redis-cli -n 0 flushdb
  jepsen/bin/jepsen-checker -base http://localhost:4438 -cluster chronicle-jepsen \
    -scenario single-holder-linz -workers 5 -workload-ms 5000
  kubectl --context k3d-chronicle-jepsen -n chronicle-jepsen exec deploy/redis -- redis-cli -n 0 flushdb
  jepsen/bin/jepsen-checker -base http://localhost:4438 -cluster chronicle-jepsen \
    -scenario stale-gen-noop
done
jepsen/bin/jepsen-checker -base http://localhost:4438 -cluster chronicle-jepsen \
  -scenario ownership-exclusivity -nemesis-dry-run
jepsen/bin/jepsen-checker -base http://localhost:4438 -cluster chronicle-jepsen \
  -scenario slot-isolation -nemesis-dry-run
jepsen/bin/jepsen-checker -base http://localhost:4438 -cluster chronicle-jepsen \
  -scenario contention-contract -nemesis-dry-run
jepsen/bin/jepsen-checker -base http://localhost:4438 -cluster chronicle-jepsen \
  -scenario lease-tail-drop-recovery -sweep=0
```

Observed results:

| Scenario | Property | Result |
| --- | --- | --- |
| `single-holder-linz` | T1 single-holder lease | **PASS**. Porcupine `CheckResult == Ok`; representative run recorded 651 ops, 626 claims, 25 grants. Three stress iterations at 5 workers also returned `linearizable: yes` with no `Illegal` or `Unknown`. |
| `cursor-monotonic` | T2 cursor monotonicity | **PASS**. The normal baseline had no regression. A short-window rerun produced 46 `kill-origin` actions, 1500 cursor samples across 8 streams, and no cursor regression. |
| `stale-gen-noop` | T4 no stale-generation effect | **PASS**. Stale ack returned `409 FENCED`; before/after subscription snapshots were byte-identical (`393` bytes). Three stress iterations also passed. |
| `at-least-once` | L1 at-least-once delivery | **PASS**. 320 messages appended, 306 wakes delivered, duplicate factor 1.00, 8/8 streams at tail. |
| `lease-tail-drop-recovery` | L3 lease-tail-drop recovery | **SUPERSEDED / PENDING CORRECTED LIVE RERUN**. The 2026-06-18 run only proved that a later direct `Claim` could rotate the fence after `drop-lease-tail`; it did **not** prove the recovery sweep re-armed stranded work from cursor/tail state. The checker now waits for Redis to show `phase=waking` with a newer generation before any post-drop claim, then requires worker B to claim that recovered wake, ack the pending tail, and leave worker A fenced. No corrected live result is recorded here yet. |
| `ownership-exclusivity` | T3 proposed shard ownership | **SCAFFOLDED / BLOCKED**. Dry-run wiring succeeds. Live check is blocked until `claim_shard.lua`, `check_owner.lua`, and `ds:{ownership}:slot:<h>` exist. |
| `slot-isolation` | T5 proposed slot homing | **SCAFFOLDED / BLOCKED**. Dry-run wiring succeeds. Live check is blocked until S-slot `{__ds:h}` key homing exists. |
| `contention-contract` | C1/C2/C3 contention suite | **SCAFFOLDED / BLOCKED**. Pure rate-contract checker is unit-tested; live C1/C2/C3 require the claimant fan-in measurement rig and, for C3, the claim-granularity fix. |

Honest L3 gap: the corrected default `lease-tail-drop-recovery` driver must be
rerun before L3 is reported green. The stricter `lease-tail-drop-recovery
-sweep=0` variant still intentionally fails on today's binary with:

```text
FAIL: lease-tail-drop-recovery with -sweep=0 is blocked on today's SUT: the deployed binary exposes the recovery sweep, not a separately disableable floor/eager-reconcile path
```

Corrected live rerun attempt on 2026-06-18:

- `jepsen/up.sh` built the Linux binary and Docker image, then created the
  `chronicle-jepsen` k3d cluster.
- The Kubernetes API timed out while applying or waiting on `jepsen/deploy/deploy.yaml`.
- A later `kubectl --context k3d-chronicle-jepsen get nodes` returned
  `NotReady` for `k3d-chronicle-jepsen-server-0`.
- Follow-up pod and node inspection hit TLS handshake and request timeouts, so the
  corrected live L3 scenario was not run.
- `jepsen/down.sh` deleted the unhealthy cluster. No corrected L3 pass or failure
  should be inferred from this attempt.

In the original 2026-06-18 harness run, one stress-loop Redis flush hit a
transient `kubectl` TLS handshake timeout. The scenario itself then ran and
passed. That note does not apply to the corrected L3 driver, which still needs a
live rerun.

## Scenario matrix

| Scenario | Fault | Crash window (research 07) | Observed |
| --- | --- | --- | --- |
| `baseline` | none | — | 6/6 streams at tail; 118 wakes for 120 msgs; dup ×1.00 |
| `origin-restart` | kill one origin every 3 s during the append storm, then kill **all** origins after the final append | **window 6/7 — the sharp edge:** cursor/lease/wake live in the origin's in-memory map; an origin death loses them, and wake creation is event-driven with no scanner | 12/12 streams at tail after both origins were replaced by fresh pods (in-memory state gone); 895 wakes for 960 msgs; dup ×1.00 |
| `redis-restart` | delete the Redis pod mid-workload; it is recreated and replays its PVC-backed AOF | the durable substrate itself — does the log **and** the subscription control plane survive a storage-tier restart | 16/16 streams at tail after Redis was recreated mid-storm; appends rode the outage via client retry; 1681 wakes for 1920 msgs; dup ×1.00 |

The three original scenarios all pass: every durably-appended message reached its
cursor.

## Hardening scenario matrix (one per slice — added, not yet run)

These four scenarios exercise the crash windows closed by the subscription-hardening
slices (docs/research/10). They are implemented in `jepsen/checker` but have **not
been run** on a cluster yet, so the table records the asserted property and the
expected outcome only — no result numbers are invented. Run them with
`jepsen/run.sh <scenario>`.

| Scenario | Slice | Fault | Asserts | Expected | Status |
| --- | --- | --- | --- | --- | --- |
| `pull-wake-arm-crash` | 1 (durable pull-wake recovery, 19c3af8) | pull-wake sub drained by a worker loop; kill origins aggressively, then kill **all** after the final append | every stream's cursor reaches tail; the sweep re-emits any wake stranded between arm and event-emit | all streams at tail; no pull-wake left in `waking` with `sent_ns==0` | **not yet run** |
| `expired-lease-takeover` | 2 (fence rotation, 457bd69) | worker A claims and stalls past `lease_ttl_ms`; worker B claims (takeover) and acks | A's later ack returns **409 FENCED**; B's generation is fresh (rotated) | `409 FENCED` for A, `200` for B, `B.generation != A.generation` | **not yet run** |
| `glob-create-crash` | 3 (glob-link reconciliation, 5f70a1c) | create matching streams while killing all origins the instant each is created (before `OnStreamCreated`/backfill) | the slow reconcile loop re-matches the glob and every stream reaches tail | all streams at tail after the reconcile interval | **not yet run** |
| `index-repair` | 4 (fan-out index repair, 909915f) | `redis-cli del` selected `ds:{__ds}:stream:<path>` fan-out SETs during a webhook workload, then append past the gap | `ReconcileIndexes` rebuilds the index from canonical links and later appends still wake | all streams at tail after the reconcile interval | **not yet run** |

### Honesty notes on the hardening scenarios

- **`pull-wake-arm-crash` is an approximation.** The exact "after arm, before wake-event
  emit" window is a few microseconds inside `issueWake`; it cannot be hit precisely from
  an out-of-process host driver. The harness instead kills origins aggressively (and all of
  them after the final append) and asserts the strictly stronger end-to-end property — a
  worker draining the wake stream eventually sees every stream reach its tail. If the
  arm/emit window were *not* recovered, at least one stream would stay in `waking` with no
  event and never advance, which this catches. A surgical version would need a server-side
  fault-injection seam between `arm_wake.lua` and `record_wake_sent.lua` that this harness
  does not have (left as a TODO in the checker).
- **`expired-lease-takeover` is deterministic** — it is a property of the claim/ack API and
  needs no pod kill, so the nemesis is idle for it.
- **`index-repair` is latency-only.** It is the lowest-severity slice (the stream self-heals
  via the sweep; only delivery latency degrades), so the asserted property is end-to-end
  delivery, not a sub-sweep timing bound.

## Why each result holds

**`origin-restart` is the load-bearing test.** Killing all origins after the last
append destroys every in-memory wake, lease, and generation counter the accepting
origins held — exactly the state the Caddy webhook engine keeps only in RAM. An
in-memory implementation would leave those last messages durable in the log but
never delivered (no wake survives, and nothing re-scans). Chronicle's cursor is a
Redis HASH field and the re-evaluation is the recovery sweep that runs on every
origin at boot and on an interval; the freshly-started origins recompute
`HasPendingWork` against the durable cursors and re-fire. The 12/12 result is that
recovery: deliveries observed *after* the accepting origins no longer exist.

This is the empirical form of research 07 §6.3 — a durable cursor is necessary but
not sufficient; the sweep is what asks the question the cursor makes answerable.

**`redis-restart` tests the substrate.** With `appendfsync always` and the AOF on a
PVC, the recreated Redis replays every acknowledged write, so the streams, the
subscription records, and the cursors are all intact when the origins reconnect.
The origins' go-redis clients re-dial transparently; the next sweep re-fires owed
wakes. 16/16 at tail shows the control plane is genuinely in Redis, not cached in
a way that a storage restart would lose.

**Coalescing shows in the wake counts.** Wakes delivered (e.g. 895) are fewer than
messages appended (960) because one wake's snapshot covers every pending offset up
to the current tail (PROTOCOL §7); a `{done:true}` acks the whole snapshot. Fewer
wakes than messages is correct, not lost work — the cursor reaching the tail is
the proof of completeness.

## Durability honesty

- **The guarantee is at-least-once, fenced** (research 07 §9). The duplicate factor
  was 1.00 in these runs because faults were coarse (whole-pod kills), but a
  reclaim or retry racing a slow-but-alive delivery can double-fire; the fence, not
  the lease, is what keeps that safe. The harness reports the factor so regressions
  toward lost (not duplicated) work are visible.
- **`appendfsync always` is the config under test.** It honors the protocol MUSTs at
  a throughput cost. The Redis default `everysec` has a ~1–2 s loss window on an
  un-fsynced crash, which would surface here as a stream stuck below its tail — the
  check that would catch it is exactly `acked_offset == tail`.
- **What is not yet tested here:** a true network partition between origin and Redis
  (as opposed to a Redis restart), and a partition that isolates one origin while
  the other sweeps. The fence makes these safe in principle (research 07 §9);
  adding a partition nemesis (e.g. `iptables`/`tc` in the pod netns) is the next
  step.
