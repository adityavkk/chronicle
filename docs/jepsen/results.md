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

## Horizontal-scale no-rebuild baseline (issue #10)

The verification plan
[`07-jepsen-style-verification.md`](../specs/horizontal-scale/research/07-jepsen-style-verification.md)
calls for **T1, T2, T4, L1, L3** to be green on today's single-`{__ds}` code as
the regression floor — no production rebuild. These were obtained on **2026-06-18**.

**Substrate honesty.** The k3d Jepsen cluster could not get a clean run in this
environment: the local colima VM (4 CPU / 8 GB) was shared with a concurrent
`k3d-bakeoff-*` cluster (another orchestrator's), and bringing up a second k3d
cluster saturated the Docker daemon (`docker version` hung > 12 s; recovered in
~5 s once load was shed). That is a **shared-VM resource limit, not a code
failure**. So the live runs below were driven against a **local chronicle binary
+ a single local Redis 7 container** (a far lighter footprint than a full k3d
cluster) — the same `jepsen/checker` scenarios over the real HTTP/Redis paths,
just without the k3d `kubectl` nemesis. The namespaced k3d cluster
(`chronicle-jepsen-claude`) was torn down cleanly; nothing was stranded.

| Property | Scenario | Verdict | Evidence |
| --- | --- | --- | --- |
| **T1** single-holder lease | `single-holder-linz` | **GREEN** | porcupine `linearizable: yes` over 293 ops (270 claims, 23 grants), 4 workers + in-process `gcPause` nemesis |
| **T2** cursor monotonicity | `cursor-monotonic` | **GREEN (no-fault)** | 137 cursor samples / 3 streams, no regression / no phantom advance. The origin-churn nemesis is `kubectl`-bound, so it was a no-op locally — the **faulted** T2 needs k3d (command below) |
| **T4** no stale-gen effect | `stale-gen-noop` | **GREEN** | deposed worker's stale ack `409 FENCED` **and** the durable cursor byte-identical; the same ack under the current generation advanced the cursor (`events/t4-0` `…0042 → …0077`) |
| **L1** at-least-once | `at-least-once` | **GREEN** | 4/4 streams at tail, 40 msgs → 28 coalesced wakes, dup ×1.00, delivered ≤ one sweep tick |
| **L3** lease-tail-drop recovery | `lease-tail-drop` | **GREEN** (manual ZREM) | the lease ZSET entry `ds:{__ds}:sched:lease` was ZREM'd with the sub hash left `live`; the cursor-reading sweep recovered it (fresh claim rotated gen 1→2, acked to tail) and the deposed ack was `409 FENCED`. The scenario's `dropLeaseTail` issues the ZREM via `kubectl`, so the **scenario** form needs k3d; the property was reproduced directly here |
| (regression) | `expired-lease-takeover` | **GREEN** | fence rotated gen 1→2 on takeover; deposed ack `409 FENCED` |
| **T3** ownership exclusivity | `ownership-exclusivity` | **GATED (#14)** | reaches the cluster, `killSlotOwner` cleanly no-ops (no ownership record yet); the porcupine CAS model is unit-tested (`go test ./jepsen/checker/ -run TestShardModel`) |
| **T5** slot isolation | `slot-isolation` | **GATED (#15)** | reports gated; needs the S-slot `{__ds:h}` tagging |

**Pure-core unit floor (the real gate): all green** — `go test -short ./...`
(root + `loadgen`) covers the T1 lease model (13 cases), the T3 ownership-CAS
model (13 cases), the T4 effect checker, the L1 delivery checker, the C1/C2/C3
contention skeleton, the six nemesis primitives, the `redismon`/`chaos` rig
builders, and the metrics golden list.

**Reproduce the full faulted suite on k3d** (when the VM is not contended):

```sh
jepsen/up.sh                                                   # CLUSTER=… to namespace it
jepsen/run.sh single-holder-linz cursor-monotonic stale-gen-noop lease-tail-drop at-least-once
jepsen/run.sh ownership-exclusivity slot-isolation             # gated scaffolds (#14/#15)
jepsen/down.sh                                                 # ALWAYS tear down
```

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
