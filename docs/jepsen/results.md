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

## Scenario matrix

| Scenario | Fault | Crash window (research 07) | Observed |
| --- | --- | --- | --- |
| `baseline` | none | — | 6/6 streams at tail; 118 wakes for 120 msgs; dup ×1.00 |
| `origin-restart` | kill one origin every 3 s during the append storm, then kill **all** origins after the final append | **window 6/7 — the sharp edge:** cursor/lease/wake live in the origin's in-memory map; an origin death loses them, and wake creation is event-driven with no scanner | 12/12 streams at tail after both origins were replaced by fresh pods (in-memory state gone); 895 wakes for 960 msgs; dup ×1.00 |
| `redis-restart` | delete the Redis pod mid-workload; it is recreated and replays its PVC-backed AOF | the durable substrate itself — does the log **and** the subscription control plane survive a storage-tier restart | 16/16 streams at tail after Redis was recreated mid-storm; appends rode the outage via client retry; 1681 wakes for 1920 msgs; dup ×1.00 |

All three pass: every durably-appended message reached its cursor.

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
