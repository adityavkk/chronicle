# RESULTS — epic #9 horizontal scale (claude orchestrator)

Real-deployment V&V evidence for the `epic-hscale-claude` branch. Captured per sub-issue
as each slice was reviewed and integrated. Every number here is measured, not asserted;
anything that requires a real GKE / multi-node / `STANDARD_HA` Redis substrate is recorded
**PENDING-CLOUD** because the active GCP project is inside a VPC Service Controls perimeter
that refuses all Compute API calls (environment-wide, reproducible, zero resources created —
confirmed by both orchestrators). Those carry the exact committed run spec + command so they
execute the moment a permitted GCP path is available.

## Test substrate used

- **Unit / pure-core:** `go test -short ./...` (both the root module and `loadgen/`) — no Redis, no clock.
- **Integration:** a local `redis:7` container (docker via colima), `REDIS_URL=redis://localhost:<port>/14 go test ./webhook/...`.
- **Jepsen safety/liveness/contention (T#/L#/C#):** the `jepsen/checker` harness driven against a
  **real running chronicle binary + a real Redis** (the local-binary path). The k3d multi-pod rig
  (`jepsen/up.sh`) was available but the colima VM (4 CPU / 8 GB) was continuously contended by a
  concurrent k3d cluster, so the multi-replica scenarios that need ≥2 pods + chaos were run where the
  VM allowed and otherwise recorded with their reproduce command. porcupine models are `Partition`-ed
  strictly per key (Unknown is treated as FAIL, never a pass); the recorder clock is pinned to the driver host.

## Per-issue V&V (integrated into epic-hscale-claude)

### #10 — V&V foundation + no-rebuild baseline (integrated)
- `go build/vet` + `go test -short ./...` GREEN (root + loadgen). Append-only `webhook.Metrics`
  extended with 6 methods (FanOut/DueSetMutation/DueWorkerTick/SlotOwnership/CoverageGap/OwnerFenced),
  consistent across interface / NopMetrics / Prometheus / golden test (GAP2).
- **T1 single-holder-linz** — porcupine leaseModel, **CheckResult Ok over 293 ops**; nemesis gcPause + clock-skew. GREEN.
- **T2 cursor-monotonic** (no-fault) GREEN. **T4 stale-gen-noop** GREEN (byte-identical durable snapshot).
- **L1 at-least-once** GREEN (every appended message delivered, 4/4 at tail). **L3 lease-tail-drop** GREEN (dropLeaseTail ZREM, only the cursor reconciler reaches tail, deposed ack FENCED).
- **Gate #1** (O(N·K) premise: CPU-vs-N at K=10k on Memorystore) — **PENDING-CLOUD**. Spec `loadtest/spec/sweep-10k-scale.yaml`; cmd `cd loadtest && make up && make gate1 && make down`.

### #11 — Claim granularity / third axis (Phase 1, the actual collapse) (integrated)
- **C1/C2 reproduce the empirical 6-clean / 12-collapse** on today's per-type subscription, no rebuild:
  at **G=1** per-worker throughput halves **21.7 → 10.4 → 5.2 ops/s** as claimants rise 6→12→24 while
  aggregate pins ~125/s and BUSY/op → 0.97 — the fence storm.
- **C3 / doc-05 gate #6 PASS (acceptance gate):** at **G=16** per-worker throughput holds **~27 flat**
  and aggregate **scales 158 → 336 → 650**; the collapse knee moved beyond G=16 (~G×). BUSY/FENCED at fixed N drop ~G×.
- **T1 still holds per `(subId, g)`** (porcupine shard-linz, partitioned per shard, Unknown=fail). Cross-shard ack impossible (proven live). Shard 0 / G=1 byte-identical to today.
- `ClaimContention` metric appended (cardinality-safe — subID discarded, only status is a label).
- Design doc `docs/specs/horizontal-scale/research/08-claim-granularity.md` (per-shard-of-type, G=16) + a ready-to-file Electric-repo client tracking issue.

### #12 — Due-set outbox (Move 2) (integrated)
- `ds:{__ds}:due` outbox ZADD/ZREM'd in arm_wake/ack/expire_lease/release (GAP3 closed, regression-tested: a released sub leaves no phantom due-mark). `dueWorker` drains owed subs O(owed) via the unchanged `claim_due.lua`; full sweep retained as backstop. Pure `DecideDue` reconcile core.
- **T1/T2/T4/L1/L3 GREEN** (fence untouched) via the local-binary path; **merged #11+#12 Redis integration GREEN** (full webhook suite, real run). Composition: webhook acks run g=0 where the due member == id, so ZADD/ZREM match; g>0 pull-wake shards never touch the due-set.
- **Gate #3** (due-set write amplification, DueSetMutation QPS) — **PENDING-CLOUD**. Spec `loadtest/spec/due-10k.yaml`.

### #13 — Event-triggered recovery + coarse floor (integrated)
- `recoverySweeper` split into `reconcile(scope)` (sealed sum type Boot|Reconnect|AppendError|Floor + a stubbed EpochBump|NewOwnerCAS case for #14) + a coarse floor raised **2 s → 30 s**. `reconcileLeases` re-derives a stranded live/waking sub's dropped lease+due tail from the durable sub hash via a fence-safe `restore_lease.lua` (schedule-only, never the generation/wake_id fence). `sweepOnce` preserved byte-for-byte (no owner guard).
- **T1/T2/T4/L1 GREEN**; **L3 sharpened to `-floor=0` + explicit takeover and GREEN** (the eager reconcile at the takeover trigger reaches tail, far under the 30 s server floor → a tick cannot be the recoverer). doc-07 honest-gap #4 resolved. Integrated-branch Redis integration GREEN (real run).
- Steady-state delivery latency unchanged by the floor raise (the 2 s sweep fired nothing in steady state). No standalone gate.

### #14 — Leased slot ownership (Move 3) — IN PROGRESS
### #15 — Slot-homed state (Move 1) — PENDING (#14)
### #16 — DR + capstone — PENDING (#14, #15)

## Gate ledger (doc-05 gates #1–#6)

| Gate | Owner | Property | Status |
|---|---|---|---|
| #6 | #11 | per-type claim contention — knee moves ~G× | **GREEN** (C3: G=16 knee beyond range; per-worker flat, aggregate scales) |
| #1 | #10 | O(N·K) premise (CPU-vs-N at K=10k) | PENDING-CLOUD (VPC-SC) — spec + cmd committed |
| #3 | #12 | due-set write amplification | PENDING-CLOUD — spec `due-10k.yaml` |
| #4 | #14 | membership churn window (coverage gap ≤ TTL+RTT, 0 lost / 0 double-grant) | (in progress) |
| #2 | #15 | OnStreamAppend fan-out p99 (S=2/4/8/256, real multi-node) | (pending; will be PENDING-CLOUD — loopback erases max-node-RTT) |
| #5 | #16 | failover fence drill (STANDARD_HA) | (pending; will be PENDING-CLOUD) |

## K=10k regression floor

`loadtest/RESULTS-gke.md` baseline: sweep p99 **509 ms**, SLO PASS. Reproduction on a real
cluster is **PENDING-CLOUD**; the requirement (sweep p99 < 1500 ms) is carried as the standing
floor and the spec (`loadtest/spec/sweep-10k.yaml` + the `-scale` variant) is committed.
