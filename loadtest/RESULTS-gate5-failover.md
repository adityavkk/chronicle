# Gate #5 failover results — the durability-honesty assertion (issue #43, P4.4)

> **The load-bearing claim under test:** an acknowledged write LOST on a Redis
> failover (async replication) degrades **only to at-least-once** delivery
> (deduplicated downstream by the consumer's monotone offset), and **never to a
> safety violation** (no two-holder fence break, no cursor regression, no lost
> update past the fence). This is the one surface the Lean proofs and the
> TLA+/Apalache model-checks structurally cannot reach: they all assume the durable
> Lua write *happened*; none can model a primary dying and a replica that never
> received the write being promoted. This page records the at-least-once
> **degradation** — it makes **no strong-consistency claim**.

| | |
|---|---|
| **Invariants** | INV-DUR-01 (Tier B WAITAOF barrier precedes dispatch), INV-JEP-L1-01 (at-least-once: cursor reaches tail), INV-FENCE-01 (single-holder survives the failover) |
| **Date** | 2026-06-23 |
| **Local substrate** | Redis 8 primary + AOF replica pair (docker), `appendfsync` per row; teardown trap |
| **Cloud substrate** | Memorystore STANDARD_HA (replica + managed failover + stable endpoint) — **PENDING-CLOUD** |

## What ran where

The durability-honesty contract has three layers. Two are fully exercised and
green **locally**; the third (the under-load managed-failover SLO on a real
multi-node cluster) is **PENDING-CLOUD**, matching the epic's framing for
gate#2 / L2 / L4 / L5. Nothing here fakes a cloud result.

| Layer | What it proves | Where it ran | Status |
|---|---|---|---|
| 1. Pure logic | `redisFailover` command builders, RPO/RTO parsers, the PASS/FAIL verdict, `AssertAOFEnabled`, the `DurabilityShort` metric wiring | `go test ./jepsen/checker/ ./webhook/ ./metrics/` (no cluster) | **PASS** (local) |
| 2. Real Redis failover mechanics | WAITAOF 1 1 acks against a *real* replica; kill primary → `REPLICAOF NO ONE` → the promoted node serves the acked write; empirical RPO from `master_repl_offset` | docker primary+replica pair (local) | **PASS** (local) |
| 3. End-to-end under load | the K-subscription webhook workload reaches cursor==tail, a managed failover drops writes in the RPO window, the boot reconcile re-fires them, CheckAtLeastOnce confirms every stream reached tail, the deposed ack is FENCED, RPO/RTO recorded under load | Memorystore STANDARD_HA via `ltctl.sh gate5` / k3d STANDARD_HA via `standard-ha-failover.sh` | **PENDING-CLOUD** |

## Layer 1 — pure logic (local, PASS)

The failover nemesis command builders, the RPO/RTO parsers, and the PASS/FAIL
verdict are pure total functions, unit-tested without a cluster, so CI exercises
the logic while the chaos run stays an on-demand job:

- `jepsen/checker/nemesis_test.go` — `promoteReplicaCmd` is exactly `REPLICAOF NO ONE`; `flipEndpointPatch` repoints the stable `redis` selector; `parseMasterReplOffset` reads `master_repl_offset`; `redisFailover` issues kill→promote→flip in order, computes RPO = `primary_before − promoted_after` (clamped ≥ 0), returns `−1` on a failed kill (honest "injection failed", not a 0-byte RPO), and **issues no WAIT/WAITAOF and reads no lease** (correction #3).
- `jepsen/checker/scenario_failover_test.go` — `failoverVerdict` is PASS iff injection succeeded **and** zero L1 gaps **and** the deposed ack was 409 FENCED. A **positive RPO is not a failure** — it is the durability-honest signal the failover really dropped writes. An L1 gap is a lost update (FAIL). An unfenced deposed ack (200 OK across the promotion) is the INV-FENCE-01 safety violation the scenario exists to catch (FAIL).
- `webhook/consistency_test.go` — `AssertAOFEnabled` fails a Tier-B-configured manager fast against `appendonly no` or a topology that cannot meet the replica requirement; Tier A/C never assert; `ParseConnectedReplicas` excludes a connected-but-syncing replica.
- `metrics/metrics_test.go` + `webhook/redis_store_durability_test.go` — a short `WAITAOF` reply increments `chronicle_durability_short_total{cmd="WAITAOF"}` through the `Metrics` seam (NopMetrics no-op + Prometheus impl + golden entry), carries durability only, and the verdict is still returned (never swallowed).

## Layer 2 — real Redis failover mechanics (local, PASS)

A throwaway primary + AOF replica pair (docker, unique prefix, teardown trap) ran
the exact promotion sequence the scenario drives, proving the STANDARD_HA
substrate's mechanics independent of Kubernetes:

| Step | Observed |
|---|---|
| Replication link | `master_link_status:up` (replica attached) |
| Tier B barrier | `WAITAOF 1 1 1000` → `[1, 1]` — local AOF fsync **and** the replica fsync acked the fence-minting write |
| Primary repl offset (pre-kill) | `master_repl_offset = 78` |
| Real failover | kill primary → `REPLICAOF NO ONE` on the replica |
| Promoted node role | `master` |
| **Acked write survived** | `ds:fence:sub-x = gen=1,wake=w_a` served by the promoted node |
| **Empirical RPO** | **0 replication bytes** |

**Read this honestly.** RPO = 0 here because the write was protected by `WAITAOF 1
1`: the Tier B barrier blocked dispatch until the replica had fsync'd the write, so
the promotion could not lose it. This is exactly the durability-honest claim — Tier
B shrinks the RPO to the replica-fsync ack. A **Tier A** write (no WAIT) under the
same kill would have an RPO equal to the full async-replication lag, and *that*
dropped write is the one the at-least-once degradation (re-fire + monotone-cursor
dedup) recovers without a safety violation. The end-to-end demonstration of the
Tier A RPO-window re-delivery under load is Layer 3.

## Layer 3 — end-to-end under load (PENDING-CLOUD)

The worktree does not run multi-node clusters; the orchestrator runs this. Two
entry points are wired and ready, both with a `trap`-based teardown on
`EXIT`/`INT`/`TERM` (STOP THE METER):

- **Self-managed (k3d STANDARD_HA):** `jepsen/deploy/standard-ha-failover.sh`.
  Gate #5a/#5c establish the L3 lease-tail-drop property before and after a real
  promotion. **Gate #5d** (issue #43) runs the dedicated `failover` scenario: it
  drives K pull-wake subscriptions to cursor==tail, injects the real primary loss +
  replica promotion mid-flight (`redisFailover`), waits for the boot reconcile,
  then asserts via `CheckAtLeastOnce` that every linked stream reached tail and that
  a deposed worker's late ack is 409 FENCED. It prints a single machine-readable
  line, `GATE5-FAILOVER-VERDICT: PASS|FAIL`, plus the empirical RPO/RTO tiers.
- **Managed (Memorystore STANDARD_HA):** `loadtest/ltctl.sh gate5
  spec/dispatch-webhook-failover.yaml`. Renders the Tier B, replica-backed webhook
  workload, triggers the managed failover mid-measure, rolls chronicle (the boot
  reconcile = the eager reconcile `Manager.Promote` drives), then runs the
  at-least-once tail check and scrapes `chronicle_owner_fenced_total`,
  `chronicle_slot_ownership_events_total`, and `chronicle_durability_short_total`
  (the RPO-exposure signal).

### Expected verdict (to be filled from the cloud run)

| Tier | RPO | RTO | At-least-once | Deposed ack |
|---|---|---|---|---|
| Tier B (WAITAOF 1 1) | ≈ replica-fsync ack (≈ 0 for the acked write) | promotion + reconnect + boot reconcile | every stream reached tail | 409 FENCED |
| Tier A (no WAIT) | full async lag + AOF fsync (~`appendfsync everysec` ≈ 1s) + link latency | promotion + reconnect + boot reconcile | every stream reached tail (re-fire + monotone-cursor dedup) | 409 FENCED |

The two rows are the durability-honest tiers: Tier B buys a small RPO, Tier A is the
fast default — and in **both** a lost acked write degrades only to at-least-once,
never to a safety violation, because exclusivity rests on the monotone `(generation,
wake_id)` fence, never on a WAIT/WAITAOF count or a lease TTL.

## Reproduce locally

```sh
# Layer 1 (pure, no cluster):
go test ./jepsen/checker/ ./webhook/ ./metrics/

# Layer 2 (real Redis failover mechanics) — needs an AOF Redis pair; appendfsync
# always makes the WAITAOF local-ack deterministic. The local project test Redis
# runs appendfsync everysec, so the Tier B local-fsync unit test
# (TestStoreTierBArmDurableLocalFsync) races the ~1s fsync interval and is expected
# to be skip/flaky there; point REDIS_URL at an appendfsync=always AOF Redis to run it.
REDIS_URL=redis://localhost:6379/14 go test ./webhook/ -run TestStoreTierB
```
