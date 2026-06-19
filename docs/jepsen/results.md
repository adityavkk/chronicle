# Jepsen-style durability results — the `__ds` subscription layer

**What this checks.** The subscription layer's central claim (docs/research/07) is
that chronicle closes the restart gap the in-memory reference servers have: the
durable cursor plus event-triggered recovery and a coarse floor guarantee that
every durably-appended message is eventually delivered, even when the origin that
accepted the append dies before the wake fires. These tests assert that property
empirically against a real Kubernetes deployment under fault injection.

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
  -scenario lease-tail-drop-recovery -floor=0
```

Observed results:

| Scenario | Property | Result |
| --- | --- | --- |
| `single-holder-linz` | T1 single-holder lease | **PASS**. Porcupine `CheckResult == Ok`; representative run recorded 651 ops, 626 claims, 25 grants. Three stress iterations at 5 workers also returned `linearizable: yes` with no `Illegal` or `Unknown`. |
| `cursor-monotonic` | T2 cursor monotonicity | **PASS**. The normal baseline had no regression. A short-window rerun produced 46 `kill-origin` actions, 1500 cursor samples across 8 streams, and no cursor regression. |
| `stale-gen-noop` | T4 no stale-generation effect | **PASS**. Stale ack returned `409 FENCED`; before/after subscription snapshots were byte-identical (`393` bytes). Three stress iterations also passed. |
| `at-least-once` | L1 at-least-once delivery | **PASS**. 320 messages appended, 306 wakes delivered, duplicate factor 1.00, 8/8 streams at tail. |
| `lease-tail-drop-recovery` | L3 lease-tail-drop recovery | **SUPERSEDED / PENDING CORRECTED LIVE RERUN**. The 2026-06-18 run only proved that a later direct `Claim` could rotate the fence after `drop-lease-tail`; it did **not** prove the reconnect-triggered eager reconcile re-armed stranded work from cursor/tail state. The checker now restarts Redis as the explicit trigger, waits for Redis to show `phase=waking` with a newer generation before any post-drop claim, then requires worker B to claim that recovered wake, ack the pending tail, and leave worker A fenced. No corrected live result is recorded here yet. |
| `ownership-exclusivity` | T3 proposed shard ownership | **LOCAL CAS PASS / LIVE PENDING**. `claim_shard.lua`, `check_owner.lua`, and the co-located `ds:{__ds}:owner:slot:<h>` owner fence record now have Redis-backed golden tests. The k3d porcupine/nemesis scenario still needs a live rerun. |
| `slot-isolation` | T5 proposed slot homing | **SCAFFOLDED / BLOCKED**. Dry-run wiring succeeds. Live check is blocked until S-slot `{__ds:h}` key homing exists. |
| `contention-contract` | C1/C2/C3 contention suite | **SCAFFOLDED / BLOCKED**. Pure rate-contract checker is unit-tested; live C1/C2/C3 require the claimant fan-in measurement rig and, for C3, the claim-granularity fix. |

Honest L3 gap: the corrected default `lease-tail-drop-recovery` driver must be
rerun before L3 is reported green. The stricter disabled-timer variant is now
`lease-tail-drop-recovery -floor=0`; it relies on the explicit Redis reconnect
trigger, while the eventless coarse-floor case is tested separately.

## #14 local ownership evidence — 2026-06-18

Commands run from `/Users/auk000v/orca/workspaces/chronicle/hs-14-ownership`:

```sh
go test -count=1 -short ./...
go test -count=1 ./webhook
```

Observed local results:

| Gate | Evidence | Result |
| --- | --- | --- |
| `claim_shard.lua` CAS table | `TestClaimShardScriptGoldenSemantics` covers first `CLAIMED` (`owner_epoch=1`), same-owner `RENEWED` without epoch bump, foreign live `BUSY`, transfer-only epoch bump, and the deposed owner becoming `FENCED` through `check_owner.lua`. | **PASS** |
| `check_owner.lua` table | `TestCheckOwnerScriptGoldenSemantics` covers `UNOWNED`, `OWNER`, wrong-owner `FENCED`, and wrong-epoch `FENCED`. | **PASS** |
| Membership + HRW + reclaim | `TestMembershipHeartbeatRemovesExpiredMembers`, `TestSlotReconcileOwnsHRWTargetAndHeldLease`, and `TestDeadMemberAgesOutAndSlotReclaimsAfterLeaseExpiry` cover `ZADD` heartbeat, `ZREMRANGEBYSCORE` dead-member cleanup, HRW-targeted ownership, dead-owner reclaim, epoch bump, and the #13 reconcile seam repairing a dropped lease schedule on takeover. | **PASS** |
| Inline TOCTOU fences | `TestOwnerEpochFencesScheduleMutatingScriptsInline` covers stale owner epochs on `arm_wake.lua`, `ack.lua`, `expire_lease.lua`, `schedule_retry.lua`, `release.lua`, `reconcile_lease.lua`, and due-claim re-score (`claim_due.lua`). | **PASS** |
| Redis Cluster script slots | `TestRedisScriptKeySetsUseOneHashTag` computes Redis hash tags for every Lua call's `KEYS`, including owner-fenced paths, and proves inline owner fences use the co-located owner slot key instead of crossing from `{__ds}` to `{ownership}`. | **PASS** |
| Webhook side-effect gate | `TestDeliverWebhookChecksOwnerBeforePost` proves a stale owner epoch is checked with `check_owner.lua` before the external POST and records `OwnerFenced("deliver_webhook")`. | **PASS** |
| Pure ownership core | Short tests cover typed replica IDs, deterministic HRW argmax, approximate `1/N` movement on replica join over nonce-like replica IDs, and config invariants (`heartbeatInterval < memberLeaseTTL/2`, `slotReconcileInterval <= heartbeatInterval`). | **PASS** |

Not yet run in this worker: live k3d T3/L2/L4 ownership scenarios, the full
T1/T2/T4/L1/L3 regression suite, hard membership-churn/gcPause stress, and the
GKE replicas>=2 pod-kill coverage-gap load gate. Those require a healthy k3d or
GKE fault-injection environment and should not be inferred from the local Redis
evidence above.

## #16 DR capstone local evidence — 2026-06-18

Commands run from `/Users/auk000v/orca/workspaces/chronicle/hs-16-dr-capstone`:

```sh
go test -count=1 -short ./...
go test -race -count=1 -short ./...
go test -count=1 ./webhook ./metrics ./jepsen/checker
(cd loadgen && go test -count=1 ./...)
(cd loadgen && go run ./cmd/render -spec ../loadtest/spec/dr-ha-webhook-codex.yaml \
  -image example.com/chronicle:codex \
  -loadgen-image example.com/chronicle-loadgen:codex \
  -redis-url redis://standard-ha-codex.example:6379/0)
```

Observed local results:

| Gate | Evidence | Result |
| --- | --- | --- |
| Consistency tiers | `webhook.ConsistencyTier` parses/defaults A/B/C as typed values; validation rejects C on Redis with an explicit durability-only reason. Config/env/flag/create-request paths all store `consistency_tier`, and legacy missing-tier hashes re-confirm as Tier A only. | **PASS** |
| Tier A/B fence durability | Redis integration tests prove Tier A does not issue `WAIT`/`WAITAOF`; Tier B issues `WAIT 1` and raw `WAITAOF 1 1` only after `arm_wake.lua` or `claim.lua` actually mints a generation. Short `WAIT` and short `WAITAOF` local/replica pairs return errors. | **PASS** |
| Durability is not authority | `TestStoreTierBDurabilityDoesNotReplaceGenerationFence` forces a Tier B takeover after the first worker's lease lapses; the old generation's later ack is still `FENCED`. The code does not use `WAIT` or `WAITAOF` as a lease/owner exclusivity signal. | **PASS** |
| Promotion / eager reconcile | `TestDeadMemberAgesOutAndSlotReclaimsAfterLeaseExpiry`, `TestReconcileLeasesRestoresDroppedLeaseAndDueFromDurableState`, and `TestRedisReconnectRepairsDroppedNonDefaultClaimShardLease` cover epoch-bump/reconnect repair of missing lease and due schedule entries from durable HASH state for live/waking leases. Existing owner-fence tests still prove deposed owner writes are rejected. | **PASS** |
| Checker classifications | `jepsen/checker` now has pure classifiers and tests for T5 slot isolation, L2 bounded recovery, L4 ownership convergence, and L5 max inter-delivery gap under pending work. These are classifier tests, not live nemesis results. | **PASS / LOCAL ONLY** |
| Local Redis integration | `go test -count=1 ./webhook ./metrics ./jepsen/checker` passed, including Redis-backed Tier A/B durability paths, slot-homed migration/build checks, promotion repair, deposed ack fencing, metrics, and checker tests. | **PASS** |
| HA load render path | `loadtest/spec/dr-ha-webhook-codex.yaml` renders a 2-replica Tier B SUT in namespace `chronicle-loadtest-codex` with `CHRONICLE_CONSISTENCY_TIER=B`, a STANDARD_HA-compatible Redis URL override, and webhook receiver URL `webhook-receiver-codex...`. | **PASS / RENDER ONLY** |

Blocked or not claimed green:

| Gate | Status |
| --- | --- |
| Full T1-T5, L1-L5 live Jepsen suite | **BLOCKED / NOT RUN** in this worker. The local checker and Redis tests passed, but no k3d or cloud fault-injection cluster was started for #16. Do not infer T1-T5 or L1-L5 live GREEN from this section. |
| L5 combined never-quiescing nemesis | **BLOCKED / NOT RUN**. The L5 classifier exists, but the combined `kill-slot-owner + partition/heal + redis-failover + lease-tail-drop near R` scenario still needs a live harness run. |
| C1-C3 | **UNCHANGED** from the #10/#15 notes: local rate-contract/checker pieces exist, but the live claimant fan-in and C3 granularity stress are not recorded green here. |
| Gate #5 real failover | **BLOCKED** before provisioning. Read-only GCP preflights from this workspace failed: Compute quota inspection was prohibited by organization policy (`vpcServiceControlsUniqueIdentifier: J6OZ1osBAlaRnbh5admsDmQq4dVPl9nCgUI4StWAyLr9AMW2Oi6rsT_vY7wvK8mJnqz_Uwfwz4dCxGA`), and `gcloud redis instances list --region=us-central1` failed with `PERMISSION_DENIED` / `SERVICE_DISABLED` for `redis.googleapis.com` in project `adityavkk-prototyping`. No RPO/RTO number is recorded. |
| Single Redis Recreate | **NOT A GATE #5 PASS**. The existing k3d deployment's single Redis `Recreate` path remains useful for AOF replay and reconnect repair, but it is not active-passive failover and is not reported as STANDARD_HA / managed Redis 8 evidence. |

Resource note: no cloud, k3d, Kubernetes namespace, or Docker resource was
created for the #16 evidence run. Redis integration used the locally reachable
test Redis URL; no `-codex` container or volume was started by this worker.

## #15 local slot-homing evidence — 2026-06-18

Commands run from `/Users/auk000v/orca/workspaces/chronicle/hs-15-slot-homing`:

```sh
go test -count=1 -short ./...
go test -race -count=1 -short ./...
go test -p 1 -count=1 ./...
go test -count=1 ./webhook ./metrics ./jepsen/checker
CHRONICLE_SKIP_REDIS_START=1 CHRONICLE_REDIS_CONTAINER=chronicle-hs15-redis-codex \
  make conformance
git diff --check
make lint
```

Observed local results:

| Gate | Evidence | Result |
| --- | --- | --- |
| Slot addressing | `TestSubscriptionSlotTagUsesStableFNV1aShard` fixes `subSlots=256`, verifies Go FNV-1a slot tags such as `s1 -> {__ds:201}` and `sub-0 -> {__ds:200}`, and confirms owner slots derive from the same subscription slot. | **PASS** |
| Redis Cluster script slots | `TestRedisScriptKeySetsUseOneHashTag` covers every Lua `KEYS` set under owner-fenced and no-owner paths after retagging; `TestRedisScriptKeySetRejectsMixedHashTags` proves a mixed keyset is detected before Redis rather than silently accepted. | **PASS** |
| GAP4 scatter/gather | `TestHighSlotSubscriptionFoundByScatterGatherPaths` creates `sub-0` in slot 200 and verifies `List`, `GetMany`, `ReconcileIndexes`, and `OnStreamAppend` all find it through S-slot union/scatter paths. | **PASS** |
| Occupied-slots fan-out | `TestOccupiedSlotsBitmapSetAndNeverCleared` verifies `SETBIT h 1` on index, no clear on deindex, and `StreamSubscribers` probing only the stale occupied bit. `TestHighSlotSubscriptionFoundByScatterGatherPaths` also checks `FanOut(slotsProbed=1, subs=1)` on the real `OnStreamAppend` path. | **PASS** |
| Lazy migration | `TestLegacySubscriptionMigratesLazilyToSlotHome` creates an old `ds:{__ds}` subscription, calls `Get`, and verifies the full sub/link/due/index key set moves to `ds:{__ds:h}` while the old sub/link keys are deleted and the migrated scripted key set remains single-tag. `TestCompletedMigrationCleansLegacyResidueWithoutOverwritingNewState` verifies a completed migration marker cleans stale legacy residue without replaying stale old HASH fields over new state. | **PASS** |
| Occupied ownership slots | `RedisStore.SubscriptionSlots` reports only slots with subscriptions, so the ownership loop claims occupied state shards instead of renewing 256 empty owner records. `TestSlotReconcileOwnsHRWTargetAndHeldLease` covers the occupied-slot claim path. | **PASS** |
| Local integration scope | `go test -count=1 ./webhook ./metrics ./jepsen/checker` ran against local Redis and covered migrated subs served from the new tag, high-slot List/GetMany fan-out reachability, metrics, and checker unit regressions. `make conformance` passed 332/332 against a clean `chronicle-hs15-redis-codex` Redis 7 container. | **PASS** |

Blocked gates not run locally:

| Gate | Status |
| --- | --- |
| T5 live k3d slot-isolation nemesis | **NOT RUN** in this worker. The local tests cover the static CROSSSLOT guard and scatter-gather equality preconditions, but no live slot-owner-churn nemesis was executed. |
| T1/T2/T4/L1/L3 live regression suite | **NOT RUN** here beyond `jepsen/checker` unit tests. These need a healthy k3d run. |
| Stress: sparse-wide streams plus slot-owner churn | **PARTIAL / LOCAL ONLY**. Sparse occupied-bit behavior is covered locally; churn during fan-out was not run. |
| Load gate #2 | **BLOCKED / DEFERRED TO ORCHESTRATOR GKE RUN**. This worker did not provision a real multi-node Redis Cluster or GKE load rig, so no S=2/4/8/256 fan-out p99 or K=10k sweep-baseline number is recorded here. Slot-homing viability still depends on that real-cluster measurement. |

Resource note: local Redis was started with the repository `make redis-up` target for
integration tests and torn down with `make redis-down`. The compose resource name did not
include the requested `-codex` suffix because it is fixed by the repo/worktree compose
project name. The conformance rerun used a separate `chronicle-hs15-redis-codex`
container on port 6379, also torn down after the run; no Redis container, network, or
volume was left running.

Local k3d attempt on 2026-06-18:

- Command: `CLUSTER=chronicle-jepsen-codex jepsen/up.sh`.
- The script built the Linux binary and Docker image, created k3d containers, and
  then hung in `k3d cluster create chronicle-jepsen-codex --servers 1 -p
  4438:30437@loadbalancer --wait` after logging `Starting node
  'k3d-chronicle-jepsen-codex-server-0'`.
- `k3d cluster list` showed `chronicle-jepsen-codex 1/1`, but
  `kubectl --context k3d-chronicle-jepsen-codex get nodes --request-timeout=10s`
  failed with `context was not found for specified context:
  k3d-chronicle-jepsen-codex`.
- Cleanup command: `CLUSTER=chronicle-jepsen-codex jepsen/down.sh`. It deleted the
  cluster, network, and attached volume. Follow-up `docker ps --filter
  name=chronicle-jepsen-codex` and `k3d cluster list | rg
  chronicle-jepsen-codex` returned no resources.

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
Redis HASH field and boot/reconnect recovery re-evaluates those durable cursors,
with a coarse floor for the eventless case; the freshly-started origins recompute
`HasPendingWork` and re-fire. The 12/12 result is that recovery: deliveries
observed *after* the accepting origins no longer exist.

This is the empirical form of research 07 §6.3 — a durable cursor is necessary but
not sufficient; recovery is what asks the question the cursor makes answerable.

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
