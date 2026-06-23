#!/usr/bin/env bash
# gate #5 — the failover-fence drill (the #16 headline). 07's L3 (a stranded webhook
# wake recovered ONLY by the cursor-reading reconciler + a deposed ack FENCED) run
# at the REAL-failover level on the STANDARD_HA substrate — the thing the single
# Redis `deploy.yaml` cannot test (07 honest-gap #3: it only replays AOF).
#
# Sequence:
#   1. Apply standard-ha.yaml (primary + AOF replica + a stable `redis` endpoint).
#   2. Establish the L3 property on the substrate with the existing lease-tail-drop
#      checker (ZREM the lease tail; assert recovery + deposed FENCED) — Tier B's
#      WAITAOF 1 1 now has a real replica to ack.
#   3. Inject a REAL failover: kill the primary, promote the replica
#      (REPLICAOF NO ONE), and flip the stable `redis` Service to the promoted node.
#      chronicle reconnects to the same DNS name; a rollout-restart runs the boot
#      reconcile (the same eager reconcile Manager.Promote drives), recovering any
#      sub stranded in the async-replication RPO window.
#   4. Re-run lease-tail-drop across the promoted primary to prove the property
#      survives a real failover, and record RPO/RTO.
#
# RPO = async replication lag + AOF fsync (~appendfsync everysec, ~1s) + link
#       latency — the window of writes a primary loss can drop (bounded, >0; Tier B
#       WAITAOF 1 1 shrinks it to the replica-fsync ack).
# RTO = promotion time (REPLICAOF NO ONE + endpoint flip + chronicle reconnect/boot
#       reconcile).
#
# STOP-THE-METER: this script ALWAYS tears the substrate down (trap on EXIT/INT/TERM).
#
# PENDING-CLOUD: the worktree does not run clusters; the orchestrator runs this.
# The managed path is Memorystore STANDARD_HA tier (or any managed Redis 8 HA SKU)
# via `loadtest/ltctl.sh gate5` — there the provider performs the promotion + endpoint
# repoint and this script's steps 3 collapse into "trigger the managed failover".
set -euo pipefail
cd "$(dirname "$0")/.."

CTX="${CTX:-k3d-chronicle-jepsen-claude}"   # NEVER k3d-bakeoff-*
NS="chronicle-jepsen-ha"
BASE="${BASE:-http://localhost:4438}"
K() { kubectl --context "$CTX" -n "$NS" "$@"; }

pf_pid=""
teardown() {
  [ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null || true
  echo "==> teardown: deleting namespace $NS (meter stopped)"
  kubectl --context "$CTX" delete namespace "$NS" --wait=false 2>/dev/null || true
}
trap teardown EXIT INT TERM

echo "==> apply STANDARD_HA substrate"
kubectl --context "$CTX" apply -f deploy/standard-ha.yaml
K rollout status deploy/redis-primary --timeout=120s
K rollout status deploy/redis-replica --timeout=120s
K rollout status deploy/chronicle --timeout=180s

echo "==> wait for replication to attach (master_link_status:up)"
for _ in $(seq 1 30); do
  if K exec deploy/redis-replica -- redis-cli info replication 2>/dev/null | grep -q 'master_link_status:up'; then
    echo "   replica attached"; break
  fi; sleep 2
done

echo "==> port-forward chronicle"
K port-forward svc/chronicle 4438:4437 >/dev/null 2>&1 &
pf_pid=$!
sleep 3

echo "==> build checker"
go build -o jepsen/bin/jepsen-checker ./jepsen/checker

echo "############################################################"
echo "# gate #5a: L3 lease-tail-drop on STANDARD_HA (pre-failover, Tier B WAITAOF 1 1)"
echo "############################################################"
# -floor=0 selects the SHARPENED variant: recovery within lease_ttl_ms+RTT proves
# the EAGER reconcile (not a 30s floor tick) recovered the stranded sub, and the
# deposed holder's late ack is asserted FENCED.
jepsen/bin/jepsen-checker -base "$BASE" -cluster "${CTX#k3d-}" -namespace "$NS" \
  -scenario lease-tail-drop -floor 0

echo "############################################################"
echo "# gate #5b: REAL failover — promote the replica, flip the endpoint"
echo "############################################################"
t_fail_start=$(date +%s%3N)
echo "==> kill the primary"
K delete pod -l app=redis,role=primary --grace-period=0 --force
echo "==> promote the replica (REPLICAOF NO ONE)"
K exec deploy/redis-replica -- redis-cli REPLICAOF NO ONE
echo "==> flip the stable 'redis' endpoint to the promoted node (role: replica)"
K patch service redis -p '{"spec":{"selector":{"app":"redis","role":"replica"}}}'
echo "==> roll chronicle so each pod reconnects to the promoted primary and runs the boot reconcile"
K rollout restart deploy/chronicle
K rollout status deploy/chronicle --timeout=180s
t_fail_end=$(date +%s%3N)
echo "==> RTO (promotion + endpoint flip + chronicle reconnect/boot reconcile): $((t_fail_end - t_fail_start)) ms"
# RPO note: with appendfsync everysec + a synchronously-acked WAITAOF 1 1 on the
# fence-minting write, the durable RPO for a Tier B wake is the replica-fsync ack
# (~0 for the acked write); for non-Tier-B state it is the async lag + ~1s fsync.

# re-establish the port-forward (the rollout replaced the pods)
kill "$pf_pid" 2>/dev/null || true
K port-forward svc/chronicle 4438:4437 >/dev/null 2>&1 &
pf_pid=$!
sleep 3

echo "############################################################"
echo "# gate #5c: L3 lease-tail-drop AFTER the real failover (on the promoted primary)"
echo "############################################################"
jepsen/bin/jepsen-checker -base "$BASE" -cluster "${CTX#k3d-}" -namespace "$NS" \
  -scenario lease-tail-drop -floor 0

echo "############################################################"
echo "# gate #5d: the ASSERTING failover scenario (#43) — at-least-once + deposed-FENCED"
echo "#           across a REAL promotion, with empirical RPO/RTO"
echo "############################################################"
# This is the issue-#43 deliverable: rather than re-running lease-tail-drop, drive
# the dedicated `failover` scenario, which itself injects the real primary loss +
# replica promotion mid-flight (the redisFailover nemesis), waits for the boot
# reconcile, then asserts via CheckAtLeastOnce that every linked stream still
# reaches acked_offset == tail (a dropped fence-write degraded ONLY to at-least-once,
# deduped by the monotone cursor) AND that a deposed worker's late ack is 409 FENCED
# (no double-grant / cursor regression survived the promotion). It prints a single
# machine-readable verdict line (GATE5-FAILOVER-VERDICT: PASS|FAIL) plus the
# durability-honest RPO/RTO tiers. We re-apply the substrate so the scenario has a
# fresh primary/replica pair to fail over (gate #5b already deposed the first one).
echo "==> re-apply STANDARD_HA so the asserting scenario has a fresh primary to fail over"
kubectl --context "$CTX" apply -f deploy/standard-ha.yaml
K rollout status deploy/redis-primary --timeout=120s
K rollout status deploy/redis-replica --timeout=120s
K rollout status deploy/chronicle --timeout=180s
# re-point the stable endpoint back to the primary (gate #5b left it on the replica)
K patch service redis -p '{"spec":{"selector":{"app":"redis","role":"primary"}}}'
for _ in $(seq 1 30); do
  K exec deploy/redis-replica -- redis-cli info replication 2>/dev/null | grep -q 'master_link_status:up' && break
  sleep 2
done
kill "$pf_pid" 2>/dev/null || true
K port-forward svc/chronicle 4438:4437 >/dev/null 2>&1 &
pf_pid=$!
sleep 3

set +e
verdict_out="$(jepsen/bin/jepsen-checker -base "$BASE" -cluster "${CTX#k3d-}" -namespace "$NS" \
  -scenario failover -streams 6 -msgs 30 2>&1)"
verdict_rc=$?
set -e
echo "$verdict_out"
echo "$verdict_out" | grep -E '^GATE5-FAILOVER-VERDICT|empirical RPO|empirical RTO|CLAIM:' || true

echo "==> gate #5 PASS criteria: gate #5a/#5b/#5c lease-tail-drop runs exit 0 (the stranded"
echo "    sub is recovered ONLY by the cursor-reading eager reconcile and the deposed ack is"
echo "    409 FENCED across a real promotion), AND gate #5d prints GATE5-FAILOVER-VERDICT: PASS"
echo "    (every linked stream reached tail = at-least-once; the deposed ack was FENCED; the"
echo "    empirical RPO/RTO are recorded as durability-honest tiers, NOT a strong-consistency"
echo "    claim). Teardown runs on exit (STOP THE METER)."
exit $verdict_rc
