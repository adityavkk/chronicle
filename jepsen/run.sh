#!/usr/bin/env bash
# Run the Jepsen-style durability scenarios against the running cluster and
# capture the output. Pass scenario names as args, or run the default set.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER="${CLUSTER:-chronicle-jepsen}"
BASE="${BASE:-http://localhost:4438}"
STREAMS="${STREAMS:-8}"
MSGS="${MSGS:-40}"
SCENARIOS=("$@")
[ ${#SCENARIOS[@]} -eq 0 ] && SCENARIOS=(baseline origin-restart redis-restart paged-catchup read-expiry sse-resume)

echo "==> building checker"
go build -o jepsen/bin/jepsen-checker ./jepsen/checker

rc=0
for s in "${SCENARIOS[@]}"; do
  echo
  echo "############################################################"
  echo "# scenario: $s"
  echo "############################################################"
  # Reset the keyspace and roll the deployments so each scenario starts clean.
  kubectl --context "k3d-$CLUSTER" -n chronicle-jepsen exec deploy/redis -- redis-cli -n 0 flushdb >/dev/null
  kubectl --context "k3d-$CLUSTER" -n chronicle-jepsen rollout restart deploy/redis deploy/chronicle >/dev/null
  kubectl --context "k3d-$CLUSTER" -n chronicle-jepsen rollout status deploy/redis --timeout=120s
  kubectl --context "k3d-$CLUSTER" -n chronicle-jepsen rollout status deploy/chronicle --timeout=120s
  jepsen/bin/jepsen-checker \
    -base "$BASE" -cluster "$CLUSTER" \
    -streams "$STREAMS" -msgs "$MSGS" -scenario "$s" || rc=1
done
exit $rc
