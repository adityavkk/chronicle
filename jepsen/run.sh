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
[ ${#SCENARIOS[@]} -eq 0 ] && SCENARIOS=(baseline origin-restart redis-restart)

echo "==> building checker"
go build -o jepsen/bin/jepsen-checker ./jepsen/checker

rc=0
for s in "${SCENARIOS[@]}"; do
  echo
  echo "############################################################"
  echo "# scenario: $s"
  echo "############################################################"
  # Reset the keyspace and roll the deployments so each scenario starts clean.
  kubectl --context "k3d-$CLUSTER" -n chronicle-jepsen exec deploy/redis -- redis-cli -n 0 flushdb >/dev/null 2>&1 || true
  jepsen/bin/jepsen-checker \
    -base "$BASE" -cluster "$CLUSTER" \
    -streams "$STREAMS" -msgs "$MSGS" -scenario "$s" || rc=1
done
exit $rc
