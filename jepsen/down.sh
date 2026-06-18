#!/usr/bin/env bash
# Tear down the Jepsen test cluster.
set -euo pipefail
CLUSTER="${CLUSTER:-chronicle-jepsen}"
k3d cluster delete "$CLUSTER"
