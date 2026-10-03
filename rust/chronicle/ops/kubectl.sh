#!/usr/bin/env bash
set -euo pipefail

CLUSTER=${CLUSTER:-chronicle-rust}
[[ "$CLUSTER" == chronicle-rust ]] || { echo "refusing unexpected cluster: $CLUSTER" >&2; exit 2; }
if command -v kubectl >/dev/null 2>&1; then
  KUBECONFIG=${KUBECONFIG:-/tmp/chronicle-kubeconfig} exec kubectl "$@"
fi
sudo docker inspect "k3d-${CLUSTER}-server-0" >/dev/null 2>&1 || {
  echo "neither kubectl nor guarded k3d server container is available" >&2
  exit 2
}
exec sudo docker exec -i "k3d-${CLUSTER}-server-0" kubectl "$@"
