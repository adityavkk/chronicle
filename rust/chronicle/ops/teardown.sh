#!/usr/bin/env bash
set -euo pipefail

CLUSTER=${CLUSTER:-chronicle-rust}
KUBECONFIG=${KUBECONFIG:-/tmp/chronicle-kubeconfig}
export KUBECONFIG
K="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/kubectl.sh"
[[ "$CLUSTER" == chronicle-rust ]] || { echo "refusing unexpected cluster: $CLUSTER" >&2; exit 2; }
[[ -r "$KUBECONFIG" ]] || { echo "missing kubeconfig: $KUBECONFIG" >&2; exit 2; }

# Namespace deletion removes local-path PVCs and all local test data, but never deletes the cluster.
"$K" delete namespace chronicle observability --ignore-not-found --wait=true
