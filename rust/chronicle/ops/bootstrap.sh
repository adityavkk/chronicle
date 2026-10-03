#!/usr/bin/env bash
set -euo pipefail

CLUSTER=${CLUSTER:-chronicle-rust}
KUBECONFIG=${KUBECONFIG:-/tmp/chronicle-kubeconfig}
export KUBECONFIG
K="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/kubectl.sh"
[[ "$CLUSTER" == chronicle-rust ]] || { echo "refusing unexpected cluster: $CLUSTER" >&2; exit 2; }

# ClusterIP only: this deliberately executes inside the cluster. The endpoint must be idempotent.
"$K" -n chronicle run bootstrap --rm -i --restart=Never \
  --image=curlimages/curl@sha256:463eaf6072688fe96ac64fa623fe73e1dbe25d8ad6c34404a669ad3ce1f104b6 \
  --command -- curl -fsS -X POST http://chronicle-0.chronicle:8080/admin/bootstrap
echo
"$K" -n chronicle run status --rm -i --restart=Never \
  --image=curlimages/curl@sha256:463eaf6072688fe96ac64fa623fe73e1dbe25d8ad6c34404a669ad3ce1f104b6 \
  --command -- curl -fsS http://chronicle-http:8080/admin/status
