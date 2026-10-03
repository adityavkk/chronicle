#!/usr/bin/env bash
set -euo pipefail

CLUSTER=${CLUSTER:-chronicle-rust}
KUBECONFIG=${KUBECONFIG:-/tmp/chronicle-kubeconfig}
export KUBECONFIG
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
K="$ROOT/kubectl.sh"

[[ "$CLUSTER" == chronicle-rust ]] || { echo "refusing unexpected cluster: $CLUSTER" >&2; exit 2; }
[[ -r "$KUBECONFIG" ]] || { echo "missing kubeconfig: $KUBECONFIG" >&2; exit 2; }

cat "$ROOT/observability.yaml" | "$K" apply --server-side -f -
"$K" -n observability rollout restart deployment/otel-collector deployment/grafana
for deployment in victoria-metrics victoria-logs victoria-traces otel-collector grafana; do
  "$K" -n observability rollout status "deployment/$deployment" --timeout=5m
done

if sudo docker image inspect chronicle-raft:dev >/dev/null 2>&1; then
  sudo k3d image import -c "$CLUSTER" chronicle-raft:dev
  cat "$ROOT/chronicle.yaml" | "$K" apply --server-side -f -
  if [[ "${NEW_GENESIS:-0}" == 1 ]]; then
    for ordinal in 0 1 2; do python3 "$ROOT/initialize.py" genesis "$ordinal"; done
  fi
  "$K" -n chronicle rollout status statefulset/chronicle --timeout=5m
else
  echo "chronicle-raft:dev is absent; observability installed, application skipped" >&2
fi

"$ROOT/verify.sh"
