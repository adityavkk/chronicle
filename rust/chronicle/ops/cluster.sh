#!/usr/bin/env bash
# Disposable local k3d only. Docker must already be running.
set -euo pipefail
case "${1:-}" in
  create)
    sudo docker info >/dev/null
    k3d version | grep -F 'v5.8.3' >/dev/null
    if ! sudo k3d cluster list -o json | grep -q '"name": "chronicle-rust"'; then
      sudo k3d cluster create chronicle-rust --servers 1 --agents 3 \
        --image rancher/k3s:v1.32.5-k3s1 --wait --timeout 180s \
        --kubeconfig-update-default=false
    fi
    sudo k3d kubeconfig get chronicle-rust > /tmp/chronicle-kubeconfig
    chmod 600 /tmp/chronicle-kubeconfig
    ;;
  delete)
    [[ "${CONFIRM_DISPOSABLE_DATA_LOSS:-}" == chronicle-rust ]] || {
      echo 'Set CONFIRM_DISPOSABLE_DATA_LOSS=chronicle-rust; deletes all local test data.' >&2; exit 2;
    }
    sudo k3d cluster delete chronicle-rust
    ;;
  *) echo 'usage: cluster.sh create|delete' >&2; exit 2;;
esac
