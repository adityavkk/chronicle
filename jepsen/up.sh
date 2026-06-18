#!/usr/bin/env bash
# Bring up the Jepsen test environment: a k3d cluster running chronicle (2
# replicas) + Redis (AOF on a PVC). Builds a static chronicle binary on the host
# and bakes it into a minimal image imported into the cluster.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER="${CLUSTER:-chronicle-jepsen}"
CTX="k3d-${CLUSTER}"
ARCH="$(uname -m)"
GOARCH="arm64"; [ "$ARCH" = "x86_64" ] && GOARCH="amd64"

echo "==> building static chronicle binary (linux/${GOARCH})"
mkdir -p jepsen/bin
GOOS=linux GOARCH="$GOARCH" CGO_ENABLED=0 go build -o jepsen/bin/chronicle-linux ./cmd/chronicle

echo "==> building image chronicle:jepsen"
docker build -q -t chronicle:jepsen -f jepsen/Dockerfile jepsen >/dev/null

if ! k3d cluster list 2>/dev/null | awk '{print $1}' | grep -qx "$CLUSTER"; then
  echo "==> raising inotify limits (best effort) and creating cluster ${CLUSTER}"
  colima ssh -- sudo sysctl -w fs.inotify.max_user_instances=1024 fs.inotify.max_user_watches=1048576 >/dev/null 2>&1 || true
  k3d cluster create "$CLUSTER" --servers 1 -p "4438:30437@loadbalancer" --wait
else
  echo "==> reusing existing cluster ${CLUSTER}"
fi

echo "==> importing image into the cluster"
k3d image import chronicle:jepsen -c "$CLUSTER" >/dev/null

echo "==> applying manifests"
kubectl --context "$CTX" apply -f jepsen/deploy/deploy.yaml
kubectl --context "$CTX" -n chronicle-jepsen rollout status deploy/redis --timeout=120s
kubectl --context "$CTX" -n chronicle-jepsen rollout status deploy/chronicle --timeout=120s
echo "==> ready: chronicle reachable at http://localhost:4438"
