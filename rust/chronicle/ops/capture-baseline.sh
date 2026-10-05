#!/usr/bin/env bash
# Offline fixture capture, not a live SQLite copy or an initialization procedure.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
k="$root/ops/kubectl.sh"
out=${1:?new private output directory required}
[[ "$($k config current-context)" == k3d-chronicle-rust ]] || exit 2
[[ "$($k -n chronicle get statefulset chronicle -o jsonpath='{.spec.replicas}')" == 5 ]] || {
  echo 'requires the known five-node baseline' >&2; exit 2;
}
mkdir "$out"
out=$(realpath "$out")
# Kubelet image GC can evict the stopped application's Never-pull image. Leave
# headroom for archives, and retain a verified local image for restoration.
df -P "$out" | awk 'NR == 2 { gsub(/%/, "", $5); if ($5 >= 80) exit 1 }' || {
  echo 'free generated build artifacts before capture; filesystem usage must be below 80%' >&2; exit 2;
}
image=$("$k" -n chronicle get statefulset chronicle -o jsonpath='{.spec.template.spec.containers[?(@.name=="chronicle")].image}')
sudo docker image inspect "$image" --format '{{.Id}}' > "$out/docker-image.txt"
"$k" -n chronicle get pods -l app=chronicle-raft -o json |
  jq '[.items[]|{name:.metadata.name,uid:.metadata.uid,node:.spec.nodeName,
      containers:[.status.containerStatuses[]|{name,image,imageID,containerID,restartCount}]}]' > "$out/processes.json"
"$k" get pv -o json | jq '[.items[]|select(.spec.claimRef.namespace=="chronicle")|
  {claim:.spec.claimRef.name,path:.spec.local.path,
   node:.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values[0]}]' > "$out/volumes.json"
[[ $(jq 'length' "$out/volumes.json") == 5 ]] || exit 2
git -C "$root" rev-parse HEAD > "$out/source.txt"
cp "$root/Cargo.lock" "$out/Cargo.lock"
cp "$root/target/release/chronicle-raft" "$out/chronicle-raft-0.9"

restore() {
  local failed=0
  sudo k3d image import -c chronicle-rust "$image" || failed=1
  "$k" -n chronicle scale statefulset/chronicle --replicas=5 || failed=1
  "$k" -n chronicle rollout status statefulset/chronicle --timeout=180s || failed=1
  return "$failed"
}
trap restore EXIT
"$k" -n chronicle scale statefulset/chronicle --replicas=0
"$k" -n chronicle wait --for=delete pod -l app=chronicle-raft --timeout=120s
while IFS=$'\t' read -r claim node path; do
  [[ "$claim" =~ ^data-chronicle-[0-4]$ && "$node" =~ ^k3d-chronicle-rust-agent-[0-9]+(-[0-9]+)?$ ]]
  [[ "$path" == /var/lib/rancher/k3s/storage/pvc-*"_chronicle_$claim" ]]
  # Include WAL/SHM, ownership files and projections. All owning processes have
  # exited. Do not extract or launch these duplicate identities on the old network.
  sudo docker exec "$node" tar -czf - -C "$path" . > "$out/$claim.tar.gz"
  gzip -t "$out/$claim.tar.gz"
done < <(jq -r '.[]|[.claim,.node,.path]|@tsv' "$out/volumes.json")
sha256sum "$out"/*.tar.gz "$out/chronicle-raft-0.9" "$out/Cargo.lock" > "$out/SHA256SUMS"
printf '%s\n' 'Stopped-process PVC capture; may require WAL recovery, not proof of clean shutdown.' \
  'Duplicate identities must remain network-isolated from the baseline.' > "$out/README.txt"
chmod -R a-w "$out"
echo "Baseline captured at $out"
