#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

K() { :; }
_wait_server_http() { :; }
MANIFEST_VARS=""
SERVER_CPU=4
DS_TARGET=remote
. scripts/chronicle-topology.sh

chronicle_configure "always:2:1:2000:2000:4096:12288:legacy"
[ "$CHRONICLE_TOPOLOGY" = "shared" ]
[ "$CHRONICLE_CPU_PER_POD" = "1000" ]
[ "$REDIS_CPU_PER_POD" = "2000" ]
[ "$CHRONICLE_REDIS_URL" = "redis://chronicle-redis:6379/15" ]

chronicle_configure "always:4:3:2000:2000:4096:12288:persistent"
[ "$CHRONICLE_TOPOLOGY" = "cluster" ]
[ "$CHRONICLE_CPU_PER_POD" = "500" ]
[ "$REDIS_CPU_PER_POD" = "666" ]
[ "$REDIS_MEMORY_PER_POD" = "4096" ]
[ "$CHRONICLE_SSE_PERSISTENT_WAIT" = "true" ]
echo "$CHRONICLE_REDIS_URL" | grep -q "redis+cluster://"

! chronicle_configure "always:3:1:2000:2000:4096:12288:legacy"
! chronicle_configure "always:2:2:2000:2000:4096:12288:legacy"
! chronicle_configure "always:2:1:2500:2000:4096:12288:legacy"
! chronicle_configure "always:2:1:2000:2000:4096:8192:legacy"
! chronicle_configure "always:2:1:2000:2000:4096:12288:other"

chronicle_configure "always:2:2"
[ "$CHRONICLE_TOPOLOGY" = "colocated" ]
[ "$CHRONICLE_CPU" = "2" ]
[ "$REDIS_CPU" = "2" ]

reset_calls=0
CHRONICLE_CAPTURE_PPROF=0
sleep() { :; }
K() {
  if [ "$1" = "get" ] && [ "$2" = "pod" ]; then
    echo "chronicle-test-0"
  elif [ "$1" = "exec" ]; then
    reset_calls=$((reset_calls + 1))
    [ "$reset_calls" = "5" ]
  else
    return 0
  fi
}
chronicle_reset_sidecar_samples
[ "$reset_calls" = "5" ]

tmp="$(mktemp -d /tmp/chronicle-topology-XXXXXX)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/sut-samples" "$tmp/redis-info"
touch "$tmp/sut-samples/chronicle-old.csv"
touch "$tmp/redis-info/redis-old.txt"
touch "$tmp/samples.csv" "$tmp/sockets-old.txt" "$tmp/chronicle-endpoints.json"

chronicle_prepare_collect_dest "$tmp"
[ -z "$(find "$tmp/sut-samples" -type f -print -quit)" ]
[ -z "$(find "$tmp/redis-info" -type f -print -quit)" ]
[ ! -e "$tmp/samples.csv" ]
[ ! -e "$tmp/sockets-old.txt" ]
[ ! -e "$tmp/chronicle-endpoints.json" ]

echo "PASS chronicle-topology"
