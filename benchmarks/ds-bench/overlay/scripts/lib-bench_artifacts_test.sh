#!/usr/bin/env bash
# Cluster-free checks for exact benchmark artifact validation.
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DS_TARGET=local KIND_CLUSTER=ds-bench

# shellcheck source=scripts/lib-bench.sh
. "$REPO_ROOT/scripts/lib-bench.sh"

tmp="$(mktemp -d /tmp/lib-bench-artifacts-XXXXXX)"
trap 'rm -rf "$tmp"' EXIT

header="ts_ms,rss_bytes,cpu_ticks,write_bytes,pod_ws_bytes"
printf '%s\n' "$header" > "$tmp/samples.csv"
! _samples_complete "$tmp/samples.csv" ||
  { echo "FAIL: header-only samples accepted"; exit 1; }
printf '1,2,3,4\n' >> "$tmp/samples.csv"
! _samples_complete "$tmp/samples.csv" ||
  { echo "FAIL: malformed samples accepted"; exit 1; }
printf '%s\n1,2,3,4,5\n' "$header" > "$tmp/samples.csv"
_samples_complete "$tmp/samples.csv" ||
  { echo "FAIL: complete samples rejected"; exit 1; }

mkdir "$tmp/fleet"
printf '{}\n' > "$tmp/fleet/reads-0.json"
printf 'hdr\n' > "$tmp/fleet/reads-0.hdr"
PARALLELISM=2
! _fleet_artifacts_complete "$tmp/fleet" ||
  { echo "FAIL: partial fleet accepted"; exit 1; }
printf '{}\n' > "$tmp/fleet/reads-1.json"
printf 'hdr\n' > "$tmp/fleet/reads-1.hdr"
_fleet_artifacts_complete "$tmp/fleet" ||
  { echo "FAIL: complete fleet rejected"; exit 1; }
printf '{\n' > "$tmp/fleet/reads-1.json"
! _fleet_artifacts_complete "$tmp/fleet" ||
  { echo "FAIL: corrupt fleet JSON accepted"; exit 1; }

echo "PASS: benchmark artifact validators require complete evidence"
