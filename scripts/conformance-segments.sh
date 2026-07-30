#!/usr/bin/env bash
# Run the complete protocol suite against the default ZSET layout and each
# feature-gated immutable segment candidate. No remote object store is used:
# object-cache points at the deterministic filesystem emulator.
#
# Read-triggered sealing is disabled because that is the fail-closed candidate
# default. Initial-body and stream-close operations still seal. The gate checks
# metrics so a serving candidate cannot pass by silently using only Redis, and
# it checks that shadow candidates seal without serving immutable bytes.
set -euo pipefail
cd "$(dirname "$0")/.."

SEGMENT_TMP="$(mktemp -d "${TMPDIR:-/tmp}/chronicle-segment-conformance.XXXXXX")"
cleanup() {
  case "${SEGMENT_TMP}" in
    "${TMPDIR:-/tmp}"/chronicle-segment-conformance.*) rm -rf "${SEGMENT_TMP}" ;;
  esac
}
trap cleanup EXIT

matrix=(
  "off:shadow"
  "redis-chunks:shadow"
  "redis-chunks:serving"
  "local-files:shadow"
  "local-files:serving"
  "object-cache:shadow"
  "object-cache:serving"
)
for cell in "${matrix[@]}"; do
  mode="${cell%%:*}"
  state="${cell##*:}"
  activity=""
  if [ "${mode}" != "off" ]; then
    activity="${state}"
  fi
  echo "==> conformance segment mode: ${mode}, state: ${state}"
  CHRONICLE_SEGMENT_MODE="${mode}" \
  CHRONICLE_SEGMENT_DIR="${SEGMENT_TMP}/${mode}-${state}" \
  CHRONICLE_SEGMENT_INITIAL_STATE="${state}" \
  CHRONICLE_SEGMENT_AUTO_SEAL_READ="${CHRONICLE_SEGMENT_AUTO_SEAL_READ:-false}" \
  CHRONICLE_SEGMENT_EXPECT_ACTIVITY="${activity}" \
  CHRONICLE_METRICS_LISTEN="${CHRONICLE_METRICS_LISTEN:-:9448}" \
  CHRONICLE_LOG_LEVEL="${CHRONICLE_LOG_LEVEL:-warn}" \
    ./scripts/conformance.sh "$@"
done
