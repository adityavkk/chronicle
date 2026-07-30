#!/usr/bin/env bash
# Issue #6 local evidence matrix. Runs the unchanged Redis ZSET baseline and
# all three feature-gated candidates at 8/32/128/512 closed-loop readers.
# Object storage is the filesystem emulator; this script never provisions or
# contacts paid cloud infrastructure.
set -euo pipefail
cd "$(dirname "$0")/.."

CHRONICLE_REPO="${CHRONICLE_REPO:-$(cd .. && pwd)}"
OUT="${OUT:-results/issue-6-local}"
REDIS_URL="${REDIS_URL:-redis://localhost:6379/13}"
REDIS_DB="${REDIS_DB:-13}"
REDIS_CONTAINER="${REDIS_CONTAINER:-}"
REDIS_ADDR="${REDIS_ADDR:-localhost:6379}"
REDIS_RESTART_EACH="${REDIS_RESTART_EACH:-0}"
PORT="${PORT:-4437}"
METRICS_PORT="${METRICS_PORT:-9090}"
BASE_URL="http://localhost:${PORT}"
SCENARIOS="${SCENARIOS:-segment-readers-8 segment-readers-32 segment-readers-128 segment-readers-512 segment-mixed-readers-8 segment-mixed-readers-32 segment-mixed-readers-128 segment-mixed-readers-512}"
MODES="${MODES:-off redis-chunks local-files object-cache}"
SEGMENT_TMP="$(mktemp -d "${TMPDIR:-/tmp}/chronicle-segment-bench.XXXXXX")"
SUT_PID=""

log() { printf '[segment-bench] %s\n' "$*"; }
cleanup() {
  [[ -n "${SUT_PID}" ]] && kill "${SUT_PID}" 2>/dev/null || true
  case "${SEGMENT_TMP}" in
    "${TMPDIR:-/tmp}"/chronicle-segment-bench.*) rm -rf "${SEGMENT_TMP}" ;;
  esac
}
trap cleanup EXIT

mkdir -p "${OUT}"
{
  date -u '+utc=%Y-%m-%dT%H:%M:%SZ'
  uname -a
  go version
  git -C "${CHRONICLE_REPO}" rev-parse HEAD
  printf 'redis_url=%s\nredis_addr=%s\nredis_restart_each=%s\nmodes=%s\nscenarios=%s\n' \
    "${REDIS_URL}" "${REDIS_ADDR}" "${REDIS_RESTART_EACH}" "${MODES}" "${SCENARIOS}"
} > "${OUT}/environment.txt"

make build >/dev/null
(cd "${CHRONICLE_REPO}" && make build >/dev/null)

redis_command() {
  if [[ -n "${REDIS_CONTAINER}" ]]; then
    docker exec -i "${REDIS_CONTAINER}" redis-cli -n "${REDIS_DB}" "$@"
  else
    redis-cli -u "${REDIS_URL}" "$@"
  fi
}

if [[ -n "${REDIS_CONTAINER}" ]] && ! redis_command PING 2>/dev/null | grep -q PONG; then
  echo "Redis container ${REDIS_CONTAINER} is not reachable" >&2
  exit 1
fi
if [[ -z "${REDIS_CONTAINER}" ]] &&
  (! command -v redis-cli >/dev/null || ! redis_command PING 2>/dev/null | grep -q PONG); then
  docker compose -p chronicle-segment-bench -f "${CHRONICLE_REPO}/docker-compose.yml" up -d --wait redis >/dev/null
  REDIS_CONTAINER="chronicle-segment-bench-redis-1"
fi

for mode in ${MODES}; do
  label="chronicle-${mode}"
  for scenario in ${SCENARIOS}; do
    log "starting ${label}: ${scenario}"
    segment_state="shadow"
    segment_auto_seal="false"
    if [[ "${mode}" != "off" ]]; then
      segment_state="serving"
      segment_auto_seal="true"
    fi
    if [[ "${REDIS_RESTART_EACH}" == "1" ]]; then
      if [[ -z "${REDIS_CONTAINER}" ]]; then
        echo "REDIS_RESTART_EACH=1 requires REDIS_CONTAINER" >&2
        exit 1
      fi
      docker restart "${REDIS_CONTAINER}" >/dev/null
      for i in $(seq 1 100); do
        redis_command PING 2>/dev/null | grep -q PONG && break
        sleep 0.1
      done
    fi
    redis_command FLUSHDB >/dev/null
    log_file="${OUT}/${label}-${scenario}.log"
    "${CHRONICLE_REPO}/bin/chronicle" \
      --listen ":${PORT}" \
      --redis-url "${REDIS_URL}" \
      --subscriptions=false \
      --ui=false \
      --metrics-listen ":${METRICS_PORT}" \
      --segment-mode "${mode}" \
      --segment-dir "${SEGMENT_TMP}/${mode}/${scenario}" \
      --segment-initial-state "${segment_state}" \
      --segment-auto-seal-read="${segment_auto_seal}" \
      > "${log_file}" 2>&1 &
    SUT_PID=$!

    for i in $(seq 1 100); do
      if curl -sf -o /dev/null -X PUT "${BASE_URL}/v1/stream/bench/health" \
        -H 'Content-Type: application/json'; then
        curl -sf -o /dev/null -X DELETE "${BASE_URL}/v1/stream/bench/health" || true
        break
      fi
      if ! kill -0 "${SUT_PID}" 2>/dev/null; then
        echo "${label} exited; see ${log_file}" >&2
        exit 1
      fi
      sleep 0.1
    done

    artifact="${OUT}/${label}/${scenario}"
    mkdir -p "${artifact}"
    curl -sf "http://localhost:${METRICS_PORT}/metrics" > "${artifact}/metrics-before.prom"
    redis_command INFO COMMANDSTATS > "${artifact}/redis-commandstats-before.txt"
    bin/dsload run \
      -scenario "scenarios/${scenario}.yaml" \
      -label "${label}" \
      -out "${OUT}" \
      -base-url "${BASE_URL}" \
      -sample-pid "chronicle=${SUT_PID}" \
      -sample-redis "redis=${REDIS_ADDR}" \
      -sample-metrics "chronicle=http://localhost:${METRICS_PORT}/metrics"
    curl -sf "http://localhost:${METRICS_PORT}/metrics" > "${artifact}/metrics-after.prom"
    redis_command INFO COMMANDSTATS > "${artifact}/redis-commandstats-after.txt"
    if [[ "${mode}" != "off" ]]; then
      segment_reads="$(
        awk '$1 == "chronicle_segment_reads_total" { print int($2) }' \
          "${artifact}/metrics-after.prom"
      )"
      segment_seals="$(
        awk '$1 == "chronicle_segment_seals_total" { print int($2) }' \
          "${artifact}/metrics-after.prom"
      )"
      segment_reads="${segment_reads:-0}"
      segment_seals="${segment_seals:-0}"
      if [[ "${segment_reads}" -le 0 || "${segment_seals}" -le 0 ]]; then
        echo "segment candidate was not exercised: mode=${mode} reads=${segment_reads} seals=${segment_seals}" >&2
        exit 1
      fi
    fi

    kill "${SUT_PID}" 2>/dev/null || true
    wait "${SUT_PID}" 2>/dev/null || true
    SUT_PID=""
  done
done

python3 scripts/compare.py "${OUT}" > "${OUT}/comparison.md"
log "complete: ${OUT}"
