#!/usr/bin/env bash
# Run the Durable Streams server conformance suite against chronicle + Redis.
#
# Usage:
#   scripts/conformance.sh            # full run (CI gate)
#   scripts/conformance.sh -t "SSE"   # extra args forwarded to vitest
#
# Mirrors the Caddy plugin's test harness: short long-poll timeout, readiness
# probe via PUT+DELETE on a health stream, suite paths under /v1/stream/.
#
# Environment overrides (ergonomics ported from codex-subscription, docs/research/10):
#   CHRONICLE_PORT             chronicle listen port              (default 4437)
#   CHRONICLE_REDIS_URL        redis URL chronicle dials          (default redis://localhost:6379/<db>)
#   CHRONICLE_REDIS_DB         redis DB index to use + flush      (default 15)
#   CHRONICLE_SKIP_REDIS_START set to skip `docker compose up redis`
#                              (use when Redis already runs, e.g. an external/shared instance)
#   CHRONICLE_REDIS_CONTAINER  container name for the flushdb exec
#                              (default: the compose `redis` service)
#   CHRONICLE_LOG_LEVEL        server log level                   (default info)
#   CHRONICLE_SEGMENT_EXPECT_ACTIVITY
#                              serving: require immutable reads > 0
#                              shadow: require seals > 0 and reads == 0
# Defaults preserve the original behavior exactly when these are unset.
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${CHRONICLE_PORT:-4437}"
BASE_URL="http://localhost:${PORT}"
REDIS_DB="${CHRONICLE_REDIS_DB:-15}"
REDIS_URL="${CHRONICLE_REDIS_URL:-redis://localhost:6379/${REDIS_DB}}"

if [ -n "${CHRONICLE_SKIP_REDIS_START:-}" ]; then
  echo "==> skipping redis start (CHRONICLE_SKIP_REDIS_START set)"
else
  echo "==> starting redis"
  docker compose up -d --wait redis
fi

echo "==> flushing conformance db ${REDIS_DB}"
if [ -n "${CHRONICLE_REDIS_CONTAINER:-}" ]; then
  docker exec -i "${CHRONICLE_REDIS_CONTAINER}" redis-cli -n "${REDIS_DB}" flushdb >/dev/null
elif [ -n "${CHRONICLE_SKIP_REDIS_START:-}" ]; then
  redis-cli -u "${REDIS_URL}" flushdb >/dev/null
else
  docker compose exec -T redis redis-cli -n "${REDIS_DB}" flushdb >/dev/null
fi

echo "==> building chronicle"
go build -o bin/chronicle ./cmd/chronicle

echo "==> starting chronicle on :${PORT} (long-poll timeout 500ms)"
CHRONICLE_LISTEN=":${PORT}" \
CHRONICLE_REDIS_URL="${REDIS_URL}" \
CHRONICLE_LONG_POLL_TIMEOUT="500ms" \
  ./bin/chronicle -log-level "${CHRONICLE_LOG_LEVEL:-info}" &
SERVER_PID=$!
trap 'kill "${SERVER_PID}" 2>/dev/null || true' EXIT

echo "==> waiting for readiness"
for i in $(seq 1 50); do
  if curl -sf -X PUT "${BASE_URL}/v1/stream/__health__" >/dev/null 2>&1; then
    curl -sf -X DELETE "${BASE_URL}/v1/stream/__health__" >/dev/null 2>&1 || true
    break
  fi
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "chronicle exited before becoming ready" >&2
    exit 1
  fi
  sleep 0.2
done

echo "==> installing conformance suite"
npm --prefix test/conformance install --no-audit --no-fund --silent

echo "==> running conformance suite against ${BASE_URL}"
# The published CLI points vitest at a file inside node_modules, which
# vitest's default exclude swallows; conformance.test.ts registers the suite
# programmatically instead. Extra args (e.g. -t "SSE") forward to vitest.
(cd test/conformance && CONFORMANCE_TEST_URL="${BASE_URL}" \
  npx vitest run --no-coverage --reporter=default "$@")

if [ -n "${CHRONICLE_SEGMENT_EXPECT_ACTIVITY:-}" ]; then
  if [ -z "${CHRONICLE_METRICS_LISTEN:-}" ]; then
    echo "segment activity assertion requires CHRONICLE_METRICS_LISTEN" >&2
    exit 1
  fi
  metrics_port="${CHRONICLE_METRICS_LISTEN##*:}"
  metrics="$(curl -sf "http://localhost:${metrics_port}/metrics")"
  segment_reads="$(awk '$1 == "chronicle_segment_reads_total" { print int($2) }' <<<"${metrics}")"
  segment_seals="$(awk '$1 == "chronicle_segment_seals_total" { print int($2) }' <<<"${metrics}")"
  segment_reads="${segment_reads:-0}"
  segment_seals="${segment_seals:-0}"
  case "${CHRONICLE_SEGMENT_EXPECT_ACTIVITY}" in
    serving)
      if [ "${segment_reads}" -le 0 ] || [ "${segment_seals}" -le 0 ]; then
        echo "serving candidate was not exercised: reads=${segment_reads} seals=${segment_seals}" >&2
        exit 1
      fi
      ;;
    shadow)
      if [ "${segment_seals}" -le 0 ] || [ "${segment_reads}" -ne 0 ]; then
        echo "shadow candidate activity mismatch: reads=${segment_reads} seals=${segment_seals}" >&2
        exit 1
      fi
      ;;
    *)
      echo "unknown CHRONICLE_SEGMENT_EXPECT_ACTIVITY=${CHRONICLE_SEGMENT_EXPECT_ACTIVITY}" >&2
      exit 1
      ;;
  esac
  echo "==> segment activity: state=${CHRONICLE_SEGMENT_EXPECT_ACTIVITY} reads=${segment_reads} seals=${segment_seals}"
fi
