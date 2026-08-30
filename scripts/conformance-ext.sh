#!/usr/bin/env bash
# Run the Write Fencing extension conformance suite (test/conformance-ext)
# against chronicle + Redis (#183, design §H.3).
#
# Usage:
#   scripts/conformance-ext.sh                 # full run (CI gate)
#   scripts/conformance-ext.sh -t "WF-16"      # extra args forwarded to vitest
#   scripts/conformance-ext.sh client.test.ts  # one file (the pinned-client control)
#
# Mirrors scripts/conformance.sh, with the extension suite's two servers:
#   - an ENFORCE-mode chronicle with the ext-suite service policy
#     (test/conformance-ext/policy.json) — the suite's main target;
#   - an INSECURE-mode chronicle on its own Redis DB for the negative
#     controls that pin today's open posture (NC-01, NC-04).
# Both run with a 500ms long-poll and --webhook-allow-private (WF-27 delivers
# to a 127.0.0.1 receiver).
#
# Environment overrides (defaults preserve local behavior exactly when unset):
#   CHRONICLE_EXT_PORT           enforce-mode listen port         (default 4439)
#   CHRONICLE_EXT_INSECURE_PORT  insecure-mode listen port        (default 4440)
#   CHRONICLE_REDIS_URL          redis URL, MUST end in /<db>     (default redis://localhost:6379/<db>)
#   CHRONICLE_REDIS_DB           redis DB for the enforce server  (default 11)
#   CHRONICLE_EXT_INSECURE_DB    redis DB for the insecure server (default 10)
#   CHRONICLE_EXT_BEARER         ext-suite service bearer token
#   CHRONICLE_SKIP_REDIS_START   set to skip `docker compose up redis`
#   CHRONICLE_REDIS_CONTAINER    container name for the flushdb exec
#   CHRONICLE_LOG_LEVEL          server log level                 (default info)
#   CHRONICLE_BUILD_TAGS         go build tags — the fault-injection controls
#                                (fence_fault_nobind | fence_fault_noseal |
#                                fence_fault_nopair) build a deliberately
#                                broken server whose named test MUST fail
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${CHRONICLE_EXT_PORT:-4439}"
INSECURE_PORT="${CHRONICLE_EXT_INSECURE_PORT:-4440}"
REDIS_DB="${CHRONICLE_REDIS_DB:-11}"
INSECURE_DB="${CHRONICLE_EXT_INSECURE_DB:-10}"
REDIS_URL="${CHRONICLE_REDIS_URL:-redis://localhost:6379/${REDIS_DB}}"
INSECURE_REDIS_URL="${REDIS_URL%/*}/${INSECURE_DB}"
BEARER="${CHRONICLE_EXT_BEARER:-conformance-ext-bearer}"
BASE_URL="http://localhost:${PORT}"
INSECURE_URL="http://localhost:${INSECURE_PORT}"

if [ -n "${CHRONICLE_SKIP_REDIS_START:-}" ]; then
  echo "==> skipping redis start (CHRONICLE_SKIP_REDIS_START set)"
else
  echo "==> starting redis"
  docker compose up -d --wait redis
fi

flush_db() {
  local url="$1" db="$2"
  if [ -n "${CHRONICLE_REDIS_CONTAINER:-}" ]; then
    docker exec -i "${CHRONICLE_REDIS_CONTAINER}" redis-cli -n "${db}" flushdb >/dev/null
  elif [ -n "${CHRONICLE_SKIP_REDIS_START:-}" ]; then
    redis-cli -u "${url}" flushdb >/dev/null
  else
    docker compose exec -T redis redis-cli -n "${db}" flushdb >/dev/null
  fi
}

echo "==> flushing extension-conformance dbs ${REDIS_DB} (enforce) and ${INSECURE_DB} (insecure)"
flush_db "${REDIS_URL}" "${REDIS_DB}"
flush_db "${INSECURE_REDIS_URL}" "${INSECURE_DB}"

echo "==> building chronicle${CHRONICLE_BUILD_TAGS:+ (tags: ${CHRONICLE_BUILD_TAGS})}"
# shellcheck disable=SC2086
go build ${CHRONICLE_BUILD_TAGS:+-tags "${CHRONICLE_BUILD_TAGS}"} -o bin/chronicle-ext ./cmd/chronicle

echo "==> starting enforce-mode chronicle on :${PORT} (ext-suite policy, long-poll 500ms)"
CHRONICLE_LISTEN=":${PORT}" \
CHRONICLE_REDIS_URL="${REDIS_URL}" \
CHRONICLE_LONG_POLL_TIMEOUT="500ms" \
CHRONICLE_AUTH_MODE="enforce" \
CHRONICLE_SERVICE_BEARER="ext-suite:${BEARER}" \
CHRONICLE_SERVICE_POLICY_FILE="test/conformance-ext/policy.json" \
  ./bin/chronicle-ext -log-level "${CHRONICLE_LOG_LEVEL:-info}" --webhook-allow-private &
SERVER_PID=$!

echo "==> starting insecure-mode chronicle on :${INSECURE_PORT} (negative controls)"
CHRONICLE_LISTEN=":${INSECURE_PORT}" \
CHRONICLE_REDIS_URL="${INSECURE_REDIS_URL}" \
CHRONICLE_LONG_POLL_TIMEOUT="500ms" \
  ./bin/chronicle-ext -log-level "${CHRONICLE_LOG_LEVEL:-info}" --webhook-allow-private &
INSECURE_PID=$!
trap 'kill "${SERVER_PID}" "${INSECURE_PID}" 2>/dev/null || true' EXIT

echo "==> waiting for readiness"
for i in $(seq 1 50); do
  if curl -sf -X PUT -H "Authorization: Bearer ${BEARER}" "${BASE_URL}/v1/stream/fence/__health__" >/dev/null 2>&1 &&
     curl -sf -X PUT "${INSECURE_URL}/v1/stream/fence/__health__" >/dev/null 2>&1; then
    curl -sf -X DELETE -H "Authorization: Bearer ${BEARER}" "${BASE_URL}/v1/stream/fence/__health__" >/dev/null 2>&1 || true
    curl -sf -X DELETE "${INSECURE_URL}/v1/stream/fence/__health__" >/dev/null 2>&1 || true
    break
  fi
  if ! kill -0 "${SERVER_PID}" 2>/dev/null || ! kill -0 "${INSECURE_PID}" 2>/dev/null; then
    echo "chronicle exited before becoming ready" >&2
    exit 1
  fi
  sleep 0.2
done

echo "==> installing extension conformance suite"
npm --prefix test/conformance-ext install --no-audit --no-fund --silent

echo "==> running extension conformance suite against ${BASE_URL}"
(cd test/conformance-ext && \
  CONFORMANCE_EXT_URL="${BASE_URL}" \
  CONFORMANCE_EXT_INSECURE_URL="${INSECURE_URL}" \
  CONFORMANCE_EXT_BEARER="${BEARER}" \
  npx vitest run --no-coverage --reporter=default "$@")
