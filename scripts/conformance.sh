#!/usr/bin/env bash
# Run the Durable Streams server conformance suite against chronicle + Redis.
#
# Usage:
#   scripts/conformance.sh            # full run (CI gate)
#   scripts/conformance.sh -t "SSE"   # extra args forwarded to vitest
#
# Mirrors the Caddy plugin's test harness: short long-poll timeout, readiness
# probe via PUT+DELETE on a health stream, suite paths under /v1/stream/.
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${CHRONICLE_PORT:-4437}"
BASE_URL="http://localhost:${PORT}"
REDIS_URL="${CHRONICLE_REDIS_URL:-redis://localhost:6379/15}"

echo "==> starting redis"
docker compose up -d --wait redis

echo "==> flushing conformance db"
docker compose exec -T redis redis-cli -n 15 flushdb >/dev/null

echo "==> building chronicle"
go build -o bin/chronicle ./cmd/chronicle

echo "==> starting chronicle on :${PORT} (long-poll timeout 500ms)"
CHRONICLE_LISTEN=":${PORT}" \
CHRONICLE_REDIS_URL="${REDIS_URL}" \
CHRONICLE_LONG_POLL_TIMEOUT="500ms" \
  ./bin/chronicle &
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
if [ "$#" -gt 0 ]; then
  # Forward filters straight to vitest against the pinned runner.
  CONFORMANCE_TEST_URL="${BASE_URL}" \
    npx --prefix test/conformance vitest run \
    test/conformance/node_modules/@durable-streams/server-conformance-tests/dist/test-runner.js \
    --no-coverage --passWithNoTests=false "$@"
else
  npx --prefix test/conformance server-conformance-tests --run "${BASE_URL}"
fi
