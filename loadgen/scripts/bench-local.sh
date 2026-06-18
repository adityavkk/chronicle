#!/usr/bin/env bash
# bench-local.sh — run the full dsload scenario suite against one SUT.
#
#   scripts/bench-local.sh caddy-mem        # Caddy plugin, memory store
#   scripts/bench-local.sh caddy-file       # Caddy plugin, file store (bbolt)
#   scripts/bench-local.sh chronicle-redis  # Chronicle on Redis 8 (docker)
#
# Env overrides:
#   OUT=dir           results root        (default: results)
#   SCENARIOS="a b"   scenario names      (default: full benchmark suite)
#   CADDY_BIN=path    prebuilt caddy-plugin server binary
#   CHRONICLE_BIN=path prebuilt chronicle binary
#   DS_REPO=path      durable-streams repo (default: ../../durable-streams)
#   CHRONICLE_REPO=path chronicle repo     (default: ../..; the worktree root)
set -euo pipefail
cd "$(dirname "$0")/.."

SUT="${1:?usage: bench-local.sh <caddy-mem|caddy-file|chronicle-redis>}"
OUT="${OUT:-results}"
SCENARIOS="${SCENARIOS:-append-steady append-sweep token-sessions producer-sessions fanout catchup mixed}"
DS_REPO="${DS_REPO:-$(cd ../../durable-streams && pwd)}"
CHRONICLE_REPO="${CHRONICLE_REPO:-$(cd .. && pwd)}"
PORT=4437
BASE_URL="http://localhost:$PORT"

log() { printf '\033[1;34m[bench]\033[0m %s\n' "$*"; }

make build >/dev/null

SUT_PID=""
SAMPLE_ARGS=()

start_caddy() { # $1 = caddyfile contents
  CADDY_BIN="${CADDY_BIN:-/tmp/ds-caddy}"
  if [[ ! -x "$CADDY_BIN" ]]; then
    log "building caddy-plugin server"
    (cd "$DS_REPO/packages/caddy-plugin" && go build -o "$CADDY_BIN" ./cmd/caddy)
  fi
  local cf
  cf=$(mktemp /tmp/Caddyfile.bench.XXXX)
  printf '%s\n' "$1" > "$cf"
  "$CADDY_BIN" run --config "$cf" --adapter caddyfile > "/tmp/bench-$SUT.log" 2>&1 &
  SUT_PID=$!
  SAMPLE_ARGS=(-sample-pid "caddy=$SUT_PID")
}

start_chronicle() {
  CHRONICLE_BIN="${CHRONICLE_BIN:-$CHRONICLE_REPO/bin/chronicle}"
  if [[ ! -x "$CHRONICLE_BIN" ]]; then
    log "building chronicle"
    (cd "$CHRONICLE_REPO" && make build >/dev/null)
  fi
  # Reuse a Redis already answering on 6379 (avoids spawning a second
  # container from a worktree's compose project); otherwise start one
  # under a fixed project name.
  if ! printf 'PING\r\n' | nc -w 2 localhost 6379 2>/dev/null | grep -q PONG; then
    docker compose -p chronicle-bench -f "$CHRONICLE_REPO/docker-compose.yml" up -d --wait >/dev/null 2>&1
  fi
  # Fresh keyspace, addressed over the same TCP path the server uses.
  printf 'FLUSHALL\r\n' | nc localhost 6379 >/dev/null
  "$CHRONICLE_BIN" --listen ":$PORT" > "/tmp/bench-$SUT.log" 2>&1 &
  SUT_PID=$!
  SAMPLE_ARGS=(-sample-pid "chronicle=$SUT_PID" -sample-redis "redis=localhost:6379")
}

case "$SUT" in
  caddy-mem)
    start_caddy "{
	admin off
	auto_https off
}
:$PORT {
	route /v1/stream/* {
		durable_streams
	}
}"
    ;;
  caddy-file)
    DATA_DIR=$(mktemp -d /tmp/ds-caddy-data.XXXX)
    log "file store data_dir: $DATA_DIR"
    start_caddy "{
	admin off
	auto_https off
}
:$PORT {
	route /v1/stream/* {
		durable_streams {
			data_dir $DATA_DIR
		}
	}
}"
    ;;
  chronicle-redis)
    start_chronicle
    ;;
  chronicle-redis-always)
    # Equal-durability variant: fsync the AOF before acking every write
    # (matches the file store's fsync-per-append contract).
    start_chronicle
    printf 'CONFIG SET appendfsync always\r\n' | nc localhost 6379 >/dev/null
    ;;
  *)
    echo "unknown SUT: $SUT" >&2; exit 2 ;;
esac

cleanup() { [[ -n "$SUT_PID" ]] && kill "$SUT_PID" 2>/dev/null || true; }
trap cleanup EXIT

for i in $(seq 1 50); do
  curl -sf -o /dev/null -X PUT "$BASE_URL/v1/stream/bench/healthz" -H 'Content-Type: application/json' && break
  sleep 0.2
  [[ $i == 50 ]] && { echo "SUT did not come up; see /tmp/bench-$SUT.log" >&2; exit 1; }
done
curl -sf -o /dev/null -X DELETE "$BASE_URL/v1/stream/bench/healthz" || true
log "$SUT up (pid $SUT_PID)"

for s in $SCENARIOS; do
  log "scenario $s"
  bin/dsload run -scenario "scenarios/$s.yaml" -label "$SUT" -out "$OUT" \
    -base-url "$BASE_URL" "${SAMPLE_ARGS[@]}" 2>&1 | tail -2
  sleep 3   # let the SUT settle between scenarios
done

log "done: $OUT/$SUT"
