#!/usr/bin/env bash
# validate.sh — drive the issue #39 trace-validation bridge end to end.
#
#   1. produce a real JSONL fence trace from the running engine (subtrace build
#      tag) against a live Redis;
#   2. convert it to per-scenario TraceData modules (tracegen);
#   3. run TLC in constrained/DFS mode on each scenario, asserting the trace is a
#      legal behavior of SubscriptionFence (#37).
#
# VERDICT per scenario: TLC reports "Invariant NotAccepted is violated" when the
# trace VALIDATES (the accepting witness reaches the end of the log). "No error
# has been found" means the trace did NOT validate — a HIGH finding (the deepest
# ti TLC reached localizes the first non-matching line). An "Invariant TraceInv
# is violated" means a SAFETY property (single-holder / stale-inert) failed on the
# real execution — also a HIGH finding.
#
# Usage: validate.sh [redis_addr]   (default localhost:6379; reuses a running
# Redis, namespaces all keys under a unique run id, deletes them on exit).
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TLA_DIR="$REPO_ROOT/formal/tla"
ADDR="${1:-localhost:6379}"
# Prefer an explicit JAR, then the one `make jar` drops in formal/tla, then /tmp.
if [ -z "${JAR:-}" ] || [ ! -f "$JAR" ]; then
  if [ -f "$TLA_DIR/tla2tools.jar" ]; then JAR="$TLA_DIR/tla2tools.jar"; else JAR="/tmp/tla2tools.jar"; fi
fi
# Absolutize: the runner cd's into per-scenario dirs, so a relative jar breaks.
JAR="$(cd "$(dirname "$JAR")" && pwd)/$(basename "$JAR")"
WORK="$(mktemp -d -t t39-trace.XXXXXX)"
TRACE="$WORK/trace.jsonl"

echo "== issue #39 trace validation =="
echo "repo:  $REPO_ROOT"
echo "redis: $ADDR"
echo "work:  $WORK"
echo

test -f "$JAR" || { echo "tla2tools.jar not at $JAR (set JAR=...)"; exit 2; }

echo "-- 1. produce trace (subtrace build) --"
( cd "$REPO_ROOT" && go run -tags subtrace ./formal/tla/trace/produce -out "$TRACE" -addr "$ADDR" ) || exit 1
echo "   $(wc -l < "$TRACE") lines"
echo

echo "-- 2. convert (tracegen) --"
( cd "$REPO_ROOT" && go run ./formal/tla/trace/tracegen -in "$TRACE" -outdir "$WORK/scenarios" -prefix T39 ) || exit 1
echo

echo "-- 3. validate each scenario (TLC, DFS) --"
fail=0
validated=0
while IFS= read -r scen; do
  [ -z "$scen" ] && continue
  dir="$WORK/scenarios/$scen"
  cp "$TLA_DIR/Trace.tla" "$TLA_DIR/SubscriptionFence.tla" "$TLA_DIR/Trace.cfg" "$dir/"
  out="$dir/tlc.out"
  ( cd "$dir" && java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -deadlock -workers 1 \
      -config Trace.cfg -metadir "$dir/states" Trace.tla ) > "$out" 2>&1
  if grep -q "Invariant NotAccepted is violated" "$out"; then
    echo "   PASS  $scen — trace is a legal behavior of SubscriptionFence (S ∩ T ≠ ∅)"
    validated=$((validated+1))
  elif grep -q "Invariant TraceInv is violated" "$out"; then
    echo "   FAIL  $scen — SAFETY violated on the real trace (HIGH). See $out"
    fail=1
  elif grep -q "No error has been found" "$out"; then
    echo "   FAIL  $scen — trace did NOT validate (no accepting run; HIGH). See $out"
    echo "         localize with: add INVARIANT (ti <= K) to Trace.cfg and bisect K;"
    echo "         the largest K that still VIOLATES is the last consumable line."
    fail=1
  else
    echo "   ERR   $scen — TLC inconclusive. See $out"
    tail -5 "$out" | sed 's/^/         /'
    fail=1
  fi
done < "$WORK/scenarios/T39_INDEX.txt"

echo
echo "== $validated scenario(s) validated; exit $fail =="
echo "(work dir kept at $WORK for inspection)"
exit $fail
