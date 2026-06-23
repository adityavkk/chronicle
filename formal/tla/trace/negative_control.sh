#!/usr/bin/env bash
# negative_control.sh — prove the #39 trace-validation bridge is NOT vacuous.
#
# A trace-validation setup that accepts everything proves nothing (research/01's
# main pitfall). This takes the validated `takeover` trace and relabels the
# deposed worker's FENCED ack as OK — a worker whose token is two generations
# stale acking successfully, which the single-holder fence makes impossible. TLC
# MUST then refuse to validate it ("No error has been found"). If TLC instead
# reports the trace as accepted, the bridge is vacuous and this script FAILS.
#
# Usage: negative_control.sh [redis_addr]   (default localhost:6379)
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TLA_DIR="$REPO_ROOT/formal/tla"
ADDR="${1:-localhost:6379}"
if [ -z "${JAR:-}" ] || [ ! -f "$JAR" ]; then
  if [ -f "$TLA_DIR/tla2tools.jar" ]; then JAR="$TLA_DIR/tla2tools.jar"; else JAR="/tmp/tla2tools.jar"; fi
fi
JAR="$(cd "$(dirname "$JAR")" && pwd)/$(basename "$JAR")"
WORK="$(mktemp -d -t t39-neg.XXXXXX)"

echo "== #39 trace-validation NEGATIVE CONTROL =="
( cd "$REPO_ROOT" && go run -tags subtrace ./formal/tla/trace/produce -out "$WORK/trace.jsonl" -addr "$ADDR" ) >/dev/null || exit 1
( cd "$REPO_ROOT" && go run ./formal/tla/trace/tracegen -in "$WORK/trace.jsonl" -outdir "$WORK/s" -prefix T39 ) >/dev/null || exit 1

D="$WORK/s/takeover"
[ -d "$D" ] || { echo "no takeover scenario produced"; exit 2; }
cp "$TLA_DIR/Trace.tla" "$TLA_DIR/SubscriptionFence.tla" "$TLA_DIR/Trace.cfg" "$D/"

# Tamper: the deposed-holder FENCED ack (w1, gen one stale) -> OK.
python3 - "$D/TraceData.tla" <<'PY'
import sys
p = sys.argv[1]; s = open(p).read()
s2 = s.replace('op |-> "ack", status |-> "FENCED", worker |-> "w1"',
               'op |-> "ack", status |-> "OK", worker |-> "w1"', 1)
assert s2 != s, "expected a FENCED w1 ack to relabel — trace shape changed?"
open(p, "w").write(s2)
print("tampered: deposed FENCED ack (w1) -> OK")
PY

out="$D/tlc.out"
( cd "$D" && java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -deadlock -workers 1 \
    -config Trace.cfg -metadir "$D/states" Trace.tla ) > "$out" 2>&1

if grep -q "No error has been found" "$out"; then
  echo "PASS: the impossible trace did NOT validate — the bridge is non-vacuous."
  rm -rf "$WORK"; exit 0
elif grep -q "Invariant NotAccepted is violated" "$out"; then
  echo "FAIL: an impossible (deposed-OK-ack) trace VALIDATED — the bridge is VACUOUS. See $out"
  exit 1
else
  echo "ERR: TLC inconclusive. See $out"; tail -5 "$out"; exit 1
fi
