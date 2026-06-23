#!/usr/bin/env bash
# run.sh — reproduce the Apalache inductive-invariant proof of []SingleHolder
# for the Chronicle subscription fence core (issue #41, FenceCore.tla).
#
# Downloads the apalache-mc distribution to /tmp on demand (NEVER committed),
# then discharges the three inductive obligations + the non-vacuity witnesses +
# the negative control. Prints a PASS/FAIL line per obligation and exits non-zero
# if any obligation does not have the required outcome.
#
# Java 11+ (the project ships Java 21) is required. From formal/tla:
#   bash apalache/run.sh           # all obligations + witnesses + negative control
#   APALACHE_VERSION=0.58.2 bash apalache/run.sh
set -uo pipefail

APALACHE_VERSION="${APALACHE_VERSION:-0.58.2}"
APALACHE_HOME="${APALACHE_HOME:-/tmp/apalache-${APALACHE_VERSION}}"
APALACHE_URL="https://github.com/apalache-mc/apalache/releases/download/v${APALACHE_VERSION}/apalache-${APALACHE_VERSION}.tgz"
MC="${APALACHE_HOME}/bin/apalache-mc"

# The script lives in formal/tla/apalache; the specs are one dir up.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TLA_DIR="$(cd "${HERE}/.." && pwd)"
cd "${TLA_DIR}"

# Apalache needs a JDK; prefer Java 21 if java_home is present (macOS).
if command -v /usr/libexec/java_home >/dev/null 2>&1; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null || /usr/libexec/java_home 2>/dev/null)"
fi

if [ ! -x "${MC}" ]; then
  echo ">>> downloading apalache-mc ${APALACHE_VERSION} to ${APALACHE_HOME} (not committed)"
  tmp="$(mktemp -d)"
  curl -fsSL -o "${tmp}/apalache.tgz" "${APALACHE_URL}" || { echo "FAIL: download"; exit 2; }
  tar -C /tmp -xzf "${tmp}/apalache.tgz" || { echo "FAIL: extract"; exit 2; }
  rm -rf "${tmp}"
fi
"${MC}" version >/dev/null 2>&1 || { echo "FAIL: apalache-mc does not run (check JAVA_HOME)"; exit 2; }

rc=0

# run_expect <label> <expect: OK|VIOLATION> <apalache check args...>
run_expect() {
  local label="$1" expect="$2"; shift 2
  local out
  out="$("${MC}" check "$@" FenceCore.tla 2>&1)"
  if echo "${out}" | grep -q "The outcome is: NoError"; then
    if [ "${expect}" = "OK" ]; then echo "PASS  ${label}  (NoError)";
    else echo "FAIL  ${label}  (expected a VIOLATION, got NoError)"; rc=1; fi
  elif echo "${out}" | grep -q "The outcome is: Error"; then
    if [ "${expect}" = "VIOLATION" ]; then echo "PASS  ${label}  (violation found, as required)";
    else echo "FAIL  ${label}  (expected NoError, got a violation)"; rc=1;
         echo "${out}" | grep -E "invariant.*violated" | head -2; fi
  else
    echo "FAIL  ${label}  (apalache produced no verdict)"; rc=1
    echo "${out}" | tail -5
  fi
}

# run_expect_mod <label> <expect> <module> <check args...>
run_expect_mod() {
  local label="$1" expect="$2" mod="$3"; shift 3
  local out
  out="$("${MC}" check "$@" "${mod}.tla" 2>&1)"
  if echo "${out}" | grep -q "The outcome is: NoError"; then
    [ "${expect}" = "OK" ] && echo "PASS  ${label}  (NoError)" || { echo "FAIL  ${label}"; rc=1; }
  elif echo "${out}" | grep -q "The outcome is: Error"; then
    [ "${expect}" = "VIOLATION" ] && echo "PASS  ${label}  (violation found, as required)" || { echo "FAIL  ${label}"; rc=1; }
  else
    echo "FAIL  ${label}  (no verdict)"; rc=1; echo "${out}" | tail -5
  fi
}

echo "=== Apalache inductive proof of []SingleHolder (FenceCore.tla, #41) ==="
"${MC}" typecheck FenceCore.tla >/dev/null 2>&1 && echo "PASS  typecheck (types are well-formed)" || { echo "FAIL  typecheck"; rc=1; }

# The three inductive obligations at the 2x2 worst-case scope.
run_expect "O1 Implies : IndInv => SingleHolder"        OK --cinit=ConstInit --init=IndInv --next=Next --inv=SingleHolder --length=0
run_expect "O2 IndInit : Init => IndInv"                OK --cinit=ConstInit --init=Init   --next=Next --inv=IndInv       --length=0
run_expect "O3 IndStep : IndInv /\\ Next => IndInv'"     OK --cinit=ConstInit --init=IndInv --next=Next --inv=IndInv       --length=1

# Scope-independence corroboration: the step also holds at larger instances.
run_expect_mod "O3 IndStep @ 3 workers x 2 subs"  OK FenceCore_3x2 --cinit=ConstInit3x2 --init=IndInv --next=Next --inv=IndInv --length=1
run_expect_mod "O3 IndStep @ 4 workers x 3 subs"  OK FenceCore_4x3 --cinit=ConstInit4x3 --init=IndInv --next=Next --inv=IndInv --length=1

# Non-vacuity: a live ack-acceptable holder and a waking fence are reachable.
run_expect_mod "W-live  : a granted live holder is reachable" VIOLATION FenceCore_Witness --cinit=ConstInit --init=Init --next=Next --inv=NoLiveHolder  --length=3
run_expect_mod "W-wake  : a waking fence is reachable"        VIOLATION FenceCore_Witness --cinit=ConstInit --init=Init --next=Next --inv=NoWakingFence --length=2

# Negative control: the INV-FENCE-04 unsound expire MUST break SingleHolder.
run_expect_mod "NEG     : unsound expire violates SingleHolder" VIOLATION FenceCore_Fault --cinit=ConstInitFault --init=Init --next=NextFault --inv=SingleHolder --length=6

echo "===================================================================="
if [ "${rc}" -eq 0 ]; then echo "ALL APALACHE OBLIGATIONS PASS"; else echo "SOME OBLIGATIONS FAILED"; fi
exit "${rc}"
