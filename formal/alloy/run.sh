#!/usr/bin/env bash
#
# run.sh -- headless Alloy runner for the issue #40 relational models.
#
# Downloads the Alloy v6 dist jar on demand to /tmp (NEVER committed -- see
# .gitignore) and runs every command in each .als file via Alloy's `exec`
# subcommand, which prints a one-line verdict per command and writes a
# machine-readable receipt.json + per-instance .md to an output dir.
#
# Verdict reading (the headline result of each model):
#   check <assert>  ->  UNSAT  == the assertion HOLDS (no counterexample in scope)
#                       SAT    == a COUNTEREXAMPLE was found (a real violation: a
#                                 finding to report)
#   run   <pred>    ->  SAT    == an instance exists (a non-vacuity witness, or a
#                                 deliberate negative/diagnostic witness)
#                       UNSAT  == the predicate is unsatisfiable in scope
#
# So for the SAFETY models here: every `check` must print UNSAT, and the
# witness `run`s must print SAT. Any `check ... SAT` is a counterexample and a
# finding.
#
# Usage:  bash run.sh            # run all models
#         bash run.sh FanoutIndex.als   # run one model
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ALLOY_VERSION="${ALLOY_VERSION:-v6.2.0}"
ALLOY_JAR="${ALLOY_JAR:-/tmp/alloy.jar}"
ALLOY_URL="https://github.com/AlloyTools/org.alloytools.alloy/releases/download/${ALLOY_VERSION}/org.alloytools.alloy.dist.jar"

if [ ! -f "$ALLOY_JAR" ]; then
  echo ">> downloading Alloy ${ALLOY_VERSION} to ${ALLOY_JAR}"
  curl -sL -o "$ALLOY_JAR" "$ALLOY_URL"
fi

models=("$@")
if [ ${#models[@]} -eq 0 ]; then
  models=(FanoutIndex.als SlotHoming.als)
fi

rc=0
for m in "${models[@]}"; do
  echo "==================================================================="
  echo ">> $m"
  echo "==================================================================="
  out="/tmp/alloy_$(basename "${m%.als}")"
  java -jar "$ALLOY_JAR" exec -f -o "$out" "$HERE/$m" || rc=$?
done
exit $rc
