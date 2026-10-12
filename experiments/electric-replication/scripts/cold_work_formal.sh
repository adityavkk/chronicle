#!/usr/bin/env bash
set -euo pipefail
root=$(git rev-parse --show-toplevel)
here="$root/experiments/electric-replication"
out=${1:?usage: cold_work_formal.sh NEW_EVIDENCE_DIRECTORY}
mkdir "$out"
out=$(realpath "$out")
cp "$here/formal/ColdWork.tla" "$here"/formal/cold-{work,work-live,admission,admission-live}.cfg "$out/"
sha256sum "$out"/*.tla "$out"/*.cfg > "$out/source-sha256.txt"
run() {
  local name=$1 expected=$2 pattern=$3 status
  set +e
  timeout 120 java -Xmx1g -cp "$root/.tmp/electric-tools/tla2tools.jar" tlc2.TLC \
    -workers 2 -deadlock -metadir "$root/.tmp/cold-work-tlc" \
    -config "$out/$name.cfg" "$out/ColdWork.tla" > "$out/$name.txt" 2>&1
  status=$?
  set -e
  printf '%s exit=%s expected=%s\n' "$name" "$status" "$expected" | tee -a "$out/results.txt"
  test "$status" -eq "$expected"
  grep -q "$pattern" "$out/$name.txt"
}
for name in cold-work cold-work-live cold-admission cold-admission-live; do
  run "$name" 0 'Model checking completed. No error has been found.'
done
for fault in StarveSparse ForgetUnknown ReleaseOnReply RacingSample AdmitBeforeRecovery PartialTailStall HighWaterOnly; do
  config=cold-admission; status=12; pattern='Invariant AdmissionSafe is violated'
  if test "$fault" = StarveSparse; then
    config=cold-work-live; status=13; pattern='Temporal properties were violated'
  elif test "$fault" = ForgetUnknown; then
    config=cold-work; pattern='Invariant ScheduleSafe is violated'
  elif test "$fault" = PartialTailStall || test "$fault" = HighWaterOnly; then
    config=cold-admission-live; status=13; pattern='Temporal properties were violated'
  fi
  sed "s/Fault = \"\"/Fault = \"$fault\"/" "$out/$config.cfg" > "$out/$fault.cfg"
  run "$fault" "$status" "$pattern"
done
printf 'Four bounded safety/liveness configurations and seven negative controls: PASS\n'
