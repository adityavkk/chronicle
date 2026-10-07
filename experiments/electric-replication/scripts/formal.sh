#!/usr/bin/env bash
set -euo pipefail
root=$(git rev-parse --show-toplevel)
here="$root/experiments/electric-replication"
tools="$root/.tmp/electric-tools"
mkdir -p "$tools" "$here/evidence/formal"
jar="$tools/tla2tools.jar"
test -f "$jar" || curl -fLsS https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar -o "$jar"
cd "$here/formal"
for mode in safety liveness; do
  java -Xmx2g -cp "$jar" tlc2.TLC -workers 2 -deadlock -config "$mode.cfg" Publication.tla > "$here/evidence/formal/$mode.txt" 2>&1
  grep -q 'Model checking completed. No error has been found.' "$here/evidence/formal/$mode.txt"
done
for mutation in EarlyFlush EarlyPublish LocalAck; do
  sed "s/$mutation = FALSE/$mutation = TRUE/" safety.cfg > "$tools/$mutation.cfg"
  set +e
  java -Xmx2g -cp "$jar" tlc2.TLC -workers 2 -deadlock -config "$tools/$mutation.cfg" Publication.tla > "$here/evidence/formal/$mutation.txt" 2>&1
  result=$?
  set -e
  test "$result" -ne 0
  grep -q 'Invariant Safe is violated' "$here/evidence/formal/$mutation.txt"
done
for mode in subscriptions subscriptions-live timed forks forks-live links links-live apply apply-live reclaim reclaim-live; do
  model=Subscriptions
  test "$mode" != timed || model=TimedApply
  [[ "$mode" != forks* ]] || model=Forks
  [[ "$mode" != links* ]] || model=Links
  [[ "$mode" != apply* ]] || model=ApplyRecovery
  [[ "$mode" != reclaim* ]] || model=JournalReclaim
  java -Xmx2g -cp "$jar" tlc2.TLC -workers 2 -deadlock -config "$mode.cfg" "$model.tla" > "$here/evidence/formal/$mode.txt" 2>&1
  grep -q 'Model checking completed. No error has been found.' "$here/evidence/formal/$mode.txt"
done
for mutation in StaleWorker LatestTailAck ForgetIntent LocalClock PartialPublish EarlyRelease FalseAbsence ForgetGrant StaleObservation OldIncarnationAck DiscoverAtTail EarlyApply ForgetMarker EarlyServe EarlyUnlink MissingDirSync DropRetained DropVote; do
  model=Subscriptions
  config=subscriptions
  invariant=Safe
  if test "$mutation" = LocalClock; then
    model=TimedApply; config=timed; invariant=SamePrefix
  fi
  case "$mutation" in
    PartialPublish|EarlyRelease|FalseAbsence|ForgetGrant) model=Forks; config=forks ;;
    StaleObservation|OldIncarnationAck|DiscoverAtTail) model=Links; config=links ;;
    EarlyApply|ForgetMarker|EarlyServe) model=ApplyRecovery; config=apply ;;
    EarlyUnlink|MissingDirSync|DropRetained|DropVote) model=JournalReclaim; config=reclaim ;;
  esac
  sed "s/$mutation = FALSE/$mutation = TRUE/" "$config.cfg" > "$tools/$mutation.cfg"
  set +e
  java -Xmx2g -cp "$jar" tlc2.TLC -workers 2 -deadlock -config "$tools/$mutation.cfg" "$model.tla" > "$here/evidence/formal/$mutation.txt" 2>&1
  result=$?
  set -e
  test "$result" -ne 0
  grep -q "Invariant $invariant is violated" "$here/evidence/formal/$mutation.txt"
done
"$HOME/.elan/bin/lean" -DwarningAsError=true Contracts.lean > "$here/evidence/formal/lean.txt" 2>&1
! grep -E 'sorryAx|warning:|error:' "$here/evidence/formal/lean.txt"
printf 'TLC publication/timing/subscriptions/forks/links/apply/reclaim safety, stable-period liveness, 21 negative mutations; Lean: PASS\n'
