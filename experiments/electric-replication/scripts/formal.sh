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
for mode in subscriptions subscriptions-live timed forks forks-live fences fences-live links links-live apply apply-live reclaim reclaim-live snapshot snapshot-live reads reads-live batches batches-live receipts receipts-live backlog backlog-live epochs epochs-live fsync fsync-live cold cold-live; do
  model=Subscriptions
  test "$mode" != timed || model=TimedApply
  [[ "$mode" != forks* ]] || model=Forks
  [[ "$mode" != fences* ]] || model=FenceCompaction
  [[ "$mode" != links* ]] || model=Links
  [[ "$mode" != apply* ]] || model=ApplyRecovery
  [[ "$mode" != reclaim* ]] || model=JournalReclaim
  [[ "$mode" != snapshot* ]] || model=SnapshotCut
  [[ "$mode" != reads* ]] || model=ReadCohorts
  [[ "$mode" != batches* ]] || model=Batches
  [[ "$mode" != receipts* ]] || model=Receipts
  [[ "$mode" != backlog* ]] || model=AsyncBacklog
  [[ "$mode" != epochs* ]] || model=AdmissionEpoch
  [[ "$mode" != fsync* ]] || model=FsyncGroups
  [[ "$mode" != cold* ]] || model=ColdOwnership
  java -Xmx2g -cp "$jar" tlc2.TLC -workers 2 -deadlock -config "$mode.cfg" "$model.tla" > "$here/evidence/formal/$mode.txt" 2>&1
  grep -q 'Model checking completed. No error has been found.' "$here/evidence/formal/$mode.txt"
done
for mutation in StaleWorker LatestTailAck ForgetIntent LocalClock PartialPublish EarlyRelease FalseAbsence ForgetGrant UnsafeRange ReplaceFence ForgetFence StaleObservation OldIncarnationAck DiscoverAtTail EarlyApply ForgetMarker EarlyServe EarlyUnlink MissingDirSync DropRetained DropVote UnpinnedReader UnpinnedPath UnboundedCopy LateMetadata OverlapInstall EarlyReference EarlyCleanup UseCompletedTicket ReuseEqual Reorder BatchMetadata EarlyReply WrongReply ReleaseOnTimeout DropForming EarlyReceipt PublishAccepted IndexOnly InvalidateAbsent AcceptMeansSuccess ReceiptSession ReleaseOnAccepted AllowRecoveredAdmission CacheAcrossTerm UnguardedEnqueue IgnoreLeaseAtAssignment DropAcceptedOnExpiry EarlyNotify LateCut EarlyColdPublish AppliedIsCheckpoint RequestedIsPurged IgnoreColdReaders ForgetRemovedParticipant UnregisteredJoin ReattachRetiredObject; do
  model=Subscriptions
  config=subscriptions
  invariant=Safe
  if test "$mutation" = LocalClock; then
    model=TimedApply; config=timed; invariant=SamePrefix
  fi
  case "$mutation" in
    PartialPublish|EarlyRelease|FalseAbsence|ForgetGrant) model=Forks; config=forks ;;
    UnsafeRange|ReplaceFence|ForgetFence) model=FenceCompaction; config=fences ;;
    StaleObservation|OldIncarnationAck|DiscoverAtTail) model=Links; config=links ;;
    EarlyApply|ForgetMarker|EarlyServe) model=ApplyRecovery; config=apply ;;
    EarlyUnlink|MissingDirSync|DropRetained|DropVote|UnpinnedReader) model=JournalReclaim; config=reclaim ;;
    UnpinnedPath|UnboundedCopy|LateMetadata|OverlapInstall|EarlyReference|EarlyCleanup) model=SnapshotCut; config=snapshot ;;
    UseCompletedTicket|ReuseEqual) model=ReadCohorts; config=reads ;;
    Reorder|BatchMetadata|EarlyReply|WrongReply|ReleaseOnTimeout|DropForming) model=Batches; config=batches ;;
    EarlyReceipt|PublishAccepted|IndexOnly|InvalidateAbsent|AcceptMeansSuccess|ReceiptSession) model=Receipts; config=receipts ;;
    ReleaseOnAccepted|AllowRecoveredAdmission) model=AsyncBacklog; config=backlog ;;
    CacheAcrossTerm|UnguardedEnqueue|IgnoreLeaseAtAssignment|DropAcceptedOnExpiry) model=AdmissionEpoch; config=epochs ;;
    EarlyNotify|LateCut) model=FsyncGroups; config=fsync ;;
    EarlyColdPublish|AppliedIsCheckpoint|RequestedIsPurged|IgnoreColdReaders|ForgetRemovedParticipant|UnregisteredJoin|ReattachRetiredObject) model=ColdOwnership; config=cold ;;
  esac
  # Require the join mutation to lose an actual cold dependency, not just break
  # the auxiliary certificate-order invariant earlier in the same bad history.
  test "$mutation" != UnregisteredJoin || invariant=DataSafe
  sed -e "s/$mutation = FALSE/$mutation = TRUE/" -e "s/INVARIANT Safe/INVARIANT $invariant/" "$config.cfg" > "$tools/$mutation.cfg"
  set +e
  java -Xmx2g -cp "$jar" tlc2.TLC -workers 2 -deadlock -config "$tools/$mutation.cfg" "$model.tla" > "$here/evidence/formal/$mutation.txt" 2>&1
  result=$?
  set -e
  test "$result" -ne 0
  grep -q "Invariant $invariant is violated" "$here/evidence/formal/$mutation.txt"
done
for mutation in StuckTimer LostWake ForgetColdRetirement; do
  config=fsync-live; model=FsyncGroups
  if test "$mutation" = LostWake; then config=reclaim-live; model=JournalReclaim; fi
  if test "$mutation" = ForgetColdRetirement; then config=cold-live; model=ColdOwnership; fi
  sed "s/$mutation = FALSE/$mutation = TRUE/" "$config.cfg" > "$tools/$mutation.cfg"
  set +e
  java -Xmx2g -cp "$jar" tlc2.TLC -workers 2 -deadlock -config "$tools/$mutation.cfg" "$model.tla" > "$here/evidence/formal/$mutation.txt" 2>&1
  result=$?
  set -e
  test "$result" -ne 0
  grep -q 'Temporal properties were violated' "$here/evidence/formal/$mutation.txt"
done
"$HOME/.elan/bin/lean" -DwarningAsError=true Contracts.lean > "$here/evidence/formal/lean.txt" 2>&1
! grep -E 'sorryAx|warning:|error:' "$here/evidence/formal/lean.txt"
sed 's/name == "accept"/name == "host"/' Contracts.lean > "$tools/DiscardHost.lean"
set +e
"$HOME/.elan/bin/lean" -DwarningAsError=true "$tools/DiscardHost.lean" > "$here/evidence/formal/DiscardHost.txt" 2>&1
result=$?
set -e
test "$result" -ne 0
grep -q 'transportHeader "host" = false' "$here/evidence/formal/DiscardHost.txt"
printf 'TLC publication/timing/subscriptions/forks/fences/links/apply/reclaim/snapshot/reads/batches/receipts/backlog/epochs/fsync/cold safety, stable-period liveness, 63 negative mutations; Lean and one header-projection negative mutation: PASS\n'
