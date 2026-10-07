------------------------- MODULE ApplyRecovery -------------------------
EXTENDS Naturals, TLC
CONSTANTS MaxEntry, EarlyApply, ForgetMarker, EarlyServe
VARIABLES committed, queued, marker, applied, snapshot, seen, phase
vars == <<committed, queued, marker, applied, snapshot, seen, phase>>
Max(a,b) == IF a > b THEN a ELSE b

\* One local replica's durability bridge. Raft supplies matching, locally
\* durable entries and committed Apply batches; snapshot reference publication
\* is already durable. These assumptions are checked separately, not proved here.
Init == /\ committed = 0 /\ queued = 0 /\ marker = 0 /\ applied = 0
        /\ snapshot = 0 /\ seen = 0 /\ phase = "Ready"
Commit == /\ phase = "Ready" /\ committed < MaxEntry
          /\ committed' = committed + 1
          /\ UNCHANGED <<queued, marker, applied, snapshot, seen, phase>>
Queue == /\ phase = "Ready" /\ applied = queued /\ queued < committed
         /\ queued' \in (queued + 1)..committed
         /\ UNCHANGED <<committed, marker, applied, snapshot, seen, phase>>
Mark == /\ phase = "Ready" /\ marker < queued
        /\ marker' = queued
        /\ UNCHANGED <<committed, queued, applied, snapshot, seen, phase>>
Apply == /\ phase = "Ready" /\ applied < queued
         /\ (EarlyApply \/ marker >= queued)
         /\ applied' = applied + 1 /\ seen' = applied + 1
         /\ UNCHANGED <<committed, queued, marker, snapshot, phase>>
Snapshot == /\ phase = "Ready" /\ snapshot < applied
            /\ snapshot' = applied
            /\ UNCHANGED <<committed, queued, marker, applied, seen, phase>>
\* Installation is serialized after queued Apply work. A received snapshot can
\* be newer than the private journal marker; restore must use max(snapshot,marker).
Install == /\ phase = "Ready" /\ queued = applied /\ committed < MaxEntry
           /\ committed' \in (committed + 1)..MaxEntry
           /\ snapshot' = committed' /\ applied' = committed'
           /\ queued' = committed' /\ seen' = committed'
           /\ UNCHANGED <<marker, phase>>
Crash == /\ phase # "Down" /\ phase' = "Down"
         /\ applied' = snapshot /\ queued' = snapshot
         /\ marker' = IF ForgetMarker THEN 0 ELSE marker
         /\ UNCHANGED <<committed, snapshot, seen>>
Boot == /\ phase = "Down" /\ phase' = IF EarlyServe THEN "Ready" ELSE "Replay"
        /\ queued' = Max(snapshot,marker)
        /\ UNCHANGED <<committed, marker, applied, snapshot, seen>>
Replay == /\ phase = "Replay" /\ applied < queued /\ applied' = applied + 1
          /\ UNCHANGED <<committed, queued, marker, snapshot, seen, phase>>
Serve == /\ phase = "Replay" /\ applied = queued /\ phase' = "Ready"
         /\ seen' = Max(seen,applied)
         /\ UNCHANGED <<committed, queued, marker, applied, snapshot>>
Next == Commit \/ Queue \/ Mark \/ Apply \/ Snapshot \/ Install \/ Crash \/ Boot \/ Replay \/ Serve
SafetySpec == Init /\ [][Next]_vars
Safe == /\ snapshot <= applied /\ applied <= committed /\ marker <= committed
        /\ seen <= Max(snapshot,marker)
        /\ (phase = "Ready" => applied >= seen)
LiveNext == Commit \/ Queue \/ Mark \/ Apply
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(Commit) /\ WF_vars(Queue)
            /\ WF_vars(Mark) /\ WF_vars(Apply)
Progress == <> (seen = MaxEntry)
=======================================================================
