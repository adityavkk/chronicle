---------------------------- MODULE FsyncGroups ----------------------------
EXTENDS Naturals, TLC
CONSTANTS MaxFrame, Delay, EarlyNotify, LateCut, StuckTimer
VARIABLES written, stable, notified, cut, ticks, phase
vars == <<written,stable,notified,cut,ticks,phase>>

\* One native shard's contiguous physical WAL prefix, not a Raft commit index.
\* A covering successful fsync guarantees its PRE-call cut only. Entries and
\* publication markers can share that cut without sharing their logical roles.
\* Honest fsync and the native written-prefix/segment selection are assumptions.
Init == /\ written = 0 /\ stable = 0 /\ notified = 0 /\ cut = 0
        /\ ticks = 0 /\ phase = "Idle"
Stage == /\ phase # "Down" /\ written < MaxFrame
         /\ written' = written + 1
         /\ UNCHANGED <<stable,notified,cut,ticks,phase>>
Begin == /\ phase = "Idle" /\ stable < written
         /\ phase' = "Gather" /\ ticks' = Delay
         /\ notified' = IF EarlyNotify THEN written ELSE notified
         /\ UNCHANGED <<written,stable,cut>>
\* Arrivals never restart this finite delay. Fair execution is not a real-time
\* scheduling bound; a stopped thread or dishonest disk is outside this model.
Tick == /\ phase = "Gather" /\ ticks > 0 /\ ~StuckTimer
        /\ ticks' = ticks - 1
        /\ UNCHANGED <<written,stable,notified,cut,phase>>
StartSync == /\ phase = "Gather" /\ ticks = 0
             /\ cut' = written /\ phase' = "Sync"
             /\ UNCHANGED <<written,stable,notified,ticks>>
FinishSync == /\ phase = "Sync" /\ stable' = cut /\ phase' = "Idle"
              /\ notified' = IF LateCut THEN written ELSE cut
              /\ UNCHANGED <<written,cut,ticks>>
Crash == /\ phase # "Down" /\ phase' = "Down"
         /\ written' = stable /\ cut' = stable /\ ticks' = 0
         /\ UNCHANGED <<stable,notified>>
Recover == /\ phase = "Down" /\ phase' = "Idle"
           /\ UNCHANGED <<written,stable,notified,cut,ticks>>
Next == Stage \/ Begin \/ Tick \/ StartSync \/ FinishSync \/ Crash \/ Recover
SafetySpec == Init /\ [][Next]_vars
Safe == /\ notified <= stable /\ stable <= written /\ cut <= written
        /\ ticks <= Delay
LiveNext == Stage \/ Begin \/ Tick \/ StartSync \/ FinishSync
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(Stage) /\ WF_vars(Begin)
            /\ WF_vars(Tick) /\ WF_vars(StartSync) /\ WF_vars(FinishSync)
Progress == <> (notified = MaxFrame)
==========================================================================
