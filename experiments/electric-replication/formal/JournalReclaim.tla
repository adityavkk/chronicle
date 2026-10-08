------------------------ MODULE JournalReclaim ------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS EarlyUnlink, MissingDirSync, DropRetained, DropVote, UnpinnedReader
VARIABLES tail, disk, stableCut, visibleCut, candidate, phase, reader, readOnce
vars == <<tail, disk, stableCut, visibleCut, candidate, phase, reader, readOnce>>

\* Eight durable native frames: vote, entry, snapshot-reference, purge,
\* then a second generation of the same. One record per segment is a bounded
\* worst case for reclamation. Raft/snapshot validity and successful file fsync
\* are assumptions; those transitions are covered by the other models.
MaxRecord == 8
VoteAt(n) == IF n >= 5 THEN 2 ELSE IF n >= 1 THEN 1 ELSE 0
Payloads(n) == {e \in {2,6}: e <= n /\ n < e + 2}
Keep(c) == (c..tail) \cup (IF DropRetained THEN {} ELSE Payloads(c))
RecoveredVote == IF tail >= 5 /\ stableCut < 5 THEN 2
                 ELSE IF tail >= 1 /\ stableCut < 1 THEN 1
                 ELSE IF DropVote THEN 0 ELSE VoteAt(stableCut)
Init == /\ tail = 0 /\ disk = {} /\ stableCut = 0 /\ visibleCut = 0
        /\ candidate = 0 /\ phase = "Idle"
        /\ reader = 0 /\ readOnce = {}
\* An indexed reader holds the index mutex through its physical read. Index
\* mutation (including Purge) and capture cannot overtake it. After capture,
\* new readers need only entries retained at the cut or frames appended after
\* it; unlink and directory fsync therefore need no index mutex. The mutation
\* incorrectly releases the reader's mutex between location lookup and I/O.
Append == /\ tail < MaxRecord /\ (reader = 0 \/ UnpinnedReader)
          /\ tail' = tail + 1 /\ disk' = disk \cup {tail'}
          /\ UNCHANGED <<stableCut, visibleCut, candidate, phase, reader, readOnce>>
Capture == /\ phase = "Idle" /\ stableCut < tail
           /\ (reader = 0 \/ UnpinnedReader)
           /\ candidate' = tail /\ phase' = "Captured"
           /\ UNCHANGED <<tail, disk, stableCut, visibleCut, reader, readOnce>>
FileSync == /\ phase = "Captured" /\ phase' = "FileSynced"
            /\ UNCHANGED <<tail, disk, stableCut, visibleCut, candidate, reader, readOnce>>
Rename == /\ phase = "FileSynced" /\ phase' = "Renamed"
          /\ visibleCut' = candidate
          /\ UNCHANGED <<tail, disk, stableCut, candidate, reader, readOnce>>
DirectorySync == /\ phase = "Renamed" /\ phase' = "Stable"
                 /\ stableCut' = visibleCut
                 /\ UNCHANGED <<tail, disk, visibleCut, candidate, reader, readOnce>>
Reclaim == /\ (phase = "Stable" \/ (EarlyUnlink /\ phase = "Captured")
                \/ (MissingDirSync /\ phase = "Renamed"))
           /\ disk' = disk \cap Keep(candidate) /\ phase' = "Idle"
           /\ UNCHANGED <<tail, stableCut, visibleCut, candidate, reader, readOnce>>
ReadStart == /\ reader = 0
             /\ \E entry \in Payloads(tail) \ readOnce:
                  /\ reader' = entry /\ readOnce' = readOnce \cup {entry}
             /\ UNCHANGED <<tail, disk, stableCut, visibleCut, candidate, phase>>
ReadDone == /\ reader # 0 /\ reader' = 0
            /\ UNCHANGED <<tail, disk, stableCut, visibleCut, candidate, phase, readOnce>>
Crash == /\ phase' = "Idle" /\ visibleCut' = stableCut /\ reader' = 0
         /\ candidate' = stableCut
         /\ UNCHANGED <<tail, disk, stableCut, readOnce>>
Next == Append \/ Capture \/ FileSync \/ Rename \/ DirectorySync \/ Reclaim \/ Crash \/ ReadStart \/ ReadDone
SafetySpec == Init /\ [][Next]_vars
Safe == /\ ((stableCut + 1)..tail \cup Payloads(tail)) \subseteq disk
        /\ RecoveredVote = VoteAt(tail)
        /\ (reader = 0 \/ reader \in disk)
\* readOnce bounds external work, not a production read limit. Stable-period
\* progress assumes finite arrivals and eventual completion of admitted I/O.
LiveNext == Append \/ Capture \/ FileSync \/ Rename \/ DirectorySync \/ Reclaim \/ ReadStart \/ ReadDone
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(Append) /\ WF_vars(Capture)
            /\ WF_vars(FileSync) /\ WF_vars(Rename) /\ WF_vars(DirectorySync)
            /\ WF_vars(Reclaim) /\ WF_vars(ReadDone)
Progress == <> (tail = MaxRecord /\ stableCut = MaxRecord /\ disk = {MaxRecord})
=======================================================================
