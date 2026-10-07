------------------------ MODULE JournalReclaim ------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS EarlyUnlink, MissingDirSync, DropRetained, DropVote
VARIABLES tail, disk, stableCut, visibleCut, candidate, phase
vars == <<tail, disk, stableCut, visibleCut, candidate, phase>>

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
Append == /\ tail < MaxRecord /\ tail' = tail + 1 /\ disk' = disk \cup {tail'}
          /\ UNCHANGED <<stableCut, visibleCut, candidate, phase>>
Capture == /\ phase = "Idle" /\ stableCut < tail
           /\ candidate' = tail /\ phase' = "Captured"
           /\ UNCHANGED <<tail, disk, stableCut, visibleCut>>
FileSync == /\ phase = "Captured" /\ phase' = "FileSynced"
            /\ UNCHANGED <<tail, disk, stableCut, visibleCut, candidate>>
Rename == /\ phase = "FileSynced" /\ phase' = "Renamed"
          /\ visibleCut' = candidate
          /\ UNCHANGED <<tail, disk, stableCut, candidate>>
DirectorySync == /\ phase = "Renamed" /\ phase' = "Stable"
                 /\ stableCut' = visibleCut
                 /\ UNCHANGED <<tail, disk, visibleCut, candidate>>
Reclaim == /\ (phase = "Stable" \/ (EarlyUnlink /\ phase = "Captured")
                \/ (MissingDirSync /\ phase = "Renamed"))
           /\ disk' = disk \cap Keep(candidate) /\ phase' = "Idle"
           /\ UNCHANGED <<tail, stableCut, visibleCut, candidate>>
Crash == /\ phase' = "Idle" /\ visibleCut' = stableCut
         /\ candidate' = stableCut
         /\ UNCHANGED <<tail, disk, stableCut>>
Next == Append \/ Capture \/ FileSync \/ Rename \/ DirectorySync \/ Reclaim \/ Crash
SafetySpec == Init /\ [][Next]_vars
Safe == /\ ((stableCut + 1)..tail \cup Payloads(tail)) \subseteq disk
        /\ RecoveredVote = VoteAt(tail)
LiveNext == Append \/ Capture \/ FileSync \/ Rename \/ DirectorySync \/ Reclaim
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(Append) /\ WF_vars(Capture)
            /\ WF_vars(FileSync) /\ WF_vars(Rename) /\ WF_vars(DirectorySync)
            /\ WF_vars(Reclaim)
Progress == <> (tail = MaxRecord /\ stableCut = MaxRecord /\ disk = {MaxRecord})
=======================================================================
