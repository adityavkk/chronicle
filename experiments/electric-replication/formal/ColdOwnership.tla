-------------------------- MODULE ColdOwnership --------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS EarlyColdPublish, AppliedIsCheckpoint, RequestedIsPurged,
          IgnoreColdReaders, ForgetRemovedParticipant, UnregisteredJoin,
          ReattachRetiredObject, ForgetColdRetirement
VARIABLES committed, blob, upload, deleted, attached, deletePending,
          participants, members, online, applied, snapshots, purged, requested,
          floors, fences, readers, transfer, joining, joinFloor, retired
vars == <<committed, blob, upload, deleted, attached, deletePending,
          participants, members, online, applied, snapshots, purged, requested,
          floors, fences, readers, transfer, joining, joinFloor, retired>>

\* One immutable object, two established storage participants and one learner.
\* Raft supplies an ordered committed prefix. Positions: 1 publishes the object,
\* 2 is a JSON-fork command whose replay needs its bytes, 3 retires the last
\* stream/ancestor/grant reference. Snapshot publication and completed purge
\* mean honest file+directory+native-WAL durability, not just issued requests.
Nodes == {1, 2, 3}
Initial == {1, 2}
NeedsObject(cut) == cut < 3 /\ committed >= 2
\* floors[0] is the committed group floor. Transfers 4..6 retain a receiver's
\* old snapshot after its sender crashed and lost its in-memory source pin.
TransferCut == IF transfer <= 3 THEN transfer ELSE transfer - 3
Init == /\ committed = 0 /\ blob = FALSE /\ upload = TRUE
        /\ deleted = FALSE /\ attached = FALSE /\ deletePending = FALSE
        /\ participants = Initial /\ members = Initial /\ online = Initial
        /\ applied = [n \in Nodes |-> 0] /\ snapshots = [n \in Nodes |-> 0]
        /\ purged = [n \in Nodes |-> 0] /\ requested = {}
        /\ floors = [n \in 0..3 |-> 0] /\ fences = {}
        /\ readers = {} /\ transfer = 0 /\ joining = "Absent"
        /\ joinFloor = 0 /\ retired = FALSE

\* An immutable, verified PUT may have a delayed duplicate already in flight.
\* It may finish after DELETE. Retirement reconciliation must run again.
Upload == /\ ~blob /\ ~deleted /\ blob' = TRUE
          /\ UNCHANGED <<committed, upload, deleted, attached, deletePending,
             participants, members, online, applied, snapshots, purged, requested,
             floors, fences, readers, transfer, joining, joinFloor, retired>>
Publish == /\ committed = 0 /\ (blob \/ EarlyColdPublish)
           /\ committed' = 1 /\ attached' = TRUE
           /\ UNCHANGED <<blob, upload, deleted, deletePending, participants,
              members, online, applied, snapshots, purged, requested, floors,
              fences, readers, transfer, joining, joinFloor, retired>>
Fork == /\ committed = 1 /\ committed' = 2
        /\ UNCHANGED <<blob, upload, deleted, attached, deletePending,
           participants, members, online, applied, snapshots, purged, requested,
           floors, fences, readers, transfer, joining, joinFloor, retired>>
Retire == /\ committed = 2 /\ committed' = 3 /\ attached' = FALSE
          /\ retired' = TRUE
          /\ UNCHANGED <<blob, upload, deleted, deletePending, participants,
             members, online, applied, snapshots, purged, requested, floors,
             fences, readers, transfer, joining, joinFloor>>
Apply(n) == /\ n \in online /\ applied[n] < committed
            /\ applied' = [applied EXCEPT ![n] = @ + 1]
            /\ UNCHANGED <<committed, blob, upload, deleted, attached,
               deletePending, participants, members, online, snapshots, purged,
               requested, floors, fences, readers, transfer, joining, joinFloor, retired>>
Checkpoint(n) == /\ n \in online /\ snapshots[n] < applied[n]
                 /\ snapshots' = [snapshots EXCEPT ![n] = applied[n]]
                 /\ UNCHANGED <<committed, blob, upload, deleted, attached,
                    deletePending, participants, members, online, applied, purged,
                    requested, floors, fences, readers, transfer, joining, joinFloor, retired>>
RequestPurge(n) == /\ n \in online /\ snapshots[n] = 3 /\ n \notin requested
                   /\ requested' = requested \cup {n}
                   /\ UNCHANGED <<committed, blob, upload, deleted, attached,
                      deletePending, participants, members, online, applied, snapshots,
                      purged, floors, fences, readers, transfer, joining, joinFloor, retired>>
CompletePurge(n) == /\ n \in online /\ n \in requested /\ purged[n] # 3
                    /\ purged' = [purged EXCEPT ![n] = 3]
                    /\ UNCHANGED <<committed, blob, upload, deleted, attached,
                       deletePending, participants, members, online, applied,
                       snapshots, requested, floors, fences, readers, transfer,
                       joining, joinFloor, retired>>
Fence(n) == /\ n \in online /\ applied[n] = 3 /\ n \notin fences
            /\ fences' = fences \cup {n}
            /\ UNCHANGED <<committed, blob, upload, deleted, attached,
               deletePending, participants, members, online, applied, snapshots,
               purged, requested, floors, readers, transfer, joining, joinFloor, retired>>
Certify(n) == /\ n \in participants \cap online /\ floors[n] # 3
              /\ n \in fences
              /\ IF AppliedIsCheckpoint THEN applied[n] = 3 ELSE snapshots[n] = 3
              /\ IF RequestedIsPurged THEN n \in requested
                  ELSE IF AppliedIsCheckpoint THEN TRUE ELSE purged[n] = 3
              /\ (IgnoreColdReaders \/ (n \notin readers /\ (n # 1 \/ transfer \notin {1, 2})))
              /\ floors' = [floors EXCEPT ![n] = 3]
              /\ UNCHANGED <<committed, blob, upload, deleted, attached,
                 deletePending, participants, members, online, applied, snapshots,
                 purged, requested, fences, readers, transfer, joining, joinFloor, retired>>
Read(n) == /\ n \in online /\ n \notin readers /\ n \notin fences
           /\ applied[n] \in {1, 2} /\ readers' = readers \cup {n}
           /\ UNCHANGED <<committed, blob, upload, deleted, attached,
              deletePending, participants, members, online, applied, snapshots,
              purged, requested, floors, fences, transfer, joining, joinFloor, retired>>
FinishRead(n) == /\ n \in readers /\ readers' = readers \ {n}
                 /\ UNCHANGED <<committed, blob, upload, deleted, attached,
                    deletePending, participants, members, online, applied, snapshots,
                    purged, requested, floors, fences, transfer, joining, joinFloor, retired>>
Crash(n) == /\ n \in online /\ online' = online \ {n}
            /\ readers' = readers \ {n}
            /\ transfer' = IF n = 1 /\ transfer \in {1, 2, 3} THEN transfer + 3 ELSE transfer
            /\ UNCHANGED <<committed, blob, upload, deleted, attached,
               deletePending, participants, members, applied, snapshots, purged,
               requested, floors, fences, joining, joinFloor, retired>>
Restart(n) == /\ n \in participants /\ n \notin online
              /\ (n # 3 \/ joining = "Ready")
              /\ online' = online \cup {n}
              /\ applied' = [applied EXCEPT ![n] = snapshots[n]]
              /\ UNCHANGED <<committed, blob, upload, deleted, attached,
                 deletePending, participants, members, snapshots, purged,
                 requested, floors, fences, readers, transfer, joining, joinFloor, retired>>
Remove == /\ 2 \in members /\ members' = members \ {2}
          /\ participants' = IF ForgetRemovedParticipant THEN participants \ {2}
                             ELSE participants
          /\ UNCHANGED <<committed, blob, upload, deleted, attached,
             deletePending, online, applied, snapshots, purged, requested,
             floors, fences, readers, transfer, joining, joinFloor, retired>>

\* Admission is ordered against GC. A pending learner holds the floor it saw;
\* it cannot be ignored until a transferred snapshot happens to finish.
Admit == /\ joining = "Absent" /\ joining' = "Pending"
         /\ joinFloor' = floors[0]
         /\ floors' = [floors EXCEPT ![3] = joinFloor']
         /\ participants' = IF UnregisteredJoin THEN participants ELSE participants \cup {3}
         /\ UNCHANGED <<committed, blob, upload, deleted, attached,
            deletePending, members, online, applied, snapshots, purged, requested,
            fences, readers, transfer, retired>>
StartTransfer == /\ joining = "Pending" /\ transfer = 0 /\ 1 \in online
                 /\ snapshots[1] \in {1, 2, 3} /\ snapshots[1] >= joinFloor
                 /\ (snapshots[1] = 3 \/ 1 \notin fences)
                 /\ transfer' = snapshots[1]
                 /\ UNCHANGED <<committed, blob, upload, deleted, attached,
                    deletePending, participants, members, online, applied, snapshots,
                    purged, requested, floors, fences, readers, joining, joinFloor, retired>>
FinishTransfer == /\ joining = "Pending" /\ transfer # 0
                  /\ joining' = "Ready" /\ online' = online \cup {3}
                  /\ participants' = participants \cup {3}
                  /\ snapshots' = [snapshots EXCEPT ![3] = TransferCut]
                  /\ applied' = [applied EXCEPT ![3] = TransferCut]
                  /\ purged' = [purged EXCEPT ![3] = TransferCut]
                  /\ transfer' = 0
                  /\ UNCHANGED <<committed, blob, upload, deleted, attached,
                     deletePending, members, requested, floors, fences,
                     readers, joinFloor, retired>>

Eligible == committed = 3 /\ retired /\ \A n \in participants : floors[n] = 3
AdvanceGc == /\ Eligible /\ floors[0] = 0 /\ floors' = [floors EXCEPT ![0] = 3]
             /\ UNCHANGED <<committed, blob, upload, deleted, attached,
                deletePending, participants, members, online, applied, snapshots,
                purged, requested, fences, readers, transfer, joining, joinFloor, retired>>
IssueDelete == /\ floors[0] = 3 /\ retired /\ ~deletePending /\ blob
               /\ deletePending' = TRUE
               /\ UNCHANGED <<committed, blob, upload, deleted, attached,
                  participants, members, online, applied, snapshots, purged, requested,
                  floors, fences, readers, transfer, joining, joinFloor, retired>>
CompleteDelete == /\ deletePending /\ deletePending' = FALSE
                  /\ blob' = FALSE /\ deleted' = TRUE
                  /\ retired' = IF ForgetColdRetirement THEN FALSE ELSE retired
                  /\ UNCHANGED <<committed, upload, attached, participants,
                     members, online, applied, snapshots, purged, requested,
                     floors, fences, readers, transfer, joining, joinFloor>>
LateUpload == /\ deleted /\ upload /\ upload' = FALSE /\ blob' = TRUE
              /\ UNCHANGED <<committed, deleted, attached, deletePending,
                 participants, members, online, applied, snapshots, purged, requested,
                 floors, fences, readers, transfer, joining, joinFloor, retired>>
Reattach == /\ ReattachRetiredObject /\ committed = 3 /\ ~attached
            /\ attached' = TRUE
            /\ UNCHANGED <<committed, blob, upload, deleted, deletePending,
               participants, members, online, applied, snapshots, purged, requested,
               floors, fences, readers, transfer, joining, joinFloor, retired>>

Next == Upload \/ Publish \/ Fork \/ Retire \/ Remove \/ Admit \/ StartTransfer
        \/ FinishTransfer \/ AdvanceGc \/ IssueDelete \/ CompleteDelete \/ LateUpload \/ Reattach
        \/ \E n \in Nodes : Apply(n) \/ Checkpoint(n) \/ RequestPurge(n)
           \/ CompletePurge(n) \/ Fence(n) \/ Certify(n) \/ Read(n)
           \/ FinishRead(n) \/ Crash(n) \/ Restart(n)
SafetySpec == Init /\ [][Next]_vars
DataSafe == /\ (attached => blob)
            /\ (readers # {} => blob)
            /\ (TransferCut \in {1, 2} => blob)
            /\ \A n \in Initial \cup (IF joining = "Ready" THEN {3} ELSE {}) :
                 NeedsObject(snapshots[n]) => blob
Safe == /\ DataSafe
        /\ \A n \in participants : floors[0] <= floors[n]
        /\ \A n \in participants : floors[n] = 3 /\ (n # 3 \/ joining = "Ready")
             => snapshots[n] = 3 /\ purged[n] = 3

\* Stable period: two healthy participants, finite old PUT, fair durable storage,
\* no new references. Offline participants and infinite readers intentionally
\* have no GC progress guarantee. Persistent retirement permits reconciliation.
LiveNext == Upload \/ Publish \/ Fork \/ Retire \/ AdvanceGc \/ IssueDelete \/ CompleteDelete
            \/ LateUpload \/ \E n \in Initial : Apply(n) \/ Checkpoint(n)
               \/ RequestPurge(n) \/ CompletePurge(n) \/ Fence(n) \/ Certify(n)
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(Upload) /\ WF_vars(Publish)
            /\ WF_vars(Fork) /\ WF_vars(Retire) /\ WF_vars(AdvanceGc) /\ WF_vars(IssueDelete)
            /\ WF_vars(CompleteDelete) /\ WF_vars(LateUpload)
            /\ \A n \in Initial : WF_vars(Apply(n)) /\ WF_vars(Checkpoint(n))
               /\ WF_vars(RequestPurge(n)) /\ WF_vars(CompletePurge(n))
               /\ WF_vars(Fence(n)) /\ WF_vars(Certify(n))
Reclaimed == <> (deleted /\ ~blob /\ ~upload /\ ~deletePending)
=============================================================================
