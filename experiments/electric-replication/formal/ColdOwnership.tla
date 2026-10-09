-------------------------- MODULE ColdOwnership --------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS EarlyColdPublish, AppliedIsCheckpoint, RequestedIsPurged,
          IgnoreColdReaders, ForgetRemovedParticipant, UnregisteredJoin,
          ReattachRetiredObject, ForgetColdRetirement,
          InventoryCampaign, InventoryFault
VARIABLES committed, blob, upload, deleted, attached, deletePending,
          participants, members, online, applied, snapshots, purged, requested,
          floors, fences, readers, transfer, joining, joinFloor, retired, g
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
BaseFairness == WF_vars(Upload) /\ WF_vars(Publish)
            /\ WF_vars(Fork) /\ WF_vars(Retire) /\ WF_vars(AdvanceGc) /\ WF_vars(IssueDelete)
            /\ WF_vars(CompleteDelete) /\ WF_vars(LateUpload)
            /\ \A n \in Initial : WF_vars(Apply(n)) /\ WF_vars(Checkpoint(n))
               /\ WF_vars(RequestPurge(n)) /\ WF_vars(CompletePurge(n))
               /\ WF_vars(Fence(n)) /\ WF_vars(Certify(n))
Reclaimed == <> (deleted /\ ~blob /\ ~upload /\ ~deletePending)

\* Certified-inventory extension. The earlier specification above remains the
\* focused recovery-certificate model. This scenario composes those boundaries
\* with multiple immutable Begins and THREE inventory replacements; it does not
\* infer this algorithm's correctness from the earlier one-object state count.
\*
\* The committed history schema deliberately places pending/live/retired objects
\* on different sides of inventory cuts. Consensus total order and unique accepted
\* singleton Begin indices are assumed, not re-proved here. All g transitions that
\* change pos/live/pending/active are atomic committed WAL metadata transitions.
\* Captures, PUTs, DELETEs, snapshots, purge and scans interleave independently.
\* births/content/dead/attempts are history/object-server ghost state, NOT retained
\* terminal records in the implementation's authoritative state machine.
Keys == {"p", "q", "a", "i1", "i2", "i3"}
Inventories == {"a", "i1", "i2", "i3"}
Birth(k) == CASE k = "p" -> 1 [] k = "a" -> 2 [] k = "i1" -> 4
                [] k = "q" -> 7 [] k = "i2" -> 9 [] OTHER -> 12
Cut(k) == CASE k = "a" -> 1 [] k = "i1" -> 3 [] k = "i2" -> 7 [] k = "i3" -> 11 [] OTHER -> 0
InvInitial == IF InventoryCampaign \in {"removed", "restart"} THEN {1, 2} ELSE {1}
InvNodes == {1, 2, 3}
NoRoot == "none"
RootsAt(cut) == CASE cut < 6 -> {} [] cut < 10 -> {"p", "i1"}
                    [] cut < 13 -> {"i2"} [] OTHER -> {"i3"}
SnapshotCutNow == CASE g.pos >= 13 -> 13 [] g.pos >= 11 -> 11
                      [] g.pos >= 7 -> 7 [] g.pos >= 3 -> 3 [] g.pos >= 1 -> 1 [] OTHER -> 0
Target == CASE g.pos >= 11 -> 11 [] g.pos >= 7 -> 7 [] g.pos >= 3 -> 3 [] OTHER -> 1
CanCommit == Cardinality(g.online \cap g.members) * 2 > Cardinality(g.members)
References == g.live \cup g.pending
Needed(n) == RootsAt(g.node[n].snap) \cup
             (IF g.pos >= 7 /\ g.node[n].snap < 7 THEN {"p"} ELSE {})
\* The second term is a committed JSON-fork replay dependency. An applied-only
\* certificate cannot discard it before a durable replacement recovery cut.
InvEmpty == [pos |-> 0, live |-> {}, pending |-> {}, dead |-> {},
            physical |-> {}, inflight |-> {}, known |-> {},
            late |-> {"p", "a"}, deletes |-> {},
            captured |-> {}, contents |-> [k \in Inventories |-> {}],
            active |-> NoRoot, previous |-> NoRoot, scan |-> 0,
            scanRoot |-> NoRoot, scanFrom |-> 0, refresh |-> FALSE,
            participants |-> InvInitial, members |-> InvInitial, online |-> InvInitial,
            node |-> [n \in InvNodes |-> [snap |-> 0, applied |-> 0, request |-> 0,
                       purge |-> 0, floor |-> 0, pin |-> {}]],
            joining |-> "Absent", joinFloor |-> 0,
            transfer |-> 0, sourcePin |-> FALSE, crashed |-> FALSE]
\* Multi-participant suffix starts at the reachable second activation, rather
\* than repeating every earlier independent PUT interleaving for three nodes.
\* Both initial participants certified cut 7; their snapshots still need p/i1.
\* The inventory campaign checks the complete preceding lifecycle from empty.
InvInit == IF InventoryCampaign \in {"removed", "transfer", "restart"}
           THEN [InvEmpty EXCEPT !.pos = 10, !.live = {"i2"}, !.pending = {"q"},
                 !.dead = {"p", "a", "i1"}, !.physical = Keys \ {"i3"},
                 !.known = Keys \ {"i3"}, !.captured = {"a", "i1", "i2"},
                 !.contents = [k \in Inventories |-> CASE k = "i2" -> {"p", "q", "i1"}
                                    [] k = "i3" -> {} [] OTHER -> {"p"}],
                 !.active = "i2", !.previous = "i1",
                 !.node = [n \in InvNodes |-> IF n \in InvInitial
                     THEN [snap |-> 7, applied |-> 10, request |-> 7, purge |-> 7, floor |-> 7, pin |-> {}]
                     ELSE InvEmpty.node[n]]]
           ELSE InvEmpty
allvars == <<vars, g>>
Bad(name) == InventoryFault = name

Begin(k) == /\ CanCommit /\ g.pos = Birth(k) - 1
            /\ (k \notin Inventories \/ k \in g.captured)
            /\ g' = [g EXCEPT !.pos = @ + 1, !.pending = @ \cup {k}]
Capture(k) == /\ k \in Inventories /\ g.pos = Cut(k) /\ k \notin g.captured
              /\ g' = [g EXCEPT !.captured = @ \cup {k},
                    !.contents[k] = IF Bad("OmitPending") THEN g.live
                                   ELSE IF Bad("OmitInventoryRoot") THEN References \ Inventories
                                   ELSE References]
StartPut(k) == /\ k \in g.pending /\ k \notin g.inflight /\ k \notin g.known
               /\ g' = [g EXCEPT !.inflight = @ \cup {k}]
FinishPut(k) == /\ k \in g.inflight
                /\ g' = [g EXCEPT !.physical = @ \cup {k}, !.inflight = @ \ {k},
                         !.known = IF g.online # {} THEN @ \cup {k} ELSE @]
UnknownPut(k) == /\ k \in g.inflight
                 /\ \E effect \in {TRUE, FALSE} :
                     g' = [g EXCEPT !.inflight = @ \ {k},
                           !.pending = IF Bad("DropPendingOnUnknown") THEN @ \ {k} ELSE @,
                           !.physical = IF effect THEN @ \cup {k} ELSE @]
LatePut(k) == /\ k \in g.late /\ k \in g.dead
             /\ g' = [g EXCEPT !.late = @ \ {k}, !.physical = @ \cup {k}]
PublishP == /\ CanCommit /\ g.pos = 5 /\ "p" \in g.pending
            /\ ("p" \in g.known \/ Bad("UnverifiedPublication"))
            /\ g' = [g EXCEPT !.pos = 6, !.pending = @ \ {"p"}, !.live = @ \cup {"p"}]
AbortInventory == /\ CanCommit /\ g.pos = 2
                  /\ g' = [g EXCEPT !.pos = 3, !.pending = @ \ {"a"}, !.dead = @ \cup {"a"}]
RetireP == /\ CanCommit /\ g.pos = 7 /\ "i2" \in g.captured
           /\ g' = [g EXCEPT !.pos = 8, !.live = @ \ {"p"}, !.dead = @ \cup {"p"},
                    !.contents["i2"] = IF Bad("MutableCapture") THEN References \ {"p"}
                                          ELSE @]
AbortQ == /\ CanCommit /\ g.pos = 10
          /\ g' = [g EXCEPT !.pos = 11, !.pending = @ \ {"q"}, !.dead = @ \cup {"q"}]
Certificate(n, cut) == g.node[n].floor >= cut
Covered(cut) == \A n \in g.participants : Certificate(n, cut)
Activate(k) == /\ CanCommit /\ k \in Inventories \ {"a"} /\ g.pos = Birth(k)
               /\ k \in g.pending /\ (k \in g.known \/ Bad("UnverifiedPublication"))
               /\ Covered(Cut(k))
               /\ g' = [g EXCEPT !.pos = @ + 1, !.pending = @ \ {k},
                    !.live = (@ \ {g.active}) \cup {k},
                    !.dead = IF g.active = NoRoot THEN @ ELSE @ \cup {g.active},
                    !.previous = g.active, !.active = k,
                    !.scan = IF Bad("ResetScan") THEN 0 ELSE @,
                    !.physical = IF Bad("DeletePreviousInventory") THEN @ \ {g.active} ELSE @]
StalePublish(k) == /\ Bad("ReattachAbsentBegin") /\ k \in g.dead /\ k \notin g.live
                  /\ g' = [g EXCEPT !.live = @ \cup {k}]

ApplyNode(n) == /\ n \in g.online /\ g.node[n].applied < g.pos
                /\ g' = [g EXCEPT !.node[n].applied = g.pos]
CheckpointNode(n) == /\ n \in g.online /\ g.node[n].applied >= SnapshotCutNow
                     /\ g.node[n].snap < SnapshotCutNow
                     /\ g' = [g EXCEPT !.node[n].snap = SnapshotCutNow]
RequestPurgeNode(n) == /\ n \in g.online /\ g.node[n].snap > g.node[n].request
                       /\ g' = [g EXCEPT !.node[n].request = g.node[n].snap]
FinishPurgeNode(n) == /\ n \in g.online /\ g.node[n].request > g.node[n].purge
                      /\ g' = [g EXCEPT !.node[n].purge = g.node[n].request]
CertifyNode(n) == /\ n \in g.online \cap g.participants /\ g.node[n].floor < Target
                 /\ (IF Bad("AppliedCertificate") THEN g.node[n].applied >= Target
                     ELSE g.node[n].snap >= Target)
                 /\ (IF Bad("RequestedPurge") THEN g.node[n].request >= Target
                     ELSE IF Bad("AppliedCertificate") THEN TRUE ELSE g.node[n].purge >= Target)
                 /\ (Bad("IgnorePins") \/ (g.node[n].pin = {} /\ (n # 1 \/ ~g.sourcePin)))
                 /\ g' = [g EXCEPT !.node[n].floor = Target]
PinReader(n) == /\ n \in g.online /\ g.node[n].pin = {}
                /\ g.node[n].snap >= g.node[n].floor /\ RootsAt(g.node[n].snap) # {}
                /\ g' = [g EXCEPT !.node[n].pin = RootsAt(g.node[n].snap)]
DrainReader(n) == /\ g.node[n].pin # {} /\ g' = [g EXCEPT !.node[n].pin = {}]
RemoveNode == /\ InventoryCampaign = "removed" /\ 2 \in g.members
              /\ g' = [g EXCEPT !.members = @ \ {2},
                       !.participants = IF Bad("ForgetRemoved") THEN @ \ {2} ELSE @]
AdmitLearner == /\ InventoryCampaign = "transfer" /\ g.joining = "Absent"
                /\ g' = [g EXCEPT !.joining = "Pending", !.joinFloor = Cut(g.active),
                   !.node[3].floor = Cut(g.active),
                   !.participants = IF Bad("UnregisteredTransfer") THEN @ ELSE @ \cup {3}]
InvStartTransfer == /\ g.joining = "Pending" /\ g.transfer = 0 /\ 1 \in g.online
                 /\ g.node[1].snap > 0 /\ g.node[1].snap >= g.joinFloor
                 /\ g.node[1].snap >= g.node[1].floor
                 /\ g' = [g EXCEPT !.transfer = g.node[1].snap, !.sourcePin = TRUE]
InvFinishTransfer == /\ g.joining = "Pending" /\ g.transfer > 0
                  /\ g' = [g EXCEPT !.joining = "Ready", !.participants = @ \cup {3},
                     !.online = @ \cup {3}, !.node[3].snap = g.transfer,
                     !.node[3].purge = g.transfer, !.node[3].applied = g.transfer,
                     !.transfer = 0, !.sourcePin = FALSE]
CrashAll == /\ InventoryCampaign \in {"restart", "transfer"} /\ g.pos >= 10 /\ ~g.crashed
            /\ g' = [g EXCEPT !.online = {}, !.crashed = TRUE, !.sourcePin = FALSE,
                     !.node = [n \in InvNodes |-> [g.node[n] EXCEPT !.pin = {}]],
                     !.known = {}, !.scanRoot = NoRoot, !.scanFrom = 0, !.scan = 0,
                     !.active = IF Bad("ForgetRestartCertificate") THEN NoRoot ELSE @]
RestartNode(n) == /\ n \in g.participants \ g.online /\ (n # 3 \/ g.joining = "Ready")
                  /\ (~Bad("QuorumBootstrap") \/ CanCommit)
                  /\ Needed(n) \subseteq g.physical
                  /\ g' = [g EXCEPT !.online = @ \cup {n}, !.node[n].applied = g.pos]

\* One sorted, finite birth-prefix round. New objects cannot extend its bound.
\* The captured active identity supplies BOTH the cutoff and protected set.
\* An empty or complete round always wraps: old PUTs can appear behind it.
\* StartScan means the COMPLETE inventory was fetched and hash-verified into
\* an immutable local handle. Object deletion cannot invalidate that handle.
\* Hash/GET failure does not authorize absence or start a partial inventory.
ScanKey(index) == CASE index = 1 -> "p" [] index = 2 -> "a" [] index = 3 -> "i1"
                       [] index = 4 -> "q" [] index = 5 -> "i2" [] OTHER -> "i3"
EligibleAt(k, root, lower) == /\ root # NoRoot
                             /\ (Bad("IgnoreBirthCut") \/ Birth(k) <= IF Bad("MixedInventoryCut") THEN Target ELSE Cut(root))
                             /\ (Bad("EmptyOnInventoryError") \/ k \notin g.contents[root])
                             /\ (~Bad("DeltaOnlyScan") \/ Birth(k) > lower)
DeleteEligible(k) == EligibleAt(k, g.active, Cut(g.previous))
StartScan == /\ g.online # {} /\ g.active # NoRoot /\ g.scanRoot = NoRoot
             /\ g' = [g EXCEPT !.scanRoot = g.active, !.scanFrom = Cut(g.previous), !.scan = 0]
ScanLimit == IF Bad("IgnoreBirthCut") THEN 6
             ELSE Cardinality({k \in Keys : Birth(k) <= IF Bad("MixedInventoryCut") THEN Target ELSE Cut(g.scanRoot)})
Scan == /\ g.online # {} /\ g.scanRoot # NoRoot /\ g.scan < ScanLimit
        /\ LET k == ScanKey(g.scan + 1) IN
             g' = [g EXCEPT !.scan = @ + 1,
                    !.deletes = IF k \in g.physical /\ EligibleAt(k, g.scanRoot, g.scanFrom)
                                THEN @ \cup {k} ELSE @]
WrapScan == /\ g.scanRoot # NoRoot /\ g.scan = ScanLimit /\ ~Bad("StopAfterCleanScan")
            /\ g' = [g EXCEPT !.scanRoot = NoRoot, !.scanFrom = 0, !.scan = 0]
FinishDelete(k) == /\ k \in g.deletes
                  /\ g' = [g EXCEPT !.deletes = @ \ {k}, !.physical = @ \ {k}]
UnknownDelete(k) == /\ k \in g.deletes
                   /\ \E effect \in {TRUE, FALSE} :
                        g' = [g EXCEPT !.deletes = @ \ {k},
                                  !.physical = IF effect THEN @ \ {k} ELSE @]

\* Abstract further strictly-newer inventory generations after the finite
\* history: their projection on these old keys is unchanged. They must not
\* restart an in-progress prefix scan indefinitely. The bit represents fresh
\* work, NOT reuse of an object identity or rollback of a committed cut.
RefreshInventory == /\ InventoryCampaign = "scan" /\ g.pos = 13
                    /\ g' = [g EXCEPT !.refresh = ~@,
                             !.scan = IF Bad("ResetScan") THEN 0 ELSE @]
InvCommands == (\E k \in Keys : Begin(k)) \/ (\E k \in Inventories : Capture(k) \/ Activate(k))
               \/ PublishP \/ AbortInventory \/ RetireP \/ AbortQ
InvStorage == \E k \in Keys : StartPut(k) \/ FinishPut(k) \/ UnknownPut(k)
                  \/ LatePut(k) \/ FinishDelete(k) \/ UnknownDelete(k) \/ StalePublish(k)
InvParticipants == (\E n \in InvNodes : ApplyNode(n) \/ CheckpointNode(n)
                      \/ RequestPurgeNode(n) \/ FinishPurgeNode(n) \/ CertifyNode(n)
                      \/ PinReader(n) \/ DrainReader(n) \/ RestartNode(n))
                   \/ RemoveNode \/ AdmitLearner \/ InvStartTransfer \/ InvFinishTransfer \/ CrashAll
InvNext == InvCommands \/ InvStorage \/ InvParticipants \/ StartScan \/ Scan \/ WrapScan \/ RefreshInventory
InvSafetySpec == Init /\ g = InvInit /\ [][InvNext /\ UNCHANGED vars]_allvars

InvDataSafe == /\ g.live \subseteq g.physical
               /\ \A n \in InvInitial \cup (IF g.joining = "Ready" THEN {3} ELSE {}) :
                    /\ g.node[n].pin \subseteq g.physical
                    /\ Needed(n) \subseteq g.physical
               /\ (g.transfer > 0 => RootsAt(g.transfer) \subseteq g.physical)
InvNoResurrection == g.live \cap g.dead = {}
InvDeleteSafe == g.deletes \cap (g.live \cup g.pending) = {}
InvAccounting == g.live \cup g.pending \cup g.dead = {k \in Keys : Birth(k) <= g.pos}
InvCertificates == \A n \in g.participants :
                     (n # 3 \/ g.joining = "Ready") =>
                          g.node[n].floor <= g.node[n].snap /\ g.node[n].floor <= g.node[n].purge
InvSafe == InvDataSafe /\ InvNoResurrection /\ InvDeleteSafe /\ InvCertificates /\ InvAccounting
EligibleSet == {k \in Keys : DeleteEligible(k)}
InvMonotone == [][EligibleSet \subseteq EligibleSet']_g
\* No historical terminal record is counted as implementation state here.
\* Captures/contents represent immutable object bytes plus model history. The
\* required real implementation has at most one active and one pending inventory
\* descriptor; payload descriptors are only live or currently pending Begins.
InvShape == /\ Cardinality(g.pending \cap Inventories) <= 1
            /\ Cardinality(g.live \cap Inventories) <= 1
            /\ g.pending \cap g.live = {} /\ g.pending \cap g.dead = {}
            /\ (g.active = NoRoot <=> g.live \cap Inventories = {})
            /\ (g.active # NoRoot => g.active \in g.live)

\* Stable period: eventual successful object/snapshot/purge operations; no new
\* reader/learner/remove arrivals. Restart campaign includes one whole-cluster
\* crash. Fair retries and finite delayed PUTs, not timeouts, imply reclamation.
InvLiveNext == InvCommands \/ (\E k \in Keys : StartPut(k) \/ FinishPut(k) \/ LatePut(k)
                    \/ FinishDelete(k))
               \/ (\E n \in InvNodes : ApplyNode(n) \/ CheckpointNode(n) \/ RequestPurgeNode(n)
                    \/ FinishPurgeNode(n) \/ CertifyNode(n) \/ DrainReader(n) \/ RestartNode(n))
               \/ CrashAll \/ StartScan \/ Scan \/ WrapScan \/ RefreshInventory
InvLiveSpec == Init /\ g = InvInit /\ [][InvLiveNext /\ UNCHANGED vars]_allvars
               /\ WF_g(InvCommands) /\ WF_g(StartScan) /\ WF_g(Scan) /\ WF_g(WrapScan)
               /\ WF_g(CrashAll) /\ WF_g(RefreshInventory)
               /\ \A k \in Keys : WF_g(StartPut(k)) /\ WF_g(FinishPut(k))
                    /\ WF_g(LatePut(k)) /\ WF_g(FinishDelete(k))
               /\ \A n \in InvNodes : WF_g(ApplyNode(n)) /\ WF_g(CheckpointNode(n))
                    /\ WF_g(RequestPurgeNode(n)) /\ WF_g(FinishPurgeNode(n))
                    /\ WF_g(CertifyNode(n)) /\ WF_g(DrainReader(n)) /\ WF_g(RestartNode(n))
InvReclaimed == <>[] ({"p", "q", "a", "i1"} \cap g.physical = {} /\ g.pos = 13)
InvRestarted == <> (g.crashed /\ g.participants \subseteq g.online /\ g.pos = 13)

\* Keep the earlier certificate checks reproducible without unconstrained new
\* variables. These wrappers freeze the inventory scenario, not its assertions.
SafetySpec == Init /\ g = InvInit /\ [][Next /\ UNCHANGED g]_allvars
LiveSpec == Init /\ g = InvInit /\ [][LiveNext /\ UNCHANGED g]_allvars /\ BaseFairness
=============================================================================
