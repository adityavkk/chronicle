--------------------------- MODULE Ownership ---------------------------
EXTENDS Naturals, FiniteSets
CONSTANT ExclusiveLock
Processes == {1, 2}
VARIABLES active, cache, disk, acknowledged
vars == <<active, cache, disk, acknowledged>>
Init == /\ active = {}
        /\ cache = [p \in Processes |-> {}]
        /\ disk = {}
        /\ acknowledged = {}
Open(p) == /\ p \notin active
           /\ (~ExclusiveLock \/ active = {})
           /\ active' = active \cup {p}
           /\ cache' = [cache EXCEPT ![p] = disk]
           /\ UNCHANGED <<disk, acknowledged>>
(* A durable application transaction publishes the owner's cached state.
   Raft correctness assumes one running process per durable node identity;
   this model checks that missing assumption, not the Raft protocol. *)
Apply(p) == /\ p \in active
            /\ p \notin cache[p]
            /\ disk' = cache[p] \cup {p}
            /\ cache' = [cache EXCEPT ![p] = disk']
            /\ acknowledged' = acknowledged \cup {p}
            /\ UNCHANGED active
Crash(p) == /\ p \in active
            /\ active' = active \ {p}
            /\ UNCHANGED <<cache, disk, acknowledged>>
Next == \E p \in Processes : Open(p) \/ Apply(p) \/ Crash(p)
Spec == Init /\ [][Next]_vars
AcknowledgedRetained == acknowledged \subseteq disk
=============================================================================
