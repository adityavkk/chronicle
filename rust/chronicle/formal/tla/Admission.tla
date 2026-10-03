----------------------------- MODULE Admission -----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Requests, Limit, ReleaseOnTimeout
VARIABLES active, pending, held
vars == <<active, pending, held>>
Init == active = {} /\ pending = {} /\ held = {}
Admit(r) == /\ r \notin held /\ r \notin pending
            /\ Cardinality(held) < Limit
            /\ active' = active \cup {r} /\ held' = held \cup {r}
            /\ UNCHANGED pending
Submit(r) == /\ r \in active /\ r \notin pending
             /\ pending' = pending \cup {r}
             /\ UNCHANGED <<active, held>>
Finish(r) == /\ r \in active
             /\ active' = active \ {r}
             /\ held' = IF ReleaseOnTimeout \/ r \notin pending
                        THEN held \ {r} ELSE held
             /\ UNCHANGED pending
Complete(r) == /\ r \in pending
               /\ pending' = pending \ {r}
               /\ held' = IF r \in active THEN held ELSE held \ {r}
               /\ UNCHANGED active
Next == \E r \in Requests : Admit(r) \/ Submit(r) \/ Finish(r) \/ Complete(r)
Spec == Init /\ [][Next]_vars
PendingOwned == pending \subseteq held
Bounded == Cardinality(pending) <= Limit
=============================================================================
