------------------------- MODULE AdmissionEpoch -------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANT CacheAcrossTerm
VARIABLES term, prepared, offered, retained, charged
vars == <<term,prepared,offered,retained,charged>>
Init == /\ term = 0 /\ prepared = 0 /\ offered = 0
        /\ retained = {} /\ charged = {}
\* All entries here are already fsynced. Publication/Receipts separately check
\* durability. Every new owner must cover inherited work before new admission.
Admit == /\ prepared = term /\ offered < 3 /\ Cardinality(charged) < 2
         /\ offered' = offered+1 /\ retained' = retained \cup {offered'}
         /\ charged' = charged \cup {offered'} /\ UNCHANGED <<term,prepared>>
Elect == /\ term < 2 /\ term' = term+1 /\ charged' = {}
         /\ prepared' = IF CacheAcrossTerm THEN term' ELSE prepared
         /\ UNCHANGED <<offered,retained>>
Prepare == /\ prepared # term /\ retained = {} /\ prepared' = term
           /\ UNCHANGED <<term,offered,retained,charged>>
Resolve == /\ retained # {}
           /\ \E r \in retained: /\ retained' = retained \ {r}
                                /\ charged' = charged \ {r}
           /\ UNCHANGED <<term,prepared,offered>>
Next == Admit \/ Elect \/ Prepare \/ Resolve
SafetySpec == Init /\ [][Next]_vars
Safe == /\ charged \subseteq retained /\ Cardinality(retained) <= 2
        /\ (prepared = term => retained = charged)
LiveSpec == SafetySpec /\ WF_vars(Prepare) /\ WF_vars(Admit) /\ WF_vars(Resolve)
Progress == <> (offered = 3 /\ retained = {})
=========================================================================
