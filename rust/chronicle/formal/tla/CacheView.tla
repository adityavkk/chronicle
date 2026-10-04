------------------------------ MODULE CacheView ------------------------------
EXTENDS Naturals, TLC
CONSTANT BadSample
VARIABLES incarnation, tail, captured, cached, reply
vars == <<incarnation, tail, captured, cached, reply>>
View == [inc |-> incarnation, end |-> tail]
Init == /\ incarnation=1 /\ tail=0
        /\ captured=View /\ cached=View
        /\ reply=[tag |-> View, body |-> View]
Append == /\ tail<2 /\ tail'=tail+1
          /\ UNCHANGED <<incarnation,captured,cached,reply>>
Recreate == /\ incarnation<2 /\ incarnation'=incarnation+1 /\ tail'=0
            /\ UNCHANGED <<captured,cached,reply>>
Capture == /\ captured'=View
           /\ UNCHANGED <<incarnation,tail,cached,reply>>
Respond == /\ reply'=[tag |-> IF BadSample THEN View ELSE captured,
                      body |-> captured]
           /\ cached'=captured
           /\ UNCHANGED <<incarnation,tail,captured>>
Next == Append \/ Recreate \/ Capture \/ Respond
Spec == Init /\ [][Next]_vars
ValidatorBound == reply.tag=reply.body
=============================================================================
