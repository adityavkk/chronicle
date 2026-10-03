----------------------------- MODULE StreamOrder -----------------------------
EXTENDS Integers
CONSTANTS BadNumeric, BadDuplicate
VARIABLES token, producer, effects, prior, supplied, duplicate, accepted
vars == <<token,producer,effects,prior,supplied,duplicate,accepted>>

(* Token ranks: 0=absent, 1=present empty, 2="10", 3="2". Raft provides
   committed application. This bounded model checks only ordering/dedup
   composition, not consensus, HTTP parsing, lifecycle or snapshot transport. *)
Init == /\ token=0 /\ producer=-1 /\ effects=0
        /\ prior=0 /\ supplied=0 /\ duplicate=FALSE /\ accepted=FALSE
Numeric(t) == CASE t=2 -> 10 [] t=3 -> 2 [] OTHER -> t-2
Valid(t) == t=0 \/ token=0 \/
           (IF BadNumeric THEN Numeric(t)>Numeric(token) ELSE t>token)
Append(q,t) ==
  LET dup == q<=producer
      fresh == q=producer+1 /\ Valid(t)
  IN /\ prior'=token /\ supplied'=t /\ duplicate'=dup
     /\ accepted'=(~dup /\ fresh)
     /\ token'=IF t#0 /\ ((~dup /\ fresh) \/ (BadDuplicate /\ dup))
                THEN t ELSE token
     /\ producer'=IF ~dup /\ fresh THEN q ELSE producer
     /\ effects'=IF ~dup /\ fresh THEN effects+1 ELSE effects
Next == \E q \in 0..2, t \in 0..3 : Append(q,t)
Spec == Init /\ [][Next]_vars
StrictToken == accepted /\ supplied#0 /\ prior#0 => token>prior
DuplicateNoEffect == duplicate => token=prior
AbsentNoEffect == supplied=0 => token=prior
ProducerEffects == effects=producer+1
=============================================================================
