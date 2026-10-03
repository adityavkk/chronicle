----------------------------- MODULE Scenario -----------------------------
EXTENDS Chronicle

P1 == CHOOSE p \in Producers : TRUE
P2 == CHOOSE p \in Producers : p # P1
ScenarioCmd(i) ==
  IF i=1 THEN [kind |-> "write",p |-> P1,epoch |-> 1,seq |-> 0,inc |-> 0,bytes |-> 1]
  ELSE IF i=2 THEN [kind |-> "write",p |-> P2,epoch |-> 1,seq |-> 0,inc |-> 0,bytes |-> 2]
  ELSE IF i=3 THEN [kind |-> "write",p |-> P1,epoch |-> 1,seq |-> 1,inc |-> 0,bytes |-> 3]
  ELSE [kind |-> "write",p |-> P1,epoch |-> 1,seq |-> 0,inc |-> 0,bytes |-> 1]

ScenarioCommit == /\ Len(log)<4 /\ log'=Append(log,ScenarioCmd(Len(log)+1))
  /\ UNCHANGED <<applied,pending,completed,results,events,byteEnd,producer,
    incarnation,life,expiryInc,snapshot,owner,term,members,learners,caughtIndex,
    promotions,authority>>
ScenarioNext == ScenarioCommit \/ ApplyNext
ScenarioSpec == Init /\ [][ScenarioNext]_vars /\ WF_vars(ApplyNext)
DuplicateCommittedOnceVisible == Len(log)=4 ~> (applied=4 /\ Len(events)=3 /\ byteEnd=6)
=============================================================================
