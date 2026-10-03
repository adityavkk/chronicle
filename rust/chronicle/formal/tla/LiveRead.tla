------------------------------ MODULE LiveRead ------------------------------
EXTENDS Naturals
CONSTANTS BadTimeout, BadIncarnation
VARIABLES tail, inc, closed, connected, from, requestInc, observed, phase, reply
vars == <<tail,inc,closed,connected,from,requestInc,observed,phase,reply>>

(* tail is committed/applied authority supplied by Raft. Observe abstracts a
   successful strict barrier followed by a captured application view. This is
   not another quorum protocol. Coverage counts stored bytes, not JSON wrappers. *)
Init == /\ tail \in 0..1 /\ from=tail /\ inc=1 /\ requestInc=1
        /\ closed=FALSE /\ connected=TRUE /\ phase="waiting"
        /\ observed=[tail |-> tail, inc |-> inc, closed |-> closed]
        /\ reply=[ok |-> FALSE, next |-> from, covered |-> 0, inc |-> inc]

Append == /\ ~closed /\ tail<2 /\ tail'=tail+1
          /\ UNCHANGED <<inc,closed,connected,from,requestInc,observed,phase,reply>>
Close == /\ closed'=TRUE
         /\ UNCHANGED <<tail,inc,connected,from,requestInc,observed,phase,reply>>
Recreate == /\ inc=1 /\ inc'=2 /\ tail'=0 /\ closed'=FALSE
            /\ UNCHANGED <<connected,from,requestInc,observed,phase,reply>>
Connectivity == /\ connected'=~connected
                /\ UNCHANGED <<tail,inc,closed,from,requestInc,observed,phase,reply>>
Observe == /\ connected /\ phase="waiting" /\ phase'="observed"
           /\ observed'=[tail |-> tail, inc |-> inc, closed |-> closed]
           /\ UNCHANGED <<tail,inc,closed,connected,from,requestInc,reply>>
Wait == /\ phase="observed" /\ phase'="waiting"
        /\ UNCHANGED <<tail,inc,closed,connected,from,requestInc,observed,reply>>
Reply == /\ phase="observed" /\ phase'="done"
         /\ (BadIncarnation \/ observed.inc=requestInc)
         /\ observed.tail>=from
         /\ reply'=[ok |-> TRUE,
              next |-> IF BadTimeout /\ observed.tail=from /\ tail>observed.tail
                       THEN tail ELSE observed.tail,
              covered |-> observed.tail-from, inc |-> observed.inc]
         /\ UNCHANGED <<tail,inc,closed,connected,from,requestInc,observed>>
Reject == /\ phase="observed" /\ observed.inc#requestInc /\ phase'="done"
          /\ UNCHANGED <<tail,inc,closed,connected,from,requestInc,observed,reply>>

Next == Append \/ Close \/ Recreate \/ Connectivity \/ Observe \/ Wait \/ Reply \/ Reject
Spec == Init /\ [][Next]_vars
NoSkippedBytes == reply.ok => reply.next=from+reply.covered
IncarnationBound == reply.ok => reply.inc=requestInc
=============================================================================
