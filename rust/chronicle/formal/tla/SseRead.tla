------------------------------ MODULE SseRead ------------------------------
EXTENDS Naturals
CONSTANTS BadCoverage, BadIncarnation
VARIABLES tail, inc, closed, connected, position, covered, observed, phase, reports, last
vars == <<tail,inc,closed,connected,position,covered,observed,phase,reports,last>>

(* Raft supplies committed bytes and safe strict observations; Projection.tla
   separately checks their range materialization. A data event may span multiple
   transport chunks. Only its completed encoding permits a following control. *)
Init == /\ tail \in 0..1 /\ position=tail /\ covered=tail
        /\ inc=1 /\ closed=FALSE /\ connected=TRUE /\ reports=0
        /\ phase="observe"
        /\ observed=[tail |-> tail, inc |-> inc, closed |-> closed]
        /\ last=[next |-> position, inc |-> 1]

Append == /\ ~closed /\ tail<2 /\ tail'=tail+1
          /\ UNCHANGED <<inc,closed,connected,position,covered,observed,phase,reports,last>>
Close == /\ closed'=TRUE
         /\ UNCHANGED <<tail,inc,connected,position,covered,observed,phase,reports,last>>
Recreate == /\ inc=1 /\ inc'=2 /\ tail'=0 /\ closed'=FALSE
            /\ UNCHANGED <<connected,position,covered,observed,phase,reports,last>>
Connectivity == /\ connected'=~connected
                /\ UNCHANGED <<tail,inc,closed,position,covered,observed,phase,reports,last>>
Observe == /\ connected /\ phase="observe" /\ reports<3
           /\ (inc=1 \/ BadIncarnation) /\ tail>=position
           /\ observed'=[tail |-> tail, inc |-> inc, closed |-> closed]
           /\ phase'="data"
           /\ UNCHANGED <<tail,inc,closed,connected,position,covered,reports,last>>
Data == /\ phase="data" /\ covered'=observed.tail /\ phase'="control"
        /\ UNCHANGED <<tail,inc,closed,connected,position,observed,reports,last>>
Control == /\ phase="control" /\ reports'=reports+1
           /\ last'=[next |-> IF BadCoverage THEN tail ELSE observed.tail,
                       inc |-> observed.inc]
           /\ position'=observed.tail
           /\ phase'=IF observed.closed THEN "done" ELSE "wait"
           /\ UNCHANGED <<tail,inc,closed,connected,covered,observed>>
Wake == /\ phase="wait" /\ phase'="observe"
        /\ UNCHANGED <<tail,inc,closed,connected,position,covered,observed,reports,last>>
(* Failure/cancellation/connection deadline ends delivery without a new cursor.
   This action does not claim successful receipt of earlier HTTP chunks. *)
Stop == /\ phase#"done" /\ phase'="done"
        /\ UNCHANGED <<tail,inc,closed,connected,position,covered,observed,reports,last>>

Next == Append \/ Close \/ Recreate \/ Connectivity \/ Observe \/ Data \/ Control \/ Wake \/ Stop
Spec == Init /\ [][Next]_vars
NoSkippedBytes == last.next<=covered
IncarnationBound == last.inc=1
=============================================================================
