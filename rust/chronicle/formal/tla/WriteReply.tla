------------------------------ MODULE WriteReply ------------------------------
EXTENDS Naturals
CONSTANTS BadRequestEcho, BadLateSample
VARIABLES closed, tail, cached, phase, boundary, requested, reply
vars == <<closed,tail,cached,phase,boundary,requested,reply>>

(* Raft supplies committed Apply. A cached producer tuple may be retried with
   a different close flag; it retains its effect, not the new request's intent.
   This models response closure, not consensus or all producer validation. *)
Init == /\ closed \in BOOLEAN /\ tail=1 /\ cached \in BOOLEAN
        /\ phase="ready" /\ boundary=closed /\ requested=FALSE /\ reply=FALSE

Apply(producer,close,empty) ==
  /\ phase="ready"
  /\ closed' = IF closed \/ (producer /\ cached) THEN closed ELSE close
  /\ tail' = IF closed \/ (producer /\ cached) \/ empty THEN tail ELSE tail+1
  /\ boundary'=closed' /\ requested'=close /\ phase'="applied"
  /\ UNCHANGED <<cached,reply>>

(* Concurrent operations after the reply boundary must not rewrite its metadata. *)
OtherClose == /\ phase="applied" /\ closed'=TRUE
              /\ UNCHANGED <<tail,cached,phase,boundary,requested,reply>>
Recreate == /\ phase="applied" /\ closed'=FALSE /\ tail'=0
            /\ UNCHANGED <<cached,phase,boundary,requested,reply>>
Respond == /\ phase="applied" /\ phase'="done"
           /\ reply'=IF BadRequestEcho THEN requested
                      ELSE IF BadLateSample THEN closed ELSE boundary
           /\ UNCHANGED <<closed,tail,cached,boundary,requested>>

Next == (\E p,c,e \in BOOLEAN : Apply(p,c,e)) \/ OtherClose \/ Recreate \/ Respond
Spec == Init /\ [][Next]_vars
ClosureFromApply == phase="done" => reply=boundary
=============================================================================
