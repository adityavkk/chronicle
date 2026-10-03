------------------------------ MODULE WriteReply ------------------------------
EXTENDS Naturals
CONSTANTS BadRequestEcho, BadLateSample
VARIABLES closed, tail, cached, phase, boundary, requested, reply
VARIABLES seq, appliedSeq, requestSeq, replySeq
vars == <<closed,tail,cached,phase,boundary,requested,reply,
          seq,appliedSeq,requestSeq,replySeq>>
NoProducer == 4

(* Raft supplies committed Apply. A cached producer tuple may be retried with
   a different close flag; it retains its effect, not the new request's intent.
   This models response closure and highest accepted sequence publication,
   not consensus or all producer validation. *)
Init == /\ closed \in BOOLEAN /\ tail=1 /\ cached \in BOOLEAN
        /\ phase="ready" /\ boundary=closed /\ requested=FALSE /\ reply=FALSE
        /\ seq \in 0..1 /\ appliedSeq=NoProducer /\ requestSeq=NoProducer /\ replySeq=NoProducer

Apply(producer,close,empty) ==
  /\ phase="ready"
  /\ closed' = IF closed \/ (producer /\ cached) THEN closed ELSE close
  /\ tail' = IF closed \/ (producer /\ cached) \/ empty THEN tail ELSE tail+1
  /\ boundary'=closed' /\ requested'=close /\ phase'="applied"
  /\ seq'=IF producer /\ ~cached /\ ~closed THEN seq+1 ELSE seq
  /\ appliedSeq'=IF producer THEN seq' ELSE NoProducer
  /\ requestSeq'=IF producer THEN (IF cached THEN 0 ELSE seq+1) ELSE NoProducer
  /\ UNCHANGED <<cached,reply,replySeq>>

(* Concurrent operations after the reply boundary must not rewrite its metadata. *)
OtherClose == /\ phase="applied" /\ closed'=TRUE
              /\ UNCHANGED <<tail,cached,phase,boundary,requested,reply,
                              seq,appliedSeq,requestSeq,replySeq>>
OtherAppend == /\ phase="applied" /\ ~closed /\ seq<3
               /\ seq'=seq+1 /\ tail'=tail+1
               /\ UNCHANGED <<closed,cached,phase,boundary,requested,reply,
                               appliedSeq,requestSeq,replySeq>>
Recreate == /\ phase="applied" /\ closed'=FALSE /\ tail'=0 /\ seq'=0
            /\ UNCHANGED <<cached,phase,boundary,requested,reply,
                            appliedSeq,requestSeq,replySeq>>
Respond == /\ phase="applied" /\ phase'="done"
           /\ reply'=IF BadRequestEcho THEN requested
                      ELSE IF BadLateSample THEN closed ELSE boundary
           /\ replySeq'=IF appliedSeq=NoProducer THEN NoProducer
                         ELSE IF BadRequestEcho THEN requestSeq
                         ELSE IF BadLateSample THEN seq ELSE appliedSeq
           /\ UNCHANGED <<closed,tail,cached,boundary,requested,
                           seq,appliedSeq,requestSeq>>

Next == (\E p,c,e \in BOOLEAN : Apply(p,c,e)) \/ OtherClose \/ OtherAppend \/ Recreate \/ Respond
Spec == Init /\ [][Next]_vars
ClosureFromApply == phase="done" => reply=boundary
ProducerFromApply == phase="done" => replySeq=appliedSeq
=============================================================================
