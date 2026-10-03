----------------------------- MODULE Chronicle -----------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Nodes, Producers, MaxSeq, MaxEpoch, MaxLog,
          AckEarly, PendingDedup, OwnerFence, PromotionFence

WriteCmds == [kind : {"write"}, p : Producers, epoch : 0..MaxEpoch,
              seq : 0..MaxSeq, inc : 0..2, bytes : {1,2,3}]
ControlCmds == {[kind |-> k, p |-> p, epoch |-> 0, seq |-> 0,
                 inc |-> i, bytes |-> 0] :
                 k \in {"close", "delete", "expire", "create"},
                 p \in Producers, i \in 0..2}
Cmds == WriteCmds \cup ControlCmds

(* A client completion key includes the operation kind.  Effects use WriteKey;
   rejected committed commands are completed but never visible. *)
ClientKey(c) == <<c.kind,c.p,c.epoch,c.seq,c.inc,c.bytes>>
WriteKey(c) == <<c.p,c.epoch,c.seq,c.inc>>

VARIABLES log, applied, pending, completed, results, events, byteEnd,
          producer, incarnation, life, expiryInc, snapshot,
          owner, term, members, learners, caughtIndex, promotions, authority

vars == <<log,applied,pending,completed,results,events,byteEnd,producer,
          incarnation,life,expiryInc,snapshot,owner,term,members,learners,
          caughtIndex,promotions,authority>>

WriteAccepted(c) == c.inc = incarnation /\ life = "open" /\
  ((producer[c.p] = <<c.epoch,c.seq-1>> /\ c.seq > 0) \/
   (c.seq = 0 /\ c.epoch > producer[c.p][1]))

Apply(c) ==
  IF c.kind = "write" /\ WriteAccepted(c)
  THEN [events |-> Append(events,[key |-> WriteKey(c), start |-> byteEnd,
                                  finish |-> byteEnd+c.bytes]),
        byteEnd |-> byteEnd+c.bytes,
        producer |-> [producer EXCEPT ![c.p] = <<c.epoch,c.seq>>],
        incarnation |-> incarnation, life |-> life, expiryInc |-> expiryInc,
        outcome |-> "written"]
  ELSE IF c.kind = "close" /\ c.inc = incarnation /\ life # "deleted"
  THEN [events |-> events, byteEnd |-> byteEnd, producer |-> producer,
        incarnation |-> incarnation, life |-> "closed", expiryInc |-> expiryInc,
        outcome |-> "closed"]
  ELSE IF (c.kind = "delete" \/ c.kind = "expire") /\ c.inc = incarnation
          /\ (c.kind # "expire" \/ expiryInc = c.inc)
  THEN [events |-> events, byteEnd |-> byteEnd, producer |-> producer,
        incarnation |-> incarnation, life |-> "deleted", expiryInc |-> expiryInc,
        outcome |-> "deleted"]
  ELSE IF c.kind = "create" /\ life = "deleted" /\ c.inc = incarnation+1
  THEN [events |-> <<>>, byteEnd |-> 0,
        producer |-> [p \in Producers |-> <<0,0>>], incarnation |-> c.inc,
        life |-> "open", expiryInc |-> c.inc, outcome |-> "created"]
  ELSE [events |-> events, byteEnd |-> byteEnd, producer |-> producer,
        incarnation |-> incarnation, life |-> life, expiryInc |-> expiryInc,
        outcome |-> "rejected"]

Init == /\ log = <<>> /\ applied=0 /\ pending={} /\ completed={}
  /\ results = <<>> /\ events = <<>> /\ byteEnd=0
  /\ producer=[p \in Producers |-> <<0,0>>] /\ incarnation=0 /\ life="open"
  /\ expiryInc=0
  /\ snapshot=[idx |-> 0, events |-> <<>>, byteEnd |-> 0,
                producer |-> producer, inc |-> 0, life |-> "open", expiryInc |-> 0]
  /\ owner \in Nodes /\ term=0 /\ members={owner} /\ learners={}
  /\ caughtIndex=[n \in Nodes |-> 0] /\ promotions = <<>> /\ authority = <<>>

Submit(c) == /\ Len(log)<MaxLog /\ Cardinality(pending)<1 /\ c \notin pending
  /\ (~PendingDedup \/ ClientKey(c) \notin completed)
  /\ (~PendingDedup \/ ~\E x \in pending : ClientKey(x)=ClientKey(c))
  /\ pending'=pending \cup {c}
  /\ (IF AckEarly THEN completed'=completed \cup {ClientKey(c)} ELSE completed'=completed)
  /\ UNCHANGED <<log,applied,results,events,byteEnd,producer,incarnation,life,
                  expiryInc,snapshot,owner,term,members,learners,caughtIndex,promotions,authority>>

Commit(c) == /\ c \in pending /\ Len(log)<MaxLog
  /\ log'=Append(log,c) /\ pending'=pending\{c}
  /\ UNCHANGED <<applied,completed,results,events,byteEnd,producer,incarnation,
                  life,expiryInc,snapshot,owner,term,members,learners,caughtIndex,promotions,authority>>

ApplyNext == /\ applied<Len(log)
  /\ LET c == log[applied+1] r == Apply(c) IN
       /\ applied'=applied+1 /\ events'=r.events /\ byteEnd'=r.byteEnd
       /\ producer'=r.producer /\ incarnation'=r.incarnation /\ life'=r.life
       /\ expiryInc'=r.expiryInc /\ completed'=completed\cup{ClientKey(c)}
       /\ results'=(IF ClientKey(c) \in completed THEN results
                     ELSE Append(results,[key |-> ClientKey(c), outcome |-> r.outcome]))
  /\ UNCHANGED <<log,pending,snapshot,owner,term,members,learners,caughtIndex,promotions,authority>>

TakeSnapshot == /\ snapshot'=[idx |-> applied,events |-> events,byteEnd |-> byteEnd,
                               producer |-> producer,inc |-> incarnation,
                               life |-> life,expiryInc |-> expiryInc]
  /\ UNCHANGED <<log,applied,pending,completed,results,events,byteEnd,producer,
    incarnation,life,expiryInc,owner,term,members,learners,caughtIndex,promotions,authority>>
Recover == /\ applied'=snapshot.idx /\ events'=snapshot.events /\ byteEnd'=snapshot.byteEnd
  /\ producer'=snapshot.producer /\ incarnation'=snapshot.inc /\ life'=snapshot.life
  /\ expiryInc'=snapshot.expiryInc
  /\ UNCHANGED <<log,pending,completed,results,snapshot,owner,term,members,learners,caughtIndex,promotions,authority>>

AddLearner(n) == /\ n\notin members /\ learners'=learners\cup{n}
  /\ UNCHANGED <<log,applied,pending,completed,results,events,byteEnd,producer,incarnation,life,expiryInc,snapshot,owner,term,members,caughtIndex,promotions,authority>>
CatchUp(n) == /\ n\in learners /\ caughtIndex'=[caughtIndex EXCEPT ![n]=Len(log)]
  /\ UNCHANGED <<log,applied,pending,completed,results,events,byteEnd,producer,incarnation,life,expiryInc,snapshot,owner,term,members,learners,promotions,authority>>
Promote(n) == /\ n\in learners /\ (~PromotionFence \/ caughtIndex[n]>=Len(log))
  /\ members'=members\cup{n} /\ learners'=learners\{n}
  /\ promotions'=Append(promotions,[node |-> n,caught |-> caughtIndex[n],commit |-> Len(log)])
  /\ UNCHANGED <<log,applied,pending,completed,results,events,byteEnd,producer,incarnation,life,expiryInc,snapshot,owner,term,caughtIndex,authority>>
Transfer(n) == /\ n\in members /\ n#owner /\ term<2 /\ owner'=n /\ term'=term+1
  /\ UNCHANGED <<log,applied,pending,completed,results,events,byteEnd,producer,incarnation,life,expiryInc,snapshot,members,learners,caughtIndex,promotions,authority>>
AuthorityOp(n,t) == /\ n\in members /\ t\in 0..2 /\ Len(authority)<2
  /\ (~OwnerFence \/ (n=owner /\ t=term))
  /\ authority'=Append(authority,[issuer |-> n, issuedTerm |-> t,
                                  acceptedOwner |-> owner, acceptedTerm |-> term])
  /\ UNCHANGED <<log,applied,pending,completed,results,events,byteEnd,producer,incarnation,life,expiryInc,snapshot,owner,term,members,learners,caughtIndex,promotions>>

Next == (\E c\in Cmds: Submit(c) \/ Commit(c)) \/ ApplyNext \/ TakeSnapshot \/ Recover
  \/ (\E n\in Nodes: AddLearner(n) \/ CatchUp(n) \/ Promote(n) \/ Transfer(n)
      \/ (\E t\in 0..2: AuthorityOp(n,t)))
Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ WF_vars(ApplyNext)

AckDurable == \A k\in completed: \E i\in 1..Len(log): ClientKey(log[i])=k
NoDuplicateVisible == \A i,j\in 1..Len(events): events[i].key=events[j].key => i=j
OffsetsAreBytes == /\ \A i\in 1..Len(events): events[i].finish>events[i].start
                   /\ \A i\in 2..Len(events): events[i].start=events[i-1].finish
SnapshotBoundary == snapshot.idx<=applied /\ snapshot.idx<=Len(log)
MembershipSafe == owner\in members
PromotionCaughtUp == \A i\in 1..Len(promotions): promotions[i].caught>=promotions[i].commit
AcceptedAuthorityCurrent == \A i\in 1..Len(authority):
  authority[i].issuer=authority[i].acceptedOwner /\
  authority[i].issuedTerm=authority[i].acceptedTerm
TypeOK == applied<=Len(log) /\ life\in{"open","closed","deleted"}
EventuallyApplied == applied<Len(log) ~> applied=Len(log)
=============================================================================
