----------------------------- MODULE Retention -----------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANT BadRetry

(* A bounded, single-stream refinement of model.rs producer-result policy. *)
Cmd(i,e,q,b) == [inc |-> i, epoch |-> e, seq |-> q, bytes |-> b]
Cmds == {Cmd(1,0,0,1), Cmd(1,0,1,2), Cmd(1,0,2,3),
          Cmd(1,0,0,3), Cmd(1,1,0,2), Cmd(1,1,1,1),
          Cmd(2,0,0,1), Cmd(2,0,1,2)}
Key(c) == <<c.epoch, c.seq>>

VARIABLES incarnation, live, frontier, producerActive, producerEpoch, producerSeq, cache,
          successes, replies, snapshot
vars == <<incarnation,live,frontier,producerActive,producerEpoch,producerSeq,cache,
          successes,replies,snapshot>>

Init == /\ incarnation = 1 /\ live = TRUE /\ frontier = 0
        /\ producerActive = FALSE /\ producerEpoch = 0 /\ producerSeq = 0 /\ cache = {}
        /\ successes = <<>> /\ replies = <<>>
        /\ snapshot = [inc |-> 1, live |-> TRUE, frontier |-> 0,
                        active |-> FALSE, epoch |-> 0, seq |-> 0, cache |-> {}]

Cached(c) == CHOOSE r \in cache : r.key = Key(c)
Success(c) == [inc |-> incarnation, key |-> Key(c), end |-> frontier+c.bytes]
Reply(c,o,e) == [cmd |-> c, outcome |-> o, end |-> e]

Write(c) ==
  /\ IF ~live THEN
       /\ replies' = Append(replies, Reply(c,"missing",0))
       /\ UNCHANGED <<frontier,producerActive,producerEpoch,producerSeq,cache,successes>>
     ELSE IF c.inc # incarnation THEN
       /\ replies' = Append(replies, Reply(c,"stale-incarnation",0))
       /\ UNCHANGED <<frontier,producerActive,producerEpoch,producerSeq,cache,successes>>
     ELSE IF producerActive /\ c.epoch < producerEpoch THEN
       /\ replies' = Append(replies, Reply(c,"epoch-fenced",0))
       /\ UNCHANGED <<frontier,producerActive,producerEpoch,producerSeq,cache,successes>>
     ELSE IF producerActive /\ c.epoch = producerEpoch /\ c.seq <= producerSeq THEN
       /\ IF \E r \in cache : r.key = Key(c)
             THEN replies' = Append(replies, Reply(c,"duplicate",
                    IF BadRetry THEN frontier ELSE Cached(c).end))
             ELSE replies' = Append(replies, Reply(c,"gap",0))
       /\ UNCHANGED <<frontier,producerActive,producerEpoch,producerSeq,cache,successes>>
     ELSE IF (~producerActive /\ c.seq # 0) \/
             (producerActive /\ c.epoch > producerEpoch /\ c.seq # 0) THEN
       /\ replies' = Append(replies, Reply(c,"gap",0))
       /\ UNCHANGED <<frontier,producerActive,producerEpoch,producerSeq,cache,successes>>
     ELSE IF producerActive /\ c.epoch = producerEpoch /\ c.seq # producerSeq+1 THEN
       /\ replies' = Append(replies, Reply(c,"gap",0))
       /\ UNCHANGED <<frontier,producerActive,producerEpoch,producerSeq,cache,successes>>
     ELSE
       LET x == Success(c) IN
       /\ frontier' = x.end /\ producerActive'=TRUE
       /\ producerEpoch' = c.epoch /\ producerSeq' = c.seq
       /\ cache' = (IF ~producerActive \/ c.epoch > producerEpoch THEN {} ELSE cache)
                    \cup {[key |-> Key(c), end |-> x.end]}
       /\ successes' = Append(successes,x)
       /\ replies' = Append(replies,Reply(c,"success",x.end))
  /\ UNCHANGED <<incarnation,live,snapshot>>

Delete == /\ live /\ live'=FALSE /\ frontier'=0 /\ cache'={}
          /\ producerActive'=FALSE /\ producerEpoch'=0 /\ producerSeq'=0
          /\ UNCHANGED <<incarnation,successes,replies,snapshot>>
Recreate == /\ ~live /\ incarnation<2 /\ incarnation'=incarnation+1 /\ live'=TRUE
            /\ frontier'=0 /\ cache'={} /\ producerActive'=FALSE
            /\ producerEpoch'=0 /\ producerSeq'=0
            /\ UNCHANGED <<successes,replies,snapshot>>
TakeSnapshot == /\ snapshot'=[inc |-> incarnation,live |-> live,frontier |-> frontier,
                                active |-> producerActive,epoch |-> producerEpoch,
                                seq |-> producerSeq,cache |-> cache]
                /\ UNCHANGED <<incarnation,live,frontier,producerActive,producerEpoch,producerSeq,
                                cache,successes,replies>>
Recover == /\ incarnation'=snapshot.inc /\ live'=snapshot.live
           /\ frontier'=snapshot.frontier /\ producerActive'=snapshot.active
           /\ producerEpoch'=snapshot.epoch
           /\ producerSeq'=snapshot.seq /\ cache'=snapshot.cache
           /\ UNCHANGED <<successes,replies,snapshot>>

Next == (\E c \in Cmds : Write(c)) \/ Delete \/ Recreate \/ TakeSnapshot \/ Recover
Spec == Init /\ [][Next]_vars

CacheSuccessOnly == \A r \in cache : \E i \in 1..Len(successes) :
  successes[i].inc=incarnation /\ successes[i].key=r.key /\ successes[i].end=r.end
OriginalFrontierReplies == \A i \in 1..Len(replies) : replies[i].outcome="duplicate" =>
  \E j \in 1..Len(successes) : successes[j].inc=replies[i].cmd.inc /\
    successes[j].key=Key(replies[i].cmd) /\ successes[j].end=replies[i].end
CacheBound == Cardinality(cache) <= 3
=============================================================================
