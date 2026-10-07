---------------------------- MODULE Batches ----------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS N, Capacity, Reorder, BatchMetadata, EarlyReply, WrongReply, ReleaseOnTimeout, DropForming
VARIABLES admitted, queue, forming, log, durable, committed, marker, applied, resolved,
          credits, timedout, replyOwner
vars == <<admitted, queue, forming, log, durable, committed, marker, applied, resolved,
          credits, timedout, replyOwner>>
Requests == 1..N
AppendRequests == {1, 2}
Items(s) == {s[i] : i \in 1..Len(s)}
Prefix(s,n) == SubSeq(s,1,n)
RECURSIVE Flat(_)
Flat(s) == IF Len(s) = 0 THEN <<>> ELSE Head(s) \o Flat(Tail(s))
Serial(n) == [i \in 1..n |-> i]

\* Composition boundary: mature consensus supplies ordered, matching entries
\* and durable-quorum commitment. ApplyRecovery models crashes/private replay.
\* Here requests 1,2 are appends; 3,4 create identities and must stay singleton.
Init == /\ admitted = 0 /\ queue = <<>> /\ forming = <<>> /\ log = <<>>
        /\ durable = 0 /\ committed = 0 /\ marker = 0 /\ applied = 0
        /\ resolved = {} /\ credits = 0 /\ timedout = {}
        /\ replyOwner = [r \in Requests |-> 0]
Admit == /\ admitted < N /\ credits < Capacity
         /\ admitted' = admitted + 1 /\ credits' = credits + 1
         /\ queue' = Append(queue,admitted')
         /\ UNCHANGED <<forming,log,durable,committed,marker,applied,resolved,timedout,replyOwner>>
\* The dispatcher may yield with a collected prefix while new callers arrive.
\* That prefix still owns its credits and precedes every newly queued request.
Gather == \E n \in 1..Len(queue):
          LET held == IF DropForming THEN <<>> ELSE forming
              batch == held \o Prefix(queue,n)
          IN /\ (Len(batch) = 1 \/ BatchMetadata \/ Items(batch) \subseteq AppendRequests)
             /\ forming' = batch
             /\ queue' = SubSeq(queue,n+1,Len(queue))
             /\ UNCHANGED <<admitted,log,durable,committed,marker,applied,resolved,credits,timedout,replyOwner>>
Seal == /\ Len(forming) > 0
        /\ LET n == Len(forming)
               chosen == IF Reorder THEN [i \in 1..n |-> forming[n+1-i]] ELSE forming
           IN log' = Append(log,chosen)
        /\ forming' = <<>>
        /\ UNCHANGED <<admitted,queue,durable,committed,marker,applied,resolved,credits,timedout,replyOwner>>
Flush == /\ durable < Len(log) /\ durable' = durable + 1
         /\ UNCHANGED <<admitted,queue,forming,log,committed,marker,applied,resolved,credits,timedout,replyOwner>>
Commit == /\ committed < durable /\ committed' = committed + 1
          /\ UNCHANGED <<admitted,queue,forming,log,durable,marker,applied,resolved,credits,timedout,replyOwner>>
Mark == /\ marker < committed /\ marker' = committed
        /\ UNCHANGED <<admitted,queue,forming,log,durable,committed,applied,resolved,credits,timedout,replyOwner>>
Apply == /\ applied < Len(Flat(Prefix(log,marker))) /\ applied' = applied + 1
         /\ UNCHANGED <<admitted,queue,forming,log,durable,committed,marker,resolved,credits,timedout,replyOwner>>
Resolve(r) == /\ r \in (1..admitted) \ resolved /\ (EarlyReply \/ r <= applied)
              /\ resolved' = resolved \cup {r} /\ credits' = credits - 1
              /\ replyOwner' = [replyOwner EXCEPT ![r] = IF WrongReply THEN (r % N)+1 ELSE r]
              /\ UNCHANGED <<admitted,queue,forming,log,durable,committed,marker,applied,timedout>>
Timeout(r) == /\ r \in (1..admitted) \ (resolved \cup timedout)
              /\ timedout' = timedout \cup {r}
              /\ credits' = IF ReleaseOnTimeout THEN credits - 1 ELSE credits
              /\ UNCHANGED <<admitted,queue,forming,log,durable,committed,marker,applied,resolved,replyOwner>>
ResolveAny == \E r \in Requests: Resolve(r)
Next == Admit \/ Gather \/ Seal \/ Flush \/ Commit \/ Mark \/ Apply \/ ResolveAny \/ (\E r \in Requests: Timeout(r))
SafetySpec == Init /\ [][Next]_vars
Safe == /\ Flat(log) \o forming \o queue = Serial(admitted)
        /\ 0 <= credits /\ credits <= Capacity
        /\ credits = Cardinality((1..admitted) \ resolved)
        /\ resolved \subseteq 1..applied
        /\ \A r \in resolved: replyOwner[r] = r
        /\ applied <= Len(Flat(Prefix(log,marker)))
        /\ marker <= committed /\ committed <= durable /\ durable <= Len(log)
        /\ \A i \in 1..Len(log): Len(log[i]) = 1 \/ Items(log[i]) \subseteq AppendRequests
        /\ (Len(forming) <= 1 \/ Items(forming) \subseteq AppendRequests)
LiveSpec == SafetySpec /\ WF_vars(Admit) /\ WF_vars(Gather) /\ WF_vars(Seal) /\ WF_vars(Flush)
            /\ WF_vars(Commit) /\ WF_vars(Mark) /\ WF_vars(Apply) /\ WF_vars(ResolveAny)
Progress == <> (resolved = Requests)
=======================================================================
