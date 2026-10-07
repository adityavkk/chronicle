-------------------------- MODULE AsyncBacklog --------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS ReleaseOnAccepted, AllowRecoveredAdmission
VARIABLES offered, live, charged, orphaned, flushed, accepted, crashes
vars == <<offered,live,charged,orphaned,flushed,accepted,crashes>>
Requests == 1..3
Cost == [r \in Requests |-> CASE r = 1 -> 2 [] r = 2 -> 1 [] OTHER -> 3]
RECURSIVE Bytes(_)
Bytes(s) == IF s = {} THEN 0 ELSE LET r == CHOOSE r \in s: TRUE
                                IN Cost[r] + Bytes(s \ {r})
Init == /\ offered = 0 /\ live = {} /\ charged = {} /\ orphaned = {}
        /\ flushed = {} /\ accepted = {} /\ crashes = 0
Admit == /\ offered < 3 /\ Cardinality(charged) < 2
         /\ Bytes(charged) + Cost[offered+1] <= 3
         /\ (AllowRecoveredAdmission \/ orphaned = {})
         /\ offered' = offered + 1
         /\ live' = live \cup {offered'} /\ charged' = charged \cup {offered'}
         /\ UNCHANGED <<orphaned,flushed,accepted,crashes>>
Flush(r) == /\ r \in live \ flushed /\ flushed' = flushed \cup {r}
            /\ UNCHANGED <<offered,live,charged,orphaned,accepted,crashes>>
Accept(r) == /\ r \in (live \cap flushed) \ accepted /\ accepted' = accepted \cup {r}
             /\ charged' = IF ReleaseOnAccepted THEN charged \ {r} ELSE charged
             /\ UNCHANGED <<offered,live,orphaned,flushed,crashes>>
ResolveLive(r) == /\ r \in live /\ live' = live \ {r} /\ charged' = charged \ {r}
                  /\ UNCHANGED <<offered,orphaned,flushed,accepted,crashes>>
\* A successful local fsync, not HTTP lifetime, decides the recovered suffix.
\* Resolution means committed/applied or removed by an authoritative replacement.
Crash == /\ crashes < 2 /\ live # {}
         /\ orphaned' = orphaned \cup (live \cap flushed)
         /\ live' = {} /\ charged' = {} /\ crashes' = crashes + 1
         /\ UNCHANGED <<offered,flushed,accepted>>
ResolveOrphan(r) == /\ r \in orphaned /\ orphaned' = orphaned \ {r}
                    /\ UNCHANGED <<offered,live,charged,flushed,accepted,crashes>>
ResolveAny == (\E r \in Requests: ResolveLive(r) \/ ResolveOrphan(r))
Next == Admit \/ (\E r \in Requests: Flush(r) \/ Accept(r)) \/ ResolveAny \/ Crash
SafetySpec == Init /\ [][Next]_vars
Safe == /\ charged = live /\ live \cap orphaned = {} /\ accepted \subseteq flushed
        /\ Cardinality(live \cup orphaned) <= 2 /\ Bytes(live \cup orphaned) <= 3
LiveSpec == SafetySpec /\ WF_vars(Admit) /\ WF_vars(ResolveAny)
Progress == <> (offered = 3 /\ live = {} /\ orphaned = {})
=======================================================================
