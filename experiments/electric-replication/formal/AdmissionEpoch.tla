------------------------- MODULE AdmissionEpoch -------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANT CacheAcrossTerm, UnguardedEnqueue
VARIABLES term, prepared, offered, queued, api, ticket, retained, charged,
          foreign, imports, unsafeStage
vars == <<term,prepared,offered,queued,api,ticket,retained,charged,foreign,imports,unsafeStage>>
Init == /\ term = 0 /\ prepared = 0 /\ offered = 0
        /\ queued = {} /\ api = {} /\ ticket = [r \in 1..3 |-> 0]
        /\ retained = {} /\ charged = {} /\ foreign = {} /\ imports = 0
        /\ unsafeStage = FALSE
\* Locally charged requests may coexist with inherited recovery debt. Do not
\* claim a follower's received log or physical WAL is bounded by local admission.
Admit == /\ offered < 3 /\ Cardinality(charged) < 2
         /\ offered' = offered+1 /\ queued' = queued \cup {offered'}
         /\ charged' = charged \cup {offered'}
         /\ UNCHANGED <<term,prepared,api,ticket,retained,foreign,imports,unsafeStage>>
Enqueue(r) == /\ prepared = term /\ r \in queued
              /\ queued' = queued \ {r} /\ api' = api \cup {r}
              /\ ticket' = [ticket EXCEPT ![r] = term]
              /\ UNCHANGED <<term,prepared,offered,retained,charged,foreign,imports,unsafeStage>>
Stage(r) == /\ r \in api /\ (UnguardedEnqueue \/ ticket[r] = term)
            /\ api' = api \ {r} /\ retained' = retained \cup {r}
            /\ unsafeStage' = (unsafeStage \/ prepared # term)
            /\ UNCHANGED <<term,prepared,offered,queued,ticket,charged,foreign,imports>>
Reject(r) == /\ r \in api /\ ticket[r] # term
             /\ api' = api \ {r} /\ charged' = charged \ {r}
             /\ UNCHANGED <<term,prepared,offered,queued,ticket,retained,foreign,imports,unsafeStage>>
Elect == /\ term < 2 /\ term' = term+1
         /\ prepared' = IF CacheAcrossTerm THEN term' ELSE prepared
         /\ foreign' = {4,5} /\ imports' = imports+1
         /\ UNCHANGED <<offered,queued,api,ticket,retained,charged,unsafeStage>>
Prepare == /\ prepared # term /\ retained = {} /\ foreign = {} /\ prepared' = term
           /\ UNCHANGED <<term,offered,queued,api,ticket,retained,charged,foreign,imports,unsafeStage>>
Resolve == /\ retained \cup foreign # {}
           /\ \E r \in retained \cup foreign:
                  /\ retained' = retained \ {r} /\ charged' = charged \ {r}
                  /\ foreign' = foreign \ {r}
           /\ UNCHANGED <<term,prepared,offered,queued,api,ticket,imports,unsafeStage>>
EnqueueAny == \E r \in 1..3: Enqueue(r)
ConsumeAny == \E r \in 1..3: Stage(r) \/ Reject(r)
Next == Admit \/ Elect \/ Prepare \/ Resolve \/ EnqueueAny \/ ConsumeAny
SafetySpec == Init /\ [][Next]_vars
Safe == /\ charged = queued \cup api \cup retained /\ Cardinality(charged) <= 2
        /\ ~unsafeStage /\ (prepared = term => foreign = {})
LiveSpec == SafetySpec /\ WF_vars(Prepare) /\ WF_vars(Admit) /\ WF_vars(Resolve)
            /\ WF_vars(EnqueueAny) /\ WF_vars(ConsumeAny)
Progress == <> (offered = 3 /\ charged = {} /\ foreign = {})
=========================================================================
