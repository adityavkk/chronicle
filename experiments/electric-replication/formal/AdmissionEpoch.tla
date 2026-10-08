------------------------- MODULE AdmissionEpoch -------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANT CacheAcrossTerm, UnguardedEnqueue, IgnoreLeaseAtAssignment, DropAcceptedOnExpiry
VARIABLES term, prepared, offered, queued, api, ticket, retained, charged,
          foreign, imports, unsafeStage, lease, expiredStage
vars == <<term,prepared,offered,queued,api,ticket,retained,charged,foreign,imports,unsafeStage,lease,expiredStage>>
Init == /\ term = 0 /\ prepared = 0 /\ offered = 0
        /\ queued = {} /\ api = {} /\ ticket = [r \in 1..3 |-> 0]
        /\ retained = {} /\ charged = {} /\ foreign = {} /\ imports = 0
        /\ unsafeStage = FALSE /\ lease = TRUE /\ expiredStage = FALSE
\* Locally charged requests may coexist with inherited recovery debt. Do not
\* claim a follower's received log or physical WAL is bounded by local admission.
Admit == /\ offered < 3 /\ Cardinality(charged) < 2
         /\ offered' = offered+1 /\ queued' = queued \cup {offered'}
         /\ charged' = charged \cup {offered'}
         /\ UNCHANGED <<term,prepared,api,ticket,retained,foreign,imports,unsafeStage,lease,expiredStage>>
Enqueue(r) == /\ prepared = term /\ r \in queued
              /\ queued' = queued \ {r} /\ api' = api \cup {r}
              /\ ticket' = [ticket EXCEPT ![r] = term]
              /\ UNCHANGED <<term,prepared,offered,retained,charged,foreign,imports,unsafeStage,lease,expiredStage>>
Stage(r) == /\ r \in api /\ (UnguardedEnqueue \/ ticket[r] = term)
            /\ (IgnoreLeaseAtAssignment \/ lease)
            /\ api' = api \ {r} /\ retained' = retained \cup {r}
            /\ unsafeStage' = (unsafeStage \/ prepared # term)
            /\ expiredStage' = (expiredStage \/ ~lease)
            /\ UNCHANGED <<term,prepared,offered,queued,ticket,charged,foreign,imports,lease>>
Reject(r) == /\ r \in api /\ (ticket[r] # term \/ ~lease)
             /\ api' = api \ {r} /\ charged' = charged \ {r}
             /\ UNCHANGED <<term,prepared,offered,queued,ticket,retained,foreign,imports,unsafeStage,lease,expiredStage>>
Elect == /\ term < 2 /\ term' = term+1
         /\ prepared' = IF CacheAcrossTerm THEN term' ELSE prepared
         /\ foreign' = {4,5} /\ imports' = imports+1
         /\ lease' = FALSE
         /\ UNCHANGED <<offered,queued,api,ticket,retained,charged,unsafeStage,expiredStage>>
Prepare == /\ prepared # term /\ retained = {} /\ foreign = {} /\ prepared' = term
           /\ UNCHANGED <<term,offered,queued,api,ticket,retained,charged,foreign,imports,unsafeStage,lease,expiredStage>>
Resolve == /\ retained \cup foreign # {}
           /\ \E r \in retained \cup foreign:
                  /\ retained' = retained \ {r} /\ charged' = charged \ {r}
                  /\ foreign' = foreign \ {r}
           /\ UNCHANGED <<term,prepared,offered,queued,api,ticket,imports,unsafeStage,lease,expiredStage>>
\* The core samples the quorum lease AT ASSIGNMENT. Expiration cannot revoke
\* prior durable acceptance or free its charged debt. The lease clock/quorum
\* calculation is an upstream assumption, not proved by this Boolean model.
Expire == /\ lease /\ lease' = FALSE
          /\ charged' = IF DropAcceptedOnExpiry THEN charged \ retained ELSE charged
          /\ UNCHANGED <<term,prepared,offered,queued,api,ticket,retained,foreign,imports,unsafeStage,expiredStage>>
Renew == /\ ~lease /\ lease' = TRUE
         /\ UNCHANGED <<term,prepared,offered,queued,api,ticket,retained,charged,foreign,imports,unsafeStage,expiredStage>>
EnqueueAny == \E r \in 1..3: Enqueue(r)
ConsumeAny == \E r \in 1..3: Stage(r) \/ Reject(r)
Next == Admit \/ Elect \/ Prepare \/ Resolve \/ EnqueueAny \/ ConsumeAny \/ Expire \/ Renew
SafetySpec == Init /\ [][Next]_vars
Safe == /\ charged = queued \cup api \cup retained /\ Cardinality(charged) <= 2
        /\ ~unsafeStage /\ ~expiredStage /\ (prepared = term => foreign = {})
\* Stable-period progress assumes lease renewal and no further disconnection.
LiveNext == Admit \/ Elect \/ Prepare \/ Resolve \/ EnqueueAny \/ ConsumeAny \/ Renew
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(Prepare) /\ WF_vars(Admit) /\ WF_vars(Resolve)
            /\ WF_vars(EnqueueAny) /\ WF_vars(ConsumeAny) /\ WF_vars(Renew)
Progress == <> (offered = 3 /\ charged = {} /\ foreign = {})
=========================================================================
