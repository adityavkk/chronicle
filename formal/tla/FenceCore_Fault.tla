---- MODULE FenceCore_Fault ----
(* NEGATIVE CONTROL for the #41 inductive proof. FenceCore with the           *)
(* INV-FENCE-04 fault injected: ExpireLeaseUNSOUND drops only the lease but    *)
(* leaves the sub in a CLAIMABLE waking state with the (gen,wake) fence and    *)
(* the deposed holder's token INTACT (the SubscriptionFence.tla                *)
(* ExpireClearsFence=FALSE variant). A second worker can then COALESCE onto    *)
(* the same (gen,wake) -- two ack-acceptable holders -- so SingleHolder MUST   *)
(* be reachable-violated from Init under NextFault. If it were NOT, the safety *)
(* proof would be vacuous / the model would not constrain the design.          *)
EXTENDS FenceCore

\* @type: ($sub) => Bool;
ExpireLeaseUNSOUND(s) ==
    /\ sub[s].phase \in {"live", "waking"}
    /\ sub[s].leaseSet
    /\ sub' = [ sub EXCEPT
                  ![s] = [ phase |-> "waking",
                           gen |-> sub[s].gen,
                           wake |-> sub[s].wake,
                           leaseSet |-> FALSE,
                           holder |-> sub[s].holder ] ]
    /\ UNCHANGED token

NextFault ==
    \/ \E s \in Subs: Arm(s)
    \/ \E w \in Workers, s \in Subs: Claim(w, s)
    \/ \E w \in Workers, s \in Subs: AckDone(w, s)
    \/ \E w \in Workers, s \in Subs: AckHeartbeat(w, s)
    \/ \E w \in Workers, s \in Subs: Release(w, s)
    \/ \E s \in Subs: ExpireLeaseUNSOUND(s)
    \/ \E w \in Workers, s \in Subs: CrashToken(w, s)

ConstInitFault ==
    /\ Workers = { "w1", "w2" }
    /\ Subs = { "s1" }
    /\ MaxGen = 4
====
