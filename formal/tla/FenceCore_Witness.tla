---- MODULE FenceCore_Witness ----
EXTENDS FenceCore
\* NonVacuity witnesses: each NotX is checked as an invariant from Init; a
\* VIOLATION is the proof the scenario is reachable (so SingleHolder/IndInv are
\* not vacuously true on a fence that is never granted or never coalesces).
\* W-LIVE: some worker is ack-acceptable (a live, granted fence exists).
NoLiveHolder == \A w \in Workers, s \in Subs: ~AckAcceptable(w, s)
\* W-WAKE: some sub is in the coalesce-relevant waking phase with a wake set.
NoWakingFence == \A s \in Subs: ~(sub[s].phase = "waking" /\ sub[s].wake # 0)
====
