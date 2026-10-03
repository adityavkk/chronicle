------------------------- MODULE MembershipAdmission -------------------------
EXTENDS Naturals
CONSTANTS BadVoteFence, BadCompletionBarrier
VARIABLES mode, term, ready, pending, applied, superseded, complete
vars == <<mode, term, ready, pending, applied, superseded, complete>>

(* Raft supplies serialized admission, durable commit and safe configuration
   transitions. This models authority around that API, not Raft itself.
   A excludes the retiring replica; B includes it. The captured old vote is 0.
   The original process can lead in terms 0 and 2, but not term 1. *)
Init == /\ mode \in {"delayed", "cancelled"} /\ term = 0
        /\ ready = (mode = "delayed") /\ pending = (mode = "cancelled")
        /\ applied = "A" /\ superseded = FALSE /\ complete = FALSE
AdvanceTerm == /\ term < 2 /\ term' = term + 1
               /\ UNCHANGED <<mode, ready, pending, applied, superseded, complete>>
AdmitOld == /\ ready /\ ~pending /\ term # 1
            /\ (BadVoteFence \/ term = 0)
            /\ ready' = FALSE /\ pending' = TRUE
            /\ UNCHANGED <<mode, term, applied, superseded, complete>>
CommitOld == /\ pending /\ pending' = FALSE /\ applied' = "B"
             /\ UNCHANGED <<mode, term, ready, superseded, complete>>
(* Another leader completes the old target and its successor before the paused
   original caller resumes. There is no outstanding accepted entry to skip. *)
OtherCompletes == /\ mode = "delayed" /\ term = 1 /\ ~pending
                  /\ superseded' = TRUE /\ complete' = TRUE /\ applied' = "A"
                  /\ UNCHANGED <<mode, term, ready, pending>>
Supersede == /\ mode = "cancelled" /\ ~superseded /\ superseded' = TRUE
             /\ UNCHANGED <<mode, term, ready, pending, applied, complete>>
(* A completed target-membership operation cannot bypass an outstanding
   membership entry. A strict read alone can still return applied A. *)
CompleteNew == /\ superseded /\ ~complete
               /\ (~pending \/ (BadCompletionBarrier /\ applied = "A"))
               /\ complete' = TRUE /\ applied' = "A"
               /\ UNCHANGED <<mode, term, ready, pending, superseded>>
Next == AdvanceTerm \/ AdmitOld \/ CommitOld \/ OtherCompletes \/ Supersede \/ CompleteNew
Spec == Init /\ [][Next]_vars
CompletedAuthority == complete => (applied = "A" /\ ~pending)
=============================================================================
