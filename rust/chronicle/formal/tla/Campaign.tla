------------------------------ MODULE Campaign ------------------------------
EXTENDS Naturals, Sequences
CONSTANT BadCooldown
VARIABLES clock, view, observed, since, attempts
vars == <<clock, view, observed, since, attempts>>

(* Policy only. Raft's election/membership/log-up-to-date checks remain trusted
   library assumptions, not a majority protocol reimplemented by this model.
   view abstracts (placement generation, current term, current leader). *)
Init == /\ clock=0 /\ view=1 /\ observed=0 /\ since=0 /\ attempts = <<>>
Tick == /\ clock<6 /\ clock'=clock+1
        /\ UNCHANGED <<view, observed, since, attempts>>
Change == /\ view<3 /\ view'=view+1
          /\ UNCHANGED <<clock, observed, since, attempts>>
Observe == /\ observed # view /\ observed'=view /\ since'=clock
           /\ UNCHANGED <<clock, view, attempts>>
Campaign == /\ observed=view /\ clock>=since+2 /\ Len(attempts)<3
            /\ attempts'=Append(attempts,clock)
            /\ since'=IF BadCooldown THEN since ELSE clock
            /\ UNCHANGED <<clock, view, observed>>
(* Cancellation/unknown campaign outcome must not undo the cooldown. A process
   restart forgets the observation and must observe then wait a fresh interval. *)
Restart == /\ observed'=0 /\ since'=clock
           /\ UNCHANGED <<clock, view, attempts>>
Next == Tick \/ Change \/ Observe \/ Campaign \/ Restart
Spec == Init /\ [][Next]_vars
CampaignSpacing == \A i \in 2..Len(attempts): attempts[i]>=attempts[i-1]+2
=============================================================================
