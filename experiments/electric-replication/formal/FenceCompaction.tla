-------------------------- MODULE FenceCompaction --------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS N, UnsafeRange, ReplaceFence, ForgetFence
Ids == 1..N
Ranges == {lo..hi : lo \in Ids, hi \in Ids}
VARIABLES issued, destination, source, reserved, retired, messages, fenced
vars == <<issued, destination, source, reserved, retired, messages, fenced>>

\* One destination/source pair. IDs are durable monotonically assigned log
\* positions, never recycled; gaps represent unrelated log commands. Source
\* transitions and range certificates stand for quorum-durable atomic apply.
\* reserved/retired are history ghosts, not implementation tombstones.
Init == /\ issued = 0
        /\ destination = [t \in Ids |-> "unused"]
        /\ source = [t \in Ids |-> "none"]
        /\ reserved = {} /\ retired = {} /\ messages = {} /\ fenced = {}
Active == {t \in Ids : destination[t] \in {"reserved", "live", "retiring"}}
Allowed(t) == t \notin fenced

Reserve == /\ issued < N
           /\ issued' = issued + 1
           /\ destination' = [destination EXCEPT ![issued+1] = "reserved"]
           /\ reserved' = reserved \cup {issued+1}
           /\ UNCHANGED <<source, retired, messages, fenced>>
SkipIndex == /\ issued < N /\ issued' = issued + 1
             /\ UNCHANGED <<destination, source, reserved, retired, messages, fenced>>
\* Includes an arbitrarily delayed/repeated old grant after destination GC.
Grant(t) == /\ t \in reserved /\ source[t] = "none" /\ Allowed(t)
            /\ source' = [source EXCEPT ![t] = "granted"]
            /\ UNCHANGED <<issued, destination, reserved, retired, messages, fenced>>
Abort(t) == /\ destination[t] = "reserved" /\ source[t] = "none"
            /\ Allowed(t)
            /\ source' = [source EXCEPT ![t] = "terminal"]
            /\ retired' = retired \cup {t}
            /\ UNCHANGED <<issued, destination, reserved, messages, fenced>>
Accept(t) == /\ destination[t] = "reserved" /\ source[t] = "granted"
             /\ destination' = [destination EXCEPT ![t] = "live"]
             /\ UNCHANGED <<issued, source, reserved, retired, messages, fenced>>
Retire(t) == /\ destination[t] = "live"
             /\ destination' = [destination EXCEPT ![t] = "retiring"]
             /\ UNCHANGED <<issued, source, reserved, retired, messages, fenced>>
Release(t) == /\ destination[t] = "retiring" /\ source[t] = "granted"
              /\ source' = [source EXCEPT ![t] = "terminal"]
              /\ retired' = retired \cup {t}
              /\ UNCHANGED <<issued, destination, reserved, messages, fenced>>
Resolve(t) == /\ destination[t] \in {"reserved", "retiring"}
              /\ source[t] = "terminal"
              /\ destination' = [destination EXCEPT ![t] = "done"]
              /\ UNCHANGED <<issued, source, reserved, retired, messages, fenced>>
Collect(t) == /\ destination[t] = "done"
              /\ destination' = [destination EXCEPT ![t] = "forgotten"]
              /\ UNCHANGED <<issued, source, reserved, retired, messages, fenced>>

\* Certificates cover only an already-applied ID interval with no active
\* destination. They remain valid after later creates, crashes or leader loss.
\* The network can duplicate or reorder any captured certificate indefinitely.
Certify(r) == /\ r # {} /\ r \subseteq 1..issued
              /\ (r \cap Active = {} \/ UnsafeRange)
              /\ messages' = messages \cup {r}
              /\ UNCHANGED <<issued, destination, source, reserved, retired, fenced>>
Compact(r) == /\ r \in messages
              /\ fenced' = IF ReplaceFence THEN r ELSE fenced \cup r
              /\ source' = [t \in Ids |-> IF t \in r THEN "none" ELSE source[t]]
              /\ UNCHANGED <<issued, destination, reserved, retired, messages>>
Crash == /\ fenced' = IF ForgetFence THEN {} ELSE fenced
         /\ UNCHANGED <<issued, destination, source, reserved, retired, messages>>
Next == Reserve \/ SkipIndex \/ Crash
        \/ (\E t \in Ids : Grant(t) \/ Abort(t) \/ Accept(t) \/ Retire(t)
                            \/ Release(t) \/ Resolve(t) \/ Collect(t))
        \/ (\E r \in Ranges : Certify(r) \/ Compact(r))
Spec == Init /\ [][Next]_vars
Safe == /\ fenced \subseteq 1..issued
        /\ fenced \cap Active = {}
        /\ (\A t \in retired : source[t] # "granted")
        /\ (\A t \in Ids : destination[t] = "live" => source[t] = "granted")
        /\ (\A t \in reserved : source[t] = "none" /\ t \in retired => t \in fenced)

\* Stable period: no new external forks after the finite workload, available
\* quorums, eventual deletion of children, fair control/GC workers. This does
\* not promise reclamation through an unavailable peer or a live descendant.
LiveSpec == Spec /\ WF_vars(Reserve)
            /\ (\A t \in Ids : WF_vars(Grant(t)) /\ WF_vars(Accept(t))
                 /\ WF_vars(Retire(t)) /\ WF_vars(Release(t))
                 /\ WF_vars(Resolve(t)) /\ WF_vars(Collect(t)))
            /\ (\A r \in Ranges : WF_vars(Certify(r)) /\ WF_vars(Compact(r)))
Progress == <> (issued = N /\ Active = {}
                /\ (\A t \in reserved : destination[t] = "forgotten")
                /\ (\A t \in Ids : source[t] = "none") /\ fenced = Ids)
=============================================================================
