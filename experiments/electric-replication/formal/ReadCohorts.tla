-------------------------- MODULE ReadCohorts --------------------------
EXTENDS Naturals, TLC
CONSTANTS UseCompletedTicket, ReuseEqual
VARIABLES value, started, completed, busy, roundValue, completedValue,
          phase, observed, required, seen
vars == <<value, started, completed, busy, roundValue, completedValue,
          phase, observed, required, seen>>
Clients == 1..2
Eligible(c) == IF ReuseEqual THEN completed >= observed[c] ELSE completed > observed[c]
Init == /\ value = 0 /\ started = 0 /\ completed = 0 /\ busy = FALSE
        /\ roundValue = 0 /\ completedValue = 0
        /\ phase = [c \in Clients |-> "New"]
        /\ observed = [c \in Clients |-> 0] /\ required = observed /\ seen = observed
Write == /\ value < 2 /\ value' = value + 1
         /\ UNCHANGED <<started, completed, busy, roundValue, completedValue, phase, observed, required, seen>>
Invoke(c) == /\ phase[c] = "New" /\ phase' = [phase EXCEPT ![c] = "Waiting"]
             /\ observed' = [observed EXCEPT ![c] = IF UseCompletedTicket THEN completed ELSE started]
             /\ required' = [required EXCEPT ![c] = value]
             /\ UNCHANGED <<value, started, completed, busy, roundValue, completedValue, seen>>
\* The atomic START generation is visible before asking Raft for a new
\* linearizable barrier. Requests arriving during it must wait for another.
BeginRound == /\ ~busy /\ started < 4
              /\ \E c \in Clients: phase[c] = "Waiting" /\ ~Eligible(c)
              /\ started' = started + 1 /\ busy' = TRUE /\ roundValue' = value
              /\ UNCHANGED <<value, completed, completedValue, phase, observed, required, seen>>
Complete == /\ busy /\ completed' = started /\ completedValue' = roundValue
            /\ busy' = FALSE
            /\ UNCHANGED <<value, started, roundValue, phase, observed, required, seen>>
Cancel == /\ busy /\ busy' = FALSE
          /\ UNCHANGED <<value, started, completed, roundValue, completedValue, phase, observed, required, seen>>
Reply(c) == /\ phase[c] = "Waiting" /\ Eligible(c)
            /\ phase' = [phase EXCEPT ![c] = "Done"] /\ seen' = [seen EXCEPT ![c] = completedValue]
            /\ UNCHANGED <<value, started, completed, busy, roundValue, completedValue, observed, required>>
Next == Write \/ BeginRound \/ Complete \/ Cancel \/ (\E c \in Clients: Invoke(c) \/ Reply(c))
SafetySpec == Init /\ [][Next]_vars
Safe == /\ completed <= started
        /\ \A c \in Clients: phase[c] = "Done" => seen[c] >= required[c]
LiveNext == Write \/ BeginRound \/ Complete \/ (\E c \in Clients: Invoke(c) \/ Reply(c))
LiveSpec == Init /\ [][LiveNext]_vars /\ WF_vars(BeginRound) /\ WF_vars(Complete)
            /\ (\A c \in Clients: WF_vars(Invoke(c)) /\ WF_vars(Reply(c)))
Progress == <> (\A c \in Clients: phase[c] = "Done")
=======================================================================
