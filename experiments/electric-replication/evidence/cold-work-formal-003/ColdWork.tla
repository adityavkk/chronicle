---------------------------- MODULE ColdWork ----------------------------
EXTENDS Naturals, Integers, FiniteSets, TLC
CONSTANT Fault
\* Scheduling is separate from ColdOwnership's publication/certificate proof.
\* One durable pending upload per stream; unknown PUT is never discarded.
Streams == 1..3
VARIABLES work, pending, inflight, cursor, served, attempts, phase, wake, provider, owed
sv == <<work, pending, inflight, cursor, served, attempts, phase, wake, provider, owed>>
Eligible == {s \in Streams : work[s] > 0}
First(ss) == CHOOSE s \in ss : \A t \in ss : s <= t
After == {s \in Eligible : s > cursor}
Choice == IF Fault = "StarveSparse" THEN First(Eligible)
          ELSE First(IF After # {} THEN After ELSE Eligible)
ScheduleInit == /\ work = [s \in Streams |-> 1]
                /\ pending = {} /\ inflight = 0 /\ cursor = 0 /\ served = {}
                /\ attempts = 0 /\ phase = "Scan" /\ wake = TRUE /\ provider = TRUE
                /\ owed = {}
Select == /\ phase = "Scan" /\ Eligible # {} /\ attempts < 2
          /\ inflight' = Choice /\ cursor' = Choice
          /\ pending' = pending \cup {Choice} /\ attempts' = attempts + 1
          /\ phase' = "IO" /\ owed' = owed \cup {Choice}
          /\ UNCHANGED <<work, served, wake, provider>>
Publish == /\ phase = "IO" /\ provider
           /\ work' = [work EXCEPT ![inflight] = IF inflight = 1 THEN 1 ELSE 0]
           /\ pending' = pending \ {inflight} /\ served' = served \cup {inflight}
           /\ inflight' = 0 /\ phase' = "Scan" /\ owed' = owed \ {inflight}
           /\ UNCHANGED <<cursor, attempts, wake, provider>>
Unknown == /\ phase = "IO" /\ ~provider
           /\ pending' = IF Fault = "ForgetUnknown" THEN pending \ {inflight} ELSE pending
           /\ inflight' = 0 /\ phase' = "Yield"
           /\ UNCHANGED <<work, cursor, served, attempts, wake, provider, owed>>
Yield == /\ phase = "Scan" /\ (attempts = 2 \/ Eligible = {})
         /\ phase' = "Yield"
         /\ UNCHANGED <<work, pending, inflight, cursor, served, attempts, wake, provider, owed>>
\* A bounded pass yields, but does not need a timer tick to start its next pass.
\* Periodic idle/retry scans also cover lost notifications and leadership changes.
Resume == /\ phase = "Yield" /\ phase' = "Scan" /\ attempts' = 0
          /\ wake' = FALSE
          /\ UNCHANGED <<work, pending, inflight, cursor, served, provider, owed>>
Crash == /\ phase' = "Yield" /\ inflight' = 0 /\ attempts' = 0 /\ wake' = TRUE
         /\ UNCHANGED <<work, pending, cursor, served, provider, owed>>
Outage == /\ provider' = ~provider
          /\ UNCHANGED <<work, pending, inflight, cursor, served, attempts, phase, wake, owed>>
ScheduleNext == Select \/ Publish \/ Unknown \/ Yield \/ Resume \/ Crash \/ Outage
ScheduleSafe == /\ inflight \in 0..3 /\ attempts <= 2
                /\ (inflight # 0 => inflight \in pending)
                /\ pending \subseteq Eligible /\ pending = owed
\* Stream 1 stays hot forever, while sparse streams 2 and 3 must be served.
SparseProgress == <> ({2,3} \subseteq served)

\* Node-local admission: sampled backlog + carried credits. A completed/unknown
\* request cannot free its credits until a sample covers materialization AND
\* retained unapplied journal debt. The sample mutex excludes releasing tickets;
\* view locks exclude apply/snapshot replacement while the sample is captured.
Requests == 1..3
Cost(r) == IF r = 2 THEN 1 ELSE 2
Limit == 4
VARIABLES stage, used, observed, credit, sampling, captured, running
av == <<stage, used, observed, credit, sampling, captured, running>>
Total(f) == f[1] + f[2] + f[3]
AdmissionInit == /\ stage = [r \in Requests |-> "New"] /\ used = 0 /\ observed = 0
                 /\ credit = [r \in Requests |-> 0] /\ sampling = FALSE
                 /\ captured = 0 /\ running = TRUE
Admit(r) == /\ running /\ ~sampling /\ stage[r] = "New"
            /\ observed + Total(credit) + Cost(r) <= Limit
            /\ stage' = [stage EXCEPT ![r] = "Active"]
            /\ credit' = [credit EXCEPT ![r] = Cost(r)]
            /\ UNCHANGED <<used, observed, sampling, captured, running>>
Write(r) == /\ running /\ stage[r] = "Active"
            /\ stage' = [stage EXCEPT ![r] = "Written"] /\ used' = used + Cost(r)
            /\ UNCHANGED <<observed, credit, sampling, captured, running>>
Finish(r) == /\ stage[r] = "Written" /\ (~sampling \/ Fault = "RacingSample")
             /\ stage' = [stage EXCEPT ![r] = "Finished"]
             /\ credit' = IF Fault = "ReleaseOnReply" THEN [credit EXCEPT ![r] = 0] ELSE credit
             /\ UNCHANGED <<used, observed, sampling, captured, running>>
Capture == /\ ~sampling /\ sampling' = TRUE /\ captured' = used
           /\ UNCHANGED <<stage, used, observed, credit, running>>
Sample == /\ sampling /\ sampling' = FALSE /\ observed' = captured /\ running' = TRUE
          /\ credit' = [r \in Requests |-> IF stage[r] = "Finished" THEN 0 ELSE credit[r]]
          /\ UNCHANGED <<stage, used, captured>>
Reclaim == /\ used > 0 /\ ~sampling /\ used' = used - 1
           /\ (Fault # "PartialTailStall" \/ used >= Limit)
           /\ UNCHANGED <<stage, observed, credit, sampling, captured, running>>
Restart == /\ running /\ running' = (Fault = "AdmitBeforeRecovery")
           /\ observed' = 0 /\ credit' = [r \in Requests |-> 0] /\ sampling' = FALSE
           /\ stage' = [r \in Requests |-> IF stage[r] = "Active" THEN "New" ELSE stage[r]]
           /\ UNCHANGED <<used, captured>>
AdmissionNext == (\E r \in Requests : Admit(r) \/ Write(r) \/ Finish(r))
                 \/ Capture \/ Sample \/ Reclaim \/ Restart
AdmissionSpec == AdmissionInit /\ ScheduleInit /\ [][AdmissionNext /\ UNCHANGED sv]_<<av,sv>>
AdmissionSafe == /\ used <= Limit
                 /\ (running => observed + Total(credit) >= used)
AdmissionLive == AdmissionInit /\ ScheduleInit
                 /\ [][((\E r \in Requests : Admit(r) \/ Write(r) \/ Finish(r))
                    \/ Capture \/ Sample \/ Reclaim) /\ UNCHANGED sv]_<<av,sv>>
                 /\ (\A r \in Requests : SF_av(Admit(r)) /\ WF_av(Write(r)) /\ SF_av(Finish(r)))
                 /\ WF_av(Capture) /\ WF_av(Sample) /\ SF_av(Reclaim)
AdmissionProgress == <> ((\A r \in Requests : stage[r] = "Finished") /\ used = 0 /\ Total(credit) = 0)
ScheduleSpec == ScheduleInit /\ AdmissionInit /\ [][ScheduleNext /\ UNCHANGED av]_<<sv,av>>
ScheduleLive == ScheduleInit /\ AdmissionInit
                /\ [][(Select \/ Publish \/ Yield \/ Resume) /\ UNCHANGED av]_<<sv,av>>
                /\ WF_sv(Select) /\ WF_sv(Publish) /\ WF_sv(Yield) /\ WF_sv(Resume)
=========================================================================
