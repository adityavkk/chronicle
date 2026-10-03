------------------------ MODULE RetentionScenario ------------------------
EXTENDS Retention

VARIABLE phase
scenarioVars == <<vars, phase>>
ScenarioInit == Init /\ phase = 0

(* Gap consumes nothing; three successes establish old frontiers; a payload-changing
   retry must return 1, not latest 6. Snapshot/recovery, epoch fencing, and
   delete/recreate then exercise every retained-state fence. *)
ScenarioNext ==
  \/ /\ phase=0 /\ Write(Cmd(1,0,1,2)) /\ phase'=1
  \/ /\ phase=1 /\ Write(Cmd(1,0,0,1)) /\ phase'=2
  \/ /\ phase=2 /\ Write(Cmd(1,0,1,2)) /\ phase'=3
  \/ /\ phase=3 /\ Write(Cmd(1,0,2,3)) /\ phase'=4
  \/ /\ phase=4 /\ Write(Cmd(1,0,0,3)) /\ phase'=5
  \/ /\ phase=5 /\ TakeSnapshot /\ phase'=6
  \/ /\ phase=6 /\ Recover /\ phase'=7
  \/ /\ phase=7 /\ Write(Cmd(1,0,0,3)) /\ phase'=8
  \/ /\ phase=8 /\ Write(Cmd(1,1,0,2)) /\ phase'=9
  \/ /\ phase=9 /\ Write(Cmd(1,0,2,3)) /\ phase'=10
  \/ /\ phase=10 /\ Delete /\ phase'=11
  \/ /\ phase=11 /\ Recreate /\ phase'=12
  \/ /\ phase=12 /\ Write(Cmd(1,1,0,2)) /\ phase'=13
  \/ /\ phase=13 /\ Write(Cmd(2,0,0,1)) /\ phase'=14

RecoveredRetry == phase = 8 =>
  /\ replies[Len(replies)].outcome = "duplicate"
  /\ replies[Len(replies)].end = 1

ScenarioSpec == ScenarioInit /\ [][ScenarioNext]_scenarioVars
=============================================================================
