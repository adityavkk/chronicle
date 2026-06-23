--------------------------- MODULE MC_LiveRace -----------------------------
EXTENDS Liveness
TwoTokensReachable == \E a, b \in Workers: a # b /\ token[a].held /\ token[b].held
TwoTokensNotReachable == ~TwoTokensReachable
=============================================================================
