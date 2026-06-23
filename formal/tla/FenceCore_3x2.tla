---- MODULE FenceCore_3x2 ----
EXTENDS FenceCore
ConstInit3x2 ==
    /\ Workers = { "w1", "w2", "w3" }
    /\ Subs = { "s1", "s2" }
    /\ MaxGen = 4
====
