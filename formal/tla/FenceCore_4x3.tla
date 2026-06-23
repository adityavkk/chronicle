---- MODULE FenceCore_4x3 ----
EXTENDS FenceCore
ConstInit4x3 ==
    /\ Workers = { "w1", "w2", "w3", "w4" }
    /\ Subs = { "s1", "s2", "s3" }
    /\ MaxGen = 4
====
