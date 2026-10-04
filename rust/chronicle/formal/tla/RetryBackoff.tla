--------------------------- MODULE RetryBackoff ---------------------------
EXTENDS Naturals
CONSTANT ImmediateRetry
VARIABLE attempts
vars == <<attempts>>
Init == attempts = 0
(* A continuously unreachable peer completes each attempt with an error.
   Elapse represents completion of the library's 500ms backoff. Other work
   can continue; no speculative application or acknowledgement is introduced. *)
Fail == /\ attempts < 2 /\ (ImmediateRetry \/ attempts = 0)
        /\ attempts' = attempts + 1
Elapse == attempts' = 0
Next == Fail \/ Elapse
Spec == Init /\ [][Next]_vars
RetryBounded == attempts <= 1
===========================================================================
