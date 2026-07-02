------------------------------ MODULE TokenRefresh ------------------------------
(* Formal model of the in-band callback-token refresh (issue #77).            *)
(*                                                                            *)
(* A pull-wake worker holds a callback token that expires at `exp` (abstract  *)
(* discrete time). Time advances by Tick; the worker heartbeats via Ack. The  *)
(* pre-#77 server left an expired token dead: the worker got a bare 401 it     *)
(* could neither `release` past (needs a valid token) nor re-`claim` past      *)
(* (still holds the lease) — a PERMANENT lockout. The #77 fix makes an Ack     *)
(* SELF-HEAL: an expired token is replaced with a fresh full-TTL one (the      *)
(* TOKEN_EXPIRED retry path) and a near-expiry token is refreshed in-band, so  *)
(* an expiry is always transient.                                             *)
(*                                                                            *)
(* This module proves the liveness property the fix exists for:               *)
(*                                                                            *)
(*     ExpiredLeadsToUsable == [](Expired ~> Usable)                          *)
(*                                                                            *)
(* under weak fairness of Ack. Two companion runs make the result meaningful  *)
(* (see the Makefile `token-refresh` target and TokenRefresh_*.cfg):          *)
(*   - Witness (NeverExpired MUST be violated): the Expired state is really    *)
(*     reachable, so the leads-to is checked on genuine expired states.        *)
(*   - Negative control (BrokenSpec MUST fail the same leads-to): WITHOUT the  *)
(*     expired self-heal — the pre-#77 behaviour — the worker stays locked     *)
(*     out, so the property is non-trivial and the self-heal is exactly what   *)
(*     buys liveness.                                                          *)
(*                                                                            *)
(* AckExp mirrors webhook/routes.go handleAckLike + writeTokenRejected;        *)
(* Threshold mirrors webhook/crypto.go tokenRefreshThreshold; the pure         *)
(* arithmetic is pinned from the other side by the rapid suite in             *)
(* webhook/token_refresh_property_test.go.                                     *)
EXTENDS Naturals

CONSTANTS TTL,        \* token lifetime minted at claim / on refresh (> Threshold)
          Threshold,  \* a refresh fires once within this much of expiry
          MaxTime     \* time horizon that bounds the model for TLC

ASSUME TTL \in Nat /\ Threshold \in Nat /\ MaxTime \in Nat
ASSUME TTL > Threshold      \* INV: a fresh token is never immediately refresh-due
ASSUME MaxTime > TTL        \* so a stalled worker can actually reach expiry

VARIABLES now, exp
vars == << now, exp >>

Expired == now > exp
Usable  == now <= exp

\* The ack-time expiry decision, mirroring handleAckLike / writeTokenRejected.
AckExp ==
    IF Expired                     THEN now + TTL   \* expired -> mint fresh (self-heal)
    ELSE IF exp - now <= Threshold THEN now + TTL   \* near-expiry -> in-band refresh
    ELSE                                exp          \* comfortably valid -> keep

\* The pre-#77 (broken) decision, used ONLY by the negative control: an expired
\* token is NOT healed, so the worker stays permanently locked out.
BrokenAckExp ==
    IF Expired                     THEN exp          \* expired -> STUCK (bare 401)
    ELSE IF exp - now <= Threshold THEN now + TTL
    ELSE                                exp

Init == now = 0 /\ exp = TTL       \* freshly minted at claim

Tick      == now < MaxTime /\ now' = now + 1 /\ exp' = exp
Ack       == exp' = AckExp       /\ now' = now
BrokenAck == exp' = BrokenAckExp /\ now' = now

Next       == Tick \/ Ack
BrokenNext == Tick \/ BrokenAck

\* Weak fairness of Ack is the worker heartbeating: while an Ack that changes the
\* token stays enabled (it does whenever a heal/refresh is due), one eventually
\* happens.
Spec       == Init /\ [][Next]_vars       /\ WF_vars(Ack)
BrokenSpec == Init /\ [][BrokenNext]_vars /\ WF_vars(BrokenAck)

TypeOK == now \in 0..MaxTime /\ exp \in 0..(MaxTime + TTL)

\* Non-vacuity witness (MUST be violated): the Expired state is reachable.
NeverExpired == ~Expired

\* The property the #77 fix exists for: an expired token always recovers.
ExpiredLeadsToUsable == [](Expired ~> Usable)
================================================================================
