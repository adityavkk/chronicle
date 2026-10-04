# Implicit lifecycle admission

Formal checks preceded implementation in a separate commit: TLC explored 16
states, while refreshing an admitted incarnation at apply produced the retained
`AdmittedIncarnationBound` counterexample. Lean's existing unbounded stale-write
fence theorem rebuilt without proof holes. These are not a proof of the HTTP
adapter or Raft/storage implementation.

The HTTP adapter now binds an omitted incarnation to the observed lifetime (or
checked next lifetime for a tombstone). Every submitted command contains a fixed
incarnation. Explicit caller headers are never refreshed; legacy persisted
commands retain their old replay behavior. `make check` passed, including the
existing deterministic stale-lifecycle/legacy guards. A new schema-3 live test
checks implicit deletion/TTL recreation, producer reset and explicit stale
create/append/delete rejection. It is not input to the older schema-2 checker.

Clients retrying across lifetimes must send the original explicit incarnation.
Headerless retries after recreation cannot be distinguished from new operations;
cross-lifetime deduplication is not promised. A concurrent lifecycle change can
reject an already bound request instead of silently retargeting it.

The conformance runner deadline is now 30 seconds, above the already configured
20-second long-poll allowance. Earlier reports retain the two 5-second runner
timeouts. No upstream test/assertion/explicit deadline or server wait was changed.
Live results and the new full-suite report will be recorded after execution.
