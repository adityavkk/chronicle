# Pinned protocol failure-family ledger

Acceptance suite: unchanged `@durable-streams/server-conformance-tests@0.3.5`.
The latest full run is `conformance-expiry.json`: **227 passed, 99 failed,
6 skipped of 332**. The six skips are upstream-default reserved subscription
tests; no new skips, filtering, weakened assertions or replacement suite count
as a full pass. Focused development runs will be labelled separately.

| Family | Failed in latest full run | Work status |
|---|---:|---|
| TTL/absolute expiry, including HEAD metadata | 1 | Fifteen resolved; implicit recreation still pending |
| HTTP/content-type/empty producer ID | 8 | Pending |
| Offset-now long polling | 2 | Pending |
| Caching/ETag | 3 | Pending |
| SSE | 5 | Pending; preserve parser-boundary evidence and unchanged upstream results |
| Ordinary same-URL recreation | 1 | Pending; reconcile implicit requests with explicit incarnation fencing |
| Concurrent-read byte property | 1 | New failure; 222-byte 503 barrier-timeout replies, not established payload corruption |
| Fork creation/read/write/lifecycle/expiry/live/JSON | 78 | Pending |
| **Total** | **99** | **Not conformant** |

The raw JSON report, not this grouping, is authoritative for individual failures.
`CONFORMANCE.md` retains prior full-run counts and diagnoses. Every subsequent
full run must retain its report, source/image identity and regressions; a passing
focused family does not remove a failure from the full-run column until rerun.
Formal refinement remains reviewed/tested, not mechanized end-to-end proof.
