# Pinned protocol failure-family ledger

Acceptance suite: unchanged `@durable-streams/server-conformance-tests@0.3.5`.
The latest full run is `conformance-recreation.json`: **239 passed, 87 failed,
6 skipped of 332**. The six skips are upstream-default reserved subscription
tests; no new skips, filtering, weakened assertions or replacement suite count
as a full pass. Focused development runs will be labelled separately.

| Family | Failed in latest full run | Work status |
|---|---:|---|
| TTL/absolute expiry, including HEAD metadata | 0 | Passed this run |
| HTTP/content-type/empty producer ID | 0 | Eight resolved; nonleader supplemental test also passed |
| Offset-now long polling | 0 | Passed with runner timeout above intentional server wait; see RECREATION.md |
| Caching/ETag | 3 | Pending |
| SSE | 5 | Pending; preserve parser-boundary evidence and unchanged upstream results |
| Ordinary same-URL recreation | 0 | Passed; explicit incarnation required for cross-lifetime retry fencing |
| Concurrent-read byte property | 0 | Passed this run; earlier 222-byte 503 barrier-timeout failure remains an open availability issue |
| Fork creation/read/write/lifecycle/expiry/live/JSON | 79 | Pending; one additional failure versus metadata run retained |
| **Total** | **87** | **Not conformant** |

The raw JSON report, not this grouping, is authoritative for individual failures.
`CONFORMANCE.md` retains prior full-run counts and diagnoses. Every subsequent
full run must retain its report, source/image identity and regressions; a passing
focused family does not remove a failure from the full-run column until rerun.
Formal refinement remains reviewed/tested, not mechanized end-to-end proof.
