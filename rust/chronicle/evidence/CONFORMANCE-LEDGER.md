# Pinned protocol failure-family ledger

Acceptance suite: unchanged `@durable-streams/server-conformance-tests@0.3.5`.
The latest full run is `conformance-resources.json`: **326 passed, 0 failed,
6 skipped of 332**. The six skips are upstream-default reserved subscription
tests; no new skips, filtering, weakened assertions or replacement suite count
as a full pass. Focused development runs will be labelled separately.

This run uses the explicit fixed-tenant mount (`STREAM_TENANT=conformance-mounted`)
on source `5781042`, image `chronicle-raft:resources2` (exact identities in
`resource-84171/`), after resource-informed placement qualification.
It uses the origin as suite base URL. Earlier runs used a nested tenant URL; those
runs did not establish correct fork-reference namespace resolution. See MOUNT.md.

| Family | Failed in latest full run | Work status |
|---|---:|---|
| TTL/absolute expiry, including HEAD metadata | 0 | Passed this run |
| HTTP/content-type/empty producer ID | 0 | Eight resolved; nonleader supplemental test also passed |
| Offset-now long polling | 0 | Passed with runner timeout above intentional server wait; see RECREATION.md |
| Caching/ETag | 0 | Passed; explicit revalidation only, Cache-Control remains no-store |
| SSE | 0 | Passed; bounded batching is not a transport-atomicity guarantee |
| Ordinary same-URL recreation | 0 | Passed; explicit incarnation required for cross-lifetime retry fencing |
| Concurrent-read byte property | 0 | Passed this run; earlier 222-byte 503 barrier-timeout failure remains an open availability issue |
| Fork creation/read/write/lifecycle/expiry/live/JSON | 0 | Passed on the real k3d deployment; see FORKS.md for source/image and proof scope |
| **Total** | **0** | **Pinned enabled suite passed; six upstream-default subscription skips** |

The raw JSON report, not this grouping, is authoritative for individual failures.
`CONFORMANCE.md` retains prior full-run counts and diagnoses. Every subsequent
full run must retain its report, source/image identity and regressions; a passing
focused family does not remove a failure from the full-run column until rerun.
Formal refinement remains reviewed/tested, not mechanized end-to-end proof.
