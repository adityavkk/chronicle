# Issue 15 adversarial review

Date: 2026-07-31

A fresh reviewer received the full internal issue, the complete staged diff,
ADR-0008, the benchmark report, managed GKE rig, exact 332-test conformance
contract, and Jepsen/Porcupine requirements. The first pass reported no P0s,
three P1s, and two P2s.

| Finding | Disposition |
| --- | --- |
| P1: the mixed gate could pass with no catch-up completions | Valid and fixed. The gate now requires nonzero completed reads and bytes, matching completed-body and latency counts, and zero append/catch-up error counters. A fresh full cell passed with 1,200 appends, 388 complete bodies, and no errors. |
| P1: retained evidence did not identify a reproducible tested tree or machine-readable local summary | Valid and fixed. The report records the baseline, exact commands and environment; a compact JSON summary is checked in. The final public commit and integration links are recorded in the internal issue because a commit cannot truthfully name its own eventual hash. |
| P1: the report still described required final gates as future work | Valid and fixed after the final gate rerun. The report and issue update contain completed outcomes; unavailable managed evidence is called out as an external blocker, never replaced with local claims. |
| P2: every `CONFIG GET` failure was treated as a warning | Valid and fixed. Only explicit permission or unsupported-command failures defer to operator enforcement. Transient, timeout, missing, and malformed responses fail startup. |
| P2: the managed rig encoded only the 1 MiB page cell | Valid and fixed. `LT_READ_PAGE_BYTES` deterministically re-renders the same topology and size-qualified result labels; the runbook requires two repeats at 256 KiB, 1 MiB, and 4 MiB before changing the default. |
| Wording: all six paged attempts were called interrupted | Valid and fixed. Five attempts inject between-page events; the sixth is the final closed-stream drain. |
| Request to retain general raw load/Jepsen corpora | Not adopted. The issue explicitly excludes general raw output. Exact commands, compact machine-readable summaries, checker counts, and externally blocked prerequisites are retained instead. |

The corrected diff was returned to the same reviewer for a release-blocker
recheck before integration. That pass reported zero P0 and zero P1 findings.
Its two cheap P2 notes were fixed by adding the compact final-gate ledger and by
naming the exact Redis 8 and Valkey 8 images used by the separate local suites.
