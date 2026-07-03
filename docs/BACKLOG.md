# Prioritized backlog

This is the working order for Chronicle's open work, produced by the 2026-07-03
issue triage: every open issue was vetted against `main`, deduplicated, and
either closed with evidence or rescoped to its actual remaining work — 34 open
issues became the 10 below. The GitHub issues stay the source of truth for scope
and acceptance detail; this document holds only the ordering and the reasons for
it. When an issue closes, delete its row. When new work is filed, slot it into
the order rather than appending it.

> **Provenance.** Triaged 2026-07-03 against `main` @ cf750eb — one vet plus one
> adversarial verification per issue; the per-issue evidence lives in each closed
> issue's closing comment and each kept issue's rescoped body.
> **Drift guard:** `make backlog-check` (CI: the `backlog-drift` job on every
> PR/push, on every issue open/close/reopen event, and nightly as a backstop)
> fails the moment the ranked table below stops being exactly the set of open
> issues — a PR that closes an issue must delete its row, and newly filed work
> must be slotted into the order. Repairs arrive as bot PRs a human merges:
> row deletions from the deterministic backlog-autofix workflow, slot
> proposals for new issues from the backlog-slot agent workflow.

The ordering rule: client-reachable defects first, then cheap CI protection of
verification work that already shipped, then production-cutover insurance, then
the paid cloud validation batched into a single rig session, then low-urgency
decisions. Effort is for the *remaining* work only (S < 0.5d, M 0.5–2d, L 2–5d).

## The order

| # | Issue | Work | Effort | Why here | Done when |
|---|-------|------|--------|----------|-----------|
| 1 | [#47](https://github.com/adityavkk/chronicle/issues/47) | Thread the original seq strings through `validate_producer` (`store/redis/scripts/common.lua:91,103`) so a `Producer-Seq >= 10^14` gap returns a clean 409 instead of a Redis reply-parse error | S | The only client-reachable defect open; also unblocks widening the proven Go/Lua/Lean agreement domain from 10^14 to the documented 2^53 bound | `go test ./store/redis/ -run 'TestDifferentialProducerReplyTostringLB\|TestDifferentialProducerProperty' -count=1` passes against live Redis with the 10^14 subtest flipped to assert `ErrProducerSeqGap` and `luaReplyExactBound` raised to 1<<53 |
| 2 | [#48](https://github.com/adityavkk/chronicle/issues/48) | Change `readOwnMessages` (`store/memory_store.go:708`) to compare the full offset, and flip the armed LB-3 tests in the same change | S | One line, behavior-preserving while `ReadSeq == 0`, and it removes a known landmine before log rotation ever lands | `TestReadSeqAgreesWhileZero\|TestReadSeqDivergesWhenNonZero\|TestReadSeqDivergenceCounterexampleLB3` green with the divergence halves rewritten to assert full-offset agreement |
| 3 | [#41](https://github.com/adityavkk/chronicle/issues/41) | Wire the landed Apalache fence proof into CI (`formal/tla/apalache/run.sh`) + Spectacle/ADR doc wiring | S | The inductive proof exists but nothing guards it; any spec or IndInv edit can silently break it | A CI job runs `bash formal/tla/apalache/run.sh` green on main ("ALL APALACHE OBLIGATIONS PASS") and fails the build on any counterexample |
| 4 | [#40](https://github.com/adityavkk/chronicle/issues/40) | Wire the Alloy checks into CI + add the INV-FORK-01 fork-lineage crash-envelope model | S–M | Same rot protection, plus it formalizes the one genuinely non-atomic cross-slot Redis path (fork create + delete-cascade), today prose-only | A CI step runs `make -C formal/tla alloy` green with every check UNSAT, including the new `formal/alloy/ForkLineage.als`, and fails on any SAT |
| 5 | [#39](https://github.com/adityavkk/chronicle/issues/39) | Trace-validation CI job (needs a Redis service container) + the `webhook/trace_test.go` per-Lua-commit under-instrumentation guard + the owner-layer scope decision | M | The bridge's whole purpose is regression detection; without CI and the guard it validates nothing on future pushes | The CI job runs `make -C formal/tla trace-validate trace-negative` green, and `go test -tags subtrace ./webhook/ -run TestTrace -count=1` passes the coverage guard |
| 6 | [#22](https://github.com/adityavkk/chronicle/issues/22) | Slot-homing migration hardening: zero-legacy-probe command-count regression test, `migrateSub` SetNX lock + `_complete` marker with a concurrent-replay test, offline migrator + orphaned-key audit | M–L | Gates the slot-homing production cutover; the replay race can resurrect stale legacy state during the transition window | `go test ./webhook -run 'TestMigratedHotPathNoLegacyProbes\|TestMigrateSubConcurrentReplay' -count=1` passes, and a documented migrator run on a seeded legacy keyspace ends with zero orphaned legacy keys |
| 7 | [#30](https://github.com/adityavkk/chronicle/issues/30) | Lean ParseOffset transcription + round-trip/rejection proofs (INV-OFF-03/04) + the matching Go rapid property | M | Closes the last unproven pure-core seam; offsets arrive in request URLs, so parser totality is security-adjacent | The `lean-proofs` CI job is green with both theorems passing the sorryAx/ofReduceBool gate, and `go test ./store/ -run 'TestParseOffset(NeverPanics\|RoundTrip)' -count=1` passes |
| 8 | [#11](https://github.com/adityavkk/chronicle/issues/11) | Open the Electric-repo client tracking issue from the doc 08 §9 draft + cross-reference docs 05/07 | S | Pure cross-repo unblock: the Electric client will not adopt per-shard claims until the issue exists | Doc 08 §9 links a real Electric-repo issue instead of "(to open)", and docs 05/07 cross-reference doc 08 |
| 9 | [#16](https://github.com/adityavkk/chronicle/issues/16) | The consolidated cloud V&V campaign: rig prerequisites, then gates #1–#5, L2/L4/L5, and the STANDARD_HA failover drill in one rig-up | L | Closes the epic #9/#25 validation ledger — every remaining PENDING-CLOUD row lives here, batched so the rig cost is paid once | The three ledgers (`loadtest/RESULTS-gate2.md` Phase 3, `loadtest/RESULTS-gate5-failover.md` Layer 3, `docs/jepsen/results.md` L2/L4/L5) show recorded PASS/GREEN rows and the rig is torn down |
| 10 | [#46](https://github.com/adityavkk/chronicle/issues/46) | Decide and execute the LB-1 offset-width migration (ADR-0003: widen to `%020d` vs enforce a `< 10^16` runtime guard) | M | Lowest urgency: the divergence needs ~10 PB in a single stream, and the safe domain is already pinned green in CI with the unsafe domain fixtured | `docs/adr/0003-offset-string-width-migration-lb1.md` reads "Status: Accepted" and the matching test gate is green (Option 1: the unguarded Compare-vs-strcmp property passes by default; Option 2: a unit test proves the runtime guard rejects any field >= 10^16) |

## Standing constraints

- The GKE/loadtest rig runs on personal GCP (~$4/hr). Batch every cloud
  measurement into #16's single rig-up and tear down after every session
  (`./ltctl.sh down`, then verify `gcloud` lists no leftover resources).
- File the `redis.googleapis.com` quota request in `adityavkk-prototyping`
  early — it is a #16 prerequisite with lead time and costs nothing while
  pending.
- Rows 1–2 and 5–8 verify locally. Rows 3–5 only prove themselves on the next
  CI run on main. Row 9 costs real money and should be one planned session.

## Triage record (2026-07-03)

Closed as already landed on `main`, each with a closing comment citing the
landing commit and verified artifacts: [#10](https://github.com/adityavkk/chronicle/issues/10),
[#12](https://github.com/adityavkk/chronicle/issues/12), [#13](https://github.com/adityavkk/chronicle/issues/13),
[#14](https://github.com/adityavkk/chronicle/issues/14), [#15](https://github.com/adityavkk/chronicle/issues/15)
(epic #9 slices) and [#26](https://github.com/adityavkk/chronicle/issues/26)–[#29](https://github.com/adityavkk/chronicle/issues/29),
[#31](https://github.com/adityavkk/chronicle/issues/31)–[#38](https://github.com/adityavkk/chronicle/issues/38),
[#42](https://github.com/adityavkk/chronicle/issues/42), [#44](https://github.com/adityavkk/chronicle/issues/44)
(epic #25 slices). Merged: #20 → #22, #21 and #43 → #16. Epics
[#9](https://github.com/adityavkk/chronicle/issues/9) and
[#25](https://github.com/adityavkk/chronicle/issues/25) closed with successor
pointers. The six scattered "run it on the rig" residuals were consolidated
into #16 so the paid rig session happens once.
