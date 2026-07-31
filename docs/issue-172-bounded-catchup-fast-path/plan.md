# Issue 172 bounded catch-up fast path: implementation plan

Status: Implemented and validated locally on 2026-07-31; external publication
remains intentionally unperformed pending explicit user approval.

Baseline: `origin/main` and branch start at
`b2a5d04bfcc0ce5274915b2a147c2fee8a6c6035` with a clean worktree. The target
one-frame root page begins at 2 `EVALSHA`, 2 metadata `HGETALL`, and 1 bounded
`ZRANGEBYLEX`; persistent GET AOF and primary replication deltas are both zero.

## Phase 1: atomic root-page fusion

Outcome: a root-owned nonempty page returns its first bounded frame page from
the same script invocation that validates the root snapshot.

- Add the typed three-way root-range oracle and table tests in `store`.
- Mirror exact offset comparison and classification in Lua.
- Extend the live differential table to compare Go and Lua decisions.
- Pass an explicit fixed snapshot tail for continuations; use the atomically
  loaded tail only for first-page capture.
- Reuse root-script messages and stats when the range is root-owned. Keep empty,
  `now`, beyond-tail, and inherited traversal behavior unchanged.
- Add command assertions for first and continuation pages, root-owned fork
  pages, inherited fork pages, sliding TTL, legacy migration, missing data, and
  absence of producer-hash reads.

Verification: pure tests, Redis differential tests, focused Redis integration
tests, command traces, race tests, and snapshot/fork/expiry regressions.

## Phase 2: register-first live reads

Outcome: a new SSE hub and `offset=now` long-poll confirm wake registration
before the authoritative first page and reuse that registration.

- Expose `MemoryStore`'s existing wake registration through the established
  `NotificationSubscriber` interface.
- Reserve an existing SSE hub before the client page. For a new hub, subscribe
  first and transfer the confirmed subscription into the hub.
- Seed hub identity and current tail from the same page; skip the duplicate
  initial no-touch refresh only when the subscription preceded that page.
- Add the delete/recreate retry that subscribes before a fresh no-touch page.
- For `offset=now` long-poll only, subscribe before the first page and wait on
  that registration without an immediate attach recheck. Keep numeric
  long-poll on `PageWaiter`.
- Release every nonfinal snapshot and close every untransferred subscription.

Verification: deterministic append-boundary tests, dropped-wake polling,
existing-hub sharing/replay, close, cancellation, lag, immutable-segment lease,
delete/recreate, and Redis command-count tests.

## Phase 3: evidence and documentation

Outcome: checked-in explanations and results state what improved and what did
not, with reproducible commands and substrate limits.

- Add a focused root-page benchmark and record before/after timings plus exact
  scripts per operation.
- Record command, AOF-size, primary-replication-offset, and producer-read
  evidence for persistent, absolute-expiry, and sliding-TTL cases.
- Update `docs/PLAN.md`, endpoint flow diagrams/pages, bounded catch-up and SSE
  explainers, immutable-segment and SSE integration notes, and the read fast-path
  results.
- Render or regenerate checked-in diagram outputs using the repository's
  documented docs workflow, then inspect the generated diff.

Verification: docs build/checks where available, link/source inspection, and an
honest results table that distinguishes deterministic counters from noisy wall
time.

## Phase 4: repository release gates

Outcome: every local mandatory gate is green, or the goal remains incomplete
with the exact blocked command and prerequisite.

Run, in order:

1. backend differential parity and focused benchmarks;
2. `make test-unit`;
3. `go test ./...` without a global `REDIS_URL`;
4. `make test` against repository Redis with race detection;
5. `make lint`;
6. `make spec-check`;
7. `make conformance`, requiring exactly 332 of 332;
8. the documented Jepsen default durability/fault suite, including
   `paged-catchup`, `read-expiry`, and `sse-resume`, followed by the documented
   opt-in safety, hardening, and liveness scenarios required by the issue.

Finally inspect the entire diff for unrelated complexity, verify no reserved
internal paths or attribution were added, and commit locally with both author
and committer set to `Aditya Kumarakrishnan
<adityavkk@users.noreply.github.com>`. Do not push, open a PR, deploy, or publish
Pages without separate user instruction.

All local release gates, including the documented default and opt-in Jepsen
sets, completed successfully after integration with `origin/main` at `49e5866`.
The namespaced Jepsen cluster was torn down. GitHub Pages deployment is excluded
from local completion because the user explicitly withheld external-state
authorization.
