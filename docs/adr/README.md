# Architecture Decision Records

This directory records chronicle's significant architecture decisions as
[ADRs](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
(Michael Nygard's format: Context → Decision → Consequences). Each file is
immutable once Accepted; a later decision that changes course is a *new* ADR that
supersedes the old one (update the old one's Status to `Superseded by ADR-NNNN`).

Naming: `NNNN-short-title.md`, zero-padded, monotonically increasing.

| ADR | Status | Decision |
|---|---|---|
| [0001](0001-lua-scripts-for-atomic-grouped-redis-operations.md) | Accepted | Use Lua `EVAL`/`EVALSHA` (not Redis Functions) for atomic grouped Redis operations |
| [0002](0002-formal-verification-and-property-testing-strategy.md) | Accepted | Two-track verification: `rapid` model-based PBT + Lean 4 pure-core proofs + TLA+/trace-validation for the subscription protocol + Porcupine data-plane linearizability |
| [0003](0003-offset-string-width-migration-lb1.md) | Proposed | Offset.String() `%016d` minimum-width inverts lex order at a field ≥ 10^16 (LB-1) — migration decision |
| [0004](0004-bounded-read-pages-and-snapshot-catch-up.md) | Accepted | Add optional bounded read pages, capture one response snapshot, and stream HTTP and SSE catch-up incrementally |
| [0005](0005-per-instance-sse-stream-fanout.md) | Accepted | Share live SSE reads and formatting through one bounded per-stream hub on each Chronicle replica |
