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
| [0006](0006-immutable-segment-read-plane-prototype.md) | Accepted for prototype | Copy sealed prefixes into range-authenticated immutable segments while Redis remains authoritative |
| [0007](0007-fuse-root-pages-and-register-live-reads-first.md) | Accepted | Fuse bounded root-owned frames into the atomic root read and register new live readers before their authoritative page |
| [0008](0008-write-fencing-extension.md) | Accepted | Make the claim-bound append fence a §11.1 protocol extension: fenced streams, server-derived write classes, producer binding, epoch ≡ generation, per-authority seal at done |
