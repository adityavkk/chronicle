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
| [0002](0002-offset-string-width-migration-lb1.md) | Proposed | Offset.String() `%016d` minimum-width inverts lex order at a field ≥ 10^16 (LB-1) — migration decision |
