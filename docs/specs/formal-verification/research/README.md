# Prior-art dossier

The research grounding for the formal-verification & property-based-testing
effort. Each note distills one domain from a search-grounded research pass and
preserves the **verified** citations (sources actually retrieved during
research). The synthesis across all six is in [`../DESIGN.md`](../DESIGN.md); the
decisions are in [ADR-0002](../../../adr/0002-formal-verification-and-property-testing-strategy.md).

| # | Note | Domain |
| --- | --- | --- |
| 01 | [`01-tla-and-trace-validation.md`](./01-tla-and-trace-validation.md) | TLA+/TLC/Apalache for distributed logs & replicated state machines; trace validation as the late-binding bridge to running code; the "is it too late?" question |
| 02 | [`02-proof-assistants-lean.md`](./02-proof-assistants-lean.md) | Machine-checked proof for systems & state machines; Lean 4 vs Coq/Isabelle/Dafny; the proof→code gap |
| 03 | [`03-property-and-model-based-testing.md`](./03-property-and-model-based-testing.md) | Property-based & model-based (stateful) testing for storage/log systems; the Go library choice |
| 04 | [`04-linearizability-checking.md`](./04-linearizability-checking.md) | Linearizability & consistency checking of histories (Porcupine, Knossos, Elle) |
| 05 | [`05-event-sourcing-and-fencing.md`](./05-event-sourcing-and-fencing.md) | Event sourcing / CQRS / append-only logs; exactly-once; fencing tokens; leases; CALM |
| 06 | [`06-redis-substrate-and-durability.md`](./06-redis-substrate-and-durability.md) | Redis 8 as the substrate: Lua atomicity, async-replication durability honesty, lease safety |

Each note follows the same shape: abstract → key findings (with citations) →
techniques and their maturity/effort → the concrete recommendation for
Chronicle → pitfalls → open questions.
