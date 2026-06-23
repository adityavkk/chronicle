# Formal verification & property-based testing

This directory is the design home for hardening Chronicle's correctness claims
to be *ironclad* — using property-based testing and formal methods, each pointed
at the part of the system it can actually close.

**Start with [`DESIGN.md`](./DESIGN.md)** — the keystone. The thesis in one
sentence: Chronicle's correctness surface splits into **sequential pure cores**
(provable in Lean 4, differentially tested with `rapid`) and a **concurrent
subscription protocol** (model-checked in TLA+ and bound to the code by trace
validation, with Porcupine over recorded histories). Those two regimes map onto
the two git worktrees this work lives in.

## Contents

| File | What it holds |
| --- | --- |
| [`DESIGN.md`](./DESIGN.md) | The keystone: the two-regime thesis, the settled tool decisions, the strategy, the provable-vs-chaos-testable line, and the central model-vs-code risk |
| [`research/`](./research/) | The prior-art dossier — six domain notes with verified citations (TLA+/trace-validation, proof assistants, PBT, linearizability, event-sourcing/fencing, Redis substrate) |
| [`INVARIANTS.md`](./INVARIANTS.md) | The invariant → method catalog: every checkable property, where it is enforced, whether it is tested today, and the tool that should pin it |
| [`FINDINGS.md`](./FINDINGS.md) | Concrete findings, including **confirmed latent bugs** (the `%016d` offset-width hazard and its `Stream-Seq` sibling) and the scope gaps the adversarial critic surfaced |
| [`ROADMAP.md`](./ROADMAP.md) | The phased execution plan (P0–P4), the ranked tool stack, and the risk register — the backbone of the GitHub epic |
| [ADR-0002](../../adr/0002-formal-verification-and-property-testing-strategy.md) | The decision record for the tool choices |

## Worktrees

| Worktree | Branch | Track |
| --- | --- | --- |
| `property-based-testing` | `adityavkk/property-based-testing` | `rapid` model-based equivalence, generative differential, metamorphic, the data-plane Porcupine model, fuzz bridge |
| `formal-verification` | `adityavkk/formal-verification` | Lean 4 proofs of the pure cores, the TLA+ subscription spec + trace validation, Apalache, Alloy *(this directory)* |

## Provenance

This design is grounded in a research pass run as a dynamic multi-agent
workflow (six prior-art domains + four code-invariant extractors + synthesis +
an adversarial completeness critic). The per-domain notes under
[`research/`](./research/) preserve the verified citations; the synthesis is
distilled into `DESIGN.md`, `INVARIANTS.md`, `FINDINGS.md`, and `ROADMAP.md`.
