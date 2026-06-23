# ADR-0002: Formal verification & property-based testing strategy

- **Status:** Accepted
- **Date:** 2026-06-22
- **Deciders:** @adityavkk
- **Tracking epic:** _(formal-verification epic — link added on epic creation)_
- **Research:** [`docs/specs/formal-verification/`](../specs/formal-verification/README.md) (design, prior-art dossier, invariant catalog, findings, roadmap)

## Context

Chronicle is already tested unusually well: a pure-core/imperative-shell split,
a Go-vs-Lua differential producer table, a Porcupine model of the subscription
lease fence, and a `k3d` Jepsen harness. The question this ADR settles is *how to
go further* — to harden the load-bearing invariants to be provable or
exhaustively checkable, not merely sampled — and *with which tools*.

A research pass (a dynamic multi-agent workflow: six prior-art domains + four
code-invariant extractors + synthesis + an adversarial completeness critic;
captured under the research folder) examined the option space. The decision
drivers:

- **Close the right question with the right tool.** The pure cores are
  sequential value functions (a *proof / per-input* question); the subscription
  engine is a concurrent state machine (a *reachability over interleavings*
  question). One tool cannot do both.
- **Bind any model back to the running code.** A proof of a model or a
  model-checked spec is worthless if the Go/Lua implementation diverges from it.
- **Lowest friction first.** Prefer moves that reuse what Chronicle already has
  (the `MemoryStore` oracle, the differential harness, the Porcupine dependency).
- **Honesty about the substrate.** Distinguish what Redis lets us *prove* from
  what is only *chaos-testable* under asynchronous replication.

## Decision

Pursue **two interlocking tracks in two worktrees** — a property-based-testing
track (`property-based-testing`) and a formal-methods track
(`formal-verification`) — with the following tool choices.

1. **Property-based testing library: `pgregory.net/rapid`.** First-class
   state-machine testing, fully automatic integrated shrinking (no user shrink
   code), generics generators, no non-stdlib dependencies, and a bridge to Go's
   coverage-guided fuzzer. `testing/quick` (frozen, no shrinking) and `gopter`
   (heavier API, manual shrinking) are rejected. The flagship use is a
   model-based equivalence harness: the `MemoryStore` as oracle vs the live
   Redis/Lua backend, over generated operation sequences — a generalization of
   the existing 10-row differential table, not new architecture.

2. **Proof assistant for the pure cores: Lean 4 (not Dafny, not Coq/Isabelle).**
   The cores are total functions over integers and a lexicographic total order —
   `mathlib` `LinearOrder` + `decide`/`omega` + induction handle them directly.
   The decisive reason over Dafny: **Lean compiles to C and is callable from Go
   via cgo**, so the proven source doubles as a third differential oracle,
   eliminating the reimplement-the-spec-for-testing gap. Lean is kept **strictly
   scoped to the small cores**; the concurrent layer is not proved in an
   assistant (the IronFleet ~5:1–8:1 proof-to-code ratio is the warning).

3. **The concurrent protocol: TLA+/TLC, bound to the code by trace validation
   (not retrofitted abstract conformance checking); Apalache for an inductive
   invariant later.** It is **not too late** to get value from TLA+ on a built
   system — Chronicle is the favorable case (a small, explicitly-enumerated fence
   state machine; the Lua reply alphabet is the trace alphabet; each per-slot Lua
   commit is one atomic spec action), the inverse of the documented MongoDB-2018
   failure. Trace validation follows the CCF "smart casual verification" recipe.

4. **Linearizability: Porcupine (already a dependency), extended to the data
   plane (not Elle).** Add the missing `streamModel()` for append/read/close —
   per-stream single-slot Lua atomicity is exactly the claim a linearizability
   checker verifies. Elle is rejected: a single non-transactional append-only
   stream has no Adya dependency graph. Borrow only Elle's `(clientId, opSeq)`
   recoverability tagging to make the Porcupine read step exact.

5. **Do not chase strong consistency with RedisRaft.** Jepsen found it immature
   (split-brain, data loss). Safety rests **only** on the monotone fence;
   durability across failover is decoupled and documented as RPO tiers, not
   proven (see the provable-vs-chaos-testable line in `DESIGN.md` §4).

**First-pass scope** (product decision): the high-leverage core **plus** the two
most user-facing protocol surfaces — **SSE/long-poll EOF semantics** and
**JSON-mode flattening + fork sub-offset**. The full HTTP status/410-Gone matrix,
fork-chain recursion termination, and webhook crypto are catalogued in
`FINDINGS.md` and deferred.

**Latent-bug handling** (product decision): the confirmed `%016d` offset-width
hazard and its `Stream-Seq` sibling (see `FINDINGS.md`) are to be **surfaced and
proved** via a failing `rapid` property and **documented**, but the persisted /
wire offset format is **not** changed in this effort — that fix needs its own
migration design.

## Consequences

- **Positive.** Each correctness question is closed by a tool that can actually
  answer it. The proven Lean cores become differential oracles, so proof and code
  are pinned together. The existing Porcupine and differential infrastructure is
  extended rather than replaced. The TLA+ spec doubles as living documentation of
  the four crash windows.
- **Costs / risks.** The Lean + `mathlib` toolchain is heavy and version-sensitive
  (pin `lake`/`mathlib`; vendor the compiled C oracle so routine Go CI needs no
  Lean toolchain). TLC checks bounded instances, not all N — a passing run is not
  a proof until escalated to an Apalache inductive invariant. Trace validation
  guarantees nothing about unlogged transitions — the log seams are load-bearing
  work. The differential harness needs a clock seam threaded into `MemoryStore`
  (which calls `time.Now()` directly today) for TTL/expiry determinism. These are
  tracked in `ROADMAP.md`'s risk register.
- **Neutral.** This is additive to the existing test suites; nothing is removed.

## Alternatives considered

- **One PBT library covering everything (no proofs).** Rejected: sampling cannot
  establish all-inputs correctness for the cores, and `rapid` alone cannot
  exhaustively explore concurrent interleavings the way TLC does.
- **Dafny / IronFleet-style verified implementation.** Rejected for the
  concurrent layer (proof-to-code ratio); for the cores, Lean's cgo path is the
  decisive differentiator.
- **Elle for the data plane.** Rejected (no transactional graph); flagged only as
  possible future work for the cross-slot fork-cascade read-write dependency.
- **RedisRaft for strong consistency.** Rejected (Jepsen immaturity; unnecessary
  given the fence).
