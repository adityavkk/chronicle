# Formal verification & property-based testing for Chronicle — design

*How we make Chronicle's correctness claims ironclad: a property-based-testing
track and a formal-methods track, each pointed at the part of the system it can
actually close. This is the keystone document; the prior-art research, the
invariant catalog, the latent-bug register, and the phased roadmap live in
sibling files linked below.*

> Status: **design / proposal**. Nothing in the two worktrees is built yet. The
> [testing page](https://adityavkk.github.io/chronicle/testing) records what is
> already green; the [verification page](https://adityavkk.github.io/chronicle/formal-verification)
> is the public-facing summary of this plan.

## Companion documents

| Document | What it holds |
| --- | --- |
| [`research/`](./research/) | The prior-art dossier — six domain notes with verified citations |
| [`INVARIANTS.md`](./INVARIANTS.md) | The invariant → method catalog: every checkable property, where it lives, and the tool that pins it |
| [`FINDINGS.md`](./FINDINGS.md) | Concrete findings the research surfaced, including confirmed latent bugs |
| [`ROADMAP.md`](./ROADMAP.md) | The phased execution plan (P0–P4) across the two worktrees |
| [`../../adr/0002-formal-verification-and-property-testing-strategy.md`](../../adr/0002-formal-verification-and-property-testing-strategy.md) | The ADR recording the tool decisions and their rationale |

This work is split across two git worktrees, by design (see the thesis below):

- **`property-based-testing`** — `rapid` model-based equivalence, generative
  differential testing, metamorphic relations, the data-plane Porcupine model,
  and a coverage-guided fuzz bridge.
- **`formal-verification`** — Lean 4 proofs of the pure cores, the TLA+ spec of
  the subscription protocol with trace validation, Apalache inductive proof, and
  the Alloy relational models. (This document lives here.)

## 1. The thesis: two regimes, two tracks

Chronicle's correctness surface splits cleanly in two, and the split is not
cosmetic — it decides which tool can actually *close* each part.

1. **The pure cores are sequential value functions.** `ValidateProducer`
   ([`store/producer.go`](https://github.com/adityavkk/chronicle/blob/main/store/producer.go)),
   `Offset.Compare`/`Add`/`ParseOffset`/`String`
   ([`store/offset.go`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go)),
   `GenerateResponseCursor`
   ([`protocol/cursor.go`](https://github.com/adityavkk/chronicle/blob/main/protocol/cursor.go)),
   and the webhook reducers (`FenceDecision`, `MergeAcks`, `offsetGreater`,
   `ClaimRotatesFence`, `DecideDue`) are small, total, deterministic functions
   over integers and total orders. The right question for them is *"is this
   correct for **every** input?"* — which is a **proof** question (Lean 4) and a
   **per-input differential** question (rapid against the Lua mirror). It is
   **not** a model-checking question; there are no interleavings to explore.

2. **The concurrent protocol is reachability over interleavings.** The
   subscription wake / claim / ack / release loop, the `(generation, wake_id)`
   fence, the owner-epoch slot CAS, lease expiry, the four crash-recovery
   windows, and the due/retry/sweep loops are a distributed state machine. The
   right question is *"does **any** interleaving of concurrent workers, crashes,
   and lease expiries violate single-holder / monotonicity / at-least-once?"* —
   which is a **model-checking** question (TLA+/TLC, Apalache) bound to the code
   by **trace validation**, complemented by **Porcupine** linearizability over
   recorded histories. A per-input proof cannot reach it.

The two worktrees map onto these two regimes exactly. That is why this is two
tracks rather than one.

A third observation runs underneath both: **the `MemoryStore` is already the
designated oracle** for the whole `Store` contract, and the Redis Lua scripts
already re-implement the pure validators so a mutation is atomic per stream.
[`store/redis/differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go)
already runs one 10-row producer table through both. The single
highest-leverage move is to *generalize that one table into millions of
generated, shrinkable operation sequences* — Chronicle is already shaped for it.

## 2. The decisions (settled by the research)

The prior-art research answered the open strategic questions decisively. These
are recorded in full in the [ADR](../../adr/0002-formal-verification-and-property-testing-strategy.md);
in brief:

| Question | Decision |
| --- | --- |
| Go property-testing library? | **`pgregory.net/rapid`** — first-class state machines, automatic integrated shrinking (zero shrink code), native-fuzzer bridge. `testing/quick` (no shrinking) and `gopter` (heavier) are out. Not yet a dependency; adding it is step one. |
| Proof assistant for the pure cores? | **Lean 4**, not Dafny. Decisive reason: Lean compiles to C and is callable from Go via cgo, so the *proven* source doubles as a third differential oracle — no reimplementation to drift. Keep Lean strictly scoped to the small cores (IronFleet's ~5:1–8:1 proof-to-code ratio is the warning against proving the concurrent layer in an assistant). |
| Is it too late for TLA+ on a built system? | **No — Chronicle is the favorable case.** Use **trace validation** (the CCF "smart casual verification" recipe), not retrofitted abstract conformance checking. The Lua reply alphabet (`ARMED/CLAIMED/BUSY/OK/FENCED`) is already the trace alphabet; each per-slot Lua commit is one atomic spec action. This inverts every reason MongoDB's 2018 trace-checking attempt failed. |
| Linearizability tooling? | **Porcupine** (already a v1.2.0 dependency), extended from the lease fence to the **data plane** (append/read/close) which is currently unchecked. **Not Elle** — a single non-transactional append-only stream has no Adya dependency graph; linearizability is the correct frame. Borrow only Elle's `(clientId, opSeq)` recoverability tagging to make the read step exact. |
| Chase strong consistency with RedisRaft? | **No.** Jepsen found RedisRaft immature (split-brain, data loss). The monotone fence already supplies the only correctness property that matters; durability is decoupled from safety (see §4). |

## 3. Strategy: cheapest, highest-coverage first

The tracks are sequenced so the cheap rungs find the bugs a proof would
otherwise have to assume away, and the proofs cover the tail the samples keep
missing. Concretely (full detail in [`ROADMAP.md`](./ROADMAP.md)):

- **P0 — Foundations & cheapest wins.** Add `rapid`; build the `MemoryStore`-vs-Redis
  model-based equivalence harness behind the shared `store.Store` interface;
  surface the confirmed latent bugs immediately (run the *unguarded* offset
  order property to exhibit the `%016d` divergence); stand up the Lean package
  skeleton; confirm the `model_shard.go` SCAFFOLD wiring.
- **P1 — Pure-core proofs & the differentials.** Prove the cores in Lean;
  compile the Lean model to C and wire it as a *third* differential oracle;
  generalize the 10-row producer table into a generator; close the triple-mirror
  fence-predicate differential (Go core vs Lua vs Jepsen checker mirror).
- **P2 — Data-plane linearizability & the TLA+ fence spec.** Add the missing
  Porcupine `streamModel()` for append/read/close; write the implementation-grain
  TLA+ spec of the fence/lease/recovery state machine and model-check safety +
  liveness.
- **P3 — Trace validation & convergence specs.** Bind the running Go/Lua to the
  TLA+ spec via a JSONL trace seam (the CCF recipe); model the membership/HRW
  convergence the cloud rig cannot exhaustively cover; add the Alloy relational
  models.
- **P4 — Inductive proof, fuzz hardening, living documentation.** Escalate the
  fence to an Apalache inductive invariant (all instance sizes, not bounded N);
  wire the `rapid` harness as a coverage-guided fuzz target; animate the spec.

First-pass scope (a product decision, recorded here): the high-leverage core
**plus** the two most user-facing protocol surfaces the adversarial critic
flagged — **SSE/long-poll EOF semantics** and **JSON-mode flattening + fork
sub-offset**. The broader surfaces (full HTTP status/410-Gone matrix, fork-chain
recursion termination, webhook crypto) are catalogued in
[`FINDINGS.md`](./FINDINGS.md) and deferred.

## 4. What is genuinely provable vs only chaos-testable

The sharp line is set by what Redis guarantees, and getting it right is what
keeps the whole effort honest.

- **Provable / model-checkable.** Redis executes a single `EVAL` over one-slot
  keys atomically with no interleaving, so **per-stream linearizability** holds
  unconditionally and the fence CAS inside the same script inherits that
  atomicity. The `(gen, wake)` **single-holder** property, **generation
  monotonicity**, **stale-inertness**, and **cursor forward-only** are pure
  monotone-token algebra independent of timing — provable in Lean and
  model-checkable in TLA+. All pure-core function properties are provable.
- **NOT provable — chaos-test and document only.** Redis replication is
  asynchronous; the Redis docs state explicitly that `WAIT`/`WAITAOF` do **not**
  make Redis strongly consistent and an acknowledged write can be lost on
  failover. Any property that depends on a write surviving a primary failure is
  therefore **durability-honest** — documented as RPO tiers and chaos-tested on
  the failover rig, never proven.

The architecture is correct *precisely because durability is decoupled from
safety*: the monotone fence makes any lost write degrade to **at-least-once**
delivery (deduplicated by the consumer's monotone offset), never to a safety
violation. No new code path may infer ordering or exclusivity from a `WAIT`
count or a lease TTL — that reintroduces the etcd "~18% lost updates" bug class;
safety rests **only** on the monotone fence.

## 5. The central risk: the model-vs-implementation gap

A Lean proof proves the Lean function; a TLA+ spec proves the spec. The Go core,
the Lua re-implementation, Redis's per-slot serialization, and the recovery
sweep can all still diverge from either. The mitigation is **non-negotiable
pairing**, and it is the reason the two tracks are interlocked rather than
independent:

- Every **Lean proof** is wired into the differential harness (the proven model
  compiled to C becomes a differential oracle), so the proof and the running Go
  are pinned to one statement from two sides.
- Every **TLA+ spec** is bound to the code by **trace validation**, so the
  model and the running Go/Lua are checked against the same behaviors.
- A **shared-core blind spot** remains: `MemoryStore` and the Redis backend
  share the same pure validators, so a bug in the shared core agrees in both and
  the equivalence test passes silently. This is covered by *independent*
  metamorphic/invariant properties (append→read round-trip, fork prefix-equality,
  read split-invariance) and the hand-derived Porcupine sequential models that
  re-state the spec from scratch.

Trace validation guarantees nothing about what is not logged: linearization
points must be emitted at every Lua commit and every claim/ack/release/arm
outcome, or CI goes green while real divergences hide. Placing those seams
correctly is explicit work, not an afterthought. See [`FINDINGS.md`](./FINDINGS.md)
and [`ROADMAP.md`](./ROADMAP.md) for the full risk register.
