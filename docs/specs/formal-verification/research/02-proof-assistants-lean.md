# Proof assistants for the pure cores: Lean 4 vs Coq/Isabelle/Dafny

*Machine-checked proof for systems and protocol state machines splits into two camps: end-to-end verified implementations (IronFleet/Dafny, Verdi/Coq, F\*/Low\*) that prove the real code at a 5:1–8:1 proof-to-code tax, and model-only methods (TLA+) whose specs are not executable. For Chronicle the right move is neither — it is to prove the three tiny, total pure cores in Lean 4 and close the proof-to-code gap with the differential property-based testing the project already runs, using the proven Lean artifact as a third oracle. This note surveys the prior art, the techniques and their cost, and a concrete plan.*

## Why not "just verify the Go"

There is no mature verifier for Go. Every end-to-end story in the literature verifies code written in a prover-friendly language (Dafny, Coq, F\*) and then either compiles or extracts it. Chronicle's hot path is Go plus Redis Lua, and a rewrite-to-extract pipeline would be a rewrite, not a hardening. The realistic question is therefore narrow: *what can we prove cheaply, and how do we connect that proof to the unproven Go and Lua that actually ship?*

Chronicle's leverage is that its correctness-critical logic is concentrated in three small, pure, total functions, each verified here against the real source:

| Core | Source | Shape |
|---|---|---|
| Idempotent producer validation | [`store/producer.go`](https://github.com/adityavkk/chronicle/blob/main/store/producer.go) — `ValidateProducer(state, epoch, seq, nowUnix)` | finite case split over six outcomes: fresh-init (Accepted, `LastSeq=0`), stale-epoch fence (`ErrStaleEpoch`), epoch bump/reset (Accepted, `LastSeq=0`), duplicate (idempotent success), next (`seq == LastSeq+1`, Accepted), gap (`ErrProducerSeqGap`) |
| Offset total order | [`store/offset.go`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go) — `Compare(a, b Offset)`, `Offset.Add` | lexicographic order on `(ReadSeq, ByteOffset)`, both `uint64`; `Add` increments `ByteOffset` |
| Cursor progression | [`protocol/cursor.go`](https://github.com/adityavkk/chronicle/blob/main/protocol/cursor.go) — `GenerateResponseCursor(clientCursor, now)` | integer interval arithmetic: return current interval, unless the client cursor is at/ahead of now, in which case advance past it by a fixed jitter step |

These are total per-input function properties — totality, exhaustive case analysis, monotonicity, strict order — not interleaving-reachability problems. That distinction drives every recommendation below.

## Key findings

### IronFleet (Dafny): you *can* verify a real distributed system end-to-end — at a steep, well-measured cost

Hawblitzel et al. (SOSP'15) blend TLA-style state-machine refinement with Hoare-logic verification across three layers — a concise top-level spec (IronRSL's was 85 lines, IronKV's 34), an abstract distributed-protocol layer, and a single-threaded imperative Dafny implementation — proving safety *and* liveness "all the way down to the bytes of the UDP packets." The reported proof-to-code annotation ratio is **7.7:1 overall** (5.4:1 excluding liveness; ~5:1 safety, ~8:1 liveness), roughly **40K lines of proof and ~3.7 person-years**. The trusted base remains non-trivial: the Dafny verifier, Z3, the Dafny-to-C#/.NET compiler, the UDP/network spec taken as an axiom, and the correctness of the top-level spec itself. ([SOSP'15 PDF](https://www.andrew.cmu.edu/user/bparno/papers/ironfleet.pdf); [Ironclad README](https://github.com/microsoft/Ironclad/blob/main/ironfleet/README.md); [the morning paper](https://blog.acolyer.org/2015/10/15/ironfleet-proving-practical-distributed-systems-correc/))

*Takeaway for Chronicle:* the refinement *idea* (prove a state machine, define what a correct trace is) is the right framing; the multi-layer, down-to-bytes, person-years tax is the warning sign. Borrow the framing, not the scope.

### Verdi (Coq): first machine-checked Raft linearizability — but extraction, runtime, and the IO shim are trusted

Wilcox, Woos et al. (PLDI'15) verify systems in Coq using "verified system transformers" that let a developer prove a system under an idealized fault model and transfer the guarantee to a harsher one with no extra proof. Verified handlers are extracted to OCaml and linked against a **trusted runtime shim** that performs the low-level socket IO; the README states event-handler code "must be extracted to OCaml and linked with one of the shims in the Verdi runtime library that handles low-level network communication." Verified artifacts include Verdi Raft and the `vard` key-value store. The formal guarantee covers only the Coq portion — extraction, runtime, and shim sit outside the proof. ([PLDI'15 abstract](https://homes.cs.washington.edu/~mernst/pubs/verify-distsystem-pldi2015-abstract.html); [uwplse/verdi](https://github.com/uwplse/verdi))

*Takeaway for Chronicle:* even the strongest extraction story still trusts a shim doing the real IO. The proof boundary always stops short of the bytes on the wire.

### F\*/Low\*/EverParse: the strongest "proof maps to real low-level code" story, and the closest to Chronicle's parsers

Low\* is a shallow embedding of a sequential C subset in F\*; KaRaMeL translates it to CompCert Clight "with a proof that this translation preserves semantics and side-channel resistance," erasing specs and proofs at extraction, and any Low\* program is memory-safe by typing. EverParse (USENIX Sec'19, PLDI'22) generates "provably secure, zero-copy parsers" from a format specification, deployed in verified QUIC, DICE, and the Azure networking stack, and now also targets Rust. ([POPL'18 / PACMPL](https://dl.acm.org/doi/10.1145/3110261); [KaRaMeL](https://github.com/FStarLang/karamel); [EverParse repo](https://github.com/project-everest/everparse); [MSR blog](https://www.microsoft.com/en-us/research/blog/everparse-hardening-critical-attack-surfaces-with-formally-proven-message-parsers/))

*Takeaway for Chronicle:* this is the closest existing work to proving a byte-format parser correct, which is exactly the shape of `ParseOffset` (one underscore, digits only, 16-digit zero-padded, lexicographically sortable) and the cursor encoding. It is the right reference *if and when* Chronicle wants a proven parser — but it is a parser problem, not the pure-arithmetic problem the rest of the cores present.

### The model-vs-implementation gap is the central pitfall

The trusted computing base (TCB) of any "formally verified" system includes the specs, the verification tools (proof kernel, Z3, compiler), and runtime infrastructure; program extraction is itself generally unverified, so trust ends at that boundary. A separate line of work documents that real implementations diverge from their models through "model-code gaps" — atomicity violations and concurrency the spec abstracted away. ([TCB — Wikipedia](https://en.wikipedia.org/wiki/Trusted_computing_base); [Multi-Grained Specifications for Distributed System Model Checking](https://arxiv.org/pdf/2409.14301))

*Takeaway for Chronicle:* the Redis-Lua-vs-Go divergence risk is real even if a model is proven. The Lua re-implementation, Redis's per-slot serialization, and the recovery sweep are all places where the implementation can drift from a proven model. A proof alone gives false confidence; it must be paired with differential testing.

### The directly transferable pattern: differential PBT against a formally verified Lean oracle

Welltyped Systems' open [`verified-ledger`](https://github.com/welltyped-systems/verified-ledger) is "a reference architecture for differential fuzzing using a formally verified oracle." A ledger (deposit/withdraw/transfer over `UInt64` with wrap-around) is modeled and proven in Lean 4 (`lean/VerifiedLedger/Proofs.lean`); the Rust build invokes `lake` to compile the Lean model to C and link it in; a harness generates random op sequences, runs them through both Rust and the Lean oracle, and asserts post-state and invariants match — catching planted bugs (an off-by-one in `withdraw`, a `transfer` that credits the sender). They argue a verified model is a better oracle than a reference implementation because it is "small, explicit, and backed by proofs" and carries no accidental behavior, and they recommend a pragmatic order: **write the differential harness and start fuzzing first, then add proofs to the model as time allows.** ([Welltyped blog](https://welltyped.systems/blog/verified-conformance-testing-for-dummies); [repo](https://github.com/welltyped-systems/verified-ledger))

*Takeaway for Chronicle:* this is structurally identical to Chronicle's existing [`store/redis/differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go) (Go oracle vs Lua over one table). Lean simply becomes a third, *proven* oracle — replacing "oracle by assertion" with "oracle by proof."

### Lean 4 fits Chronicle's pure cores specifically

mathlib defines `LinearOrder` as a reflexive/transitive/antisymmetric/total `≤` with decidable `≤`, `<`, `=` — which directly models `Compare` (a lexicographic order on `(ReadSeq, ByteOffset)`, itself just the order on pairs of `Nat`/`UInt64`). Lean's `decide` tactic proves decidable propositions by definitional computation; `native_decide` compiles the `Decidable` instance to native code (at the cost of a larger TCB). Producer validation is a small finite case split, so case-exhaustiveness and the monotonicity invariants are provable by structural case analysis plus induction over an operation sequence. Cursor progression is an arithmetic monotonicity lemma in the integer domain, well within `linarith`/`omega`. ([Mathlib LinearOrder](https://leanprover-community.github.io/mathlib4_docs/Mathlib/Order/Defs/LinearOrder.html); [decide & native_decide](https://lean4.dev/tactics/automation/decide); [Lean 4 — de Moura & Ullrich, CADE 2021](https://pp.ipd.kit.edu/uploads/publikationen/demoura21lean4.pdf))

### Lean 4 can be proof artifact *and* test oracle for a Go codebase

Lean 4's reference documents the `@[extern]` attribute binding Lean declarations to C symbols, and the `lake` build configuration for compiling and linking C alongside Lean; Lean's runtime API is C, so a cgo bridge is feasible — the same mechanism Welltyped used to link the Lean model into Rust. Unlike Dafny, "the very same source code is ahead-of-time compiled to a native binary, so [Lean] can both state and prove value-indexed properties and run the verified program itself," eliminating a "reimplement the spec for testing" gap. This means the exact object Chronicle proves correct is the one it fuzzes against. ([Lean 4 FFI reference](https://lean-lang.org/doc/reference/latest/Run-Time-Code/Foreign-Function-Interface/); [Verifying Distributed Protocols in Veil](https://proofsandintuitions.net/2026/02/09/distributed-verification-veil/)) *(confidence: medium — the Go/cgo variant specifically has not been validated at fuzz-loop call volumes; see open questions)*

### Lean is now credible for state machines/protocols, not just pure math

Veil is a Lean 4 library for distributed protocols that combines model checking (via Lace), SMT (cvc5) deductive verification, and (LLM-assisted) invariant inference in Ivy's whole-system-state style. It adopts Ivy's global-state transition modeling and proceeds by inductive invariant proof (init establishes, every action preserves), generating theorems checked via `#check_invariants`. It is positioned against TLA+/Quint (great at finding bugs, weak proof support — TLAPS is a separate, less expressive tool) and Ivy. ([Veil](https://proofsandintuitions.net/2026/02/09/distributed-verification-veil/)) *(confidence: medium)*

*Takeaway for Chronicle:* Lean's ecosystem now has first-class tooling for exactly the fence/lease/generation state-machine reasoning Chronicle's subscription control plane needs, *if* Chronicle later wants to push past the pure cores. For the first pass, this stays out of scope.

### TLA+ is wrong for the pure cores, right for the concurrent protocol the Lean proofs cannot reach

AWS has used TLA+ since 2011 across at least seven teams (DynamoDB, S3) to catch deep design bugs by model-checking a specification with TLC; the same language expresses both correctness properties and the design. But a TLA+ spec is not executable, so it cannot itself close the proof-to-code gap — its value is design-level reachability of bad interleavings. ([How AWS Uses Formal Methods, CACM 2015](https://cacm.acm.org/research/how-amazon-web-services-uses-formal-methods/); [Lamport-hosted PDF](https://lamport.azurewebsites.net/tla/formal-methods-amazon.pdf))

*Takeaway for Chronicle:* reserve TLA+/TLC for the concurrent subscription protocol — generation fencing, leases/TTL, the four-window recovery sweep — which a per-input pure-function proof in Lean cannot reach, and which is where Chronicle's known architecture-bound serialization lives. See [./01-tla-and-trace-validation.md](./01-tla-and-trace-validation.md) for that thread.

## Techniques and their maturity/effort

| Technique | What it is | Maturity | Effort | Fit for Chronicle |
|---|---|---|---|---|
| **Differential PBT against a verified Lean oracle** | Prove a small model in Lean 4, compile to C via `lake`, expose via `@[extern]`/cgo, run as a third oracle inside the existing differential harness; assert Go, Lua, and Lean agree on result, error, and post-state | emerging | medium | **Highest leverage.** Slots directly into [`store/redis/differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go). Replaces the unproven Go oracle with a proven Lean one for `ValidateProducer`, `Compare`, `GenerateResponseCursor` while keeping the Lua mirror honest. Closes the proof-to-code gap empirically. |
| **TLA-style state-machine refinement (IronFleet)** | Define an abstract spec (init predicate + next-step relation); prove the concrete system refines it via a refinement function | mature | high | Adopt the *idea* in Lean (model `(epoch, lastSeq)` and the six transitions; prove invariants by induction), not the multi-layer down-to-bytes tax. The 5:1–8:1 ratio is the warning. Full refinement only if Chronicle later verifies the protocol layer. |
| **Verified extraction/compilation** (Coq→OCaml, Low\*→C via KaRaMeL, Lean→C) | Generate the running executable from the proof artifact so deployed code *is* verified code | mature | high | Not directly adoptable — no extract-to-Go pipeline; rewriting the hot path would be a rewrite, not a hardening. Borrow narrowly: use Lean→C only to produce the *test-oracle binary*, never production code. Keep KaRaMeL/EverParse in mind for `ParseOffset`/cursor parsers specifically. |
| **Proof by decidable computation + induction** (`decide`/`native_decide`) | Prove decidable goals by evaluating a `Decidable` instance; combine with structural induction over an operation list to discharge "every reachable state satisfies invariant I" | mature | low | **The low-effort on-ramp.** The producer case split is finite and decidable; offset order properties reduce to `Nat`/`UInt64` arithmetic via `omega`/`decide`; cursor monotonicity is one lemma. Prefer `decide` over `native_decide` to keep the TCB at the Lean kernel only. First theorems can be one-liners. |
| **TLA+/TLC model checking for the protocol layer** | Specify the control plane (fencing counter, leases+TTL, wake/claim/ack/release, four recovery windows) as a TLA+ state machine; exhaustively model-check small instances for safety and liveness | mature | medium | Complements the Lean proofs by covering concurrency and crash-restart interleavings they cannot. The design-time twin of the existing Jepsen-style runtime checker (`model_fence`, `model_shard`, `check_stalegen`, `nemesis`). Separate, later effort. |

## Recommendation for Chronicle

**Adopt Lean 4 for the three pure cores. Explicitly do NOT verify Go directly or replace production code.** Concretely:

1. **Build a tiny Lean 4 package** mirroring [`store.ValidateProducer`](https://github.com/adityavkk/chronicle/blob/main/store/producer.go), [`store.Compare`/`Offset.Add`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go), and [`protocol.GenerateResponseCursor`](https://github.com/adityavkk/chronicle/blob/main/protocol/cursor.go) as pure total functions.
2. **Compile it to C with `lake`** and wire it as a third oracle into the existing differential harness ([`store/redis/differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go) style) so it is fuzzed against both the Go core and the Lua mirror.
3. **Follow Welltyped's order:** stand up the differential harness with the Lean oracle *first* (immediate value — catches Go/Lua drift today), then add proofs to the model as time allows.

Choose **Lean over Dafny** because Lean's compile-to-C + cgo lets the proven artifact double as the oracle (no second reimplementation to drift). Choose **Lean over TLA+** because these are total per-input function properties, not interleaving-reachability problems — and reserve TLA+/TLC for the concurrent subscription protocol the pure-core proofs cannot reach.

### First theorems (all small; `decide`/induction/`omega`-discharged)

- **Producer determinism & well-formedness:** `ValidateProducer` is total and its case split is exhaustive (no input falls through the six outcomes).
- **Monotonicity / zombie fencing:** on an `Accepted` outcome, `NewState.LastSeq = old LastSeq + 1` (fresh-epoch case: `= 0`); `Duplicate` and `StaleEpoch` never advance state; epoch is non-decreasing across any accepted sequence.
- **Sequence-replay safety (induction over an op list):** replaying any prefix yields `LastSeq` equal to the count of accepted appends, and a duplicate is reported with the true highest accepted seq.
- **Offset order is a `LinearOrder`:** `Compare` is total, antisymmetric, transitive; `LessThan` is a strict order; `Add bytes` (`bytes > 0`) is strictly monotone in `ByteOffset` — the strict monotonicity the protocol relies on for catch-up reads.
- **Cursor strict progression:** `GenerateResponseCursor(c, now)` parses to an interval strictly greater than the client's echoed interval whenever the client cursor is at or ahead of now (the anti-cache-loop property of [`docs/spec/PROTOCOL.md`](https://github.com/adityavkk/chronicle/blob/main/docs/spec/PROTOCOL.md) §10.1), and equals the current interval otherwise.

These theorems are the proven contract; the differential harness is what carries that contract across the model-to-Go-to-Lua boundary. Cross-link the property-side framing in [./03-property-and-model-based-testing.md](./03-property-and-model-based-testing.md) and the invariant catalogue this proves against in [../INVARIANTS.md](../INVARIANTS.md).

## Pitfalls

- **Proving the model is not proving the code.** A Lean proof covers the Lean function only. The Go core, the Redis-Lua re-implementation, Redis's per-slot serialization, and the recovery sweep can all still diverge. Pair the proof with differential PBT or it gives false confidence — the core model-vs-implementation gap documented for every verified-systems project.
- **The TCB never disappears.** A Lean proof trusts the Lean kernel; `native_decide` additionally trusts the compiler and admits results via an axiom (larger TCB). The cgo/FFI bridge used to run Lean as an oracle is itself unverified glue. Be explicit in the design doc about exactly what is trusted.
- **Cost blowup if scope creeps to the protocol layer.** IronFleet's 5:1–8:1 ratio and ~3.7 person-years are the warning sign: full refinement of a concurrent protocol is a multi-person-year effort. Keep Lean scoped to the small pure cores; use *model checking* (not full proof) for the concurrent parts.
- **Spec-is-wrong risk.** The proofs are only as good as the Lean spec faithfully restating `PROTOCOL.md`. If the Lean model and the protocol disagree, you have proven the wrong thing. The differential harness mitigates this (it forces agreement with real Go behavior) but does not eliminate it — the Go behavior could also be wrong w.r.t. the spec.
- **`UInt64` wrap-around and parsing edge cases.** `Offset` uses `uint64` and `Add` can overflow; the producer uses `int64`. Lean's `UInt`/`Int` semantics must match Go's exactly (wrap vs panic vs saturating) or the oracle diverges spuriously — Welltyped explicitly had to model `UInt64` wrap-around. `ParseOffset`'s validation (one underscore, digits only, 16-pad) is a *parser*; if proven, it is an EverParse-shaped problem, not pure arithmetic.
- **Lean toolchain operational cost in a Go CI.** Lean/mathlib builds are heavy and version-sensitive; pinning `lake`/mathlib and caching the compiled C oracle in CI is required or the build becomes the bottleneck. mathlib is a moving target across Lean versions.

## Open questions

- **Where does the existing fuzzing live — Go (`gopter`/`rapid`) or TypeScript (conformance suite)?** The cleanest oracle wiring differs: a Lean→C library is callable from Go via cgo and from TS via N-API/FFI. Confirm which harness hosts the Lean oracle.
- **How exactly does Go's `int64`/`uint64` arithmetic in the three cores map to Lean's `Int`/`UInt64`/`Nat`?** A precise correspondence is needed — especially `LastSeq+1`, `ByteOffset+bytes` overflow, and the `int64` epoch comparisons — before the oracle can be trusted to agree bit-for-bit.
- **Is the team willing to take Lean/mathlib as a CI build dependency,** or should the Lean model be compiled to a vendored C artifact checked into the repo so day-to-day Go CI does not need the Lean toolchain?
- **Should the cursor jitter step (currently a fixed middle-of-range constant) be part of the proven contract,** or treated as an implementation choice? Strict progression holds regardless, but pinning the exact value in Lean would make the oracle reject a legitimate future jitter-strategy change.
- **Is there appetite to extend beyond the pure cores to a TLA+ spec of the subscription control plane** (fencing/leases/recovery windows), given the saturation work already suggests an architecture-bound serialization a TLA+ model would likely expose at design time?
- **Has anyone validated Lean→C→cgo as a stable, low-overhead oracle path at fuzz-loop call volumes?** The Lean runtime has its own GC/boxing. Welltyped did Lean→C→Rust; the Go/cgo variant needs a spike to confirm performance and memory behavior.
