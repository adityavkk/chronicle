# Formal Verification & Property-Based Testing Roadmap

*Chronicle's correctness surface splits into two regimes that map exactly onto the two existing worktrees: the **pure cores** (total per-input functions over integers and total orders — Lean 4 territory) and the **concurrent protocol** (the subscription wake/lease/`(gen,wake_id)` fence and its four crash-recovery windows — TLA+/TLC, trace validation, and Porcupine territory). This roadmap ranks the tool stack, then lays out five phases (P0–P4) that turn "oracle by assertion" into "oracle by proof" and bind every proof back to the running Go and Lua so the model-vs-code gap cannot hide. It is the backbone of the verification epic; each deliverable below is phrased as a concrete, end-to-end unit of work and tagged with the worktree it lands in.*

## Scope of the first pass

Two worktrees already exist and divide the work:

- **`property-based-testing`** — the executable, runs-in-CI side: [rapid](https://pkg.go.dev/pgregory.net/rapid) model-based differential testing, the [Porcupine](https://github.com/anishathalye/porcupine) linearizability harness (already a dependency at v1.2.0), and the coverage-guided fuzz bridge.
- **`formal-verification`** — the proof/model side: Lean 4 proofs of the pure cores, the TLA+/TLC spec of the subscription protocol with trace validation, the Apalache inductive upgrade, and the Alloy relational models.

The high-leverage core is the spine, but the first-pass scope **explicitly includes** two surfaces beyond it: **SSE / long-poll EOF semantics** (when a stream `Close` must surface as a clean end-of-stream to a streaming or long-polling reader, and the indeterminate-on-timeout framing of appends) and **JSON-mode flattening / fork-sub-offset** (the JSON envelope normalization plus the fork-subscription offset arithmetic, including the documented fork-sub-offset overshoot branch). Both are pulled in at the data-plane phase rather than deferred.

The line between *provable* and *only chaos-testable* is sharp and load-bearing throughout: per-stream linearizability, the `(gen,wake)` single-holder fence, generation monotonicity, cursor forward-progress, and every pure-core function property are **provable / model-checkable**. **Durability across asynchronous Redis replication is NOT** — [`WAIT`/`WAITAOF` explicitly do not make Redis strongly consistent](https://redis.io/docs/latest/commands/waitaof/) and an acked write can be lost on failover. That surface is documented as durability-honest RPO tiers and chaos-tested only, with the monotone fence guaranteeing any lost write degrades to *at-least-once* delivery, never to a safety violation.

See the companion design docs in this directory for depth on each tool: the Lean approach in [./research/02-proof-assistants-lean.md](./research/02-proof-assistants-lean.md), and the per-property formal statements that this roadmap sequences.

## The ranked stack

Ranked by leverage first, then by effort (lowest-friction wins float up within a leverage band). "Worktree" is where the deliverable lands.

| # | Tool | Leverage | Effort | Worktree | Rationale in brief |
|---|------|----------|--------|----------|--------------------|
| 1 | **rapid model-based + differential PBT** ([`pgregory.net/rapid`](https://pkg.go.dev/pgregory.net/rapid)) | high | low | `property-based-testing` | Highest leverage, lowest friction. One [`store.Store`](https://github.com/adityavkk/chronicle/blob/main/store/store.go) interface, [`MemoryStore`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go) as documented oracle, [`differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go) already doing the comparison for a 10-row table. rapid has first-class state-machine testing, fully automatic shrinking (zero user shrink code), and a [fuzz bridge](https://pkg.go.dev/pgregory.net/rapid). Not yet a dependency — adding it is step one. |
| 2 | **TLA+/TLC of the subscription fence/lease/recovery state machine** | high | medium | `formal-verification` | The concurrent control plane is reachability over interleavings — Porcupine only samples it (NP-hard, capped at 3–5 workers); TLC exhaustively interleaves the crash-between-durable-write-and-Go-followup windows. State is small: one sub's `{phase,gen,wake_id,lease_until,wake_event_sent,cursor}` + N workers + a crash action. Direct precedent: [AWS DynamoDB](https://cacm.acm.org/research/how-amazon-web-services-uses-formal-methods/), Kafka KIP-101/279/966. |
| 3 | **TLA+ trace validation** (constrained TLC binding spec ↔ running Go/Lua) | high | medium | `formal-verification` | The late-binding bridge that closes the model-to-code gap for an already-built system. The Lua reply alphabet (`ARMED/CLAIMED/BUSY/OK/FENCED` with `gen`/`wake_id`) is already the event alphabet. [CCF found 4 of 6 bugs this way](https://arxiv.org/html/2406.17455v1) that pure model-checking missed, at ~1 engineer-day of logging. Depends on the spec existing first. |
| 4 | **Lean 4 proofs of the pure cores** (producer SM, offset order, cursor, webhook reducers) | high | medium | `formal-verification` | Converts "oracle by assertion" into "oracle by proof" for the validators the Lua mirror is held to. Total finite-branching SMs + a lexicographic total order → [mathlib `LinearOrder`](https://leanprover-community.github.io/mathlib4_docs/Mathlib/Order/Defs/LinearOrder.html) + `decide`/`omega` + induction. Lean over Dafny because Lean compiles to C and is cgo-callable, so the proven artifact doubles as a third differential oracle ([the Welltyped verified-ledger pattern](https://welltyped.systems/blog/verified-conformance-testing-for-dummies)) — no second reimplementation to drift. |
| 5 | **Porcupine — extend to the data plane + composed two-fence** | high | medium | `property-based-testing` | Already a dependency, already drives [`model_fence.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/model_fence.go) (GREEN) and [`model_shard.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/model_shard.go) (still SCAFFOLD-headed — confirm wiring first). The gap is the **data plane**: no model for append/read/close even though per-stream single-slot Lua atomicity is exactly what a linearizability checker verifies. |
| 6 | **Differential PBT of the triple-mirror predicates** (Go core vs Lua vs checker) | high | low | `property-based-testing` | `offsetGreater`, `FenceDecision`/`fenced`, and `dsSlotOf+crc16` each exist in THREE independent copies (Go core, Lua, [Jepsen checker mirror](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/check_stalegen.go)). The control-plane fence predicates have NO committed differential today — a divergence among the three copies is currently invisible. |
| 7 | **Apalache symbolic checking + inductive-invariant proof of the fence** | medium | high | `formal-verification` | Second-phase upgrade after the TLC spec stabilizes. [Apalache](https://github.com/apalache-mc/apalache/blob/main/README.md) compiles TLA+ to SMT and proves an *inductive* invariant — single-holder for ALL instance sizes, not just bounded N. Scope to the fence core only, after TLC has shaken out the design. |
| 8 | **Alloy relational model of fan-out index + slot-homing** | medium | low | `formal-verification` | The fan-out index (`INV-RECOVER-04`: `streamSubs` SET == projection of the canonical links HASH) and slot-homing scatter-gather (`INV-JEP-T5-01`: scatter set == reference set for all topologies) are pure set-equality / relational properties — Alloy's sweet spot, bounded-exhaustive over all topologies up to a scope. |
| 9 | **`go test` coverage-guided fuzz bridge** (`testing.F.Fuzz` via rapid `MakeFuzz`) | medium | low | `property-based-testing` | Reuses the SAME rapid state machine as a native fuzz target to steer toward rare Lua branches (epoch-bump-at-nonzero-seq, gap-at-boundary, fork-sub-offset overshoot, close-by-producer duplicate) that uniform random under-samples. Not a separate tool — a wiring of the rapid harness. |
| 10 | **Spectacle interactive spec animation** (living documentation) | low | low | `formal-verification` | Once the TLA+ fence spec exists, Spectacle runs it in-browser with forward/backward stepping and URL-shareable traces — realizing [AWS's "spec as living documentation"](https://cacm.acm.org/research/how-amazon-web-services-uses-formal-methods/) benefit for the four crash windows. Produces documentation, not verification, so lowest rank. |
| — | **Elle / Adya isolation-anomaly checking** | low | high | — | **Do NOT adopt for the data plane.** [Elle](https://arxiv.org/abs/2003.10554) needs multi-object multi-version transactional histories; a single non-transactional append-only stream has no such graph — linearizability is the correct frame. Borrow only the recoverability IDEA: unique `(clientId,opSeq)` tags in appended frames to make the Porcupine read step exact. |

For the why-this-over-the-alternatives reasoning (rapid over `testing/quick`/gopter; Lean over Dafny; TLA+ is the favorable case here not the cautionary one; what async Redis replication forecloses), see the verdicts captured in the research synthesis.

## Phases

Phases are sequenced by dependency, not by a hard calendar; the week ranges are planning estimates and the later phases overlap. Each phase names its goal, its concrete deliverables, the worktree each deliverable lands in, and what it depends on.

### P0 — Foundations and the cheapest wins (weeks 1–2)

**Goal.** Stand up the differential PBT harness and the Lean package skeleton, confirm the existing Porcupine wiring, and surface the known latent bugs immediately.

**Dependencies.** None — this is the entry phase.

**Deliverables.**

| # | Deliverable | Worktree |
|---|-------------|----------|
| P0.1 | **Add `pgregory.net/rapid` to `go.mod`.** It is not yet present ([only porcupine v1.2.0 is](https://github.com/adityavkk/chronicle/blob/main/go.mod)). This is the literal first step. | `property-based-testing` |
| P0.2 | **Build the rapid model-based equivalence harness:** [`MemoryStore`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go) oracle vs live Redis subject behind the shared [`store.Store`](https://github.com/adityavkk/chronicle/blob/main/store/store.go) interface, covering `Create` / `Append` (incl. producer epoch-seq) / `Read` / `Close` / `CloseStreamWithProducer` / `Delete` / `Fork`. Single-threaded per stream (sound — Redis serializes per hash-tag slot); diff `(result, error, tail, metadata)` after each step. **Thread an injected clock into BOTH backends** so `IsExpired` is reproducible (real work: [`MemoryStore`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go) calls `time.Now()` directly today). | `property-based-testing` |
| P0.3 | **Surface the `%016d` offset-width bug.** Run the **UNGUARDED** offset `Compare`-vs-`strcmp` rapid property to produce the `>= 10^16` counterexample. Confirmed latent at [`store/offset.go:25`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go): `%016d` is a *minimum* width, max uint64 is 20 digits, so lexicographic order diverges from numeric order — and Redis stores frames as ZSET members keyed by the offset string, so this breaks read ordering at large byte counts. File the width fix (bump to 20, or guard the `< 10^16` domain) as a tracked finding. | `property-based-testing` |
| P0.4 | **Confirm [`model_shard.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/model_shard.go) wiring.** It carried a SCAFFOLD header (removed in #28) even though [`claim_shard.lua`](https://github.com/adityavkk/chronicle/blob/main/webhook/scripts/claim_shard.lua) and [`check_owner.lua`](https://github.com/adityavkk/chronicle/blob/main/webhook/scripts/check_owner.lua) have shipped. ✅ **Done (#28):** confirmed via `redis MONITOR` that the live driver is [`scenario_ownership.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/scenario_ownership.go) (the `ownership-exclusivity` gate) — *not* `scenario_shard.go`, which drives the orthogonal per-`(sub,gen)` lease layer. It exercises the real `claim_shard.lua`/`check_owner.lua`; SCAFFOLD header removed, `INV-OWNER-01` validated against live Lua, Redis-only `linz` CI gate added. | `property-based-testing` |
| P0.5 | **Lean 4 package skeleton:** pin `lake`/`mathlib`; transcribe `ValidateProducer` ([`store/producer.go`](https://github.com/adityavkk/chronicle/blob/main/store/producer.go)), `Offset` `Compare`/`Add` ([`store/offset.go`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go)), and `offsetGreater` ([`webhook/state.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/state.go)) as total functions — **no proofs yet**, just the typed transcription that P1 builds on. | `formal-verification` |

### P1 — Pure-core proofs and the producer/fence differentials (weeks 2–6)

**Goal.** Prove the pure cores in Lean, wire the Lean→C oracle into the differential harness, and close the triple-mirror differential gaps the existing 10-row table misses.

**Dependencies.** P0.1/P0.2 (rapid + the harness), P0.5 (the Lean skeleton).

**Deliverables.**

| # | Deliverable | Worktree |
|---|-------------|----------|
| P1.1 | **Lean theorems for the pure cores.** Producer SM totality / determinism + `newState`-iff-`Accepted` + epoch monotonicity + per-epoch idempotency (induction over op list); `Offset.Compare` is a `LinearOrder` (reuse mathlib `Prod.Lex`); `offsetGreater` strict total order + `MergeAcks` monotone/idempotent; cursor strict-progression + `GenerateCursor` monotonicity ([`protocol/cursor.go`](https://github.com/adityavkk/chronicle/blob/main/protocol/cursor.go)); the offset lex-order theorem **with the explicit `< 10^16` hypothesis** (documenting the safe domain); and the **parametric monotone-token ⇒ single-holder theorem** that later instantiates for both fences. | `formal-verification` |
| P1.2 | **Compile the Lean producer/offset model to C** via `lake`, expose `@[extern]`, bridge via cgo, and add it as a **THIRD oracle** in the differential harness (Go core vs Lua vs proven Lean). **Vendor the compiled C** so day-to-day Go CI needs no Lean toolchain. | both (proof in `formal-verification`, wiring in `property-based-testing`) |
| P1.3 | **Generalize [`differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go)** from the 10-row table to a rapid generator over `(state, epoch, seq)` concentrated near `2^53` and `2^63`; assert the full reply tuple + the persist decision; document and enforce the `< 2^53` Lua-double domain. | `property-based-testing` |
| P1.4 | **New triple-mirror differential for the control-plane fence:** `FenceDecision`/`fenced` ([`webhook/state.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/state.go)) and `dsSlotOf+crc16` — Go core vs live Lua vs the [checker mirror](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/check_stalegen.go). Closes the fence-differential gap (these predicates have no committed differential today). | `property-based-testing` |
| P1.5 | **rapid properties for the JSON-mode / config normalizations:** `ContentTypeMatches` / `ConfigMatches` / `norm_ct` (three independent normalizations) and `ParseTTL` / `ParseSubOffset` / `IsValidIntegerString` including the int64-overflow handler seam. This is the first-pass **JSON-mode flattening** surface entering the harness. | `property-based-testing` |

### P2 — Porcupine data plane + the TLA+ fence spec (weeks 4–10, overlapping)

**Goal.** Add the missing data-plane linearizability check (including SSE/long-poll EOF and fork-sub-offset), and write the implementation-grain TLA+ spec of the subscription fence/lease/recovery state machine and model-check it.

**Dependencies.** P0.2/P0.4 (harness + confirmed shard wiring) for the Porcupine work; P0.5 and the protocol understanding for the TLA+ work. The two tracks run in parallel across the two worktrees.

**Deliverables.**

| # | Deliverable | Worktree |
|---|-------------|----------|
| P2.1 | **Porcupine `streamModel()`** in `jepsen/checker`: state = `(frames, closed, tail)`; partition by path (Herlihy-Wing locality); reuse [`history.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/history.go)'s monotonic-clock recorder; tag frames with `(clientId,opSeq)` per the Elle recoverability idea so the read step is exact; model timed-out / retried appends as **indeterminate**. A new `scenario_store.go` drives K concurrent clients on one stream under the existing [network/partition/clock-skew nemeses](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/nemesis.go). **First-pass SSE/long-poll EOF lands here:** `CLOSE` is idempotent and `READ` past close returns a clean end-of-stream, and `READ(offset)` returns exactly the contiguous suffix (a prefix of the linearized append order). Covers per-stream linearizability and the optimistic RETRY-no-gaps reframe. | `property-based-testing` |
| P2.2 | **Composed Porcupine model** carrying BOTH `(gen,wake)` and `(owner,epoch)` to check the two-fence interaction no isolated checker sees (`model_fence.go` and `model_shard.go` each check one layer alone). Also exercises **fork-sub-offset** prefix-equality so the JSON-mode fork arithmetic is covered against a sequential oracle. | `property-based-testing` |
| P2.3 | **TLA+ `SubscriptionFence` module.** State `{phase, gen, wake_id, lease_until, wake_event_sent, cursor}` + N workers + `Crash`/`Restart` + the **four crash windows as crash points BETWEEN the durable Lua write and the Go follow-up**; actions `Arm` / `Claim(rotate \| coalesce)` / `Ack` / `Release` / `ExpireLease` (a server step that clears `wake` but **NOT** `gen` — matching [`expire_lease.lua`](https://github.com/adityavkk/chronicle/blob/main/webhook/scripts/expire_lease.lua)) / `SweepReemit` / `DueDrain`. SAFETY invariants: single-holder, gen-monotone, stale-inert, ≤1 in-flight wake, cursor forward-only. Start at 1 sub / 1 worker, scale to 2/2. | `formal-verification` |
| P2.4 | **TLA+ `Ownership` module + the layering proof.** Model-check the composed spec twice — with `owner_fenced` enabled, and with `owner_fenced` forced to ALWAYS-PASS — and show `[]SingleHolder` of the inner `(gen,wake)` fence holds in BOTH, proving owner-epoch is optimization-only and never a correctness dependency. | `formal-verification` |
| P2.5 | **Liveness encoding.** Pending-work `~>` wake under weak fairness of the sweep/due loops, and an explicit check that the **`3*sweepInterval` T4 threshold** (an unvalidated magic number in the recently-fixed post-emit re-emit window in [`webhook/manager.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/manager.go)) cannot double-deliver to a slow live consumer and does guarantee eventual re-emit. | `formal-verification` |

### P3 — Trace-validation bridge + recovery/convergence specs (weeks 8–14)

**Goal.** Bind the running Go/Lua to the TLA+ spec via trace validation (the [CCF recipe](https://arxiv.org/html/2406.17455v1)), and model the distributed membership/HRW convergence the cloud rig cannot exhaustively cover.

**Dependencies.** P2.3 (the spec must exist before traces can be validated against it); the Jepsen scenarios and conformance suite as trace sources.

**Deliverables.**

| # | Deliverable | Worktree |
|---|-------------|----------|
| P3.1 | **Trace-recording seam** in `Manager`/`RedisStore` (behind the existing `Metrics` seam or a build tag): append-only JSONL of every transition `{sub, op, preState, args, luaStatus, postState, owner_scope, slot_epoch}`, keyed to the Lua reply. The linearization point must be logged at **every** Lua commit and every arm/claim/ack/release outcome — an under-instrumented trace validates trivially. Budget a spike to place the seams correctly in [`webhook/manager.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/manager.go) and the [Lua scripts](https://github.com/adityavkk/chronicle/blob/main/webhook/scripts/common.lua). | `formal-verification` (seam touches `webhook/`) |
| P3.2 | **`Trace.tla` wrapper** with per-action `IsEvent` predicates; run TLC in **DFS-constrained mode** to prove traces from the Jepsen scenarios + conformance suite are legal spec behaviors. Compose actions for the non-atomic `arm→emit` and Lua-then-Go index splits (grain-of-atomicity matching). Wire into CI exactly as [the CCF "Validating Traces" recipe](https://arxiv.org/html/2404.16075v2) did. | `formal-verification` |
| P3.3 | **TLA+ membership / HRW / slot-reconcile convergence spec** — the `L2`/`L4`/`L5` surface that is PENDING-CLOUD with no exhaustive check today. Liveness under fairness: after churn stops, every slot has exactly one unexpired owner; plus the `L3` lease-tail-drop refinement (recoverable from the durable hash even when the ZSET entry is `ZREM`-ed). | `formal-verification` |
| P3.4 | **Alloy models:** fan-out index == projection of canonical links (`INV-RECOVER-04`: reconcile never invents membership); slot-homing scatter == reference set for all topologies up to scope (`INV-JEP-T5-01`). | `formal-verification` |

### P4 — Inductive proof, fuzz hardening, and living documentation (weeks 12+, opportunistic)

**Goal.** Convert "no counterexample up to bound N" into a size-independent guarantee for the fence; harden via coverage-guided fuzzing; turn the spec into onboarding documentation; and chaos-test the one surface that is not provable.

**Dependencies.** P2.3 (a stabilized TLC spec) for the Apalache upgrade; P0.2/P1.3 (the rapid harness) for the fuzz bridge; the GKE rig for the failover chaos.

**Deliverables.**

| # | Deliverable | Worktree |
|---|-------------|----------|
| P4.1 | **Apalache inductive-invariant proof of the single-holder fence** (rotate ⇒ strictly-greater, coalesce ⇒ equal, accepted-ack ⇒ current gen) for ALL instance sizes — add type annotations, find the inductive strengthening, scope to the fence core only. | `formal-verification` |
| P4.2 | **Wire the rapid harness as a native coverage-guided fuzz target** (`MakeFuzz`) aimed at the rare Lua branches; run nightly in CI with persisted seeds + the `go fuzz` corpus as regression fixtures. | `property-based-testing` |
| P4.3 | **Spectacle animation** of the four crash windows and the rotate-vs-coalesce decision, URL-shareable, replacing the `docs/research` prose for onboarding and the ADR. | `formal-verification` |
| P4.4 | **Real Redis Sentinel/Cluster failover nemesis on the GKE rig** asserting a lost fence-write degrades only to *at-least-once* delivery, never a safety violation; confirm the `WAITAOF` AOF-enabled startup assertion and the `DurabilityShortError` operator metric. **The rig runs on personal GCP at ~$4/hr and a crashed session once stranded a cluster — ALWAYS tear down; never leave it running across a session.** | `property-based-testing` |

## Worktree split at a glance

| Worktree | Owns |
|----------|------|
| `property-based-testing` | rapid harness + clock seam (P0.1–P0.3), shard-wiring confirmation (P0.4), generalized + triple-mirror differentials (P1.3–P1.5), the Lean→C oracle *wiring* (P1.2), Porcupine data plane + composed two-fence incl. SSE/EOF and fork-sub-offset (P2.1–P2.2), the fuzz bridge (P4.2), the failover chaos rig (P4.4). |
| `formal-verification` | Lean skeleton + proofs incl. the Lean→C *compile* (P0.5, P1.1–P1.2), the TLA+ fence/ownership/liveness spec (P2.3–P2.5), trace validation + membership/HRW + Alloy (P3.1–P3.4), Apalache inductive upgrade + Spectacle (P4.1, P4.3). |

## Risks & mitigations

| Risk | Mitigation |
|------|------------|
| **Model-vs-implementation gap is the central risk for BOTH worktrees.** A Lean proof covers only the Lean function; a TLA+ spec only the spec. The Go core, the Lua re-implementation, Redis per-slot serialization, and the recovery sweep can all still diverge. | Non-negotiable pairing: every Lean proof is wired into the differential harness (P1.2), and the TLA+ spec is bound to the code via trace validation (P3.1–P3.2). Without this you get false confidence. |
| **Shared-core blind spot.** `MemoryStore` and the Redis/Lua backend share the SAME pure validators, so a bug in the shared core agrees in both and the equivalence test passes silently. | Independent metamorphic/invariant properties (append-read round-trip, fork prefix-equality, read split-invariance) and the hand-derived Porcupine sequential models that re-state the spec independently (P2.1). |
| **Trace validation guarantees nothing about what you do not log.** An under-instrumented trace validates trivially; CI goes green while real divergences hide. | Log linearization points at every Lua commit and every claim/ack/release/arm outcome. Budget a dedicated spike to place the log seams correctly in `webhook/manager.go` and the Lua scripts (P3.1). |
| **TLC checks bounded instances, not all N.** A passing run on 2–3 workers is not a proof; do not oversell it. | Either argue a small-scope bound suffices, or escalate the single-holder fence to an [Apalache inductive invariant](https://github.com/apalache-mc/apalache/blob/main/README.md) (P4.1). Liveness needs explicit fairness assumptions and is weakly covered by trace validation — budget for it separately (P2.5). |
| **The `%016d` offset-width bug is real and latent** ([`store/offset.go:25`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go)): lexicographic order diverges from numeric order at fields `>= 10^16`, breaking Redis ZSET-keyed read ordering at large byte counts. Invisible to all current tests (only tiny values tested). | Surface it with the unguarded rapid property and fix it (width bump to 20 or a guarded domain invariant) **before it bites in production at scale** (P0.3). |
| **Latent `ReadSeq` divergence.** `MemoryStore.readOwnMessages` compares only `ByteOffset` while Redis compares the full offset-string prefix INCLUDING `ReadSeq`; they agree only because `ReadSeq` is always 0 today (log rotation unimplemented). Silently breaks when `ReadSeq` becomes non-zero. | Add a regression property now, in the differential harness (P0.2/P1.3). |
| **`model_shard.go` still carries its SCAFFOLD header** even though `claim_shard.lua`/`check_owner.lua` have shipped; if the live driver is not actually wired, `INV-OWNER-01/02` are specified but never validated against real Lua. | Confirm the wiring in P0.4 **before** building on it. |
| **The `3*sweepInterval` staleness threshold** for the recently-fixed T4 post-emit re-emit window is an unvalidated magic number — no proof it cannot re-emit to a still-live slow consumer, nor that 3× guarantees eventual re-emit. | Model it explicitly in TLA+ (P2.5). |
| **Lean/mathlib toolchain is heavy and version-sensitive;** an unpinned or uncached build becomes the CI bottleneck. | Pin `lake`/`mathlib` and vendor the compiled C oracle so routine Go CI never invokes the Lean toolchain (P0.5, P1.2). Prefer plain `decide` over `native_decide` where feasible — `native_decide` enlarges the TCB (admits via an axiom + trusts the compiler). |
| **Time/TTL/lease-expiry nondeterminism breaks naive equivalence** (sliding TTL, `ExpiresAt`). | Treat clocks as observed outputs, or inject a controllable clock into both backends; never assert `MemoryStore` and Redis expire on the same wall-clock instant. The harness REQUIRES a clock seam threaded into `MemoryStore` (which calls `time.Now()` directly today) — real work, not just wiring (P0.2). |
| **Durability over-claiming.** Any path that reads a `WAIT`/`WAITAOF` count to infer ordering or lease ownership, or trusts a lease TTL for exclusivity, reintroduces the [etcd ~18%-lost-update bug class](https://jepsen.io/analyses/etcd-3.4.3). | The doc must state that safety rests ONLY on the monotone fence; `consistency.go` already forbids this, but every new path must re-check the fence, not lease-held-ness. Documented as durability-honest RPO tiers, chaos-tested only (P4.4). |
| **GKE failover chaos rig runs on personal GCP at ~$4/hr and a crashed session once stranded a cluster.** | P4.4 cloud failover testing must always tear down; never leave it running across a session. |
