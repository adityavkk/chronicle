# Property-based & model-based testing; the Go library choice (rapid)

*Prior-art note for Chronicle's formal-verification effort. The state of the art for testing storage/log/database systems is the QuickCheck stateful-testing pattern: write a **simple** sequential reference model, generate random command sequences against the real system, check a postcondition after each step, and automatically shrink any failure to a minimal reproducer. This note surveys that lineage, picks [`pgregory.net/rapid`](https://pkg.go.dev/pgregory.net/rapid) as Chronicle's PBT engine, and grounds every technique in concrete Chronicle files — most concretely, using [`MemoryStore`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go) as the oracle for the Redis/Lua backend behind the shared [`store.Store`](https://github.com/adityavkk/chronicle/blob/main/store/store.go) interface.*

## Why this matters for Chronicle

Chronicle is an append-only durable-streams server with a deliberately layered design: a pure-Go core ([`store/producer.go`](https://github.com/adityavkk/chronicle/blob/main/store/producer.go), [`store/offset.go`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go), [`protocol/cursor.go`](https://github.com/adityavkk/chronicle/blob/main/protocol/cursor.go)) is the *oracle*, Redis Lua scripts ([`store/redis/scripts/*.lua`](https://github.com/adityavkk/chronicle/blob/main/store/redis/scripts/append.lua)) *mirror* it, and [`MemoryStore`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go) is an in-process oracle backend that satisfies the identical [`store.Store`](https://github.com/adityavkk/chronicle/blob/main/store/store.go) interface. That shape is exactly the shape the QuickCheck stateful pattern wants: a simple model and a real system that expose the same API. The research below establishes that the pattern is mature, finds deep bugs, and is well-supported in Go; the recommendation section turns that into a concrete harness.

## Key findings

The pattern, its track record, the property taxonomy, and the Go-tooling comparison are below; each row carries its verified citation.

### The canonical stateful pattern: simple model + commands + shrinking

The core of Quviq QuickCheck's `eqc_statem` is: a **simple** reference model, a set of commands each with a *precondition*, *action*, and *postcondition*/*next-state*, generated as random sequences of valid actions, with the real system's state checked against the model's after each step, and any failing sequence automatically shrunk to a minimal reproducer. Hughes' *Experiences with QuickCheck* introduced these stateful extensions; `eqc` was the first PBT library to support state machines.

> Confidence: **high** (verified). Source: [Experiences with QuickCheck: Testing the Hard Stuff and Staying Sane (Hughes, PDF)](https://www.cs.tufts.edu/~nr/cs257/archive/john-hughes/quviq-testing.pdf) · [Springer chapter](https://link.springer.com/chapter/10.1007/978-3-319-30936-1_9). *Note: the PDF was retrieved but not text-extractable in the research run; the pattern is corroborated by the LevelDB and AUTOSAR primary sources below.*

### Track record 1 — LevelDB "ghost key" (17 steps)

QuickCheck model-based testing found a real data-corruption bug in Google LevelDB: a deleted key reappeared (a "ghost key") on a seek-first after a specific open/close/put/delete sequence. The tester wrapped LevelDB via an Erlang NIF and let QuickCheck **find and minimize** a failing case of 17 steps "in a few minutes." This is the canonical demonstration that long, structured command sequences provoke storage bugs that example-based tests miss — directly relevant to Chronicle's append/close/read/delete surface.

> Confidence: **high** (verified). Source: [google/leveldb issue #50](https://github.com/google/leveldb/issues/50) · [QuickCheck helps debug Google LevelDB (Quviq blog)](https://www.quviq.com/blog/google-leveldb/).

### Track record 2 — AUTOSAR / Volvo Cars (industrial scale)

Modeling AUTOSAR basic-software specifications for Volvo Cars, QuickCheck identified roughly **200 problems, including well over 100 ambiguities/errors in the AUTOSAR standard itself**, across about 1M lines of code, by translating 3000+ pages of textual spec into executable models. The lesson for Chronicle: a model-based test campaign surfaces *spec* ambiguities, not just implementation bugs — useful when reconciling the pure core, the Lua mirror, and the prose protocol.

> Confidence: **medium** (not independently verified verbatim). Source: [Testing AUTOSAR software with QuickCheck (IEEE Xplore)](https://ieeexplore.ieee.org/document/7107466/) · [Modelling of AUTOSAR Libraries for Large Scale Testing (arXiv 1703.06574)](https://arxiv.org/pdf/1703.06574). *Caveat: the IEEE page and arXiv PDF were not text-extractable in the research run; the figures come from secondary summaries — treat as approximate and re-verify before quoting.*

### Track record 3 — Riak eventual consistency

The same model-based approach found eventual-consistency bugs in Riak (Basho's Dynamo-style distributed KV store) by modeling an eventually-consistent database in QuickCheck and checking the implementation against it. Relevant to Chronicle's relaxed-consistency read paths (catch-up reads, long-poll, SSE) where the observable contract is weaker than strict linearizability.

> Confidence: **medium** (verified the talks exist and are attributed; specific bugs are from the index description, not primary talk content). Source: [Testing Distributed Systems — curated resources (asatarin)](https://github.com/asatarin/testing-distributed-systems).

### The two property families Chronicle should lean on

Hughes' *How to Specify It!* presents five generic ways to write properties — Invariant, Postcondition, Metamorphic, Inductive, and Model-based — and singles out two that fit Chronicle:

| Family | Definition | Chronicle fit |
| --- | --- | --- |
| **Model-based** | The real operation *commutes* with the model operation via an abstraction function. Together, model-based properties "form a complete specification of the code and so should be expected to find every bug"; each one "tests just one operation, and finds every bug in that operation." | The shared [`store.Store`](https://github.com/adityavkk/chronicle/blob/main/store/store.go) interface *is* the abstraction function: diff `MemoryStore` against Redis per operation. |
| **Metamorphic** | Relate two related calls without computing an exact expected result. | Append-read round-trip, read split-invariance, fork prefix-equality — cheap, and they don't need a second implementation. |

> Confidence: **high** (verified). Source: [How to Specify It! (Hughes, Chalmers full text)](https://research.chalmers.se/publication/517894/file/517894_Fulltext.pdf) · [How to Specify It! In Java! (jlink companion)](https://johanneslink.net/how-to-specify-it/). *Note: the Chalmers PDF was not text-extractable in the research run; the taxonomy and the "complete specification" claim were confirmed via search snippets and the jqwik/jlink companion.*

### Go tooling: rapid is the choice

| Library | State-machine API | Shrinking | Deps | Verdict |
| --- | --- | --- | --- | --- |
| [**rapid**](https://pkg.go.dev/pgregory.net/rapid) | `T.Repeat(map[string]func(*T))` and `StateMachineActions` (reflection over `ActionName(t *rapid.T)` methods + a `Check(*T)` invariant run after every action) | **Fully automatic, integrated, no user code** | **zero non-stdlib** | **Recommended.** |
| [`testing/quick`](https://pkg.go.dev/testing/quick) (stdlib) | none | **none** — open issue [#9282](https://github.com/golang/go/issues/9282) never landed | stdlib | Frozen, "not accepting new features." Unsuitable. |
| [gopter](https://pkg.go.dev/github.com/leanovate/gopter/commands) | ScalaCheck-style `Command{Run,NextState,PreCondition,PostCondition}` + `Commands{...}` | sequence shrinking, but **often needs user code** | non-stdlib | Heavier API; rapid "provides a much simpler API" and shrinks "without any user code." |

rapid's feature list explicitly includes "Support for state machine (stateful / model-based) testing" and "Fully automatic minimization of failing test cases," and it bridges to Go's coverage-guided native fuzzer: **"any rapid test can be used as a fuzz target for the standard fuzzer"** via `MakeFuzz`, and `MakeCheck(prop func(*T)) func(*testing.T)` wires a rapid property into `testing.T.Run`. rapid's own stated tradeoff vs `testing.F.Fuzz`: rapid "shines in generating complex structured data, including state machine tests, but lacks coverage-guided feedback and mutations" — the fuzz bridge recovers that feedback.

> Confidence: **high** (verified). Source: [rapid docs (pkg.go.dev)](https://pkg.go.dev/pgregory.net/rapid) · [rapid README](https://github.com/flyingmutant/rapid) · [state-machine example](https://github.com/flyingmutant/rapid/blob/master/example_statemachine_test.go) · [testing/quick docs](https://pkg.go.dev/testing/quick) · [go #9282](https://github.com/golang/go/issues/9282) · [gopter/commands docs](https://pkg.go.dev/github.com/leanovate/gopter/commands) · [gopter README](https://github.com/leanovate/gopter) · [Go Fuzzing](https://go.dev/doc/security/fuzz/).

### Cross-ecosystem: the pattern is the standard

The same stateful pattern recurs across ecosystems, which both validates the design and supplies mature patterns to copy:

- **Python Hypothesis** `RuleBasedStateMachine`: `@rule` / `@initialize` (runs once before any normal rule) / `@precondition` (filters rules by state) / `@invariant` (runs after every step) + `Bundle` to carry generated values; "automatically shrinks failing sequences into minimal reproducible programs."
- **JS fast-check**: `Command{check(model)->bool, run(model, real)}`, `fc.commands` + `modelRun` / `asyncModelRun` / `scheduledModelRun` (the last detects race conditions), with a `replayPath` that shrinks to only the commands whose `check()` passed. Its docs warn the model "should not be a carbon copy of the system but a simplified representation."

> Confidence: **high** (verified). Source: [Hypothesis stateful docs](https://hypothesis.readthedocs.io/en/latest/stateful.html) · [fast-check model-based testing](https://fast-check.dev/docs/advanced/model-based-testing/) · [asyncModelRun reference](https://fast-check.dev/api-reference/functions/asyncModelRun.html).

### Linearizability checking is complementary, not a substitute

Linearizability checkers (Jepsen/Knossos, Porcupine) verify that a **concurrent** history has **some** valid sequential ordering against a single-threaded model; single-threaded model-based PBT verifies **step-by-step** equivalence. Knossos "requires a model that defines a single-threaded datatype" and "tries to find a serial ordering consistent with each client's observed responses"; the search is NP-complete, so checkers bound it. Chronicle already uses Porcupine — `jepsen/checker` imports `github.com/anishathalye/porcupine` and drives sequential pure models (`model_fence.go`, `model_shard.go`) recorded via `history.go` with host-monotonic timestamps — so the two techniques compose: PBT for per-stream serialized mutations, Porcupine for the concurrent control plane.

> Confidence: **high** (verified — the Chronicle files were read in the research run). Source: [jepsen-io/knossos](https://github.com/jepsen-io/knossos) · [porcupine (anishathalye)](https://github.com/anishathalye/porcupine).

## Techniques and their maturity / effort

| # | Technique | Maturity | Effort | What it buys Chronicle |
| --- | --- | --- | --- | --- |
| 1 | MemoryStore-as-oracle equivalence (rapid `T.Repeat`) | mature | medium | Generalizes the static [`differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go) table into millions of shrinkable sequences |
| 2 | Metamorphic relations for append/read/fork | mature | low | Invariants that hold against each store independently — also guard the oracle |
| 3 | Coverage-guided fuzz bridge (`MakeFuzz`/`MakeCheck`) | mature | low | One model doubles as a nightly fuzz target aimed at rare Lua branches |
| 4 | Concurrent linearizability (Porcupine) for the control plane | mature | medium | Extend existing harness; generate concurrent schedules with rapid |
| 5 | Shrink-to-minimal-reproducer discipline | mature | low | A property of rapid, but make it an explicit doc requirement |

### 1. Linearizable model-based equivalence (MemoryStore oracle vs Redis SUT)

A rapid state-machine test whose *model* is in-process [`MemoryStore`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go) and whose *real system* is the live Redis backend, both behind the identical [`store.Store`](https://github.com/adityavkk/chronicle/blob/main/store/store.go) interface. Each generated action — `Create`, `Append` (including producer epoch/seq), `CloseStream` / `CloseStreamWithProducer`, `Read`, `GetCurrentOffset`, `Delete`, and fork-create — is applied to **both**; the `Check` invariant asserts identical `(result, error)` and identical observable tail/metadata. This *is* Hughes' model-based property: the real op commutes with the model op via the shared interface as the abstraction function. It generalizes [`store/redis/differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go), which already does exactly this comparison for a hand-written producer table. Wrapping that comparison in `rapid.T.Repeat` (a `Chronicle` struct with action methods plus a `Check` that diffs the two stores) turns ~10 rows into shrinkable sequences and drives idempotent-producer fencing (the `ErrStaleEpoch` / `ErrInvalidEpochSeq` / `ErrProducerSeqGap` / `ErrSequenceConflict` semantics in [`store/producer.go`](https://github.com/adityavkk/chronicle/blob/main/store/producer.go) and the `Append` contract in [`store.Store`](https://github.com/adityavkk/chronicle/blob/main/store/store.go)), Stream-Seq ordering, fork lineage/refcount, and close/closed-by semantics. **Keep the model single-threaded and sequential** (sound because Redis serializes a whole mutation per hash-tag slot); use Porcupine for concurrency.

> Source: [rapid `T.Repeat`/`StateMachineActions`](https://pkg.go.dev/pgregory.net/rapid) · [How to Specify It!](https://research.chalmers.se/publication/517894/file/517894_Fulltext.pdf).

### 2. Metamorphic relations for append / read / fork

Relate two related executions without a full second model — valuable for the SSE/long-poll read paths and fork copy-on-write lineage, where building a full model is expensive. Concrete relations:

1. **Append-then-read round-trip** — reading from offset 0 after appending `p1..pn` yields exactly `p1..pn` in order.
2. **Read split-invariance** — `Read(0..tail) == Read(0..k) ++ Read(k..tail)` for any split `k` (catch-up == long-poll == SSE consistency, against the `Read`/`WaitForMessages` contract in [`store.Store`](https://github.com/adityavkk/chronicle/blob/main/store/store.go)).
3. **Append monotonicity** — the tail offset is strictly non-decreasing and `Offset.Compare` ([`store/offset.go`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go)) is a total order.
4. **Fork prefix-equality** — immediately after forking `S` at offset `f`, the fork's bytes `< f` are byte-identical to `S`'s, and appends to the fork never mutate `S`.
5. **Idempotent-producer dedup** — re-sending an already-accepted `(epoch, seq)` returns Duplicate and does **not** change the tail.

These hold for `MemoryStore` **and** Redis independently, so they also double as a smoke test that the oracle itself is correct (see the shared-validator pitfall below). Cheap to express over generated byte slices and split points.

> Source: [How to Specify It! — Metamorphic (jlink)](https://johanneslink.net/how-to-specify-it/) · [fast-check model-based testing](https://fast-check.dev/docs/advanced/model-based-testing/).

### 3. Coverage-guided fuzz bridge

Reuse a single rapid state machine as a target for Go's coverage-guided native fuzzer: rapid generates structured command sequences, and `testing.F.Fuzz` adds mutation + coverage feedback to steer toward unexplored Lua branches. This is specifically aimed at [`store/redis/scripts/*.lua`](https://github.com/adityavkk/chronicle/blob/main/store/redis/scripts/append.lua), which re-implement the pure validators — coverage-guided mutation is good at hitting rare branches (epoch-bump-at-nonzero-seq, gap-at-`lastSeq+1`, fork-sub-offset overshoot, close-by-producer duplicate) that uniform random generation under-samples. Run the equivalence harness as a fuzz target in nightly CI; keep the same model for fast deterministic PBT and long coverage-guided fuzzing.

> Source: [rapid README (`MakeFuzz`)](https://github.com/flyingmutant/rapid) · [Go Fuzzing](https://go.dev/doc/security/fuzz/).

### 4. Concurrent linearizability with a sequential model (Porcupine)

For operations that run concurrently — the `__ds` wake/claim/ack/release protocol, worker leases, generation fencing — record a real-time history of concurrent client ops and check it against a **sequential** model. Chronicle already does this: `jepsen/checker` uses Porcupine with `model_fence.go` (the generation fence as a monotonic register) and `model_shard.go` (slot ownership as a CAS register), recorded via `history.go` with host-monotonic timestamps. The recommendation is to **extend coverage** (lease-TTL takeover, the four origin-restart recovery windows, the recovery sweep) and to **generate the concurrent schedules with rapid** rather than hand-writing scenarios, so the schedule space is explored and shrunk automatically.

> Source: [porcupine](https://github.com/anishathalye/porcupine) · [knossos — single-threaded model requirement](https://github.com/jepsen-io/knossos).

### 5. Integrated-shrinking discipline

The capability that makes stateful testing usable in practice: when a generated sequence fails, automatically minimize it to the smallest sequence that still fails. rapid tracks how random bytes map to drawn values and shrinks the bitstream, minimizing command sequences **and** their parameters with no user-written shrink functions — the same capability that reduced LevelDB to a 17-step case and gopter's BuggyCounter to `[INC INC INC INC DEC GET]`. This is a property of the library, but the design doc should make it an explicit requirement: a failing CI run must print a minimal, replayable command sequence plus a deterministic-replay seed, and failing seeds should be persisted as regression fixtures.

> Source: [rapid README — automatic minimization](https://github.com/flyingmutant/rapid) · [leveldb #50 — 17-step minimized case](https://github.com/google/leveldb/issues/50).

## Recommendation for Chronicle

Adopt [`pgregory.net/rapid`](https://pkg.go.dev/pgregory.net/rapid) as Chronicle's property-based and stateful testing engine, and build a **single** rapid `T.Repeat` state-machine harness that uses [`MemoryStore`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go) as the reference model (oracle) and the live Redis backend as the system under test, both driven through the identical [`store.Store`](https://github.com/adityavkk/chronicle/blob/main/store/store.go) interface.

Justification:

1. **rapid is the right tool.** First-class state-machine testing (`T.Repeat` / `StateMachineActions`), fully automatic integrated shrinking, and zero non-stdlib dependencies — where `testing/quick` is frozen with no shrinking and gopter needs a heavier API with more user-written shrink code.
2. **Chronicle is already shaped for it.** One `Store` interface, `MemoryStore` as the documented oracle, and [`differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go) already doing the exact comparison for a static producer table — this is generalization, not new architecture.
3. **One model, two runners.** rapid bridges to Go's native fuzzer (`MakeFuzz` / `MakeCheck`), so the same model doubles as a nightly coverage-guided fuzz target against the Lua scripts' rare branches.

Concretely: define a `Chronicle` struct with action methods `Create` / `Append` (incl. producer epoch/seq) / `CloseStream` / `Read` (catch-up + split) / `Delete` / `Fork`; after each action, diff `(result, error)` + tail + metadata between the two stores in `Check`; persist failing rapid seeds as regression fixtures. Keep this harness **strictly single-threaded per stream** (sound because Redis serializes a whole mutation per hash-tag slot), and keep the existing Porcupine/Jepsen linearizability harness for the concurrent `__ds` subscription control plane (wake/claim/ack/release, leases, generation fencing, recovery sweep) — generating those concurrent schedules with rapid as well. Add the metamorphic properties (append-read round-trip, read split-invariance, fork prefix-equality, producer-duplicate-no-tail-change) that hold against each store independently, to also guard the oracle itself.

See the sibling notes for how the pure cores get machine-checked proofs ([./02-proof-assistants-lean.md](./02-proof-assistants-lean.md)) and how the subscription protocol is model-checked ([./01-tla-and-trace-validation.md](./01-tla-and-trace-validation.md)); the invariants this harness asserts are catalogued in [../INVARIANTS.md](../INVARIANTS.md).

## Pitfalls

1. **The model must be *simpler* than the system, not a second copy** — but Chronicle's twist is the *opposite* risk. fast-check warns the model "should not be a carbon copy of the system." For Chronicle, `MemoryStore` and the Redis/Lua backend share the **same** pure validators ([`store.ValidateProducer`](https://github.com/adityavkk/chronicle/blob/main/store/producer.go), `Offset.Compare`, [`GenerateResponseCursor`](https://github.com/adityavkk/chronicle/blob/main/protocol/cursor.go)), so a bug in the shared core will agree in both stores and the equivalence test passes silently. Mitigate with the independent metamorphic/invariant properties and the Porcupine sequential models that re-derive the spec by hand.
2. **Single-threaded equivalence is only sound where the backend serializes.** Redis mutations are atomic per hash-tag slot, so per-stream sequential equivalence holds; cross-stream ordering, fork-cascade GC, and the subscription control plane are concurrent and must go through linearizability checking, not step-by-step equivalence. Mixing them up produces false confidence.
3. **Time, TTL, and lease expiry are nondeterministic** (sliding TTL renewal, `ExpiresAt`, lease TTL) and break naive equivalence. Treat clocks as observed outputs (as `model_fence.go` / `model_shard.go` already do — "time is deliberately absent") or inject a controllable clock; do not assert the two stores expire on the same wall-clock instant.
4. **Neither runner suffices alone.** `testing.F.Fuzz` under-explores deep stateful sequences and lacks structured generation; rapid alone lacks coverage feedback. Use the bridge (one model, both runners) — this is rapid's own stated tradeoff.
5. **Shrinking can mask flakiness.** If a failure depends on real Redis timing or a nemesis, the shrinker may produce an unreproducible minimal case. Pin the rapid seed, make actions deterministic given the seed, and isolate environmental nondeterminism (network, GC pauses) out of the asserted postcondition.
6. **Generator bias matters.** Uniformly random producer epoch/seq rarely hits the interesting boundaries (epoch bump at `seq > 0`, gap at `lastSeq + 1`, fork sub-offset overshoot). Bias generators toward edge values (rapid already biases to small values / edge cases) and explicitly seed the known boundary table from [`differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go) as part of the action space.

## Open questions

1. The verbatim AUTOSAR/Volvo figures (≈200 problems / 100+ ambiguities / 1M LOC / 3000 pages) come from secondary summaries; the IEEE paper and Hughes chapter were fetched but not text-extractable in the research run. Re-verify against the primary text before quoting figures in the design doc.
2. Should the equivalence harness diff full byte payloads on every `Read` (expensive at scale) or only offsets/lengths plus a periodic full read? A sampling strategy is needed that keeps shrinking fast while still catching content corruption like the LevelDB ghost-key class of bug.
3. How should the four origin-restart recovery windows and the recovery sweep be modeled so Porcupine can check them — as linearizable register operations (like `model_fence` / `model_shard`), or do they need a richer fault-injection model (combining fault injection with PBT, as in the Quviq line of work)?
4. Standardize on rapid's seed replay + `go fuzz` corpus files only, or also adopt a gopter `commands.Replay`-style persisted regression corpus? This affects how minimized counterexamples are pinned in CI.
5. Does rapid's reflection-based `StateMachineActions` or the explicit `T.Repeat` map form fit Chronicle's action set better? The map form gives finer control over preconditions / `Skip` for closed or soft-deleted streams. Prototype both before committing the design-doc API shape.
