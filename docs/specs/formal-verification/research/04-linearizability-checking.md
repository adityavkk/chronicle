# Linearizability & consistency checking (Porcupine, Knossos, Elle)

*Black-box checking of recorded operation histories is the workhorse of distributed-systems correctness testing. This note maps that body of prior art — Herlihy & Wing's linearizability, Jepsen's Knossos, Anthony Athalye's Porcupine, and Kingsbury & Alvaro's Elle — onto Chronicle, and concludes that the single highest-value addition is a second Porcupine model whose sequential spec is the [`MemoryStore`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go) append/read/close semantics, partitioned per stream. Elle is the wrong tool for a single non-transactional append-only stream; only its recoverability/traceability idea transfers.*

## Why linearizability, and why now

Chronicle's data plane stores each stream behind a single Redis Lua script. [`append.lua`](https://github.com/adityavkk/chronicle/blob/main/store/redis/scripts/append.lua) runs the whole `existence → soft-delete → expiry → closed → content-type → producer → stream-seq → write + tail + notify` chain as one atomic unit, and [`keys.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/keys.go) places every key for one stream under the same `{<path>}` hash tag so a multi-key script executes within a single slot with no interleaving. That design *asserts* per-stream linearizability — but it is currently only asserted, not tested against concurrent histories.

The control plane is in better shape. [`model_fence.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/model_fence.go) already drives Porcupine against the `(generation, wake_id)` lease-fence register, and [`history.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/history.go) records operations in exactly the shape Porcupine wants. The gap is the data plane: append/read/close has no linearizability check, even though it is the textbook case a checker is built for. This note's recommendation closes that gap.

## Key findings

Each finding below carries its verified citation. Confidence is `high` for every item in the source research.

### Linearizability is the exact correctness target, and it is *local*

Linearizability (Herlihy & Wing, TOPLAS vol. 12 no. 3, July 1990) requires that every operation appear to take effect atomically at a single instant between its invocation and its response, consistent with a sequential specification. Two properties matter for Chronicle:

- **Locality**: a system is linearizable iff *every object* is linearizable independently. This is precisely what justifies checking each stream in isolation — Porcupine's per-partition search.
- **Atomic linearization point**: a single Lua script under one hash-tag slot *is* an atomic linearization point by construction.

Citations: [Herlihy & Wing 1990 (PDF)](https://cs.brown.edu/people/mph/HerlihyW90/p463-herlihy.pdf), [dblp record](https://dblp.org/rec/journals/toplas/HerlihyW90.html).

### Porcupine implements the Wing–Gong / Lowe search with P-compositionality

Porcupine reimplements the Wing–Gong–Lowe depth-first backtracking search (provisional-linearize / undo over candidate linearizations), optimized with Horn & Kroening's P-compositionality, and reports **1,000×–10,000× faster than Knossos** (millions of times faster where P-compositionality applies). Gavin Lowe's paper presents the base algorithms; Horn & Kroening generalize Herlihy–Wing locality to operations on the same datatype.

Citations: [porcupine README](https://raw.githubusercontent.com/anishathalye/porcupine/master/README.md), [Horn & Kroening, P-compositionality (arXiv:1504.00204)](https://arxiv.org/abs/1504.00204), [Lowe, Testing for Linearizability (PDF)](https://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf).

### Porcupine's API is one Chronicle already drives

The `porcupine.Model` is a small struct: `Init() state`, `Step(state, input, output) -> (bool, newState)` (the pure sequential spec), an optional `Partition` that splits the history per key, and `Equal` / `DescribeOperation` / `DescribeState`. `CheckOperations(model, history) bool` and `CheckOperationsVerbose(model, history, timeout) (CheckResult, LinearizationInfo)` decide linearizability over a `[]Operation` of `(ClientId, Input, Call, Output, Return)`, with `CheckResult ∈ {Ok, Illegal, Unknown}`.

[`model_fence.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/model_fence.go) already implements `leaseModel()` with `Init` / `leaseStep` / `partitionBySub` / `Equal` / `describeFenceOp` / `describeFenceState`, and [`history.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/history.go) records the `Operation` shape (Call captured before the request, Return after). A data-plane model is the same pattern with a different state and step.

Citations: [porcupine on pkg.go.dev](https://pkg.go.dev/github.com/anishathalye/porcupine), [anishathalye/porcupine (register example)](https://github.com/anishathalye/porcupine).

### A green check proves the *spec*, not the script's atomicity

Redis serializes a script within a slot, so single-execution linearizability for [`append.lua`](https://github.com/adityavkk/chronicle/blob/main/store/redis/scripts/append.lua) is near-trivial. The value of a checker is not to "prove" the script atomic — it is to prove that the *spec the script is supposed to implement* (close-rejects-append, seq-monotonicity, tail/offset monotonicity, read-returns-a-prefix) actually holds under real concurrency, fault injection, retries, and reconnects. The bugs it can catch live in:

- the Go shell framing/reordering around the script (`append.lua` already has a RETRY / re-frame path on concurrent tail movement);
- Lua-mirror vs. pure-Go-oracle divergence beyond the single table in [`differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go);
- indeterminate outcomes on timeout/reconnect, where a client must treat an append as possibly-applied.

Citations: [porcupine README](https://raw.githubusercontent.com/anishathalye/porcupine/master/README.md), [Herlihy & Wing (locality)](https://cs.brown.edu/people/mph/HerlihyW90/p463-herlihy.pdf).

### Knossos works but hits a scaling wall

Knossos needs the same single-threaded model + step function as Porcupine, but state-space-explodes on long histories and reports `:unknown` when it runs out of memory, with the author's caveat that results are "plausible but verify by hand." Jepsen runs `knossos.linear` (graph search) and `knossos.wgl` (tree search) in parallel via `knossos.competition`. The practical consequence for Chronicle: prefer Porcupine over a Knossos port, keep per-stream histories short and partitioned, and bound each check with a timeout so `CheckOperationsVerbose` returns `Unknown` instead of hanging.

Citations: [jepsen-io/knossos](https://github.com/jepsen-io/knossos), [jepsen.checker docs](https://jepsen-io.github.io/jepsen/jepsen.checker.html).

### Elle is for transactional isolation — a graph Chronicle's data plane does not have

Elle (Kingsbury & Alvaro, VLDB 2020) infers isolation anomalies by recovering an Adya dependency graph (ww / wr / rw edges) from multi-object, multi-version transactional histories and detecting cycles (G0, G1c, G-single, G2) in linear time via Tarjan's algorithm, scaling to hundreds of thousands of transactions. Every anomaly class it discriminates is a relationship *between* transactions over objects. A single non-transactional append-only Chronicle stream has no such graph: linearizability — a real-time, per-object property — is the correct frame, not isolation.

Citations: [Elle (arXiv:2003.10554)](https://arxiv.org/abs/2003.10554), [Elle — the morning paper](https://blog.acolyer.org/2020/11/23/elle/).

### Elle's one transferable idea: recoverable / traceable values

Elle defines *recoverability* (unique written values map back to the writing op, recovering read dependencies) and *traceability* (appends accumulate the full write history inside one value). Ported to Chronicle — which is natively append-structured — this means each test client appends a frame carrying a unique `(clientId, opSeq)` tag. Any READ is then self-describing: the checker reconstructs exactly which appends a read observed, so the model's read step is *decided* rather than guessed. This is the same "fill in the unknown read value" trick Knossos uses, made deterministic.

Citations: [Elle (arXiv:2003.10554)](https://arxiv.org/abs/2003.10554), [Elle — the morning paper](https://blog.acolyer.org/2020/11/23/elle/).

### The existing lease-fence model targets a real, Jepsen-documented bug class

Jepsen's etcd 3.4.3 analysis (checked with Knossos) found two clients holding the same lock concurrently and ~18% acked-update loss with 2-second lease TTLs, and prescribed a monotonic fencing token whose lower values must be rejected. [`model_fence.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/model_fence.go) cites this exact bug and models claim/ack as a `(generation, wake_id)` register where claim rotates strictly upward and ack rejects stale tokens — the single-holder invariant Porcupine surfaces a witness for. This confirms the existing checker is well-aimed; the gap is the data plane, not the control plane.

Citations: [Jepsen: etcd 3.4.3](https://jepsen.io/analyses/etcd-3.4.3), [Horn & Kroening (register/fence checking)](https://arxiv.org/abs/1504.00204).

### Histories must use the driver's monotonic clock

Linearizability is defined over real-time precedence between invocation and response, so the checker reasons over `[Call, Return]` intervals. Those intervals must come from the *driver's* monotonic clock, never a cluster node's wall clock — otherwise a clock-skew nemesis can forge intervals and produce false witnesses (or hide real ones). [`history.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/history.go) does this correctly (host monotonic clock, Call captured before the request and Return after the response); a data-plane model must reuse this same recorder seam, not invent its own timing.

Citations: [Herlihy & Wing (real-time ordering)](https://cs.brown.edu/people/mph/HerlihyW90/p463-herlihy.pdf), [porcupine Operation fields (pkg.go.dev)](https://pkg.go.dev/github.com/anishathalye/porcupine).

## Techniques and their maturity / effort

| Technique | What it is | Maturity | Effort | Chronicle fit |
|---|---|---|---|---|
| **Data-plane Porcupine model (`streamModel()`)** | A second `porcupine.Model` alongside `leaseModel()` whose `Step` is the pure append/read/close machine from [`MemoryStore`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go): state `= (acceptedFrames, closed, tail Offset)`. APPEND-open → tail `+= len(data)`, returns offset; APPEND-closed → `ErrStreamClosed`, no change; CLOSE → idempotent set-closed, returns final offset; READ(offset) → the contiguous suffix at/after offset (a prefix of the linearized append order). Partition by stream path; reuse the recorder; check with `CheckOperationsVerbose` under a timeout. | mature | medium | **Highest-value addition.** Reuses the recorder ([`history.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/history.go)), the partition pattern (`partitionBySub` in [`model_fence.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/model_fence.go)), the nemesis harness ([`nemesis.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/nemesis.go)), and an executable oracle ([`MemoryStore`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go)). Same shape as the fence model, different state. |
| **Recoverable / traceable payloads** | Each client appends frames tagged with a unique `(clientId, opSeq)` (plus an optional running hash). A READ becomes self-describing, so the model's read step is exact. | mature | low | Cheap workload-design change that makes the read step exact and counterexamples self-explanatory. Borrowed from Elle's recoverability/traceability, *not* its transactional cycle machinery. Also lets one read detect a torn / duplicated / reordered append. |
| **Indeterminate-outcome handling (`:info`)** | A timed-out / dropped / retried op is *unknown* — possibly applied. Jepsen/Knossos model this as `:info` the checker may place anywhere after invocation; in Porcupine, encode it as an Output the `Step` accepts under both "applied" and "not-applied" linearizations. | mature | medium | [`append.lua`](https://github.com/adityavkk/chronicle/blob/main/store/redis/scripts/append.lua) already has a RETRY / re-frame path, and idempotent producers (Producer-Id/Epoch/Seq in [`producer.go`](https://github.com/adityavkk/chronicle/blob/main/store/producer.go)) exist so retries are safe. The checker must not flag a timed-out-then-retried append as a violation; modeling this validates the dedup story end-to-end under the network/partition nemeses. |
| **Bounded-search hygiene** | Linearizability checking is NP-hard in general. Practical levers: (1) minimal model state, (2) per-key partition (Herlihy–Wing locality, formalized as P-compositionality), (3) a timeout that yields `Unknown` rather than a hang. | mature | low | [`model_fence.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/model_fence.go) already practices levers 1–2 (drops the lease holder from state, partitions by sub, citing the NP-hard search). The data-plane model follows the same discipline: state `= (frames, closed, tail)`, partition by path, bound concurrency per stream. |
| **Counterexample visualization** | `CheckOperationsVerbose` returns a `LinearizationInfo` that `Visualize` / `VisualizePath` renders into a per-partition timeline, using `DescribeOperation` / `DescribeState` for labels. | mature | low | [`model_fence.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/model_fence.go) already implements `describeFenceOp` / `describeFenceState`; the data-plane model implements analogous describers (e.g. `append(c1#7, 12 bytes) -> off=…`, `read(off) -> [c1#5,c1#6]`) and wires `VisualizePath` into the failure path the way [`main.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/main.go) drives the fence scenarios. |

Citations across these techniques: [porcupine README](https://raw.githubusercontent.com/anishathalye/porcupine/master/README.md), [porcupine pkg.go.dev](https://pkg.go.dev/github.com/anishathalye/porcupine), [anishathalye/porcupine](https://github.com/anishathalye/porcupine), [Horn & Kroening (arXiv:1504.00204)](https://arxiv.org/abs/1504.00204), [Lowe (PDF)](https://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf), [jepsen.checker docs](https://jepsen-io.github.io/jepsen/jepsen.checker.html), [jepsen-io/knossos](https://github.com/jepsen-io/knossos), [Elle (arXiv:2003.10554)](https://arxiv.org/abs/2003.10554), [Elle — the morning paper](https://blog.acolyer.org/2020/11/23/elle/).

## Recommendation for Chronicle

Add a second Porcupine model to `jepsen/checker` — `streamModel()` — whose sequential specification is the [`MemoryStore`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go) append/read/close state machine, partitioned by stream path. Concretely:

- **State**: `(acceptedFrames []frame, closed bool, tail Offset)`.
- **Step rules**:
  - `APPEND` on an open stream → append, `tail += len(data)`, return the new offset;
  - `APPEND` on a closed stream → `ErrStreamClosed`, no state change;
  - `CLOSE` → idempotent set-closed, return final offset;
  - `READ(offset)` → the contiguous suffix at/after `offset`, which must be a prefix of the linearized append order.
- **Harness**: reuse the recorder in [`history.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/history.go); drive from a new scenario (`scenario_stream.go`) that runs K concurrent clients against **one** stream while the existing network / partition / clock-skew nemeses ([`nemesis.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/nemesis.go)) run; check with `CheckOperationsVerbose` under a timeout; wire `VisualizePath` into the failure path exactly as the fence scenarios do in [`main.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/main.go).
- **Workload design**: tag appended frames with a unique `(clientId, opSeq)` (Elle's recoverability/traceability, the only thing worth borrowing) so the READ step is exact and counterexamples self-describing, and model timed-out / retried appends as indeterminate outcomes so the idempotent-producer dedup path (see [`producer.go`](https://github.com/adityavkk/chronicle/blob/main/store/producer.go)) is validated rather than flagged.

This is the cleanest closure of the gap between the control-plane checker that already exists ([`model_fence.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/model_fence.go)) and the data-plane claim ("one per-stream Lua script atomically validates + appends ⇒ per-stream linearizable") that is currently only asserted.

**Do not invest in Elle itself.** A single non-transactional append-only stream has no multi-object / multi-version transactional dependency graph for Elle's cycle detection to operate on. One forward-looking exception: if cross-stream fork lineage or multi-stream transactional operations are ever added — [`store.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/store.go) already notes the fork cascade spans two cluster slots and is non-atomic — an Adya/Elle-style cycle check over fork-source/fork-child read-write dependencies could become relevant. Flag it as future work, not now.

This note's recommendation pairs with the property-based and differential-testing work tracked elsewhere in this directory; the Porcupine model and the pure-Go oracle ([`offset.go`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go) `Compare`, [`producer.go`](https://github.com/adityavkk/chronicle/blob/main/store/producer.go) `ValidateProducer`, [`cursor.go`](https://github.com/adityavkk/chronicle/blob/main/protocol/cursor.go) `GenerateResponseCursor`) share the same source of truth.

## Pitfalls

- **NP-hard search blow-up.** An over-rich model state, or a too-long / too-wide per-stream history, will explode the search. Keep state minimal `(frames, closed, tail)`, partition per path, bound concurrency, and always pass a timeout so you get `Unknown` instead of a hung CI run (the fence model already follows this discipline).
- **A green run is not a proof.** The checker validates the spec against the histories it actually observed; it does not prove the Lua script atomic (Redis serializes scripts within a slot, so single-execution linearizability is near-trivial). Design the workload and nemeses to exercise the real bug surface: Go-shell framing/reordering, oracle/Lua divergence beyond [`differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go)'s one table, and indeterminate-outcome handling.
- **Clock provenance.** Histories must use the driver's monotonic clock for Call/Return, never a cluster node's wall clock; a clock-skew nemesis on node timestamps would otherwise forge real-time intervals. [`history.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/history.go) does this right — the new model must reuse that recorder, not invent its own timing.
- **Indeterminate ≠ failure.** Timed-out or retried appends are *unknown*, not violations. If the model treats a possibly-applied append as definitely-applied (or definitely-not), it will report spurious violations under partition/network nemeses. Model these as outcomes `Step` can satisfy under either linearization, and lean on idempotent-producer dedup.
- **Reads are ambiguous without recoverable values.** If appended bytes are not uniquely tagged, the model cannot tell which appends a read observed and must guess, exploding the search and weakening counterexamples. Tag every frame (Elle's recoverability) before relying on a read step.
- **Elle is the wrong tool for the data plane.** Porting it would be wasted effort: it requires multi-object, multi-version transactional histories to build the Adya graph it detects cycles in. Reaching for Elle here conflates isolation (transactional) with linearizability (per-object, real-time).

## Open questions

1. **What is the exact READ contract under concurrency?** Must a read that observes offset N always be able to observe all bytes `< N` (a true prefix), or can the catch-up / long-poll / SSE paths legitimately return a stale-but-monotonic view? The read-step legality predicate depends on pinning this; confirm against `PROTOCOL.md` and [`read.lua`](https://github.com/adityavkk/chronicle/blob/main/store/redis/scripts/read.lua) semantics.
2. **Should the model fold in producer-idempotency outcomes** (`ProducerResultDuplicate` / `StaleEpoch` / `SeqGap`) as part of the append step, or keep the data-plane model purely about bytes/offsets/closed and leave producer fencing to the existing [`differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go) table? Folding it in raises search cost but tests the full append-precedence chain.
3. **How wide/long can a per-stream history be in CI** before P-compositionality stops helping and the per-partition search times out? Needs an empirical sweep (clients × ops/stream) to set safe bounds, mirroring how the saturation work bounded the wake path.
4. **Do any cross-stream operations** (fork copy-on-write lineage, GC-cascade, refcount) create read-write dependencies between streams that a per-stream-partitioned linearizability check would miss? [`store.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/store.go) already flags the fork cascade as non-atomic across two slots — the one place an Elle/Adya-style cross-object cycle check might eventually earn its keep.
5. **Can the data-plane model and the fence model share one combined history and nemesis run,** or must they stay separate checks? Combining them would test control-plane / data-plane interactions (e.g. a wake racing a close) but complicates partitioning.

## See also

- [./02-proof-assistants-lean.md](./02-proof-assistants-lean.md) — Lean 4 proofs of the pure cores.
- [../INVARIANTS.md](../INVARIANTS.md) — the invariant catalogue the Porcupine spec must agree with.
- [../DESIGN.md](../DESIGN.md) — overall formal-verification design and stack rationale.
