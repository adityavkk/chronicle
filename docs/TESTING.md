# Testing and verification catalog

This document lists every kind of test and proof Chronicle runs, what each one
gives you, where it lives, and how to run it. It also states, plainly, what is
proven, what is only sampled, and what cannot be proven at all.

The shape of the suite follows one rule: split each component into a pure
deterministic core and an imperative shell that does input and output, then test
each part at the cheapest level that is still faithful. The cores are tested by
proofs and by properties that hold for every input. The shell is tested against a
real Redis. Properties that only appear across processes, such as a failover or a
race between two workers, are tested with fault injection and linearizability
checks. The wire protocol is checked from outside the code by a black-box suite.

## The levels at a glance

| Level | What runs | What it gives you | Infrastructure |
| --- | --- | --- | --- |
| Pure core | unit tables, Lean proofs, the cursor and fence model checkers | per-input correctness, and for the proofs, correctness for every input | none |
| Differential | the same input through Go, Lua, and the compiled Lean model | the three implementations agree on every input | real Redis |
| Property and model based | `rapid` generates random operation sequences against two backends | the backends agree across reachable states, with a shrunk counterexample on failure | real Redis |
| Fuzzing | Go's coverage-guided fuzzer drives the same model | reaches rare branches that random sampling misses | real Redis |
| Linearizability | `porcupine` checks recorded concurrent histories | the concurrent history has a valid sequential order | real Redis |
| Concurrent protocol | TLA+ model checking, Apalache induction, Alloy | safety and liveness over all interleavings, and for Apalache, over all sizes | Java |
| Fault injection | a `k3d` cluster and a Redis failover nemesis | end to end safety under crashes and failover | k3d or local Docker |
| Wire contract | the vendored conformance suite | the HTTP surface matches the protocol | a live server |
| Scale | the load generator and the GKE rig | throughput and latency under load | managed Redis |

## The kinds of tests

### 1. Unit tests on the pure cores

Table-driven Go tests on the pure functions: `ValidateProducer` (the idempotent
producer state machine), `Offset.Compare` and the offset codec, `GenerateResponseCursor`,
and the webhook reducers such as `FenceDecision` and `MergeAcks`. They run in
milliseconds with no Redis and no clock, because the clock is passed in.

They give you correctness on the specific cases you wrote. The downside is that
they only check the cases you wrote, so they miss the inputs you did not think of.
That gap is closed by the proofs and the property tests below.

Run: `make test-unit`.

### 2. Integration tests against a real Redis

The `store/redis` and `webhook` packages run against a live Redis 8, each on its
own database, flushing once at start, and skipping cleanly when Redis is not
reachable or under `-short`. These exercise the Lua scripts and the full
read-write round trip that the in-memory store cannot.

The downside is that they need Redis, so they do not run in a bare unit pass.

Run: `make test` (needs Redis; `make redis-up` first).

### 3. Differential tests

The same input is sent through three independent implementations and the results
are required to match: the pure Go function (the oracle), the Lua script Redis
runs (the mirror), and the Lean model compiled to C (the proof, see kind 9). The
producer state machine, the fence and slot predicates, the content-type and
config normalizers, the `Stream-Seq` comparison, and the JSON flatten path each
have a differential test.

This gives you confidence that the three copies of a rule have not drifted apart.
A differential test found the `10^14` producer-reply bug (issue #47): the Lua
reply rendered large sequence numbers in scientific notation, which the Go reply
parser rejected. The downside is that a differential test only catches a
disagreement; if all three copies share the same wrong assumption, they agree and
the test passes. That residual gap is covered by the independent properties and
the proofs.

Where: `store/redis/differential_*.go`, `webhook/predicate_differential_test.go`,
`store/leanoracle` (the compiled-Lean oracle, behind the `leanoracle` build tag).

### 4. Property-based and model-based tests

The `rapid` library generates random, shrinking sequences of operations and runs
each operation against both the in-memory `MemoryStore`, which is the reference
model, and the live Redis backend, which is the subject. After every step the two
must agree on the result, the error, the tail offset, the metadata, and the bytes
a read returns. A single fake clock drives both, so expiry is reproducible.

This gives you agreement across many reachable states rather than a fixed list,
and when it fails it shrinks the failure to the smallest sequence that still
breaks it. The downside is that it samples the space rather than covering it, and
both backends call the same pure cores, so a bug in a shared core agrees in both.
Independent metamorphic checks and the proofs cover that.

Where: `store/redis/equivalence_test.go`, `store/redis/differential_producer_test.go`,
`store/offset_property_test.go`.

### 5. Coverage-guided fuzzing

The same `rapid` model is wrapped as a native Go fuzz target with `rapid.MakeFuzz`,
so Go's coverage-guided fuzzer steers the generated sequences toward rare branches
that uniform random sampling under-reaches, such as an epoch bump at a nonzero
sequence or a fork sub-offset overshoot. A committed corpus of seeds, including
four branch-verified fixtures, runs on every pull request, and a nightly job runs
the fuzzer for a fixed time.

The downside is that fuzzing finds crashers and divergences but proves nothing; it
is a search, not a guarantee.

Where: `store/redis/equivalence_fuzz_test.go`, the corpus under
`store/redis/testdata/fuzz/`, and `.github/workflows/fuzz-nightly.yml`.

### 6. Linearizability checking with Porcupine

`porcupine` takes a recorded concurrent history and a sequential model and decides
whether the history has a valid sequential order. Chronicle uses it for the lease
fence (`model_fence.go`), the owner-epoch slot fence (`model_shard.go`), the
stream data plane of append, read, and close (`model_store.go`), and a composed
model that carries both fences at once.

This gives you a guarantee that concurrent operations behaved as if they ran one
at a time, which is the correctness condition Herlihy and Wing defined. A torn
read, a lost append, or a reordering has no valid order and the checker prints the
witness. The downside is that the search is NP-hard, so it runs over bounded
histories and a small number of workers, and a too-large history can return
Unknown, which is counted as a failure.

Where: `jepsen/checker/model_*.go` and the `*-linz` scenarios; run via the
`jepsen-checker` binary.

### 7. Fault injection and the failover nemesis

A nemesis crashes origin pods and Redis on a `k3d` cluster while load runs, and a
Redis failover nemesis kills the primary and promotes a replica. The checks assert
that every subscription cursor reaches its stream tail within a settle window and
that a deposed worker's late acknowledgement is fenced. The failover check asserts
the load-bearing safety property: an acknowledged write that is lost on failover
degrades only to at-least-once delivery, deduplicated by the consumer's monotone
offset, and never to a safety violation.

This gives you end-to-end evidence under real crashes. The downside is that the
full multi-node run under load needs a managed cluster, so the local run covers
single-node failover and the multi-node verdict is pending a cloud run.

Where: `jepsen/checker/nemesis.go`, `jepsen/checker/scenario_failover.go`,
`jepsen/`.

### 8. Black-box conformance

A vendored suite of about 330 tests, with `fast-check` property fuzzing, drives a
live server over HTTP and checks it against the Durable Streams protocol:
idempotent producers, closure, forks, JSON mode, SSE, and the subscription APIs.

This gives you protocol compliance independent of the implementation. The downside
is that it sees only the HTTP surface, not the internal state.

Run: `make conformance` (needs Redis and a built server).

### 9. Machine-checked proofs in Lean 4

The pure cores are transcribed into Lean and proven correct for every input,
checked by Lean's kernel. The proofs cover the producer state machine (totality,
determinism, epoch monotonicity, and idempotency), the offset order (a strict
total order and the lexical-equals-numeric property below `10^16`), cursor
progression, and a parametric lemma that a monotone token implies a single holder,
instantiated for both the generation fence and the owner-epoch fence. The proofs
rest only on the standard axioms `propext`, `Classical.choice`, and `Quot.sound`,
with no `sorry` and no `native_decide`.

The proof is tied to the running code in two ways. The same `Stream-Seq` and
producer rules are checked by the differential tests, and the Lean model is
compiled to C and run as the third differential oracle, so the proof and the Go
code are pinned to one statement. The downside is that a proof covers the Lean
model, not the Go directly; the differential oracle is what closes that gap.

Where: `lean/`. Run: `cd lean && lake build`.

### 10. Model checking in TLA+

The concurrent subscription protocol is specified in TLA+ and checked by TLC over
every interleaving of workers, crashes, and lease expiries, up to a bounded size.
The specs cover the wake, claim, acknowledge, and release fence, the owner-epoch
layering, and liveness. The safety properties checked are single-holder,
generation monotonicity, stale-operation inertness, the soundness of treating a
fenced operation as a no-op, and forward-only cursors. The liveness properties are
that pending work eventually issues a wake and that an expired lease is eventually
reclaimed.

Trace validation binds the spec to the code: the running engine emits a log of its
fence transitions, and TLC checks that each real trace is a behavior the spec
allows. A tampered trace is rejected, which shows the check is not vacuous. The
downside is that TLC explores a bounded number of workers, so a passing run is
evidence up to that size, not a proof for all sizes. Apalache removes that caveat
for the central property.

Where: `formal/tla/`. Run: `cd formal/tla && make tlc`, `make trace-validate`.

### 11. Inductive proof with Apalache

Apalache proves single-holder as an inductive invariant by symbolic reasoning
rather than enumeration: it shows the invariant holds in the initial state and is
preserved by every action. This establishes single-holder for all sizes, not just
the bounded number of workers TLC explores. A negative control confirms that an
unsound lease expiry breaks the invariant, so the proof is not vacuous.

The downside is that finding the inductive strengthening is manual work and the
proof is scoped to the fence core, not the whole engine.

Where: `formal/tla/FenceCore.tla`. Run: `cd formal/tla && make apalache`.

### 12. Bounded relational checking with Alloy

Alloy checks relational invariants over every configuration up to a scope: the
fan-out index equals the projection of the canonical links, so reconciliation
never invents membership, and the slot-homing scatter set equals the reference
set, so there is no cross-subscriber leakage. The slot-homing model also shows,
formally, that never clearing the occupied-slots bit on de-index is a correctness
requirement and not just an optimization.

The downside is that Alloy is exhaustive only up to the scope it is given.

Where: `formal/alloy/`.

### 13. Load, stress, and scale

The load generator and the GKE rig measure throughput and latency, and sweep the
subscription recovery sweep to tens of thousands of subscriptions on managed Redis.

The downside is cost: the rig runs on cloud infrastructure and must be torn down
after each run.

Where: `loadgen/`, `loadtest/`.

## What is proven, what is sampled, what cannot be proven

The line is set by what Redis guarantees.

- Proven or checked exhaustively: the pure-core function properties (Lean), the
  single-holder fence for all sizes (Apalache), per-stream linearizability of the
  data plane (Porcupine, because a single Lua script over one hash-tag slot is the
  one commit point), generation monotonicity, and forward-only cursors.
- Sampled: the property and fuzz tests cover many reachable states but not all of
  them, and the TLA+ runs cover a bounded number of workers.
- Not provable: durability across a failover. Redis replication is asynchronous,
  and `WAIT` and `WAITAOF` do not make it strongly consistent, so an acknowledged
  write can be lost on a primary failure. This is documented as a durability-honest
  property and tested with the failover nemesis, not proven. The design is correct
  because durability is separated from safety: a lost write degrades to
  at-least-once delivery, never to a safety violation, since safety rests only on
  the monotone fence.

## Bugs the formal work found

The property and differential tests found real defects that example tests had
missed. Each is tracked with the `found-by-formal-methods` label, naming the
method and the intended fix.

- Issue #47: a producer sequence gap with a value at or above `10^14` makes the
  Redis backend error while parsing its own reply, instead of returning a clean
  409. Found by the producer differential property. Client reachable.
- Issue #46: the offset string format uses a minimum width, so lexical order
  inverts against numeric order at or above `10^16`. Found by the offset property.
  The fix is a format migration, so it is deferred.
- Issue #48: the in-memory read compares only the byte offset while Redis compares
  the full offset string, so they will diverge once log rotation makes the read
  sequence nonzero. Found by the property test and armed with a failing test that
  fires when the code changes.

## How to run everything

```bash
make test-unit                 # pure cores, no infrastructure
make redis-up && make test     # unit and integration against Redis
make conformance               # the black-box protocol suite
cd lean && lake build          # the Lean proofs
cd formal/tla && make tlc      # the TLA+ model checks
cd formal/tla && make apalache # the Apalache inductive proof
cd formal/tla && make trace-validate   # trace validation against the spec
go test -tags leanoracle ./store/redis/   # the triple oracle, incl. the Lean model
```

The design and its tradeoffs are in [docs/PLAN.md](PLAN.md); the formal-methods
strategy, the invariant catalog, and the prior-art research are under
[docs/specs/formal-verification/](specs/formal-verification/README.md).
