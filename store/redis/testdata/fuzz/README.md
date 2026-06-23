# FuzzStoreEquivalence seed corpus

The committed corpus files live in `FuzzStoreEquivalence/` next to this note.
This README sits one level up on purpose: Go fails to load the fuzz target if
any non-corpus file (like a README) sits inside the corpus directory itself.

`FuzzStoreEquivalence/` is the committed seed / regression corpus for
`FuzzStoreEquivalence` (issue #42) — the coverage-guided fuzz target that wraps
the existing MemoryStore-vs-Redis rapid state machine
(`runEquivalenceModel` / `chronicleModel` in
`store/redis/equivalence_test.go`, issue #26) with `rapid.MakeFuzz`. The target
reuses that model verbatim: no new model, actions, or invariants. Go's
coverage-guided mutation steers the input bitstream toward the rare Lua branches
uniform random under-samples.

## Persisted-seed-format decision (resolves research/03 open question #4)

We standardize on the **Go `go test` fuzz corpus file format** as the single
persisted regression-fixture format, layered on rapid's seed-replay discipline:

- Every file here is a standard `go test fuzz v1` corpus entry: a header line
  followed by one `[]byte("…")` value line. The bytes ARE the rapid bitstream:
  `rapid.MakeFuzz` packs the fuzz input into the same little-endian `uint64`
  draw stream `rapid.Check` feeds the engine, so each file is a fully
  deterministic, replayable op sequence.
- These files replay automatically on every ordinary `go test ./store/redis/`
  (Go runs corpus files deterministically when NO `-fuzz` flag is given), so the
  PR gate inherits every fixture as a fast regression test for free.
- When the nightly fuzzer finds a divergence or panic, `go test -fuzz` writes the
  failing input here in this same format, and rapid prints a minimal, replayable
  command sequence plus a deterministic seed. The crashing file is then
  minimized and committed — the corpus only grows, and the nightly run is what
  grows it.

This is rapid's documented seed-replay model PLUS the `go fuzz` corpus files,
chosen over a bespoke serialization because it needs zero custom tooling: `go
test` is both the fuzzer and the replayer.

## Nightly vs PR-gate split

- **PR gate** (every push / PR, `.github/workflows/ci.yml` `test` job): the fast
  deterministic `rapid.Check` property run (`TestEquivalenceMemoryVsRedis`) plus
  this corpus replayed via `go test ./...` (no `-fuzz`). No coverage-guided
  budget on the critical path.
- **Nightly** (`.github/workflows/fuzz-nightly.yml`, cron + manual dispatch):
  the long coverage-guided run, `go test -fuzz=FuzzStoreEquivalence
  -fuzztime=<budget> -parallel=1` against a containerized Redis 8. It fails on
  any new crasher / divergence and uploads newly-discovered corpus entries as an
  artifact.

A CI failure MUST print a minimal replayable command sequence and a
deterministic seed; rapid's integrated shrinking provides both automatically.

## Fixtures in this corpus

Hand-named, self-documenting fixtures (verified to reach each of the four named
rare branches the issue calls out — each was harvested by replaying inputs
through the model and confirming the branch fires):

- `branch-epoch-bump-at-nonzero-seq` — reaches `store.ErrInvalidEpochSeq`
  (producer.go: epoch bump must start at seq 0). [INV-PROD-08]
- `branch-seq-gap-at-boundary` — reaches `store.ErrProducerSeqGap`
  (producer.go: gap at `lastSeq + 1`). [INV-PROD-08]
- `branch-close-by-producer-duplicate` — reaches
  `store.ProducerResultDuplicate` (idempotent duplicate producer op).
  [INV-FENCE-03]
- `branch-fork-suboffset-overshoot` — reaches `store.ErrInvalidForkSubOffset`
  (`resolveForkSubOffset` overshoot). [INV-CFG-01]

Regression fixtures pinning a defect the fuzzer surfaced and we fixed:

- `regression-no-valid-action-empty-paths` — a degenerate bitstream (a long run
  of one byte) that drew only path-requiring actions, all of which `t.Skip()`
  when no stream exists, so rapid's 100-try `executeAction` budget exhausted and
  the property aborted with "can't find a valid (non-skipped) action". This was a
  fuzz-harness robustness gap (NOT a backend divergence): `rapid.Check`'s uniform
  draw reached `Create` early so it never bit, but a coverage-guided minimized
  input exposed it. Fixed by bootstrapping one baseline stream in
  `runEquivalenceModel` so at least one action is always applicable from step 1.
  This fixture pins that the input now passes.

The remaining hex-named files are coverage-discovered inputs the fuzzer wrote
while growing branch coverage; they are kept as additional regression fixtures.

## Committing a new divergence as a fixture

1. Take the failing input `go test -fuzz` wrote here (hex-named) and the
   minimal replayable sequence + seed rapid printed.
2. Replay it explicitly to confirm it still fails before the fix and passes
   after:

   ```
   go test -run='FuzzStoreEquivalence/<name>' -count=1 ./store/redis/
   ```

3. Give it a descriptive name (`branch-…` or `regression-…`) and commit it,
   referencing the bug it pins.
