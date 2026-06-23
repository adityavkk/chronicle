# Equivalence-harness regression seeds

This directory holds committed, minimized reproducers for the MemoryStore-vs-Redis
model-based equivalence harness (`TestEquivalenceMemoryVsRedis` in
`store/redis/equivalence_test.go`, issue #26).

## How seeds are produced

When the harness finds a divergence, `pgregory.net/rapid`:

1. Shrinks the failing operation sequence to a minimal counterexample.
2. Prints a replay invocation, e.g.

   ```
   -run="TestEquivalenceMemoryVsRedis" \
     -rapid.failfile="testdata/rapid/TestEquivalenceMemoryVsRedis/<name>.fail"
   ```

   (or `-rapid.seed=<N>`), and writes the `.fail` file under
   `store/redis/testdata/rapid/` automatically.

## Committing a seed as a regression fixture

To pin a divergence so it can never silently regress:

1. Copy the auto-written `.fail` file from
   `store/redis/testdata/rapid/TestEquivalenceMemoryVsRedis/` into this
   directory with a descriptive name (e.g. `lb3-readseq-divergence.fail`).
2. Replay it explicitly:

   ```
   go test ./store/redis/ -run TestEquivalenceMemoryVsRedis \
     -rapid.failfile=testdata/redis/equivalence_seeds/<name>.fail
   ```

3. Reference the seed and the bug it pins in the harness doc comment or the
   fixing PR.

## Current status

The harness is green over the **non-JSON, single-threaded** contract at a frozen
clock. No divergence seed is committed yet: the only MemoryStore-vs-Redis
differences observed so far are the **documented expiry-cleanup-timing
asymmetry** (an expired fork SOURCE is reported `ErrStreamNotFound` by the
MemoryStore but `ErrStreamSoftDeleted` by Redis — INV-EXP-01), which is handled
in the harness as an equivalence class, not a bug (see `diffErr` /
`inaccessible` in `equivalence_test.go`).

Out-of-scope divergences expected to land seeds here when their sibling issues
generalize the generator:

- JSON-mode flatten/fork-sub-offset arithmetic (#44).

## Pinned elsewhere (no seed needed here)

- **LB-3** — `MemoryStore.readOwnMessages` compares the numeric `ByteOffset`
  alone while Redis compares the full offset string (incl. `ReadSeq`) via
  `read.lua`'s `ZRANGEBYLEX`. Armed and pinned directly as committed boundary
  fixtures in `store/readseq_divergence_test.go` (`TestReadSeqAgreesWhileZero`,
  `TestReadSeqDivergesWhenNonZero`, `TestReadSeqDivergenceCounterexampleLB3`,
  issue #32) rather than as an equivalence-harness `.fail` seed — the divergence
  is a pure read-comparison-key property and never materializes through the
  harness while `ReadSeq == 0`.
- The producer Go/Lua reply boundary at `10^14` (validate_producer's `tostring`
  of the SEQ_GAP detail fields renders `>= 10^14` in `%.14g` scientific
  notation, which `decodeScriptReply`'s `ParseInt` rejects) is pinned in
  `store/redis/differential_producer_test.go`
  (`TestDifferentialProducerReplyTostringLB`, issue #32).
