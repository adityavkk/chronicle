# Bounded 0.9 storage fixtures

Run from `rust/chronicle` in the candidate checkout:

```sh
CARGO_TARGET_DIR="$PWD/target" bash ops/upgrade-fixtures/generate.sh /path/to/baseline/rust/chronicle
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --locked --test upgrade_fixture
```

The generator builds a **separate temporary Cargo root**, imports the actual old
Chronicle `SqliteStore` and OpenRaft 0.9.25 types, and repeats the baseline root's
`[patch.crates-io]` to `vendor/openraft`. It copies the baseline lock resolution,
checks that no new dependency identity appeared, and uses offline Cargo. It
does not copy dependency sources. Baseline sources are never modified. The
second argument optionally selects an output directory for reproducibility
comparison. SQL dumps contain the old store's raw SQLite tables and blobs, not
manually composed representations of Raft objects. Generation uses no Raft
instance, sockets, or RPCs. `provenance.txt` records baseline HEAD, source and
vendor file hashes, baseline manifest/lock hashes, generator hashes, and output
hashes; the exact temporary-project manifest and resolved lock are retained.

## Independently specified oracle

All nonempty fixtures contain term 7 / leader 9 logs, recovering as node 1.
Index 0 is uniform membership `{1,2,9}`; index 1 creates bytes `base` with
incarnation 1; index 2 appends `-ack` for producer `p`, epoch 3, sequence 0.
The saved state is applied through 2 and contains exactly `base-ack` (8 bytes).
Index 3 is committed but unapplied sequence 1 (`-replay`), producing exactly
`base-ack-replay` (15 bytes). Retrying sequence 0 must return cached end 8,
duplicate true, and producer high-water sequence 1 after replay, without changing
bytes. The tests specify these outcomes rather than deriving them from the dump.

* `replay`: committed 3, applied 2, retained logs 0..4. Index 4 is uncommitted
  sequence 2 and must never enter the state machine during recovery.
* `snapshot-joint`: snapshot/applied/purged boundary 2, retained logs 3..5,
  committed 4. Index 4 is joint membership `{1,2,9}` / `{1,3,9}`; index 5 is
  the uncommitted suffix. This represents a membership operation stopped in
  joint consensus, not a completed/canceled uniform transition.
* `absent-vote`: no vote row, empty log/state.
* `zero-vote`: persisted uncommitted `(term=0,node=0)`, empty log/state.

Tests open each imported database with `open_existing`, then start `Raft::new`
with a no-network factory and disabled ticks/elections/heartbeats. Already
committed work must replay without quorum; no new commit or successful fresh
linearizable read is asserted for the quorumless joint state. Tests also install
the old snapshot, build a candidate snapshot after replay, install/reopen it,
and compare bytes, producer results, complete state, applied and membership
metadata. They verify the suffix remains in the log but not in state.

These small logical fixtures supplement actual stopped-process PVC/WAL captures;
they are not evidence of WAL crash/power-loss recovery, mixed-version RPC
compatibility, rollback, or live-cluster behavior.
