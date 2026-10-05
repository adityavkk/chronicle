# Isolated OpenRaft alpha36 qualification — not baseline promotion

The stable `chronicle-rust` k3d cluster still runs OpenRaft 0.9.25/resources2.
The candidate lives in the `rust/transfer-qualification` worktree and a separate
`chronicle-upgrade` k3d cluster/Docker bridge. Both use k3d 5.8.3 and
k3s 1.32.5. No candidate identity was started on the baseline network.

OpenRaft and its upstream legacy chunked-snapshot adapter are pinned to
0.10.0-alpha.36. They are MIT OR Apache-2.0. Registry checksums are in Cargo.lock;
the exact upstream revision/checksum and model-to-code mapping are in
[`formal/UPGRADE.md`](../formal/UPGRADE.md). The old vendored private membership
patch remains available as baseline provenance but is **not a candidate dependency**.
The candidate uses upstream committed-leader/effective-membership preconditions.
This is a prerelease; no mixed-version, rolling-upgrade or downgrade claim is made.

## Executed evidence

* Before adapter changes: the complete formal suite passed. `UpgradeStorage`
  explored 460 states; four independent negative mutations detect early flush,
  early application reply, inclusive truncation and split snapshot publication.
  Lean proves exclusive truncation retains every committed entry below its valid
  boundary, including the distinction between `Some(0)` and `None`.
* `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 make check` passed: fmt, Clippy,
  Rust and upstream storage suites, SQLite VFS faults, docs and 37 Python tests.
  [Output](upgrade-alpha36/check.txt).
* Real Raft apply responders remain pending at the pre-transaction-commit and
  post-commit gates. Releasing returns the exact outcome; kill/reopen preserves
  old acknowledged data and distinguishes the two transaction boundaries.
  Whole-node fail-stop also passes for log, pre-apply-commit and post-apply-commit
  errors in either of two groups, despite an unrelated blocked response reader.
  [Responder output](upgrade-alpha36/responders.txt), [fail-stop output](upgrade-alpha36/fatal.txt).
* Four small fixtures were emitted by the **actual baseline 0.9 store**. The
  generator and source/dependency hashes accompany the SQL dumps. Independent
  regeneration was byte-identical. Candidate `open_existing` and `Raft::new`
  recover committed-but-unapplied entries without applying an uncommitted
  suffix, snapshot/purge/tail state, joint membership, a remote advanced leader
  ID, absent votes and persisted `(0,0)` votes. Old and new snapshot installation
  and reopen preserve metadata and producer results. These tests use no network
  and do not fabricate a quorum or claim successful fresh reads from joint state.
  See [`upgrade_fixture.rs`](../tests/upgrade_fixture.rs) and
  [`generate.sh`](../ops/upgrade-fixtures/generate.sh).
* Five real stopped-process PVC/WAL copies recovered in the candidate k3d cluster
  with zero pod restarts. Strict reads matched all 9,600 historical records
  byte-for-byte. Eight sequence-0 retries returned the original saved frontiers
  without modifying bytes; four new appends and duplicate retries succeeded.
  [Recovery evidence](upgrade-alpha36/recovery.jsonl).
* The unchanged pinned full conformance suite passed **326/0/6 upstream-default
  skips**, 68.24 seconds, on that recovered candidate.
  [Full output](upgrade-alpha36/conformance.txt), [JSON](upgrade-alpha36/conformance.json).
* Real SQLite/HTTP Raft tests establish directed successor election followed by
  quorum readiness, strict read and replicated write, with exactly one
  delivery to the designated target. One trigger also broadcasts to the other
  effective voters; this is not a claim of one total RPC. Actual recipients reject
  stale votes and unavailable flush boundaries.
  Heartbeat-enabled failure tests separately measure aggregate traffic and
  in-flight requests; the 500 ms replication-retry floor does not apply to
  independent heartbeats. [Network output](upgrade-alpha36/network.txt).
* A three-voter dead-target test with production 200/800/1600 ms timing and
  continuous source ReadIndex probes recovered a strict read plus a replicated
  write on the surviving voter in 2.552 seconds. With automatic elections disabled,
  the same transfer did not recover in four seconds. The target core was stopped,
  so it could not campaign through outgoing RPCs. This is one measured recovery,
  not an outage bound. [Positive/negative output](upgrade-alpha36/dead-target-first.txt).

The tested image is `chronicle-raft:alpha36-qualify`, Docker ID
`sha256:29b335d9c198bcfb2766f5af787bbecfa661a55660325ad2ba8743b54519d505`;
binary SHA256
`465edfe8105c7cea4e88cb8647be40d66ed9b45d5dbfcc0fcc071e203a7bc725`.
The image preceded additional test-only fixtures/network cases. Retained
[pod metadata](upgrade-alpha36/pods.json) records containerd identities separately.
The baseline capture is immutable and checksummed, not an assertion of clean
shutdown. `ops/qualify-upgrade.py CAPTURE IMAGE` restores only into a new isolated
cluster and refuses an existing destination. Its first execution copied the
private conformance service separately; the script now includes that service.
`tests/upgrade_recovery.py` verifies histories and intentionally appends one new
record per stream, so it must run only against the isolated copy.

## Review findings and remaining gate

Oracle found no concrete adapter regression. Its requested real-responder
failure tests, old-serializer edge fixtures and heartbeat-enabled tests were
added. The single-implementation membership extension trait was replaced with
a private function. Direct application helpers retain outcome assertions but
are not substitutes for real-responder tests.

The first transfer test incorrectly equated `Leader` state with write readiness
and encountered `LeaseExpired`; it now observes quorum contact and performs a
strict read/write. An unavailable transfer endpoint returns submission success
but can leave the source write-fenced. There is no source rollback timer: another
voter must elect a successor. A 700 ms elections-disabled observation is not
evidence of eventual recovery. The upstream trigger also does not reject a
nonvoter target and has no expected-vote admission fence.

Subsequent [leadership qualification](LEADERSHIP.md), [membership overlap and
restoration](RETIRED-ROUTING.md), [memory recovery](SNAPSHOT-MEMORY.md) and independent
integration review qualify the local source candidate. The earlier 0.9 deployment
and immutable captures remain separate and unchanged. Elective leadership and
native campaigns remain default-off; integrated executor restart after a consumed
claim is still required before considering default-on elective policy.
Neither these tests nor the bounded model prove end-to-end durability under
power loss, arbitrary scheduling, filesystem dishonesty or independent AZ loss.
