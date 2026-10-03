# OpenRaft 0.9.25: bounded membership admission patch

## Origin and licensing

- Published source: <https://static.crates.io/crates/openraft/openraft-0.9.25.crate>
- Archive SHA-256 (the original Chronicle `Cargo.lock` registry checksum):
  `a97014fb78acb77be3a40ac2da305f6dd3a6b243f3a908ace87d29b3972eaafd`.
- `.cargo_vcs_info.json`: revision `8815cdba2826f74e848acef361ad03f93bb1c3f8`,
  path `openraft`, upstream tag `v0.9.25` in
  <https://github.com/datafuselabs/openraft>.
- The archive's entire `src/` was compared byte-for-byte with the authoritative
  upstream clone at that revision before patching; no differences.
- Every published file is retained, including the **normalized** `Cargo.toml`,
  `Cargo.toml.orig`, upstream `Cargo.lock`, README and VCS metadata. No workspace
  members or fixture crates were imported. The published archive omits licenses;
  `LICENSE-MIT` and `LICENSE-APACHE` are exact copies from the upstream revision.
- Attribution remains in the manifest: Databend Authors
  `<opensource@datafuselabs.com>` and Anthony Dodd
  `<Dodd.AnthonyJosiah@gmail.com>`. Upstream license: **MIT OR Apache-2.0**,
  compatible with Chronicle's Apache-2.0. Neither upstream license was rewritten.

License SHA-256:

```text
23f18e03dc49df91622fe2a76176497404e46ced8a715d9d2b67a7446571cca3  LICENSE-MIT
a60eea817514531668d7e00765731449fe14d059d3249e0bc93b36de45f759f2  LICENSE-APACHE
```

## Reproduce pristine comparison

From this directory (or supply a cached crate archive to the script):

```sh
archive=$(mktemp)
curl --fail --location https://static.crates.io/crates/openraft/openraft-0.9.25.crate -o "$archive"
python3 verify-source.py "$archive" --diff
rm "$archive"
```

The script verifies the archive checksum, every retained published file, the exact
changed-file/addition inventory, VCS metadata and both license hashes. `--diff`
prints the small patch against pristine sources. It also prints a reproducible
pristine-tree digest: SHA-256 over sorted archive paths (prefix stripped), each
followed by NUL and that file's binary SHA-256. Build `target/` is excluded.
Verified pristine-tree digest (246 published files):
`4099ff3c0d22d7b48ae7b02998f087d049d303b3cc1689410903baef7cf7e7cc`.

## Patch inventory

| File | Change |
| --- | --- |
| `src/raft/impl_raft_blocking_write.rs` | Public `change_membership_if_vote(members, retain, expected_vote)`; shared private two-phase implementation; unchanged original vote in both messages. Old API and `add_learner` send `None`. |
| `src/core/raft_msg/mod.rs` | Optional expected complete `Vote` on `ChangeMembership`. |
| `src/core/raft_core.rs` | Reject mismatch with existing typed `ClientWriteError::ForwardToLeader` before membership derivation/append, with no intervening await; forward optional vote from message. Existing membership/leadership checks untouched. |
| `src/raft/mod.rs` | Register dependency-local admission test module. |
| `src/raft/membership_admission_test.rs` (new) | Deterministic public API/message/real-core harness, explicitly completing accepted proposals; IO interfaces panic if called. |
| `LICENSE-MIT`, `LICENSE-APACHE`, this file, `verify-source.py` (new) | Licenses, provenance, comparison tooling. |

Outside this directory, only Chronicle's `Cargo.toml` patch table and `Cargo.lock`
OpenRaft source/checksum removal are needed. No dependency versions change.
This is not a consensus algorithm change or OpenRaft upgrade.

## Contract and integration

This is an **admission fence**, not revocation of an accepted entry. Vote equality
includes leader identity and the committed flag, not merely term. The original
vote is never refreshed between the joint and uniform phases. If phase two is
rejected, the accepted joint configuration remains. Cancellation does not retract
already queued messages. On rejection, callers must reread placement intent,
not refresh the vote and retry a previously captured target. `ForwardToLeader`
can point to the same node after reelection; do not blindly follow/retry it.

The controller is deliberately out of scope. Its intended integration uses fenced
`ChangeMembers::AddNodes`, explicit replication catch-up, fenced promotion/pruning,
and an awaited membership barrier for new-placement completion. The local
formal-first prerequisite was supplied as commit `7e4e5a7` (28 model states, with
negative vote/barrier mutations failing); this dependency unit does not alter or
rerun the model.

## Verification

From the repository root:

```sh
cargo check --manifest-path rust/chronicle/Cargo.toml --locked
cargo build --manifest-path rust/chronicle/Cargo.toml --locked
cargo test --manifest-path rust/chronicle/vendor/openraft/Cargo.toml \
  --locked --features serde,storage-v2 --lib membership_admission
cargo test --manifest-path rust/chronicle/vendor/openraft/Cargo.toml \
  --locked --features serde,storage-v2
```

The six admission tests cover old-vote rejection after same-node reelection,
complete vote equality (different node and committed flag), phase-one success
then a term change before phase two, unchanged unfenced two-phase behavior,
unchanged `add_learner`, and fenced two-phase success. Rejection checks compare
engine state and assert no emitted commands or registered write responders.
The harness controls message processing and completion; it does not sleep or
run a cluster. It models commit completion, not storage durability or replication.

Results: Chronicle check and build passed; all **196** dependency unit tests passed;
**51** upstream doctests are marked ignored (none failed). No extra fixture
crates were required. Targeted admission tests: **6 passed**. Negative source
mutations were run and reverted: disabling the gate failed all three rejection
tests; dropping the phase-two expected vote failed the phase-two test; refreshing
it from current metrics likewise failed that test. The test fixture targets
`storage-v2` with the normal snapshot type (Chronicle's feature selection).
