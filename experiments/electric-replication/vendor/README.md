# Pinned OpenRaft admission backport

`openraft/` is the packaged **0.9.25** crate, source
[`8815cdb`](https://github.com/databendlabs/openraft/commit/8815cdba2826f74e848acef361ad03f93bb1c3f8),
crate SHA-256 `a97014fb78acb77be3a40ac2da305f6dd3a6b243f3a908ace87d29b3972eaafd`.
The normalized Cargo manifest keeps registry dependencies; no build depends on
an external checkout or a modified Cargo registry cache. MIT and Apache-2.0
licenses are retained from that source. `OPENRAFT-TREE.sha256` records the
unmodified import (including both licenses) before local changes.

Only three production files differ: `src/raft/mod.rs`,
`src/core/raft_msg/mod.rs`, and `src/core/raft_core.rs`. They backport the expected
leader condition from upstream
[`f13f0ee`](https://github.com/databendlabs/openraft/commit/f13f0eec7922e84ad9a3beeb166bb8a8133805ad)
(`WriteRequest::with_leader`, subsequently released in 0.10 alpha14). The local
API is `client_write_ff_with_leader(data, CommittedLeaderId)` because 0.9.25 does
not have the newer write builder. The condition is checked inside RaftCore
immediately before log assignment. A mismatch returns the existing
`ClientWriteError::ForwardToLeader`; no index, WAL frame or durability receipt
is produced. Ordinary writes retain their existing behavior.

Election, log matching, membership, commitment, storage and acknowledgement rules
are unchanged. This is not a private consensus implementation or an upgrade to
the previous SQLite experiment's prerelease. The adapter needs the condition to
fence queued writes against its per-leadership-epoch recovery/admission boundary;
an ingress-only check races the internal API queue. `AdmissionEpoch.tla` was
extended and checked before this backport; native-Journal integration tests cover
matching/stale leaders and actual leadership transitions. Those tests do not prove
the whole consensus library correct.
