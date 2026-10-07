# Replicated Electric engine experiment

This isolated Rust binary extends Electric's actual native storage/read engine.
It does not alter Chronicle's Go/Redis default, and it is not the SQLite-backed
`rust/distributed-streams` experiment. **Not production-qualified.**

The imported source is Electric npm 0.1.5 at
[`88793e7`](https://github.com/electric-sql/electric/commit/88793e76595d69be300731b9b25c58538923a53b),
Apache-2.0. `UPSTREAM-TREE.txt` pins original blobs; `engine/LICENSE` retains the
license. OpenRaft 0.9.25 provides consensus over Electric's WAL/group-fsync path.
Committed commands materialize the native wire files; native file-range/sendfile
and epoll SSE paths serve reads. No SQLite, RocksDB or Redis payload dependency.

## Contracts and scope

* [CONTRACT.md](CONTRACT.md): partitions, quorum-fsync acknowledgements, unknown
  outcomes, linearizable/prefix/session reads, recovery and source provenance.
* [TIMED-STATE.md](TIMED-STATE.md): committed TTL/access clock and replay.
* [FORKS.md](FORKS.md): cross-partition retained-prefix grants, bounded wire import,
  hidden publication and descendant-aware release.
* [SUBSCRIPTIONS.md](SUBSCRIPTIONS.md): partition-owned durable webhook/pull-wake
  leases, cursors, fencing, retries, signing, SSRF validation and catalog repair.
* [evidence/README.md](evidence/README.md): executed gates and preserved failures.

Paths hash to fixed groups, not to the current node count. Replica movement uses
learner catch-up and joint membership; it does not repartition a live stream.
One ordered stream still has one owner group. There is no general cross-group
transaction or global read snapshot. The experimental control plane is trusted,
loopback-only; it is not a production authentication/transport design.

Only quorum-fsync writes are offered. An HTTP timeout/503 after admission can
have committed; producer epoch/sequence handles append retry effects. Reads use
`Stream-Consistency: linearizable` by default. `prefix` allows unbounded staleness;
`session` requires a same-cluster, same-partition `Stream-Session` response token.
Live streams become committed-prefix feeds after their initial read barrier.

## Reproduce locally from the repository root

```sh
cargo test --locked --manifest-path experiments/electric-replication/engine/Cargo.toml --features replication
cargo build --locked --release --manifest-path experiments/electric-replication/engine/Cargo.toml --features replication
bash experiments/electric-replication/scripts/formal.sh

pnpm --dir .tmp/electric-conformance add --save-exact @durable-streams/server-conformance-tests@0.3.5
python3 experiments/electric-replication/scripts/conformance.py experiments/electric-replication/evidence/conformance-new
python3 experiments/electric-replication/scripts/qualify.py experiments/electric-replication/evidence/fault-new
python3 experiments/electric-replication/scripts/fork_faults.py experiments/electric-replication/evidence/fork-fault-new
```

Use a fresh output directory name each time. Drivers start/stop supervised orb
services; data directories are disposable under `.tmp/electric-labs/`. They keep
per-node JSON configuration, source/binary hashes, logs and raw histories. None
creates cloud infrastructure. The full conformance suite runs unchanged with
`subscriptions:true`, three processes and two partitions, with initial leaders
co-located for its single direct HTTP endpoint. The fork fault campaign starts
the two leaders on different nodes and verifies actual learner snapshot install.

Building without `--features replication` retains standalone native mode. The
replicated binary starts with `--cluster-config <node.json>`; the harness writes
examples. It rejects old experimental data identities rather than silently
migrating them. Never start two copies of one node identity, even on different
disks; filesystem locking only fences one local directory.

## Remaining gates

Conformance and the existing fault/property tests are necessary, not sufficient.
Subscription process histories, storage-fault campaigns, matched pinned ds-bench
measurements and profiles remain required. No throughput or availability promise
is made from blog numbers or single-orb runs. Paid evaluation needs separate
authorization; no cloud budget or publication permission is implied.

Cold-tier ownership/GC, physical journal reclamation, obsolete-snapshot cleanup,
and terminal transaction-fence compaction remain unimplemented. Catalog repair
scans and subscription/fork cardinality need scale qualification. Snapshots pause
one group's apply while copying its files; the cost must be measured. Native
JSON sub-offset resolution still has upstream's unbounded scan and comma-counting
limitation and requires follow-up before claiming complete protocol qualification.
