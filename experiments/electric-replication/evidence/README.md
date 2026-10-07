# Retained qualification ledger

Counts refer to exact source/binary hashes in each run, not to an unspecified
current worktree. These are local, single-orb qualifications, not independent
disks/AZs, arbitrary power loss, production availability or cloud performance.

| Evidence | Executed result | Boundary |
| --- | --- | --- |
| `conformance-001` | 218 passed, 114 failed, zero skips | Initial real replicated failing ledger |
| `conformance-002` | 326 passed, 6 failed, zero skips | Subscriptions still absent at that revision |
| `conformance-003` | 332 passed, zero failed/skipped/todo | Three replicas, one partition, subscriptions enabled |
| `conformance-004` through `conformance-006` | 332 passed, zero failed/skipped/todo each | Three replicas, two partitions, subscriptions enabled; default linearizable/quorum-fsync |
| `fault-001` through `fault-004` | Failures retained | Includes harness corrections and a real snapshot-install/purge ordering failure in 004 |
| `fault-005` | Independent checker PASS, 128 operations | Majority/minority, delayed Raft RPCs, leader SIGKILL, learner snapshot/membership, full restart; append-prefix/dedup/session model |
| `fork-fault-001` | Independent checker PASS, 744 operations | Different group leaders, grant-before-import pause, SIGKILL, confirmed learner snapshot installation, joint membership, full restart, descendant collection/recreation |
| `fault-006`, `fork-fault-002` | Independent checkers PASS, 129 and 745 operations | Same campaigns after the network-backoff correction |
| `formal/` | Safety and stable-period liveness pass; 11 negative mutations detected; Lean without `sorry` | Assumes Raft and honest storage; not Rust refinement proofs |
| `properties/forks-001-fixture-too-small.txt` | Failed and preserved | New larger test records exceeded the test's 4 KiB segment; fixture corrected to 256 KiB, not reduced payload coverage |
| `properties/rust-125.txt` | 125 passed, zero failed, 2 ignored | Ignored helpers are unchanged upstream forensic dump/replay entry points, not skipped conformance tests |

The upstream suite is **unchanged `@durable-streams/server-conformance-tests@0.3.5`**,
with `subscriptions:true`. All 332 tests are discovered and executed in each full
run. No assertions were weakened and no filters or custom skips added. Later
ledgers also hash every suite JavaScript file, not only its entry point.

`fork-fault-001` rejects two deliberately corrupted histories: truncated inherited
bytes and false absence after a durable grant. Its checker reconstructs bytes
from client requests. It covers an explicitly ordered binary lifecycle fixture,
not all concurrent lifecycle histories. Unknown HTTP outcomes remain in the raw
history rather than being relabeled aborted.

Logs exposed a further defect even when data checking passed: the adapter mapped
connection refusals and known partitions to OpenRaft `NetworkError` (immediate
retry), generating a log/CPU storm. The adapter now follows the pinned upstream
[reqwest example](https://github.com/databendlabs/openraft/blob/8815cdba2826f74e848acef361ad03f93bb1c3f8/examples/raft-kv-memstore/src/network/raft_network_impl.rs#L45-L59):
connect failures/known partitions return `Unreachable`, enabling its default
500 ms backoff. A real refused-TCP regression test covers the mapping. The full
suite and both process campaigns pass on the corrected revision. Raw node logs
for the fork campaign shrink from about 56 MiB to 48 KiB without log filtering;
this is a scheduling/retry correction, not a throughput benchmark.

Server logs are preserved losslessly as `.log.gz`; raw copies stay local and are
Git-ignored. `server-log-sha256.txt` records both forms. Decompress to inspect the
original warnings/errors; compression is not log filtering. Large failure logs
are evidence of the retry bug, not successful performance results. Newer runs
write their own `log-sha256.json` manifest automatically when stopping services.
