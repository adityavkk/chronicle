# Retained qualification ledger

Counts refer to exact source/binary hashes in each run, not to an unspecified
current worktree. These are local, single-orb qualifications, not independent
disks/AZs, arbitrary power loss, production availability or cloud performance.

| Evidence | Executed result | Boundary |
| --- | --- | --- |
| `conformance-001` | 218 passed, 114 failed, zero skips | Initial real replicated failing ledger |
| `conformance-002` | 326 passed, 6 failed, zero skips | Subscriptions still absent at that revision |
| `conformance-003` | 332 passed, zero failed/skipped/todo | Three replicas, one partition, subscriptions enabled |
| `conformance-004` through `conformance-007` | 332 passed, zero failed/skipped/todo each | Three replicas, two partitions, subscriptions enabled; default linearizable/quorum-fsync |
| `fault-001` through `fault-004` | Failures retained | Includes harness corrections and a real snapshot-install/purge ordering failure in 004 |
| `fault-005` | Independent checker PASS, 128 operations | Majority/minority, delayed Raft RPCs, leader SIGKILL, learner snapshot/membership, full restart; append-prefix/dedup/session model |
| `fork-fault-001` | Independent checker PASS, 744 operations | Different group leaders, grant-before-import pause, SIGKILL, confirmed learner snapshot installation, joint membership, full restart, descendant collection/recreation |
| `fault-006`, `fork-fault-002` | Independent checkers PASS, 129 and 745 operations | Same campaigns after the network-backoff correction |
| `subscription-fault-001` | Independent checker PASS, 187 operations, 1 unknown outcome | Claim races, stale workers, failover during delivery/ack, durable retries, dropped wake, invalid-batch atomicity, recreation, explicit/glob links, full restart, SSRF/redirect rejection; 9 independently verified Ed25519 deliveries |
| `storage-fault-001`, `storage-fault-002` | Harness failures retained | First omitted the hot-file `write` hook; second lost its disposable hot fixture during rejected snapshot startup. Neither established a false server acknowledgement |
| `storage-fault-003` | Independent checker PASS, 28 operations, 3 unknown outcomes | Real short writes, WAL fsync EIO/ENOSPC, hot-file ENOSPC, snapshot fsync EIO, corrupt authoritative frames/snapshot, hot-file rebuild; seven intercepted failing/short syscalls |
| `formal/` | Safety and stable-period liveness pass; 21 negative mutations detected; Lean without `sorry` | Includes delayed catalog/incarnation/link observations, private apply recovery and proposed journal reclamation; assumes Raft and honest storage; not Rust refinement proofs |
| `properties/forks-001-fixture-too-small.txt` | Failed and preserved | New larger test records exceeded the test's 4 KiB segment; fixture corrected to 256 KiB, not reduced payload coverage |
| `properties/rust-125.txt` | 125 passed, zero failed, 2 ignored | Ignored helpers are unchanged upstream forensic dump/replay entry points, not skipped conformance tests |
| `properties/json-boundary-001` through `004` | PATH/import errors, then real malformed JSON fork reproduction, then passing regression properties | The original comma counter split nested/quoted JSON and materialized the whole suffix; bounded lexical scanning fixes both |
| `properties/rust-127.txt`, `properties/standalone-112-json-fix.txt` | 127 / 112 passed, zero failed, same 2 upstream forensic helpers ignored | Replicated and standalone modes after the JSON boundary correction |
| `formal/apply-qualification.txt`, `properties/rust-128-apply.txt` | 17 negative mutations detected; 128 Rust tests passed, same 2 forensic helpers ignored | The same native commit-marker fsync moves before publication on the ordered apply worker; paired OpenRaft committed methods remain no-op/None |
| `conformance-008` | 332 passed, zero failed/skipped/todo | Full replicated suite after the apply barrier scheduling change |
| `fault-007` | Harness failure retained | Leader lookup briefly returned no leader during election; fixed the wait predicate, not server assertions |
| `fault-008`, `fork-fault-003`, `subscription-fault-002`, `storage-fault-004` | Independent checkers PASS: 129 / 748 / 192 / 28 operations | Same real-process campaigns after the apply change; subscription/storage runs retain 1 / 3 unknown outcomes; nine webhook signatures verified |
| `bench-smoke-001`, `bench-smoke-002` | 0/3 then 3/3 cells completed | Initial supervised-service names exceeded the platform length bound; hashed lab identifiers fixed startup |
| `bench-local-001` | 36/45 cells completed | Read/mixed seeder incorrectly required HTTP 200 instead of accepting successful 204; all nine failed cells stopped before measurement |
| `bench-local-002` | 9/9 read/mixed/profile cells completed | Corrected exact-byte seeding, still pinned unmodified ds-bench and native server |
| `bench-local-003` | 44/45 cells completed | Three-replica 1,000-subscriber fanout received linearizable-read 503s and timed out at 240 s; client output/logs and failed outcome retained, no throughput inferred |
| `conformance-009` | 332 passed, zero failed/skipped/todo | Full replicated suite after physical WAL reclamation and snapshot cleanup |
| `properties/rust-129-reclaim.txt`, `properties/standalone-112-reclaim.txt` | 129 / 112 passed, same 2 forensic helpers ignored | Real files across randomized segment boundaries, retained/post-checkpoint/uncommitted suffixes, corruption, open snapshot descriptors |
| `fault-009`, `fork-fault-004`, `subscription-fault-003`, `storage-fault-005` | Independent checkers PASS: 129 / 745 / 189 / 28 operations | Post-reclamation reruns; subscription/storage retain 1 / 3 unknown outcomes; nine independently verified signatures |
| `reclaim-fault-001` | Independent checker PASS: 20 operations; nine exact filler probes | Real checkpoint ENOSPC, file-fsync EIO, directory-fsync EIO: no premature unlink; all replicas reclaim and survive restart with 38,010,880 identical bytes |

The upstream suite is **unchanged `@durable-streams/server-conformance-tests@0.3.5`**,
with `subscriptions:true`. All 332 tests are discovered and executed in each full
run. No assertions were weakened and no filters or custom skips added. Later
ledgers also hash every suite JavaScript file, not only its entry point.

`fork-fault-001` rejects two deliberately corrupted histories: truncated inherited
bytes and false absence after a durable grant. Its checker reconstructs bytes
from client requests. It covers an explicitly ordered binary lifecycle fixture,
not all concurrent lifecycle histories. Unknown HTTP outcomes remain in the raw
history rather than being relabeled aborted.

The subscription checker is an independent, ordered-fixture model with concurrent
claims/external deliveries, not an arbitrary-history linearizability checker. It
rejects stale accepted acknowledgements, unauthorized cursor advancement and a
forged webhook. The storage campaign checks actual syscalls and startup rejection;
it cannot simulate a dishonest disk returning successful fsync or prove survival
of shared-disk rollback. Failed snapshot files are deliberately corrupted before
restart to show they were not published as authoritative references.

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
