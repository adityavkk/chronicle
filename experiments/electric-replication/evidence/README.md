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
| `formal/` | Safety and stable-period liveness pass; 23 negative mutations detected; Lean without `sorry` | Includes delayed catalog/incarnation/link observations, private apply recovery, journal reclamation and read cohorts; assumes Raft and honest storage; not Rust refinement proofs |
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
| `conformance-010`, `properties/rust-130-reads.txt` | 332/332 conformance, zero fail/skip/todo; 130 Rust tests, same two forensic helpers ignored | Arrival-fenced read cohorts; late arrivals cannot reuse an in-flight confirmation, including failure/cancellation |
| `fault-010`, `fork-fault-005`, `subscription-fault-004`, `storage-fault-006`, `reclaim-fault-002` | Independent checkers PASS: 129 / 748 / 190 / 28 / 20 operations | Post-coalescing reruns; subscription/storage retain 1 / 3 unknown outcomes, nine verified signatures |
| `bench-local-004` | All 45 workload/profile cells completed | Fixed the 1,000-subscriber opening failure without stale fallback; one closing `/proc` resource-sampling error retained, not hidden; short shared-orb windows, no capacity claim |
| `fanout-drain-001` | 1,000/1,000 readers each received exactly 100 records | Separate finite JSON full-drain fixture with independent sequence-ledger check; not substituted for unchanged ds-bench's windowed counters |
| `publication-001` | Corrected post-push fixture audit PASS | The initial auxiliary validator wrongly assumed JWT and failed; the push shell was not fail-fast. Corrected audit verifies 56 issued signatures plus four intentional tamper/401 cases, 12 retired PIDs, and no encoded match for 16 local signing/HMAC keys across 2,839 historical blobs (599 gzip payloads). This does not retroactively make the pre-push check pass |
| `write-diagnostics-001` | Six matched write cells PASS | Native 90–107k/s versus one member 3.6–3.9k/s; native-WAL counters isolate failed fsync amortization; no capacity claim |
| `formal/batches-qualification.txt`, `properties/rust-132-batches.txt` | 28 total negative mutations detected; 132 Rust tests pass, same two forensic helpers ignored | FIFO batching, singleton metadata, exact byte bounds, per-command replies, cancellation capacity, partial committed materialization recovery |
| `conformance-011`, `fault-011` | 332/332 conformance, zero failures/skips; 194 checked operations | Real batched entries in both groups; independent checker additionally verifies exact per-payload append/read byte offsets and rejects swapped replies |
| `fork-fault-006`, `subscription-fault-005`, `storage-fault-007`, `reclaim-fault-003` | Independent checkers PASS: 749 / 193 / 28 / 20 operations | Post-batching reruns; subscription/storage retain 1 / 3 unknown outcomes; nine verified webhook signatures |
| `write-diagnostics-002` | Six matched write cells PASS | Native 91–102k/s versus one member 48–51k/s, about 2×; all exact byte/offset probes pass, no client errors; one closing client `/proc` sampling gap retained |
| `write-profiles-001` | All three traced write cells PASS | Separate syscall profiles; tracing collapses native group-commit amortization, so traced throughput is not an unperturbed capacity comparison |
| `conformance-012`, `fault-012`, `properties/rust-132-pipeline.txt` | 332/332, zero failures/skips; 193 operations; 132 Rust tests | Two outstanding FIFO-enqueued consensus batches, same charged admission and durability barriers |
| `fork-fault-007`, `subscription-fault-006`, `storage-fault-008`, `reclaim-fault-004` | Independent checkers PASS: 747 / 195 / 28 / 20 operations | Post-pipelining reruns; same 1 / 3 subscription/storage unknowns, nine verified webhook signatures |
| `write-diagnostics-003` | Six matched write cells PASS | Native 99–104k/s versus one member 53–58k/s, 1.72–1.89×; no client errors, exact byte/offset checks; closing native client `/proc` sampling gap retained |
| `formal/async-qualification.txt` | Receipt/backlog safety and conditional liveness pass; 36 total negative mutations detected; Lean without `sorry` | Before async implementation: 89 receipt states and 295 backlog states; full identity, semantic rejection, expiration, local-fsync versus acceptance, restart/cancellation credit boundaries; not Rust refinement |
| `formal/receipts-001-unspecified-assignment.txt` | Failed model run retained | Missing parentheses made a disjunctive assignment underspecified; corrected before the passing safety/liveness and mutation runs |
| `formal/epochs-queue-qualification.txt` | Epoch admission safety and conditional liveness pass; 38 total negative mutations detected; Lean without `sorry` | 1,481 states distinguish reservation, application queue, Raft API queue and assignment; preparation must be term-owned and checked inside the consensus core |
| `properties/epoch-native-001.txt` through `003.txt` | Failed test fixtures retained | The deterministic leadership-transition fixture was corrected before claiming epoch-race coverage |
| `properties/epoch-native-004.txt` | Native-Journal epoch regression PASS | Three leadership transitions with queued stale proposals; no log index, WAL frame or local receipt for rejected tickets |
| `properties/rust-async-epochs-001.txt`, `properties/openraft-backport-001.txt` | 135 adapter/native tests and 190 OpenRaft unit tests PASS | Two unchanged upstream forensic helpers ignored in the first suite, none in the second; real WAL receipt/recovery properties, not a proof of consensus or Rust refinement |
| `properties/receipts-client-002.txt` | Three client-helper tests PASS | Await deadline/cancellation and distinct terminal outcomes; helper also exercised against live replicas |
| `conformance-013`, `conformance-014` | 332 discovered/executed/passed, zero failures/skips/todo each | Three processes, two partitions, subscriptions enabled; 014 includes expected-leader admission backport and operator-configurable append default, with the suite using the shipped quorum-fsync default |
| `async-fault-001` | Independent checker PASS: 267 operations, 38 receipts | 20 committed, one rejected, 17 invalidated; four unknown HTTP outcomes retained; before operator-configurable append default |
| `async-fault-002` | Independent checker PASS: 270 operations, 39 receipts | 21 committed, one rejected, 17 invalidated; five unknown HTTP outcomes retained. Minority count/byte bounds, no prefix/session/SSE leakage, real WAL fsync EIO, mixed modes, configured async default, snapshot/movement/full restart |
| `fault-013` | Failed batching-evidence assertion retained | TCP setup staggered the nominal concurrent burst; connections now open before its barrier so the unchanged batching assertion exercises actual simultaneous work |
| `fault-014`, `fork-fault-008`, `subscription-fault-007`, `storage-fault-009`, `reclaim-fault-005` | Independent checkers PASS: 194 / 748 / 190 / 28 / 20 operations | Strong-mode reruns after async/epoch changes; subscription/storage retain one / three unknown outcomes, nine verified signatures, seven injected storage syscalls, 38,010,880 exact reclaimed/restarted bytes |
| `async-writes-001` | Nine of 12 cells PASS; all three async cells FAIL the zero-error gate | Three repetitions, 30-second unpaced windows, 256 connections, identical 256-byte payloads. Native 88–99k/s, one member 39–43k/s, quorum-three 25–28k/s; async 28–30k acceptances/s with 114–135k backpressure responses per window, not a successful capacity result. All accepted bytes drain exactly on every replica; shared-host evidence only |
| `write-diagnostics-004`, `async-writes-002` | Six short cells PASS; nine of twelve long cells PASS | Scheduler yield lowers fsyncs/ack without an established throughput win. Long runs: native 88–102k/s, one member 40–45k/s (2.09–2.45× gap); all async cells fail with 24–44k backpressure responses per window |
| `cpu-profiles-001` | Three perturbed write cells PASS | 728 / 549 / 916 user-space CPU samples; allocation, deserialization and scheduling costs visible. Raw DWARF memory stays local-only; symbolized stacks/hashes retained. No blocked/kernel-time or capacity inference |
| `heap-profiles-001`, `heap-profiles-002` | Profiler harness failures retained | `.zst` naming and supervisor restart/trace-finalization problems; not valid allocation measurements |
| `heap-profiles-003`, `heap-profiles-004` | Three perturbed write cells PASS each | Before/after durable-header projection: 45.79→41.78 one-member and 146.24→118.32 aggregate three-member allocations/ack; native 25.01 in both. Whole process lifetimes normalized by all-phase client acks, not exact measure-window counts |
| `formal/headers-qualification.txt`, `properties/rust-136-headers.txt` | 38 TLC negative mutations plus one Lean header mutation detected; 136 Rust tests PASS | Stable retained-header lookup, duplicate order and actual bincode round-trip property; same two unchanged forensic helpers ignored |
| `conformance-015` | 332 discovered/executed/passed, zero failures/skips/todo | Three processes, two partitions, subscriptions enabled; unchanged default linearizable/quorum-fsync suite after header projection |
| `fault-015`, `fork-fault-009`, `subscription-fault-008`, `storage-fault-010`, `reclaim-fault-006` | Independent checkers PASS: 195 / 749 / 194 / 28 / 20 operations | Post-projection reruns; one / three subscription/storage unknowns, nine verified signatures, seven intercepted storage faults and 38,010,880 exact reclaimed/restarted bytes |
| `async-fault-003` | Independent checker PASS: 269 operations, 39 receipts | 21 committed, one rejected, 17 invalidated; five unknown HTTP outcomes retained. Mixed modes, minority bounds, committed-only visibility, storage EIO, snapshot/movement/restart |
| `async-writes-003` | Nine of twelve cells PASS; all async cells FAIL the zero-error gate | Header projection reduces allocations but does not establish throughput improvement: native 88–108k/s, one member 40–45k/s (2.14–2.39×), quorum-three 26–28k/s; async 31–34k accepts/s with 51–62k backpressure/window. Every accepted byte drains to each replica |

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
