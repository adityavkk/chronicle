# Replicated Electric engine experiment

This isolated Rust binary extends Electric's actual native storage/read engine.
It does not alter Chronicle's Go/Redis default, and it is not the SQLite-backed
`rust/distributed-streams` experiment. **Not production-qualified.**

The imported source is Electric npm 0.1.5 at
[`88793e7`](https://github.com/electric-sql/electric/commit/88793e76595d69be300731b9b25c58538923a53b),
Apache-2.0. `UPSTREAM-TREE.txt` pins original blobs; `engine/LICENSE` retains the
license. The isolated stock OpenRaft 0.10.0-alpha.36 candidate provides consensus
over Electric's WAL/group-fsync path; qualified 0.9.25 remains its comparison
baseline. Its initial write regression and measured scheduling correction remain
in the evidence ledger; broader performance qualification is incomplete.
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
* [ASYNC.md](ASYNC.md): opt-in local-fsync202 receipts, await helper, bounded
  admission and committed-only reads; acceptance can be lost after failover.
* [evidence/README.md](evidence/README.md): executed gates and preserved failures.

Paths hash to fixed groups, not to the current node count. Replica movement uses
learner catch-up and joint membership; it does not repartition a live stream.
One ordered stream still has one owner group. There is no general cross-group
transaction or global read snapshot. The experimental control plane is trusted,
loopback-only; it is not a production authentication/transport design.

Quorum-fsync remains the default. Opt-in local-fsync POST appends return 202 and
a receipt, not a session or a successful append result. An HTTP timeout/503 after
admission can have committed; producer epoch/sequence handles retry effects. Reads use
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
python3 experiments/electric-replication/scripts/subscription_faults.py experiments/electric-replication/evidence/subscription-fault-new
python3 experiments/electric-replication/scripts/storage_faults.py experiments/electric-replication/evidence/storage-fault-new
python3 experiments/electric-replication/scripts/reclaim_faults.py experiments/electric-replication/evidence/reclaim-fault-new
python3 experiments/electric-replication/scripts/async_faults.py experiments/electric-replication/evidence/async-fault-new
python3 experiments/electric-replication/scripts/snapshot_progress.py experiments/electric-replication/evidence/snapshot-progress-new
node --test experiments/electric-replication/client/receipts.test.mjs
```

`scripts/benchmark.py` uses the pinned unmodified native server and ds-bench
binaries at `.tmp/electric-tools/{upstream,bench}-target/release/`; their SHA-256
values and exact source pins are in each run's `provenance.json`. It records fresh
per-cell configurations, command lines, exact seed/offset probes, raw client/HDR
results, syscall profiles, process/socket samples (`samples.jsonl.gz`), storage
sizes and errors. Run it sequentially with other builds/tests stopped.

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

Work proceeds in this order. The clean local 0.9 checkpoint `7fc1605` and its
qualified release binary remain the comparison baseline; the published branch
checkpoint is not production-qualified. Each row requires the actual engine,
not a substitute store or a model-only result.

| Order | Acceptance gate | Current state and required evidence |
| --- | --- | --- |
| 1 | Stock OpenRaft 0.10 isolation and lease admission | Local correctness qualification passes: 142 Rust tests, 332/332 conformance, storage/recovery/snapshot/identity and process fault campaigns. Assignment-time lease rejection retains outstanding receipts/credits. Identity 8 rejects identity 7 before recovery; no rolling upgrade or migration. Performance comparison below blocks promotion. |
| 2 | Single-node overhead and replicated sync/async performance | Open: matched hot/many-stream repetitions, 60/60 corrected read/fanout/mixed execution checks and a 60-second reject-free async operating point are retained below. The pinned-snapshot checkpoint still has 16 lease-expiry rejects in one 90-second/16-connection repetition. Coalesced physical GC passes its delayed-fsync progress/recovery probe; sustained-load comparison is outstanding. Replicated latency/memory and instrumentation overhead still need qualification. Report accepts versus commit progress, rejection, backlog/lag, p50/p99, CPU/RSS/fsync and profiles; retain unsuccessful candidates. |
| 3 | Storage lifecycle and bounded metadata | Open: native cold ownership/offload/recovery/GC, terminal fences and tombstone compaction. Require generated/state-machine or independently checked histories for readers, retries, transfers, crashes and sustained growth/reclamation. |
| 4 | Production observability, security and operations | Open: start instrumentation with earlier work; qualify versioned redacted events, W3C/OTel correlation, quorum/admission/receipt/storage/GC metrics, bounded profiling, alerts and telemetry failure/overhead. Transport/admin auth, TLS, tenant/resource isolation, bootstrap/drain/replacement, backup/restore and upgrade/DR paths need actual implementations and fault/property coverage. |
| 5 | Independent-host acceptance and release | Blocked on separate resource/access approval after no-spend work. Require independent machine/disk failures, volume loss/spares, partition and membership/subscription histories under load. Local shared-host tests do not discharge this gate. Prepare bounded cost, limits and cleanup before paid resources; no use of the PostgreSQL budget. |

Every deliverable needs a substantive TLA+, Lean, generated state-machine test
or independently checked fault history over its risky behavior, with explicit
model-to-code gaps and useful negative controls. Qualified publication and trunk
integration are authorized, not automatic: audit the full unpublished range,
preserve human commit identities, integrate concurrent `origin/main`, run
fail-fast checks and CI, then verify the actual remote merge. No shared
deployment is authorized. Independently complete experimental milestones may
merge with explicit limits; they cannot waive the remaining acceptance gates.

Conformance and the existing fault/property tests are necessary, not sufficient.
The candidate's `conformance-030` executes **332/332 passing, zero failures/skips/todo**,
with subscriptions enabled, three processes and two partitions. All 142 Rust
tests pass; two unchanged upstream forensic helpers remain ignored, not
conformance exclusions. `storage-fault-022` checks seven real failing/short
syscalls and retains three unknown HTTP outcomes. `async-fault-013` checks 311
operations and 40 receipts: 21 committed, one rejected and 18 invalidated, with
six unknown requests retained. Its isolated leader rejects new assignment on
lease expiry despite spare backlog capacity, without changing WAL bytes or
releasing unresolved credit. These are single-host qualifications, not
independent-disk/AZ or power-loss evidence.

`reclaim-progress-001/002/003` isolates a second scheduling boundary: OpenRaft
awaits logical purge inside its core task, so worker handoff alone still stalls
the group. Coalescing physical GC after durable logical purge raises completions
inside the same two-second checkpoint fsync from zero to 38, with no unknowns
or leadership change and exact restart. The reader/reclamation model checks
4,910 safety states, 2,047 stable-period liveness states and six mutations.
Background storage errors still fail-stop; no durability/lease bound changes.
This is not evidence that the remaining sustained-load lease rejects are fixed.

The latest hot-stream matrix (`openraft010-async-002`, zero warmup) measures
**51.8–61.2k/s one member versus 87.4–102.3k/s native**, a paired **1.67–1.74×**
gap. All strong cells pass; all six async cells fail the unchanged zero-rejection
gate, with accepted bytes nevertheless draining exactly. A separate four-connection
campaign (`openraft010-envelope-c4-001`) passes all **12/12 sixty-second cells**;
async accepts 4.18–5.01k/s without errors/rejections. That is an operating point,
not maximum sustainable capacity. No admission bound or durability was relaxed.

The 1,024-stream matrix (`openraft010-many-001`) passes **12/12 thirty-second
cells**: native 39.6–45.0k/s, one member 38.2–51.9k/s, quorum-three 19.0–19.3k/s,
async 19.7–20.1k/s. Async does not approach one-member throughput here. Its
111–113 ms p99 and 215–216 MiB aggregate RSS need diagnosis. Exhaustive post-run
drain includes thousands of HEAD requests; it is not pure replication latency.
Heap profiles separately attribute 26.80 decimal MB per current process to stock
`base2histogram` startup tables, including with OpenRaft runtime-stats disabled.
That fixed allocation explains the startup regression versus 0.9, not the extra
async-versus-quorum memory or a leak. See [BATCHING.md](BATCHING.md) for evidence
and remaining limits; no private histogram/consensus fork was added.

The corrected read matrix (`openraft010-reads-002`) passes **60/60 execution
checks**, with stable sampled clocks and twelve separate finite 1,000-reader
sequence checks. Mixed writes measure native 19.5–20.5k/s, one member 11.5–12.1k/s,
quorum 6.20–6.63k/s and async 6.66–8.20k accepts/s. Paced fanout frame counters are
not per-reader sequence proof or capacity. Follow-on syscall profiling retains
an analyzer failure caused by interleaved diagnostic logs; it does not resolve
the untraced p99. Allocation profiles implicate bounded receipt results in the
extra async heap, without changing retention or establishing a leak.

The later PID-scoped kernel tracepoint campaign (`kernel-fsync-many-001`) passes
all four workload/export checks and reproduces the quorum tail: 112.7 ms append
p99 with 460 fsync/fdatasync calls taking at least 100 ms. Native also has 32 such
calls. These are whole-capture syscall intervals including setup/drain, not pure
device latency or unperturbed capacity. The bounded profiler exports no unused
register arguments; raw captures stay local-only. [BATCHING.md](BATCHING.md) has
the counts and generated parser/redaction checks. Disk versus descheduling and
the separate occasional lease rejection remain unresolved.

Opt-in `RAFT_WRITE_OUTCOMES` schema-1 counters distinguish count/byte backlog
limits, epoch/lease/forward rejection, locally durable/applied command stages,
and unknown post-assignment outcomes. They contain fixed labels, not request
data; overlapping stages and internal commands are not HTTP totals. The fault
checker tests real count/byte/lease rejection and rejects misclassified evidence.
Benchmark envelope controls include concurrency, stream count and zero warmup:
the pinned write pool is closed-loop (ignores `rate-per-stream`), combines
429/503, drops rejected attempts and excludes warmup errors. Progress samples
cover at most 16 explicitly declared streams without extrapolation; final drain
still checks every stream on every replica. These are diagnostic instruments,
not completed production observability or proof of sustained capacity.

`snapshot-progress-002/003` isolates a scheduling bug and its fix with an actual
two-second snapshot fsync delay. Before worker handoff, no unrelated-partition
operations complete during the delay and leadership changes; afterwards, 38
complete with no unknown outcomes or term change. The exclusive snapshot view
and durable publication order are unchanged. Apply in that group still pauses,
so this is not proof of a zero-rejection async load envelope. The snapshot change
passes the tests/formal/conformance above and fork/storage process reruns.

Direct metadata capture subsequently removes redundant hot-sidecar fsyncs without
changing the archive's durability barriers. `snapshot-cost-001` measures three
untraced 1,024-stream snapshots at **709–1,139 ms before versus 27–35 ms after**;
separate traces show 1,024 sidecar fsyncs versus zero. All eight cells survive
restart with exact bytes/offsets and producer retry state. Generated snapshot-only
restore and conformance/fault reruns pass. This isolated cost reduction does not
remove the hot-payload copy pause or qualify sustained async capacity.

The next pinned-cut change releases apply during payload copy/hash/fsync while
retaining a separate build/install/publication/cleanup lock. TLA+ precedes the
implementation (865 safety states, 60 liveness states, six negative mutations);
generated real-file tests restore the original cut despite later appends,
receipts and unlink/recreation. Same-group delayed-fsync histories improve from
zero completed operations / six unknowns to 38 completions / zero unknowns, no
term change and exact restart recovery. `snapshot_cut` records the remaining
metadata-capture pause. The conformance and fault counts above cover this change;
sustained performance and memory qualification remain separate gates.

The completed pinned-cut comparison (`snapshot-cut-envelope-001`) retains **17
PASS / 4 FAIL**, all stable sampled clocks. One member is 8.93–9.48k/s versus
native 15.84–17.31k/s at 16 connections, a paired 1.71–1.83× gap; all synchronous
cells pass. Async improves from three failing repetitions (1,723–2,199 rejects)
to two passing and one failing (16 lease-expired requests). No current async
cell has a count-bound reject; exact accepted-byte drain passes everywhere.
Leader apply-lock maxima fall from hundreds of milliseconds to below 11 ms, while
snapshot copy/fsync continues concurrently. The metadata copy costs memory:
aggregate async RSS rises from 129–131 to 147–148 MiB. This is a qualified
scheduling fix, **not** a zero-error 90-second envelope or broad capacity claim.

The first matched stock-0.10 comparison, `openraft010-writes-001`, has three
30-second repetitions with identical local-fsync contracts, 256-byte payloads and
256 connections. Native measures **91.6–100.4k/s**, qualified 0.9 one member
**34.4–40.0k/s**, and initial 0.10 one member **3.4–10.5k/s**. All nine workload
checks pass; the performance gate fails. WAL fsyncs/ack rise from about **0.049**
on 0.9 to **0.46 / 0.82 / 1.68** on 0.10 as command batches collapse. Candidate
CPU is only 0.80–0.84 cores. Separate `openraft010-cpu-001` profiles are retained;
they do not establish the cause of blocked time. The 0.10 builder await is
enqueue-only, verified against pinned upstream; no performance improvement is
inferred from its nonblocking storage API. Dispatch/apply interaction remains
under investigation. The experiment has not switched its accepted baseline.

Reducing locally-unflushed dispatch slots from two to one restores batching
without a timer or weaker durability. `openraft010-flight1-001` measures
**47.4–59.8k/s** against the original candidate's **3.4–3.5k/s**, with all nine
cells passing exact-byte and zero-error checks. WAL fsyncs/ack drop to
**0.033–0.034**, from 1.67–1.70. Native varies 70.6–101.8k/s in that matrix;
this is evidence for the scheduling fix, not broad parity. The latest
conformance/formal/fault reruns above include this change.

The broader `openraft010-async-001` matrix retains **14 passing and seven failing
cells**, with a nonzero driver exit. Native measures **94.3–100.7k/s**; current
0.10 one member **52.3–60.7k/s** versus 0.9 **34.1–45.3k/s**, all zero-error with
exact bytes. Current quorum measures 36.6–41.6k/s, but one repetition rejects
2,864 requests and changes term during snapshots, so the quorum gate fails.
Every async repetition rejects load: 7.1–7.9k attempts on 0.10, 90.8–101.7k on
0.9. Accepted bytes drain exactly to every replica; these are overload runs, not
sustainable zero-error async capacity. The pinned client combines 429 and 503;
per-cause load diagnosis remains open; the controlled snapshot-delay result
above does not make these earlier load failures pass.

The matched `async-writes-006` matrix has complete resource samples: native
**77–84k/s**, one member **39–46k/s** (**1.84–1.96× slower**), and quorum-three
**24–25k/s**. All three async cells fail the unchanged zero-backpressure gate:
28–29k acceptances/s and 81–94k rejected attempts per 30-second window, despite
every accepted byte draining exactly to every replica. Neither this nor the
earlier 53–58k/s short one-member runs establishes performance parity.
`async-writes-005` remains invalid after ENOSPC truncated a resource sample; its
failure and the unchanged analyzer's rejection are retained. A harness headroom
guard now runs before each cell; it is not a product disk-size requirement.

[BATCHING.md](BATCHING.md) records the measured fsync-amortization defect behind
the original roughly 30× gap, its batching fix, and later profiling experiments.
One-member WAL fsyncs/ack remain about twice native; snapshots also pause apply.
Allocation projection saves measured allocations but does not establish a
throughput gain. Raising the command ceiling and inlining the durability waiter
likewise showed no established benefit and were reverted. A 100 µs fsync
collection interval improved grouping but measured only 32–33k/s one-member;
it too was rejected. The preserved 0.9 engine/vendor source hashes and release binary
match `conformance-021` exactly, and its full suite was rerun as 022. Raw CPU memory captures
stay local-only; symbolized profiles, allocation traces, failed histories and
exact source/binary/config hashes remain in the evidence ledger.

The broader **pre-batching** `bench-local-004` completes all **45/45** matched ds-bench cells
after arrival-fenced read coalescing. The prior `bench-local-003` three-replica,
1,000-subscriber failure (503s and a 240-second timeout) is retained. One 004
write cell lost its closing resource sample to a `/proc` permission race; the
driver traceback is retained and resource coverage is explicitly partial.

| Pre-batching local qualification cell | Unmodified Electric | One-member adapter | Three replicas |
| --- | ---: | ---: | ---: |
| One stream, concurrency 256 (writes/s) | 105,877 | 3,242 | 4,200 |
| 1,024 streams, concurrency 256 (writes/s) | 59,638 | 3,579 | 4,242 |
| Seeded 4 MiB replay (GiB/s) | 17.61 | 17.53 | 15.67 |
| Mixed, unpaced writes and fixed-rate reads (writes/s) | 26,922 | 6,625 | 4,908 |

These short single runs share a disk/page cache and 16 GiB host memory. Each arm
gets four aggregate SUT CPU affinities, the client another four; the three-node
arm does not get three times the CPU budget. Native and one-member writes are
local-fsync; three-node writes are quorum-fsync on this **shared host**. The client
is not independently calibrated and the native write ladder has not plateaued.
Replay is page-cache/loopback throughput, not disk bandwidth; different runs vary
substantially. Paced fanout is not saturation capacity. Profiles are separate,
perturbed executions: all read arms call real `sendfile`; write profiles show
substantial fsync and scheduling costs. The adapter has a large measured write
gap, not Electric performance parity. Independent-host capacity, sustained memory
and disk growth, tail latency, and overload behavior remain evaluation gates.

For the hot-stream concurrency-256 cells, sampled aggregate SUT CPU is
1.78 / 0.49 / 0.73 cores and peak sampled RSS is 6.8 / 13.6 / 30.9 MiB
(native / one-member / three replicas). RSS excludes shared page cache. The
three-replica outbound socket counter delta is at least 30.8 MiB over its sampled
write window, not a complete packet capture. `scripts/benchmark_summary.py`
recomputes `summary.json` from raw samples: writes use exact client measure
timestamps; other modes lack them and explicitly use the outer client invocation.
The low write CPU alongside fsync/scheduling profiles points to serialized
coordination, not exhausted CPU; profiles are evidence, not a complete causal
decomposition or a claim that tuning one knob will close the gap.

The pinned fanout client does **not** guarantee full drain. All readers obtain
HTTP headers before writing starts, but begin body polling after that barrier.
It counts every completed append attempt as sent, and only complete parseable
data frames as received, without per-reader sequence/dedup accounting. Readers
stop at the nominal deadline plus two seconds; a boundary append can be issued
after the deadline check and complete later. Its bounded task join does not
extend that window. Partial frames are discarded; body EOF/error, join errors
and join timeout need not increment its subscriber-error count. Thus the 004
1,000-reader fractions (99.97% / 99.90% / 99.88%) are windowed observations, not
proof of loss or complete delivery. See the pinned
[read/write boundaries](https://github.com/electric-sql/ds-bench/blob/93a1a066a511ad2ce5114dc429afb1fd0f6d99bf/ds-bench/src/fanout.rs#L146-L303).
The separate `fanout-drain-001` correctness probe keeps full sequence ledgers:
**all 1,000 replicated readers received exactly records 0–99**, with payload
checks and no accepted EOF, timeout, duplicate or gap. This finite JSON fixture
is not a throughput measurement or proof about every omitted benchmark frame.

No throughput or availability promise is made from blog numbers or single-orb
runs. Paid evaluation needs separate authorization; qualified publication and
trunk integration do not imply a cloud budget or deployment permission.

Physical WAL reclamation now uses a checksummed metadata-only checkpoint; old
snapshot cleanup preserves open transfer descriptors. The new real-process
campaign injects checkpoint write/fsync/directory-fsync errors before reclaim,
then verifies physical deletion and complete restart against 38,010,880 payload bytes.
The 004 measurements above include this lifecycle change.

Cold-tier ownership/GC and terminal transaction-fence compaction remain
unimplemented. Catalog repair
scans and subscription/fork cardinality need scale qualification. Snapshots pause
one group's apply while copying its files; the cost must be measured. Native
JSON sub-offset resolution now scans bounded 64 KiB windows with lexical state
across chunk boundaries; generated native-WAL/snapshot/restart cases cover nested
values, quoted commas, escapes and large values. Pure Lean chunk-composition
lemmas do not prove the Rust scanner or native file reader correct.
