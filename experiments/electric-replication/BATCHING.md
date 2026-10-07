# Restoring native group-commit amortization

The specification and formal checks preceded implementation. This is batching
within a real Raft group, not a one-member bypass or a weaker fsync mode.

## Measured reason

`write-diagnostics-001` uses the same pinned unmodified Electric and ds-bench
sources as the main matrix. Three fresh hot-stream, 256-byte, concurrency-256
runs per arm enable the existing native WAL/appender counters on both sides.
Native measures 90,299 / 106,807 / 105,233 writes/s; one-member Raft measures
3,631 / 3,866 / 3,763. All six cells pass exact byte/offset checks with no client
errors. These are local diagnostics with shared disk/cache, not cloud capacity.

Whole-invocation native WAL counters (including warmup/setup, **not** the client
measure window) show 45.45 / 47.32 / 46.45 records per fsync and
0.0220 / 0.0211 / 0.0215 fsyncs per append. Replicated counters show
1.01 / 1.02 / 1.02 records per fsync and 1.9793 / 1.9573 / 1.9633 fsyncs per
append. The native materialization handler inside apply averages 4–5 µs, excluding
consensus and journal waits. Traced profiles are separate perturbed executions.

Pinned OpenRaft 0.9.25 calls `write_entry` with a one-element vector, then runs
engine commands after each queued client request. `append_to_log` awaits both
the storage append future and its `LogFlushed` callback before the core handles
the next request. Returning early from our storage future cannot fix this.
`max_payload_entries` limits follower traffic, not client batching. See the
[core implementation](https://github.com/databendlabs/openraft/blob/8815cdba2826f74e848acef361ad03f93bb1c3f8/openraft/src/core/raft_core.rs#L707-L730)
and [client-message loop](https://github.com/databendlabs/openraft/blob/8815cdba2826f74e848acef361ad03f93bb1c3f8/openraft/src/core/raft_core.rs#L986-L1014).

Before batching, the adapter spends approximately two WAL fsyncs per command: its entry
and the covering apply marker. It also rereads each entry from its indexed native
WAL for apply. Native amortizes its fsync across dozens of concurrent appends.
This is the measured first bottleneck. Remaining serialization, copying,
allocation and scheduling costs must still be profiled after batching; no
throughput improvement is asserted before measurement.

## Ordered, bounded append batches

One per-group ingress queue owns admitted requests and their permits. It drains
already queued POST appends in FIFO order into one immutable consensus entry,
bounded by command count and serialized bytes. It never waits for a batch timer.
The first non-append command ends the batch and remains first for the next entry.
All creation, deletion, clock, subscription and fork commands are singleton
entries. Their existing log-index-derived stream/incarnation/fencing identities
therefore remain unique without introducing a second identity counter.

Each command still has its own deterministic status, headers and result. The
ordered response vector must route position i to request i, including protocol
rejections and canceled HTTP receivers. A batch is not an all-or-nothing client
transaction: earlier successful appends remain successful if a later command is
rejected. Producer epoch/sequence deduplication runs for each command in order.

Entry durability, quorum commitment and the native-WAL covering Commit marker
are unchanged. Apply must wait for that marker before **any** command can touch
native files or notify live readers. The group view lock excludes ordinary reads
and snapshots through the entire batch. Existing live readers may observe its
successive committed prefixes. A `Stream-Session` position covers the entire
applied entry, never a speculative command or a partially applied snapshot.

HTTP timeout/disconnect does not cancel an admitted command or release its
capacity permit. Capacity releases only when consensus resolves that command;
reply receiver disappearance is irrelevant. The pending bound covers requests
both in the ingress queue and in consensus/apply, not just an emptying channel.

Recovery privately rebuilds a partially materialized committed batch from its
durable entry/marker or a whole-batch snapshot. No native hot-file size establishes
commitment. The input schema change requires a new experimental data identity;
there is no silent upgrade of earlier local data or mixed-version operation.

`Batches.tla` checks FIFO sealing, metadata singleton identity, response routing,
publication bounds, timeout capacity, and fairness-qualified eventual resolution.
Negative variants reorder entries, batch metadata, reply early, swap reply owners,
or free credits on timeout. Lean checks deterministic fold composition; neither
proves Rust refinement, OpenRaft, filesystem behavior or distributed liveness.
Real-WAL property/recovery tests, full unchanged 332-test conformance and process
fault campaigns remain required after implementation. Async acceptance is a
separate contract: batching must not implement local acknowledgement implicitly.

## First implemented checkpoint

`write-diagnostics-002` repeats the same six fresh cells after bounded batching:

| Repetition | Native writes/s | One-member writes/s | Native / one-member | Native p99 ms | One-member p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 102,327.5 | 50,576.1 | 2.02 | 4.607 | 6.887 |
| 2 | 96,630.1 | 49,071.8 | 1.97 | 6.071 | 7.263 |
| 3 | 90,656.4 | 48,115.3 | 1.88 | 7.795 | 8.431 |

All six exact byte/offset probes pass, with zero client backpressure/errors.
The one-member rate improves about 13× over the preceding 3.6–3.9k/s runs.
The gap is now about 2× in this workload, not general performance parity.
Sampled CPU is 1.26–1.30 cores and RSS 13.3–14.5 MiB for one member, versus
1.56–1.78 cores and 6.3–7.0 MiB natively. These remain short, shared-orb runs.
One closing client `/proc` observation was unavailable; its recorded sampling
gap is outside the measurement interval, not replaced with a zero counter.

`benchmark_summary.py` now computes whole-invocation native-WAL counters against
the client's exact all-phase acknowledgement count. With the configured 1-second
interval, `WAL_CONT` divides by exactly 1, so its summed staged/fsync values are
counter deltas. `SRV_STATS` instead divides by actual elapsed time and cannot
provide exact append counts by summing its rounded rates. WAL fsyncs per ack are
about 0.0313 for one member versus 0.0195–0.0224 natively. The replicated entry
contains up to 64 commands; its two barriers are amortized without eliminating
either one. These counters include setup/warmup and exclude filesystem syncs
outside the WAL; they are not measurement-window syscall counts.

`write-profiles-001` preserves separate `strace -f -c -w` executions for native,
one member and three replicas. Tracing strongly changes scheduling: native WAL
grouping collapses in the traced workload, so its throughput and normalized
fsync counts must **not** be substituted for the untraced comparison. The traces
show real `fdatasync`, per-append hot-file writes, journal rereads and scheduling
calls; blocked syscall wall time across threads is not CPU time. Allocation and
copying costs have not yet been independently attributed.

`conformance-011` passes all 332 unchanged tests with subscriptions enabled and
zero skips. `fault-011` passes 194 operations with exact per-payload returned
offsets (including asymmetric UTF-8) and proves both groups actually batched.
Fork/subscription/storage/reclamation campaigns pass 749/193/28/20 operations.
132 Rust tests pass; two unchanged upstream forensic helpers remain ignored.
Node data identity is 5 because the command schema changed; snapshot envelope
format remains 4 because snapshots contain materialized state, not commands.

## Next scheduling hypothesis

The first worker waits for commit/apply before submitting its next batch. The
bounded `Batches.tla` model already permits multiple FIFO-sealed entries before
resolution; admission credits remain charged until resolution, independent of
HTTP timeout. Pipelining two batches is a refinement of that allowed ordering.

Pinned OpenRaft's `client_write_ff` enqueues synchronously on its successful
awaited path and returns an independent final-response receiver. A single FIFO
dispatcher can await **enqueue**, then retain the receiver and capacity until
resolution while admitting the next bounded batch. It must enqueue before
spawning the receiver waiter; spawning unordered `client_write` tasks would not
preserve ingress order. This may overlap entry persistence with the preceding
apply marker/materialization. It is a measured-next hypothesis, not an achieved
speedup. See the pinned [API](https://github.com/databendlabs/openraft/blob/8815cdba2826f74e848acef361ad03f93bb1c3f8/openraft/src/raft/mod.rs#L667-L680).

The two-entry implementation now passes `conformance-012` (332/332, zero skips),
`fault-012` (193 operations), and fork/subscription/storage/reclamation reruns
(747/195/28/20 operations). `write-diagnostics-003` passes all six cells. Its
one-member rates are 58,043 / 57,813 / 52,743 writes/s versus native
104,161 / 99,371 / 99,705: 1.72–1.89× slower. One-member p99 is
6.015 / 6.403 / 9.703 ms; native p99 is 4.511 / 4.639 / 6.723 ms. WAL fsyncs per
ack remain 0.0312–0.0321: scheduling overlap, not reduced durability work, changes
the rate. The median is about 18% above the preceding serial-batch runs, with
material run-to-run variation. This is not yet broad workload or async parity.

## Async dispatch changes the strong-mode batch shape

Local acceptance frees the dispatch window after local fsync while command/byte
credits stay charged until resolution. The longer `async-writes-001` comparison
uses 30-second windows and 1,024 pending-command credits on every replicated arm.
Native measures 98,995 / 99,312 / 88,451 writes/s; one member measures
42,377 / 42,569 / 38,677: 2.29–2.34× slower. All strong cells pass exact-byte and
zero-error checks. Async cells fail that same zero-error gate because of bounded
admission rejection; accepted bytes still drain exactly to all three replicas.

One-member WAL counters rise to 0.0554–0.0578 fsyncs per acknowledgement. Final
log positions and all-phase acknowledgement counts imply about 34–35 commands
per entry, rather than almost 64. Apply still averages roughly 4–6 µs per native
handler in the logged intervals. Each 10,000-entry snapshot now arrives sooner
in byte terms; all three long one-member runs build three snapshots. A scheduler
race between local-flush dispatch and the preceding apply's response wakeups can
seal smaller batches. That is a testable scheduling hypothesis, not a reason to
remove either durability barrier or suppress snapshot work.

The candidate yields once to ready tasks before sealing POST cohorts. It adds no
batch timer or quorum wait and changes none of the FIFO, admission, epoch or
publication rules in the checked models. `write-diagnostics-004` has six passing
short cells, but one-member rates of 49,346 / 35,892 / 47,896 writes/s do **not**
establish an improvement. Fsyncs per ack fall to 0.0444–0.0507; these shorter
windows are not an isolated comparison with the prior 30-second run. The matched
long rerun `async-writes-002` measures 40–45k one-member writes/s against 88–102k
native: 2.09–2.45× slower. Its 0.0454–0.0483 fsyncs/ack improves grouping, not an
established throughput gain. Nine of twelve cells pass; all three async cells
fail the unchanged zero-error gate (24–44k backpressure responses per window).

## Allocation evidence and durable-header projection

`cpu-profiles-001` contains separate 99 Hz user-space CPU profiles, not blocked or
kernel time: 728 native / 549 one-member / 916 three-member samples. Raw DWARF
stack-memory captures stay local-only under `.tmp/electric-profiles/`; retained
symbolized samples, tool output and hashes do not include those memory dumps.
Allocation, deserialization and task scheduling appear in the samples, but this
small perturbed profile does not account for every microsecond of the write gap.

`heap-profiles-001` and `002` preserve harness failures: the installed heaptrack
emits `.zst`, does not substitute `%p` in a custom path, and supervisor restart
overwrote unfinished traces. The corrected driver moves the open output to its
recorded PID's name before stopping that process, lets the interpreter drain,
and requires a nonempty report from every original SUT PID. No failed trace is
relabelled a passing profile.

`heap-profiles-003` passes all three profiles. Whole-run allocations divided by
all-phase client acknowledgements are 25.01 natively, 45.79 for one member and
146.24 across three replicas. The one-member WAL reread deserializes 10 owned
Strings/append; the three-member leader deserializes 30 and each follower 20.
These are allocation counts, not seconds saved or unperturbed throughput.

The next change projects HTTP commands after framing: omit `content-length`,
`transfer-encoding`, `expect`, `connection`, `accept` and `user-agent`, which have
no mutation-handler consumer. Keep `Host` (Location/callback semantics), protocol
and unknown headers, and duplicate order. The Lean stable-filter/first-lookup
proof and negative Host mutation preceded implementation. The Rust generator
checks independently labelled retained/discarded fields, duplicates, arbitrary
values/payloads, and the actual bincode round trip. The serialized schema and both
durability barriers are unchanged; existing WAL commands still decode.

`conformance-015` passes 332/332 unchanged tests with subscriptions enabled, zero
failures/skips/todo; 136 Rust tests pass with the same two upstream forensic
helpers ignored. Six process campaigns pass after the projection, including
39 async receipts (21 committed, one rejected, 17 invalidated) and five unknown
HTTP outcomes retained. This remains single-host qualification.

`heap-profiles-004` confirms the expected allocation reduction: 25.01 native,
41.78 one-member and 118.32 aggregate three-member allocations/ack. String
deserializations fall to 6 / 18 / 12 (one member / leader / each follower).
However, `async-writes-003` does **not** establish a throughput improvement:

| Repetition | Native writes/s | One-member writes/s | Native / one-member | Quorum-three writes/s | Async accepts/s | Async backpressure |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 108,169 | 45,299 | 2.39 | 26,061 | 34,302 | 61,973 |
| 2 | 87,782 | 41,083 | 2.14 | 27,531 | 30,504 | 50,886 |
| 3 | 90,595 | 39,847 | 2.27 | 26,110 | 32,138 | 56,829 |

Nine of twelve cells pass; all three async cells fail the zero-error gate.
Every accepted byte drains exactly to each replica. One-member p50 is
4.759–5.243 ms and p99 12.983–15.319 ms; WAL fsyncs/ack are 0.0480–0.0505.
The 30-second client windows, all-phase counters, committed-progress samples,
source/binary/config hashes and failures remain separate in the raw evidence.
Reducing allocations is not proof of performance parity. Journal-stage, durable
wait, reread, apply and snapshot timing still need attribution before the next
storage/scheduling change.

## Phase timing and the next bounded-batch experiment

Optional cumulative `RAFT_TIMING` probes use the existing `stats_secs` switch.
They count completed attempts, including failures, across both groups in each
process. Entry staging includes bincode encoding, index locking and native WAL
framing/write, not durability. The entry wait starts before spawning its flush
waiter, so it includes scheduling and notification latency; the marker wait spans
its native durability await. Neither is pure fsync syscall time. Reread includes
`pread` and decoding. Apply excludes its initial view-lock and marker waits;
snapshot timing starts after acquiring that view lock. Resolve includes queued
consensus, waits, apply and completion. These phases overlap and must not be added
as if they were a CPU profile. Histogram bins end at inclusive 1,2,...,524288 µs
with a final overflow bucket. Cumulative logs retain count, bytes, total/max ns
and all buckets; client/resource windows remain separate.

`write-timings-001` has six passing matched short cells. One-member whole-run
means are 428–548 µs per entry wait, 377–476 µs per marker wait, 46–60 µs per entry
stage, 75–80 µs per reread and 213–224 µs per apply call. Each run makes one
259–302 ms snapshot. Only about 35–44 appends share an entry on average. These
measurements point to durability-wait amortization before another allocation-only
change. `conformance-016` passes 332/332 with zero skips and 137 Rust tests pass.

The next candidate raises the **count ceiling** from 64 to 128 commands per
append batch, keeping the 2 MiB byte ceiling, two locally unflushed batches,
FIFO order, singleton metadata and charged count/byte credits unchanged. No
linger timer, fsync omission or one-member bypass is introduced. `Batches.tla`
already permits any nonempty FIFO append prefix at sealing; its bounded check
has only two append requests, while the Lean fold theorem is length-generic.
Thus neither checks every 128-command interleaving; generated batch/native-WAL
tests and real-process qualification remain the code-level obligations. The
snapshot trigger still counts entries, so larger batches can increase snapshot
bytes and pauses. Bounded receipt history can hold at most 131,072 replies rather
than 65,536; retention remains count-based, not a time guarantee. This candidate
has no claimed performance benefit until new matched measurements complete.

The completed `async-writes-004` run rejects the simple ceiling hypothesis.
One-member batches still average 42.1–45.1 appends and WAL fsyncs/ack are
0.0436–0.0466. Native varies from 75–97k/s, one member 41–48k/s and quorum-three
26–28k/s. The per-repetition ratios (1.95 / 2.31 / 1.56×) do not isolate a gain:
native slowed markedly in repetitions 1 and 3, while batch formation scarcely
changed. All three async cells still fail with 39–42k backpressure responses per
window, despite exact accepted-byte drain. Aggregate async RSS rises to
103–105 MiB from 86–92 MiB in 003. There is no reason to retain a larger memory
bound on this evidence. Its 137 Rust tests, `conformance-017` (332/332, zero
failures/skips), and all six fault campaigns pass; correctness does not establish
the performance benefit. These candidate results remain separate from the
retained default and from the next scheduling experiment.

The next candidate restores the 64-command ceiling and moves the existing single
scheduler yield to the first empty queue **after** collecting a partial append
prefix. A full batch or a metadata singleton seals without yielding. The held
commands/reply owners/credits stay intact while arriving requests join the queue;
after one yield the collector drains again, then seals if still empty. There is
no linger timer, quorum wait, unbounded yield loop or changed local-flush pipeline.
An encountered metadata command or byte boundary still ends the append batch.

Before this change, `Batches.tla` now separates `forming` from the remaining
queue, preserving `Flat(log) ∘ forming ∘ queue = Serial(admitted)` and the original
credit/publication/metadata invariants. The `DropForming` negative mutation loses
the first collected prefix when gathering more arrivals; it must fail safety.
Fair scheduling/gather/seal are still liveness assumptions. The existing Lean
length-generic fold composition theorem covers concatenated batches, not the Rust
future or Tokio scheduler. A directly polled Rust collector test must establish
arrival during the yield, not rely on sleeps to guess that interleaving. Matched
measurements must decide whether this relocation improves actual batch size.

The held-prefix candidate passes 138 Rust tests, `conformance-018` (332 executed
and passed, zero failures/skips), and all six process campaigns. The direct-poll
test observes the drained prefix before yielding, injects later arrivals and a
metadata boundary, and checks that canceled HTTP receivers retain their credits.
The full formal run detects 39 TLC mutations plus the Lean header mutation.

`write-timings-002` does **not** establish a performance improvement. One-member
rates are 46,748 / 47,560 / 46,812 writes/s, versus native 102,911 / 100,474 /
54,804. The apparent third-repetition narrowing comes from native slowing, not
from one member accelerating. Actual batches still average 43.0–43.4 commands;
WAL fsyncs/ack remain 0.0456–0.0458. Mean entry waits are 473–487 µs, marker waits
414–437 µs and apply calls 224–236 µs. Every cell passes the unchanged exact-byte
and zero-error checks, but those checks do not make it a performance win. One
closing client `/proc` observation is unavailable and remains in the raw ledger.
The next diagnostic must separate the synchronous WAL-fsync loop from the
scheduling/notification time included in these durability waits; neither barrier
will be removed on the basis of overlapping phase times.

## Synchronous fsync timing and callback scheduling

`write-timings-003` adds opt-in timing directly around the native committer's
covering `fdatasync` loop, before `publish_durable`. Counters distinguish loops
from segment calls, count only successful loops and exclude async notification.
The unchanged upstream baseline lacks this additional probe; missing timing is
not zero. Its original WAL counters remain available. The new loop timer includes
OS descheduling and is not pure device latency or CPU time.

All six short cells pass. Native measures 104,354 / 108,322 / 108,369 writes/s;
one member measures 55,916 / 48,645 / 44,508. Whole-invocation synchronous loops
average 255 / 295 / 330 µs, entry waits 395 / 462 / 538 µs, and marker waits
342 / 405 / 472 µs. Actual cohorts average 43.4 / 44.0 / 38.6 appends. These
different, overlapping populations cannot be subtracted into an exact additive
cost model, but show both storage and coordination cost. This is diagnosis,
not a claimed timing-probe speedup. `conformance-019` passes 332/332 with zero
failures/skips; 139 replicated and 113 standalone Rust tests pass, with the same
two unchanged forensic helpers ignored in each build.

The next candidate removes the extra task around the Journal's durability wait.
Pinned OpenRaft [awaits append and then LogFlushed](https://github.com/databendlabs/openraft/blob/8815cdba2826f74e848acef361ad03f93bb1c3f8/openraft/src/core/raft_core.rs#L707-L730)
before processing another command. Its [trait contract](https://github.com/databendlabs/openraft/blob/8815cdba2826f74e848acef361ad03f93bb1c3f8/openraft/src/storage/v2.rs#L108-L129)
expressly permits callback-before-return, and both the built-in
[compatibility adapter](https://github.com/databendlabs/openraft/blob/8815cdba2826f74e848acef361ad03f93bb1c3f8/openraft/src/storage/adapter.rs#L150-L158)
and [RocksDB store](https://github.com/databendlabs/openraft/blob/8815cdba2826f74e848acef361ad03f93bb1c3f8/stores/rocksstore-v2/src/lib.rs#L352-L377)
use that ordering. Our fsync still runs on Electric's dedicated OS thread; the
Raft task asynchronously awaits its oneshot, never executes the syscall itself.

The candidate retains `stage → native durability → local receipt notification →
LogFlushed → return`. It does not await quorum before local acceptance, move the
publication marker, or change consensus ordering. Existing Batches/Receipts models
already order acceptance/callback after Flush; removing a task changes scheduling,
not their transitions. Canceling a core can still leave an unknown request outcome;
it cannot justify an early receipt or releasing retained-entry admission debt.
Model checks do not prove Rust cancellation/refinement. Real storage/process
tests and new matched measurements remain required before claiming a benefit.

Inline completion passes 139 Rust tests, `conformance-020` (332/332, zero skips)
and all six fault campaigns. But `write-timings-004` supplies no reason to keep
the optimization: one member measures 44,506 / 38,830 / 33,045 writes/s versus
native 110,107 / 101,214 / 103,449. Synchronous fsync loops also rise to
315–361 µs and entry waits to 507–592 µs. These sequential runs do not isolate a
causal regression in the scheduler, but do not establish the intended benefit.
The candidate and raw qualification stay in history; the retained implementation
restores its separate waiter task. API permission to inline is not evidence of a
throughput gain on this runtime.

The 128→64 ceiling experiment also exposed a compatibility obligation: receipt
decoding uses that ceiling, but the old node identity did not bind it. A directory
from the 128-command candidate could start while its ordinals 64–127 became
invalid requests. The retained format must bind the command ceiling in node
identity and reject earlier experimental directories, under the existing
no-migration/no-mixed-version policy. Changing the marker by hand is not a
migration. Storage qualification must test both an old identity and a changed
ceiling, verifying rejection before any journal or hot-file mutation.

The restored waiter and identity-7 fence pass 139 Rust tests,
`conformance-021` (332 discovered/executed/passed, zero failures/skips/todo),
`storage-fault-014` and `async-fault-007`. Both incompatible identity probes
reject before changing any stored file bytes. The async history checks 271
operations and 39 receipts: 21 committed, one rejected and 17 invalidated, with
five unknown HTTP outcomes retained.

`async-writes-005` is an invalid performance qualification: ENOSPC truncated a
resource sample. Its raw failure and analyzer rejection are retained unchanged.
After removing only retired disposable lab data and debug build cache, the
harness now requires 8 GiB free before each cell and records the actual free
bytes. That is a local measurement guard, not a product storage requirement or
a guarantee against disk exhaustion. Its below/at-threshold regression is part
of 11 passing Python checker/analyzer/harness tests.

The fresh `async-writes-006` matrix has complete samples and the same exact-byte
checks. Nine cells pass; all three async cells fail the zero-backpressure gate:

| Repetition | Native writes/s | One-member writes/s | Native / one-member | Quorum-three writes/s | Async accepts/s | Async backpressure |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 77,145 | 39,414 | 1.96 | 24,429 | 28,199 | 93,542 |
| 2 | 77,562 | 39,869 | 1.95 | 24,825 | 29,481 | 92,102 |
| 3 | 84,016 | 45,731 | 1.84 | 24,605 | 28,188 | 81,258 |

Every accepted byte drains to every replica; there are no other client errors.
This remains shared-host diagnosis, not parity or cloud capacity. One-member
p50 is 4.995–5.367 ms and p99 9.271–15.311 ms; native p50 is 2.519–2.683 ms and
p99 10.631–10.847 ms. Mean one-member cohorts are 39.9–43.1 commands and WAL
fsyncs/ack 0.0457–0.0494, versus native 0.0230–0.0241. Synchronous one-member
WAL loops average 307–346 µs. Only 1.014–1.015 staged records share each fsync,
so the entry and publication marker still rarely share a covering sync.

Three one-member snapshots consume 1.89–2.36 seconds total per invocation;
these pauses matter but cannot alone explain the roughly twofold write gap.
Async apply handles roughly 195–207 commands per call, versus 89–91 for
quorum-three, so raw per-call durations are not comparable. Normalized by
all-phase acknowledgements, leader apply wall time is 13.6–14.2 µs/async accept
versus 5.47–5.77 µs/quorum acknowledgement. Followers are 5.85–6.16 versus
4.21–4.64 µs. These wall-time probes include scheduling/descheduling, not pure
CPU costs. The difference needs profiling rather than assuming receipt-cache
insertion alone causes it. The async logs contain two snapshots per replica,
not a complete explanation of 81–94k backpressure responses.

`cpu-profiles-002` retains new 30-second profiles of all four arms. All four
symbolized exports succeed; the async workload still fails backpressure while
the other workload checks pass. Deserialization, allocation, native apply and
scheduling appear in both replicated modes. `SeqAccess::next_element` accounts
for 2.72% of one-member and 6.33% of aggregate async user-space samples; this
does not count blocked or kernel time or identify every decoded field. There is
no evidence here that receipt-cache insertion alone explains the wall-time gap.

## Bounded native-fsync coalescing candidate

Pinned OpenRaft queues ordered apply without awaiting its completion, then may
stage the following entry (see [apply dispatch](https://github.com/databendlabs/openraft/blob/8815cdba2826f74e848acef361ad03f93bb1c3f8/openraft/src/core/raft_core.rs#L734-L766)).
The prior publication marker and following entry can therefore share a native
WAL fsync. This is an opportunity, not a scheduler guarantee. Entry B's callback
must still wait for a captured prefix that includes B; durability beside marker
A does not make B committed or visible. Separate client entries cannot share a
log-append fsync because the core awaits each preceding `LogFlushed`.

The candidate adds one 100 µs collection interval on the existing dedicated
native committer thread before capturing a dirty prefix for fsync. The interval
is shorter than the measured 307–346 µs mean sync loop and aims to group the
marker/entry scheduling race. It is a measured-next experiment, not a promised
speedup or latency bound. OS scheduling can exceed the requested sleep. New
arrivals do not reset the interval; a single request must progress without a
second record. The already existing shutdown drain remains immediate. Standalone
native mode keeps zero added collection delay. All replicated group sizes and
both acknowledgement modes use the same candidate policy.

`FsyncGroups.tla` models this physical-prefix bridge before implementation:
collection, a pre-fsync cut, racing later writes, completed fsync, notification,
crash and recovery. Safety rejects notification before sync and resampling the
cut after sync; the stuck-timer mutation must violate stable-period liveness.
The model assumes an honest covering fsync and native contiguous-written/segment
selection. Existing Publication/ApplyRecovery/Receipts models and Lean prefix
lemmas still supply the separate logical contracts; none proves the Rust timer,
OS scheduler or storage hardware. Real-WAL tests must exercise a lone request,
segment rolls and shutdown; unchanged conformance and storage/process faults
remain acceptance gates if measurements justify retaining the candidate.

`write-coalescing-002` completes all six short diagnostic cells with no client
errors and exact byte checks. One-member throughput is **32,058 / 32,955 / 31,796
writes/s**, versus native **47,669 / 90,475 / 83,743**. The first native result
shows substantial host/run variance. WAL records/fsync rise to 1.516–1.563 and
fsyncs/ack fall to about 0.030, but entry/marker waits rise to 840–902 µs and p99
to 14.3–14.9 ms. The measured synchronous fsync loops themselves average
527–563 µs, versus 307–346 µs in `async-writes-006`; these runs do not isolate
every source of that difference. Fewer fsyncs did **not** establish a throughput
benefit. The candidate is rejected, with its source and measurements retained
in local history; the qualified immediate-commit implementation is restored.

The preceding `write-coalescing-001` refused all six cells before server startup
because available disk was below the unchanged 8 GiB harness headroom guard.
Its disposal manifest records twelve temporary native-test WAL fixtures removed
after their test processes exited, not result data or subscription signing keys.
The first new property test also failed: its oracle used the consensus-only
record reader on native Append frames. The retained failure and regression seed
precede a corrected independent frame decoder. Corrected candidate tests pass
140 replicated / 114 standalone cases (the same two upstream forensic helpers
remain ignored). All 42 TLC negative mutations and one Lean mutation are
detected. These are not full protocol/fault qualification of the rejected
candidate; `conformance-021` still names the restored engine, not that candidate.

## Upstream pipelining has an admission-contract tradeoff

Authoritative source inspection pins the available 0.10 release to
[`v0.10.0-alpha.36`](https://github.com/databendlabs/openraft/tree/0acd6b8d547ad4468f66708b05bc03baaf04c7c8),
not a stable release. Its
[`run_append_entries`](https://github.com/databendlabs/openraft/blob/0acd6b8d547ad4468f66708b05bc03baaf04c7c8/openraft/src/core/raft_core.rs#L2378-L2399)
returns after staging readable entries and does not await `IOFlushed`. This
would permit multiple entries to share native group fsync without falsely
acknowledging any of them early. The initial upstream implementation is
[`c55a58d`](https://github.com/databendlabs/openraft/commit/c55a58d4c66d2cfac6103a3417ac9c0ba6ff61d3);
it changes IO tracking and callbacks across the core and storage layer, not just
one await. A private one-line removal of the 0.9 wait is not a sound backport.

However, the current 0.10
[`ensure_writable_leader_handler`](https://github.com/databendlabs/openraft/blob/0acd6b8d547ad4468f66708b05bc03baaf04c7c8/openraft/src/core/raft_core.rs#L609-L617)
unconditionally rejects new proposals when the quorum lease expires, even if
the node still reports Leader and has backlog capacity. The
[CheckQuorum contract](https://github.com/databendlabs/openraft/blob/0acd6b8d547ad4468f66708b05bc03baaf04c7c8/openraft/src/docs/protocol/check_quorum.md)
sets that lease to `election_timeout_max` (700 ms with this adapter's current
configuration). `quorum_loss_probe_interval` changes heartbeat suppression, not
the admission predicate; `WriteRequest::with_leader` does not bypass it. A true
single-voter group is its own quorum and is unaffected by this partition case.

That changes the existing async contract: the prepared isolated owner currently
can issue local-fsync receipts until its count/byte allowance is full. With 0.10,
it would stop earlier on lease expiry and resume only after quorum contact is
renewed. Already accepted receipts could still be pending, committed or lost;
committed-only reads and local durability need not change. No upgrade or lease
bypass has been implemented. Whether this narrower minority admission behavior
is acceptable is a product decision before formalizing and qualifying an
upgrade. Source-level pipelining is an opportunity, not measured performance.
