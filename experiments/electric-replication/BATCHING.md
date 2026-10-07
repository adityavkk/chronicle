# Restoring native group-commit amortization

Specification before implementation. This is batching within a real Raft group,
not a one-member bypass or a weaker fsync mode.

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

The adapter currently spends approximately two WAL fsyncs per command: its entry
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
