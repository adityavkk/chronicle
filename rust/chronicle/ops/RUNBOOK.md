# Chronicle Raft local runbook

## Loss of quorum

Check `kubectl -n chronicle get pods -o wide` and `/admin/status`. Preserve PVCs. Restore failed agents/pods before changing membership; never bootstrap an existing data set. A three-node group requires two healthy members.

## Migration stalled

Inspect the group map and source/destination pod logs. Confirm both nodes are healthy, neither is marked draining unexpectedly, and disk is available. Do not start another migration for the group until its current transition is resolved.

## Backpressure

Identify the affected node and group in metrics, then inspect request latency, disk, and migration activity. Stop load or migration rather than increasing unbounded queues. Capture status and metrics before restarting a pod.

## Telemetry drops

Inspect `kubectl -n observability logs deploy/otel-collector`, then the Victoria backend pod. Check collector memory limiter activity and endpoint availability. Telemetry loss does not establish server data loss.

`chronicle_telemetry_dropped_total` counts completion queue overflows/closure;
`chronicle_telemetry_export_errors_total` counts failed OTLP HTTP exports;
`chronicle_log_dropped_total` counts nonblocking stdout queue drops. These are
separate signals. The 1,024-completion OTLP queue and 4,096-line stdout queue are
lossy: consensus never awaits either exporter. An outage/overflow regression
checks that behavior. Incoming W3C trace context and sampled flags are preserved;
forwarding creates child spans, and request IDs correlate both completion events.

The storage histograms measure completed actor queue waits, log transactions, and
apply transactions; RPC includes response decoding. Proposal-through-apply includes
queueing, quorum and persistence, and records timeouts too: it is **not pure
replication latency**. Completion events carry barrier/read/proposal/forward timings,
request and response byte counts, duplicate result, and response frontier/commit
index when present. Sampled term/applied metrics are diagnostic, not read authority.
Batch persistence timings are not falsely attributed to individual requests.
Duration begins in admission middleware and ends when the response is constructed,
not when the last byte reaches the client. Early extractor/admission rejections do
not yet produce completion events. No payload, raw path, or producer ID is logged.
