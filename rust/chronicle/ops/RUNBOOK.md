# Chronicle Raft local runbook

## Loss of quorum

Check `kubectl -n chronicle get pods -o wide` and `/admin/status`. Preserve PVCs. Restore failed agents/pods before changing membership; never bootstrap an existing data set. A three-node group requires two healthy members.

## Storage-fatal exit

Any Raft group's typed storage-fatal error terminates all groups in the process
with exit 1. Inspect the previous container log and termination status; the final
log event is best effort and may be dropped. The supervisor restarts the same
identity/PVC. Restore storage access/capacity before repeated restarts; never
delete SQLite/WAL, identity or lock files to make startup succeed. A lost or
corrupt disk requires the explicit fresh-identity replacement procedure, not
genesis initialization or a copied live identity. Quorum loss alone is not this
trigger. Hung I/O that never returns an error is not detected by the watcher.

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
Duration begins in admission middleware; the event is emitted when the observed
handler/response body is dropped, with complete, body-error or cancelled delivery.
Yielded bytes are not proof of client receipt. Early extractor/admission rejections
do not yet produce completion events. No payload, raw path, or producer ID is logged.

## Grafana panels suddenly show no data

Check datasource query errors, Grafana pod termination reason and the supervised
port-forward before concluding that Victoria lost data. A local dashboard refresh
exposed `OOMKilled` at the former 384 MiB Grafana limit; the restarted instance was
already near 365 MiB. The local manifest now requests 256 MiB and limits 768 MiB.
The replacement rendered all six panels and sustained repeated queries without
restart during a four-minute check, reaching 498 MiB reported memory. This
is measured headroom, not a long-duration memory-leak diagnosis or production
sizing guarantee. The forwarding service reconnects after pod replacement;
wait for Grafana provisioning and successful datasource queries as well as Pod
Ready. API health alone can precede provisioned dashboard/datasource readiness.
