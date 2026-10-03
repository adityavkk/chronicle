# Chronicle Raft local runbook

## Loss of quorum

Check `kubectl -n chronicle get pods -o wide` and `/admin/status`. Preserve PVCs. Restore failed agents/pods before changing membership; never bootstrap an existing data set. A three-node group requires two healthy members.

## Migration stalled

Inspect the group map and source/destination pod logs. Confirm both nodes are healthy, neither is marked draining unexpectedly, and disk is available. Do not start another migration for the group until its current transition is resolved.

## Backpressure

Identify the affected node and group in metrics, then inspect request latency, disk, and migration activity. Stop load or migration rather than increasing unbounded queues. Capture status and metrics before restarting a pod.

## Telemetry drops

Inspect `kubectl -n observability logs deploy/otel-collector`, then the Victoria backend pod. Check collector memory limiter activity and endpoint availability. Telemetry loss does not establish server data loss.
