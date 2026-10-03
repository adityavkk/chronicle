# Local k3d deployment

This directory deploys a bounded, local-only three-node Chronicle Raft cluster and a Victoria observability stack. It is guarded to the `chronicle-rust` k3d context and creates only `ClusterIP` services: admin endpoints are never externally exposed.

## Prerequisites and lifecycle

The expected cluster is k3d 5.8.3 / k3s v1.32.5-k3s1, one server and three agents, with kubeconfig `/tmp/chronicle-kubeconfig`. Build `chronicle-raft:dev` from this subtree's Dockerfile first.

```sh
export KUBECONFIG=/tmp/chronicle-kubeconfig
./ops/cluster.sh create
NEW_GENESIS=1 ./ops/setup.sh   # FIRST genesis only; separate explicit storage-init Jobs
./ops/bootstrap.sh             # one explicit, in-cluster bootstrap
./ops/teardown.sh              # deletes both namespaces and all local data
```

For ordinary restarts, preserve all PVCs and use Kubernetes restart/rollout; do
not run genesis again. `CLUSTER_ID` and `NODE_ID` must match `identity.json`.
The binary validates all databases before starting any Raft group. Missing or
truncated storage fails closed. Initialization Jobs never overwrite existing data.

Schema-changing binary upgrades require stopping **all** Chronicle processes,
preserving PVCs, selecting the new pinned image, and then starting them together.
In particular the retirement-history metadata cannot be rolled out with mixed
revisions. Do not confuse an ordinary same-image restart with a binary upgrade;
mixed-version snapshot/log compatibility is not established.

For a fourth **fresh** ordinal after provisioning another k3d agent:

```sh
kubectl -n chronicle scale sts chronicle --replicas=4
python3 ops/initialize.py learner 3
kubectl -n chronicle rollout status sts/chronicle
```

The Job reserves fresh ID 4 in the control group before initializing its disk.
Replica placement and learner catch-up follow automatically. On disk loss, use
a **new ID and address**, not a recreated empty PVC at the old ordinal. An unknown
admission outcome may have consumed the ID: inspect control state and choose a
fresh ID rather than retrying initialization blindly. Partial initialization
without the durable identity file is not restartable. Genesis is trusted operator
authorization for a brand-new cluster, never a recovery or disk-replacement mode.

Drain through `/admin/register`, preserving the registered address and setting
`draining:true`; wait until `/admin/control` shows complete placements excluding
that ID and `/admin/retirement/{id}` returns true before stopping it. Kubernetes
`drain` alone does not remove Raft membership. Keep three healthy destinations.
`python3 ops/drain.py --url "$PRIVATE_CHRONICLE_URL" --node 4` performs the
request once and polls verified retirement. It never deletes storage or retries
an ambiguous mutation. Voter replacement completes without the departing node;
graceful retirement additionally requires that process to report durable applied
nonvoter membership covering its last demotion, or recovery-confirmed empty state
for a never-assigned group. Unreachable learners may be pruned to release resources,
but that is not graceful retirement. Preserve their volume and keep them fenced.
Returning obsolete voters are temporarily re-added as learners, demoted and pruned.
Do not undrain a node concurrently with stopping it: that explicitly permits a
new assignment. Cleanup is rate-limited and fair, not resource-informed balancing.

Every Raft RPC checks intended cluster/node against local persisted identity, so
DNS aliases cannot be counted as additional replicas. This is not authentication.
Cloned volumes, external lock-file replacement, and mixed old non-locking binaries
are unsupported. The recipient-check rollout used a stop-all/start-all transition;
rolling compatibility with the earlier development protocol is not claimed.

For opt-in local access, run foreground port-forwards (they are not started by scripts):

```sh
kubectl -n chronicle port-forward svc/chronicle-http 8080:8080
kubectl -n observability port-forward svc/grafana 3000:3000
kubectl -n observability port-forward svc/victoria-metrics 8428:8428
kubectl -n observability port-forward svc/victoria-logs 9428:9428
kubectl -n observability port-forward svc/victoria-traces 10428:10428
```

Grafana is anonymous read-only because it remains ClusterIP/local port-forward only. Its provisioned dashboard and Prometheus, VictoriaLogs, and Jaeger-compatible VictoriaTraces data sources are useful starting points, not production security configuration. Observability uses ephemeral bounded storage (7-day retention, 1 GiB emptyDir); Chronicle uses 1 GiB local-path PVCs and required hostname anti-affinity.

The collector discovers replicas through the headless service's HTTP SRV record,
not a static seed list. Joined replicas therefore receive metrics coverage without
Kubernetes API credentials. `evidence/victoria-dns-nodes.json` records all four
pods and 20 node/group applied-index series after fourth-node admission/drain.
Drained replicas may retain older applied indices; those gauges are not read frontiers.

## Binary contract

The image contains `/bin/sh` and `/usr/local/bin/chronicle-raft`. The StatefulSet derives `NODE_ID` (ordinal + 1) and `ADVERTISE`; it supplies `CLUSTER_ID`, `LISTEN`, `DATA_DIR`, the three-node `CLUSTER_NODES` JSON map, and the collector endpoint. The binary exports OTLP/HTTP logs and traces to port 4318 (legacy 4317 configuration is translated); the collector scrapes metrics. `POST /admin/bootstrap` is restricted to the first persisted genesis seed and initializes all five groups. `/healthz` means the process responds without a published Raft storage-fatal error, **not that quorum is available**. A storage-fatal error in any group terminates the shared process with exit 1. Quorum, migration and admission depth have separate metrics.

`alerts.yaml` is a portable Prometheus/vmalert rule file; no notification receiver is fabricated for local use. See [RUNBOOK.md](RUNBOOK.md).

## Immutable components and protocol basis

Manifests use linux/amd64-resolved Docker Hub digests: VictoriaMetrics v1.153.0, VictoriaLogs v1.53.0, VictoriaTraces v0.12.0, OTel Collector Contrib 0.137.0, Grafana 13.2.3, and curl 8.16.0. The authoritative Victoria documentation confirms OTLP metrics at `/opentelemetry/v1/metrics`, logs at `/insert/opentelemetry/v1/logs`, traces at `/insert/opentelemetry/v1/traces`, and VictoriaTraces' Jaeger query API:

- <https://docs.victoriametrics.com/opentelemetry/>
- <https://docs.victoriametrics.com/victorialogs/data-ingestion/opentelemetry/>
- <https://docs.victoriametrics.com/victoriatraces/data-ingestion/opentelemetry/>

`verify.sh` sends one explicitly labelled synthetic setup log and confirms it can be queried. It must never be reported as Chronicle server telemetry.
