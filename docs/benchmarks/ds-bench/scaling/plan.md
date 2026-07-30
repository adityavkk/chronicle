# Chronicle topology scaling

## Summary

Extend the ds-bench adapter so one suite can compare Chronicle replica counts,
standalone Redis, and a three-master Redis Cluster under one declared resource
budget. Preserve the published colocated configuration. Add a separately labeled
persistent SSE wait mode to test whether subscription churn, rather than process
count, limits live delivery. Run a six-topology screen before adding CPU, then
select the smallest candidate that meets the approved performance contract.

## Domain model

### Nouns

- `ChronicleTopology(chronicle_replicas: int, redis_masters: int,
  chronicle_cpu_millis: int, redis_cpu_millis: int,
  chronicle_memory_mib: int, redis_memory_mib: int,
  sse_wait_mode: str)` describes one SUT configuration.
- `PerPodResources(cpu_millis: int, memory_mib: int)` is the uniform limit for
  one Chronicle or Redis pod. Integer division may leave at most two millicores
  or two MiB unused. It must never exceed the declared total.
- `TopologyKind` is `colocated`, `shared`, or `cluster`.
- `SUTSample(component: str, pod: str, ts_ms: int, rss_bytes: int,
  cpu_ticks: int, write_bytes: int, pod_ws_bytes: int)` is one raw server sample.
- `ScalingCandidate(topology: ChronicleTopology, cells: list[ValidatedCell])`
  owns one configuration's results across all required workloads.
- `ClosenessTarget(throughput_ratio: float = 0.80,
  completion_ratio: float = 0.98, latency_ratio: float = 2.0)` is frozen before
  execution.

### Verbs

- `parse_topology_args(value: str) -> ChronicleTopology` parses the extended
  Chronicle server configuration while retaining the legacy three-field syntax.
- `validate_topology(topology, budget) -> None` rejects resource overcommit,
  unsupported replica counts, and Redis master counts other than one or three.
- `render_topology(topology) -> KubernetesResources` selects the shared or
  cluster manifest and calculates per-pod limits.
- `reset_topology(topology) -> None` recreates Redis storage, establishes all
  cluster slots when needed, restarts Chronicle, and proves every endpoint ready.
- `aggregate_sut_samples(raw_files) -> samples.csv` sums CPU, RSS, and pod
  working set by time bucket while counting device write bytes once.
- `qualifies(candidate, target, references) -> bool` applies the approved
  throughput, completion, error, latency, and memory rules to every required cell.
- `select_minimal(candidates) -> ScalingCandidate | None` orders qualifying
  candidates by total CPU, memory, Redis masters, and Chronicle replicas.

### Boundaries

- `benchmarks/ds-bench/dsbench.py` owns topology types, campaign resolution,
  frozen provenance, validation, and selection.
- `benchmarks/ds-bench/patches/chronicle.patch` owns upstream deployment, reset,
  per-pod collection, and aggregation behavior.
- `benchmarks/ds-bench/overlay/gke/` owns Chronicle and Redis Kubernetes
  resources. Every SUT pod uses the same server node selector.
- `benchmarks/ds-bench/scaling.json` owns the fixed six-topology screen and the
  approved target.
- Chronicle runtime code owns the optional persistent SSE waiter. The default
  remains the current behavior.
- Existing sealed result archives remain immutable reference evidence.

## Slices

### Slice 1: Shared standalone Redis tracer

- Add `ChronicleTopology` parsing and resource validation to
  `benchmarks/ds-bench/dsbench.py`.
- Add a shared standalone Redis manifest and a variable Chronicle replica count.
- Keep the legacy colocated manifest and configuration syntax unchanged.
- Recreate Redis before each cell, restart all Chronicle replicas, and verify the
  Service has the requested ready endpoints.
- Add adapter and upstream shell tests for one Chronicle replica and one shared
  Redis process.

### Slice 2: Three-master Redis Cluster

- Add three Redis cluster pods, a headless Service, and an idempotent cluster
  initialization Job.
- Require all 16,384 slots to be covered before Chronicle starts.
- Use Chronicle's existing `redis+cluster://` client URL.
- Reset every Redis data volume and rebuild the cluster between cells.
- Test rendered resources, cluster seed URLs, resource totals, reset order, and
  failure when slot coverage is incomplete.

### Slice 3: Multi-pod evidence

- Collect raw samples from every pod labeled `ds-bench-sut=chronicle`.
- Preserve each raw file with component and pod identity.
- Produce the existing aggregate `samples.csv` format for upstream reports.
- Capture Redis `INFO commandstats`, `INFO clients`, `INFO persistence`, and
  `CLUSTER INFO` after each cell.
- Reject a cell when the requested Chronicle endpoint count or Redis master
  count is missing.

### Slice 4: Fixed-budget scaling campaign

- Add `benchmarks/ds-bench/scaling.json` with the six approved topologies.
- Resolve six discriminator suites: write, blog SSE, SSE scale, catchup, mixed
  writes, and mixed delivery.
- Freeze topology metadata, exact effective resources, image digests, target
  thresholds, and durable reference archive digests in the manifest.
- Add report logic that evaluates every topology against Rust WAL and Ursula
  disk and identifies dominated candidates.

### Slice 5: Persistent SSE wait diagnostic

- Add an optional Redis notification subscription that stays open for one SSE
  connection.
- Keep the current read and wait loop as the default.
- Add a runtime flag and an extended topology argument that enables the new path.
- Test missed-wakeup protection, append and close ordering, cancellation,
  reconnect polling, and one subscription lifecycle per SSE connection.
- Label these results as a code diagnostic. Do not mix them with topology-only
  results.

### Slice 6: Verification and execution

- Run adapter tests, every pinned upstream test, Chronicle unit tests, and Redis
  integration tests.
- Run the local shared and cluster smoke paths when the local container runtime
  is available.
- Freeze the exact paid six-suite screen and estimated maximum cluster hours.
- Obtain explicit approval for the new paid screen, then run it sequentially with
  the existing watchdog and absence proofs.
- If no 4 vCPU candidate qualifies, generate the next 6, 8, and 12 vCPU screen
  only from non-dominated topologies and request approval for that added spend.
- Publish the minimal qualifying candidate or the closest non-qualifying
  candidate with every failed cell shown.

## Risks and mitigations

- A Kubernetes Service can distribute long-lived sockets unevenly. Record ready
  endpoints and socket counts per Chronicle pod.
- Global Redis Pub/Sub can make three masters slower for fanout. Keep one stream
  fanout as a required discriminator.
- Three Redis masters share one local NVMe device. Record this as software
  sharding, not three-device storage scaling.
- Per-pod integer limits can leave a small amount unused. Record effective totals
  and never round upward.
- A Redis Cluster reset can leave old membership state. Delete and recreate each
  data volume, then require full slot coverage.
- The repository forbids agent-authored commits. Implement and test each slice
  without creating commits.

## Open questions

None. The user approved the target, fixed-budget-first order, and separate SSE
diagnostic on July 28, 2026.

## Out of scope

- Redis replicas or failover measurements.
- More than one physical server node or more than one local SSD.
- Presenting added CPU results in the equal-resource headline table.
- Redis shard Pub/Sub changes. The first cluster screen measures current global
  Pub/Sub behavior.
