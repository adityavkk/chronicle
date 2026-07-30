# Chronicle topology scaling research

## Status

Research is complete enough to choose the experiment contract. No new cloud
benchmark has run for this study.

## Question

Find the smallest Chronicle and Redis topology that approaches the durable Rust
or Ursula results across every ds-bench workload. Test whether more Chronicle
replicas help live SSE traffic and whether Redis sharding helps workloads spread
over many streams.

## What the published campaign tested

The published campaign used one Chronicle process and one colocated standalone
Redis process. Both ran in one pod on one server node. Chronicle and Redis shared
4 vCPUs, 16 GiB of memory, and one local NVMe device.

The campaign tested three CPU splits inside that one topology:

| Chronicle CPU | Redis CPU | Median append/s | Result |
| ---: | ---: | ---: | --- |
| 1 | 3 | 19.5k | Valid, slower |
| 2 | 2 | 33.9k | Selected |
| 3 | 1 | 0 at the last rung | Invalid creation choke |

The campaign did not test multiple Chronicle replicas, multiple Redis masters,
or a larger total SUT budget.

## Current gap to the durable references

The table uses Chronicle AOF `always`, Rust WAL, and Ursula disk. Rust's 100,000
stream write result is a lower bound because its load ladder ended before a
plateau.

| Workload and cell | Chronicle | Rust WAL | Ursula disk |
| --- | ---: | ---: | ---: |
| Write, 100,000 streams | 34.6k append/s | at least 64.3k | 4.9k |
| Blog SSE, 1,000 subscribers | 7.4k events/s | 50.1k | 50.0k |
| SSE scale, 2,048 clients and 100 streams | 10.8k events/s | 102.5k | 80.1k |
| Catchup, 512 readers and 100 streams | 56.5 MiB/s | 2.8 GiB/s | 2.7 GiB/s |
| Mixed writes, 100,000 readers | 5.6k writes/s | 50.0k | 1.4k |
| Mixed delivery, highest clean offered rate | 4.0k writes/s | 66.0k | 4.0k |

Chronicle already exceeds Ursula disk on standalone writes and mixed writes. It
roughly matches Ursula disk's clean mixed delivery rate. The largest gaps are
SSE fanout, SSE connection scale, and catchup.

## Evidence from the archived server samples

The existing metrics poller combined Chronicle and Redis process CPU. It did not
record them separately. A diagnostic pass over the archived samples calculated
CPU from changes in Linux process ticks during active intervals. These values
include warmup and are not new publication metrics, but they show whether the
combined 4 vCPU limit was continuously full.

| Cell | Average active CPU | Highest one second CPU | Peak working set |
| --- | ---: | ---: | ---: |
| Write, 100,000 streams | 1.59 cores | 2.89 cores | 598 MiB |
| Blog SSE, 1,000 subscribers | 1.63 cores | 2.95 cores | 230 MiB |
| SSE scale, 2,048 clients and 100 streams | 2.20 cores | 3.03 cores | 297 MiB |
| Catchup, 512 readers and 100 streams | 1.44 cores | 2.15 cores | 5.0 GiB |
| Mixed writes, 100,000 readers | 1.67 cores | 2.84 cores | 2.4 GiB |
| Mixed delivery, 66,000 offered writes/s | 1.91 cores | 2.85 cores | 533 MiB |

The combined samples never show sustained use of all 4 vCPUs. This does not
prove spare capacity. Chronicle and Redis each had a separate 2 vCPU limit, so
one container may have been throttled while the other had unused CPU.

The SSE result is already incomplete at low connection counts:

| Connections | Delivered events/s | Declared events/s | Completion | Average active CPU |
| ---: | ---: | ---: | ---: | ---: |
| 64 | 1.4k | 3.2k | 44.3% | 1.33 cores |
| 256 | 3.6k | 12.8k | 28.0% | 1.50 cores |
| 1,024 | 6.9k | 51.2k | 13.5% | 1.61 cores |
| 2,048 | 10.8k | 102.4k | 10.5% | 2.20 cores |

This weakens the claim that more Chronicle replicas under the same total CPU
will close the SSE gap by themselves. The missing per-process CPU split leaves
three plausible causes: Chronicle reaches its own 2 vCPU limit, Redis reaches
its own limit, or the request path spends most of its time on serialized Redis
round trips and subscription setup.

## Relevant implementation facts

- [`benchmarks/ds-bench/overlay/gke/chronicle.yaml`](../../../../benchmarks/ds-bench/overlay/gke/chronicle.yaml)
  places Chronicle and Redis in one pod. Redis listens only on
  `127.0.0.1`. Increasing this Deployment's replica count would create a
  separate database for each Chronicle replica. The Kubernetes Service would
  then route requests to inconsistent stores.
- [`cmd/chronicle/main.go`](../../../../cmd/chronicle/main.go) accepts a standalone
  `redis://` URL and a `redis+cluster://` seed list. The server already uses the
  go-redis cluster client when it receives the cluster URL.
- [`store/redis/keys.go`](../../../../store/redis/keys.go) places every key for one
  stream in one Redis hash slot. A Redis Cluster can spread different streams
  over masters. It cannot split one stream across masters.
- [`handler_sse.go`](../../../../handler_sse.go) gives every SSE connection its own
  read and wait loop. The loop waits for at most 100 ms before it reads again.
- [`store/redis/notify.go`](../../../../store/redis/notify.go) creates a Redis
  subscription for each wait and closes it when the wait ends. At 2,048 idle
  SSE connections, the 100 ms loop can create about 20,000 subscription cycles
  per second before counting reads and metadata checks.
- The Redis Lua scripts use `PUBLISH`, and the wait path uses `SUBSCRIBE`. They
  do not use Redis 7 shard channels through `SPUBLISH` and `SSUBSCRIBE`.
  [Redis documents](https://redis.io/docs/latest/develop/pubsub/) that global
  Pub/Sub propagates messages across the cluster, while shard Pub/Sub confines
  messages to the owning shard.
- Redis documents that a cluster that works as expected has at least three
  masters. It recommends three masters and three replicas for production.
  This benchmark can use three masters without replicas because the published
  standalone Redis arm also has no replica. The experiment measures throughput,
  not availability. See
  [Scale with Redis Cluster](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/).
- Chronicle used at most 654 MiB of pod working set in the reported write
  cells. More memory alone is unlikely to explain the write gap. Catchup keeps
  up to 1.6 GiB of declared stream data at the 100 stream setup, so memory
  cannot be reduced from the current 16 GiB without a separate validation run.

## Hypotheses

| ID | Change | Expected benefit | Expected limit |
| --- | --- | --- | --- |
| H1 | Run 2 or 4 Chronicle replicas against one shared Redis | Spread HTTP handling, response formatting, open SSE sockets, and go-redis client work across Go processes | Redis still handles the same reads and subscription churn |
| H2 | Run three Redis masters with one Chronicle replica | Spread writes, reads, and AOF work when a workload uses many streams | One stream stays on one master, so blog fanout should not improve much |
| H3 | Combine 2 or 4 Chronicle replicas with three Redis masters | Spread both application work and multi-stream storage work | Global Pub/Sub still crosses the cluster, and all processes still recreate subscriptions |
| H4 | Add total CPU after testing the fixed budget | Show whether the design scales when the current 4 vCPU budget is the limit | These results cannot replace the equal 4 vCPU headline comparison |
| H5 | Reduce the 100 ms SSE subscription churn in a separate code variant | Test whether the live workload gap comes from the wait algorithm instead of topology | This changes Chronicle code, so it must not be described as a configuration result |
| H6 | Add memory after CPU and topology tests | Help only if a candidate reaches memory pressure | Existing write measurements do not show memory pressure |

The main prediction is workload specific:

- More Chronicle replicas should help live SSE more than write saturation.
- Redis shards should help write, catchup, and mixed workloads that use many
  streams.
- Redis shards should have little effect on the one stream blog fanout cell.
- A configuration only search may fail to close the SSE gap because the current
  wait loop recreates subscriptions every 100 ms.

## Proposed comparison contract

Use AOF `always` for every Chronicle candidate. Do not use `everysec` to claim
parity with Rust WAL or Ursula disk.

Test fixed resource configurations first:

- Keep 4 total SUT vCPUs, 16 GiB of memory, and one local NVMe device.
- Keep all SUT pods on the same server node.
- Split the same total Chronicle CPU over 1, 2, or 4 Chronicle replicas.
- Compare one standalone Redis process with three Redis Cluster masters.
- Give all Redis masters together the same Redis CPU and memory assigned to the
  standalone Redis process.
- Place every Redis AOF directory on the same local NVMe filesystem. Sharding
  must not add storage devices.

Only if no fixed resource candidate meets the target, increase the total SUT CPU
in steps. Keep those results in a separate scale out table. After finding a CPU
and topology candidate, reduce memory in steps to find the smallest valid
memory limit.

The first fixed budget screen should contain these six topologies:

| Chronicle replicas | Redis masters | Total Chronicle CPU | Total Redis CPU |
| ---: | ---: | ---: | ---: |
| 1 | 1 | 2 | 2 |
| 2 | 1 | 2 | 2 |
| 4 | 1 | 2 | 2 |
| 1 | 3 | 2 | 2 |
| 2 | 3 | 2 | 2 |
| 4 | 3 | 2 | 2 |

Run a short discriminator set on all six topologies:

- Write saturation at 100,000 streams.
- Blog SSE at 1,000 subscribers.
- SSE scale at 2,048 clients and 100 streams.
- Catchup at 512 readers and 100 streams.
- Mixed writes at 100,000 readers.
- Mixed delivery at the current 4k, 16k, and 66k offered write rates.

Run the full ds-bench matrix only for candidates that are not dominated on both
performance and resources. Repeat qualifying cells with the same confirmation
rules as the published campaign.

Before comparing topologies, rerun the current 1 Chronicle and 1 Redis control
with separate CPU counters. If Chronicle reaches its 2 vCPU limit while Redis
does not, prioritize Chronicle replicas and Chronicle CPU. If Redis reaches its
limit, prioritize shards and Redis CPU. If neither reaches its limit, prioritize
the SSE wait loop diagnostic because adding processes is unlikely to remove
serialized Redis round trips.

## Proposed meaning of close and minimal

The proposed durable target is the better measured result from Rust WAL or
Ursula disk for each matching cell. A Chronicle candidate is close only when
every required cell meets all of these rules:

- Throughput or delivered event rate is at least 80 percent of the target.
- Offered load completion is at least 98 percent.
- The cell has zero request errors and passes every existing setup check.
- p50 and p99 are no more than twice the target when the target records that
  latency.
- Peak pod working set is no more than twice the lower durable reference and
  remains within the candidate's declared memory limit.

This is an aggressive target because it forms an envelope from two systems.
Neither durable reference must be best at every workload. If no Chronicle
configuration qualifies, the report should name the closest Pareto candidate
and the cells that still fail. It should not weaken the threshold after seeing
the data.

Among qualifying candidates, minimal means:

1. Lowest total requested SUT vCPU.
2. Lowest total requested SUT memory.
3. Fewest Redis masters.
4. Fewest Chronicle replicas.

The report must show absolute performance and performance per requested vCPU.
Added compute results remain separate from the original equal resource
comparison.

## Measurements needed to test the hypotheses

The current poller assumes one SUT pod. A multi-pod run needs:

- Total and per-pod Chronicle CPU, memory, and connection counts.
- Total and per-master Redis CPU, memory, network traffic, AOF writes, and
  command counts.
- Redis `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, read, and Lua command rates.
- Redis Cluster slot and per-master stream distribution.
- The number of ready Chronicle endpoints and the SSE connection count on each
  endpoint.
- One aggregate SUT working set and write byte count for comparison with the
  existing report.

Each cell must start with one empty shared Redis store. Restarting a Chronicle
pod must not be used as a data reset once Redis is separate.

## Risks

- Kubernetes Service balancing can put most long lived SSE sockets on one
  Chronicle replica. Record the connection count per replica before using a
  result.
- Three Redis masters on one node test software sharding and parallel Redis
  event loops. They do not test more disks or more machines.
- Global Redis Pub/Sub can add cluster bus work. A shard count increase may
  reduce key command pressure while increasing Pub/Sub overhead.
- A fixed 2 vCPU Chronicle total divided over four replicas gives each process
  0.5 vCPU. Scheduler and runtime overhead may make that candidate slower even
  if multiple processes remove a connection bottleneck.
- Redis Cluster initialization and reset add failure modes to the harness. A
  cell is invalid unless all slots are covered and every Chronicle endpoint
  uses the same cluster.
- A code variant that changes the SSE wait loop can answer the bottleneck
  question, but its results are not topology only results.
- Paid cloud runs are not covered by the earlier authorization for the four
  catchup suites. A new campaign needs an explicit scope and approval.

## Decisions needed before planning

1. Approve or change the proposed target. The default compares Chronicle with
   the better durable result in each cell and uses the 80 percent throughput and
   two times latency thresholds.
2. Approve fixed budget testing before added compute.
3. Decide whether the SSE wait loop code variant belongs in this study as a
   diagnostic arm. The default includes it after the topology screen, but never
   mixes it with configuration only results.
