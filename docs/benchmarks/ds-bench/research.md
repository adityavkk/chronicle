# Chronicle on ds-bench: research

## Context

- Electric published the benchmark method and results in [Durable Streams at kernel speed](https://electric.ax/blog/2026/06/26/durable-streams-at-kernel-speed). The post compares the Rust Durable Streams server, the Node reference server, and Ursula.
- Electric publishes the harness in [`electric-sql/ds-bench`](https://github.com/electric-sql/ds-bench). The current revision inspected for this work is `93a1a066a511ad2ce5114dc429afb1fd0f6d99bf`, dated 2026-07-23.
- The current upstream harness does not support Chronicle. It supports the Rust server, Node, Ursula, and S2. A server integration needs a Kubernetes manifest, an image variable, a deployment mode, a reset path, and a target URL plus API style.
- Chronicle already speaks the `durable` API style that `ds-bench` uses. Both use `/v1/stream/{stream}` for create, append, replay, long poll, and SSE operations. Chronicle needs no protocol-specific client path. The shared catchup setup code is hardened for every server as described below.
- Chronicle already has `loadgen/`, which measures realistic Durable Streams sessions and subscription behavior. It is useful for Chronicle development, but it is not a substitute for `ds-bench` because its workload definitions and saturation method differ.

## Benchmark contract

The comparison must run all systems from the same pinned `ds-bench` checkout. It must not compare a new Chronicle run with numbers copied from the June report.

The upstream repository now states that its June 25 and June 30 datasets predate two correctness fixes:

1. A fleet start barrier and a measurement window alignment check prevent throughput from being counted more than once when client pods start at different times.
2. The report takes latency from the largest rung at or below 80 percent of peak throughput. Latency at saturation mostly measures client queueing.

The current canonical method also requires:

- A bounded concurrency client pool whose stream domain is split across pods.
- Stream creation before the measured window.
- `lazy_creates = 0`.
- Two consecutive low gain rungs before declaring a plateau.
- At least two confirmation runs for a publishable ceiling.
- Aligned measurement windows.
- A clear distinction between a measured plateau and a ladder bound.
- Pod working set memory, which includes active page cache.
- Raw histograms, per cell samples, image digests, suite files, hardware, and commit hashes in the archived result.
- Exact catchup setup. Every stream must be probed at the declared seed size before measurement. A failed append must be retried from a fresh probe.
- Separate setup validity from overload. Invalid setup cannot support a comparison. A valid cell that does not complete its offered work remains an overload observation.

## Scope

The campaign should run the union of the current maintained `ds-bench` suites and the four workloads in the blog.

| Workload | Main result |
| --- | --- |
| Write saturation | Append records per second for 256 byte records at 10,000 and 100,000 streams |
| Memory under write load | Median and peak pod working set for each write cell |
| SSE fanout | Delivery p50 and p99 for one writer at 50 events per second and 1, 10, 100, and 1,000 subscribers |
| Catch up | Aggregate replay MiB per second and per client p50 and p99 for a 16 MiB stream written in 4 KiB records |
| Read scalability | Catch up and live SSE performance as connection count increases |
| Mixed interference | Write throughput and SSE delivery while catch up readers or writers add load |

The comparison set from the blog is:

- Rust Durable Streams in WAL and memory modes.
- Node reference server in memory mode.
- Ursula in disk and memory modes.
- Chronicle with Redis AOF `appendfsync always`.
- Chronicle with Redis AOF `appendfsync everysec`, reported as a weaker durability arm.

S2 is outside the blog comparison. The integration should leave it available through upstream commands, but the default blog campaign should not add it to the headline table.

## Chronicle deployment

The official harness measures one server node. Chronicle needs Redis, so the Chronicle deployment must place Chronicle and Redis in one Kubernetes pod on that node.

The pod must meet these conditions:

- Chronicle and Redis share the same total CPU and memory limit used for one competing server.
- Redis stores its AOF on the server node's local SSD.
- Redis uses `maxmemory-policy noeviction`.
- A pod restart clears both Chronicle and Redis state between cells.
- The metrics sidecar includes both Chronicle and Redis process CPU and resident memory.
- The pod working set includes both containers and the Redis page cache.
- The result records the Chronicle and Redis CPU split.
- The result records the Redis image digest and full persistence configuration.

`appendfsync always` waits for a local AOF flush before Redis replies. It is the closest Chronicle configuration to a single node server that acknowledges only durable writes. `appendfsync everysec` can lose acknowledged writes after a crash, so it must not be presented as equal durability to the Rust WAL arm or Ursula disk arm.

The colocated Redis deployment measures Chronicle as a single node system. It does not represent Chronicle's production managed Redis topology. A later managed Redis campaign can measure that topology, but its numbers belong in a separate table because the hardware and failure model differ.

## Apples to apples resource profile

The primary comparison will use the blog's single node shape for every system:

- 4 total SUT vCPUs.
- 16 GiB total SUT memory.
- One local NVMe SSD where the system needs disk.
- The same client machine type, client fleet limits, zone, payloads, stream counts, measurement windows, and repetition rules.

Chronicle and Redis together count as one SUT. Their container CPU limits must add to 4 vCPUs and their memory limits must add to 16 GiB. The report must use the whole pod's working set, not Chronicle's process memory alone.

Chronicle needs a short calibration before the full campaign. The calibration will test Chronicle to Redis CPU splits of 1 to 3, 2 to 2, and 3 to 1 while keeping the total at 4 vCPUs. It will choose one split using a declared rule: the highest median valid append throughput across repeated 10,000 stream cells, with zero request errors, zero lazy creates, aligned windows, and no client bound result. The selected split then stays fixed for every Chronicle workload and stream count. All calibration results remain in the published dataset.

Electric's maintained Rust canonical suite uses 8 pinned vCPUs and six local SSD devices. Those upstream canonical runs remain useful as a separate reference and regression check. Their numbers must not share a headline table with the 4 vCPU, one SSD comparison.

## Proposed repository boundary

The Chronicle repository should own a small adapter under `benchmarks/ds-bench/`.

The adapter should:

- Pin the upstream URL and full commit.
- Clone the upstream repository into the ignored `.tmp/` directory.
- Apply a small, reviewable patch that adds the Chronicle mode.
- Copy Chronicle manifests and suites into the prepared checkout.
- Build a Chronicle image from the current worktree.
- Run local smoke suites and remote measurement suites through upstream `scripts/bench`.
- Run the upstream unit tests after applying the patch.
- Capture provenance before any cluster is removed.
- Remove billable clusters after a campaign, including failed campaigns unless an explicit keep flag is set.
- Combine upstream result files into one comparison report without changing raw data.

The adapter should not vendor the upstream repository. The pin and patch make upstream changes explicit while keeping the official client and report code intact.

## Environment preflight on 2026-07-27

The current workstation has `kubectl` 1.35, Google Cloud CLI 559, Python 3, Git, `jq`, and `envsubst`.

The active personal Google Cloud configuration uses `adityavkk@gmail.com` and project `adityavkk-prototyping`. The project is active, billing is enabled, and Artifact Registry, Cloud Build, Compute Engine, and GKE APIs are enabled. VPC Service Controls allow the required API calls from the current network.

The project now has a custom `benchmarking` VPC, a `10.80.0.0/20` subnet in `europe-west4`, and a Docker repository named `ds-bench` in `europe-west1`. Build phase preflight passes.

The frozen topology needs 80 VM vCPUs. The server uses 16 C4D vCPUs, and the two client nodes use 64 N2D vCPUs. Google Cloud approved an increase in the project wide CPU quota from 32 to 96. Google Cloud denied the standard `europe-west4` N2D increase, but approved a separate 64 vCPU Spot pool. The clients use that Spot pool. Campaign preflight now passes every topology and quota check.

Spot capacity is not guaranteed. A client node can be unavailable during creation or preempted during a run. The harness treats that event as a failed cell, deletes the exact cluster, and requires a clean rerun.

`kind` is not installed persistently. A local smoke suite passed on July 26, 2026 by starting the existing Colima runtime and supplying `kind` through an ephemeral Nix shell. This affects local reruns, but it does not block the remote image build or campaign.

The adapter must fail its campaign preflight before creating infrastructure when any required quota or access check fails. Build phase preflight omits VM capacity checks so image creation can continue during quota review. The adapter must also support preparing, testing, and reporting without a live cluster so harness development does not depend on cloud access.

## Final campaign status on 2026-07-28 UTC

Campaign `20260727T122510Z-bd85274b` ran all 24 planned suites. Every suite
returned control to the runner, and every teardown proof confirms that its GKE
cluster was absent before the next suite started. No `chdb-*` cluster remains.

The archive contains 2,584 raw files. Its evidence tree SHA-256 is
`ad3104c8a0a10ca8b7e81b88e636517181ad33f82ecf3b56c9bf6977d1a1c917`.
The seal excludes generated reports and validation files, so those files can be
regenerated. Any change to a suite, raw result, histogram, log, sample, image
record, or teardown proof makes seal verification fail.

The validator found 20 invalid catchup rows. Sixteen belong to Chronicle because
the first seed helper did not create exactly 16 MiB on every stream. Four belong
to Ursula disk mode at 100 streams because setup timed out. These rows are not
performance results. The other workload data remains usable. The validator also
labels 64 valid rows as overload because they completed less than 98 percent of
the declared offered load or returned request errors.

Two diagnostic attempts were kept out of the result set. Campaign
`20260728T092002Z-59260e88` stopped before measurement because the campaign CLI
did not forward the workload selection. Campaign
`20260728T093328Z-59260e88` proved that Node adds five stored framing bytes per
append. A fixed 4,096-byte payload therefore overshot the 16 MiB stored target.
The runner stopped before Ursula and Chronicle, deleted Node, and marked the
archive invalid.

The final supplement is `20260728T122629Z-59260e88`. Its client adapter is
`0260541bc07293d6b645b0e122d98125beced1ea3b442eecc0c05aab6ac5918d`.
The client first calibrates stored framing, chooses payload lengths that reach
the target exactly, retries failed appends from a fresh size probe, and verifies
every stream. The supplement reused the exact Chronicle, Rust, Node, Ursula, and
Redis image digests from the baseline. Only the ds-bench client image changed.

All four supplement suites returned zero, none timed out, and every teardown
proof reports the cluster absent. The archive contains 56 catchup cells. All 56
have parseable raw client JSON, the expected stream count, and an exact
16,777,216-byte minimum and maximum. The validator reports zero invalid or
missing rows and nine overload observations.

The supplement seal audit found one post-seal log append. The final watchdog
wrote its normal standing-down line after the campaign created the checksum
inventory. No measurement, aggregate, manifest, image, execution, diagnostic,
teardown, or absence proof changed. `seal-repair.json` records the original and
final file hashes. The final seal covers 481 files with tree SHA-256
`c0860ab25e172de648dde9280ea749d5343a142d1e6d3298fcd8a8cbf4c569e8`.
The harness now waits for a completed watchdog to exit before sealing and seals
only complete campaigns.

The current repository adapter is
`2e178d06b991dfea1184937ce832f5da96dd3bfe2d9eb59fdc3d828d6367739b`.
It also makes aggregate catchup status use the post-write stored-size evidence.
This prevents a framed server's response payload bytes from being mistaken for
its stored history size. The supplement manifest correctly retains the earlier
adapter digest because that is the client image that ran.

The final combined report is in the baseline archive. It contains 217 cells,
73 valid overload observations, and no invalid or missing rows. All 56 catchup
cells come from the supplement. The other 161 cells come from the baseline.
Both evidence seals verify. An independent `gcloud container clusters list`
returned an empty list after the final teardown.

The main limits are visible in the data. Chronicle AOF `always` reached 34,600
appends per second at 100,000 streams. Rust WAL reached at least 64,300 because
its load ladder ended before a plateau, and Ursula disk reached 4,900.
Chronicle delivered about 15 percent of the declared one-stream fanout load at
1,000 subscribers. Chronicle catchup was clean at 8 and 32 readers, then
overloaded at 128 and 512. At 512 readers it replayed 56.5 to 61.9 response MiB
per second, while the faster memory and WAL arms were generally in the 1.6 to
2.8 GiB per second range.

Chronicle recovered 8,141 failed seed appends out of 3,612,621 attempts, or
0.225 percent, before exact verification. Rust, Node, and Ursula recorded no
failed seed appends. These retries happened before measurement, so they do not
change replay throughput. They remain an operational warning about Chronicle's
prefill path.

The downside is that this is a single-node throughput study. It does not measure
replication, failover, data recovery, or managed Redis. It also reports response
payload throughput. Stored histories include server-specific framing which is
checked separately by the exact-size probe.

## Fixed-budget topology results on 2026-07-29 UTC

The concise
[configuration and cross system comparison](configuration-comparison.md)
summarizes the Chronicle topology results alongside Rust WAL, Node memory, and
Ursula disk.

The direct comparison across Chronicle, Rust, Node, and Ursula remains in the
[combined comparison report](results/20260727T122510Z-bd85274b/report.md).
The new [topology report](results/20260728T210619Z-f3226954/scaling-report.md)
compares six Chronicle layouts against the better result from Rust WAL and
Ursula disk in each selected cell. The separate
[persistent SSE report](results/20260728T210646Z-7a6173cb/scaling-report.md)
compares Chronicle's legacy wait loop with a persistent Redis notification
subscription.

Every topology used the same total primary SUT budget: 4 vCPUs, 16 GiB, one
server node, and one local SSD. More Chronicle replicas divided the same 2 vCPU
Chronicle allocation. Three Redis masters divided the same 2 vCPU Redis
allocation and shared the same disk. These are comparable software topology
tests. They are not tests with more total compute, more machines, or more disks.

No configuration met every gate. The evaluator requires at least 80 percent of
the better durable Rust or Ursula result, at least 98 percent completion, zero
request errors, no more than twice the reference latency, and memory within the
declared limit. The closest overall configuration was the original one
Chronicle and one Redis layout, with 10 of 59 checks passing. It still had a
worst throughput ratio of 2.1 percent because catchup remained far behind the
reference. One Chronicle and three Redis masters passed 9 of 59 checks and had
a 3.9 percent worst throughput ratio.

The result is workload specific:

| Workload | Useful configuration | Result | What changed |
| --- | --- | --- | --- |
| Write saturation, 100,000 streams | 1 Chronicle, 3 Redis | 33.8k writes/s, 6.8 ms p50, 12.9 ms p99 | 7 percent more throughput and better latency than 1 Chronicle, 1 Redis |
| One-stream SSE, 1,000 clients | 2 Chronicle, 1 Redis for the topology screen | 10.8k events/s, 138 ms p50, 305 ms p99 | Small latency gain, 3 percent less throughput |
| 100-stream SSE, 2,048 clients | 1 Chronicle, 3 Redis | 16.8k events/s, 238 ms p50, 287 ms p99 | 19 percent more throughput and better latency |
| Catchup, 100 streams and 512 readers | 1 Chronicle, 3 Redis for balance | 106.7 MiB/s, 24.5 s p50, 28.5 s p99 | 82 percent more bandwidth than 1 Chronicle, 1 Redis |
| Mixed writes with 100,000 readers | 4 Chronicle, 3 Redis | 9.31k writes/s, 104.0 read MiB/s, zero errors | 2.8 times baseline writes and 72 percent more read bandwidth |
| Mixed live delivery at high writer rates | 1 Chronicle, 3 Redis | About 9.0k delivered events/s, about 0.52 s p99 | 34 percent more delivery at level 8 and 65 percent more at level 33 |

More Chronicle replicas were not a general fix. They reduced write throughput,
hurt one-stream fanout, and usually hurt many-stream SSE when the same CPU was
split across processes. They helped the mixed request burst, where four
Chronicle replicas plus three Redis masters produced the strongest zero-error
result. Two Chronicle replicas plus three Redis masters also gave the best
low-rate live-delivery completion. Once the delivery path saturated, extra
Chronicle replicas let writes run ahead of delivery and reduced delivered event
throughput.

Redis sharding helped workloads that spread work across many stream keys. It
improved write saturation, many-stream SSE, catchup, and high-rate mixed
delivery. It hurt the one-stream fanout test because that stream stays on one
Redis master while cluster coordination adds work.

The persistent SSE diagnostic was the largest software-only improvement:

| SSE cell | Legacy wait | Persistent wait | Change |
| --- | ---: | ---: | --- |
| 1 stream, 1,000 clients | 11.4k events/s, 138 ms p50, 377 ms p99 | 29.8k events/s, 62 ms p50, 79 ms p99 | 2.6 times throughput and 79 percent lower p99 |
| 100 streams, 2,048 clients | 13.8k events/s, 268 ms p50, 343 ms p99 | 25.4k events/s, 134 ms p50, 165 ms p99 | 1.8 times throughput and about half the latency |

Persistent wait still did not qualify against Rust WAL or Ursula disk. It
reached 59.5 percent of the one-stream throughput target and 24.7 percent of the
100-stream target. Its peak working set also increased from 249 to 376 MiB in
the one-stream cell and from 503 to 644 MiB in the 100-stream cell. It holds one
Redis notification subscription for each SSE connection, so connection count
and Redis Pub/Sub capacity are the main downside.

All eight paid suites returned zero, both evidence seals verify, and every
teardown proof reports its exact cluster absent. The six topology suites used
4.84 cluster-hours. The two SSE diagnostics used 0.49 cluster-hours. The two
earlier setup attempts were stopped before a usable measurement and their exact
clusters were also deleted. The campaign stayed well below the approved
22 cluster-hour ceiling.

The practical conclusion is not to add Chronicle replicas by default. Keep one
Chronicle process unless a request-heavy mixed workload proves that more
processes help. Use Redis sharding for workloads with many independent stream
keys, but not for one hot stream. Persistent SSE waiting is the highest-value
next implementation direction. Catchup remains the largest unresolved
architecture gap: even the best fixed-budget Chronicle result was about
110 MiB/s, while the durable reference target was about 2.8 GiB/s.

## Risks

- **Old headline numbers are not valid baselines.** The current upstream documentation calls the June write ceilings upper bounds. The mitigation is to rerun every compared server with the pinned current harness.
- **Durability labels can mislead.** Redis `everysec`, Node memory, Rust memory, and Ursula memory do not make the same crash guarantee as the durable arms. The report must split results by durability class.
- **Resource accounting can omit Redis.** The current metrics script samples one process for CPU and RSS. The Chronicle adapter must sum Chronicle and Redis, while retaining pod working set as the memory headline.
- **Client capacity can look like server capacity.** The client pod concurrency must be calibrated before the full ladder. Nonzero errors, nonzero lazy creates, or a client throughput plateau invalidate a server ceiling.
- **A fixed pod ladder can end too early.** A `ladder_exhausted` result is a lower bound. The campaign must extend that system's ladder before making a ceiling claim.
- **Node may run out of memory at 100,000 streams.** That result is a gap with an observed failure reason. It must not be converted to zero or omitted.
- **Local kind results are not publication data.** They prove protocol compatibility and harness wiring. GKE results on pinned hardware provide the comparison data.
- **Cloud runs cost money.** Each suite needs a scoped teardown watchdog. The campaign must verify that no owned cluster remains.
- **Network policy can block cleanup as well as creation.** The preflight must prove that the runner can list and delete owned GKE clusters before it creates one.
- **The current Chronicle worktree may be dirty.** A result must include a patch digest or source archive digest, not only the Git commit.
- **Catchup can benchmark unequal histories.** The pinned upstream seed helper counted failed concurrent appends but did not retry them or verify the final stream size. The first campaign exposed short Chronicle histories. The corrected client retries from a fresh size probe, requires the exact target for every server, and records seed diagnostics. Rows from the first run with short histories are invalid rather than low throughput.
- **A transient artifact copy error can hide complete evidence.** Kubernetes can report a reset after transferring a file. The adapter snapshots samples and fleet inputs, compares the remote and local SHA-256 digest, and retries before deciding that evidence is missing.

## Decisions from review

- Use one 4 vCPU, 16 GiB, one SSD SUT budget for the cross-system headline comparison. Chronicle and Redis share that budget.
- Run the workloads required by the current maintained `ds-bench` suite and the blog. Keep the current 8 vCPU Rust canonical campaign separate from the headline comparison.
- Use the active Adityavkk personal GCP account and project `adityavkk-prototyping`.
- Run one measurement cluster at a time. Use scoped deadline watchdogs and record an estimated cost from cluster wall time. Cloud Billing data can lag, so elapsed time and machine inventory are the enforceable safety limits.
