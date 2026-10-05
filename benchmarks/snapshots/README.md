# Projection snapshot benchmarks

`benchmark.mjs` exercises the implemented Chronicle API, not the earlier in-memory folding
experiment. A Node client uses real TCP HTTP to a separate Chronicle binary,
which uses a separate Redis process. The benchmark produces fresh materialized
state on every recovery. It is **not an Electric runtime benchmark**: it does
not use StreamDB, TanStack collections, EntityStreamDB, claims, or handlers.

## Bounded Electric recovery avoids prefix reads

The corrected [Electric/SDK bundle](../../experimental/electric-checkpoints/README.md)
uses real Chronicle signed webhooks, ElectricAgentsServer, Postgres, Electric
shapes and `processWake`. Both compared paths use the same explicit
`bounded-checkpoint-v1` input contract: replay retained history versus restore an
aligned completed-progress/state image and read only its guarded entity-source
suffix. Neither path represents arbitrary legacy handlers that inspect history.
The runtime contract remains opt-in; these patches are not released upstream.

The final-source default and scaled campaigns each have **180 measured
activations plus 12 warmups**, 30 complete pairs per scenario and both orders.
All **360 measured activations** passed, with zero
failures, unpaired successes, fallback or source drift. The independent reporter
verified all **180 checkpoint hits**: the first source GET starts at the image
cut, every admitted GET starts at or beyond it, and every GET carries the matching
incarnation guard. Failed/aborted requests are included in this audit. Exact
handler-observed state, input/reply multiplicity, final ack=head, released Redis
leases and observed intermediate fixture history also pass.

Activation latency is **p50 / p95 milliseconds**, using nearest-rank percentiles
over raw measured samples, not interpolated even-sample medians:

| Campaign / workload | Replay | Checkpoint + suffix |
|---|---:|---:|
| Default: 500 updates / 16 keys | 60.08 / 70.09 | 63.55 / 86.42 |
| Default: 300 append-only rows | 67.70 / 77.53 | 78.97 / 102.60 |
| Default: 100 pending 8 KiB inputs | 75.59 / 99.49 | 91.36 / 106.09 |
| Scaled: 10,000 updates / 16 keys | 107.25 / 136.66 | 60.18 / 72.42 |
| Scaled: 3,000 append-only rows | 155.12 / 179.13 | 184.13 / 236.33 |
| Scaled campaign: repeat pending-input case | 77.08 / 95.50 | 93.21 / 106.17 |

**The 10k-update workload improves; small, append-only and pending-input cases
do not.** At 10k updates, p50 is 44% lower (1.78×), preload p50 falls from
50.16 to 6.35 ms, and total captured request-plus-response bodies fall from
1,610,749 to 18,475 bytes, including publication. The
500-update case reduces preload p50 from 9.45 to 6.88 ms and captured response
bodies from 82,532 to 8,495 bytes, but lookup, completion and publication work
erase that saving in total activation latency. Append-only images retain almost
all rows; a large pending inbox still has to be read and handled. Avoid enabling
this globally without measuring the application's state shape and save cadence.

Resource arrows are **replay → checkpoint**; CPU is median combined Node-process
milliseconds, and body bytes count captured source-gate responses:

| Workload | CPU p50 | Response body bytes p50 | Publish HTTP p50 / p95 ms |
|---|---:|---:|---:|
| 500 updates | 52.23 → 52.95 | 82,532 → 8,495 | 2.30 / 4.64 |
| 300 append-only rows | 58.24 → 70.21 | 40,682 → 76,674 | 2.96 / 3.99 |
| Pending inputs, default campaign | 66.79 → 77.61 | 871,021 → 872,739 | 11.98 / 15.88 |
| 10k updates | 89.06 → 48.39 | 1,609,174 → 8,574 | 2.12 / 3.38 |
| 3k append-only rows | 155.37 → 182.99 | 386,892 → 742,099 | 9.93 / 13.95 |
| Pending inputs, scaled campaign | 70.64 → 79.41 | 871,021 → 872,739 | 12.26 / 16.08 |

Publication request bodies are additional: default medians are 8,229, 76,401 and
884,459 bytes; scaled medians are 8,326, 741,838 and 884,459 bytes. The
pending-input case uploads nearly the entire input
payload in its successor image. Snapshot publication gates are deliberately
zeroed to expose that cost; these are not measurements of the default
100-event / 25 ms / 60-second eligibility policy.

Median sampled Node peak heap/RSS are 169.4/461.0 → 167.1/456.9 MiB for 10k
updates, and 264.5/446.4 → 233.8/486.2 MiB for 3k append-only rows. These are
whole-process peaks influenced by fixture history, caching and GC; lower heap
does not imply lower RSS or isolated per-activation memory use.

Measurement scope:

- A 4-vCPU, approximately 8 GB orb runs Node 26.10.0, Go 1.26.2, Redis 7.4.11,
  Postgres 18.6 and source-built Electric 1.8.1. A fresh Node process runs each
  mode/scenario/order, with one warmup; each activation rebuilds EntityStreamDB.
  This is warm-process application-state recovery, not cold process startup.
  Each path's 30 observations span two processes, not 30 independent startups;
  p95 is descriptive, not a production tail-latency estimate.
- Timing starts at wake-received logging after `claimHeaders` resolution and
  ends after cleanup and a successful real `done:true` callback. It excludes
  trigger submission and semantic validation; claim/preload can overlap.
- Gate capture begins before trigger submission or held-webhook release. It
  counts admitted source-gate request/response bodies, not headers, transport
  framing, callback traffic, Electric/Postgres traffic or validation reads.
  Publication HTTP time excludes preceding export, serialization and hashing;
  those steps can still affect activation time/CPU. Publication is asynchronous.
- CPU/heap/RSS include the runtime handler, embedded agents-server, source proxy
  and test instrumentation in one Node process. They exclude Chronicle, Redis,
  Postgres, Electric and the callback gate. Two-millisecond memory samples plus
  endpoints can miss shorter peaks and do not measure per-activation allocation.
- Seed updates/inserts share one batched append; inbox tests hold notifications
  until 100 inputs accumulate. These are synthetic handlers with no LLM calls,
  not a production arrival trace, HA/failover campaign or capacity study.

Raw [default samples](../../experimental/electric-checkpoints/default-results.json)
have SHA-256 `ae1b0b2f63f0549fbe2e084168e00ba0b01c2130599a831a1debf5264ae2fc81`;
[scaled samples](../../experimental/electric-checkpoints/scaled-results.json)
have SHA-256 `e68855b2b2aa3488b95129113380585c75b5d668ffcee84a21a7537848a0e8b9`.
The bundle retains source archives, cumulative patches, full run logs, commands
and checksums. Campaign fields named `electricPatchSha256` and
`durablePatchSha256` are working-tree fingerprints, not hashes of exported patch
bytes; the file manifest records those separately.

```sh
node --test benchmarks/snapshots/*.test.mjs
node benchmarks/snapshots/process-wake-report.mjs \
  experimental/electric-checkpoints/default-results.json
node benchmarks/snapshots/process-wake-report.mjs \
  experimental/electric-checkpoints/scaled-results.json
```

## Electric runtime measurements use the actual activation path

The [upstream patch bundle](../../experimental/electric-snapshots/README.md#real-processwake-e2e-and-cold-activation-campaign)
contains a separate harness using real Chronicle webhooks, ElectricAgentsServer,
Postgres, Electric shapes, `processWake`, and subscription callbacks. Snapshot
mode restores state but still replays the full raw log; it is not suffix-only
activation. Do not apply the projection-only speedups below to that runtime.

This frozen bundle is historical evidence, **not a deployment recommendation**.
Later review found a legacy partial-row update regression in its SDK patch;
the measured fixtures did not expose it. The corrected
[bounded-input integration](../../docs/adr/0013-bounded-electric-runtime-recovery.md)
and its final-source measurements are described separately above. The following
results apply only to the immutable historical full-raw-replay bundle.

Recorded on 2026-10-03 in a separate 4-vCPU, approximately 8 GB orb, with Node
26.10.0, Go 1.26.2, Redis 7.4.11 (AOF, `appendfsync always`), Postgres 18.6 and
source-built Electric 1.8.1. Two campaigns each contain 180 measured activations
plus 12 warmups: **360 measured activations passed**, with 30 complete pairs per
scenario/campaign, both phase orders, 180/180 measured image hits, and no failed,
unpaired, fallback or source-drifted samples. No source responses, producers,
claims or acknowledgments were mocked.

Activation latency below is **p50 / p95 milliseconds**:

| Campaign / workload | Replay | Snapshot + full raw replay |
|---|---:|---:|
| Default: 500 updates / 16 keys | 38.93 / 57.51 | 49.08 / 57.19 |
| Default: 300 append-only rows | 49.24 / 57.26 | 68.30 / 75.89 |
| Default: 100 pending 8 KiB inputs | 45.19 / 53.68 | 77.43 / 86.43 |
| Scaled: 10,000 updates / 16 keys | 74.26 / 94.18 | 76.08 / 102.71 |
| Scaled: 3,000 append-only rows | 63.56 / 76.51 | 143.90 / 194.62 |
| Scaled campaign: repeat pending-input case | 49.08 / 64.58 | 78.58 / 99.41 |

**There is no measured end-to-end median win for this full-raw-replay policy.**
The 10k-update case is near break-even: preload p50 improves from 43.45 to
36.60 ms, but lookup and other checkpoint work erase that saving in total wake
latency. These runs intentionally force publication on every eligible wake by
setting replay/time/cadence gates to zero; they are not measurements of the
default throttled policy. Do not enable it globally on the strength of the
projection-only benchmark. Bounded raw-input recovery is a separate semantic
contract, not an optimization the frozen patches already make.

The same measured samples retain resource and publication costs. Arrows are
**replay → snapshot**, and CPU is median combined-Node-process milliseconds:

| Workload | CPU p50 | Source-gate response body bytes p50 | Publish HTTP p50 / p95 ms |
|---|---:|---:|---:|
| 500 updates | 31.68 → 44.43 | 81,899 → 89,303 | 2.04 / 2.44 |
| 300 append-only rows | 36.73 → 60.99 | 40,049 → 124,800 | 2.71 / 4.24 |
| Pending inputs, default campaign | 31.10 → 66.55 | 870,600 → 1,741,438 | 10.28 / 13.61 |
| 10k updates | 56.45 → 58.21 | 1,608,537 → 1,615,997 | 2.11 / 3.13 |
| 3k append-only rows | 56.91 → 143.68 | 386,257 → 1,225,515 | 11.42 / 13.54 |
| Pending inputs, scaled campaign | 39.55 → 67.51 | 870,600 → 1,741,438 | 10.54 / 13.67 |

The 3k-row image is 838,854 bytes; its successor publication sends 840,539
bytes. Median sampled Node peak heap is 197.1 → 220.6 MiB and peak RSS is
396.8 → 460.8 MiB for that case. These are whole-process peaks, influenced by
fixture history, caches and GC, not per-activation allocations or runtime-only
memory. All per-sample endpoints and 2 ms sampled peaks are retained; shorter
peaks may be missed.

Method and scope:

- Each mode/scenario/order starts a fresh Node process, but measurements follow
  a warmup and rebuild a fresh EntityStreamDB inside that process. This is cold
  application-state recovery, not cold process startup. There are two process
  runs per mode/scenario, not 30 independent process startups; p95 is descriptive.
- Timing starts at wake-received logging, after initial `claimHeaders`
  resolution, and ends after the successful real `done:true` callback. It includes
  lookup, recovery, handler writes, cleanup and ack; not trigger submission or
  post-run assertions. Claim/preload overlap, so component times do not sum.
- Gate traffic starts just before trigger submission or held-webhook release,
  slightly earlier than that timer. It includes the triggering append for state
  workloads, excludes the separate callback gate and validation reads, and counts
  bodies, not headers or transport framing. `total` is not all system traffic.
- Publication latency measures gate-observed HTTP only; prior export, JSON
  serialization and hashing are not in that number. Their in-activation work
  does contribute to activation CPU/time. Publication is asynchronous.
- CPU/memory include the runtime, embedded agents-server, source proxy and test
  instrumentation in one Node process. They exclude Chronicle, Redis, Postgres,
  Electric and the callback-gate processes. Handler observation copies occur
  inside timing; expected-state comparisons occur afterward. There is no LLM call.
- Update/append histories are each seeded in one batched append; pending-input
  tests hold delivery while 100 inputs accumulate. These are reproducible state
  shapes, not production traces or a capacity/HA/failover study. Real cancellation
  and callback-503 redelivery are separate semantic tests, not benchmark samples.

Raw evidence: [default campaign](../../experimental/electric-snapshots/process-wake-e2e/final-results.json)
(SHA-256 `dfdbcc5a464cf40670472b19a4acf259abcff683dc554889c810020854305679`)
and [scaled campaign](../../experimental/electric-snapshots/process-wake-e2e/scaled-results.json)
(`83a6cf73e15abe866055a77a3a1cacdf215d3f310fd92f61f5fa85d72011d06b`).
The bundle includes pinned sources, source/image/binary fingerprints, tests,
checksums, setup and commands. No upstream changes were pushed or released.

Generate statistics from its retained campaign without rerunning services:

```sh
node --test benchmarks/snapshots/*.test.mjs
node benchmarks/snapshots/process-wake-report.mjs \
  experimental/electric-snapshots/process-wake-e2e/final-results.json
node benchmarks/snapshots/process-wake-report.mjs \
  experimental/electric-snapshots/process-wake-e2e/scaled-results.json
```

The independent reporter checks frozen hashes, fixed semantic source histories,
duplicate sample identities, handler-observed hydrated-state assertions, exact
outputs, quiescent ack=head, released Redis leases, finite measurements, and
traffic accounting. It excludes warmups and uses only complete passing pairs
for nearest-rank percentiles. Failed/incomplete runs, unmatched successes,
missing phase orders/warmups, and sample-target shortfalls remain visible and
make the command fail. Earlier pilots that validated only the source history,
not state read by the handler, do not satisfy this checker.

## Recorded result: small live state wins; retained history may not

Executed on 2026-10-02 in one 8-CPU, 16 GiB linux/amd64 orb (Intel Xeon 2.60 GHz),
with Node 26.10.0, Go 1.26.2, and Redis 7.0.15. The full campaign took 585 seconds.
All **15,663 measured recoveries** passed row/order/sequence/raw-input checks.
There were 30 publication attempts per scenario: 390 accepted replacements and
30 expected oversize rejections. Warmups and fixture setup are not in these counts.

Each serial path has 80 observations, eight-reader paths have 640 each, and the
pressure paths have 320 full / 12,143 snapshot observations because the faster
path completes more recoveries during its minimum-duration window. These are
local closed-loop measurements, not production latency or capacity guarantees.

All latency columns below are **milliseconds, p50 / p95**. Speedup is full p50
divided by snapshot p50; a value below 1 means the snapshot strategy was slower.

| Scenario | Full replay | Snapshot strategy | p50 speedup | Publish p50 / p95 |
|---|---:|---:|---:|---:|
| Short, 100 events | 1.99 / 2.95 | 2.91 / 4.16 | 0.68× | 1.54 / 2.15 |
| Update-heavy, 10k events | 41.71 / 48.36 | 4.17 / 5.78 | 10.00× | 1.99 / 4.24 |
| Update-heavy, 100k events | 448.34 / 538.55 | 4.29 / 5.94 | 104.61× | 2.03 / 3.28 |
| Stale image, 50k events remain | 447.33 / 481.19 | 216.40 / 235.70 | 2.07× | 2.04 / 2.54 |
| Large image, 807.6 KiB | 534.34 / 559.70 | 18.88 / 24.35 | 28.29× | 20.95 / 29.35 |
| Large events, 4 KiB text | 332.11 / 346.50 | 14.92 / 17.53 | 22.26× | 10.65 / 12.55 |
| Append-only, 2.5k rows | 11.56 / 12.95 | 18.08 / 20.93 | 0.64× | 17.16 / 21.81 |
| Oversize image → miss + replay | 25.39 / 28.30 | 27.25 / 31.11 | 0.93× | 11.55 / 15.58 (413) |
| Projection-version miss | 41.51 / 45.73 | 44.02 / 50.43 | 0.94× | 1.92 / 2.66¹ |
| Also recover 50k raw inputs | 454.61 / 490.95 | 211.75 / 227.75 | 2.15× | 1.91 / 3.04 |
| Eight readers, 100k events | 1461.59 / 1536.83 | 6.72 / 10.28 | 217.52× | 2.10 / 2.76 |
| Four readers + append pressure | 870.98 / 939.03 | 3.67 / 5.08 | 237.40× | 2.19 / 3.13 |
| Short + 5 ms/request | 7.66 / 8.59 | 14.10 / 15.92 | 0.54× | 1.16 / 1.42² |
| 100k updates + 5 ms/request | 455.99 / 493.02 | 15.03 / 16.45 | 30.35× | 1.92 / 2.69² |

¹ Publications update `state-v1`; recovery deliberately requests missing `state-v2`.
² Injected delay applies to recovery requests, not publication. Publication is
measured separately without concurrent recovery/append pressure in every case.

The 100k-update case transferred **34,120,781 → 69,736 body bytes per recovery**
(99.8% less), including the image and suffix. The append-only case saved only
**754,171 → 661,915 bytes** (12.2%) and was slower despite the reduction.

Mean CPU milliseconds per recovery, **full → snapshot**:

| Scenario | Node reader thread | Chronicle process | Redis process | Chronicle allocated MiB |
|---|---:|---:|---:|---:|
| 100k updates | 240.40 → 2.76 | 271.50 → 2.25 | 155.48 → 0.60 | 87.66 → 0.28 |
| Large image | 296.91 → 7.96 | 313.50 → 16.38 | 178.02 → 3.12 | 112.80 → 3.10 |
| Append-only | 6.76 → 7.56 | 8.25 → 16.75 | 3.72 → 2.52 | 2.08 → 2.38 |
| 50k raw inputs required | 247.59 → 100.50 | 255.13 → 131.00 | 152.79 → 77.22 | 87.65 → 44.09 |

CPU can overlap across processes/threads, so it must not be summed as wall time.
The CPU attribution limits are described below.

Under append pressure, the writer achieved **1,000.04 / 999.98 events/s** during
full / snapshot phases. Append p50/p95 was **6.05/10.64 ms → 3.59/5.30 ms**.
Worst writer scheduling lag was **3.38 / 2.00 ms**. Thus the faster recoveries
were not obtained by silently starving the offered write load. This does not
measure simultaneous large-image publication contention.

Implications for Electric integration:

- Enable snapshots based on measured replay work versus image/hydration cost,
  not simply because a stream has any history. Small streams can lose.
- A generic exact image of an append-only/token-row collection can retain almost
  all history and become slower or exceed 1 MiB. A compact runtime projection
  needs an explicit semantic contract; do not silently discard rows.
- Pending raw inputs and stale cuts can dominate even with a tiny image. Resolve
  the runtime's input-recovery contract before promising the 100k-update gain.
- Check the advertised size bound before upload; do not repeatedly attempt the
  1.49 MiB image that this benchmark deliberately submits to test fallback.
  Back off repeated misses. Budget the approximately 21 ms large-image save
  separately from recovery and choose cadence from measured workload costs.

Evidence: [complete raw samples](results/local-2026-10-02.json), SHA-256
`4348b0a9896aa16a2ae163d69379efe9a4ec9a5c206bab0b9fbd38d9223b602c`.
The raw file records the base revision and hashes of the uncommitted source used.
The measured binary SHA-256 was
`a418fb2cf252f091bc95051d8216ee36346f942902a7fa0caea43def7e6cfdfc`.

## Reproduce locally

Requirements: Go from `go.mod`, Node 26 (uses `process.threadCpuUsage`), Redis and
`redis-cli`. No npm dependencies. Run from the repository root. Use dedicated
local processes and run no other tests/benchmarks during measurement.

In an Amp orb:

```bash
mkdir -p .tmp/snapshot-bench
go build -o .tmp/snapshot-bench/chronicle ./cmd/chronicle

amp orb service start snapshot-bench-redis --command \
  'redis-server --bind 127.0.0.1 --port 6381 --save "" --appendonly no --maxmemory-policy noeviction'

# This is a public, local-only fixture credential, NOT a deployment secret.
# Bind the service to loopback; never expose this fixture as a portal.
amp orb service start snapshot-bench-server --command \
  "env CHRONICLE_AUTH_MODE=enforce \
  CHRONICLE_SERVICE_BEARER=snapshot-bench:snapshot-bench-local-only \
  CHRONICLE_SERVICE_POLICY_FILE=$PWD/benchmarks/snapshots/service-policy.json \
  $PWD/.tmp/snapshot-bench/chronicle \
  -listen 127.0.0.1:4438 -redis-url redis://127.0.0.1:6381/0 \
  -metrics-listen 127.0.0.1:9098 -subscriptions=false \
  -enable-snapshots -ui=false -log-level error"

node --test benchmarks/snapshots/*.test.mjs
node benchmarks/snapshots/benchmark.mjs \
  --samples 20 --rounds 4 --publish-samples 30 \
  --out benchmarks/snapshots/results/local.json

amp orb service stop snapshot-bench-server
amp orb service stop snapshot-bench-redis
```

Outside an orb, run the same Redis/Chronicle commands in foreground terminals.
Do not run this against a shared Redis or public endpoint. The harness refuses
non-loopback HTTP URLs, uses unique `snapshot-bench/<run>/<case>` paths, deletes
its fixtures in `finally`, and never flushes Redis. On abrupt process termination,
stop the dedicated nonpersistent Redis to discard fixtures. There is no remote
provisioning, deployment, or paid infrastructure step.

`SNAPSHOT_BENCH_URL`, `SNAPSHOT_BENCH_METRICS`, `SNAPSHOT_BENCH_REDIS_PORT`, and
`SNAPSHOT_BENCH_TOKEN` override the local endpoints/credential. Ensure the counters
come from the exact Redis and Chronicle serving the requests. `--cases` selects
comma-separated scenario names; `--samples`, `--rounds`, and `--publish-samples`
control measurement counts. Use an even number of rounds to balance order.

Smoke check (not performance evidence):

```bash
node benchmarks/snapshots/benchmark.mjs --cases short,oversize-fallback \
  --samples 2 --rounds 1 --publish-samples 2 --out /tmp/snapshot-smoke.json
```

## Fixed-arrival contention campaign: large-image publication alongside recovery

`contention.mjs` is a distinct open-loop campaign; it does not alter the raw
closed-loop baseline above. Run it against the same dedicated, loopback-only
Redis and Chronicle processes after building the finalized server:

```bash
node --test benchmarks/snapshots/*.test.mjs
for rate in 1 8; do
  for order in full,snapshot snapshot,full; do
    node benchmarks/snapshots/contention.mjs \
      --duration-ms 30000 --recovery-rate "$rate" --mutation-rate 20 \
      --max-inflight 8 --max-deferred 32 --publication-every 10 \
      --order "$order" \
      --out "benchmarks/snapshots/results/contention-local-${rate}-${order%%,*}.json"
  done
done
```

Recorded on 2026-10-03 with the binary-body/quota implementation, using the same
orb, Node, Go and Redis versions as above. Each run seeds 100k events and an
approximately 808 KiB image; each path receives 30 seconds of scheduled arrivals.
Both orders were run for each load. All **848 completed recoveries** passed
captured-cut equivalence checks, alongside **4,302 completed appends** and **293
advancing snapshot replacements**, with zero operation/validation errors.
Rejected load-generator arrivals are explicitly counted below, not successes.

Pooled p50 / p95 **milliseconds from scheduled arrival through completion**, so
these include queueing and verification. Percentiles are computed over raw
observations, not averaged across runs:

| Offered recoveries/s | Full replay | Snapshot + suffix | Recovery completions / offered, full → snapshot | Append completions / offered, full → snapshot |
|---|---:|---:|---:|---:|
| 1 | 553.50 / 588.29 | 21.16 / 26.56 | 60/60 → 60/60 | 1200/1200 → 1200/1200 |
| 8 | 14033.59 / 14740.71 | 17.38 / 27.33 | 248/480 → 480/480 | 702/1200 → 1200/1200 |

At 1 recovery/s, both paths sustain all offered work. Source append p95 is
8.29 → 4.98 ms; unrelated-stream append p95 is 7.24 → 4.69 ms. Large-image
publication p50/p95 is 12.97/18.06 ms under full replay and 13.13/19.78 ms under
snapshot recovery. These append/publication times begin at dispatch, not at
the scheduled arrival.

At 8 recoveries/s the **full-replay client pipeline overloads**, misses 232
recoveries and 498 appends, and drains after the offered window. Snapshot recovery
completes all offered work; publication p50/p95 is 13.92/18.92 ms. This is **not a
Chronicle capacity limit or an equal-achieved-write-load comparison**: parsing,
folding, writer scheduling and verification share the Node event loop. The raw
queueing and achieved-rate fields expose that bottleneck instead of hiding it.

High-load client sampled peak heap is 103.4–103.7 MiB for full replay versus
52.7–56.7 MiB for snapshots; Chronicle sampled peak RSS is 47.2–49.1 versus
29.7–29.7 MiB. Client RSS is notably order-dependent (full 651.9–668.2 MiB,
snapshot 219.7–502.2 MiB), since allocators retain memory. These are sampled
whole-process peaks, not isolated per-request allocations. Redis `used_memory`
also includes idle conformance fixtures in DB10 from the preceding validation;
it is not snapshot-only storage. No conformance server was running during timing.

Raw evidence, including per-arrival disposition, timings, source/binary hashes,
memory counters, and phase order:

- [1/s, full first](results/contention-2026-10-03-1-full.json)
- [1/s, snapshot first](results/contention-2026-10-03-1-snapshot.json)
- [8/s, full first](results/contention-2026-10-03-8-full.json)
- [8/s, snapshot first](results/contention-2026-10-03-8-snapshot.json)

Full and snapshot paths receive independently generated but identical absolute
recovery and mutation schedules for the same duration, each on a fresh unique
fixture. Recovery arrivals are bounded by `--max-inflight`; one bounded FIFO
holds `--max-deferred`, and overflow is recorded as `missed`. Results retain
every arrival, deferred queue time, error, service latency, end-to-end latency
from its scheduled time, and offered/achieved rates. Errors make `complete`
false and fail the command; they are not retried or discarded.

While recoveries execute, the mutation schedule mixes source appends (including
append/recover races), unrelated-stream appends, and advancing replacements of
the approximately 0.8 MiB snapshot. Append and publication p50/p95 and all raw
samples are retained. Each recovery validates its own terminal opaque offset
against a chronological offset-to-event-count boundary map, then independently
checks sequence, rows, and ordering at that captured cut. It never compares to
a mutable fixture tail or performs arithmetic on offsets.

Memory evidence includes the client process peak RSS and heap sampled every
25 ms, and Chronicle RSS / Redis `used_memory` sampled every 250 ms plus phase
endpoints. Sub-interval peaks can be missed and Redis `INFO` sampling adds slight
load; this campaign uses asynchronous operations in one Node process (no worker
threads). These measurements and achieved rates characterize only this offered
load and must not be presented as maximum capacity. The harness retains the
existing loopback refusal, scoped fixture credential, unique paths, and targeted
DELETE cleanup; it never flushes a shared Redis database.

### Binary Redis layout measurement

`go test -race -count=1 -run TestProjectionSnapshotRedisBinaryLayout -v ./store/redis`
stores the **same 808 KiB arbitrary binary body** in both the previous unpublished
JSON/base64 envelope and the new binary-field layout. On Redis 7.0.15:

```text
body=827392 descriptor=214 legacy-envelope=1103416
Redis MEMORY USAGE: legacy=1310976 binary=918096
```

That is approximately **30.0% less Redis memory for this image**, including the
new descriptor and quota fields. Allocator size classes and Redis versions affect
the percentage; the general encoding saving is the removal of base64's roughly
33% expansion. This is a direct representation comparison, **not a latency A/B**.
The new publication timings above must not be attributed entirely to this layout
change by comparing them with the earlier, differently loaded campaign.

## Workloads expose different tradeoffs

Events use State-shaped `{type, key, value, headers.operation}` JSON plus a
benchmark sequence index. Update-heavy cases replace complete rows in a bounded
key set; every eleventh cycle includes a deletion cycle. The projection preserves
the next sequence number, row sequence numbers, and first surviving insertion
order. Append-only cases keep every row. Payload strings have deterministic
lengths and are repetitive; compression is disabled and not benchmarked.

| Scenario | Events | Events covered by image | Maximum keys | Text bytes/event | Special condition |
|---|---:|---:|---:|---:|---|
| `short` | 100 | 90 | 10 | 128 | Extra-request overhead |
| `updates-10k` | 10,000 | 9,900 | 100 | 256 | Small live state |
| `updates-100k` | 100,000 | 99,900 | 100 | 256 | Long update-heavy history |
| `stale-half-history` | 100,000 | 50,000 | 100 | 256 | Half the log remains |
| `near-limit-image` | 100,000 | 99,900 | 1,800 | 384 | Approximately 0.8 MiB image |
| `large-events` | 10,000 | 9,900 | 100 | 4,096 | Approximately 38 MiB history |
| `append-only` | 2,500 | 2,400 | 2,500 | 192 | Little state reduction |
| `oversize-fallback` | 6,000 | 5,900 | 6,000 | 192 | Image rejected with 413; miss + replay |
| `version-miss` | 10,000 | 9,900 | 100 | 256 | Request an unpublished projection version |
| `pending-raw-half` | 100,000 | 99,900 | 100 | 256 | Also recover the last 50,000 raw events |
| `eight-readers` | 100,000 | 99,900 | 100 | 256 | Eight independent V8 isolates recover one source |
| `append-pressure` | 100,000 | 99,900 | 100 | 256 | Four readers plus separate-source append traffic |
| `short-plus-5ms` | 100 | 90 | 10 | 128 | Add 5 ms per HTTP request |
| `updates-plus-5ms` | 100,000 | 99,900 | 100 | 256 | Add 5 ms per HTTP request |

The pending-raw case models a runtime consumer that still needs historical
inputs even after materialized rows are restored. Full replay collects those
events in the same pass. Snapshot recovery performs a separate guarded raw read
without applying that window twice. This is a cost model for the documented
Electric integration constraint, not proof that Electric's input recovery is
implemented or equivalent.

## What the timing includes

- **Full:** HTTP catch-up from `-1`, all body transfer, JSON parsing, and folding.
- **Snapshot:** HTTP image lookup, SHA-256 verification, JSON decode/hydration,
  incarnation-guarded suffix read, transfer, parsing, and folding. Also the raw
  read when required. A miss costs a lookup plus full replay.
- **Publication:** synchronous image export/serialization, hashing, HTTP upload,
  server validation, Redis Lua CAS, and response. Every replacement advances the
  source cut; these are not cheap equal-image no-ops. The preceding event append
  is outside publication latency. Images too large for the API remain rejected.

Both recovery paths use Chronicle's default unbounded HTTP catch-up response
with bounded storage reads internally. The client buffers each JSON response
before parsing. No artificial `?limit` amplifies the full-replay HTTP request
count. Full replay generally needs one HTTP request; image + suffix needs two.
The 5 ms cases add a timer delay before each request, not bandwidth limits,
packet loss, TCP/TLS handshakes, or a realistic network emulator.

Fixtures and initial images are created through HTTP. Snapshot offsets come
from actual server replies, not arithmetic. The initial image is built by
replaying its prefix. Every measured recovery checks every final row and its
ordering against an independent last-event-per-key oracle. Input sequence checks
reject skips, duplicates, and reordering. Raw input windows are verified too.
Oracle assertions are excluded from recovery latency; semantic work such as
digest validation and sequence checks is included. Failures stop the campaign;
they are never retried into a successful sample. An incomplete output has
`complete: false` and must not be used as a completed campaign.

## Measurement boundaries and evidence

Four rounds alternate full/snapshot and snapshot/full. Each phase starts fresh
workers, warms each twice, then measures 20 recoveries per worker by default:
80 observations per path for serial cases; 640 for eight readers. Workers use
separate V8 isolates but share one OS process. Processes, connections, Redis
data, and server caches are warm; application state is rebuilt each time. This
does **not** measure cold process startup, TLS setup, or disk-cold recovery.

The append-pressure phase runs for at least three seconds and at least 20
recoveries per reader. A separate worker offers 50 events every 50 ms to another
source, targeting 1,000 events/s. The raw result retains append latencies,
achieved counts, elapsed time, and scheduling lag so overload is visible. The
recovery source remains fixed for a fair state-equivalence comparison. This is
closed-loop recovery load, not a saturation/capacity claim or a same-source
append-race correctness test.

Raw JSON retains every timing sample, phase ordering, publication sample, CPU
counter boundary, body byte count, environment, and hashes of local changed
source files. Percentiles use nearest rank across observations, not averaged
round percentiles. With 80 serial observations, p95 is descriptive; do not infer
production p99/SLOs or statistically independent production samples.

- `bytes` counts response bodies, including image and required raw history, not
  headers, TCP framing, or Redis wire traffic.
- `clientThreadCPUms` measures the reader thread through recovery, excluding
  the independent oracle. Native/other V8 thread CPU is not attributed there.
- `clientCPUmsIncludingVerification` uses whole-process CPU and includes all
  workers, verification and the pressure writer when active.
- Chronicle CPU/allocation and Redis CPU/command deltas come from `/metrics`
  and `INFO`. The pressure case includes writes in those process-wide counters.
  Redis counts include Lua subcommands and the small INFO measurement overhead.
  Very short phases can fall below process CPU counter resolution.
- Server RSS before/after is retained but is **not a peak-memory measurement**.
  There is no client peak-heap claim.

The isolated Redis disables AOF/RDB persistence and uses `noeviction`.
Subscriptions are disabled to isolate the data path. Authentication and scoped
publication authorization are enabled. The client, Chronicle, and Redis share
the orb's CPUs and loopback network. Results do not establish managed Redis 8,
multi-replica, WAN, persistence/failover, or Electric activation performance.

See [the recorded local campaign](results/local-2026-10-02.json) and the
[research design](../../docs/research/13-checkpointing-and-agent-recovery.md).
