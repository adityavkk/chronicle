# Issue 6 immutable segment evidence

## Result

Keep the current Redis frame ZSET as the default.

Local files were the fastest sealed path. They reached 764 to 1,242 MiB/s, which was 2.67 to 4.26 times the same-host Redis baseline. The bounded object cache reached 449 to 788 MiB/s at 32 to 512 readers. Redis strings reached 347 to 379 MiB/s.

The result is not ready for production. No candidate reached the directional 5 times target. More importantly, synchronous read-time sealing created many small segments during the mixed workload. Local files and object cache then fell to 55 to 139 MiB/s. The accepted matrix also predates the later incarnation, TTL-neutral snapshot, generation-qualified repair, and fail-closed GC fixes. Treat it as design evidence only and rerun it before making any claim about the current code.

## Method

The accepted local matrix used:

- One Apple arm64 host with 14 logical CPUs and Go 1.26.4.
- Redis 8.8 with AOF enabled and `appendfsync everysec`.
- One Chronicle process and one load generator on the same host.
- Sixteen streams, each prefilled with 4,096 messages of 1,024 bytes.
- A 3 second warmup and a 10 second requested measurement window.
- Fixed closed-loop reader counts of 8, 32, 128, and 512.
- A second matrix with the same readers plus 16 writers at 5 appends per second each.
- A 256 MiB object cache. The object origin was a separate filesystem tree, not a cloud service.
- A fresh Chronicle process and Redis database for each cell.

High-concurrency requests can finish after the requested 10 second window. The report divides completed work by the recorded measurement interval, including that bounded drain. All accepted catch-up reads completed without an HTTP error. The machine artifact includes p50, p95, p99, completion, errors, drops, resources, Redis counters, and segment counters for all 32 cells.

The accepted closed-loop harness assigned each reader to one stream for the full run. The 8-reader cells therefore used only 8 of 16 configured streams. Cells with 32 or more readers covered every stream. The harness now rotates each reader through the full set, but these results were not rerun. Do not use the 8-reader cells as full-working-set evidence.

The accepted resource samples were not phase tagged, so CPU and RSS summaries include warmup and drain samples. The harness now labels warmup, measurement, and drain and summarizes the measurement phase when available. That fix also needs a rerun.

Exact command:

```bash
cd loadgen
OUT=results/issue-6-local \
REDIS_CONTAINER=chronicle-p0-catchup-redis-1 \
REDIS_DB=13 \
./scripts/bench-segments-local.sh
```

The filesystem cache implementation was improved after its first diagnostic cell. Cache hits now update an in-memory LRU clock instead of writing two filesystem timestamps. All object-cache cells in the accepted summary use the improved implementation.

## Sealed replay

Every accepted cell had 100 percent catch-up completion, zero errors, and zero drops.

| Readers | Mode | MiB/s | Gain over `off` | Full-read p50 | Full-read p99 |
|---:|---|---:|---:|---:|---:|
| 8 | `off` | 291.3 | 1.00 times | 107.7 ms | 166.5 ms |
| 8 | `redis-chunks` | 361.7 | 1.24 times | 88.6 ms | 101.2 ms |
| 8 | `local-files` | 1,241.6 | 4.26 times | 25.7 ms | 33.2 ms |
| 8 | `object-cache` | 739.4 | 2.54 times | 41.4 ms | 81.1 ms |
| 32 | `off` | 274.3 | 1.00 times | 498.2 ms | 660.5 ms |
| 32 | `redis-chunks` | 347.0 | 1.27 times | 417.0 ms | 468.5 ms |
| 32 | `local-files` | 845.4 | 3.08 times | 149.4 ms | 214.3 ms |
| 32 | `object-cache` | 448.9 | 1.64 times | 223.4 ms | 3,188.7 ms |
| 128 | `off` | 261.6 | 1.00 times | 2,324.5 ms | 3,233.8 ms |
| 128 | `redis-chunks` | 378.9 | 1.45 times | 1,550.3 ms | 3,354.6 ms |
| 128 | `local-files` | 820.8 | 3.14 times | 618.5 ms | 743.4 ms |
| 128 | `object-cache` | 599.2 | 2.29 times | 864.3 ms | 1,371.1 ms |
| 512 | `off` | 286.5 | 1.00 times | 7,163.9 ms | 12,705.8 ms |
| 512 | `redis-chunks` | 357.9 | 1.25 times | 5,898.2 ms | 8,237.1 ms |
| 512 | `local-files` | 763.6 | 2.67 times | 2,846.7 ms | 3,631.1 ms |
| 512 | `object-cache` | 788.4 | 2.75 times | 2,582.5 ms | 4,853.8 ms |

The 32-reader object p99 came from a small cold-cache tail. Its p50 and higher-concurrency results were stable enough to keep the cell, but a real object test must report cold and warm results separately.

## Mixed replay and retained writes

The append target was 80 requests per second. “Drops” are append schedules rejected by the client-side in-flight cap. There were no HTTP errors or catch-up drops in the accepted matrix.

| Readers | Mode | Replay MiB/s | Replay p99 | Appends/s | Append p99 | Append drops |
|---:|---|---:|---:|---:|---:|---:|
| 8 | `off` | 279.1 | 165.4 ms | 81.1 | 79.5 ms | 0 |
| 8 | `redis-chunks` | 248.7 | 174.6 ms | 81.0 | 28.9 ms | 0 |
| 8 | `local-files` | 61.6 | 697.3 ms | 78.1 | 23.1 ms | 0 |
| 8 | `object-cache` | 55.2 | 700.4 ms | 78.5 | 13.8 ms | 0 |
| 32 | `off` | 290.3 | 663.0 ms | 79.8 | 275.5 ms | 0 |
| 32 | `redis-chunks` | 339.0 | 476.9 ms | 79.9 | 45.2 ms | 0 |
| 32 | `local-files` | 62.4 | 3,115.0 ms | 74.2 | 133.8 ms | 0 |
| 32 | `object-cache` | 59.6 | 3,082.2 ms | 72.1 | 14.6 ms | 0 |
| 128 | `off` | 308.1 | 3,237.9 ms | 72.3 | 10,141.7 ms | 140 |
| 128 | `redis-chunks` | 374.9 | 3,026.9 ms | 76.0 | 214.3 ms | 0 |
| 128 | `local-files` | 67.6 | 13,639.7 ms | 61.1 | 54.8 ms | 0 |
| 128 | `object-cache` | 71.5 | 11,935.7 ms | 58.9 | 17.8 ms | 0 |
| 512 | `off` | 240.1 | 15,769.6 ms | 17.3 | 19,087.4 ms | 767 |
| 512 | `redis-chunks` | 374.5 | 12,296.2 ms | 65.7 | 5,079.0 ms | 2 |
| 512 | `local-files` | 135.0 | 23,183.4 ms | 38.5 | 24.1 ms | 0 |
| 512 | `object-cache` | 138.9 | 20,512.8 ms | 44.6 | 14.3 ms | 0 |

The file candidates protected append latency by serializing and slowing replay work. That is not a win. Their per-read manifest reconciliation and growing list of small segment files must be removed before another production comparison.

## Resource evidence at 512 sealed readers

“Allocated per byte” is Go heap allocation divided by returned payload bytes. It is a useful copy-pressure proxy, not an exact byte-copy counter. Exact copy instrumentation remains a release gap.

| Mode | Chronicle RSS max | Chronicle CPU | Go allocated | Allocated per byte | Redis CPU | Redis memory max | Redis ops | Redis out |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `off` | 2,873 MiB | 66.9% | 14.1 GiB | 3.36 | 47.1% | 652 MiB | 12,012 | 3,967 MiB |
| `redis-chunks` | 3,104 MiB | 84.1% | 27.9 GiB | 5.44 | 6.4% | 272 MiB | 17,465 | 4,780 MiB |
| `local-files` | 1,136 MiB | 221.5% | 34.9 GiB | 3.85 | 5.6% | 99 MiB | 36,291 | 8.9 MiB |
| `object-cache` | 504 MiB | 231.6% | 35.3 GiB | 3.63 | 5.7% | 99 MiB | 37,918 | 9.3 MiB |

The object cell had a 99.37 percent cache hit rate, 65.3 MiB of cache occupancy, 17 origin data-and-index pairs, 34 estimated object GETs, and 65.3 MiB read from the origin. It had no checksum failure or primary fallback.

Redis command time was collected separately because requesting command statistics during a 512-reader cell changed the workload. The original sampler returned zero because it did not assign parsed `cmdstat_eval*` values. That bug is fixed for future runs. The low-concurrency evidence run sampled command statistics only before setup and after teardown:

| Mode | Completed reads/s | Redis operations | Redis Lua time, run-wide |
|---|---:|---:|---:|
| `off` | 41.4 | 6,477 | 6.583 s |
| `redis-chunks` | 50.6 | 10,716 | 0.463 s |
| `local-files` | 91.4 | 19,000 | 0.527 s |
| `object-cache` | 96.2 | 16,302 | 0.815 s |

The scope includes setup, warmup, measurement, and teardown. Those throughput numbers were collected after the host had entered memory compression, so they are not part of the accepted throughput comparison. The Lua counters remain exact Redis deltas for each isolated run.

## Object cost estimate

The emulator created no cloud bill. For an illustrative regional Standard Cloud Storage deployment, Google’s current pricing example uses $0.020 per GiB-month, $0.005 per 1,000 Class A operations, $0.0004 per 1,000 Class B operations, and $0.12 per GiB for the first internet transfer tier. Same-location transfer to another Google Cloud service is free. See the [official pricing example](https://cloud.google.com/storage/pricing-examples) and [location transfer rules](https://cloud.google.com/storage/pricing).

At an 8 MiB segment target, 1 TiB contains about 131,072 segments:

- Stored data costs about $20.48 per month before index and manifest overhead.
- Two object writes per segment cost about $1.31 once, before manifest and pointer writes.
- One fully cold replay needs about 262,144 Class B GETs and costs about $0.10 in operations.
- Same-location transfer costs $0. Internet transfer of the same 1 TiB starts near $122.88 before tiering.

Small mixed-workload segments increase operation cost sharply. Compaction is therefore a cost gate as well as a latency gate.

## Cross-system context

The prior sealed study reported 57.6 MiB/s for one Chronicle and one Redis at 512 readers and 109.9 MiB/s for its strongest sharded Chronicle topology. Rust WAL, Node memory, and Ursula disk were about 2.6 to 2.8 GiB/s.

Those numbers used different hardware, process topology, durability, and storage. They are not directly comparable with this local matrix. The same-host gains over `off` are the only comparative claim in this report. The new local-file result closes part of the gap, but it does not reach the other systems and does not satisfy the mixed workload.

## Decision

Keep `object-cache` as the production design direction because it gives all replicas a shared sealed origin and removes sealed payload capacity from Redis. Keep `local-files` as the page-cache performance reference. Keep `redis-chunks` as the reversible migration reference.

Do not enable any candidate by default. The blocking downsides are:

- Less than 5 times sealed replay gain.
- Severe mixed-workload fragmentation for file and object modes.
- No streaming or `sendfile` path.
- One bounded primary page read is still required to capture each response
  snapshot before immutable serving begins.
- Process-local snapshot pins.
- Reclamation disabled because no publication staging barrier exists.
- No real object service latency, quota, credential, or regional-failure evidence.
- No exact byte-copy counter.
- No accepted benchmark run after the range-authenticated `PageReader`
  integration.
- Accepted raw inputs are ignored working-session artifacts rather than checked-in immutable inputs.

## Artifacts

- [Working-session 32-cell machine summary](results/issue-6-local-summary.json)
- [Run-wide Redis Lua-time summary](results/issue-6-script-time-summary.json)
- `loadgen/results/issue-6-local/` contains ignored raw local artifacts from the working session. Later rejected diagnostics overwrote some accepted cells, including the off-mode 512-reader cell. The historical tables above were recorded before those overwrites, but the exact accepted machine inputs were not retained. The regenerated machine summary is therefore diagnostic only and does not support a cross-cell release claim. Another checkout cannot independently reproduce it. This is a release evidence gap.
