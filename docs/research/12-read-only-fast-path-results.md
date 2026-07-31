# Read-only fast path results

Issue 7 removes write commands and duplicate metadata loads from ordinary read
work. These measurements compare commit
`4c78ea9daef78278a73f1637dd41e3fbfca0d2cf` with the implementation on
`perf/read-only-fast-path`.

## Method

Both versions ran against the same local Redis 8.10.0 server with AOF enabled.
Subscriptions were disabled for the plain GET measurement. Lua scripts were
warmed before each sample. Redis `MONITOR` output was bracketed with unique
`ECHO` markers, so each count contains commands from one request only.

The GET case read one five-byte `text/plain` frame from offset `-1`. The SSE
case attached to an empty non-expiring stream. AOF sizes and the primary
replication offset were sampled immediately before and after each request.

## Redis command deltas

| Case | Version | HGETALL | HSET | HSETNX | PEXPIRE | PERSIST | EVALSHA | Other |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Five-byte GET | Base | 6 | 1 | 0 | 0 | 4 | 2 | 1 ZRANGEBYLEX |
| Five-byte GET | Fast path | 2 | 0 | 0 | 0 | 0 | 2 | 1 ZRANGEBYLEX |
| Empty SSE attach | Base | 8 | 2 | 0 | 0 | 8 | 3 | 1 SUBSCRIBE |
| Empty SSE attach | Fast path | 2 | 0 | 0 | 0 | 0 | 2 | 1 SUBSCRIBE |

The two remaining GET metadata loads come from its two bounded read pages. Each
Lua invocation now returns the metadata map that it already loaded. The script
does not run a second `HGETALL` to build its reply. The HTTP handler no longer
loads the producer hash before the first page, and neither long-poll nor SSE
loads it during wait or refresh work.

Sixteen concurrent first reads of a newly created non-expiring stream produced
one metadata `HGETALL` per read and zero `HSET`, `HSETNX`, `PEXPIRE`, or
`PERSIST` commands. The PTTL values for the metadata, message, producer, and
fork keys were unchanged. A legacy stream without an incarnation produced one
`HSETNX` on its first read and none on its second read.

## Persistence and replication deltas

The base five-byte GET grew the AOF from 2,801,697 to 2,801,783 bytes, a delta
of 86 bytes. Its primary replication offset advanced from 17,699 to 17,700.

The fast-path five-byte GET left the AOF at 32,707,105 bytes and the primary
replication offset at 149,959. Both deltas were zero. The fast-path empty SSE
attach also left the AOF unchanged at 32,707,373 bytes.

No replica was connected during these local samples. Redis therefore reported
zero replication network bytes before and after both versions. The primary
offset is the useful local signal for whether Redis generated replication work.

## Focused timings

The timing loops included one `curl` process start per request. They measure the
whole local shell loop, not server throughput.

| Workload | Base | Fast path |
|---|---:|---:|
| 200 serial five-byte GETs | 2.23 s | 2.20 s |
| 50 serial SSE attaches with a 100 ms client deadline | 6.06 s | 5.78 s |

These runs are too short and too dependent on process startup to support a
speedup multiplier. The command and persistence deltas are deterministic. The
wall-clock results only confirm that the new path did not add an obvious local
regression.

## Issue 172 follow-up: fused root pages and register-first live reads

Issue 172 removes the remaining duplicate script on a root-owned bounded page
and the duplicate attach page for new SSE hubs and `offset=now` long-polls. The
comparison baseline is refreshed `origin/main` at
`b2a5d04bfcc0ce5274915b2a147c2fee8a6c6035`.

Redis `MONITOR` tests bracket each action with unique `ECHO` markers and warm
`read.lua` first. The final deterministic command shapes are:

| Case | Baseline EVALSHA / metadata HGETALL | Final | ZRANGEBYLEX | Producer HGETALL | Stream writes |
|---|---:|---:|---:|---:|---:|
| One-frame root-owned first page | 2 / 2 | **1 / 1** | 1 | 0 | 0 |
| Root-owned continuation at a fixed tail | 2 / 2 | **1 / 1** | 1 | 0 | 0 |
| Root-owned fork suffix | 2 / 2 | **1 / 1** | 1 | 0 | 0 |
| Empty or `offset=now` root page | 1 / 1 | **1 / 1** | 0 | 0 | 0 |
| New empty SSE hub | 2 / 2 | **1 / 1** | 0 | 0 | 0 |
| `offset=now` long-poll timeout | 3 / 3 | **2 / 2** | 0 | 0 | 0 |

An inherited fork prefix deliberately remains a cross-stream operation. Its
trace contains one root script, the Go traversal metadata read for the source,
and one source script with the bounded range. The root message ZSET is not read
before the inherited prefix. This unchanged cost preserves chronological fork
traversal and Redis Cluster slot boundaries.

For persistent and absolute-expiry-only one-frame root pages, `WAITAOF` was
used around the read. `aof_current_size` and `master_repl_offset` were unchanged
in both cases: persistent `28,762,994 → 28,762,994` bytes and
`24,081 → 24,081`; absolute expiry `28,763,747 → 28,763,747` bytes and
`24,088 → 24,088`. The sliding-TTL command test still records exactly one
access `HSET` and one metadata `PEXPIRE` on the first snapshot and none on its
continuation. Legacy incarnation migration still records exactly one `HSETNX`.
The isolated Redis had no attached replica, so a separate replication-network
byte delta was not locally observable; the primary replication offset is the
available server-side write-work counter.

The focused benchmarks ran the untouched baseline and final code sequentially
against the same isolated Redis 8 container. The one-frame comparison uses six
samples of 100 operations and the official `benchstat` tool:

| Signal | Baseline | Final | Change |
|---|---:|---:|---:|
| `redis_scripts/op` | 2.000 | **1.000** | -50.0% |
| time/op | 1.603 ms | **0.660 ms** | -58.85%, `p=0.002` |
| allocations/op | 220 | **119** | -45.91%, `p=0.002` |
| allocated bytes/op | 59.58 KiB | **30.08 KiB** | -49.52%, `p=0.002` |

The broader storage matrix uses five samples of 100 operations. It reports the
median client time and Redis `cmdstat_evalsha` microseconds. This makes the cost
of the longer fused script visible instead of counting only the removed round
trip.

| Page shape | Scripts/op | Median time, baseline → final | Median Redis EVALSHA µs/op, baseline → final |
|---|---:|---:|---:|
| Near 1 MiB byte target | 2 → **1** | 8.090 → **7.466 ms** | 1877 → **1674** |
| 128 small frames at the cap | 2 → **1** | 1.610 → **1.080 ms** | 148.6 → **117.3** |
| Empty tail | 1 → 1 | 0.799 → 0.918 ms | 41.2 → 55.0 |
| Root-owned fork suffix | 2 → **1** | 1.533 → **1.232 ms** | 93.2 → **74.8** |
| Inherited fork prefix | 2 → 2 | 2.253 → 2.952 ms | 89.2 → 130.2 |
| Sliding-TTL first page | 2 → **1** | 1.437 → **0.905 ms** | 90.9 → **69.1** |
| Sliding-TTL continuation | 2 → **1** | 1.318 → **0.824 ms** | 73.1 → **55.7** |

The empty and inherited shapes do not lose a round trip. Their samples expose
the downside of exact typed classification: roughly 14 µs more Redis script
time on the empty-tail median and 41 µs across the inherited two-script
operation. The shared host produced large wall-time outliers, so those two rows
are not treated as throughput regressions or improvements. Root-owned command
reductions, allocation reductions, and server-time changes are the stronger
signals.

The live-mode matrix uses six samples of 50 operations. The append in the wake
case is released only after the old attach recheck, or the new authoritative
registered page, has completed. Append scripts and setup creates are excluded
from the reported read-script count.

| Live operation | Baseline read work | Final read work | Median time, baseline → final |
|---|---:|---:|---:|
| `offset=now` long-poll timeout | 3 scripts | **2 pages / 2 scripts** | 7.224 → **6.214 ms** |
| `offset=now` long-poll wake | 4 scripts | **2 pages / 2 scripts** | 7.431 → **5.995 ms** |
| SSE initial catch-up, new hub | 2 pages / 3 scripts | **1 page / 1 script** | 4.883 → **3.241 ms** |
| Empty SSE attach, new hub | 2 pages / 2 scripts | **1 page / 1 script** | 4.262 → **3.545 ms** |

These are local mechanism measurements, not production service SLOs. The exact
page and script counts are deterministic; the timing medians merely show that
the accepted register-first ordering did not erase its saved work locally.

The 1,000-client shared-SSE guard was also rerun with its counter advanced only
after a durable page completed. Across ten measured appends it observed ten
Redis `PUBLISH` calls and ten `ZRANGEBYLEX` calls, or exactly 1.000 shared live
reads per publish. Registration-first removes setup work; it does not change
the established one-live-read-per-hub fanout bound.

### Final correctness gates

After the recorded baseline, `origin/main` advanced to `49e5866` with the
independent async subscription-fanout change and its integration-test fixes.
The branch was fast-forwarded to that commit, the issue 172 changes were
reapplied, and every final gate below ran on the integrated tree:

- backend Go/Lua differential parity, command traces, AOF and replication
  checks, and the expanded storage/live benchmarks;
- `make test-unit`, `go test ./...` with `REDIS_URL` unset, repository-Redis
  `make test` with race detection, `make lint`, and the online upstream form of
  `make spec-check`;
- `make conformance`: **332 passed of 332**;
- documentation check and production build;
- Jepsen default scenarios `baseline`, `origin-restart`, `redis-restart`,
  `paged-catchup`, `read-expiry`, and `sse-resume`;
- hardening scenarios `pull-wake-arm-crash`, `expired-lease-takeover`,
  `glob-create-crash`, and `index-repair`;
- safety/liveness scenarios `single-holder-linz`, `cursor-monotonic`,
  `stale-gen-noop`, `lease-tail-drop`, and `at-least-once`;
- Redis-direct `ownership-exclusivity`, `slot-isolation`, and `shard-linz`.

The Jepsen cluster was namespaced as `chronicle-i172-fastpath` and deleted with
its network and volume after the run. `paged-catchup` survived Chronicle and
Redis restarts while every captured snapshot remained a contiguous Redis-oracle
prefix. `sse-resume` observed the forced Pub/Sub reconnect, lag disconnect,
write timeout, exact-once replay, and final closed tail.
