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
