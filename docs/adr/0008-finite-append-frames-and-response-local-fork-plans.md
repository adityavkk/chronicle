# ADR-0008: Bound oversized frames and reuse fork plans within one response

- **Status:** Accepted
- **Date:** 2026-07-31
- **Deciders:** @adityavkk
- **Tracking issue:** GPA/chronicle#15

## Context

ADR-0004 bounds every storage page by returned payload bytes and 1,024 frames.
It deliberately returns one oversized first frame so a reader can always make
progress. Without an append policy, that exception is finite only because of
Redis's bulk-argument and sorted-set member limits.

A fork response also rebuilds the same ordered lineage plan on every page. The
page scripts must continue to validate the root and each source incarnation,
but the response does not need to rediscover unchanged ancestry between serial
pages.

Both optimizations are optional. A default request limit would change protocol
compatibility, while retained fork state would be unjustified without a
measured reduction in metadata work or latency.

## Decision

### Operator-controlled append ceiling

Chronicle adds `CHRONICLE_MAX_APPEND_BYTES` and `--max-append-bytes`. Zero is
the default and keeps the existing unlimited HTTP policy. A positive value
applies equally to create-with-initial-data and append requests.

A declared body larger than the ceiling is rejected with HTTP 413 before body
allocation or store access. A body without a declared length is wrapped by
`http.MaxBytesReader` before `io.ReadAll`, so at most the configured limit plus
the one detection byte is consumed. The existing authorization and producer
fencing order is unchanged.

At startup, the Redis backend probes `proto-max-bulk-len` when `CONFIG GET` is
permitted. The configured body ceiling plus Chronicle's 34-byte ZSET frame
prefix must fit one Redis bulk argument. An observed mismatch refuses startup.
If the managed service denies the probe, Chronicle logs that the deployment
must enforce the relationship and continues.

The managed-Valkey acceptance deployment uses a finite 16 MiB ceiling. This
makes the oversized-first-frame exception finite for the measured production
shape without changing the default for other operators.

### Response-local fork-plan reuse

`PageReaderSessionFactory` is an optional capability next to `PageReader`.
Chronicle's Redis implementation retains one immutable ordered segment plan for
one serial HTTP response. The plan contains segment bounds and expected source
incarnations only.

Every page still invokes the root script. Every selected source still invokes
the source script and validates the expected incarnation. A missing planned
source is a loud `ErrReadDataMissing`; a changed incarnation is
`ErrReadSnapshotChanged`. Closing the session drops the plan. There is no
process-wide cache, eviction policy, background work, connection, or goroutine.

SSE does not create a response-local session. Its live delivery remains owned
by the existing shared hub and notification multiplexer.

The measured gate passed against local Valkey 8 using 64 one-frame pages per
response. At fork depth one, response-local reuse lowered median latency from
65.214 ms to 43.902 ms (32.7 percent) and metadata commands from 192 to 129
(32.8 percent). At depth four, it lowered median latency from 123.841 ms to
48.942 ms (60.5 percent) and metadata commands from 384 to 132 (65.6 percent).
Root-plus-source script invocations remained 128 per response in every cell.

## Consequences

The append ceiling gives operators a direct memory and Redis-member bound, but
adds a client-visible 413 response when enabled. With the compatible zero
default, deployments that do not configure it still rely on Redis and proxy
limits for the oversized-frame exception.

Fork responses avoid repeated ancestry discovery while preserving page-level
root validation and source fencing. The cost is a small optional interface and
response-scoped state that must be closed on every HTTP exit.

The page target and append ceiling remain separate controls. The page target
governs ordinary returned work; the append ceiling governs the exceptional
indivisible first frame.

## Rejected alternatives

- A nonzero default ceiling was rejected because it would reject requests that
  the current protocol-compatible server accepts.
- Checking `Content-Length` alone was rejected because chunked and HTTP/2
  request bodies can omit it.
- Comparing the body ceiling directly to `proto-max-bulk-len` was rejected
  because the stored Redis argument also includes Chronicle's frame prefix.
- A global fork-plan cache was rejected because it adds staleness, eviction,
  and memory-lifetime problems without improving one response's correctness.
- Trusting cached ancestor metadata was rejected. Every source access remains
  fenced by its expected incarnation in the atomic read script.
