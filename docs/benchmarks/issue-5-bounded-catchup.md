# Issue 5 bounded catch-up evidence

## Result

Each returned catch-up page targets 1 MiB and contains no more than 1,024
frames. Redis fetches the first candidate alone, bulk-fetches only end offsets
that can fit the remaining byte budget, and does not issue a lookahead query.
For a normal aligned range, fetched and returned candidate bytes are equal.
Candidate bytes can exceed the target through one indivisible oversized first
frame. A fork-segment transition can inspect one non-fitting first candidate
before it stops. The 1,024-frame cap separately bounds small-frame work. HTTP
and SSE write each returned page directly to the socket. One response uses one
captured tail and does not include later appends.

The final-script 512-reader local cell completed 336 measured reads of 16 MiB
streams with no server errors. Every counted body had the exact expected
16,781,313-byte JSON response length. Chronicle RSS peaked at 1,305.3 MiB. The
returned raw page target for 512 active readers is 512 MiB, instead of the old
8 GiB shape. The difference between this target and RSS is decoded frames, Go
objects, socket buffers, and runtime memory. Sampled final-path counters
recorded 12,201 MiB fetched and 12,201 MiB returned, with zero discarded bytes.

Independent review rejected that candidate-fetch shape because it allowed up
to 1,024 large frames to be materialized before applying the 1 MiB returned
target. The final Lua path fetches one candidate, then optionally makes one
bounded bulk call whose end-offset range cannot cross the remaining byte
budget. It makes no lookahead call. The figures above explain why that
tightening was necessary, but they do not describe the final candidate-fetch
implementation. Fresh results for the tightened path are reported below.

The default is 1 MiB. In fresh 32-reader final-path cells, all three page sizes
completed 64 measured responses during the six-second offer window. The
256 KiB cell had the lowest p99 and RSS on this host. The 4 MiB cell used
653.2 MiB peak RSS, versus 195.9 MiB at 1 MiB. This short local result does not
claim that 1 MiB is the throughput optimum. It shows the memory cost of larger
pages; the production-representative GKE comparison remains blocked by cloud
access.

## Environment and method

The local runs used an arm64 macOS host with 14 logical CPUs, Go 1.26.4, and
Redis 8.8.0 in Docker with AOF `everysec`. Chronicle, Redis, and dsload shared
the host, so these numbers show boundedness and local tradeoffs. They are not
production capacity numbers.

The before binary came from
`0ab5d5832b87c673477c1021a78458e7e391fe0d`. The after binary came from this
working tree. Each stream held 4,096 records of 4,096 bytes. Eight streams were
prefilled. Catch-up requests arrived at 200 per second and the selected reader
count capped active requests. A client-cap drop means dsload declined to start
another request. It is not a server error.

Requests are assigned to warmup or measurement by their scheduled start time.
Measurement-eligible requests may drain after the offer window closes, and
their completions and latency remain in the measured result. Warmup requests
remain excluded even if they finish after the measurement window opens.
Throughput divides eligible completed bytes by the configured offer duration.

Build and server commands:

```sh
go build -o bin/chronicle ./cmd/chronicle
(cd loadgen && go build -o /private/tmp/dsload-issue5 ./cmd/dsload)

./bin/chronicle \
  -listen 127.0.0.1:4447 \
  -metrics-listen 127.0.0.1:9447 \
  -redis-url redis://127.0.0.1:6379/13 \
  -subscriptions=false -ui=false -log-level=error \
  -read-page-bytes=1048576
```

One after-cell command, with `N` and the short-cell duration overrides changed
by the matrix below:

```sh
/private/tmp/dsload-issue5 run \
  -scenario loadgen/scenarios/catchup-paged.yaml \
  -label final-1m-rN \
  -out /private/tmp/issue5-final-results \
  -base-url http://127.0.0.1:4447 \
  -duration DURATION -warmup 1s -catchup-readers N \
  -sample-pid chronicle=PID \
  -sample-redis redis=127.0.0.1:6379 \
  -sample-metrics chronicle=http://127.0.0.1:9447/metrics
```

The 8- and 32-reader cells used a six-second offer window and one-second
warmup. The 128- and 512-reader cells used the scenario's standard 30-second
offer window and five-second warmup. The local base used the old short matrix
for 8, 32, and 128. Deliberately repeating the unbounded 512-reader shape on the
shared laptop would recreate the known multi-GiB failure mode, so the before
value for 512 is the sealed issue artifact `20260729T151520Z-675c412d`.

## Reader curves

| Source | Readers | Completed | MiB/s | p50 | p99 | Errors | Chronicle RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| base local | 8 | 51 | 121.26 | 1.25 s | 1.40 s | 0 | 436.3 MiB |
| final local | 8 | 56 | 149.37 | 0.81 s | 0.90 s | 0 | 69.5 MiB |
| base local | 32 | 46 | 79.94 | 6.30 s | 10.12 s | 0 | 793.5 MiB |
| final local | 32 | 64 | 170.71 | 3.26 s | 3.72 s | 0 | 195.9 MiB |
| base local | 128 | 0 | 0 | n/a | n/a | 128 | 723.7 MiB |
| final local | 128 | 435 | 232.06 | 8.77 s | 8.95 s | 0 | 470.7 MiB |
| base sealed, 16 vCPU | 512 | n/a | 57.60 | n/a | 34.10 s | 940 | n/a |
| final local | 512 | 336 | 179.24 | 24.12 s | 28.85 s | 0 | 1,305.3 MiB |

The sealed base and local final runs use different hardware and are not a
latency comparison. The useful result at 512 is the finite memory shape, 336
body-integrity-checked completions, and zero server errors. The local base
failed all 128 accepted requests with HTTP 500 after Redis read timeouts. The
final paged cell completed 435 measured responses at that reader cap.

Machine-readable results are in
[`artifacts/issue-5-local-curves.tsv`](artifacts/issue-5-local-curves.tsv).

## Final storage and runtime metrics

Every completed response used 16 pages. The sampled counter deltas below start
at the first one-second sample, so fetched, returned, discarded, and allocated
bytes are lower bounds when work began before that sample. Script time is the
sum of client-observed wall time across concurrent calls. It includes time
queued behind Redis and is not Redis CPU time.

| Readers | Allocated | Heap max | GC cycles | Fetched | Returned | Discarded | Script wall time / calls | Redis CPU | Redis ops/s max | Blocked |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 2,150.08 MiB | 26.63 MiB | 165 | 934 MiB | 934 MiB | 0 | 21.12 s / 1,868 | 23% | 2,260 | 0 |
| 32 | 2,653.73 MiB | 130.90 MiB | 55 | 1,154 MiB | 1,154 MiB | 0 | 106.71 s / 2,308 | 25% | 2,422 | 0 |
| 128 | 18,035.40 MiB | 394.87 MiB | 108 | 7,879 MiB | 7,879 MiB | 0 | 2,037.09 s / 15,758 | 44% | 3,157 | 0 |
| 512 | 27,917.34 MiB | 1,126.92 MiB | 58 | 12,201 MiB | 12,201 MiB | 0 | 11,170.94 s / 24,402 | 47% | 3,557 | 0 |

The final aligned workload has no candidate-byte amplification: fetched equals
returned at every reader point, and discarded is zero. Redis reads one
candidate and, only when more of the selected segment exists, bulk-fetches the
offset range that can fit the remaining target. It does not fetch and discard a
lookahead frame. The 1,024-frame limit still bounds small-frame work. A first
indivisible oversized frame can exceed the byte target, but a page cannot
speculatively materialize hundreds of later oversized frames.

Full compact metrics are in
[`artifacts/issue-5-after-metrics.tsv`](artifacts/issue-5-after-metrics.tsv).

## Returned page size choice

| Returned payload target | Readers | Completed | MiB/s | p50 | p99 | Errors | Chronicle RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 KiB | 32 | 64 | 170.71 | 2.38 s | 2.65 s | 0 | 61.4 MiB |
| 1 MiB | 32 | 64 | 170.71 | 3.26 s | 3.72 s | 0 | 195.9 MiB |
| 4 MiB | 32 | 64 | 170.71 | 2.81 s | 3.12 s | 0 | 653.2 MiB |

All three short cells retained the same 64 measured completions and therefore
the same offered-window byte rate. The 256 KiB cell had the best p99 and lowest
RSS on this shared host. The 4 MiB cell used 457.3 MiB more RSS than 1 MiB.
These results establish the memory tradeoff but do not prove that 1 MiB is the
best production latency point. Managed-Valkey GKE data is still required for
that claim.

The exact values are in
[`artifacts/issue-5-page-size.tsv`](artifacts/issue-5-page-size.tsv).

## Mixed load

The accepted mixed workload offered 40 single-record writes per second from
eight writers while 32 catch-up readers remained active. It completed all
1,200 scheduled writes during the 30-second offer window, which is 40.0 per
second. Append p99 was 163.33 ms. It also completed 290 measured catch-up reads,
returned 4,648.19 MiB, and recorded no errors. A fresh Chronicle process peaked
at 190.1 MiB RSS.

The paid mixed cell has two explicit gates. It must complete at least 38
successful appends per second, which retains 95 percent of the 40 per second
offer. The rate denominator is the configured offer window, not the later
catch-up drain period. Append p99 must not exceed 2,000 ms. The fresh local
accepted run met both limits at 40.0 appends per second and 163.33 ms p99. Zero
error counters remain required, but they no longer decide the mixed result by
themselves.

An exploratory 500-write-per-second offer was beyond this local cell's
capacity. It produced 275 append errors and a 23.15-second append p99. That run
is an overload boundary, not a passing capacity result. Per-shard admission
control remains separate work.

## Redis-side bound

A Redis commandstats regression captures the common one-frame refresh. It
resets commandstats, reads one frame whose offset is also the segment upper
bound, and observes exactly one `ZRANGEBYLEX` call. A multi-frame segment can
make one additional bounded range call:

```text
"evalsha" "<read-sha>" "4" ... "(<offset-0>\xff" "[<offset-4>\xff"
  "1048576" "1024" "<incarnation>" "0" "1" "1" "1"
[lua] "ZRANGEBYLEX" "ds:{/trace}:msg"
  "(<offset-0>\xff" "[<offset-4>\xff" "LIMIT" "0" "1"
commandstats:cmdstat_zrangebylex calls=1
```

The returned byte target and frame cap are script arguments. The first
`ZRANGEBYLEX` has `LIMIT 0 1`. If more of the segment exists and the first
frame fits, the second call ends at `firstOffset + remainingTargetBytes` and
uses the remaining part of the 1,024-frame limit. Static inspection finds no Go
`ZRangeByLex` suffix read, no Lua `ZRANGEBYLEX` without `LIMIT`, and no
lookahead query.

## Allocation profile

The 16 MiB handler benchmark used a discard response writer that records its
largest individual `Write`:

```sh
go test . -run '^$' \
  -bench 'BenchmarkCatchupStreaming16MiB/page_bytes=1048576$' \
  -benchtime=50x -benchmem \
  -memprofile /private/tmp/chronicle-catchup-alloc.pprof
```

Result:

```text
50  109128 ns/op  153739.58 MB/s  4096 max-write-B  385583 B/op  124 allocs/op
```

The largest write was one 4 KiB frame, not a 16 MiB body. In the allocation
profile, the fixture store append used 99.31 MiB. `MemoryStore.ReadPage` used
21.16 MiB cumulatively across 50 responses, about 0.423 MiB per response.
`Handler`, `handleRead`, and `streamCatchupResponse` had zero flat allocation.
The final in-use profile retained only 2.57 MiB in runtime scheduling objects.
There is no second complete response buffer.

## Correctness and fault results

The new `paged-catchup` k3d scenario ran six attempts across a 136-frame fork
snapshot. Four attempts injected a fault or interruption: client cancellation,
a transport interruption, all Chronicle origins being killed, and Redis being
killed and restarted. A fifth captured a snapshot while an append and close ran
concurrently. The sixth performed a clean final drain. The direct Redis ZSET
oracle matched the acknowledged frames exactly.

The maintained fault scenarios also passed:

- Baseline, Chronicle restart, and Redis restart each preserved all 320
  appended messages and all 8 final tails with a duplicate factor of 1.
- The data-plane Porcupine history was linearizable across 3,472 operations.
- The single-holder Porcupine history was linearizable across 672 operations,
  including 648 claims and 24 grants.

The k3d cluster was deleted after the run.

## Adversarial regression evidence

The release-blocking paths have focused reproductions:

- A real HTTP/1.1 client receives the first frame and advertised snapshot tail,
  then body read fails with `unexpected EOF` for both a later storage error and
  a no-progress page. HTTP/2 fails both cases with an `INTERNAL_ERROR` stream
  reset. Neither transport can accept a clean complete response with the
  advertised tail.
- A 1 MiB ordinary SSE payload produces three underlying writes: the `data: `
  prefix, one contiguous payload write, and the record terminator. It does not
  perform one write per byte.
- A warmup operation that completes after measurement opens contributes zero
  samples. A request scheduled during measurement and completed during drain
  contributes one sample.
- The catch-up acceptance gate rejects zero completed readers. It also rejects
  a result whose aggregate body total is one byte below eight complete
  16,781,313-byte responses.
- Redis oversized-frame cases recorded `65,536 fetched = 65,536 returned` and
  `8 fetched = 8 returned`, both with zero discarded bytes. The common
  one-frame page recorded one `ZRANGEBYLEX`.
- Deleting a selected stream message key while metadata still advertises a
  nonempty range returns `ErrReadDataMissing`. It never advances to the
  advertised tail.
- The legacy compatibility pager with frame sizes 6, 10, and 1 at an eight-byte
  target produces pages of 6, then an oversized-first 10, then 1. Concatenation
  is byte-for-byte exact, so no durable frame is skipped.

## Local gates

All required gates passed:

```text
make test         PASS, race-enabled packages including Redis differential tests
make lint         PASS, 0 issues with fresh Go and golangci-lint caches
make conformance  PASS, 332/332
make spec-check   PASS, version 0.3.5 and no upstream protocol drift
docs-site check   PASS, Astro reported 0 errors, warnings, or hints
docs-site build   PASS, 13 static pages built
dsui gates        PASS, typecheck, Biome, and 471 Vitest tests
```

The loadgen module also passed `go test -race -count=1 ./...`, including the
mixed capacity and latency gate tests. Both new scenarios passed
`dsload validate`, the paid spec rendered with its SLO command, and
`bash -n loadtest/ltctl.sh` passed. A 50-iteration repeat of the 16 MiB
streaming benchmark kept its largest write at 4 KiB and reported 385,583
bytes and 124 allocations per operation.

## Cloud preflight blocker

The paid suite was retried on 30 July 2026 after the reported VPC fix. It did
not provision anything. Preflight confirmed active account
`adityavkk@gmail.com`, project `adityavkk-prototyping`, enabled billing, and the
Container, Redis, Artifact Registry, and Cloud Build APIs. The
`memorystore.googleapis.com` API required to enumerate managed Valkey 8
instances is not enabled.

The current identity still received `SECURITY_POLICY_VIOLATED` from the
project's VPC Service Controls perimeter:

| Check | VPC Service Controls unique ID |
| --- | --- |
| regional Compute quota | `lSSbKTDwapQUsn64PozcLMAwZmBM0aTaBjNs-Ljys8cyYDeNmLR6l4GXeL31StDdFaw7FWBG8HswugM` |
| Artifact Registry describe | `8tZ1iAqyE_HHoyHP0oDccAatzJN-FndAV6rNGYzhDliZcl11h1YwmPViEmVXU0V-VVTDkh8-pWOXknM` |
| default VPC describe | `g5jR-uCZTgoZgKRC3pIu_iQs0kvA-TYO4bxKK-X180PyItz-gym88GCi75cX1QSDqtECFe4ICa7oqn0` |
| GKE cluster list | `2C0CxDK75qzAZE5VmI_LAtDbZ_O46W_n3qFNWFmS375NyY3xyYWhS6akyQrjao9vdR-SnOTroXgYgEI` |

The load-test runbook forbids provisioning until these checks pass, so stopping
before either paid suite was required. No GKE or Valkey resources were created,
so there was nothing to tear down. The experiment is ready in
`loadtest/spec/catchup-paged.yaml`. For the production target, enable the
Memorystore API and pass an approved managed Valkey 8 primary endpoint:

```sh
cd loadtest
LT_PROJECT=adityavkk-prototyping \
LT_MACHINE=e2-highcpu-16 \
LT_REDIS_URL=redis://VALKEY8_PRIMARY_IP:6379/0 \
make all SPEC=spec/catchup-paged.yaml
```

The spec renders one 14-CPU Chronicle pod and one loadgen job on the separate
loadgen pool. The job runs 8, 32, 128, and 512 readers plus the mixed cell,
copies result artifacts before teardown, and enforces the retained write-rate,
append-latency, and zero-error gates.

## Acceptance checklist

- No single Redis range or script invocation can return an unbounded suffix.
  The first `ZRANGEBYLEX` fetches one candidate. An optional second call is
  bounded by both the remaining target bytes and the remaining part of the
  1,024-frame cap. The compatibility `Store.Read` method can concatenate
  multiple bounded pages in Go. The commandstats regression proves the common
  one-frame page uses one range call.
- The handler has no second complete response body. Pages are written and
  flushed directly; the 16 MiB allocation profile and 4 KiB maximum write prove
  the memory shape.
- Reader curves report throughput, p50, p99, errors, RSS, heap allocation,
  Redis CPU, script time, blocked clients, and fetched versus returned bytes.
- The 512-reader cell used a 1 MiB returned payload target, had bounded
  1,305.3 MiB RSS, and recorded zero server errors at 200 offered reads per
  second. The corrected measurement window counted 336 complete,
  body-integrity-checked responses; warmup completions were excluded.
- Mixed load preserved 40.0 successful appends per second with 163.33 ms append
  p99 while 32 catch-up readers were active. The paid gate requires at least 38
  appends per second and at most 2,000 ms p99.
- Page equivalence covers empty, exact, over-target, oversized, 256 KiB, one
  MiB, four MiB, JSON, binary, offsets, close, concurrent append, fork, TTL,
  absolute expiry, non-expiring streams, and MemoryStore versus Redis.
- Cancellation is tested before the first page, between pages, during storage
  queueing, during write, and after flush. The Redis pool test proves a queued
  read exits and the connection is reusable.
- The paged fault checker proves forward-only, exact-once coverage against the
  direct Redis oracle. Maintained Jepsen and Porcupine histories still pass.
- `make test`, `make lint`, `make conformance`, and `make spec-check` pass. The
  paid cloud run is the only missing evidence, and its external access blocker
  is recorded above.
