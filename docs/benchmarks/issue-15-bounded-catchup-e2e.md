# Issue 15 bounded catch-up end-to-end evidence

Date: 2026-07-31

This report records the internal GPA/chronicle issue 15 implementation and the
evidence retained in the public source-of-truth repository. General raw load
output, credentials, caches, and deployment paths are deliberately excluded;
the compact machine-readable summary is
[`results/issue-15-local-summary.json`](results/issue-15-local-summary.json).

## Baseline and dependency handoffs

The refreshed public baseline recorded before editing was
`origin/main@2f703d4a3dc034e646f2bd1cf06b21dc904a7939`. Issue 15 was then reconciled
on the integration-ready dependency line:

- issue 5 bounded paging: `3318a2c`;
- issue 7 read purity and typed read state: `9983d0e`, followed by the public
  fused root decision in `4efc2ec`;
- issue 13 shared SSE fanout and notification multiplexing:
  `0171e01dc99f13da70264b092f33c9ab5c090144` with its evidence commit
  `c7be7b49f739637792870f6860f4e11da06fe2f4`.

The issue 15 branch was based on `c7be7b4`. The obsolete duplicate branch
commit `d52a114` was not merged. No internal mirror was edited or pushed.

## Architecture result

The ordinary root-owned page uses the existing pure typed
`ClassifyRootReadRange` oracle and its atomic Lua mirror. Root validation,
snapshot capture, and the first bounded frame fetch occur in one `read.lua`
invocation. The first-candidate plus byte-bounded bulk algorithm still has no
lookahead, returns at most 1,024 frames, and admits one oversized first frame so
the cursor always progresses.

`PageWaiter` returns the exact final durable page from the shared notification
multiplexer. Long-poll does not reread the same delivery offset. A new SSE hub
confirms notification registration before its authoritative page and seeds the
hub from that page. Delivery uses the existing hub; issue 15 adds no hub,
subscription loop, or per-wait Pub/Sub connection.

`--max-append-bytes` and `CHRONICLE_MAX_APPEND_BYTES` add an optional request
ceiling. Zero keeps the existing policy. A declared excess body returns 413
before body allocation or store access. An unknown-length body is bounded while
being read. Startup checks that the ceiling plus Chronicle's 34-byte stored
frame prefix fits `proto-max-bulk-len` when the service permits `CONFIG GET`.

The Redis-only `PageReaderSessionFactory` passed its profile gate. It retains
an immutable fork plan for one serial, non-SSE response. Every page still runs
the root script and every selected source still runs its script with the
expected incarnation. A missing source is `ErrReadDataMissing`; a recreated
source is `ErrReadSnapshotChanged`. A nil snapshot starts a new response plan,
and `Close` discards it. There is no global cache, connection, goroutine, or
background lifetime.

## Exact command and write-shape checks

Focused Redis tests assert these steady-state shapes:

- one ordinary root page: one `EVALSHA`, one `HGETALL`, one
  `ZRANGEBYLEX`, no producer-hash read, and no lookahead;
- one one-frame page: one `ZRANGEBYLEX`;
- valid persistent and absolute-expiry reads: no `HSET`, `HSETNX`,
  `PEXPIRE`, `PERSIST`, `DEL`, or other steady-state write;
- persistent read AOF size: `19891648 -> 19891648`; replication offset:
  `36737 -> 36737`;
- absolute-expiry read AOF size: `19892401 -> 19892401`; replication offset:
  `36744 -> 36744`;
- multiple oversized frames: fetched bytes equal returned bytes on every page,
  with zero discarded bytes;
- long-poll delivery: the `ReadWaitResult.Page` is returned directly, with no
  same-offset reread;
- SSE: one data event and one control event per storage page, with exact final
  close ordering.

The command tests are deterministic `MONITOR`, `INFO`, `PTTL`, and AOF-size
assertions in `store/redis/read_commands_test.go`,
`store/redis/page_differential_test.go`, `live_read_redis_test.go`, and the
handler and hub tests. The Go and Lua root-range decisions run through the same
differential table.

## Append and completed-reader integrity

`TestCompletedReaderIntegrityAtExact16MiBAppendCeiling` appends exactly
16,777,216 bytes at the configured ceiling and drains the completed response
through a constant-memory SHA-256 writer. It checks the exact byte count,
digest, next offset, and up-to-date state. JSON and binary load fixtures compute
their expected response digest incrementally; a measured read counts as
complete only when the received digest matches. Warmup responses do not enter
the measured completion or integrity counters.

Declared and chunked excess bodies, exact-limit create and append, request
authorization order, producer fencing, and store non-entry are covered by the
append-ceiling handler tests. The default remains disabled, so protocol
compatibility does not acquire a new unconditional body limit.

## Fork-plan profile gate

`BenchmarkReadPageForkPlan` used local Valkey 8 and 64 one-frame pages per
response. Each cell ran 20 samples.

| Fork depth | Reader | Median | Metadata `HGETALL` | `EVALSHA` |
| ---: | --- | ---: | ---: | ---: |
| 1 | baseline per-page plan | 65.214 ms | 192 | 128 |
| 1 | response-local plan | 43.902 ms | 129 | 128 |
| 4 | baseline per-page plan | 123.841 ms | 384 | 128 |
| 4 | response-local plan | 48.942 ms | 132 | 128 |

The latency improvements were 32.7 and 60.5 percent. The unchanged script
count is intentional: plan reuse removes metadata discovery, not root or source
validation. Allocation counts also fell from about 13,183 to 11,548 at depth
one and from 19,136 to 11,641 at depth four.

## Local reader and mixed gates

The reader suite used eight 16 MiB streams made from 4,096 frames of 4,096
bytes, JSON response framing, a five-second warmup, and a 30-second measured
window. Chronicle, Valkey, and the load generator shared the same host. These
are boundedness and regression results, not production capacity claims. The
same Chronicle process served the four reader cells, so the table is not a
fresh-process capacity comparison.

| Readers | Measured complete | Integrity OK | p99 | Peak Chronicle RSS |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 376 | 376 | 818.687 ms | 72.3 MiB |
| 32 | 396 | 396 | 2,494.463 ms | 133.5 MiB |
| 128 | 389 | 389 | 9,748.479 ms | 501.0 MiB |
| 512 | 115 | 115 | 26,263.551 ms | 1,318.4 MiB |

Every completed response contained exactly 16,781,313 framed bytes. All cells
had zero request errors. Sampled storage-counter deltas had fetched bytes equal
returned bytes and zero discarded bytes at 8, 32, 128, and 512 readers. Those
deltas span cell setup and drain and are intentionally not divided by the
measurement-only completion counts.

The corrected mixed suite used a fresh Chronicle process, 32 catch-up readers,
and eight writers offering 40 appends per second for 30 seconds. It completed
all 1,200 appends: 40.00 successful appends per second, append p99 298.24 ms,
zero append or catch-up errors, 388 catch-up completions, 388 completed-body
SHA-256 reads, 6,521,047,285 catch-up bytes, and 153.2 MiB peak Chronicle RSS.
It passes the issue gate of at least 38 successful appends per second and
append p99 no greater than 2,000 ms. A static catch-up digest is not asserted
during mixed concurrent appends because each read legitimately captures a
different tail; the gate instead proves every successful response body reached
EOF and produced its SHA-256, and rejects zero work, partial counts, or any
append or catch-up error counter.

The final constant-memory 16 MiB handler benchmark ran ten complete responses at
each decision-matrix size. It reported 214,917 ns/op and 235 allocs/op at
256 KiB, 97,242 ns/op and 124 allocs/op at 1 MiB, and 67,425 ns/op and 72
allocs/op at 4 MiB. Every cell limited the underlying writer to 4,096 bytes per
call. The derived in-process copy rates are handler microbenchmark numbers, not
network or production-capacity claims.

## Transport, Jepsen, and Porcupine

After committed catch-up headers, an injected page read error and a no-progress
page both abort the transport. HTTP/1.1 returned the already committed `aa`
prefix and then `unexpected EOF`; HTTP/2 returned the same prefix and reset the
stream with `INTERNAL_ERROR`. Neither path appended a JSON error body.

The k3d AOF-Redis run passed all default scenarios:

- baseline: 320 appends, 8/8 streams at tail;
- origin restart: 320 appends, kill-origin and kill-all-origins, 8/8 at tail;
- Redis restart: 320 appends, 8/8 at tail;
- paged catch-up: five injected between-page events plus the final closed drain
  passed against a 136-frame Redis oracle;
- read expiry: sliding renewal, HEAD no-touch, fixed absolute expiry, and
  persistent reads all passed;
- SSE resume: 8/8 clients observed 320 messages exactly once through 13 strict
  notification, origin, Redis, slow/stuck-reader, page-boundary,
  delete/recreate, and timeout faults.

The cluster-backed safety run also passed `single-holder-linz` (662 operations,
linearizable), `cursor-monotonic` (1,352 samples, no regression or phantom
advance), and `stale-gen-noop` (409 FENCED with byte-identical cursor state).

The CI Redis-direct Porcupine matrix passed:

| Model | Operations | Result |
| --- | ---: | --- |
| ownership exclusivity | 169 across 4 slots | linearizable |
| per-shard lease | 24 across 12 partitions | linearizable |
| store, Redis frames | 2,077 | linearizable |
| store, Redis chunks | 1,767 | linearizable |
| store, local files | 1,442 | linearizable |
| store, object cache | 1,333 | linearizable |
| composed two-fence | 1,423 | linearizable |

Unknown is a failure in every Porcupine driver; none returned Unknown.

## Managed Valkey 8 boundary

The authorized target is project `adityavkk-prototyping` with managed Valkey 8.
The production reader curve, mixed gate, profiles, and page-size decision are
not replaced by the local runs above.

The 2026-07-31 preflight used active account `adityavkk@gmail.com`. Billing was
enabled. Container, Redis, Artifact Registry, and Cloud Build APIs were enabled,
but `memorystore.googleapis.com` was disabled. Therefore no managed Valkey 8
primary endpoint existed that the rig could be authorized to use.

The documented quota command was blocked before returning quota by VPC Service
Controls, unique identifier
`S8PQvq8PPCwnzLa0U9v2w2XR6aAwWCo-UpnOnr1BfYbpYEBGbsP3hHEPqr8UXtut2ZSfIVsZCSZVltw`:

```sh
gcloud compute regions describe us-central1 \
  --project adityavkk-prototyping --flatten=quotas \
  --format='csv[no-heading](quotas.metric,quotas.limit,quotas.usage)'
```

Read-only inventory calls were also policy-blocked: GKE cluster list identifier
`2oPooAF7zZP8CbAI6lkt4nZTlDB81qv4uWOlOeXg4j20aqUGj6dCixPJ1z4aiZxsD8Q7Thd62mU4gVY`,
Redis list identifier
`OpRZiKqEzqzLZ3n5qB5aKx12KqItd0vZEs_w1dz9Si9Kwi4WiBQ9DUd8J2eljVdBo-g7Zr-ZtSZpBgg`,
and Artifact Registry list identifier
`rjmD1q_fT3r8ySFOTZ2lZDZ_pBpPhyFXI5iCUcJMHlO1rTOp_87oqrWn8s_Zq1GTQMa4QBUfIkUV0RQ`.
`gcloud memorystore instances list --location=us-central1` returned
`SERVICE_DISABLED` for `memorystore.googleapis.com`.

No provisioning or build command ran after the failed preflight, so this task
created no GKE, managed Valkey, Redis, Artifact Registry, or Cloud Build
resource. The exact prerequisite is access from a context admitted by the VPC
Service Controls perimeter plus an enabled Memorystore API and an authorized
managed Valkey 8 primary URL. Then run:

```sh
cd loadtest
LT_PROJECT=adityavkk-prototyping \
LT_MACHINE=e2-highcpu-16 \
LT_REDIS_URL=redis://VALKEY8_PRIMARY_IP:6379/0 \
make all SPEC=spec/catchup-paged.yaml
```

For the controlled page-size decision, repeat the same topology twice at each
size. `LT_READ_PAGE_BYTES` changes only the SUT page target, and result labels
include the selected size:

```sh
for page_bytes in 262144 1048576 4194304; do
  for repeat in 1 2; do
    LT_PROJECT=adityavkk-prototyping \
    LT_MACHINE=e2-highcpu-16 \
    LT_REDIS_URL=redis://VALKEY8_PRIMARY_IP:6379/0 \
    LT_READ_PAGE_BYTES="$page_bytes" \
    LT_TAG="i15-${page_bytes}-r${repeat}" \
    make all SPEC=spec/catchup-paged.yaml
  done
done
```

The checked-in 1 MiB default remains unchanged unless all six managed runs are
available and the repeated comparison supports a different choice.

`make all` owns and tears down its GKE cluster and node pools. It treats the
provided managed Valkey endpoint as external and never deletes it.

## Reproduction commands

The task-local Go cache is always explicit:

```sh
GOCACHE=/private/tmp/chronicle-i15-gocache make test-unit
env -u REDIS_URL GOCACHE=/private/tmp/chronicle-i15-gocache go test -count=1 ./...
GOCACHE=/private/tmp/chronicle-i15-gocache make test
GOCACHE=/private/tmp/chronicle-i15-gocache make lint
GOCACHE=/private/tmp/chronicle-i15-gocache make spec-check
GOCACHE=/private/tmp/chronicle-i15-gocache make conformance
(cd loadgen && GOCACHE=/private/tmp/chronicle-i15-gocache go test -race -count=1 ./...)
./bin/chronicle -listen 127.0.0.1:4447 -metrics-listen 127.0.0.1:9097 \
  -subscriptions=false -ui=false -redis-url redis://127.0.0.1:6379/15 \
  -read-page-bytes 1048576 -max-append-bytes 16777216
./bin/dsload run -scenario loadgen/scenarios/mixed-catchup-paged.yaml \
  -label i15-review-mixed-r32 -out /private/tmp/chronicle-i15-results \
  -base-url http://127.0.0.1:4447 -sample-pid chronicle=PID \
  -sample-redis redis=127.0.0.1:6379 \
  -sample-metrics chronicle=http://127.0.0.1:9097/metrics
./bin/dsload gate-mixed \
  -result /private/tmp/chronicle-i15-results/i15-review-mixed-r32/mixed-catchup-paged/results.json \
  -min-append-rate 38 -max-append-p99-ms 2000
CLUSTER=chronicle-i15-jepsen GOCACHE=/private/tmp/chronicle-i15-gocache jepsen/up.sh
CLUSTER=chronicle-i15-jepsen GOCACHE=/private/tmp/chronicle-i15-gocache STREAMS=8 MSGS=40 jepsen/run.sh
CLUSTER=chronicle-i15-jepsen jepsen/down.sh
```

The commands above intentionally name the task-local cache and evidence root.
The raw result directory is deleted during final teardown after the compact
summary and exact gate values are recorded here.

## Final gate ledger

| Gate | Final outcome |
| --- | --- |
| `make test-unit` | PASS, race detector, all short packages |
| `env -u REDIS_URL go test -count=1 ./...` | PASS, including Redis differential parity and immutable-segment modes |
| `make test` | PASS, race detector with repository Redis |
| loadgen `go test -race -count=1 ./...` | PASS |
| `make lint` | PASS, 0 issues from a new task-local lint cache |
| `make spec-check` | PASS, suite 0.3.5 and upstream commit `82f9963` unchanged |
| `make conformance` | PASS, exactly 332 of 332 tests |
| dsui | PASS, typecheck, Biome 100 files, 38 test files and 471 tests |
| docs | PASS, Astro check 0 errors/warnings/hints; build 17 pages |
| 16 MiB handler benchmark and integrity | PASS, exact body and digest; bounded 4 KiB underlying writes |
| local reader curve and corrected mixed gate | PASS, values above and in the compact JSON summary |
| k3d default Jepsen plus safety supplement | PASS; cluster deleted after the run |
| Redis-direct Porcupine maintained matrix | PASS, every model linearizable and none Unknown |
| fresh adversarial re-review | PASS, zero P0 and zero P1; both cheap P2 notes fixed |
| managed Valkey 8 GKE acceptance | BLOCKED before provisioning by the recorded VPC Service Controls and API prerequisites; no local substitute claimed |
