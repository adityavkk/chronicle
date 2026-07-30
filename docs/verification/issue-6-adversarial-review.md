# Issue 6 adversarial review and validation record

## Independent review scope

A fresh independent adversarial reviewer inspected base
`ec1a63be7571f6ca03fd4401332002730c8c8349` plus the full uncommitted diff,
issue requirements, ADR, migration and rollback design, all segment modes,
object cache, benchmark artifacts, crash points, the 332-test Durable Streams
contract, and the Jepsen and Porcupine requirements. The review was
attack-oriented and made no repository edits.

Its initial verdict was **not releasable**. The reviewer found no direct
acknowledged-write loss in the authoritative Redis frame ZSET or the unchanged
default `off` path. It did find two P0 integrity races, five P1 release gaps,
three P2 evidence/default gaps, two hardening risks, and two unrelated
artifacts.

## Finding dispositions

Line ranges below refer to the pre-fix diff reviewed at the stated base.

| Severity | Finding and evidence | Reproduction | Disposition |
|---|---|---|---|
| P0 | GC could delete staged objects between `Backend.Put` and manifest `Publish`: `store/segments/file_backend.go:148-188,269-305,550-638`; `store/segments/redis_backend.go:105-139,161-197,231-315`. | Pause generation 2 after `Put`, collect from an independent backend, resume `Publish`, then read every current ref. Before the fix the collector reported `{ManifestsKept:1 ManifestsDeleted:0 SegmentsKept:2 SegmentsDeleted:2}`. | **Valid, safety fixed, operation narrowed.** File, object-emulator, and Redis GC are audit-only and delete nothing. Deterministic file/object and Redis publisher-race tests pass. Real reclamation remains disabled until a publication/staging barrier and shared holds exist. |
| P0 | `segments.Store.Read` could validate an old manifest and then concatenate a recreated stream's primary tail: `store/segments/store.go:95-106,286-319,359-411`. | Pause after the old manifest is loaded and verified, delete and recreate with the same clock and content type, append past the old sealed boundary, then resume. Before the fix the test returned two messages: an old prefix plus a new-incarnation payload. | **Valid, fixed.** Streams have a persisted random 128-bit incarnation, Redis backfills legacy metadata atomically, and `ReadAtomic` binds metadata and tail. A mismatched snapshot retries through the primary without mixing bytes. Same-node, Redis, mixed-version seal, and pinned-snapshot regressions pass. |
| P1 | Internal sealing used public `Read` and renewed sliding TTL: `store/segments/store.go:202-205,331-346`; `store/redis/store.go:642-646,835-837`; `store/memory_store.go:787-813`. | Advance a fake clock near expiry, seal or close-and-seal, then cross the original deadline. | **Valid, fixed in the integrated design.** Sealing and repair drain the bounded `PageReader` with `NoTouch`. A client renews TTL on the first snapshot page only. Continuations validate the same snapshot without renewal. Memory and Redis differential tests cover both rules. |
| P1 | Shadow promotion and no-change seal validated metadata only, not referenced data and indexes: `store/segments/store.go:206-209,376-380,449-491`; `docs/adr/0006-immutable-segment-read-plane-prototype.md:101-109`; `docs/runbooks/immutable-segment-migration.md:33-56`. | Seal in shadow, remove or corrupt a ref, no-op seal, then promote. | **Valid, fixed.** No-change seal and every transition verify every ref. `Repair` rebuilds generation-qualified objects from a TTL-neutral primary snapshot and publishes by CAS. Missing and corrupt local, object-emulator, and Redis cases pass. |
| P1 | Snapshot and rollback pins were process-local and ordinary reads did not pin: `store/segments/store.go:359-411,499-555`; `docs/runbooks/immutable-segment-migration.md:121-123`. | Hold a reader on replica A while collector B deletes a retained generation. | **Valid, fixed for local multi-page reads and still fail-closed for reclamation.** Every segment reader now gets a unique lease that pins one manifest and one primary snapshot through final-page confirmation, then explicit release. Errors and cancellation release it immediately. Cross-replica deletion cannot hurt because deletion remains disabled. Shared holds are still required before reclamation. |
| P1 | Conformance avoided candidate defaults and could pass without exercising segment reads: `config.go:267-272`; `scripts/conformance-segments.sh:6-11,23-39`. | Run serving with a backend that always falls back to Redis. | **Valid, fixed for the branch.** Candidate defaults are now shadow with read-triggered sealing disabled. Seven 332-test cells cover off plus shadow and serving for every mode. Metrics require shadow `reads=0,seals>0` and serving `reads>0,seals>0`. |
| P1 | Maintained fault/model CI did not cover candidate modes: `.github/workflows/ci.yml:118-122`; `jepsen/checker/main.go:116-121`; `docs/verification/immutable-segments-fault-matrix.md:24-40`. | Run `store-linz` only with its default `segment-mode=off`. | **Valid, fixed for Porcupine.** CI has candidate cells. The final local matrix passed all four modes directly and all four through randomized Toxiproxy partitions and latency. Multi-process Chronicle restart, server-mode Redis restart, and a real object service remain release-environment gates. |
| P2 | Benchmark evidence omitted p95, did not assign Redis Lua time, covered only half the streams at eight readers, and mixed warmup/drain resource samples: `docs/adr/0006-immutable-segment-read-plane-prototype.md:180-193`; `loadgen/run/sampler.go:187-230,261-274`; `loadgen/run/catchup.go:35-52`. | Inspect the sampler, reader assignment, and checked summaries. | **Valid; cheap code fixes included, evidence still rejected.** The harness now emits p95, parses `cmdstat_eval*`, rotates readers, and phase-tags samples. The 32-cell working summary has catch-up and TTFB p95 in 32/32 rows and append p95 in all 16 mixed rows; the four read-only script-time rows have catch-up and TTFB p95. Raw accepted inputs were overwritten or ignored and the run predates correctness fixes, so no release comparison is claimed. |
| P2 | Enabling a candidate defaulted new manifests directly to serving: `config.go:267-272`; `docs/runbooks/immutable-segment-migration.md:78-90`. | Enable a segment mode without specifying state. | **Valid, fixed.** Global layout remains `off`; candidate state defaults to `shadow`; read-triggered sealing defaults false. |
| P2 | The fault matrix overstated one-shot hook coverage: `docs/verification/immutable-segments-fault-matrix.md:7-20`; `store/segments/store_test.go:506-611,773-814`. | Compare each claimed crash surface with the first-matching file fault injector. | **Valid, fixed in documentation.** The matrix now states exact deterministic hooks and separates remaining Redis-command, later-object, partial-cache, real-object, restart, and cross-replica gates. |
| Hardening | Unknown migration state could be treated as serving: `store/segments/store.go:376-380,449-491`. | Load a digest-valid manifest with an unrecognized state. | **Valid, cheap fix included.** `validateManifest` rejects it and the regression passes. |
| Hardening | Sparse index entries were not fully bound to decoded record boundaries: `store/segments/codec.go:87-156`. | Forge an internally consistent index before checksum creation. | **Valid, fixed in manifest version 2.** The manifest authenticates every sparse entry and every 64 KiB data-block digest. Full verification binds every entry to its ordinal and data position. Ranged reads bind the selected entry to the fetched record header before returning bytes. |
| Unrelated | Generated Python bytecode at `loadgen/scripts/__pycache__/summarize-segments.cpython-313.pyc`. | `git status --untracked-files=all` showed the generated file. | **Valid, excluded during teardown.** |
| Unrelated | A pull-wake drain-worker repair appeared at `jepsen/checker/main.go:728-760`, `jepsen/checker/scenario_leasetail.go:170-174`, and `jepsen/checker/main_pullwake_test.go:8-20`. | The worker sent `done=true` without pending cursor acknowledgements. | **Valid but out of scope, excluded.** The helper and its test were removed from this diff. It should land separately with an end-to-end pull-wake regression. |

## Integration validation findings

The first combined Toxiproxy run produced a 3,585-operation counterexample
after a Redis partition. Redis had closed the stream at byte 222. A later read
returned the exact closed suffix, but the checker's separate metadata call
failed. The checker recorded the unknown closed state as `false`, which
fabricated an open read after close. `storeDoRead` now omits a read when that
metadata call fails. Tests cover the unavailable and confirmed-closed cases.
This reduces history density during a partition, but it does not assert a state
the driver did not observe.

The first combined `paged-catchup` run received an EOF before the
Chronicle-restart fault trigger, immediately after the prior canceled response.
The request returned no response headers and triggered no fault, so it produced
no stream observation for the checker to evaluate. The checker now retries only
request-start transport failures. It never retries after response headers
arrive or after a fault starts. The retry window can delay a failed attempt by
up to two seconds. A transport test proves that an initial EOF is retried and
the next complete envelope is checked.

## Exact final validation

All Go commands below used `GOCACHE=/private/tmp/chronicle-go-cache`. The first
lint attempt could not resolve `proxy.golang.org` inside the restricted
sandbox. The authorized rerun completed and reported `0 issues.` No shared
cache was cleaned.

- `env -u REDIS_URL ... make test`: every race-enabled package passed,
  including the root handler package, Redis store, segment store, webhook, and
  Jepsen checker.
- `cd loadgen && ... make test`: every race-enabled load-generator package
  passed. Five command packages had no test files.
- `... make lint`: `0 issues.`
- `... make spec-check`: `OK: conformance suite version consistent (0.3.5)`
  and `OK: upstream PROTOCOL.md unchanged since the pinned commit
  (82f9963ae0b489566352393be9b4796c788c99c2)`.
- Segment conformance: 7/7 cells and 2,324/2,324 assertions passed:
  `off/shadow 332`, `redis-chunks/shadow 332`,
  `redis-chunks/serving 332`, `local-files/shadow 332`,
  `local-files/serving 332`, `object-cache/shadow 332`, and
  `object-cache/serving 332`. Every candidate shadow cell reported
  `reads=0,seals=238`; every serving cell reported `reads=77,seals=238`.
- Direct Porcupine `store-linz`: 4/4 cells passed with one path partition:
  off 3,852 operations, Redis chunks 3,909, local files 2,960, and object cache
  2,043.
- Toxiproxy Porcupine `store-linz`: 4/4 cells passed under randomized Redis
  partitions and latency with one path partition after the checker correction:
  off 2,652 operations, Redis chunks 3,341, local files 3,517, and object cache
  1,645.
- Jepsen baseline delivered 318 wakes and reached all 8 stream tails.
  Chronicle restart delivered 317 wakes and reached all 8 tails. Redis restart
  delivered 310 wakes and reached all 8 tails.
- Jepsen `paged-catchup` passed six interrupted and resumed attempts across a
  136-frame fork, one full Chronicle restart, and one Redis restart.
- Jepsen `sse-resume` completed all 8 clients and all 320 observations. It
  verified 11 fault actions, 2 subscription reconnects, 1 lag disconnect, and
  1 write timeout.

## Post-review integration status

The integration closed the three code blockers found above:

1. `segments.Store` now implements bounded `PageReader` and
   `PageSnapshotReleaser`. The handler selects it directly.
2. The wrapper delegates `NotificationSubscriberProvider` to the Redis primary,
   so the shared SSE hub keeps its persistent Pub/Sub wake path.
3. Manifest version 2 adds generation-bound authenticated range reads,
   per-reader leases, a fixed primary-tail handoff, oversized-frame and
   frame-count handling, and deterministic delete/recreate, TTL, cancellation,
   corruption, and pin-release tests.

Three operational limits remain:

1. GC reclamation is disabled. File and object modes need a durable publication
   staging lease or shared lock. Redis needs an atomic same-slot staging
   protocol. Every mode needs shared cross-replica holds.
2. The object backend is a filesystem emulator. Real object generations,
   conditional range reads, multipart interruption, credentials, quotas, and
   regional failures remain untested.
3. Benchmarks must be rerun from immutable, checksummed raw inputs after the
   integrated correctness gates pass. The current result files are diagnostic
   only.

The Redis ZSET layout remains the default. No performance result can relax the
332-test Durable Streams contract or the maintained Jepsen and Porcupine
correctness gates.
