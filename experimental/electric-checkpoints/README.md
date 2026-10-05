# Electric / Durable Streams checkpoint integration — bundle v5

This is a local, unpushed integration at Electric `bb397424db0e1c153dc356713fd3dfd40315470c` and Durable Streams `461b40267aabd644558f9b19dbb9507dd5f691cf`. The runtime input contract is **bounded-checkpoint-v1**; the canonical image codec is **electric-entity-image/v3**. Bundle v5 is not an image/campaign version: campaign JSON retains schemaVersion 1. Sibling immutable earlier bundles and their pilots/campaigns are pre-fix/pre-format evidence, not final performance. The historical full-raw-replay bundle/results are copied byte-identically into this standalone bundle as `prior-full-replay-bundle-unchanged.tar.gz`; they are superseded experimental evidence, not the recommended recovery path.

## Behavior and supported contract

Legacy/full-replay is still the default. Opted-in handlers receive schema-owned activation inputs after durable completed progress, rather than arbitrary historical raw inputs. Setup, `ctx.events`, and self-observation share this bounded contract. Handlers must consume the entire delivered delta; arbitrary historical observers must use full-replay. Canonical timers, cron, and forks append source facts. Opaque supplied `notification.wakeEvent` has no authenticated source identity and is rejected/noack in bounded mode, even when payload/offset equal an old inbox item. Legacy explicit-wake behavior is unchanged.

**Public export preconditions:** StreamDB.exportState requires explicit coherent capture via onCommittedBatch; EntityStreamDB.utils.exportSnapshot requires onCommittedSnapshot. A no-op callback enables capture when only pull export is wanted. Bootstrap alone does not enable export. Without capture, export rejects rather than pairing queued canonical rows with an advanced cursor; default legacy sync timing/visibility is unchanged. Export also rejects synchronously during onBeforeBatch/onEvent/onBatch, when Entity pointer/position maps may have advanced but canonical rows/cut have not committed. Export is allowed inside onCommittedSnapshot after the complete commit. Normal bounded processWake already enables capture and is unaffected by these API restrictions.

```diagram
Legacy snapshot path
┌───────────────┐     ┌──────────────────┐
│ Hydrate rows  │────→│ Read raw prefix  │────→ handler
└───────────────┘     └──────────────────┘

Bounded checkpoint hit
┌─────────────────┐     ┌─────────────────────────┐
│ Hydrate v3 image│━━━━▶│ One guarded suffix reader│━━━━▶ handler
└─────────────────┘     └─────────────────────────┘
                        no source-prefix GET
```

The runtime appends a checkpoint only after a successful fully drained source prefix. `processedThrough` is the pre-marker committed source cut; `processedSeq` counts known projected events. Neither is a producer append cursor, notification tail, mutable ack cursor, or merely an accepted batch boundary. The checkpoint image must end immediately after its marker. Pending fresh inputs, queued/paused inbox entries, unresolved signals/next wake, or required processing/persistence failures prevent certification. Tracked primary/shared writes wait for flush and authoritative echo; bounded secondary producer buffers flush before certification. The reader remains alive for the checkpoint echo.

The checkpoint certifies **durable handler/tracked-effect completion**, not infallible later session/reader/sandbox/transport teardown. A resource failure before callback attempts release with no ack; an already-durable marker remains, so retry may skip completed work. A callback transport failure can leave the server outcome uncertain. External side effects still require idempotency; this is not exactly-once external execution. In particular, A output after unresolved live B cannot acknowledge B. If A never reached a certified drained prefix, retry may repeat A.

Publication never advances ack. Chronicle's image is a deterministic source-prefix projection; equal cut/different bytes remains an error. v3 canonical pointers use stream-root plus flattened all-item position, including control/unknown items, separately from projection sequence. Inclusive fork semantics are preserved; both TS reference memory/file stores were corrected for flattened JSON counts across append entries. Root-based late forks can scan more history. Order tokens use 16 decimal digits, monotonic through the nonnegative safe-integer domain. Codec v3 rejects old-width v2 images; rebuild compatible images under a fresh projectionVersion, not by mixing indexes with new suffix tokens. Provisional applyEvent ordering never enters canonical indexes, and source echoes establish first canonical insertion order even while optimistic writes persist.

**Pre-release progress migration:** changing image codec or projectionVersion does not invalidate a bounded-checkpoint-v1 source marker. Earlier buggy WIP builds could certify unseen input. Discard their disposable bounded streams. Real-data users would need an application-level source-progress audit/rebuild; image rotation alone cannot repair incorrectly certified progress. A reader update likewise cannot repair a valid-digest incomplete image.

**Trust prerequisite:** direct Chronicle raw writers are trusted infrastructure. Untrusted inbox/user writes must use Electric's existing claim/write-token write boundary. Chronicle does not classify runtime event types. Bounded claims require a write token; nested inbox payloads cannot become a top-level progress marker. Dedicated snapshot publisher authority is separate from append/browser authority.

## Verification scope

`checks/targeted-199.log`: 199/199 targeted runtime tests, including all seven original adversarial findings, stamped/unstamped tail cuts, deterministic grouping, legacy partial merge, inclusive pointer/timeline behavior, WakeSession behavior, disposal failure/noack/skip-completed-work, tracked applyEvent echo bytes, pending optimistic export, B-before-A canonical insertion, safe-integer ordering, old-codec rejection, export opt-in and synchronous atomic-export guards. Runtime typecheck passes. `checks/sdk-client-state-110.log`: 110/110 (client49, state61), all SDK declaration builds and state typecheck pass; canonical export is verified inside a still-persisting mutationFn without losing its distinct optimistic overlay, disabled export rejects, and unrelated B while A persists is restored/replayed exactly. `checks/real-e2e-10.log`: 10/10 real Chronicle/Electric/Postgres integration cases: deletes and trust, bounded/self-observation input, control-inclusive restored fork, pending signal/live cancellation, concurrent inputs, A-sleep/B-pending/noack, claim denial/delay, source412/release/noack/retry, handler retry, saved checkpoint then actual done503/redelivery suppression. Fork conformance116 also passes; the explicit fork filter skips562 unrelated cases.

Scoped Prettier checks and Electric ESLint pass after removing the new outer-try prettier-ignore and formatting its checkpoint/publication blocks. Durable ESLint exits0 with one pre-existing response.ts no-shadow warning, no errors. The optional broader client package typecheck still fails: a separately installed/built untouched pinned worktree produces exactly the same113 diagnostic identities/counts. Both failing logs are retained in diagnostics; no new diagnostic was observed. This is not an all-package typecheck-green claim, and unrelated baseline issues were not changed.

Positive integration paths and campaigns use real HTTP, patched DurableStream/IdempotentProducer/StreamDB, embedded Electric agents-server, real Electric sync service/Postgres, Chronicle, and Redis. **The two claim fault cases use a fetch forwarding/stall shim at Electric's actual locally answered claim boundary. Denial is injected HTTP401, not a backend claim decision.** All nonfault traffic in those cases is real HTTP. Other fault cases inject failures through transparent loopback gates. Unit/adversarial suites also contain mocks; there is no blanket “no fetch/claim mocks anywhere” claim. Original failing repros, earlier failed E2E/metrics runs, and superseded pilot evidence are retained under diagnostics and the immutable review captures.

## Measurement methodology

Both compared paths use the **same explicit bounded-checkpoint-v1 input contract**: `replay` versus `snapshot-checkpointed-inputs`. No legacy `ctx.events` equivalence is claimed. Campaign schemaVersion 1 retains shared metrics, `labels.rawInputRecovery`, `recovery.sourceReads=[{offset,nextOffset,incarnation}]`, checkpoint incarnation/cut, and `assertions.noPrefixReads`. Replay/fallback has noPrefixReads=null; actual hits require a concrete first GET at image cut, no omitted/-1/now/earlier offset, and matching incarnation guards. Admission-frozen source-gate records allow independent recomputation, rather than relying on the explicit-offset=-1 traffic classifier.

- Default campaign: 500 state updates over at most 16 keys; 300 append-only historical rows; 100 new 8KiB inbox payloads. Scaled campaign: 10000/3000/100. Each campaign has 30 measured samples/path/scenario, both orders, plus one warmup per process run. Each scenario/mode/order gets a fresh Node/Vitest/runtime/agents-server process; each webhook gets a fresh in-process EntityStreamDB. This measures activation recovery, not process cold startup.
- State-workload seed events are concatenated into **one batched source append** (500/10000 updates or 300/3000 inserts). Inbox payloads are appended sequentially through Electric while notification delivery is held, then released together. This avoids racing short handler passes against workload construction; it is not an organic production arrival distribution.
- Runtime activation timing starts at **wake received after claimHeaders resolution**, not literal function entry, and ends after cleanup and successful done callback. It includes snapshot lookup, recovery, handler writes, and ack; excludes trigger append and semantic validation.
- Source-gate capture starts **before entity.send()/held-webhook release**, so it is not an identical timing window. State-workload traffic includes the triggering append. Traffic totals count admitted source-gate body bytes once, not headers/TLS, and exclude callback-gate claim/done traffic, Electric/Postgres sync traffic, and direct validation reads. Captures freeze admitted requests and counters at metrics; later bytes/requests do not mutate captured totals.
- Snapshot publication duration is gate-observed HTTP only, excluding preceding export/serialization/hash work. Those preparations can still affect activation/Node resources. Publication may complete outside activation timing; post-timing gate observation reports it explicitly. Snapshot maxImageBytes remains 1MiB.
- Snapshot publication eligibility gates are intentionally all zero in **both campaigns** (minReplayEvents, minReplayMs, minPublishIntervalMs, missBackoffMs). This exercises publication overhead, not the default 25ms/100-event/60s eligibility policy. Not a production throughput or universal speedup claim.
- CPU/heap/RSS are **combined runtime handler, embedded agents-server, test and source-gate instrumentation Node process**, not runtime-only. CPU is process.cpuUsage; memory samples every 2ms plus endpoints. Go Chronicle, Redis, Postgres and Electric process resources are not included. Every sampler exit is cleaned up, including denied claims and snapshot lookup errors.
- Semantic validation is after capture/timing: exact persisted replies/multiplicity and activation count; handler-observed pre-mutation hydrated main/work rows and append seq values; exact raw input sequence; persisted state; ack; safe Redis HMGET of phase/holder/lease_until_ns only. Complete observed intermediate seeded state history and inbox insert sequence must equal an independently constructed fixture. sourceHash hashes that observed normalized history, excluding nondeterministic envelope fields/offsets/timestamps, not exact wire bytes.
- Full subprocess stdout/stderr is retained for every run, including failures. Fault/redelivery expectations are separate from deterministic campaign multiplicity. There is no inferred exactly-once external-effects guarantee.

## Safe environment metadata

Orb Linux x86_64, 4 logical CPUs (2 cores × 2 threads), Intel Xeon Processor @ 2.60GHz; MemTotal 8147616KiB (about 7.77GiB). Shared CPU/load and timing noise are not controlled beyond sequential campaigns. Node v26.10.0, pnpm10.12.1; Chronicle compiled with Go1.26.2 (binary build information); Redis7.4.11, jemalloc5.3.0; PostgreSQL18.6; Electric sync service release1.8.1, source-built at the pinned Electric revision. Isolated disposable Compose project electric-snapshot-processwake. Electric image config/repo digest sha256:d461daaa4ed138126812e228ed8eecc3443cd9331e48cd0b8a123f40cfb8dbc6. Node differs from repo's Node24 pin and is explicitly recorded, not silently normalized. Services are supervised checkpoint-redis/callback/chronicle; credentials stay in private files outside this bundle.

## Transfer, provenance and reproduction

`electric-checkpoint-v5.patch` and `durable-checkpoint-v5.patch` are cumulative at the pins above, including tests. `*-modified-sources.tar.gz` contains each changed/new source file at its repo-relative path; `*-MODIFIED-FILES.txt` inventories them. Apply patches to pinned checkouts, install both roots, build/link the patched SDKs, then run commands below. No push, PR, deployment, or published-history rewrite was performed.

**Hash distinction:** campaign `electricPatchSha256` / `durablePatchSha256` are working-tree fingerprints: git diff HEAD bytes plus sorted untracked file names/contents with delimiters. They are NOT SHA256 of exported patch bytes. Final SHA256SUMS records actual exported file bytes. Campaign before/after fingerprints and drift must match; actual patch hashes and separate source archives establish the transferable revision. Old baseline hashes/results are retained unchanged, not mixed into new statistics.

```sh
git -C electric checkout bb397424db0e1c153dc356713fd3dfd40315470c
git -C electric apply --index /bundle/electric-checkpoint-v5.patch
git -C durable-streams checkout 461b40267aabd644558f9b19dbb9507dd5f691cf
git -C durable-streams apply --index /bundle/durable-checkpoint-v5.patch
# Install BOTH roots before package checks. In this Node26 orb:
pnpm --dir electric install --offline --ignore-scripts --config.engine-strict=false
pnpm --dir durable-streams install --offline --ignore-scripts --config.engine-strict=false
# Build client/server, share Electric's TanStack DB copy with Durable state,
# build state, then link the patched SDKs into Electric runtime/server.
pnpm --dir durable-streams --filter @durable-streams/client build
pnpm --dir durable-streams --filter @durable-streams/server build
rm durable-streams/packages/state/node_modules/@tanstack/db
ln -s "$(realpath electric/packages/agents-runtime/node_modules/@tanstack/db)" durable-streams/packages/state/node_modules/@tanstack/db
pnpm --dir durable-streams --filter @durable-streams/state build
for p in electric/packages/agents-runtime electric/packages/agents-server; do
  rm "$p/node_modules/@durable-streams/client" "$p/node_modules/@durable-streams/state"
  ln -s "$(realpath durable-streams/packages/client)" "$p/node_modules/@durable-streams/client"
  ln -s "$(realpath durable-streams/packages/state)" "$p/node_modules/@durable-streams/state"
done
rm electric/packages/agents-runtime/node_modules/@durable-streams/server
ln -s "$(realpath durable-streams/packages/server)" electric/packages/agents-runtime/node_modules/@durable-streams/server
pnpm --dir electric/packages/agents-runtime run typecheck
```

Fixture environment (disposable local services, not production): DATABASE_URL=postgres://electric_agents:electric_agents@127.0.0.1:55432/electric_agents; ELECTRIC_URL=http://127.0.0.1:53060; ELECTRIC_AGENTS_TEST_BACKEND_MANAGED=0; CHRONICLE_STREAM_ROOT=http://127.0.0.1:18438/v1/stream/; CHRONICLE_CALLBACK_GATE_URL=http://127.0.0.1:18439; CHRONICLE_REDIS_URL=redis://127.0.0.1:6388; CHRONICLE_TOKEN_DIR points to private .writer-token/.publisher-token. Configure Chronicle's advertised origin to the supervised callback gate; actual signed callback URLs end in /callback, not /ack. Entity writes still use Electric-mediated authority. Never export token contents or Redis secret fields.

```sh
# Run from Electric root with the fixture environment above.
pnpm --dir packages/agents-runtime exec vitest run test/review-atomic-export.test.ts test/review-bounded-additional.test.ts test/review-bounded-faults.test.ts test/review-bounded-repro.test.ts test/review-bounded-tail-cut.test.ts test/runtime-checkpoint.test.ts test/entity-stream-db-snapshot.test.ts test/process-wake.test.ts test/entity-stream-db-principal.test.ts test/event-pointer.test.ts test/entity-timeline.test.ts test/timeline-context.test.ts test/wake-session.test.ts
pnpm --dir packages/agents-runtime exec vitest run test/chronicle-checkpoint-e2e.test.ts
# Also set ELECTRIC_CHECKOUT, DURABLE_STREAMS_CHECKOUT, CHRONICLE_BINARY,
# CHRONICLE_BENCH_OUTPUT to absolute paths. The driver records each run command.
CHRONICLE_BENCH_CONTRACT=bounded-checkpoint-v1 CHRONICLE_BENCH_SAMPLES_PER_PHASE=15 CHRONICLE_BENCH_WARMUPS=1 node packages/agents-runtime/test/run-chronicle-process-wake-benchmark.mjs
# Scaled campaign, separate output path:
CHRONICLE_BENCH_CONTRACT=bounded-checkpoint-v1 CHRONICLE_BENCH_SAMPLES_PER_PHASE=15 CHRONICLE_BENCH_WARMUPS=1 CHRONICLE_BENCH_UPDATE_EVENTS=10000 CHRONICLE_BENCH_APPEND_EVENTS=3000 CHRONICLE_BENCH_INBOX_EVENTS=100 node packages/agents-runtime/test/run-chronicle-process-wake-benchmark.mjs
```

## Completed frozen campaigns

The corrected v5 pilot completed with 24 total / 12 measured samples. Both full campaigns completed with 192 total / 180 measured samples each: 30 complete measured replay/checkpoint pairs per scenario, both phase orders, zero failures, unpaired successes, fallback hits, or source drift. Across the two full campaigns, all 180 measured checkpoint hits pass all-source-GET offset/incarnation/no-prefix checks and aligned contract/cut/progress checks. The parent independently downloaded and accepted the exact pilot and both full campaign bytes with its reporter. This validates the retained measurements and recovery boundary, not a general performance claim.

All table values use **nearest-rank percentiles**: sort the 30 measured samples for each path/scenario/campaign and select index ceil(q × 30) − 1, with no interpolation. Activation latency includes the ranges documented above. `descriptive-summary.json` also records preload, combined Node CPU, body traffic, heap and RSS statistics; raw samples and all subprocess logs remain unchanged.

| Campaign | Scenario                 | Replay p50 / p95 (ms) | Checkpoint p50 / p95 (ms) |
| -------- | ------------------------ | --------------------: | ------------------------: |
| Default  | 500 updates / ≤16 keys   |         60.08 / 70.09 |             63.55 / 86.42 |
| Default  | 300 append-only rows     |         67.70 / 77.53 |            78.97 / 102.60 |
| Default  | 100 × 8KiB inbox inputs  |         75.59 / 99.49 |            91.36 / 106.09 |
| Scaled   | 10000 updates / ≤16 keys |       107.25 / 136.66 |             60.18 / 72.42 |
| Scaled   | 3000 append-only rows    |       155.12 / 179.13 |           184.13 / 236.33 |
| Scaled   | 100 × 8KiB inbox inputs  |         77.08 / 95.50 |            93.21 / 106.17 |

For 10000 overwritten updates, captured total-body p50 fell from 1,610,749 to 18,475 bytes and combined Node CPU p50 from 89.061 to 48.387ms. This is the demonstrated favorable workload: compact final state replacing a large update history. Default activation latency is slower in all scenarios; append-only and large-inbox checkpoint paths are slower and transfer more total body bytes even in the scaled campaign. Publication gates were deliberately zero, and the inbox workload is unchanged between campaigns. These results support workload-dependent gains, not universal speedup, constant-cost recovery, or production throughput predictions.

Retained exact raw-file SHA256:

- `pilot-results.json`: `1693e6d2638783e50d35e44b8a0cce9a965bf8b8dbb985bb10046462b19210ea`
- `default-results.json`: `ae1b0b2f63f0549fbe2e084168e00ba0b01c2130599a831a1debf5264ae2fc81`
- `scaled-results.json`: `e68855b2b2aa3488b95129113380585c75b5d668ffcee84a21a7537848a0e8b9`

Independent review closed the seven original adversarial findings and the additional applyEvent determinism defect. The exact-v5 narrow export-guard follow-up passed its real-client matrix (4/4), Entity hook/export checks (3 passed, 2 unrelated skipped), relevant typechecks and builds. That follow-up covers the two public export guards only, not a new broad sign-off. Reviewer-owned evidence remains separately retained by the parent; the adopted regressions and our full execution logs are in this bundle.

`BUNDLE-INVENTORY.txt` lists the delivered payload files and their sizes; it excludes itself and `SHA256SUMS` to avoid recursive metadata. Verify `SHA256SUMS` after extraction with `sha256sum -c SHA256SUMS`; it covers all other bundle files, including the inventory and unchanged historical archive. No sources changed during or after these campaigns. Delivery is a standalone local artifact, not a commit, push, merge or deployment.
