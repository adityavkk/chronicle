# Corrected source freeze: bundle v5 (image codec v3)

These cumulative Electric/Durable patches target Electric bb397424db0e1c153dc356713fd3dfd40315470c and Durable Streams 461b40267aabd644558f9b19dbb9507dd5f691cf. Sources are local/unpushed; tests and harness are included. Earlier bundles v2/v3 remain immutable pre-fix evidence, including v2 campaigns and both pilots; none is final-source performance evidence. v3 pilot was independently accepted by the parent reporter, validating the harness rather than the later-fixed public export API.

## Additional findings and fixes

- applyEvent formerly seeded provisional timeline ordering in the canonical Map. It now decorates only the optimistic row; the first source echo establishes canonical value AND insertion position, and later updates preserve first insertion. `review-bounded-additional.test.ts` checks tracked insert/update same-cut bytes and B-before-A canonical Map order.
- While an optimistic transaction persists, committed capture must reflect the authoritative source batch without the optimistic overlay. SDK collection sync uses TanStack's public immediate transaction mode only when onCommittedBatch is opted in; default legacy timing remains unchanged. SDK's strengthened echo test asserts canonical export inside the still-persisting mutationFn, while the visible optimistic row remains distinct. Runtime test checks unchanged export while A is pending, then canonical B alone, then exact replay bytes after A's echo.
- Cumulative positions cross the former eight-digit boundary. Order tokens now use 16 digits, covering Number.MAX_SAFE_INTEGER. Tests assert both argument orders at 99,999,999/100,000,000 and MAX_SAFE_INTEGER-1/MAX_SAFE_INTEGER, plus invalid-domain rejection. Codec is electric-entity-image/v3; decoder and direct EntityStreamDB bootstrap reject v2. Rebuild old experimental images with fresh projectionVersion: never mix old-width indexes and new suffix tokens.
- **Image migration does not erase durable source progress.** Discard disposable streams written by buggy pre-release bounded builds. Real-data users need an application-level progress audit/rebuild; rotating image codec/projectionVersion alone cannot repair falsely certified source markers.
- SDK exportState now rejects unless coherent capture was explicitly enabled by onCommittedBatch. Entity utils.exportSnapshot likewise requires onCommittedSnapshot. Bootstrap alone does not enable export. Default legacy sync visibility remains unchanged. Adopted `review-coherent-export.test.ts` covers the actual txid-echo and unrelated-B/held-A repros with enabled/disabled branches, exact restored rows and replay bytes.
- Synchronous in-batch export rejects until canonical rows and cursor commit, preventing Entity's already-advanced pointers/positions from mixing with the prior SDK image. Export inside onCommittedSnapshot is allowed. A central SDK batchInProgress precondition protects both APIs. The adopted real-server `review-atomic-export.test.ts` and two-cut mock test cover rejection and exact complete image after B. The mock guard test was run red against the pre-guard built SDK before rebuild; log retained.
- Ultra ruled out the separate suspected canonical row-array order issue against independent replay, including bounded processWake. No unrelated ordering redesign was made.
- Scoped upstream ESLint/Prettier checks ran before final hashes. Removed the implementation's prettier-ignore on the new outer cleanup try so its nested checkpoint/publication blocks are correctly indented; this accounts for the large formatting-only delta. The projection-name ASCII control check now inspects its already-encoded UTF-8 bytes rather than a lint-prohibited control regexp, preserving validation semantics; boundary tests cover control/space and 128-byte multibyte names.

## Seven original findings → current coverage

1. Output beyond pending B: review-bounded-repro plus real A-sleep/B test; ack only from a drained aligned image, never producer append position.
2. Explicit wake redelivery ambiguity: opaque hints rejected/noack in bounded mode; canonical completed source delivery skips, distinct new source work runs. Legacy hints unchanged.
3. Export failure skips cleanup: review-bounded-faults; catch export/write/encode errors, noack, continue disposal/release. Durable handler/effect completion is separate from teardown-only failure; additional disposal regression retains marker then skips completed work on retry.
4. Batch-dependent image bytes/pointers: review-bounded-faults and real control-inclusive restored Chronicle fork; canonical all-item root/count addressing, separate from known projection sequence. TS memory/file fork counts corrected across physical appends.
5. Marker self-wake: review-bounded-faults; actual WakeRegistry excludes runtime_checkpoint from on:change, as does runtime input dispatch.
6. Legacy partial-update data loss: review-bounded-faults and SDK export/echo tests; preserve merge semantics, no global rowUpdateMode:full override.
7. Stamped pre-tail input omitted: stamped/unstamped review-bounded-tail-cut; bounded delta follows durable progress, not notification tail/header stamp.

## Final evidence and delivery

- checks/targeted-199.log: 199/199, including all additional determinism, width and export API regressions.
- checks/sdk-client-state-110.log: 110/110 (client49, state61); all SDK declaration builds and state typecheck pass.
- checks/runtime-typecheck.log: passes.
- checks/real-e2e-10.log: 10/10 real Chronicle/Electric/Postgres, including trust, control-inclusive fork, pending signal/live cancellation, concurrent inputs, pending-B noack, claim faults, source412, handler failure, saved checkpoint then done503/retry.
- checks/fork-conformance-116.log: 116 memory/file fork conformance cases pass (562 unrelated cases skipped by explicit fork filter).
- Scoped formatting checks pass. Electric ESLint passes; Durable ESLint exits0 with one existing no-shadow warning in response.ts (verified unchanged at the pin), no errors. Client package typecheck fails with exactly the same113 diagnostic identities/counts as a separately installed, built pinned worktree; no new diagnostic. Both full failing logs are retained under diagnostics; this is not an all-package typecheck-green claim.
- No source edits after these checks or during/after campaigns. Pilot24/12 measured and both full campaigns192/180 measured completed. Across full campaigns: 360 measured samples, 30 complete measured pairs per scenario/campaign, both orders, zero failures/unpaired/fallback/drift, all180 checkpoint hits independently verified for guarded no-prefix GETs and aligned contract/cuts/progress. The parent independently accepted exact raw bytes for all three campaigns.
- Nearest-rank activation p50/p95 (ms), replay → checkpoint: default updates60.08/70.09 →63.55/86.42, append67.70/77.53 →78.97/102.60, inbox75.59/99.49 →91.36/106.09; scaled updates107.25/136.66 →60.18/72.42, append155.12/179.13 →184.13/236.33, inbox77.08/95.50 →93.21/106.17. Scaled update total-body p50:1,610,749 →18,475 bytes; combined Node CPU p50:89.061 →48.387ms. Workload-dependent gains only: default latencies and scaled append/inbox are slower. README documents percentile method and measurement limits; descriptive-summary.json contains remaining metrics. Raw results/logs are unchanged.
- Ultra's exact-v5 narrow guard follow-up passed SDK real-client4/4, Entity hook/export3 passed (2 unrelated skipped), relevant typechecks/builds. It verified opt-in/bootstrap-only rejection, unchanged legacy visibility, canonical hydration/replay bytes, in-flight rejection and committed-callback export. This is scoped guard evidence, not a new broad review sign-off.
- SHA256SUMS covers all other bundle files, including BUNDLE-INVENTORY.txt and the byte-identical prior-full-replay-bundle-unchanged.tar.gz. Verify it after extracting the standalone electric-checkpoint-final-v5.tar.gz. Local sources remain uncommitted/unpushed; archive delivery does not imply merge or deployment.

Exact cumulative patch byte hashes:

- electric-checkpoint-v5.patch: afaf50a0b9ca782f0e9ba0dd73580aee1e5f2b52337e19e9c6977fead8b5657d
- durable-checkpoint-v5.patch: 194d6aad5181defc95c1d6ae229a7532f09838012ddbabd48a57221bd3c7d721

Exact final raw campaign hashes:

- pilot-results.json: 1693e6d2638783e50d35e44b8a0cce9a965bf8b8dbb985bb10046462b19210ea
- default-results.json: ae1b0b2f63f0549fbe2e084168e00ba0b01c2130599a831a1debf5264ae2fc81
- scaled-results.json: e68855b2b2aa3488b95129113380585c75b5d668ffcee84a21a7537848a0e8b9

## Contract and verification boundary

bounded-checkpoint-v1 is explicitly opt-in; legacy/full-replay remains default. One guarded state/onBatch reader collects bounded suffix. Bootstrap rejection resets raw collector before guarded full replay. Only successful fully drained source prefixes can certify; publication never advances ack. Direct raw writers are trusted infrastructure; untrusted inputs require Electric write-token mediation. Chronicle does not classify event types. Unsupported explicit wakeEvent has no source identity and is rejected, without adding a new identity protocol.

Tracked primary/shared effects settle through flush plus txid echo; raw secondary buffers flush before certification. Marker means durable handler/effect completion, not infallible DB/sandbox/transport teardown. Resource failure before callback attempts noack release; lost callback reply can leave server outcome uncertain. External side effects require application idempotency.

Positive paths and campaigns use real HTTP. Two claim fault tests use forwarding/stall fetch shims at Electric's actual claim boundary; HTTP401 is injected, not a backend claim decision. All nonfault traffic remains real. Unit suites contain mocks. See README for commands, environment, source-gate/timing/publication ranges, scope, and historical evidence qualifications.

Campaign retains schemaVersion1; modes replay/snapshot-checkpointed-inputs, labels.rawInputRecovery=bounded-checkpoint-v1, recovery.sourceReads/incarnation/contract/stateCut/checkpointSourceCut/processedThrough/processedSeq, assertions.noPrefixReads. Both paths share the explicit bounded input contract, not legacy ctx.events equivalence. Observed complete intermediate source history is validated/hashed after timing. Process resource labels match the instrumentation scope.
