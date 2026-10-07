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

## Current evidence and remaining work

- checks/targeted-199.log: 199/199, including all additional determinism, width and export API regressions.
- checks/sdk-client-state-110.log: 110/110 (client49, state61); all SDK declaration builds and state typecheck pass.
- checks/runtime-typecheck.log: passes.
- checks/real-e2e-10.log: 10/10 real Chronicle/Electric/Postgres, including trust, control-inclusive fork, pending signal/live cancellation, concurrent inputs, pending-B noack, claim faults, source412, handler failure, saved checkpoint then done503/retry.
- checks/fork-conformance-116.log: 116 memory/file fork conformance cases pass (562 unrelated cases skipped by explicit fork filter).
- Scoped formatting checks pass. Electric ESLint passes; Durable ESLint exits0 with one existing no-shadow warning in response.ts (verified unchanged at the pin), no errors. Client package typecheck fails with exactly the same113 diagnostic identities/counts as a separately installed, built pinned worktree; no new diagnostic. Both full failing logs are retained under diagnostics; this is not an all-package typecheck-green claim.
- No source edits after these checks. Corrected-source pilot/default/scaled campaigns are next, not claimed green yet.

## Contract and verification boundary

bounded-checkpoint-v1 is explicitly opt-in; legacy/full-replay remains default. One guarded state/onBatch reader collects bounded suffix. Bootstrap rejection resets raw collector before guarded full replay. Only successful fully drained source prefixes can certify; publication never advances ack. Direct raw writers are trusted infrastructure; untrusted inputs require Electric write-token mediation. Chronicle does not classify event types. Unsupported explicit wakeEvent has no source identity and is rejected, without adding a new identity protocol.

Tracked primary/shared effects settle through flush plus txid echo; raw secondary buffers flush before certification. Marker means durable handler/effect completion, not infallible DB/sandbox/transport teardown. Resource failure before callback attempts noack release; lost callback reply can leave server outcome uncertain. External side effects require application idempotency.

Positive paths and campaigns use real HTTP. Two claim fault tests use forwarding/stall fetch shims at Electric's actual claim boundary; HTTP401 is injected, not a backend claim decision. All nonfault traffic remains real. Unit suites contain mocks. See README for commands, environment, source-gate/timing/publication ranges, scope, and historical evidence qualifications.

Campaign retains schemaVersion1; modes replay/snapshot-checkpointed-inputs, labels.rawInputRecovery=bounded-checkpoint-v1, recovery.sourceReads/incarnation/contract/stateCut/checkpointSourceCut/processedThrough/processedSeq, assertions.noPrefixReads. Both paths share the explicit bounded input contract, not legacy ctx.events equivalence. Observed complete intermediate source history is validated/hashed after timing. Process resource labels match the instrumentation scope.
