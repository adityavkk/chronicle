# Immutable segment fault matrix

`store/segments` keeps the Redis frame ZSET as the complete primary copy. Each fault must either leave the prior manifest current or publish a complete, checksum-valid new generation.

The one-shot file fault injector reaches the first matching operation in each test. It does not claim exhaustive command-by-command coverage of every later data file, index file, manifest file, or Redis command. The object mode below is a filesystem emulator, not a real object service.

## Deterministic publication faults

| Fault point | Redis strings | Local files | Object cache | Required result | Automated evidence |
|---|---|---|---|---|---|
| Object creation | Not applicable to Redis strings | Injected before temporary file creation | Injected before temporary origin file creation | The current token does not change | `TestInterruptedSealNeverAdvancesVisibleGeneration` |
| Data or index write | Redis command error is covered by backend integration | Injected before a staged write | Injected before a staged origin write | The current token does not change | `TestInterruptedSealNeverAdvancesVisibleGeneration` |
| Data or index sync | Redis durability follows Redis configuration | Injected before `fsync` | Injected before origin-emulator `fsync` | The current token does not change | `TestInterruptedSealNeverAdvancesVisibleGeneration` |
| Rename | Not applicable | Injected before immutable rename | Injected before origin-emulator rename | The current token does not change | `TestInterruptedSealNeverAdvancesVisibleGeneration` |
| Upload | Not applicable | Not applicable | Injected before origin write | The current token does not change | `TestInterruptedSealNeverAdvancesVisibleGeneration` |
| Checksum | Injected before put or read | Injected before put or read | Injected before put or read | Chronicle returns primary bytes or an error. It never returns corrupt bytes | `TestChecksumFailureFallsBackWithoutCorruptBytes` |
| Manifest publication | Injected before manifest write | Injected before manifest write | Injected before manifest write | The prior generation remains current | `TestInterruptedSealNeverAdvancesVisibleGeneration` |
| Cache write | Not applicable | Not applicable | Injected before cache population | Chronicle reads the primary copy. A partial cache file is not used | `TestObjectCacheFaultFallsBackToPrimary` |
| Migration | Shared manager fault | Shared manager fault | Shared manager fault | The manifest state does not change | `TestMigrationRollbackCutoverAndFaults` |
| Cutover | Shared manager fault | Shared manager fault | Shared manager fault | The serving generation remains current | `TestMigrationRollbackCutoverAndFaults` |
| Rollback | Shared manager fault | Shared manager fault | Shared manager fault | The serving generation remains current | `TestMigrationRollbackCutoverAndFaults` |
| Garbage collection | Backend fault and staged-publisher barrier | Backend fault and staged-publisher barrier | Backend fault and staged-publisher barrier | Audit classification only. No manifest or object is removed | `TestMigrationRollbackCutoverAndFaults`, `TestFileAndObjectGCKeepUnpublishedObjectsRacingWithPublish`, `TestRedisGCKeepsUnpublishedObjectsRacingWithPublish` |

## Runtime and concurrency faults

| Fault | Safety rule | Current automated evidence | Remaining environment gate |
|---|---|---|---|
| Chronicle restart | `CURRENT` references only durable objects | `TestRestartRecoveryAndConservativeGC` | Restart the server process during the conformance suite |
| Redis restart | A serving reader validates Redis before it returns a segment | Existing Redis restart Jepsen scenario and mode-specific conformance | Run the maintained Jepsen Redis restart scenario with each server mode |
| Upload interruption | An unreachable object can remain, but `CURRENT` does not change | `TestInterruptedSealNeverAdvancesVisibleGeneration/object-cache/upload` | Repeat with the selected object service and request cancellation |
| Redis network partition | An acknowledged append follows the existing indeterminate Redis rules. A segment is never a second append authority | `store-linz` passed for `off`, `redis-chunks`, `local-files`, and `object-cache` with randomized Toxiproxy partition and latency windows | Retain histories and repeat in CI or a release runner |
| Stale cache | A checksum failure removes the cache pair and fetches the immutable origin | `TestObjectCacheRejectsStaleBytesAndRefetches` | Repeat with an HTTP cache and object generation preconditions |
| Origin checksum failure | Chronicle returns primary bytes or an error | `TestChecksumFailureFallsBackWithoutCorruptBytes` | Corrupt one emulated origin object during the local matrix |
| Shadow verification and repair | An unreadable generation cannot become serving. Repair writes generation-qualified replacement objects from a TTL-neutral primary snapshot | `TestShadowPromotionRejectsMissingOrCorruptObjects` covers local files and the object emulator; `TestRedisShadowRepairRebuildsCorruptGeneration` covers Redis | Repeat against a real object service |
| Internal TTL touch | Sealing, close-triggered sealing, migration verification, repair, and continuation pages do not extend sliding TTL | `TestInternalSealingDoesNotRefreshSlidingTTL`, `TestSegmentReadPageContinuationDoesNotRenewSlidingTTL`, and `TestReadPageDifferentialContinuationDoesNotRenewSlidingTTL` | Repeat through a restarted Chronicle process |
| Concurrent append and seal | A manifest covers one atomic Redis read snapshot. A later append remains in the Redis hot tail | `TestConcurrentAppendSealAndManifestPublishRace`; direct and Toxiproxy `store-linz` passed in every mode | Repeat across two Chronicle processes |
| Concurrent sealers | Only one expected current token can publish | `TestConcurrentAppendSealAndManifestPublishRace` uses two backend instances and a shared path | Repeat across two Chronicle processes on the selected shared storage |
| Concurrent fork | Each fork captures one primary-store source boundary, then materializes exactly that prefix | `TestConcurrentForkAndSealMatchesPrimary` compares every fork with `MemoryStore` during source appends and seals | Repeat across two Chronicle processes |
| Fork source deletion | A fork manifest contains a self-contained logical prefix | `TestForkSurvivesSourceSoftDelete` | Run fork conformance in every mode |
| Snapshot and resume | A captured read result is immutable. Resuming from its last offset returns the exact suffix | `TestSealedSnapshotMatchesPrimaryAndResumeStaysExact`, `TestReadPageBoundsFixedSnapshotAndAutomaticPinRelease` | Repeat with two Chronicle processes before production cutover |
| Active snapshot during GC | A per-reader lease pins one manifest generation until the final page, an error, cancellation, or explicit handler release | `TestReadPageBoundsFixedSnapshotAndAutomaticPinRelease`, `TestReadPageCancellationReturnsPromptlyAndReleasesPin`, `TestActiveSnapshotPinProtectsManifestFromGC` | Add shared cross-replica holds before enabling deletion |
| Delete and recreate | One response cannot join an old manifest prefix to a recreated stream tail, even with the same clock and content type | `TestReadPageDeleteRecreateDuringContinuationFailsClosed`, `TestReadRejectsDeleteRecreateBetweenManifestAndTail`, `TestRedisReadRejectsDeleteRecreateBetweenManifestAndTail`, `TestSealReplacesPriorIncarnationFromMixedVersionNode`, `TestTTLClosureProducerAndDeleteRecreateParity` | Repeat through two Chronicle processes |
| Cache eviction | Eviction removes disposable cache files only | `TestObjectCacheEnforcesByteBoundAndRefetchesEvictedData` | Run a cache smaller than one working set and report hit rate |
| Garbage collection race | Collection is fail closed until publication staging is distinguishable from an orphan | Deterministic file, object-emulator, and Redis tests pause generation 2 after `Put`, run `GC`, publish, and read every referenced object | Implement a real staging barrier before enabling deletion |

## Reclamation status

`GCResult` reports kept and deferred counts. Deleted counts must stay zero. This is a tested safety posture, not a finished garbage collector. The branch makes no claim that immutable generations can be reclaimed safely.

Real file and object reclamation needs a durable staging lease or a lock that spans every `Put` through `Publish`. Redis needs an atomic same-slot staging protocol. All modes need shared snapshot, fork, rollback, and repair holds. Those are release blockers.

## Rejection rules

Reject a candidate if any test observes:

- Loss of an append that the primary store acknowledged.
- A duplicate append for one accepted producer tuple.
- A byte or message offset gap.
- Reordered or changed payload bytes.
- A segment that becomes visible before its data and index are durable.
- Bytes from a deleted stream incarnation.
- A fork that loses inherited bytes after source deletion.
- Garbage collection of an object referenced by the current or retained rollback manifests.
- Acceptance of a stale producer or subscription fence.

Performance does not override these rules.
