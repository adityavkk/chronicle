# Chronicle ds-bench harness

## Summary

Add a revision pinned adapter under `benchmarks/ds-bench/`. The adapter will run Chronicle through Electric's unmodified Rust load generator and result formats. It will add Chronicle deployment support to a temporary upstream checkout, enforce one 4 vCPU and 16 GiB SUT budget, run the blog workloads and current maintained suites, archive provenance, and remove each paid cluster before starting the next one.

## Implementation status

All six slices are complete. The 24-suite baseline campaign finished on July 28,
2026 UTC. Each suite removed its GKE cluster before the next suite started. The
archive contains 2,584 raw evidence files with tree SHA-256
`ad3104c8a0a10ca8b7e81b88e636517181ad33f82ecf3b56c9bf6977d1a1c917`.

A corrected four-suite supplement now replaces every system's full catchup
slice. It reuses the exact five server image digests from the sealed baseline
and changes only the ds-bench client image. The supplement contains 56 valid
cells, including nine overload observations, and no invalid or missing rows. Its
481 evidence files have tree SHA-256
`c0860ab25e172de648dde9280ea749d5343a142d1e6d3298fcd8a8cbf4c569e8`.

The combined report contains 217 cells, 73 overload observations, and no invalid
or missing rows. All four catchup slices come from the supplement. The other 161
cells come from the baseline. Both evidence seals verify, and an independent GKE
list returned no owned campaign clusters.

The adapter tests and every upstream test pass. The `benchmarking` VPC, regional
subnet, and `ds-bench` Docker repository exist. Google Cloud approved a
project-wide CPU limit of 96 and a separate 64 vCPU Spot pool in
`europe-west4`. Campaign preflight passes.

## Domain model

### Nouns

- `UpstreamPin(url: str, commit: str)` identifies the exact `ds-bench` source.
- `SourceIdentity(commit: str, diff_sha256: str)` identifies a Chronicle worktree, including uncommitted code.
- `ResourceBudget(cpu_millis: int, memory_mib: int, local_ssds: int)` is the total budget for one SUT.
- `ChronicleSplit(chronicle_cpu_millis: int, redis_cpu_millis: int, chronicle_memory_mib: int, redis_memory_mib: int)` must sum to the SUT budget.
- `ServerArm(system: str, label: str, durability: str, config: dict)` describes one server configuration.
- `SuiteRun(suite: str, arm: ServerArm, profile: str)` is one upstream suite invocation.
- `CellVerdict(status: str, reason: str, errors: int, lazy_creates: int, windows_aligned: bool, plateau: bool)` decides whether a result can support a claim.
- `CampaignManifest` records source identities, suite digests, image digests, hardware, Redis configuration, zone, and ordered runs.
- `EvidenceSeal` records the size and SHA-256 digest of every raw archive file plus one tree digest.

### Verbs

- `prepare(pin, overlay) -> PreparedCheckout` clones the pin, applies the adapter patch, and copies suite files.
- `preflight(target, manifest) -> PreflightReport` checks tools, billing, quotas, API access, image access, and cluster deletion access.
- `calibrate(checkout, budget) -> ChronicleSplit` runs the declared 1 to 3, 2 to 2, and 3 to 1 CPU split cells and selects one valid split.
- `resolve_suites(split, profile) -> list[SuiteRun]` writes immutable run specific suite JSON files.
- `run_suite(run) -> SuiteResult` invokes upstream `scripts/bench` and preserves its raw files.
- `validate_cell(raw) -> CellVerdict` rejects misaligned, client bound, error, lazy create, and unproven ceiling cells.
- `archive_campaign(results, manifest) -> Archive` copies raw data, resolved suites, logs, and provenance into a dated directory.
- `seal_archive(archive) -> EvidenceSeal` prevents silent changes to raw evidence after teardown.
- `render_report(archive) -> report.md` compares only cells with the same resource profile and labels weaker durability arms.
- `teardown(run) -> TeardownProof` deletes only the run's cluster and proves that it is gone.

### Boundaries

- `benchmarks/ds-bench/dsbench.py` owns orchestration, validation, provenance, and reporting.
- `benchmarks/ds-bench/patches/chronicle.patch` contains the smallest upstream integration patch.
- `benchmarks/ds-bench/overlay/` contains Chronicle manifests and declarative suites.
- The prepared upstream checkout lives under `.tmp/` and is never committed.
- Upstream `cells.json`, HDR files, CSV samples, and reports remain authoritative measurements. Chronicle code does not recalculate raw percentiles.
- `loadgen/` remains the Chronicle specific workload suite. This adapter does not merge its numbers with `ds-bench`.

## Slices

### Slice 1: One Chronicle smoke cell

Prove the full path from a pinned checkout to one upstream write result.

- Add `benchmarks/ds-bench/upstream.json` with commit `93a1a066a511ad2ce5114dc429afb1fd0f6d99bf`.
- Add `benchmarks/ds-bench/dsbench.py` with `prepare`, `test`, `run`, and `teardown` commands.
- Add `patches/chronicle.patch` for `chronicle` mode, target URL, reset behavior, image digest lookup, and local server cleanup.
- Add `overlay/gke/chronicle.yaml` with Chronicle and Redis in one pod.
- Add `overlay/suites/chronicle-smoke.json`.
- Add tests that prepare twice, verify a clean pin, run upstream unit tests, validate the suite schema, and exercise upstream dry run paths.
- Verification: `python3 benchmarks/ds-bench/dsbench.py test`.

### Slice 2: Fixed resource accounting and calibration

Make the Chronicle result use the same total SUT budget as every comparison arm.

- Validate that Chronicle plus Redis limits equal 4 vCPUs and 16 GiB.
- Mount Redis AOF data on the server node's local SSD and set `maxmemory-policy noeviction`.
- Add Redis `appendfsync always` and `appendfsync everysec` arms.
- Extend the upstream metrics poller to sum Chronicle and Redis process CPU and RSS while retaining pod working set as the memory headline.
- Add `overlay/suites/chronicle-calibration.json` for the three CPU splits.
- Add calibration parsing and the declared selection rule to `dsbench.py`.
- Verification: unit tests reject resource overcommit, invalid cells, ties without a deterministic winner, and a split that changes after calibration.

### Slice 3: The comparison workload union

Run the blog workloads and the maintained upstream suite set.

- Add 4 vCPU parity suites for Rust WAL and memory, Node memory, Ursula disk and memory, and Chronicle Redis always and everysec.
- Cover write saturation, write memory, one stream SSE fanout, catch up, connection scale reads, and mixed interference.
- Keep the upstream 8 vCPU and six SSD canonical Rust suites available under a separate `canonical` profile.
- Express the blog's one stream SSE workload through the current declarative `reads` runner, which already supports Node and Chronicle through the same client method.
- Generate run specific suites after Chronicle calibration. Archive those files before execution.
- Verification: every expected system, durability arm, workload, stream count, and subscriber level appears exactly once in the resolved campaign matrix.

### Slice 4: Provenance and report validity

Make each number traceable to source, hardware, and raw evidence.

- Add `manifest`, `validate`, and `report` commands to `dsbench.py`.
- Record full Git commits, Chronicle diff digest, resolved image digests, Redis digest and config, suite SHA256 values, GKE machine types, CPU and memory limits, disk mode, zone, timestamps, and command lines.
- Reject headline cells with errors, nonzero lazy creates, misaligned windows, client saturation, or an exhausted load ladder.
- Report exhausted ladders as lower bounds and observed failures as gaps.
- Separate durable arms from memory and `everysec` arms in every table.
- Add fixture based tests against upstream aggregate JSON and invalid synthetic cells.

### Slice 5: Paid run safety

Prevent a failed run from leaving infrastructure active.

- Add local and remote preflight commands with machine readable output.
- Check the frozen VM topology against project, region, machine family, disk, address, and instance quotas before cluster creation.
- Run calibration through the same exact name watchdog, archive, teardown, and absence proof used by the full campaign.
- Use project `adityavkk-prototyping`, GKE region `europe-west4`, and Artifact Registry region `europe-west1` by default.
- Run one measurement cluster at a time.
- Arm a cluster name scoped deadline watchdog before each suite.
- Capture logs and provenance before teardown.
- Delete the suite cluster on success or failure unless `--keep-failed-cluster` is explicit.
- List owned clusters after deletion and store the empty result as teardown proof.
- Verification: dry run tests prove that cleanup targets only names created by the current campaign.

### Slice 6: Execute and publish the baseline

Produce the first Chronicle comparison dataset.

- Run the local smoke suite. Completed on July 26, 2026.
- Run remote preflight. Completed on July 27, 2026 with every access and capacity check passing.
- Run calibration. Completed with the 2 to 2 Chronicle and Redis CPU split.
- Run the full parity campaign. Completed all 24 suites on July 28, 2026 UTC.
- Seal the raw archive. Completed with 2,584 files under one SHA-256 tree digest.
- Rerun the four corrected catchup suites. Completed on July 28, 2026 UTC with
  56 valid cells.
- Reuse the exact Chronicle, Rust, Node, Ursula, and Redis image digests from the
  sealed baseline. Completed and verified from both manifests.
- Add the catchup supplement to the report. Completed by replacing each full
  system and catchup workload slice.
- Verify both evidence seals and confirm that the final report has no invalid or
  missing rows. Completed.
- Verify that the project has no owned benchmark clusters. Completed after both
  the baseline and supplement.

## Risks and mitigations

- The adapter patch can drift from upstream. `prepare` applies it only to the pinned commit, and tests fail on any rejected hunk.
- Chronicle can gain resources through Redis. Resource validation checks the sum of both containers, and the report uses whole pod memory.
- Calibration can become result shopping. The split set and selection rule are fixed before the run, and all calibration cells are published.
- A load generator limit can look like a server limit. The validator rejects client bound cells and exhausted ladders.
- Response payload bytes can differ from stored bytes when a server adds record
  framing. Catchup validity uses post-write stored-size probes. Payload MiB per
  second remains a throughput metric, not a seed-size proof.
- Cloud Billing data can lag. Deadline watchdogs and sequential clusters bound live infrastructure even when cost data is delayed.
- Spot client capacity is not guaranteed. A missing or preempted client node invalidates the cell, and the sequential runner must delete the cluster before a clean rerun.

## Open questions

None. The reviewed research document fixes the comparison profile, workload scope, and GCP account.

## Out of scope

- A managed Redis result in the same table as the single node comparison.
- Multi node availability comparisons.
- Replacing upstream HDR merge or report calculations.
- Treating local kind numbers as publication results.
