# Snapshot allocation and OOM recovery qualification

Seed 85262 failed before its planned drain intervention: all three voting pods
hit the old 512 MiB limit and subsequently OOM-looped during recovery. This is
an availability failure, not a passing membership-overlap experiment. Five stopped
PVC/WAL captures remain under `/tmp/chronicle-oom-85262`; checksums are retained
in `snapshot-memory/pvc-sha256.txt`. Do not attach copied Raft identities to a live
cluster. The earlier seed 85241 missed its intent window and injected no fault.
Both failed experiments are retained as compressed evidence, not discarded.

Recovery used the original volumes, no bootstrap/reset, with a 1536 MiB limit.
All 1,256 acknowledged workload appends were observed in subsequent strict reads.
The four extended histories returned Porcupine `Ok`; the originals and extensions
are separate files in `snapshot-memory/oom-85262.tar.gz`. Original reads did not
cover every acknowledgement. The smoke checker now reports that missing retention
evidence instead of treating an unobserved acknowledged record as verified.

The allocation refinement was documented and formal checks run before code.
Building snapshots now borrows actor-owned state and serializes directly into
the checksum-prefixed buffer. It removes two full-state clones and a second encoded
buffer; it does not change the wire format, atomic publication or install checks.
Oracle found that the initial refactor lost the precommit test gate. The reviewed
version preserves it and adds subprocess crash/reopen and SQLite xWrite/xSync
failure tests for snapshot publication. `make check` passed, including 39 Python
tests. Formal and check logs are retained beside this report. No memory bound is
proved by the unchanged abstract snapshot transitions.

## Isolated recovery measurements are not capacity bounds

`tests/recovery_memory.py` copies the same stopped node-1 PVC, disables Docker
networking, never initializes it, and records kernel OOM state, sampled cgroup peak
and the authenticated recovery barrier. This is not quorum readiness. Results:

| Image | Limit | Sampled peak bytes | Recovery barrier |
|---|---:|---:|---|
| leadership1 | 512 MiB | 514,895,872 | OOMKilled, exit 137 |
| leadership1 | 1536 MiB | 580,583,424 | 2.588 s |
| leadership2, before gate review | 512 MiB | 476,168,192 | 2.603 s |
| leadership2, before gate review | 1536 MiB | 446,861,312 | 2.639 s |
| leadership3, reviewed | 512 MiB | 483,414,016 | 3.206 s |

Image IDs are in the JSON results. Reviewed image:
`sha256:ccf403c316d4cd693555fc458216734f6e77764bca56335b8e0ca83210ee513c`.
Single runs do not isolate all causes of the peak difference or establish a safe
512 MiB limit. Snapshot decoding/install and concurrent group allocations remain.
The local manifest therefore uses 1536 MiB, with a 512 MiB request, and requires
workload-specific memory qualification. Logical quotas are not RSS bounds.

These are process/container failures on one host, not power-loss or AZ tests.
The reviewed image rolled onto all five candidate pods with the original volumes.
The unchanged full protocol suite then passed: **326 passed, 6 upstream-default
skips, zero failures**, 96.59 seconds, while the four-shard qualification workload
was active. Full JSON and text output are in `snapshot-memory/conformance.*`.
Live membership-overlap qualification remains separate acceptance work.
