# Resource-informed replica movement on real k3d

Source `57810424bd30cd4ec1c9130c76c2bd3452c5d236`, image
`chronicle-raft:resources2`. Exact binary, Docker and containerd identities are in
`resource-84171/`. All five pods retained their PVCs; the upgrade stopped every
pod before changing the image. Mixed-version operation is not supported.

## Executed result

Four fresh streams, one per data shard, used eight paced producers and four strict
readers. Node 4 was an already registered, verified retired replica—not a new
identity. The hook enabled it in the same **simulated** domain `a` as node 1,
observed balancing, required unchanged placement for 125 seconds, then drained
it and verified restoration. These domains share one physical orb host.

* The resource controller moved shards **2 then 1**, replacing node 1 with node 4.
  First weights were `{0:16, 1:27, 2:28, 3:26, 4:27}`. Second weights were
  `{0:16, 1:28, 2:28, 3:26, 4:27}`. Recorded committed control entries were
  `T9775-N1-588` and `T9775-N1-590`, **66.417 seconds apart**.
  The matching placement generations and `resource placement intent committed`
  events establish resource selection, rather than inferring it from a move.
* All five target placements continued to span three supplied domains. The new
  placement remained unchanged for **126.425 seconds**. Restoration returned all
  groups to voters `[1,2,3]`; the spare passed retirement verification.
* Pod UIDs/container identities/restart counts remained unchanged throughout the
  hook. No native campaigns were enabled.
* All four histories received independent Porcupine **Ok**, each retaining 2,400
  records: **9,600 acknowledged appends**, no duplicate effects or lost acknowledged
  records observed. All producers completed their full workload.
* There were **249 unknown append attempts**, zero definite failed append attempts.
  Logical append p99 including retries was **215.55 ms**, maximum **2,864.05 ms**.
  The paced run achieved **23.94 acknowledged appends/s** over 401.00 seconds;
  this is a fault/placement qualification rate, **not throughput capacity**.
* `make check` passed on the final revision, including 37 Python tests. The
  unchanged full pinned conformance suite passed again: **326 passed, 0 failed,
  6 upstream-default subscription skips**, 70.13 seconds. Reports are
  `conformance-resources.{json,txt}`; no suite assertions or skip settings changed.

The raw movement observations, controller events, histories, checker outcomes and
statistics are retained in `resource-84171/`. History timestamps are monotonic;
Raft diagnostics are not the checker's source of truth. Histories are losslessly
compressed after checking; uncompressed checksums are retained.

## Reproduce and interpret

`tests/history.py` remains the sole workload/fault timing owner. Its primary run
used `--nemesis node-join --nemesis-delay 15` and
`--hook 'node-join:start=python3 tests/resource_balance.py --output NEW.jsonl'`.
All four streams used two producers, one reader, 1,200 operations per producer,
0.3-second append pacing, two-second read pacing, ten-second timeout and at most
100 retries with 0.2-second delay. The cluster was in fixed-tenant mount mode;
effective identities were `conformance-mounted` plus `resource-balance/PATH`.
Paths and seeds are retained in each history's first event. Use fresh paths and
output files; the hook refuses an unexpected initial placement.

The policy contract and bounded proof scope are in
[`formal/RESOURCE-BALANCE.md`](../formal/RESOURCE-BALANCE.md). Actor service time
includes I/O waits; charged bytes are not RSS or physical disk measurements.
Retired files are not reclaimed. Hysteresis may leave residual imbalance, and a
fixed-demand local descent does not establish convergence under changing load.
The 125-second observation is one measured stability interval, not a universal
oscillation bound. Replica movement can dislodge a leader but is not independent
resource-informed leadership transfer. The latter remains unfinished.

Oracle review found term-window reuse and unnecessary polling during pending
repair; both were fixed. The follow-up found no scoped blockers. Unit regressions
also cover real sample-to-weight selection, actor reopen/counter resets, competing
global-cooldown proposals, stale generations, queue and known-domain guards.
The initial unit test incorrectly expected three moves; independently calculating
the configured 10% hysteresis gives two moves and final potential 92,928. That
expectation was corrected without changing the policy to satisfy the test.
