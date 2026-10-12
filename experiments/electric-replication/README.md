# Replicated Electric engine experiment

This isolated Rust binary extends Electric's actual native storage/read engine.
It does not alter Chronicle's Go/Redis default, and it is not the SQLite-backed
`rust/distributed-streams` experiment. **Not production-qualified.**

The imported source is Electric npm 0.1.5 at
[`88793e7`](https://github.com/electric-sql/electric/commit/88793e76595d69be300731b9b25c58538923a53b),
Apache-2.0. `UPSTREAM-TREE.txt` pins original blobs; `engine/LICENSE` retains the
license. The isolated stock OpenRaft 0.10.0-alpha.36 candidate provides consensus
over Electric's WAL/group-fsync path; qualified 0.9.25 remains its comparison
baseline. Its initial write regression and measured scheduling correction remain
in the evidence ledger; broader performance qualification is incomplete.
Committed commands materialize the native wire files; native file-range/sendfile
and epoll SSE paths serve reads. No SQLite, RocksDB or Redis payload dependency.

## Contracts and scope

* [CONTRACT.md](CONTRACT.md): partitions, quorum-fsync acknowledgements, unknown
  outcomes, linearizable/prefix/session reads, recovery and source provenance.
* [TIMED-STATE.md](TIMED-STATE.md): committed TTL/access clock and replay.
* [FORKS.md](FORKS.md): cross-partition retained-prefix grants, bounded wire import,
  hidden publication and descendant-aware release.
* [SUBSCRIPTIONS.md](SUBSCRIPTIONS.md): partition-owned durable webhook/pull-wake
  leases, cursors, fencing, retries, signing, SSRF validation and catalog repair.
* [ASYNC.md](ASYNC.md): opt-in local-fsync202 receipts, await helper, bounded
  admission and committed-only reads; acceptance can be lost after failover.
* [evidence/README.md](evidence/README.md): executed gates and preserved failures.

Paths hash to fixed groups, not to the current node count. Replica movement uses
learner catch-up and joint membership; it does not repartition a live stream.
One ordered stream still has one owner group. There is no general cross-group
transaction or global read snapshot. The experimental control plane is trusted,
loopback-only; it is not a production authentication/transport design.

Quorum-fsync remains the default. Opt-in local-fsync POST appends return 202 and
a receipt, not a session or a successful append result. An HTTP timeout/503 after
admission can have committed; producer epoch/sequence handles retry effects. Reads use
`Stream-Consistency: linearizable` by default. `prefix` allows unbounded staleness;
`session` requires a same-cluster, same-partition `Stream-Session` response token.
Live streams become committed-prefix feeds after their initial read barrier.

## Reproduce locally from the repository root

```sh
cargo test --locked --manifest-path experiments/electric-replication/engine/Cargo.toml --features replication
cargo build --locked --release --manifest-path experiments/electric-replication/engine/Cargo.toml --features replication
bash experiments/electric-replication/scripts/formal.sh

pnpm --dir .tmp/electric-conformance add --save-exact @durable-streams/server-conformance-tests@0.3.5
python3 experiments/electric-replication/scripts/conformance.py experiments/electric-replication/evidence/conformance-new
python3 experiments/electric-replication/scripts/qualify.py experiments/electric-replication/evidence/fault-new
python3 experiments/electric-replication/scripts/fork_faults.py experiments/electric-replication/evidence/fork-fault-new
python3 experiments/electric-replication/scripts/subscription_faults.py experiments/electric-replication/evidence/subscription-fault-new
python3 experiments/electric-replication/scripts/storage_faults.py experiments/electric-replication/evidence/storage-fault-new
python3 experiments/electric-replication/scripts/reclaim_faults.py experiments/electric-replication/evidence/reclaim-fault-new
python3 experiments/electric-replication/scripts/async_faults.py experiments/electric-replication/evidence/async-fault-new
python3 experiments/electric-replication/scripts/snapshot_progress.py experiments/electric-replication/evidence/snapshot-progress-new
node --test experiments/electric-replication/client/receipts.test.mjs
```

`scripts/benchmark.py` uses the pinned unmodified native server and ds-bench
binaries at `.tmp/electric-tools/{upstream,bench}-target/release/`; their SHA-256
values and exact source pins are in each run's `provenance.json`. It records fresh
per-cell configurations, command lines, exact seed/offset probes, raw client/HDR
results, syscall profiles, process/socket samples (`samples.jsonl.gz`), storage
sizes and errors. Run it sequentially with other builds/tests stopped.

Use a fresh output directory name each time. Drivers start/stop supervised orb
services; data directories are disposable under `.tmp/electric-labs/`. They keep
per-node JSON configuration, source/binary hashes, logs and raw histories. None
creates cloud infrastructure. The full conformance suite runs unchanged with
`subscriptions:true`, three processes and two partitions, with initial leaders
co-located for its single direct HTTP endpoint. The fork fault campaign starts
the two leaders on different nodes and verifies actual learner snapshot install.

Building without `--features replication` retains standalone native mode. The
replicated binary starts with `--cluster-config <node.json>`; the harness writes
examples. It rejects old experimental data identities rather than silently
migrating them. Never start two copies of one node identity, even on different
disks; filesystem locking only fences one local directory.

## Remaining gates

Work proceeds in this order. The clean local 0.9 checkpoint `7fc1605` and its
qualified release binary remain the comparison baseline; the published branch
checkpoint is not production-qualified. Each row requires the actual engine,
not a substitute store or a model-only result.

| Order | Acceptance gate | Current state and required evidence |
| --- | --- | --- |
| 1 | Stock OpenRaft 0.10 isolation and lease admission | Local correctness qualification passes: the work-driven cold candidate has 150 replicated Rust tests and unchanged 332/332 conformance in both hot-only and cold-enabled configurations. Assignment-time lease rejection retains outstanding receipts/credits. Hot identity 10 is preserved; cold identity 11 is isolated, with no rolling upgrade or migration. Performance comparison below blocks production promotion. |
| 2 | Single-node overhead and replicated sync/async performance | Open: the work-driven campaign rechecks 114/132 raw cells, with 18 failures retained and 107/107 in the explicitly selected matched set. Ninety-second c4 hot-stream offload now keeps up at the measured finite points; three-minute high-cardinality backlog still rises and one-member hot p99 worsens. The separate 8 MiB/c16 comparison has a 1.67× native/one-member throughput ratio, not general parity. The earlier 119/120 baseline campaign and its ten async lease rejections remain failures. Keep commit markers and admission bounds; sustainable high-cardinality capacity, tail latency, RSS and instrumentation overhead still block performance acceptance. |
| 3 | Storage lifecycle and bounded metadata | Terminal fences and cold publication/pins/certificates/recovery/inventory GC have TLA+, native-WAL properties and real-MinIO fault qualification, rerun as `cold-work-runtime-001`. Node-local backlog/disk admission passes held-PUT/outage/restart and actual disk-floor probes. Eight matched 48-generation cells verify remote reads, pin-protected deletion and eventual zero-payload/no-pending/four-root storage, with 96 rejected history mutations. Candidate RSS rises from 103–105 to 159–186 MiB aggregate; no plateau is proved. Cold stays opt-in, loopback qualification-only. Participant revocation, longer/higher-cardinality bounds, moving SSE pin watermarks and provider/independent-host durability remain gates. |
| 4 | Production observability, security and operations | Open: start instrumentation with earlier work; qualify versioned redacted events, W3C/OTel correlation, quorum/admission/receipt/storage/GC metrics, bounded profiling, alerts and telemetry failure/overhead. Transport/admin auth, TLS, tenant/resource isolation, bootstrap/drain/replacement, backup/restore and upgrade/DR paths need actual implementations and fault/property coverage. |
| 5 | Independent-host acceptance and release | Blocked on separate resource/access approval after no-spend work. Require independent machine/disk failures, volume loss/spares, partition and membership/subscription histories under load. Local shared-host tests do not discharge this gate. Prepare bounded cost, limits and cleanup before paid resources; no use of the PostgreSQL budget. |

Every deliverable needs a substantive TLA+, Lean, generated state-machine test
or independently checked fault history over its risky behavior, with explicit
model-to-code gaps and useful negative controls. Qualified publication and trunk
integration are authorized, not automatic: audit the full unpublished range,
preserve human commit identities, integrate concurrent `origin/main`, run
fail-fast checks and CI, then verify the actual remote merge. No shared
deployment is authorized. Independently complete experimental milestones may
merge with explicit limits; they cannot waive the remaining acceptance gates.

The resumed local completion sequence is below. A passing scoped checkpoint is
not completion of later rows; cold remains opt-in and loopback-only. The
[publication audit](evidence/cold-publication-005/README.md) covers the completed
scheduler/admission checkpoint and its complete unpublished ancestry, not only
the most recent evidence increment.

| Next local work | Required exit evidence |
|---|---|
| Attribute many-stream phase time and retained memory | Bounded existing timing probes and allocation/RSS/thread observations; matched preserved binaries, no concurrent heavy jobs. Separate live allocations from allocator retention and incomplete offload from bounded partial tails. |
| Correct measured costs, then establish supported operating points | Exact bytes/offsets and every-replica drain; repeated sustained resource/GC windows, explicit rejected/unknown attempts and p99. Keep rejected candidates. A paced zero-error point is not native parity. |
| Participant revocation and moving live-reader watermarks | Extend the actual ownership model before behavior changes; stale disks cannot regain authority after revocation, and consumed SSE prefixes cannot release bytes still needed by a reader/transfer. Include full restart and delayed external I/O histories. |
| Native diagnostics and resource isolation | Versioned/redacted bounded events and metrics; causal correlation, telemetry outage/overhead and hostile cardinality tests. Diagnostic counters alone do not meet production observability. |
| Transport, admin and tenant boundaries | Locally verifiable authentication/TLS/authorization contracts and deny/allow cases. Do not invent deployment identities, tenant policy or provider credentials; flag any required new dependency or incompatible public contract before adopting it. |
| Recovery and upgrade operations | Executable bootstrap/drain/replacement/backup/restore histories, stale-identity rejection and explicit format/rolling-upgrade compatibility. Never call format rejection a supported migration. |
| External acceptance plan | Bound machines/disks, duration, traffic, cost and cleanup; obtain separate authorization before provisioning. Shared-host MinIO is not independent-host, cloud-provider or power-loss evidence. |

Conformance and the existing fault/property tests are necessary, not sufficient.
The prior post-campaign `conformance-036` and `conformance-cold-004` each execute
**332/332 passing, zero failures/skips/todo**, with subscriptions enabled,
three processes and two partitions. Cold-enabled conformance records 84
publications but zero cold HTTP bodies: compatibility, not forced-cold reads.
The unchanged release retains the earlier 149-pass replicated Rust qualification;
the benchmark task changed no runtime inputs and did not rerun the Rust suite.
Three upstream helpers remain ignored
(two forensic entry points and a standalone S3 fixture), not conformance
exclusions. Standalone runs 116 tests with two forensic helpers ignored.
`storage-fault-024` checks seven real failing/short
syscalls, rejects old data identities without changing stored bytes, and retains
three unknown HTTP outcomes. `async-fault-015` checks 311
operations and 40 receipts: 21 committed, one rejected and 18 invalidated, with
six unknown requests retained. Its isolated leader rejects new assignment on
lease expiry despite spare backlog capacity, without changing WAL bytes or
releasing unresolved credit. These are single-host qualifications, not
independent-disk/AZ or power-loss evidence.

`reclaim-progress-001/002/003` isolates a second scheduling boundary: OpenRaft
awaits logical purge inside its core task, so worker handoff alone still stalls
the group. Coalescing physical GC after durable logical purge raises completions
inside the same two-second checkpoint fsync from zero to 38, with no unknowns
or leadership change and exact restart. The reader/reclamation model checks
4,910 safety states, 2,047 stable-period liveness states and six mutations.
Background storage errors still fail-stop; no durability/lease bound changes.
The subsequent `reclaim-envelope-c16-001` passes all **12/12 ninety-second cells**
(three repetitions per arm, zero warmup): native 17.07–17.45k/s, one member
9.42–9.70k/s, three-member quorum 6.09–6.43k/s, async 8.26–9.06k accepts/s.
Every cell has zero client errors/rejections, stable sampled clocks and exact
all-replica byte drain. This establishes that finite operating point, not maximum
capacity or elimination of lease expiry under every load. Async uses 144–148 MiB
aggregate sampled RSS versus quorum's 117 MiB; resource costs remain explicit.

The current performance investigation stops at a concrete safety decision:
**retain per-apply durable commit markers**. One-member WAL fsyncs/ack are about
0.343 versus native 0.172, and marker waits average 277–280 µs per cohort.
Stock recovery-wait alone is not a safe replacement across conflicting snapshot
truncation and repeated crashes. No marker-removal or changed restart contract
is implemented in this checkpoint; [BATCHING.md](BATCHING.md) records the
source-derived failure sequence and the extra qualification a future candidate
would need. Native cold ownership/recovery/GC now has the local qualification below.
Production security/operations and native
telemetry integration remain queued, beyond the diagnostic instruments below.

Terminal-fence compaction now passes its local gate (`fork-fault-019`): 1,718
checked operations, 323 fork resources, four delayed retired Grants rejected,
eight reclamation observations, and one preserved unknown mutation. After 320
retirement cycles around an older live fork, each peer group's fences coalesce
to one interval; source/destination records and dead mirror indices are gone,
and terminal replies remain bounded at 256. Fresh-learner snapshot installation,
full-cluster crashes and recreation preserve those results. TLA+ precedes the
implementation (12,526 safety states, 349 liveness states, three mutations);
native-WAL generated and 4,096-record boundary tests cover the encoding/recovery
gap. [FORKS.md](FORKS.md) documents the fixed admission limits and count-only
diagnostics. Identity 9 is incompatible with prior data; there is no migration.
The performance matrices below qualify the earlier identity-8 implementation,
not a new performance measurement of terminal compaction.

Additional testing after `conformance-031` found a protocol gap outside that
suite: a readable fork incorrectly returns 409 to a matching PUT after its parent
is deleted. `fork-retry-001` retains the actual three-process failure; the native
generated regression failed before the fix. Identity 10 now retains the original
source configuration in the grant and reconfirms the current child at its owner,
without a live source lookup. `fork-fault-020` checks 1,728 operations, 323 forks,
ten reconfirmations, four delayed Grants and eight reclamation observations;
two unknown mutations remain. Six negative history mutations are rejected.
Inherited/overridden TTL comparisons survive source-only outage and destination
snapshot/restart. The source fixture temporarily has one voter while the
destination retains three; this is an availability test, not independent-disk
durability evidence. All 144 replicated and 113 standalone Rust tests pass,
along with the full formal and storage/async/subscription/reclamation reruns.
Snapshot `ERSP0007` and RPC `/_raft10/` reject older formats; no migration exists.

`cold-foundation-001` adds the shared-object ownership model and corrects the
native filesystem object fixture's key aliasing, shared upload temporary name
and missing directory durability. Red regressions are retained; generated
operation/reopen/range/reclamation tests and six actual failing PUT/DELETE
syscalls pass. The full rerun passes 147 replicated and 116 standalone Rust
tests, 63 TLC mutations plus the Lean negative control, and unchanged 332/332
conformance. The local fixture uses a fresh layout, not a migration from the old
ambiguous flat keys. At that milestone replicated cold was still disabled; those
results alone did not qualify the later ownership/GC runtime.

The formal-first **certified-inventory GC** milestone extends `ColdOwnership.tla`,
not the native runtime: all six new positive configurations and 20 new negative
controls pass (`cold-inventory-001`). The full suite detects 83 TLC mutations
plus the existing Lean negative control. The architecture requires committed
singleton Begin IDs, an exact live+pending+active-inventory cut, completed durable
snapshot/purge certificates, storage participants distinct from membership,
learner registration before transfer and durable full-cluster restart authority.
Inventory replacement cannot immediately delete the prior root. Repeated finite
prefix scans collect late PUTs without resetting progress on every replacement.
[The contract](CONTRACT.md#shared-cold-ownership-contract)
states the invariants, finite scenario bounds, listing/durability/fairness
assumptions and unproved implementation gaps. Metadata proportional to current
live/pending/participant state is the representation goal, not a TLC proof of
unbounded growth control; indefinitely unreachable participants or delayed PUTs
preclude an unconditional physical-space bound. No runtime or performance claim
follows from the model alone; the subsequent implementation is qualified below.

## Replicated cold storage has local end-to-end evidence, not absolute proof

`cold-runtime-012` passes **817 recorded events** against a separate unmodified
MinIO process. All three original replicas have stat-verified zero-length native
hot files before full restart and remote binary reads. Cold SSE catches up over
three 64 KiB windows, then delivers a committed append and closure. It checks
JSON/sub-offset forks, quorum-loss publication fencing, an HTTP reader across
retirement, actual learner snapshot/suffix replay through remote objects,
removed-but-undrained restart/rejoin, corrupt/unavailable objects and inventories,
real snapshot/certificate fsync EIO, unknown DELETE, and late payload PUTs.
A real multipart inventory completes after its uploader crashes, its Begin is
aborted and newer inventories activate: stale resume is rejected and repeated
scanning removes the late object. Provider outage during committed JSON-fork
apply fail-stops the replica; recovery is checked after the provider returns.

The independent ordered-history checker verifies 28 client reads, one live feed,
a deduplicated retry, 391 observed payload GETs, 263 DELETEs and 74 inventories.
Two unknown HTTP outcomes remain unknown. Eight corrupted histories are rejected.
Twelve retired generations preserve the exact 14-live-descriptor baseline and
leave no pending Begin; another 160 live
objects exercise inventories and scanner traversal beyond its 128-key page,
then return to the prior 28-live-object baseline. The first checker incorrectly
compared total live segments with the count of retired generations; increasing
the surviving payload exposed that mistake. Its failure is retained, and the
corrected check compares exact pre/post live sets and rejects a retained-generation
mutation. These are finite measurements,
not a long-duration, unbounded-state or reclamation-rate proof.

The real native-WAL generated property covers JSON/binary forks, snapshot/replay,
exact inventory cuts and actual hot reclamation in 48 cases. Removing the final
pin fence or omitting pending records from capture makes it fail; both temporary
mutations were restored. A second 48-case property found unbounded native inline
SSE catch-up. Replicated cold now bounds its binary/text windows at 64 KiB and
grows JSON windows only to a complete value, capped at 8 MiB, without splitting
UTF-8 or advertising an unconsumed offset. The failing original and a separate
test-journal reopen-size error are retained alongside the final passing run.
The final Rust runs pass 149 replicated and 116 standalone tests. A disk-full
standalone attempt is retained separately, not counted as a pass. The unchanged
cold TLA models were rerun: eight positive
safety/liveness configurations and all 20 inventory/recovery negative controls.
Lean still proves only the existing deterministic contracts, not cold Rust.
The final release SHA-256 is
`bd4834f72cf88609377c273a844ba89967edec21cc2122edf18304e119f2f878`;
[the qualification manifest](evidence/cold-runtime-tests-001/qualification.json)
cross-checks 280 engine/retained-vendor/Cargo manifest and lockfile input hashes,
both suite runs, the cold campaign, model files, driver/checker and retained logs.
The active OpenRaft 0.10 registry dependency sources are not included in those
280 hashes; Cargo.lock pins their registry checksums.

Reproduce after installing the pinned conformance package above, building MinIO
from `minio/minio` commit `0d7408fc9969caf07de6a8c3a84f9fbb10a6739e` to
`.tmp/electric-tools/minio`, and creating a Python venv at
`.tmp/electric-tools/cold-venv` with `boto3==1.35.99` and `botocore==1.35.99`:

```sh
cargo test --locked --manifest-path experiments/electric-replication/engine/Cargo.toml --features replication,tier
cargo build --locked --release --manifest-path experiments/electric-replication/engine/Cargo.toml --features replication,tier
.tmp/electric-tools/cold-venv/bin/python experiments/electric-replication/scripts/cold_faults.py experiments/electric-replication/evidence/cold-new
python3 experiments/electric-replication/scripts/check_cold.py experiments/electric-replication/evidence/cold-new
.tmp/electric-tools/cold-venv/bin/python experiments/electric-replication/scripts/conformance.py experiments/electric-replication/evidence/conformance-cold-new 2 cold
```

Every run uses fresh fixture credentials and source/binary/config hashes; raw
stores and provider diagnostics remain private. [The correspondence table and
assumptions](CONTRACT.md#native-implementation-and-correspondence) link modeled
actions to actual code and tests. Long-lived SSE holds its original pin until
disconnect, and unreachable participants deliberately block GC. Incomplete
multipart uploads need a provider lifecycle policy; no automatic revocation or
arbitrary bucket lifecycle is safe. The proof/implementation gap, provider
honesty, separate-disk/power-loss behavior, production auth/TLS/observability and
cold performance remain explicit. No paid resources or main merge were used.

### Baseline cold-enabled performance campaign

The baseline local campaign has completed; **performance acceptance is not met**.
`cold-bench-001` executes three rotated repetitions of native / one-member /
three-member quorum / three-member local-ack, each hot and cold, over five
workloads (120 cells, 24 seconds each). Its independent checker passes 118/120.
The unchanged native pin is rebuilt with `--features tier`; the same resulting
binary serves both native controls. The pinned ds-bench client is unchanged.
All 280 runtime input hashes and its release binary match the cold runtime
qualification above. That campaign changed harness/evidence/documentation only;
the later scheduler/admission candidate below changes runtime code separately.
The [final campaign checker](scripts/cold_campaign_check.py) independently reruns
the ledgers and pins their reports in
[cold-bench-qualification-003](evidence/cold-bench-qualification-003/checked.json).
Its exit zero means the evidence agrees with the **non-green** ledger, not that
performance passed. Profiles and the paced generation probe are separate from
untraced ds-bench throughput.

All arms share this orb's one disk and 16 GiB memory. The SUT has four aggregate
CPU affinities, MinIO/proxies/observer two, and ds-bench two. Two WAL shards or
partitions, 8 MiB WAL segments, two workers per process, no tail cache and 64 KiB
cold segments/compaction are fixed. Small segments deliberately exercise offload
and collection; these are not Electric's default 8 MiB capacity settings. The
test requires 8 GiB free before startup and stops clients below 6 GiB. Only owned
passing disposable stores are removed; failed stores remain local and evidence
is retained. `cold-bench-preflight-001` records the prior cleanup of exactly
153 verified all-zero, exited native test WAL fixtures, not arbitrary old data.

Each read cell preconditions immutable objects and verifies actual zero local
payload/segment bytes before measurement. Only GETs of those published keys
wholly inside the invocation count as client remote fetches; inventory reads,
upload verification, recovery and post-run probes do not. Native `object_store`
encodes `~` in keys differently from the replicated S3 client; attribution uses
the actual listing and one forward encoding, never recursive URL decoding.
Remote fetched bytes include object hash verification and are not equal to bytes
emitted over the client HTTP connection. The client does not report exact
monotonic drive boundaries for reads/mixed/fanout: those resource/S3 observations
are explicitly outer-invocation windows, not invented per-operation timings.

Write cells use one 256-byte stream or 1,024 4 KiB streams at 16 connections;
replay uses four reclaimed 1 MiB streams. Mixed traffic uses 16 streams with
32 writes/second each, four replay readers and 16 SSE readers. Live fanout uses
100 readers at 128 writes/second. Retired garbage is deleted during every
measurement. Live-only fanout is not forced-cold catch-up: a separate 16-reader
binary probe checks exact cold prefix, transition, committed suffix and closure.
All cells require exact final bytes/offsets on every replica. HTTP 202 counts as
acceptance, not committed semantic success; ds-bench discards receipts, so the
independent final drain is essential and is not a replication-latency metric.

#### Measured operating points, not a capacity claim

The native cold/many-stream repetition 2 failed **before SUT/client startup**:
MinIO's readiness URL returned 200 while its object layer was uninitialized.
The harness now waits using signed, read-only ListBuckets and attempts bucket
creation exactly once; unknown mutations and permanent errors are not retried.
`cold-bench-repair-001` repeats the entire native hot/cold pair, passing 2/2.
The selected matrix is therefore **119/120**, not 120/120. Async hot/many-stream
repetition 3 retains 112,481 accepts, ten lease-expiry rejections and zero other
client errors, followed by an exact 527,831,040-byte committed drain on each
replica (including seed). It is not a zero-error operating point.

Medians of three matched repetitions follow. `H / C` means hot / cold-enabled;
writes are acknowledgements per second, **202 acceptances for async**. Replay
is client MiB/s, one exact 1 MiB response per operation. No cross-arm c4/c16
comparison or confidence interval is implied by three repetitions.

| Arm | One 256 B stream, acks/s H / C | 1,024 × 4 KiB streams, acks/s H / C | 1 MiB replay, MiB/s H / C |
| --- | ---: | ---: | ---: |
| Native | 13,526 / 10,707 | 8,422 / 6,512 | 4,178 / 40.46 |
| One-member consensus | 7,258 / 6,452 | 5,335 / 4,982 | 3,999 / 36.54 |
| Three-member quorum | 5,927 / 4,733 | 3,348 / 2,824 | 3,426 / 36.17 |
| Three-member async | 8,627 / 6,204 | **failed** / 2,817 | 3,385 / 36.29 |

The hot-stream paired native/one-member gap is 1.76–1.95×, cold-enabled
1.58–1.80×. These settings are not the earlier 87–102k/s native workload.
Cold hot-stream latency and aggregate SUT peak RSS across repetitions:

| Arm | p50 range, ms | p99 range, ms | Peak RSS range, MiB |
| --- | ---: | ---: | ---: |
| Native | 1.06–1.37 | 6.11–8.38 | 8.7–9.5 |
| One-member | 1.97–2.19 | 9.58–16.64 | 67.3–79.3 |
| Quorum | 2.54–2.81 | 29.63–32.72 | 152.6–154.6 |
| Async acceptance | 1.76–1.92 | 13.68–15.24 | 178.7–189.9 |

Startup is included separately: cold native 4.94–5.14 MiB, one-member
34.17–34.55 MiB, three-member quorum/async approximately 103–105 MiB aggregate.
This includes the previously measured stock histogram fixed allocation; it is
not all pending payload. Raw per-process CPU/RSS/I/O samples, socket-byte lower
bounds, object requests/bytes, live/pending counts, stage deltas and GC lag are in
each `summary.json`/`samples.jsonl.*`. Entry gaps are not byte lag; internal
applied-command counters also contain lifecycle metadata. RSS excludes page cache.

All 24 mixed cells sustain the **offered**, not maximum, 512 writes/s (the client
reports 512.67 including its initial immediate writes), with four replay readers
and 16 live readers. All 24 paced fanout cells have zero reported subscriber
errors. Timed frame totals are not record-sequence proofs: all 24 separate
16-reader probes verify exact catch-up/live/closure, including 12 cold probes.
Cold replay/mixed cells have actual client-attributable remote GET bytes during
invocation after verified local reclamation; cold live-only fanout is not
relabeled as cold replay. First-repeat replay observations fetch 0.91–1.00 GB
remotely, with SUT CPU 0.26–0.36 cores and provider/proxies 1.46–1.47 cores.
This small-segment local proxy/provider path is not cloud-provider bandwidth.

`cold-bench-envelope-001` separately passes all six **60-second, c4** many-stream
cells: async hot 1,811–2,155 accepts/s and cold 1,252–1,536 accepts/s, zero
rejections/errors and exact all-replica drain. It establishes a finite
replication operating point, **not sustainable cold-offload capacity**. For
example, cold repetition 3 commits 439,713,792 bytes but still has 331,710,470
bytes in node 1's local hot payload files at client completion.

#### The cold worker does not keep up with these write rates

In hot-stream repetition 1, native commits 73,815,296 bytes and leaves only
21,763 local hot-file bytes. One-member commits 41,151,744 bytes but leaves
34,991,366 locally. This is not acceptance hiding uncommitted replication:
every replica's committed bytes drain exactly. It is **offload backlog**.
Native also accumulates backlog in the high-cardinality workload; do not treat
native's high-rate cold results as unlimited offload capacity either.

There was a concrete scheduling limit in the baseline `cold_worker`:
one stream candidate per 100 ms iteration, plus an empty cursor-reset iteration
for one eligible stream. Thus a single stream gets at most five segment attempts
per second **before** object I/O, consensus and inventory work. The three
one-member hot-stream repetitions record only 93 / 93 / 94 payload PUTs in 24
seconds, consistent with that limit, versus native 1,125 / 859 / 1,003.
No durability or GC fence was changed during that campaign. The bounded fair
drain/scheduling change and disk/offload admission policy below have their own
formal and real-engine qualification; baseline results are not relabeled as
measurements of the new worker.

#### Repeated retirement reclaims objects, but RSS has no proven plateau

`cold-generations-001` originally has 24 driver passes but **22/24 independent
passes**. One async terminal observation catches a maintenance pin; its next
observation has none. The driver's drain predicate omitted the stricter checker's
pin condition; it now waits for that condition and runs the independent check
before declaring success/removing its owned fixture. One quorum cell has a
76.209-second wall/monotonic discontinuity and remains invalid timing evidence.
Both entire affected hot/cold pairs pass in `cold-generations-repair-001` (4/4).
The explicit selected set is **24/24**, with all original failures retained.

Each cell runs 24 generations over 120–126 seconds: four 128 KiB streams per
generation, concurrently paced asymmetric 256-byte appends at 64/s. Every
replica returns exact bytes/offsets; all cold preconditions have zero local
payload/segments. Selected cold generation read windows fetch 288 MiB from
MinIO, excluding verification, held readers and final drain probes. Nine
replicated cold cells hold a real remote reader for seven seconds across
deletion and more than two inventory periods. Its object survives until release.
Unpinned replicated GC p99 is 3.56–3.82 seconds; the held cases take 7.37–9.67
seconds, measured from application DELETE completion.

After deleting the last live stream, every selected cold cell has zero payload
objects, zero pending Begins and no observed remaining pin. Native has zero
objects; replication has four physical inventory roots, two per group (the
captured predecessor and replacement). Peak pending replicated descriptors are
at most eight in these low-rate cells. The same histories reject **174 mutations**
across the selected cells; another 17 actual benchmark-history mutations reject
wrong attribution, unreclaimed data, corrupt bytes/offsets, missing replicas and
false fanout completion. Generated attribution tests distinguish client reads
from upload verification/inventories and either window edge.

This does **not** establish an RSS plateau. Cold first-quarter → last-quarter
median SUT RSS rises approximately 56–57 → 71–80 MiB for one-member,
136–147 → 162–171 MiB for quorum, and 138–147 → 175–186 MiB for async. Stream
sidecar bytes return to zero, but native WAL preallocation remains (about 16 MiB
one-member / 48 MiB three-member after cleanup). Allocator retention versus a
leak has not been separated. Finite histories do not prove indefinite bounds;
unreachable participants, live pins and indefinitely late PUTs retain the
formal contract's intentional GC blockers.

#### Profiles preserve failure evidence and do not measure capacity

CPU and replay-syscall campaigns each pass 8/8 cells. The final binary actually
calls `sendfile` in **all four hot replay arms**; reclaimed cold replay uses the
remote-body path instead. Summary syscall error counts are unclassified by
errno; they are not HTTP error counts.
User CPU samples have no lost-sample report, but are sparse and exclude blocked
and kernel time. They do not establish a new allocation or scheduling cause.

The first kernel-fsync matrix passes 7/8; repeating the entire async pair passes
1/2. Both failed hot traces contain duplicate identical-timestamp enter events
near capture end. They remain invalid; no deduplication or invented duration was
applied. A matched ptrace-fsync campaign first fails all four cold cells on
MinIO's SIGURG signal records. The corrected invocation selects only fsync/
fdatasync and no signal records; its complete rerun passes **8/8**, with no
failed/incomplete/unmatched sync calls. Traced hot/cold total sync calls per
client ack are native 0.890/0.946, one-member 0.275/0.282, quorum 0.763/0.907,
async 0.569/0.693. These include provider and lifecycle work; ptrace changes
batching and slows the workload dramatically. They are not untraced per-append
device costs, a durability weakening or capacity parity.

All smoke/parser failures remain, including the original one-root-per-group
assumption and an invalid 200-only check for a legitimate S3 range GET 206.
Large object/sample logs were losslessly recompressed with zstd: 202.1 MB of
gzip becomes 13.6 MB, with identical decoded SHA-256 values and original gzip
copies retained locally. The manifest is in
`cold-bench-qualification-001/recompression.json`. Reanalysis first missed the
new sample extension; `cold-bench-qualification-002` preserves that failure,
and final `003` reruns all checks successfully. The final checker reads gzip
and zstd; archived older harnesses need the zstd data decompressed back to gzip.

The increment-only evidence audit checks 3,986 files (1.57 GB decoded), including
archives and 243 strictly decoded numeric HDR histograms. Sixty-eight retired
fixture keys have zero matches in the tested encodings. Before the stock secret
scan, 5,676,377 inventory hash fields are independently recomputed from object
names and complete inventory bodies, then represented by counts in the scanner
projection; original evidence is unchanged. Gitleaks exits **1**, with 20 source
hash findings, all independently verified and none unexplained. The initial
binary-HDR decoding error, scanner result, review scripts and scope are retained
in [the audit manifest](evidence/cold-bench-qualification-003/audit-inventory.json)
and [finding review](evidence/cold-bench-qualification-003/findings-review.json).
This is not a new full-history pre-push audit. Raw stores, process-memory captures
and provider diagnostics remain private; all benchmark services are stopped.

Reproduce sequentially, with fresh output names and no competing builds/tests:

```sh
PY=.tmp/electric-tools/cold-venv/bin/python
S=experiments/electric-replication/scripts
E=experiments/electric-replication/evidence
$PY $S/cold_benchmark.py $E/cold-bench-new
$PY $S/cold_benchmark_check.py $E/cold-bench-new
$PY $S/cold_benchmark.py $E/cold-envelope-new --arms async3 --workloads many-write --connections 4 --duration 60
$PY $S/cold_generations.py $E/cold-generations-new
$PY $S/cold_generations_check.py $E/cold-generations-new
$PY $S/cold_benchmark.py $E/cold-fsync-new --workloads hot-write --repetitions 1 --duration 12 --profile fsync
$PY $S/cold_benchmark.py $E/cold-cpu-new --workloads hot-write --repetitions 1 --duration 12 --profile cpu
$PY $S/cold_benchmark.py $E/cold-sendfile-new --workloads replay --repetitions 1 --duration 12 --profile syscalls
# Recheck the specific retained ledgers above, including their expected failures:
$PY $S/cold_campaign_check.py $E/cold-audit-new
```

Fresh full hot/cold conformance after all workloads is 332/332 each, unchanged
suite with subscriptions, no skips; it still supplies compatibility rather than
forced-cold reads. No runtime change, new commit, publication, main merge,
deployment or cloud spend occurred in this campaign. Production telemetry,
security/operations and independent-host/provider/power-loss gates remain open.

### Work-driven cold scheduling and admission: local campaign complete, performance partial

The implementation and feasible local qualification are complete; **production
performance acceptance remains NOT_MET**. The [closure manifest](evidence/cold-work-qualification-001/checked.json)
rechecks sixteen ledgers, model/source/binary/suite identities, the pressure
history and seventeen additional corrupted read histories. It records **114/132
raw passes**, retaining sixteen preflight-only failures and two invalid generation
observations. Fresh entire matched reruns produce **107/107 selected passes**,
including diagnostic profiles; selection does not erase the original failures
or turn byte correctness into a capacity certificate. Figures are derived from
the named, hashed inputs in [figures.json](evidence/cold-work-qualification-001/figures.json).

The candidate retains one uploader per group, bounded fair passes and the same
64 KiB segments in before/after measurements. No upload concurrency, distributed
queue, weakened commit marker or GC certificate is introduced. The node-local
admission policy accounts for unreclaimed payload, retained unapplied WAL and
active/spent credits. Originating writes reject with 429 at their backlog bound;
replication additionally rejects with 507 at the measured disk floor, without
applying the originator's backlog cap to followers. Different group leaders can
each originate their node's budget; this is not a cluster-wide reservation or
an absolute physical-disk guarantee. See the [contract and model correspondence](CONTRACT.md#work-driven-offload-and-local-admission).

Four `ColdWork` safety/liveness configurations and seven negative controls pass
in `cold-work-formal-004`. The existing full formal suite also passes separately
in `cold-work-full-formal-001`, retaining 83 TLC mutations and the Lean control.
The new liveness assumptions and earlier failed weak-fairness model are explicit;
there is no composed/unbounded refinement proof. `cold-work-build-006` passes
150 replicated and 116 native-tier Rust tests (three existing fixture/forensic
ignores in each), plus 35 Python harness tests. The subsequent completion-observation
repair passes 36 Python tests. An actual isolated Rust mutation removing spent
credits fails the generated admission property with exit 101, after its unmodified
control passes. The minimized sequence promises 65 bytes under a 64-byte bound;
this is an implementation failure, not a compilation error or altered test
expectation. [Logs, patch and binary identities](evidence/cold-work-implementation-mutation-001/checked.json)
are retained; the shipping runtime was not mutated. Its release binary is
`3035f73be03e4600e396fdb2b9fb3e2c6ea51cb744a2d910041a074d90d46ce3`.

`cold-work-faults-001` retains the partial-tail admission stall; `002` retains
the incomplete repair which uploaded short prefixes but failed to reclaim them.
The original expanded `004` passes 195 attempts, 198 exact replica checks and five negative
histories: provider outage/recovery, full restart, eight concurrent producers,
sparse streams beside a busy stream, and actual disk-floor crossing using only
an owned 96 MiB allocation. It observes 78 expected 429s, two expected 507s and
5,898,240 client-attributed remote bytes after zero local payload. This is a
safe-overload/fault probe, not a zero-error throughput cell. The subsequent
`007` also holds an actual PUT while filling the budget: **227 attempts, 201
exact replica checks and seven negative histories pass**. Its failed predecessors
remain: `005` wrongly expected rejection to remain sticky after conservative
credits were resampled, while `006` failed to cross the disk floor because
restart GC freed space after the fixture chose it. The corrected probe verifies
stable rejection across sample intervals and adapts only its owned disposable
allocation, capped at 512 MiB; it never fakes `statvfs` or changes runtime bounds.
The existing full
real-MinIO campaign also passes on this candidate in `cold-work-runtime-001`:
752 events, 28 exact reads, one SSE feed/retry, two retained unknown HTTP outcomes
and eight rejected history mutations.

The first matched 24-second comparison, `cold-work-bench-001`, passes all 36
execution/byte/drain checks at c4 and **identical 64 KiB segments**. Hot-stream
retained local payload falls sharply, but one-member hot p99 rises from 3.425 to
5.643 ms. Many-stream one-member throughput falls from 2,119 to 1,614 appends/s,
and sampled backlog still grows. A green workload checker is not a green
offload-capacity gate.

The longer `cold-work-sustained-hot-001` completes **18/18** ninety-second cells,
three repetitions per variant/arm, with zero API errors/rejections and exact
all-replica drains. These are medians, decimal MB, c4/256-byte appends:

| 64 KiB hot-stream mode | Acks/s, before → after | p99 ms, before → after | Leader local payload MB, before → after | Payload PUTs, before → after |
|---|---:|---:|---:|---:|
| One member | 2,724 → 3,034 | 5.191 → 6.523 | 40.422 → 0.031 | 341 → 1,066 |
| Quorum three | 1,925 → 2,173 | 10.223 → 8.631 | 22.496 → 0.045 | 336 → 764 |
| Async three | 2,446 → 3,255 | 8.607 → 8.003 | 37.145 → 0.032 | 292 → 1,144 |

The candidate's median second-half local-payload slopes are +171 / +39 / −211
bytes/s, versus the baseline's +386 / +206 / +345 KB/s. Small tails oscillate;
this supports keeping up at this finite hot-stream operating point, not an
indefinite bound. Increased uploads perform previously deferred work; they are
not fewer object requests or speculative cloud-dollar savings. The lower local
retention comes with the measured one-member tail-latency cost. Async acks are
local acceptance; final committed drains and sampled progress are separate.

`cold-work-sustained-many-001` passes **6/6** three-minute cells at c1, 1,024
streams and 4,096-byte appends. Before → after medians are 525 → 562 accepts/s,
15.175 → 12.303 ms p99, 1,758 → 5,415 payload PUTs and 272.81 → 58.84 MB retained
leader payload. Second-half slope improves from +1.072 MB/s to +0.0655 MB/s but
remains positive in every candidate repetition (+0.0389 to +0.167 MB/s); one
ends at 86.38 MB. This does **not** establish sustained high-cardinality capacity.
Candidate sampled publication is 1.72–2.00 MB/s across both groups, versus
2.05–2.33 MB/s estimated eligible arrival (publication plus full/closed-tail
gauge change over the same 175–176-second sample interval; no pressure seals).
That gauge includes pending work and is not an independent HTTP throughput
counter; small partial tails and non-atomic observations remain explicit.
Proxy-observed payload HEAD/PUT/GET intervals consume 19.8–26.9% of each candidate
group's measured window (baseline 8.1–8.8%). These are not end-to-end worker spans:
they exclude consensus, scheduling, compaction, inventory and snapshot work.
The remaining time is **not** established as Raft overhead. Sparse CPU profiles
show snapshot hashing but do not isolate a blocking cost or justify concurrent
upload machinery. Phase-level attribution is the next bounded investigation,
not an assumption that more upload tasks will solve it.

The final native controls (`cold-work-native-002`) pass 6/6: 24 seconds/c4,
median hot/cold rates 4,305/4,066 acks/s and p99 3.247/6.419 ms. The separate
8 MiB-segment/c16 comparison (`cold-work-large-002`, `cold-work-large-native-002`)
passes all nine cells: one-member baseline/candidate 3,982/3,985 acks/s with
p99 10.015/9.935 ms, native 6,651 acks/s with p99 7.155 ms. Both replicated
variants perform a median two payload PUTs in 24 seconds, native four. The
native/candidate rate ratio is 1.67× at this point only; changing both segment
size and connection count is not a causal segment-size comparison. The work
budget disallows 8 MiB high-cardinality prefill on this disk, rather than silently
shrinking only the candidate's workload.

`cold-work-reads-001` passes 18/18 matched replay/fanout/mixed cells, three
repetitions per variant. Replay and mixed reads fetch actual client-attributed
object bodies after verified local reclamation. Pure ds-bench fanout starts at
now; six separate 16-reader probes check exact remote catch-up, live suffix and
closure. Replay median rate is effectively unchanged (37.375→37.458 MiB/s),
but p99 increases 146.943→163.839 ms. Resource/object observations use outer
invocation windows because the pinned client lacks exact monotonic drive
boundaries for these workloads; no invented per-operation attribution follows.

`cold-work-generations-002` passes **8/8**, two matched repetitions each of
baseline/candidate quorum/async, with 48 generations and 244–246 seconds per
cell. It is the independent paced 64-appends/s probe, **not ds-bench**. Each cell
checks 72 MiB of client remote reads; all eight return to zero payload objects,
zero pending Begins/pins and four physical inventory roots. Candidate peak
pending descriptors are at most two per group. Its unpinned GC p99 is
3.55–3.71 seconds, compared with baseline 3.97–4.15; the eight seven-second
reader holds delay deletion to 7.53–7.82 seconds. All 96 history mutations are
rejected, including absence without observed DELETE completion.

Storage reclamation does not prove a memory bound: candidate aggregate RSS
starts at 103–105 MiB and ends at 159–186 MiB; three of four second-half slopes
remain positive (13–29 KB/s). One negative slope is not a plateau or a leak
diagnosis. Original `cold-work-generations-001` remains 6/8: one +83.187-second
wall-clock discontinuity and one collection observation 2.628 ms before DELETE
completion. The repaired harness waits for absence **and** completed deletion,
including partially written event records; it does not relax the checker.

The first matched many-stream ptrace capture has zero measured payload PUTs;
tracing suppressed eligibility, so it cannot diagnose active offload overhead.
The final active-offload pair (`cold-work-profile-fsync-003`) observes 55→89 PUTs
and 7,835/7,610 completed successful sync calls across the whole capture, with
no failed/incomplete/unmatched calls. Throughput is perturbed downward
(1,130→967 acks/s, p99 129.855→152.447 ms); it is **not** capacity or a causal
untraced latency result. Sixteen failed preflight cells remain separate: the
unchanged 8 GiB floor stopped them before any SUT started. Only a hash-verified
disposable scanner projection was reclaimed; original evidence/failed stores
and the disk floor were preserved.

`cold-work-conformance-hot-001` and `cold-work-conform-cold-002` each execute
the unchanged subscriptions-enabled **332/332**, zero failures/skips/todo, on
three processes/two partitions. The latter is cold-enabled compatibility, not
forced-cold coverage of every test: 141 publications but zero HTTP cold bodies.
Actual reclaimed remote reads are qualified by the dedicated histories above.
The earlier cold conformance launch with an
overlong service name is retained as `cold-work-conformance-cold-001`; it started
zero tests and is not a pass. All campaigns ran sequentially, without competing
builds/profiles. Raw attempts, statuses, configs, client/source/binary hashes,
resource/object observations and original failures remain in the evidence tree.

The decoded **increment**, not a fresh full-history publication audit, checks
6,893 files/4.77 GB, 357 numeric HDR histograms and 68 retired fixture keys with
zero tested key-encoding matches. Every one of 22,216,699 inventory key-hash
fields is independently verified before count-projection for the stock scanner.
Gitleaks exits **1**, not zero: all 70 findings are independently verified
hashes (69 source, one public object name), with zero unexplained findings in
[the retained review](evidence/cold-work-qualification-001/findings-review.json).
Its initial size guard and scanner-vocabulary false positive remain recorded.
Lossless recompression reduces 98 JSONL files from 656.0 to 37.8 MB, retaining
the original gzip files locally and verifying identical decoded SHA-256 values;
the largest campaign's recheck produces an identical report. No history was
filtered to make the audit or workload ledger pass.

Reproduce sequentially with the pinned tools and fresh output names:

```sh
PY=.tmp/electric-tools/cold-venv/bin/python
S=experiments/electric-replication/scripts
E=experiments/electric-replication/evidence
bash $S/cold_work_formal.sh $E/cold-work-formal-new
$PY $S/cold_work_faults.py $E/cold-work-faults-new
$PY $S/cold_work_benchmark.py $E/cold-work-bench-new
$PY $S/cold_work_benchmark.py $E/cold-work-hot-new --workloads hot-write --duration 90
$PY $S/cold_work_benchmark.py $E/cold-work-many-new --arms async3 --workloads many-write --connections 1 --duration 180
$PY $S/cold_work_benchmark.py $E/cold-work-reads-new --arms quorum3 --workloads replay fanout mixed
$PY $S/cold_work_generations.py $E/cold-work-generations-new --generations 48 --period 5 --repetitions 2
$PY $S/cold_generations_check.py $E/cold-work-generations-new
$PY $S/cold_work_summary.py $E/cold-work-hot-new
# Offline closure of the named retained ledgers, not a new workload:
PYTHONPATH=$S $PY $E/cold-work-qualification-001/verify.py.txt $E/cold-work-check-new
```

The remaining priorities are (1) attribute the high-cardinality phase costs and
RSS growth, then qualify a declared sustainable rate without hiding rejection;
(2) production metrics/events, auth/TLS/tenant limits and recovery/upgrade
runbooks with fault/property coverage; (3) independently hosted disk/provider
failure and power-loss qualification, subject to a separate bounded resource
approval. Do not add upload concurrency or weaken durability based on these
profiles. This is uncommitted local work above the audited cold checkpoint;
no new commit, push, merge, deployment or cloud spend, and no signing-default
change, is included in this milestone.

### Earlier hot-only performance checkpoints

The earlier hot-stream matrix (`openraft010-async-002`, zero warmup) measures
**51.8–61.2k/s one member versus 87.4–102.3k/s native**, a paired **1.67–1.74×**
gap. All strong cells pass; all six async cells fail the unchanged zero-rejection
gate, with accepted bytes nevertheless draining exactly. A separate four-connection
campaign (`openraft010-envelope-c4-001`) passes all **12/12 sixty-second cells**;
async accepts 4.18–5.01k/s without errors/rejections. That is an operating point,
not maximum sustainable capacity. No admission bound or durability was relaxed.

The 1,024-stream matrix (`openraft010-many-001`) passes **12/12 thirty-second
cells**: native 39.6–45.0k/s, one member 38.2–51.9k/s, quorum-three 19.0–19.3k/s,
async 19.7–20.1k/s. Async does not approach one-member throughput here. Its
111–113 ms p99 and 215–216 MiB aggregate RSS need diagnosis. Exhaustive post-run
drain includes thousands of HEAD requests; it is not pure replication latency.
Heap profiles separately attribute 26.80 decimal MB per current process to stock
`base2histogram` startup tables, including with OpenRaft runtime-stats disabled.
That fixed allocation explains the startup regression versus 0.9, not the extra
async-versus-quorum memory or a leak. See [BATCHING.md](BATCHING.md) for evidence
and remaining limits; no private histogram/consensus fork was added.

The corrected read matrix (`openraft010-reads-002`) passes **60/60 execution
checks**, with stable sampled clocks and twelve separate finite 1,000-reader
sequence checks. Mixed writes measure native 19.5–20.5k/s, one member 11.5–12.1k/s,
quorum 6.20–6.63k/s and async 6.66–8.20k accepts/s. Paced fanout frame counters are
not per-reader sequence proof or capacity. Follow-on syscall profiling retains
an analyzer failure caused by interleaved diagnostic logs; it does not resolve
the untraced p99. Allocation profiles implicate bounded receipt results in the
extra async heap, without changing retention or establishing a leak.

The later PID-scoped kernel tracepoint campaign (`kernel-fsync-many-001`) passes
all four workload/export checks and reproduces the quorum tail: 112.7 ms append
p99 with 460 fsync/fdatasync calls taking at least 100 ms. Native also has 32 such
calls. These are whole-capture syscall intervals including setup/drain, not pure
device latency or unperturbed capacity. The bounded profiler exports no unused
register arguments; raw captures stay local-only. [BATCHING.md](BATCHING.md) has
the counts and generated parser/redaction checks. Disk versus descheduling and
the separate occasional lease rejection remain unresolved.

Opt-in `RAFT_WRITE_OUTCOMES` schema-1 counters distinguish count/byte backlog
limits, epoch/lease/forward rejection, locally durable/applied command stages,
and unknown post-assignment outcomes. They contain fixed labels, not request
data; overlapping stages and internal commands are not HTTP totals. The fault
checker tests real count/byte/lease rejection and rejects misclassified evidence.
Benchmark envelope controls include concurrency, stream count and zero warmup:
the pinned write pool is closed-loop (ignores `rate-per-stream`), combines
429/503, drops rejected attempts and excludes warmup errors. Progress samples
cover at most 16 explicitly declared streams without extrapolation; final drain
still checks every stream on every replica. These are diagnostic instruments,
not completed production observability or proof of sustained capacity.

`snapshot-progress-002/003` isolates a scheduling bug and its fix with an actual
two-second snapshot fsync delay. Before worker handoff, no unrelated-partition
operations complete during the delay and leadership changes; afterwards, 38
complete with no unknown outcomes or term change. The exclusive snapshot view
and durable publication order are unchanged. Apply in that group still pauses,
so this is not proof of a zero-rejection async load envelope. The snapshot change
passes the tests/formal/conformance above and fork/storage process reruns.

Direct metadata capture subsequently removes redundant hot-sidecar fsyncs without
changing the archive's durability barriers. `snapshot-cost-001` measures three
untraced 1,024-stream snapshots at **709–1,139 ms before versus 27–35 ms after**;
separate traces show 1,024 sidecar fsyncs versus zero. All eight cells survive
restart with exact bytes/offsets and producer retry state. Generated snapshot-only
restore and conformance/fault reruns pass. This isolated cost reduction does not
remove the hot-payload copy pause or qualify sustained async capacity.

The next pinned-cut change releases apply during payload copy/hash/fsync while
retaining a separate build/install/publication/cleanup lock. TLA+ precedes the
implementation (865 safety states, 60 liveness states, six negative mutations);
generated real-file tests restore the original cut despite later appends,
receipts and unlink/recreation. Same-group delayed-fsync histories improve from
zero completed operations / six unknowns to 38 completions / zero unknowns, no
term change and exact restart recovery. `snapshot_cut` records the remaining
metadata-capture pause. The conformance and fault counts above cover this change;
sustained performance and memory qualification remain separate gates.

The completed pinned-cut comparison (`snapshot-cut-envelope-001`) retains **17
PASS / 4 FAIL**, all stable sampled clocks. One member is 8.93–9.48k/s versus
native 15.84–17.31k/s at 16 connections, a paired 1.71–1.83× gap; all synchronous
cells pass. Async improves from three failing repetitions (1,723–2,199 rejects)
to two passing and one failing (16 lease-expired requests). No current async
cell has a count-bound reject; exact accepted-byte drain passes everywhere.
Leader apply-lock maxima fall from hundreds of milliseconds to below 11 ms, while
snapshot copy/fsync continues concurrently. The metadata copy costs memory:
aggregate async RSS rises from 129–131 to 147–148 MiB. This is a qualified
scheduling fix, **not** a zero-error 90-second envelope or broad capacity claim.

The first matched stock-0.10 comparison, `openraft010-writes-001`, has three
30-second repetitions with identical local-fsync contracts, 256-byte payloads and
256 connections. Native measures **91.6–100.4k/s**, qualified 0.9 one member
**34.4–40.0k/s**, and initial 0.10 one member **3.4–10.5k/s**. All nine workload
checks pass; the performance gate fails. WAL fsyncs/ack rise from about **0.049**
on 0.9 to **0.46 / 0.82 / 1.68** on 0.10 as command batches collapse. Candidate
CPU is only 0.80–0.84 cores. Separate `openraft010-cpu-001` profiles are retained;
they do not establish the cause of blocked time. The 0.10 builder await is
enqueue-only, verified against pinned upstream; no performance improvement is
inferred from its nonblocking storage API. Dispatch/apply interaction remains
under investigation. The experiment has not switched its accepted baseline.

Reducing locally-unflushed dispatch slots from two to one restores batching
without a timer or weaker durability. `openraft010-flight1-001` measures
**47.4–59.8k/s** against the original candidate's **3.4–3.5k/s**, with all nine
cells passing exact-byte and zero-error checks. WAL fsyncs/ack drop to
**0.033–0.034**, from 1.67–1.70. Native varies 70.6–101.8k/s in that matrix;
this is evidence for the scheduling fix, not broad parity. The latest
conformance/formal/fault reruns above include this change.

The broader `openraft010-async-001` matrix retains **14 passing and seven failing
cells**, with a nonzero driver exit. Native measures **94.3–100.7k/s**; current
0.10 one member **52.3–60.7k/s** versus 0.9 **34.1–45.3k/s**, all zero-error with
exact bytes. Current quorum measures 36.6–41.6k/s, but one repetition rejects
2,864 requests and changes term during snapshots, so the quorum gate fails.
Every async repetition rejects load: 7.1–7.9k attempts on 0.10, 90.8–101.7k on
0.9. Accepted bytes drain exactly to every replica; these are overload runs, not
sustainable zero-error async capacity. The pinned client combines 429 and 503;
per-cause load diagnosis remains open; the controlled snapshot-delay result
above does not make these earlier load failures pass.

The matched `async-writes-006` matrix has complete resource samples: native
**77–84k/s**, one member **39–46k/s** (**1.84–1.96× slower**), and quorum-three
**24–25k/s**. All three async cells fail the unchanged zero-backpressure gate:
28–29k acceptances/s and 81–94k rejected attempts per 30-second window, despite
every accepted byte draining exactly to every replica. Neither this nor the
earlier 53–58k/s short one-member runs establishes performance parity.
`async-writes-005` remains invalid after ENOSPC truncated a resource sample; its
failure and the unchanged analyzer's rejection are retained. A harness headroom
guard now runs before each cell; it is not a product disk-size requirement.

[BATCHING.md](BATCHING.md) records the measured fsync-amortization defect behind
the original roughly 30× gap, its batching fix, and later profiling experiments.
One-member WAL fsyncs/ack remain about twice native; snapshots also pause apply.
Allocation projection saves measured allocations but does not establish a
throughput gain. Raising the command ceiling and inlining the durability waiter
likewise showed no established benefit and were reverted. A 100 µs fsync
collection interval improved grouping but measured only 32–33k/s one-member;
it too was rejected. The preserved 0.9 engine/vendor source hashes and release binary
match `conformance-021` exactly, and its full suite was rerun as 022. Raw CPU memory captures
stay local-only; symbolized profiles, allocation traces, failed histories and
exact source/binary/config hashes remain in the evidence ledger.

The broader **pre-batching** `bench-local-004` completes all **45/45** matched ds-bench cells
after arrival-fenced read coalescing. The prior `bench-local-003` three-replica,
1,000-subscriber failure (503s and a 240-second timeout) is retained. One 004
write cell lost its closing resource sample to a `/proc` permission race; the
driver traceback is retained and resource coverage is explicitly partial.

| Pre-batching local qualification cell | Unmodified Electric | One-member adapter | Three replicas |
| --- | ---: | ---: | ---: |
| One stream, concurrency 256 (writes/s) | 105,877 | 3,242 | 4,200 |
| 1,024 streams, concurrency 256 (writes/s) | 59,638 | 3,579 | 4,242 |
| Seeded 4 MiB replay (GiB/s) | 17.61 | 17.53 | 15.67 |
| Mixed, unpaced writes and fixed-rate reads (writes/s) | 26,922 | 6,625 | 4,908 |

These short single runs share a disk/page cache and 16 GiB host memory. Each arm
gets four aggregate SUT CPU affinities, the client another four; the three-node
arm does not get three times the CPU budget. Native and one-member writes are
local-fsync; three-node writes are quorum-fsync on this **shared host**. The client
is not independently calibrated and the native write ladder has not plateaued.
Replay is page-cache/loopback throughput, not disk bandwidth; different runs vary
substantially. Paced fanout is not saturation capacity. Profiles are separate,
perturbed executions: all read arms call real `sendfile`; write profiles show
substantial fsync and scheduling costs. The adapter has a large measured write
gap, not Electric performance parity. Independent-host capacity, sustained memory
and disk growth, tail latency, and overload behavior remain evaluation gates.

For the hot-stream concurrency-256 cells, sampled aggregate SUT CPU is
1.78 / 0.49 / 0.73 cores and peak sampled RSS is 6.8 / 13.6 / 30.9 MiB
(native / one-member / three replicas). RSS excludes shared page cache. The
three-replica outbound socket counter delta is at least 30.8 MiB over its sampled
write window, not a complete packet capture. `scripts/benchmark_summary.py`
recomputes `summary.json` from raw samples: writes use exact client measure
timestamps; other modes lack them and explicitly use the outer client invocation.
The low write CPU alongside fsync/scheduling profiles points to serialized
coordination, not exhausted CPU; profiles are evidence, not a complete causal
decomposition or a claim that tuning one knob will close the gap.

The pinned fanout client does **not** guarantee full drain. All readers obtain
HTTP headers before writing starts, but begin body polling after that barrier.
It counts every completed append attempt as sent, and only complete parseable
data frames as received, without per-reader sequence/dedup accounting. Readers
stop at the nominal deadline plus two seconds; a boundary append can be issued
after the deadline check and complete later. Its bounded task join does not
extend that window. Partial frames are discarded; body EOF/error, join errors
and join timeout need not increment its subscriber-error count. Thus the 004
1,000-reader fractions (99.97% / 99.90% / 99.88%) are windowed observations, not
proof of loss or complete delivery. See the pinned
[read/write boundaries](https://github.com/electric-sql/ds-bench/blob/93a1a066a511ad2ce5114dc429afb1fd0f6d99bf/ds-bench/src/fanout.rs#L146-L303).
The separate `fanout-drain-001` correctness probe keeps full sequence ledgers:
**all 1,000 replicated readers received exactly records 0–99**, with payload
checks and no accepted EOF, timeout, duplicate or gap. This finite JSON fixture
is not a throughput measurement or proof about every omitted benchmark frame.

No throughput or availability promise is made from blog numbers or single-orb
runs. Paid evaluation needs separate authorization; qualified publication and
trunk integration do not imply a cloud budget or deployment permission.

Physical WAL reclamation now uses a checksummed metadata-only checkpoint; old
snapshot cleanup preserves open transfer descriptors. The new real-process
campaign injects checkpoint write/fsync/directory-fsync errors before reclaim,
then verifies physical deletion and complete restart against 38,010,880 payload bytes.
The 004 measurements above include this lifecycle change.

At that earlier 004 milestone, cold-tier ownership/GC was unimplemented; the
current local qualification is described above. Catalog repair scans and
subscription/fork cardinality need scale qualification. Snapshot metadata capture
pauses one group's apply; pinned-descriptor copy/hash/fsync runs outside that
lock, with its measured memory cost retained above. Native
JSON sub-offset resolution now scans bounded 64 KiB windows with lexical state
across chunk boundaries; generated native-WAL/snapshot/restart cases cover nested
values, quoted commas, escapes and large values. Pure Lean chunk-composition
lemmas do not prove the Rust scanner or native file reader correct.
