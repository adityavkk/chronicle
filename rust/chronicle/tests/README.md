# Chronicle history workload

`history.py` is a Python-stdlib-only, Jepsen-style black-box workload. It does not
claim to be Jepsen and the synthetic unit fixtures are **checker tests, not a
distributed-system success result**.

With one port-forward per pod (for example ports 18080–18082), run:

```sh
python3 tests/history.py run --seed 20261003 --output history.jsonl \
  --url http://127.0.0.1:18080 --url http://127.0.0.1:18081 \
  --url http://127.0.0.1:18082 --producers 8 --readers 4 --operations 500
python3 tests/history.py check history.jsonl
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

Preserve every failing history and its seed. Records are exactly 96 ASCII bytes,
newline included, and uniquely encode seed, producer, and sequence. PUT creates an
empty octet stream; POST supplies `producer-id`, epoch 0, and sequence; GET is strict
unless the event is explicitly `stale-read`. `--stale-fraction` reads are recorded but
excluded from safety checks.

## JSONL contract and checks

Every request has an `invoke` followed by `ok`, `fail`, or `unknown`, sharing
`process`, `id`, and `f`; each has driver-monotonic `time_ns`. Completion `value` contains
HTTP status, latency, stream response headers, and decoded `records` for reads.
`info/run` records the representative payload/concurrency configuration (performance
labels, **not an SLO**). `info/record` associates a logical append with its record,
terminal outcome, and attempt count. Nemesis phase commands and bounded output are
also `info` plus `ok`/`fail` events.

The independent checks are: duplicate records, all strict reads forming committed
prefixes, retention of every acknowledged append in later strict reads, and real-time
ordering constraints. An `unknown` append may be absent or present; if observed it is
ordered like any other committed record. This is a purpose-built register/log checker,
not a general exhaustive Knossos search; it cannot establish Raft internals, fsync
behavior, or safety beyond observed responses.

## Nemeses

Choices are `leader-kill`, `minority-partition`, `majority-partition`, `drop-delay`,
`snapshot-crash`, `node-join`, and `node-drain`. The first, snapshot crash, and drain have k3d/
kubectl defaults targeting namespace `chronicle`; review them before use. Network
topology and traffic-control differ by k3d setup, so partition/drop-delay scenarios
deliberately require injectable commands:

```sh
--nemesis minority-partition \
--hook 'minority-partition:start=YOUR_KUBE_OR_DOCKER_NETWORK_COMMAND' \
--hook 'minority-partition:heal=YOUR_HEAL_COMMAND'
```

Hooks run through the shell, inherit `KUBECONFIG`, are logged, and a failed hook aborts
the run rather than silently reporting a clean test. `snapshot-crash` requests group 1
then kills its process. `node-join` requires a `node-join:start` hook because provisioning
and addressing are environment-specific; `/admin/register` expects JSON `[id,node]`.
The built-in `node-drain` exercises Kubernetes drain/uncordon, not Raft membership
removal. The workload does not alter a cluster unless `run --nemesis ...` is selected.

For packet-loss tests use `bash ops/partition.sh isolate N` and `heal N`, where N
is a k3d agent suffix. This modifies only a named iptables chain inside that
disposable node. Docker network disconnect is **not** an equivalent nemesis: it
destroys the node's VXLAN interface, and reconnect alone may not heal pod routing.
Always verify convergence after healing, not just Ready status.

`retirement_partition.py` is a combined hook for the existing history driver.
It requires the three seeds eligible and a verified drained non-seed replica.
It resumes that replica, waits for all three data-group assignments, isolates its
agent, requests draining once, checks voter repair with retirement still false,
heals in `finally`, then requires verified retirement. For example, on the private
five-replica rig, use a fresh history path/output:

```sh
python3 tests/history.py run --url "$PRIVATE_CHRONICLE_URL" --seed 530004 \
  --path retirement-join-1 --producers 2 --readers 1 --operations 360 \
  --append-interval .25 --read-interval 1 --timeout 5 --retries 20 \
  --retry-interval .25 --nemesis node-join --nemesis-delay 4 --hook-timeout 400 \
  --hook 'node-join:start=python3 tests/retirement_partition.py --node 5 --output partition-events.jsonl' \
  --output partition-history.jsonl
```

The `node-join` hook slot runs this whole sequence; the separate events record the
actual partition and repair phases. This is not a mid-catch-up or mid-snapshot
failure test. The helper never retries an ambiguous administrative mutation.

## Offline Porcupine subset

From the repository root:

```sh
go -C rust/chronicle/tests/checker run . -rust-history ../../evidence/baseline.jsonl \
  -rust-history-timeout 30s
go -C rust/chronicle/tests/checker test -race ./...
```

This standalone module reuses the repository's pinned Porcupine version, not the
Go/Redis checker's models or CLI. Root Go commands do not traverse this module.
Historical evidence records the old `jepsen/checker` invocation before relocation;
use the commands above to recheck it with the unchanged experimental model.

Only `Ok` exits successfully. `Illegal`, malformed/unsupported input, and `Unknown`
(search timeout) fail closed. The retained original leader-kill history is Illegal;
the post-lock leader-kill and guarded-join histories are Unknown at 30 seconds.
Their successful smoke/retention checks do not override that verdict. Unknown
mutations remain pending through history end, with either no effect or a later
effect; their transport timeout is not a commit deadline.

Schema 1 is this generator's one-incarnation octet-stream workload. Schema 2 adds
explicit operation fields in each invoke's `value`: `tenant`, `path`, `data` (UTF-8),
`incarnation`, optional `producer`/`epoch`/`seq`, `close`, and for create
`content_type`/`expected_incarnation`. Completions carry `end`, `incarnation`,
`duplicate`, `error`, and read `data`/`closed`. It models create/append/delete/full
strict read, partitioning by tenant/path across incarnations. It does not model
TTL clock expiry, JSON wire transformation, range/streaming reads, placement,
capacity limits, or physical fsync. The v2 synthetic cases test the checker,
not the server. `lifecycle.py` additionally executes 21 schema-2 operations on
the real HTTP cluster: gap/fill/retry, original duplicate frontier, mismatched
duplicate payload, epoch takeover/fencing, close, delete and recreation.
`evidence/lifecycle-k3d.jsonl` received Porcupine `Ok`. This is sequential
contract coverage, not a concurrent fault history. Schema/API coverage remains partial.

```sh
python3 tests/lifecycle.py --url "$PRIVATE_CHRONICLE_URL" \
  --path a-new-unused-path --output lifecycle.jsonl
# From the repository root:
go -C rust/chronicle/tests/checker run . -rust-history ../../lifecycle.jsonl
```

`tests/openraft_storage.rs` runs the pinned upstream `Suite::test_all` with a
fresh SQLite database per case and awaits actor shutdown before removing files.
The suite checks logical storage contracts; it does not prove physical flush or
crash safety. The separate VFS and process-ownership tests address narrower
persistence failures explicitly.

## Storage fault boundary

`make fault-check` runs the isolated `tests/vfs` package against the same bundled
SQLite ABI as production. Its default VFS delegates to the local Unix VFS except
for filename-scoped one-shot `SQLITE_IOERR_WRITE` and `SQLITE_IOERR_FSYNC` returns.
The sole test serializes global registration/target changes and closes all actors
before retargeting. Unsafe FFI is isolated from the safe-Rust production crate.
Failure must actually fire, apply must return an error, and cached state must not
advance. Reopen must retain earlier acknowledged state; the failed operation may
be absent or fully present because its durable outcome is unknown. This is not
power-loss/torn-write simulation, and it does not yet check every log-flush callback
or snapshot/membership persistence boundary.

`make fault-check` also builds the optional `storage-faults` feature and runs
`tests/fault_gates.rs`. Normal builds contain no gates. A test binary reads
`CHRONICLE_FAULT_DIR`; within the hex-encoded store-filename directory, the
harness creates `<gate>.arm`, waits for `<gate>.reached`, then creates
`<gate>.release` or kills the process. The blocking storage actor never removes
these files or ownership locks. Use a fresh control directory for each run.
Alternatively `<gate>.error` writes the reached marker and returns an injected
I/O error without waiting. This persistent control remains active across restart
until the external harness removes it. It is an application storage boundary,
not an SQLite VFS or power-loss simulation. `failure_tests.rs` checks that actual
Raft storage errors terminate the process despite a gated blocking body reader.
The [live k3d fail-stop history](../evidence/FAIL-STOP.md) uses
`tests/fatal_storage.py` and retains same-PVC recovery plus Porcupine evidence.

The four gates are `after-log-commit-before-log-flushed`,
`after-apply-commit-before-return`, `before-snapshot-install-transaction`, and
`after-snapshot-install-transaction`. Subprocess tests kill at each boundary,
reopen exclusively, and check exact log/data and old-or-new snapshot state.
`evidence/fault-gates-pvc.txt` retains the same suite executed with `TMPDIR=/data`
inside the real k3d pod. This exercises the supported PVC filesystem, not a live
Raft membership change or a mid-transaction disk/power failure. The separate VFS
suite tests selected actual write/sync errors within SQLite transactions.

`tests/gated_history.py` is the bounded external counterpart for the disposable
`k3d-chronicle-rust` cluster and `chronicle` namespace. Deploy a binary built with
`storage-faults`, set `CHRONICLE_STORAGE_FAULTS=1` and `CHRONICLE_FAULT_DIR` to
the same path within `/data` on every Chronicle container, and supply a private URL
and a genuinely unused path:

```bash
python3 tests/gated_history.py \
  --url "$PRIVATE_CHRONICLE_URL" --tenant gated-history \
  --path "gate-$USER-$(date +%s)" --seed 42 \
  --gate after-log-commit-before-log-flushed \
  --fault-root /data/fault-controls --output gated-log.jsonl
```

The other supported gate is `after-apply-commit-before-return`. The harness
refuses any other Kubernetes context, validates both marker environment
variables before touching a pod, discovers the data-group leader through the
Kubernetes pod proxy, and uses only `ops/kubectl.sh`. It creates/removes only its
known `.arm`, `.reached`, and `.release` controls. The environment marker is an
operator assertion; actually reaching the gate checks the instrumented behavior.
Never run two drivers against the same gate directory. It pauses identical seq-1
requests, deletes exactly the gated pod with a one-second grace period, waits for
replacement readiness and a strict read, retries seq 1, and checks the retained
schema-1 history with `tests/history.py`. A failure history is retained at
`--output`; it fails if the gate is not reached or either paused request reports
success during the one-second observation before pod deletion (a changed schedule
or legitimate failover must be investigated before calling that a safety defect).
After the committed-apply gate, retry must return the retained duplicate result;
the log-only gate permits either outcome. Existing output files are never overwritten.
Cleanup leaves `.release` present so a blocked
actor can observe it. This is pod termination at a known persistence boundary,
not SIGKILL, power-loss, torn-write, or a namespace/PVC lifecycle test.

`evidence/gated-{log,apply}-k3d.jsonl` retain actual executions on the four-pod
cluster (three voters, one drained ingress). Both received Porcupine `Ok`.
Both requests were still pending during the pause and returned unknown after
pod termination. The log-gated retry returned 200; the apply-gated retry returned
204 with the original frontier. Each final strict read contained exactly two
records. This distinguishes unknown outcomes; it does not enumerate every schedule.

## Withheld-volume qualification

`lost_volume.py` is a destructive **disposable-cluster-only** history hook. It
requires stable voters `[1,2,3]` in all five groups, a verified drained spare 4,
one Chronicle pod per agent, and the exact guarded local PVC layout. It stops
replica 3's agent before changing its volume. The original data is moved aside,
not copied into a concurrently running identity. A normal (non-fault-feature)
release is sufficient. Run only one fault driver at a time.

Use a fresh history/path/seed and run `history.py` with:

```sh
--nemesis volume-loss --nemesis-delay 5 --hook-timeout 300 \
--hook 'volume-loss:start=exec python3 tests/lost_volume.py inject --run NEW_RUN --output NEW_EVENTS.jsonl --confirm-disposable-data-loss chronicle-rust'
```

The recorded four-shard run used two producers and one strict reader per shard,
384 appends per producer, `--append-interval .2 --read-interval .3 --timeout 10
--retries 10 --retry-interval .3`. Only one history owns the injection hook; the
other three run concurrently without a second nemesis. Choose paths mapping to
all four shards with the existing `gated_history.group_for` function. See
`evidence/volume-loss-64040/paths.tsv` and each history's initial run record.

Check every history with the existing Go Porcupine adapter **before restoration**.
The hook intentionally leaves replica 3 unready and data withheld. It enables an
existing spare, waits for automatic replacement, and marks the lost identity
draining; it does not provision or reuse an identity. After validation, restore
the original volume as a separate cleanup step:

```sh
python3 tests/lost_volume.py restore --run SAME_RUN --output NEW_RESTORE_EVENTS.jsonl \
  --confirm-disposable-data-loss chronicle-rust
```

Restore refuses a non-quarantined identity or unexpected new files, and does not
restore eligibility. If interrupted, inspect events, Docker state and both
directories before acting; never repeat a possibly completed filesystem mutation
blindly. Wait for verified retirement before any subsequent operator eligibility
change. The [retained evidence](../evidence/FAIL-STOP.md#withheld-volume-replacement-under-concurrent-load)
distinguishes simulated volume loss, post-restart equality and cleanup from
physical disk/power-loss guarantees.

`leader_drain.py` exercises autonomous spare placement, live control/data leader
drain and baseline restoration without any process restart. See
[the complete schedule and interrupted-client results](../evidence/LEADER-DRAIN.md).
`kill_leader.py` uses containerd SIGKILL and verifies exit 137 plus same-PVC
container replacement. Use its explicit history hook rather than assuming
`kubectl delete pod` kills immediately; see [the retained comparison](../evidence/RETRY-BACKOFF.md).
