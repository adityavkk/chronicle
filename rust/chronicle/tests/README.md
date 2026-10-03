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

## Offline Porcupine subset

From the repository root:

```sh
go run ./jepsen/checker -rust-history rust/chronicle/evidence/baseline.jsonl \
  -rust-history-timeout 30s
go test ./jepsen/checker
```

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
go run ./jepsen/checker -rust-history rust/chronicle/lifecycle.jsonl
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

The four gates are `after-log-commit-before-log-flushed`,
`after-apply-commit-before-return`, `before-snapshot-install-transaction`, and
`after-snapshot-install-transaction`. Subprocess tests kill at each boundary,
reopen exclusively, and check exact log/data and old-or-new snapshot state.
`evidence/fault-gates-pvc.txt` retains the same suite executed with `TMPDIR=/data`
inside the real k3d pod. This exercises the supported PVC filesystem, not a live
Raft membership change or a mid-transaction disk/power failure. The separate VFS
suite tests selected actual write/sync errors within SQLite transactions.
