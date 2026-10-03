# Terminal storage failures terminate the shared node

A fatal local control group could leave healthy local data-group leaders alive
but unable to reconcile: the controller's local control read barrier failed,
while constant-success health probes still advertised the node as eligible.
All five groups share a node/PVC, so the binary now exits on any group's typed
OpenRaft `Fatal::StorageError`. Quorum loss, elections, ordinary application
errors, uninitialized learners and other `Fatal` variants are not this trigger.
It is not a detector for storage calls that hang without publishing an error.

The pre-implementation formal mapping is in `formal/README.md`: this policy
implements the existing crash abstraction, without changing persistent state or
introducing a consensus authority. No new end-to-end proof follows from it.
`make formal` passed again (`fail-stop-formal.txt`). Existing retained negative
counterexamples remain the earlier runs; regenerating them also passed.

## Deterministic regression and independent review

`make check` passed (`fail-stop-check.txt`). `failure_tests.rs` uses two actual
Raft/SQLite groups and the production watcher. After acknowledged writes, it
blocks an actual response-file task and injects a log-storage I/O error, separately
in group 0 and group 1. Both children exit 1 before the body gate is released;
reopening both stores retains the acknowledged prefix. The interrupted write is
unknown, not asserted absent. This checks store reopening, not a complete Raft
restart. The storage append return and `LogFlushed` callback both carry the error;
it is not an isolated callback-only test.

A temporary mutation that returned from the watcher instead of exiting failed
at the 15-second deadline (`fail-stop-negative.txt`). Restoring exit passes
(`fail-stop-restored.txt`). The scoped oracle review found no correctness blocker
in the typed predicate, watch initial/change/closure handling, runtime exit or
test evidence. Logging is best effort: termination never waits for an exporter,
log flush or Tokio destruction, which can hang behind a blocking file task.

## Actual k3d/PVC evidence

The local image was built from source commit
`6393655db6d9f1c164e696dcd25c006b74f3b261`.
`fail-stop-image.json` records Docker's image ID and
`fail-stop-deployed-pods.json` records the runtime image identity. All five old
pods stopped before the uniform-version upgrade; their PVCs were preserved.

`fatal_storage.py` ran as the external nemesis for `history.py`, using two
producers, two strict readers, 96-byte records and 250-ms append pacing. Its
target led control group 0 and all data groups before injection. An auxiliary
strict read entered the actual file-body gate. The harness then injected an
I/O error after control-log SQLite commit and before successful flush reporting.
The exact original container terminated with exit 1 before the body gate was
released. No signal was sent and no pod was deleted. Kubernetes restarted it
in the same Pod UID/PVC; the identity checksum was unchanged. The previous log
contains both the core storage error and binary fail-stop event.

`fail-stop-live3.jsonl` retained all 480 records. There were six unknown append
attempts (including retries) and two unknown reads; 63 strict reads succeeded.
The smoke/prefix checker passed, and the independent Porcupine command returned
`Ok` (`fail-stop-live3-porcupine.txt`):

```sh
go run ../../jepsen/checker -rust-history evidence/fail-stop-live3.jsonl \
  -rust-history-timeout 30s
```

Two earlier attempted schedules are deliberately retained as failures. The first
preflight required drained replicas' stale leader hints to match current voters;
the second could not traverse the host's root-only k3s storage ancestors as UID
1000. Neither injected a fault. The corrected harness chooses current placement
voters and uses host-root access, changing ownership only on disposable fault
control directories. It never changes database, identity or lock ownership.

`fail-stop-live3-events.jsonl` records the actual gate, container and recovery
observations. `fail-stop-remaining-controls.txt` is empty; five pods were ready in
`fail-stop-final-pods.json`. This tests process self-exit and recovery on the local
PVC filesystem, not physical disk failure, power loss or AZ isolation. Persistent
storage failure followed by automatic spare replacement remains a separate
unqualified schedule. Full conformance, resource-informed leadership balancing
and equal-semantics performance remain outstanding; campaigns remain default-off.

To repeat, run `history.py` with an unused history/path/seed and:

```sh
--nemesis node-join --nemesis-delay 3 --hook-timeout 300 \
--hook 'node-join:start=exec python3 tests/fatal_storage.py --url "$PRIVATE_URL" --path UNUSED_AUX_PATH --output NEW_EVENTS.jsonl'
```

This reuses the generic one-shot hook label, not a node-join operation. The hook
requires a control leader also leading a data group, test gates enabled under
`/data/faults`, and the guarded disposable cluster. On harness interruption,
inspect retained controls and pod state before reuse; Python cleanup cannot run
after SIGKILL. Do not expose the unauthenticated admin/Raft endpoints publicly.
