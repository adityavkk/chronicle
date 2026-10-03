# Pending placement repair while learner persistence is blocked

Previously, any incomplete placement prevented accepting its own replacement.
A failed prospective learner could therefore strand automatic repair even when
the original three-voter quorum was healthy. The new control command permits
same-shard supersession with a generation CAS. Other shard movements still wait,
and historical possible-voter identities remain retirement obligations.

The controller only replaces a pending target if a member is unavailable or
draining, a distinct three-node healthy target exists, and the existing 15-second
cooldown has elapsed. Healthy slow targets keep their intent. Health remains a
preference, not permission to reduce quorum. Eight concurrent 500-ms probes keep
dead registry entries from exhausting the entire 20-second reconciliation tick.
This is health/topology-based repair, not resource-informed balancing.

## Formal-first, implementation tests and review

`PlacementIntent.tla` was committed before the implementation. Its 380-state
check covers generation-current completion, one pending shard, retained history
and repair availability. Four negative configurations violate those properties.
The liveness check assumes bounded intent changes and fair membership completion;
the membership-admission model separately supplies the vote/completion boundary.
These are not mechanically composed models or a Rust refinement proof.
`intent-formal-before-code.txt` retains the complete Lean/TLC run.

`make check` passed (`intent-check.txt`). Policy tests cover the cooldown boundary,
healthy pending targets, insufficient candidates and unrelated missing shards.
The storage test crosses snapshot installation and reopen with a superseded
intent, checks rejection of legacy/concurrent/stale replacements and stale
completion, and verifies that the excluded identity remains `MayVote` until the
current completion supplies its demotion boundary.

Oracle found a concrete starvation regression in serial health probing. The
128-identity HTTP test now passes in 8.06 seconds; a serial-concurrency mutation
fails at its 12-second deadline. Both outputs are retained. Review found no
supersession safety blocker under the existing single-controller and admission
fence assumptions. It also found harness cleanup gaps, which were fixed before
the live run: uncertain admission/arm creation is tracked before mutation, cleanup
restores draining and independently attempts every disarm/release/heal, and an
uncertain quarantine mutation is reconciled by reading rather than blind retry.
The 25 Python tests include those failures. Actual k3d tests cover chain-only,
INPUT-only, OUTPUT-only and two-jump partial-injection cleanup.

## Real cluster evidence

The release binary with test-only storage gates ran in all five pods as
`chronicle-raft:pending-repair`. Source and image identities are in
`intent-built-image.json`, `intent-runtime-image.json`, and
`intent-running-pods.json`. All old pods stopped before the uniform-version
upgrade; PVCs were preserved. Default-false command fields preserve replay of
old logs, **not mixed-version state-machine compatibility**.

The harness first resumes an already verified drained replica 5 and observes its
group-1 persistence gate and an incomplete `{2,3,5}` placement. It isolates that
agent using iptables. Before releasing either fault, the controller completes a
higher-generation `{1,2,3}` placement with applied uniform membership. Only then
does the harness quarantine replica 5 to prevent legitimate reassignment after
healing; retirement is still false. After release/heal, retirement succeeds and
three observations retain the successor and the old replica's uniform nonvoter
configuration. Those observations do not exclude every intermediate state.

| History | Traffic shard | Records acknowledged | Unknown append attempts | Successful / unknown strict reads | Generation | Observed repair | Porcupine |
|---|---:|---:|---:|---:|---|---:|---|
| `intent-catchup.jsonl` | 4 (unaffected) | 720 | 16 | 111 / 2 | 24 → 25 | 23.25 s | Ok |
| `intent-catchup-shard1.jsonl` | 1 (repaired) | 720 | 16 | 105 / 5 | 26 → 27 | 23.35 s | Ok |
| `intent-snapshot.jsonl` | 1 (repaired) | 720 | 15 | 105 / 4 | 28 → 29 | 22.12 s | Ok |
| `intent-snapshot-crash.jsonl` | 1 (repaired) | 720 | 13 | 105 / 4 | 30 → 31 | 23.43 s | Ok |

The first two hit `after-log-commit-before-log-flushed`, **not** the separately armed
snapshot-install gate. Their `*-events.jsonl` files retain gated/pending state,
automatic repair before quarantine/release, DROP counters, cleanup results and
post-release observations. Matching smoke/prefix and independent Porcupine
outputs are retained; unknown mutations stay pending through history end.
`intent-counts.json` derives counts and elapsed observation times from the raw
records. The workload used two producers, 96-byte records and 250-ms pacing:
these are fault schedules, not throughput benchmarks or recovery SLOs.

The last two force snapshot catch-up: the leader's purge boundary must first
exceed the drained learner's log. They reach `before-snapshot-install-transaction`
in the snapshot receiver, before the atomic SQLite replacement transaction.
The crash run then uses the host container runtime to SIGKILL that container,
not an in-container PID-1 signal. Its event history verifies exit 137, the same
Pod UID, a different container ID and restart count 0 → 1. This tests interrupted
installation before its transaction and recovery, not a torn SQLite transaction
or power loss. `intent-snapshot-counts.json` retains the counts; final pod and
gate-file captures confirm recovery and cleanup. The harness now has 27 unit tests.
Use `--gate before-snapshot-install-transaction` on the hook for this schedule,
and add `--crash-gated` for the runtime kill. Replica repair must still complete
before either fault is released.

Reproduce after the private k3d deployment has a verified drained non-seed replica
and only seeds 1/2/3 eligible; enable the test-only gate environment as described
in `tests/README.md`. Use an unused path hashing to the affected shard:

```sh
python3 tests/history.py run --url "$PRIVATE_URL" --seed "$UNUSED_SEED" \
  --path intent-catchup-48 --output "$NEW_HISTORY" \
  --producers 2 --readers 2 --operations 360 --read-interval 2 \
  --append-interval .25 --retries 20 --timeout 2 --nemesis node-join \
  --nemesis-delay 3 --hook-timeout 900 \
  --hook 'node-join:start=exec python3 tests/pending_placement.py --node 5 --output NEW_EVENTS.jsonl'
```

The shown path was used in this checkout; choose another unused path and confirm
its shard with `tests/gated_history.py`'s `group_for` before repeating. Do not
overwrite prior histories. On interruption or timeout, inspect draining state,
gate controls and iptables explicitly before reuse: SIGKILL cannot promise Python
cleanup. Successful runs removed all owned gate files and partition rules.

This does not demonstrate recovery without an applicable joint quorum, failure
inside the SQLite snapshot transaction, power-loss durability, independent disks or
AZs, full protocol conformance, or resource-informed balancing. Experimental
leadership campaigns remain off.
