# Replica retirement and quorum repair are different outcomes

The older drain histories established replacement voters and completed placement
records, not delivery of demotion to the departing process. Node 4 subsequently
kept campaigning with obsolete joint membership; see
`drained-node-candidate-status.json`. OpenRaft 0.9.25 need not deliver the final
uniform configuration to a voter removed with `retain=false`.

The new controller temporarily retains departed voters as learners, verifies
applied nonvoter metadata against a per-identity demotion boundary, then prunes
them. Unreachable learners may be pruned without claiming graceful retirement.
Historical identities can be rediscovered when they return. Placement history
distinguishes genuinely untouched groups from unknown legacy history; probes
fence startup recovery and rotate fairly. Replicated eligibility checks reject
cached assignments after a draining registration. An explicit undrain deliberately
permits future assignment and must not race an operator stopping the node.

## Checks before deployment

* Retirement TLC: 32 states; drain-ordering TLC: 12 states. Negative mutations
  expose unreachable-peer, stale-learner and late-assignment false completion.
* `make formal` passed the complete Lean/TLC suite (`retirement-formal.txt`).
  This is not mechanized Rust refinement; the retirement model fixes one cleanup
  episode, and the empty-group implementation case relies on placement history.
* `make check` passed (`retirement-check.txt`): locked lint/tests/doc checks,
  storage VFS faults, snapshot/reopen histories, and 23 Python tests. Tests cover
  stale completion during re-promotion, same-generation retries, retained older
  boundaries, absent legacy fields and apply-time drain fencing.
* Oracle found the freshness, untouched-group and fairness gaps, then the stale
  placement/drain race. Follow-up found no blocker in the scoped fixes. This was
  not a fresh review of every outstanding protocol or scaling requirement.

## Real local k3d evidence

The release image uses source revision recorded in `retirement-built-image.json`.
`retirement-runtime-image.json` associates the containerd image digest with that
source label; `retirement-final-images.json` records all five running replicas.
All old processes were stopped before this schema-changing upgrade;
`retirement-stopped-pods.json` has zero pods. PVCs were preserved. Mixed binary
revisions are unsupported. Startup briefly returned 503 and logged higher-term
conflicts before the obsolete replica converged.

Node 4 then reported Learner in all five groups (`retirement-node4-status.json`),
and the stronger drain command returned successfully (`retirement-initial-drain.json`).
The raw status capture also includes kubectl's pod-deletion notice; the parsed JSON
is retained separately, not mistaken for an independent second observation.

| Run | Successful append responses | Unknown append attempts | Successful strict reads | Unknown reads | Porcupine |
|---|---:|---:|---:|---:|---|
| Fresh node-5 admission and three shard moves | 600 | 0 | 80 | 0 | Ok |
| Verified node-5 drain, process removal, same-PVC restart | 600 | 0 | 80 | 0 | Ok |
| Node-5 packet loss, voter repair, reconnection and demotion | 720 | 23 | 96 | 3 | Ok |

Histories are `retirement-{join,drain,partition2}.jsonl`; matching smoke-check and
Porcupine outputs are retained. These are paced representative workloads (two
producers, 96-byte records, 250-ms inter-append delay), not throughput benchmarks.
The failed first partition attempt is also retained: its admin POST used the wrong
media type, failed before fault injection and omitted the final read. It is **not**
partition evidence, regardless of the successful ordinary traffic it recorded.

`retirement-node5-joined.json` and `retirement-node5-restarted.json` confirm that
groups 0 and 4 remained empty, while groups 1..3 were assigned and subsequently
became learners. `retirement-partition2-events.jsonl` records successful voter
repair while retirement was false, actual DROP counters, healing, and verified
retirement afterward. The fault helper owns injection and heals in `finally`.
It resumes an already drained replica, waits for all intended assignments, then
isolates its k3d agent. This does not test failure during initial learner catch-up
or midway through snapshot installation. Pending-intent recovery under those
failures remains separate acceptance work.

The fifth k3d agent initially failed: kubelet exhausted inotify instances. The
failure log and decisive lines are retained in `retirement-node-create-failed.txt`
and `retirement-inotify.txt`. Raising the disposable orb's
`fs.inotify.max_user_instances` to 1024 and restarting that agent made it Ready.
This does not establish that 1024 is appropriate on arbitrary hosts.

The deployment remains one nested-Docker host, not independent disks or AZs.
Process restart is not power loss. Full protocol conformance and resource-informed
replica/leadership balancing remain outstanding; native campaigns remain off.
