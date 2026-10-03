# Default-off native campaign experiment

This is not directed leadership transfer or resource-informed balancing.
OpenRaft 0.9.25 exposes a native election trigger; it does not promise that the
preferred voter wins. `CHRONICLE_EXPERIMENTAL_CAMPAIGNS=1` explicitly enables
the experiment. It remains absent from normal deployment configuration.
Automatic replica placement and repair do not require this switch.

The preferred voter is the sorted voter at `shard % voters.len()`. A controller
requires 30 seconds of unchanged generation, term and leader observations, and
consumes that cooldown before submitting a campaign. Ineligible, incomplete or
draining placements reset observations. Membership and fresh metrics are checked
before submission. This reduces unnecessary elections; it cannot make a queued
campaign cancellable or guarantee availability during movement.

## Real k3d observations

All runs used the local persistent-volume cluster and release image whose exact
identity is retained in `campaign-enabled-images.json`. Four registered nodes
were available; normal placement used voters 1/2/3 with node 4 drained. Histories
use monotonic times. Diagnostic observations are not linearizability authority.

| Exercise | Acknowledged appends | Successful reads | Offline Porcupine | Observation |
|---|---:|---:|---|---|
| Leader pod restart | 480 | 591 | Ok | Terms unchanged; recovery evidence, not leadership movement |
| Preferred voter isolated for 10 seconds, then healed | 480 | 572 | Ok | Preferred leader observed 31.36 seconds after healing; stable 66.98 seconds |
| First membership round trip | 144 | See retained history | Ok | Harness exhausted immediate retries and advanced unresolved sequences; stability threshold missed |
| Paced membership round trip | 1,200 | 597 | Ok | Voters 1/3/4 then 1/2/3; both target views stable over 66 seconds |

The partition run recorded 36 ambiguous append and 19 ambiguous read responses.
The paced membership run recorded 287 ambiguous append and 159 ambiguous read
responses. Successful-response p99 was respectively 28.17/17.92 ms and
30.25/19.67 ms for appends/reads. Largest gaps between successes were 3.17/3.46
seconds and 4.05/4.06 seconds. These include pacing, retries and routing through
a drained ingress; they are **not continuous outage durations or capacity
measurements**. Raw counts and timings are in the corresponding latency JSON.

The first partition hook exited 126 because it was not executable; no partition
was installed. Its history and observations remain as `campaign-partition-*`.
The corrected run invoked the hook through Bash (`campaign-partition2-*`). The
first membership observer timed out: restoration began after at most 63.9 seconds
of stability, below its 65-second threshold. Its observations and history remain;
there is deliberately no successful result file. The second run paced retries and
stopped a producer after an unresolved operation instead of advancing sequence.
No failed attempt was replaced with a passing artifact.

## Reproduction and boundaries

Use `tests/history.py` as the sole fault timing owner; its `--help` documents
invocation, retry and pacing controls. `tests/campaign_membership.sh start URL`
resumes registered node 4 and drains node 2; `restore URL` reverses this using
the existing identities. It verifies the disposable cluster context and expected
placement, submits mutations once, and fails rather than blindly retrying unknown
admin results. `tests/campaign_observe.py --output FILE` independently samples
all voter views until they remain preferred and unchanged for 65 seconds.

Histories are losslessly compressed; `campaign-history-sha256.txt` records their
uncompressed checksums. From this directory, for example:

```sh
gzip -dc campaign-membership2-history.jsonl.gz > /tmp/campaign-history.jsonl
go run ../../../jepsen/checker -rust-history /tmp/campaign-history.jsonl -rust-history-timeout 30s
```

The retained `*-porcupine.txt` files contain actual checker outcomes. Python's
`*-check.json` is only the additional prefix/retention smoke check. Unknown writes
remain pending through history end in the Porcupine adapter.

Stable sampled terms give bounded evidence against repeated successful elections,
not a proof that no unsuccessful campaign can be queued. Concurrent controller
observations, preferred-voter unavailability and membership changes occurred in
these runs. A deterministic suspended-membership/changed-metrics regression is
still missing. Resource-informed selection, directed transfer, continuous
availability measurement and sustained convergence qualification remain open.

After the experiments all four pods returned to the normal release image with
campaign and fault environment controls absent (`campaign-disabled-images.json`).
The formal campaign model checked 711 states and caught a cooldown-reset mutation;
this is a bounded controller-policy check, not a proof of distributed liveness.

## Dependency advisory checkpoint

`cargo-audit` 0.22.2 checked the pinned lockfile against the RustSec revision
recorded in `cargo-audit.json` on 2026-10-03: 278 dependencies, zero reported
vulnerabilities, no warnings. This is advisory coverage, not a security proof.
