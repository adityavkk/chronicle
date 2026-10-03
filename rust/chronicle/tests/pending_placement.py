#!/usr/bin/env python3
"""Pause prospective learner persistence, partition it, and require automatic intent repair.

Run as a history.py nemesis hook. Only this process controls the fault schedule.
The initially drained non-seed replica is quarantined again before reconnection.
"""
import argparse
import json
from pathlib import PurePosixPath
import re
import subprocess
import time

from gated_history import control, leader_for, pod_status, preflight
from retirement_partition import K, ROOT, api, command, wait

GATES = ("after-log-commit-before-log-flushed", "before-snapshot-install-transaction")


def applied_uniform(metrics, voters):
    membership = metrics["membership_config"]
    return (membership["membership"]["configs"] == [voters]
            and membership["log_id"] is not None and metrics["last_applied"] is not None
            and metrics["last_applied"]["index"] >= membership["log_id"]["index"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", type=int, required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--fault-root", default="/data/faults")
    args = parser.parse_args()
    root = PurePosixPath(args.fault_root)
    if args.node <= 3 or not root.is_relative_to("/data") or ".." in root.parts:
        parser.error("requires a non-seed replica and fault directory within /data")
    preflight(args)
    pod = f"chronicle-{args.node - 1}"
    node = command(K, "-n", "chronicle", "get", "pod", pod, "-o", "jsonpath={.spec.nodeName}")
    ordinal = node.removeprefix("k3d-chronicle-rust-agent-")
    if not re.fullmatch(r"[0-9]+(?:-[0-9]+)?", ordinal):
        raise RuntimeError("unexpected k3d agent")
    if "CHRONICLE_FAULT" in command("sudo", "docker", "exec", node, "iptables", "-S"):
        raise RuntimeError("refusing existing partition")
    state = api("/admin/control")
    registered = state["nodes"][str(args.node)]
    if not registered["draining"] or api(f"/admin/retirement/{args.node}") is not True:
        raise RuntimeError("requires a verified drained replica")
    if {int(id_) for id_, n in state["nodes"].items() if not n["draining"]} != {1, 2, 3}:
        raise RuntimeError("requires only the three seeds eligible before admission")
    directory = str(root / "group-1.sqlite".encode().hex())
    paths = {gate: {suffix: f"{directory}/{gate}.{suffix}"
                   for suffix in ("arm", "reached", "release", "resumed")} for gate in GATES}
    for files in paths.values():
        for path in files.values():
            if control(pod, "test", path, check=False).returncode == 0:
                raise RuntimeError(f"refusing existing gate control: {path}")
    with open(args.output, "x", encoding="utf-8") as output:
        def note(phase, **values):
            output.write(json.dumps({"phase": phase, "time_ns": time.monotonic_ns(), **values}) + "\n")
            output.flush()

        def partition(action):
            result = subprocess.run(["bash", str(ROOT / "ops/partition.sh"), action, ordinal],
                                    capture_output=True, text=True, timeout=30)
            note(action, code=result.returncode, stdout=result.stdout, stderr=result.stderr)
            result.check_returncode()

        command(K, "-n", "chronicle", "exec", pod, "--", "mkdir", "-p", directory)
        armed, isolated, reached = [], False, None
        try:
            for files in paths.values():
                # Refuse replacement rather than silently overwriting a concurrent owner.
                command(K, "-n", "chronicle", "exec", pod, "--", "sh", "-c",
                        'set -C; : > "$1"', "sh", files["arm"])
                armed.append(files)
            registered["draining"] = False
            api("/admin/register", [args.node, registered])
            reached = wait(lambda: next((gate for gate, files in paths.items()
                if control(pod, "test", files["reached"], check=False).returncode == 0), None),
                "learner persistence gate")
            state = api("/admin/control")
            old = state["placements"]["1"]
            if old["complete"] or args.node not in old["voters"]:
                raise RuntimeError("gate did not intercept the pending prospective learner")
            note("pending-gated", gate=reached, state=state)
            isolated = True  # Cleanup owns even a partially applied injection.
            partition("isolate")

            def repaired():
                state = api("/admin/control")
                p = state["placements"]["1"]
                return state if p["complete"] and p["generation"] > old["generation"] and p["voters"] == [1, 2, 3] else None

            state = wait(repaired, "automatic replacement of the pending target")
            generation = state["placements"]["1"]["generation"]
            leader, _ = leader_for(1)
            metrics = pod_status(leader)["1"]
            assert applied_uniform(metrics, [1, 2, 3]), metrics
            assert api(f"/admin/retirement/{args.node}") is False
            assert control(pod, "test", paths[reached]["release"], check=False).returncode != 0
            note("repaired-before-release", state=state, leader=metrics)
            # Repair above must be automatic, before this operator quarantine.
            # Otherwise reconnection legitimately schedules a fresh placement.
            registered["draining"] = True
            api("/admin/register", [args.node, registered])
        finally:
            try:
                # Both gates belong to one serial storage actor. Disarm both
                # before waking it, so no later hit can reuse an old resumed marker.
                for files in armed:
                    control(pod, "rm", files["arm"])
                for files in armed:
                    control(pod, "touch", files["release"])
            finally:
                if isolated:
                    partition("heal")
            if reached is not None:
                wait(lambda: control(pod, "test", paths[reached]["resumed"], check=False).returncode == 0,
                     "storage gate release")
                for files in armed:
                    for suffix in ("reached", "resumed", "release"):
                        control(pod, "rm", files[suffix], check=False)
            elif armed:
                note("unobserved-gate-release-retained", controls=armed)
        wait(lambda: api(f"/admin/retirement/{args.node}") is True, "retirement after release")
        for _ in range(3):
            state = api("/admin/control")
            assert state["placements"]["1"]["generation"] == generation
            assert state["placements"]["1"]["voters"] == [1, 2, 3]
            metrics = pod_status(pod)["1"]
            assert applied_uniform(metrics, [1, 2, 3]), metrics
            note("stable-after-release", state=state, old_replica=metrics)
            time.sleep(2)


if __name__ == "__main__":
    main()
