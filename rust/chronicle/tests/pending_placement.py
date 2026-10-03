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


def quarantine(node, note):
    registered = api("/admin/control")["nodes"][str(node)]
    if not registered["draining"]:
        registered["draining"] = True
        try:
            api("/admin/register", [node, registered])
        except Exception as error:
            note("quarantine-unknown", error=repr(error))
    # Read after an ambiguous mutation; never assume it did not execute or retry blindly.
    if not api("/admin/control")["nodes"][str(node)]["draining"]:
        raise RuntimeError("quarantine unverified")


def cleanup(pod, node, armed, reached, isolated, partition, note):
    errors = []

    def attempt(label, action):
        try:
            action()
            return True
        except Exception as error:
            errors.append(f"{label}: {error!r}")
            return False

    if node is not None:
        attempt("restore draining", lambda: quarantine(node, note))
    # Disarm every gate before releasing any; failures must not skip the others.
    disarmed = [attempt("disarm", lambda f=f: control(pod, "rm", f["arm"])) for f in armed]
    released = [attempt("release", lambda f=f: control(pod, "touch", f["release"])) for f in armed]
    if isolated:
        attempt("heal", lambda: partition("heal"))
    if reached is not None and all(disarmed) and all(released):
        resumed = attempt("resume", lambda: wait(
            lambda: control(pod, "test", reached["resumed"], check=False).returncode == 0,
            "storage gate release"))
        if resumed:
            for files in armed:
                for suffix in ("reached", "resumed", "release"):
                    # Unused gate markers need not exist; these exact paths are owned.
                    attempt("remove marker", lambda p=files[suffix]: command(
                        K, "-n", "chronicle", "exec", pod, "--", "rm", "-f", p))
    elif armed:
        note("gate-controls-retained", controls=armed)
    note("cleanup", errors=errors)
    return errors


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
        armed, isolated, reached, admitted = [], False, None, False
        try:
            for files in paths.values():
                # Refuse replacement rather than silently overwriting a concurrent owner.
                # One harness owns these preflight-absent paths; creation can be ambiguous.
                armed.append(files)
                command(K, "-n", "chronicle", "exec", pod, "--", "sh", "-c",
                        'set -C; : > "$1"', "sh", files["arm"])
            registered["draining"] = False
            admitted = True
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
            assert control(pod, "test", paths[reached]["release"], check=False).returncode != 0
            note("repaired-before-release", state=state, leader=metrics)
            # Repair above must be automatic, before this operator quarantine.
            # Otherwise reconnection legitimately schedules a fresh placement.
            quarantine(args.node, note)
            assert api(f"/admin/retirement/{args.node}") is False
            note("quarantined-before-release", retired=False)
        except BaseException as error:
            note("failed", error=repr(error))
            raise
        finally:
            errors = cleanup(pod, args.node if admitted else None, armed,
                             paths[reached] if reached else None, isolated, partition, note)
            if errors:
                raise RuntimeError(f"cleanup incomplete: {errors}")
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
