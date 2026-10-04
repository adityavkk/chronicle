#!/usr/bin/env python3
"""SIGKILL an observed data leader in the disposable k3d cluster and verify restart."""
import argparse
import json
import re
import time

from gated_history import group_for, pod_status
from pending_placement import crash_gated
from retirement_partition import K, command


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if command(K, "config", "current-context").strip() != "k3d-chronicle-rust":
        raise RuntimeError("refusing another cluster")
    group = str(group_for(args.tenant, args.path))
    pods = json.loads(command(K, "-n", "chronicle", "get", "pods", "-l",
                              "app=chronicle-raft", "-o", "json"))["items"]
    leaders = []
    for pod in pods:
        metrics = pod_status(pod["metadata"]["name"])[group]
        if metrics["state"] == "Leader":
            leaders.append((pod, metrics))
    if len(leaders) != 1:
        raise RuntimeError("expected one observed runtime leader")
    pod, metrics = leaders[0]
    agent = pod["spec"]["nodeName"]
    if not re.fullmatch(r"k3d-chronicle-rust-agent-[0-9]+(?:-[0-9]+)?", agent):
        raise RuntimeError("unexpected node")
    with open(args.output, "x", encoding="utf-8") as output:
        def note(phase, **data):
            if phase == "restarted-with-gate-unreleased":
                phase = "restarted"  # Shared runtime helper; this schedule arms no gate.
            output.write(json.dumps({"phase": phase, "time_ns": time.monotonic_ns(), **data}) + "\n")
            output.flush()

        note("observed-leader", group=group, metrics=metrics, pod=pod["metadata"]["name"], agent=agent)
        crash_gated(pod["metadata"]["name"], agent, note)


if __name__ == "__main__":
    main()
