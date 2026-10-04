#!/usr/bin/env python3
"""History hook: drain a live control/data leader without restarting any process.

Requires the disposable k3d cluster, eligible seeds 1/2/3 and verified spare 4.
Restores seed eligibility afterward. Every mutation is single-shot; failed runs
retain their event file and require state inspection before another attempt.
"""
import argparse
import json
import time

from gated_history import pod_status
from retirement_partition import K, api, command, wait


def processes():
    pods = json.loads(command(K, "-n", "chronicle", "get", "pods", "-l",
                              "app=chronicle-raft", "-o", "json"))["items"]
    result = {}
    for pod in pods:
        container = next(c for c in pod["status"]["containerStatuses"] if c["name"] == "chronicle")
        if not container["ready"]:
            raise RuntimeError("all replicas must stay ready")
        result[pod["metadata"]["name"]] = [pod["metadata"]["uid"], container["containerID"], container["restartCount"]]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if command(K, "config", "current-context").strip() != "k3d-chronicle-rust":
        raise RuntimeError("refusing another cluster")
    with open(args.output, "x", encoding="utf-8") as output:
        def note(phase, **data):
            output.write(json.dumps({"phase": phase, "time_ns": time.monotonic_ns(), **data}) + "\n")
            output.flush()

        def draining(node, value):
            registered = api("/admin/control")["nodes"][str(node)]
            registered["draining"] = value
            note("register-invoke", node=node, draining=value)
            api("/admin/register", [node, registered])
            note("register-ok", node=node, draining=value)

        state = api("/admin/control")
        if {int(i) for i, n in state["nodes"].items() if not n["draining"]} != {1, 2, 3}:
            raise RuntimeError("requires eligible seeds only")
        if api("/admin/retirement/4") is not True:
            raise RuntimeError("requires verified spare 4")
        before = processes()
        note("before", processes=before, control=state)
        draining(4, False)

        def joined():
            state = api("/admin/control")
            placements = state["placements"]
            return state if all(p["complete"] for p in placements.values()) and all(
                4 in placements[str(g)]["voters"] for g in (1, 2, 3)) else None

        note("joined", control=wait(joined, "spare placement"))
        statuses = {pod: pod_status(pod) for pod in before}
        leaders = [pod for pod, status in statuses.items() if status["0"]["state"] == "Leader"]
        if len(leaders) != 1:
            raise RuntimeError("expected one live control leader")
        pod = leaders[0]
        node = statuses[pod]["0"]["id"]
        if node not in (1, 2, 3) or not any(statuses[pod][str(g)]["state"] == "Leader" for g in range(1, 5)):
            raise RuntimeError("control leader must also lead a data group")
        note("leader-before-drain", node=node, status=statuses)
        draining(node, True)
        wait(lambda: api(f"/admin/retirement/{node}") is True, "leader retirement")
        after = pod_status(pod)
        if any(m["state"] != "Learner" for m in after.values()):
            raise RuntimeError("retired process still leads or votes")
        note("leader-retired", node=node, status=after, control=api("/admin/control"))
        if processes() != before:
            raise RuntimeError("process restart masked retirement")
        # A second membership transition qualifies restoration, not just removal.
        draining(node, False)
        draining(4, True)

        def restored():
            state = api("/admin/control")
            return state if all(p["complete"] and p["voters"] == [1, 2, 3]
                                for p in state["placements"].values()) else None

        state = wait(restored, "seed placement restoration")
        wait(lambda: api("/admin/retirement/4") is True, "spare retirement")
        if processes() != before:
            raise RuntimeError("process restart masked restoration")
        note("restored", control=state, processes=before)


if __name__ == "__main__":
    main()
