#!/usr/bin/env python3
"""History-driver hook: resource-based join, stable assignment, and Raft drain.

Private disposable k3d only. No fault timing or workload generator lives here:
tests/history.py invokes this hook while its writers and readers remain active.
"""
import argparse
import json
import time

from leader_drain import processes
from retirement_partition import K, api, command, wait


def signature(state):
    return tuple((g, p["generation"], tuple(p["voters"]), p["complete"])
                 for g, p in sorted(state["placements"].items()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--stable-seconds", type=float, default=125)
    parser.add_argument("--timeout", type=float, default=420)
    args = parser.parse_args()
    if command(K, "config", "current-context").strip() != "k3d-chronicle-rust":
        raise RuntimeError("refusing another cluster")
    with open(args.output, "x", encoding="utf-8") as output:
        def note(phase, **data):
            output.write(json.dumps({"phase": phase, "time_ns": time.monotonic_ns(), **data}) + "\n")
            output.flush()

        def register(node):
            note("register-invoke", node=4, value=node)
            api("/admin/register", [4, node])
            note("register-ok", node=4, value=node)

        state = api("/admin/control")
        if {int(i) for i, n in state["nodes"].items() if not n["draining"]} != {1, 2, 3}:
            raise RuntimeError("requires eligible seeds only")
        if len(state["placements"]) != 5 or not all(p["complete"] and p["voters"] == [1, 2, 3]
                                                   for p in state["placements"].values()):
            raise RuntimeError("requires complete seed placements")
        if api("/admin/retirement/4") is not True:
            raise RuntimeError("requires verified spare 4")
        before = processes()
        original = state["nodes"]["4"].copy()
        note("before", control=state, processes=before)
        try:
            spare = original.copy()
            spare.update(draining=False, zone="a")  # Same simulated domain as node 1.
            register(spare)
            deadline = time.monotonic() + args.timeout
            previous, stable_since = None, None
            while time.monotonic() < deadline:
                state = wait(lambda: api("/admin/control"), "control observation")
                current = signature(state)
                for p in state["placements"].values():
                    domains = {state["nodes"][str(i)]["zone"] for i in p["voters"]}
                    if len(domains) != 3:
                        raise RuntimeError("placement collapsed supplied failure domains")
                ready = all(p["complete"] for p in state["placements"].values()) and any(
                    4 in p["voters"] for p in state["placements"].values())
                if current != previous or not ready:
                    stable_since = time.monotonic() if ready else None
                note("observe", control=state)
                if stable_since is not None and time.monotonic() - stable_since >= args.stable_seconds:
                    break
                previous = current
                time.sleep(5)
            else:
                raise TimeoutError("resource placement did not stabilize")
            if processes() != before:
                raise RuntimeError("process restart masked movement")
            note("stable", control=state, seconds=time.monotonic() - stable_since)
        except BaseException as error:
            note("failed", error=repr(error))
            raise
        finally:
            # A different desired state, not a retry of an ambiguous admission.
            current = api("/admin/control")["nodes"]["4"]
            if current != original:
                register(original)
        def restored():
            state = api("/admin/control")
            return state if all(p["complete"] and p["voters"] == [1, 2, 3]
                                for p in state["placements"].values()) else None

        state = wait(restored, "seed restoration")
        wait(lambda: api("/admin/retirement/4") is True, "spare retirement")
        if processes() != before:
            raise RuntimeError("process restart masked restoration")
        note("restored", control=state, processes=before)


if __name__ == "__main__":
    main()
