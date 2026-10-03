#!/usr/bin/env python3
"""Observe experimental leader preference convergence; never inject a fault.

These diagnostic metrics do not establish stream correctness. Run the existing
history driver and offline checker independently while exercising the cluster.
"""
import argparse
import json
import time

from gated_history import CLUSTER_CONTEXT, NAMESPACE, kubectl, pod_status, running_pods


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--stable-seconds", type=float, default=65)
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    if kubectl("config", "current-context").stdout.strip() != CLUSTER_CONTEXT:
        raise RuntimeError("unexpected Kubernetes context")
    deadline = time.monotonic() + args.timeout
    previous, since = None, time.monotonic()
    with open(args.output, "x", encoding="utf-8") as output:
        while time.monotonic() < deadline:
            sample = {"time_ns": time.monotonic_ns()}
            try:
                state = json.loads(kubectl("get", "--raw",
                    f"/api/v1/namespaces/{NAMESPACE}/pods/chronicle-0:8080/proxy/admin/control").stdout)
                statuses = {pod["metadata"]["name"]: pod_status(pod["metadata"]["name"])
                            for pod in running_pods()}
                views = {}
                for shard, placement in state["placements"].items():
                    voters = sorted(placement["voters"])
                    target = voters[int(shard) % len(voters)]
                    leaders = {str(node): {
                        "leader": statuses[f"chronicle-{node-1}"][shard]["current_leader"],
                        "term": statuses[f"chronicle-{node-1}"][shard]["current_term"],
                    } for node in voters}
                    views[shard] = {"target": target, "generation": placement["generation"],
                        "complete": placement["complete"], "voters": leaders}
                sample["views"] = views
                matched = len(views) == 5 and all(v["complete"] and all(
                    n["leader"] == v["target"] for n in v["voters"].values()) for v in views.values())
                signature = json.dumps(views, sort_keys=True) if matched else None
            except (RuntimeError, KeyError, ValueError) as exc:
                sample["error"] = str(exc)
                signature = None
            now = time.monotonic()
            if signature is None or signature != previous:
                since = now
            previous = signature
            sample["stable_seconds"] = now - since
            output.write(json.dumps(sample, sort_keys=True) + "\n")
            output.flush()
            if signature is not None and now - since >= args.stable_seconds:
                print(json.dumps({"converged": True, "stable_seconds": now - since, "views": views}))
                return
            time.sleep(2)
    raise TimeoutError("preferred leadership did not converge; retain observations")


if __name__ == "__main__":
    main()
