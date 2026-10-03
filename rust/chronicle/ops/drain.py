#!/usr/bin/env python3
"""Request Raft removal and await applied placement completion; never deletes a PVC."""
import argparse
import json
import time
import urllib.error
import urllib.request


def request(url, value=None):
    data = None if value is None else json.dumps(value).encode()
    req = urllib.request.Request(url, data=data, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="private cluster or local port-forward URL")
    parser.add_argument("--node", required=True, type=int)
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    control = args.url.rstrip("/") + "/admin/control"
    state = request(control)
    node = state["nodes"][str(args.node)]
    if sum(not n["draining"] for id_, n in state["nodes"].items() if id_ != str(args.node)) < 3:
        raise RuntimeError("drain needs at least three other registered non-draining nodes")
    node["draining"] = True
    # Exactly one mutation. A transport error leaves the outcome unknown; the
    # operator inspects state rather than blindly replaying a stale node record.
    request(args.url.rstrip("/") + "/admin/register", [args.node, node])
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        try:
            state = request(control)
            placements = state["placements"]
            if len(placements) == 5 and all(
                p["complete"] and args.node not in p["voters"] for p in placements.values()
            ):
                print(json.dumps({"drained": args.node, "control": state}, indent=2))
                return
        except (urllib.error.URLError, TimeoutError):
            pass  # Safe to retry only this read, including during control-group transfer.
        time.sleep(1)
    raise TimeoutError("drain incomplete; keep the node and its volume running")


if __name__ == "__main__":
    main()
