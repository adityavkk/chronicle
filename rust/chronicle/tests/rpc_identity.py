#!/usr/bin/env python3
"""Negative RPC-recipient test against a disposable, otherwise idle cluster."""
import argparse
import json
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--cluster", required=True)
    args = parser.parse_args()

    def status():
        with urllib.request.urlopen(args.url + "/admin/status", timeout=5) as r:
            return json.load(r)["0"]

    before = status()
    # If a misdirected request reaches Raft, this term is unmistakable in status.
    vote = {"leader_id": {"term": before["current_term"] + 1000, "node_id": 9999}, "committed": True}
    bodies = {
        "append": {"vote": vote, "prev_log_id": None, "entries": [], "leader_commit": None},
        "vote": {"vote": vote, "last_log_id": None},
        "snapshot": {"vote": vote, "meta": {"last_log_id": None,
                     "last_membership": {"log_id": None, "membership": {"configs": [], "nodes": {}}},
                     "snapshot_id": "must-not-install"}, "offset": 0, "data": [], "done": True},
    }
    results = []
    for route, body in bodies.items():
        for name, headers in [
            ("missing", {}),
            ("wrong-node", {"x-chronicle-cluster": args.cluster, "x-chronicle-recipient": "9999"}),
            ("wrong-cluster", {"x-chronicle-cluster": args.cluster + "-other", "x-chronicle-recipient": str(before["id"])}),
        ]:
            req = urllib.request.Request(args.url + "/raft/0/" + route,
                  data=json.dumps(body).encode(), headers={"Content-Type": "application/json", **headers})
            try:
                with urllib.request.urlopen(req, timeout=5) as response:
                    code = response.status
            except urllib.error.HTTPError as error:
                code = error.code
            results.append({"route": route, "case": name, "status": code})
            assert code == 421, results[-1]
    after = status()
    assert after["current_term"] < vote["leader_id"]["term"], "misdirected term reached Raft"
    assert after["last_applied"] == before["last_applied"], "application state changed during test"
    print(json.dumps({"valid": True, "results": results, "before": before, "after": after}, indent=2))


if __name__ == "__main__":
    main()
