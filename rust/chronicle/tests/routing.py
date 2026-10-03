#!/usr/bin/env python3
"""Prove an ingress removed from a shard can still route committed writes/reads."""
import argparse
import hashlib
import json
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--seed", required=True)
    args = parser.parse_args()
    with urllib.request.urlopen(args.url + "/admin/status", timeout=5) as response:
        status = json.load(response)
    paths = {}
    n = 0
    while len(paths) < 4:
        path = f"routing-{args.seed}-{n}"
        group = int.from_bytes(hashlib.sha256(("4:test" + path).encode()).digest()[:8], "big") % 4 + 1
        paths[group] = path
        n += 1
    checked = []
    for group, path in sorted(paths.items()):
        url = args.url + "/v1/stream/test/" + path
        payload = f"committed through ingress for shard {group}".encode()
        headers = {"content-type": "application/octet-stream"}
        with urllib.request.urlopen(urllib.request.Request(url, method="PUT", data=b"", headers=headers), timeout=10):
            pass
        headers.update({"producer-id": "routing-test", "producer-epoch": "0", "producer-seq": "0"})
        with urllib.request.urlopen(urllib.request.Request(url, method="POST", data=payload, headers=headers), timeout=10) as response:
            assert int(response.headers["stream-next-offset"].split("_")[1]) == len(payload)
        with urllib.request.urlopen(url, timeout=10) as response:
            assert response.read() == payload
        metrics = status[str(group)]
        voters = metrics["membership_config"]["membership"]["configs"]
        removed = all(metrics["id"] not in config for config in voters)
        checked.append({"group": group, "path": path, "ingress_removed": removed})
    assert any(result["ingress_removed"] for result in checked), "test requires ingress removed from at least one shard"
    print(json.dumps({"valid": True, "checked": checked}, indent=2))


if __name__ == "__main__":
    main()
