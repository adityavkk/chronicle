#!/usr/bin/env python3
"""Explicit disposable-cluster genesis or fresh learner initialization, never recovery."""
import argparse
import json
import pathlib
import subprocess
import time

K = str(pathlib.Path(__file__).with_name("kubectl.sh"))


def kubectl(*args, data=None):
    return subprocess.check_output([K, "-n", "chronicle", *args], input=data, text=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["genesis", "learner"])
    parser.add_argument("ordinal", type=int)
    args = parser.parse_args()
    if args.ordinal < 0 or (args.mode == "genesis") != (args.ordinal < 3):
        parser.error("genesis ordinals are 0..2; learners need a fresh ordinal >=3")
    template = json.loads(kubectl("get", "sts", "chronicle", "-o", "json"))["spec"]["template"]
    container = template["spec"]["containers"][0]
    for key in ["readinessProbe", "livenessProbe", "ports"]:
        container.pop(key, None)
    container["args"] = [f"export NODE_ID={args.ordinal + 1}; "
                         f"export ADVERTISE=chronicle-{args.ordinal}.chronicle:8080; "
                         f"exec /usr/local/bin/chronicle-raft --init-{args.mode}"]
    name = f"initialize-{args.ordinal}"
    job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": name},
           "spec": {"backoffLimit": 0, "template": {"spec": {
               "restartPolicy": "Never", "securityContext": template["spec"]["securityContext"],
               "containers": [container], "volumes": [{"name": "data", "persistentVolumeClaim": {
                   "claimName": f"data-chronicle-{args.ordinal}"}}]}}}}
    print(kubectl("create", "-f", "-", data=json.dumps(job)))
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            status = json.loads(kubectl("get", "job", name, "-o", "json"))["status"]
            if status.get("failed"):
                raise RuntimeError("initialization failed; do not retry an unknown learner admission with the same ID")
            if status.get("succeeded"):
                break
            time.sleep(1)
        else:
            raise TimeoutError("initialization did not complete")
    finally:
        print(kubectl("logs", f"job/{name}"))


if __name__ == "__main__":
    main()
