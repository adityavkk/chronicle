#!/usr/bin/env python3
"""Gate one real file response, truncate its disposable projection, then recover.

Reuses the existing history/checker and Kubernetes gate controls. This is a
serving/telemetry fault, not a Raft durability or power-loss experiment.
"""
import argparse
import concurrent.futures
import hashlib
import pathlib
import random
import threading
import time

from gated_history import NAMESPACE, control, group_for, kubectl, leader_for, preflight, wait_until
from history import Client, check_file, emit, operation, record

GATE = "before-response-file-read"


def run(args):
    root = pathlib.PurePosixPath(args.fault_root)
    if not root.is_relative_to("/data") or ".." in root.parts or not 0 < args.seed < 2**64:
        raise ValueError("use a positive u64 seed and fault directory within /data")
    preflight(args)
    client = Client([args.url], "delivery", args.path, 30, random.Random(args.seed))
    group = group_for("delivery", args.path)
    payload = record(0, 0, args.seed)
    headers = {"content-type": "application/octet-stream", "producer-id": f"delivery-{args.seed}",
               "producer-epoch": "0", "producer-seq": "0"}
    request_id = f"delivery-{args.seed}-fault"
    traceparent = f"00-{args.seed:032x}-0000000000000001-01"
    lock = threading.Lock()
    with open(args.output, "x", encoding="utf-8") as output:
        emit(output, lock, {"type": "info", "f": "run", "value": {
            "seed": args.seed, "tenant": "delivery", "path": args.path,
            "record_size": 96, "scenario": "projection-truncate", "request_id": request_id,
            "traceparent": traceparent, "group": group}})
        call = lambda process, op_id, kind, fn: operation(output, lock, process, op_id, kind, fn)
        typ, value = call("setup", "create", "create", lambda: client.request("PUT", b"", {"content-type": "application/octet-stream"}))
        if typ != "ok" or value["status"] != 201:
            raise RuntimeError(f"unused-path create failed: {value}")
        typ, value = call("producer", "p0-0", "append", lambda: client.request("POST", payload, headers))
        emit(output, lock, {"type": "info", "f": "record", "id": "p0-0", "value": {
            "record": payload.decode(), "terminal": typ, "attempts": 1}})
        if typ != "ok":
            raise RuntimeError(f"baseline append not acknowledged: {value}")
        typ, value = call("retry", "p0-0", "append-retry", lambda: client.request("POST", payload, {
            **headers, "x-request-id": f"delivery-{args.seed}-duplicate", "traceparent": traceparent}))
        if typ != "ok" or value["status"] != 204:
            raise RuntimeError(f"duplicate did not return 204: {value}")
        typ, value = call("reader", "warm", "read", lambda: client.request("GET"))
        if typ != "ok":
            raise RuntimeError(f"warm read failed: {value}")
        pod, observed = leader_for(group)
        directory = f"/data/group-{group}.projection"
        # Match the fixture digest, never a guessed filename or an authoritative DB.
        found = kubectl("-n", NAMESPACE, "exec", pod, "--", "sh", "-c",
            'set -eu; for file in "$1"/.[!.]*; do [ -f "$file" ] || continue; '
            'if [ "$(sha256sum "$file" | cut -d " " -f 1)" = "$2" ]; then printf "%s\\n" "$file"; fi; done',
            "sh", directory, hashlib.sha256(payload).hexdigest()).stdout.splitlines()
        if len(found) != 1 or pathlib.PurePosixPath(found[0]).parent != pathlib.PurePosixPath(directory):
            raise RuntimeError(f"expected one matching projection file, found {found}")
        target = found[0]
        gate_dir = f"{root}/{'http-body'.encode().hex()}"
        arm, reached, release, resumed = (f"{gate_dir}/{GATE}.{suffix}" for suffix in ("arm", "reached", "release", "resumed"))
        kubectl("-n", NAMESPACE, "exec", pod, "--", "mkdir", "-p", gate_dir)
        if any(control(pod, "test", p, check=False).returncode == 0 for p in (arm, reached, release, resumed)):
            raise RuntimeError("refusing pre-existing gate controls")
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        finished = False
        try:
            control(pod, "touch", arm)
            future = executor.submit(call, "reader", "fault", "read", lambda: client.request("GET", headers={
                "x-request-id": request_id, "traceparent": traceparent}))
            wait_until(lambda: control(pod, "test", reached, check=False).returncode == 0,
                       time.monotonic() + 20, "response file read gate")
            if future.done():
                raise RuntimeError("client completed before fault injection")
            emit(output, lock, {"type": "info", "f": "nemesis", "value": {
                "phase": "truncate-projection", "pod": pod, "file": target, "observations": observed}})
            kubectl("-n", NAMESPACE, "exec", pod, "--", "truncate", "--no-create", "-s", "0", target)
            control(pod, "touch", release)
            typ, value = future.result(timeout=35)
            if typ != "unknown":
                raise RuntimeError(f"truncated body was not unknown: {typ} {value}")
            wait_until(lambda: control(pod, "test", resumed, check=False).returncode == 0,
                       time.monotonic() + 20, "response reader resumed")
            finished = True
        finally:
            try:
                control(pod, "touch", release, check=False)
                control(pod, "rm", arm, check=False)
                # A timed-out client may leave a detached blocking reader. Keep
                # release on failure until restoring the normal image stops it.
                if finished:
                    for path in (reached, resumed, release):
                        control(pod, "rm", path, check=False)
            finally:
                executor.shutdown(wait=True)
        typ, value = call("final", "final-read", "read", lambda: client.request("GET", headers={
            "x-request-id": f"delivery-{args.seed}-recovered", "traceparent": traceparent}))
        if typ != "ok" or value.get("records") != [payload.decode()]:
            raise RuntimeError(f"committed payload not recovered: {value}")
    result = check_file(args.output)
    if not result["valid"]:
        raise RuntimeError("history check failed; preserve the history")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--path", required=True, help="new, unused stream path")
    parser.add_argument("--fault-root", default="/data/delivery-gates")
    parser.add_argument("--output", required=True)
    run(parser.parse_args())
