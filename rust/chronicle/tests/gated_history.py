#!/usr/bin/env python3
"""Exercise one live Chronicle storage gate and retain a schema-1 history."""

import argparse
import concurrent.futures
import hashlib
import json
import os
import pathlib
import random
import subprocess
import sys
import threading
import time

from history import Client, check_file, emit, operation, record


NAMESPACE = "chronicle"
CLUSTER_CONTEXT = "k3d-chronicle-rust"
SHARDS = 4
GATES = (
    "after-log-commit-before-log-flushed",
    "after-apply-commit-before-return",
)
KUBECTL = pathlib.Path(__file__).resolve().parents[1] / "ops" / "kubectl.sh"


def kubectl(*args, timeout=30, check=True):
    result = subprocess.run(
        [str(KUBECTL), *args], text=True, capture_output=True, timeout=timeout
    )
    if check and result.returncode:
        raise RuntimeError(
            f"kubectl {' '.join(args)} failed ({result.returncode}): {result.stderr.strip()}"
        )
    return result


def pod_json():
    return json.loads(kubectl("-n", NAMESPACE, "get", "pods", "-l", "app=chronicle-raft", "-o", "json").stdout)


def running_pods():
    return [
        item for item in pod_json()["items"]
        if item.get("status", {}).get("phase") == "Running"
    ]


def pod_status(name):
    path = f"/api/v1/namespaces/{NAMESPACE}/pods/{name}:8080/proxy/admin/status"
    return json.loads(kubectl("get", "--raw", path).stdout)


def group_for(tenant, path):
    key = f"{len(tenant.encode())}:{tenant}{path}".encode()
    return int.from_bytes(hashlib.sha256(key).digest()[:8], "big") % SHARDS + 1


def preflight(a):
    context = kubectl("config", "current-context").stdout.strip()
    if context != CLUSTER_CONTEXT:
        raise RuntimeError(f"refusing Kubernetes context {context!r}; expected {CLUSTER_CONTEXT!r}")
    pods = running_pods()
    if not pods:
        raise RuntimeError("no running Chronicle pods")
    for pod in pods:
        container = next((c for c in pod["spec"]["containers"] if c["name"] == "chronicle"), None)
        if container is None:
            raise RuntimeError(f"{pod['metadata']['name']} has no chronicle container")
        env = {entry["name"]: entry.get("value") for entry in container.get("env", [])}
        if env.get("CHRONICLE_STORAGE_FAULTS") != "1":
            raise RuntimeError(f"{pod['metadata']['name']} lacks CHRONICLE_STORAGE_FAULTS=1 marker")
        if env.get("CHRONICLE_FAULT_DIR") != a.fault_root:
            raise RuntimeError(f"{pod['metadata']['name']} CHRONICLE_FAULT_DIR does not match --fault-root")
    return pods


def leader_for(group):
    observations = []
    for pod in running_pods():
        name = pod["metadata"]["name"]
        status = pod_status(name)
        metrics = status.get(str(group), {})
        observations.append((name, metrics.get("id"), metrics.get("current_leader")))
    leaders = {leader for _, _, leader in observations if leader is not None}
    candidates = [name for name, node, leader in observations if node == leader and leader in leaders]
    if len(leaders) != 1 or len(candidates) != 1:
        raise RuntimeError(f"no unique current leader for group {group}: {observations}")
    return candidates[0], observations


def control(pod, action, path, check=True):
    # PATH is assembled solely from validated CLI values and fixed gate filenames.
    args = [action, "-f", path] if action == "test" else [action, path]
    return kubectl("-n", NAMESPACE, "exec", pod, "-c", "chronicle", "--", *args, check=check)


def wait_until(predicate, deadline, description):
    last = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (RuntimeError, json.JSONDecodeError) as exc:
            last = exc
        time.sleep(0.2)
    suffix = f": {last}" if last else ""
    raise TimeoutError(f"deadline waiting for {description}{suffix}")


def run(a):
    if not a.path or a.path.startswith("/") or ".." in a.path.split("/"):
        raise ValueError("--path must be a non-empty relative, unused stream path")
    root = pathlib.PurePosixPath(a.fault_root)
    if a.gate not in GATES or not root.is_relative_to("/data") or ".." in root.parts:
        raise ValueError("use an allowed --gate and a --fault-root within /data")
    preflight(a)  # No pod is touched before every guard has passed.
    group = group_for(a.tenant, a.path)
    gate_dir = f"{a.fault_root.rstrip('/')}/{f'group-{group}.sqlite'.encode().hex()}"
    arm, reached, release = (f"{gate_dir}/{a.gate}.{suffix}" for suffix in ("arm", "reached", "release"))
    lock = threading.Lock()
    os.makedirs(os.path.dirname(os.path.abspath(a.output)), exist_ok=True)
    armed_pod = None
    controls_owned = False
    actors = []
    failed = None
    seq1_recorded = False
    with open(a.output, "x", encoding="utf-8") as fp:
        def note(typ, phase, **value):
            emit(fp, lock, {"type": typ, "f": "nemesis", "value": {"phase": phase, **value}})

        emit(fp, lock, {"type": "info", "f": "run", "value": {
            "seed": a.seed, "urls": [a.url], "tenant": a.tenant, "path": a.path,
            "record_size": 96, "producers": 1, "reads": 0, "operations_per_producer": 2,
            "payload": "fixed-width ASCII record", "consistency": "strict except labelled stale reads",
            "scenario": "storage-gate-pod-termination", "gate": a.gate, "group": group,
        }})
        client = Client([a.url], a.tenant, a.path, a.request_timeout, random.Random(a.seed))
        payload0, payload1 = record(0, 0, a.seed), record(0, 1, a.seed)
        headers = lambda seq: {"Content-Type": "application/octet-stream", "producer-id": f"gated-{a.seed}",
                               "producer-epoch": "0", "producer-seq": str(seq)}
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        try:
            typ, value = operation(fp, lock, "setup", "create", "create", lambda: client.request("PUT", b"", {"Content-Type": "application/octet-stream"}))
            if typ != "ok" or value["status"] != 201:
                raise RuntimeError(f"unused-path create did not return 201: {value}")
            typ, value = operation(fp, lock, "producer-0", "p0-0", "append", lambda: client.request("POST", payload0, headers(0)))
            emit(fp, lock, {"type": "info", "f": "record", "id": "p0-0", "value": {"record": payload0.decode(), "terminal": typ, "attempts": 1}})
            if typ != "ok":
                raise RuntimeError(f"baseline append was not acknowledged: {value}")

            armed_pod, observed = leader_for(group)
            note("info", "leader", pod=armed_pod, observations=observed)
            kubectl("-n", NAMESPACE, "exec", armed_pod, "-c", "chronicle", "--", "mkdir", "-p", gate_dir)
            existing = [path for path in (arm, reached, release)
                        if control(armed_pod, "test", path, check=False).returncode == 0]
            if existing:
                raise RuntimeError(f"refusing pre-existing control files: {existing}")
            controls_owned = True
            control(armed_pod, "touch", arm)
            note("ok", "armed", pod=armed_pod, control=arm)

            entered = {name: threading.Event() for name in ("append", "append-retry")}
            def append(process, function):
                def request():
                    entered[function].set()  # operation() has already emitted the invoke.
                    return client.request("POST", payload1, headers(1))
                return operation(fp, lock, process, "p0-1", function,
                                 request)
            actors.append(executor.submit(append, "producer-0", "append"))
            wait_until(lambda: control(armed_pod, "test", reached, check=False).returncode == 0,
                       time.monotonic() + a.gate_timeout, "gate.reached")
            note("ok", "reached", pod=armed_pod, control=reached)
            actors.append(executor.submit(append, "producer-retry", "append-retry"))
            if not entered["append-retry"].wait(a.gate_timeout):
                raise TimeoutError("deadline waiting for concurrent retry invocation")
            observation_end = time.monotonic() + 1
            while time.monotonic() < observation_end:
                early = [future.result() for future in actors if future.done()]
                if any(typ == "ok" for typ, _ in early):
                    raise RuntimeError(f"paused schedule did not hold; inspect possible failover before classifying correctness: {early}")
                time.sleep(.01)
            note("ok", "paused-observation", completed=early, observation_seconds=1)

            control(armed_pod, "rm", arm)  # Known script-owned file only; prevents restart retrigger.
            note("ok", "disarmed", pod=armed_pod)
            old_uid = next(p["metadata"]["uid"] for p in running_pods() if p["metadata"]["name"] == armed_pod)
            note("info", "terminate", pod=armed_pod, boundary=a.gate, method="Kubernetes pod deletion")
            deleted = kubectl("-n", NAMESPACE, "delete", "pod", armed_pod, "--grace-period=1", "--wait=true", timeout=a.recovery_timeout)
            note("ok", "terminated", pod=armed_pod, output=deleted.stdout.strip())

            def recovered():
                pods = pod_json()["items"]
                replacement = next((p for p in pods if p["metadata"]["name"] == armed_pod), None)
                ready = lambda p: any(c["type"] == "Ready" and c["status"] == "True" for c in p.get("status", {}).get("conditions", []))
                return replacement is not None and replacement["metadata"]["uid"] != old_uid and all(ready(p) for p in pods)
            wait_until(recovered, time.monotonic() + a.recovery_timeout, "replacement pod readiness")
            wait_until(lambda: client.request("GET")[0] == 200,
                       time.monotonic() + a.recovery_timeout, "quorum through strict read")
            note("ok", "recovered", pod=armed_pod)
            for future in actors:
                future.result(timeout=a.request_timeout + 2)
            typ, value = operation(fp, lock, "producer-final", "p0-1", "append-retry",
                                   lambda: client.request("POST", payload1, headers(1)))
            emit(fp, lock, {"type": "info", "f": "record", "id": "p0-1", "value": {"record": payload1.decode(), "terminal": typ, "attempts": 3}})
            seq1_recorded = True
            if typ != "ok":
                raise RuntimeError(f"post-recovery retry was not acknowledged: {value}")
            if a.gate == "after-apply-commit-before-return" and (
                value["status"] != 204 or value.get("stream-duplicate") != "true"
            ):
                raise RuntimeError(f"committed apply lost its retained producer result: {value}")
            typ, value = operation(fp, lock, "final", "final-read", "read", lambda: client.request("GET"))
            if typ != "ok" or value.get("records") != [payload0.decode(), payload1.decode()]:
                raise RuntimeError(f"strict final read did not contain exactly baseline and seq1: {value}")
        except BaseException as exc:
            failed = exc
            emit(fp, lock, {"type": "fail", "f": "harness", "value": {"error": repr(exc)}})
        finally:
            if controls_owned:
                # Release is idempotent and intentionally retained until the pod/actor can observe it.
                try:
                    control(armed_pod, "touch", release)
                except Exception as exc:
                    failed = failed or exc
                    note("fail", "gate-cleanup", error=repr(exc), release=release)
            for future in actors:
                try:
                    future.result(timeout=a.request_timeout + 2)
                except BaseException as exc:
                    emit(fp, lock, {"type": "info", "f": "actor-cleanup", "value": {"error": repr(exc)}})
            if actors and not seq1_recorded:
                emit(fp, lock, {"type": "info", "f": "record", "id": "p0-1", "value": {
                    "record": payload1.decode(), "terminal": "unknown", "attempts": len(actors)}})
            executor.shutdown(wait=True)
    result = check_file(a.output)
    if failed:
        raise failed
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="private Chronicle URL")
    parser.add_argument("--tenant", default="gated-history")
    parser.add_argument("--path", required=True, help="unused stream path")
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--gate", required=True, choices=GATES)
    parser.add_argument("--fault-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--request-timeout", type=float, default=10)
    parser.add_argument("--gate-timeout", type=float, default=30)
    parser.add_argument("--recovery-timeout", type=float, default=120)
    args = parser.parse_args()
    try:
        return 0 if run(args)["valid"] else 1
    except BaseException as exc:
        print(f"gated history failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
