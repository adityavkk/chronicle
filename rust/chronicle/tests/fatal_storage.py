#!/usr/bin/env python3
"""History hook: a real storage fatal must self-exit despite a blocked file body.

Uses only the guarded disposable k3d cluster. Fault controls live on the PVC and
are removed through its host mount even when the target is crash-looping.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import PurePosixPath
import random
import re
import time

from gated_history import group_for, pod_status, preflight
from history import Client
from retirement_partition import K, api, command, wait


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--path", required=True, help="unused auxiliary stream path")
    parser.add_argument("--output", required=True)
    parser.add_argument("--fault-root", default="/data/faults", choices=["/data/faults"])
    args = parser.parse_args()
    preflight(args)
    placement = api("/admin/control")["placements"]["0"]
    if not placement["complete"]:
        raise RuntimeError("control placement is still moving")
    observed = {f"chronicle-{id_ - 1}": pod_status(f"chronicle-{id_ - 1}")["0"]
                for id_ in placement["voters"]}
    candidates = [name for name, m in observed.items() if m["id"] == m["current_leader"]]
    if len(candidates) != 1:
        raise RuntimeError(f"no unique self-observed voter leader: {observed}")
    pod = candidates[0]
    metrics = pod_status(pod)
    node_id = metrics["0"]["id"]
    if not any(m["current_leader"] == node_id for g, m in metrics.items() if g != "0"):
        raise RuntimeError("requires control leader also leading a data group")
    path = next(f"{args.path}-{i}" for i in range(100)
                if metrics[str(group_for("fatal-test", f"{args.path}-{i}"))]["current_leader"] == node_id)
    client = Client([args.url], "fatal-test", path, 10, random.Random(0))
    created = client.request("PUT", b"acknowledged-before-fatal", {"Content-Type": "application/octet-stream"})
    if created[0] != 201:
        raise RuntimeError(f"auxiliary stream creation failed: {created}")

    def status():
        p = json.loads(command(K, "-n", "chronicle", "get", "pod", pod, "-o", "json"))
        c = next(c for c in p["status"]["containerStatuses"] if c["name"] == "chronicle")
        return {"pod_uid": p["metadata"]["uid"], **c}

    before = status()
    spec = json.loads(command(K, "-n", "chronicle", "get", "pod", pod, "-o", "json"))["spec"]
    agent = spec["nodeName"]
    if not re.fullmatch(r"k3d-chronicle-rust-agent-[0-9]+(?:-[0-9]+)?", agent):
        raise RuntimeError("unexpected node")
    claim = next(v["persistentVolumeClaim"]["claimName"] for v in spec["volumes"] if v["name"] == "data")
    volume = command(K, "-n", "chronicle", "get", "pvc", claim, "-o", "jsonpath={.spec.volumeName}")
    mount = command(K, "get", "pv", volume, "-o", "jsonpath={.spec.local.path}")
    if not PurePosixPath(mount).is_relative_to("/var/lib/rancher/k3s/storage") or ".." in PurePosixPath(mount).parts:
        raise RuntimeError("unexpected PVC mount")

    def host(*args):
        return command("sudo", "docker", "exec", agent, *args)

    body = f"{mount}/faults/{'http-body'.encode().hex()}/before-response-file-read"
    log = f"{mount}/faults/{'group-0.sqlite'.encode().hex()}/after-log-commit-before-log-flushed"
    files = [f"{gate}.{suffix}" for gate in (body, log)
             for suffix in ("arm", "error", "reached", "release", "resumed")]
    for f in files:
        if host("sh", "-c", 'if [ -e "$1" ]; then echo present; fi', "sh", f).strip():
            raise RuntimeError(f"refusing existing fault file {f}")
    identity = host("sha256sum", f"{mount}/identity.json").split()[0]
    registered = api("/admin/control")["nodes"][str(node_id)]
    with open(args.output, "x", encoding="utf-8") as output, ThreadPoolExecutor(max_workers=1) as executor:
        def note(phase, **values):
            output.write(json.dumps({"phase": phase, "time_ns": time.monotonic_ns(), **values}) + "\n")
            output.flush()

        note("before", pod=pod, before=before, observations=observed, metrics=metrics,
             claim=claim, volume=volume, identity=identity, auxiliary_path=path)
        try:
            host("mkdir", "-p", str(PurePosixPath(body).parent), str(PurePosixPath(log).parent))
            # The host's storage ancestors are root-only. Only these disposable
            # control directories need the Chronicle process's ownership.
            host("chown", "1000:1000", f"{mount}/faults",
                 str(PurePosixPath(body).parent), str(PurePosixPath(log).parent))
            host("touch", body + ".arm")
            # Direct pod proxy ensures the body task lives on this process.
            reader = executor.submit(command, K, "get", "--raw",
                f"/api/v1/namespaces/chronicle/pods/{pod}:8080/proxy/v1/stream/fatal-test/{path}")
            wait(lambda: host("sh", "-c", 'if [ -f "$1" ]; then echo reached; fi', "sh", body + ".reached").strip(),
                 "blocked body task")
            note("body-reached")
            host("touch", log + ".error")
            # Same registry value, but a real replicated control write. Ambiguous
            # completion is recorded and never retried as a supposedly absent write.
            try:
                note("trigger-returned", result=api("/admin/register", [node_id, registered]))
            except Exception as error:
                note("trigger-unknown", error=repr(error))

            def terminated():
                s = status()
                previous = s.get("lastState", {}).get("terminated", {})
                current = s.get("state", {}).get("terminated", {})
                matched = any(t.get("exitCode") == 1 and t.get("containerID") == before["containerID"]
                              for t in (previous, current))
                return s if s["pod_uid"] == before["pod_uid"] and matched else None

            stopped = wait(terminated, "self-exit1, not a liveness-probe signal")
            note("self-exited-with-body-unreleased", status=stopped,
                 log_reached=host("test", "-f", log + ".reached") == "")
        finally:
            # Host access does not depend on a live Chronicle process. Disarm
            # first, release any surviving reader, then wait for runtime recovery.
            try:
                host("rm", "-f", log + ".error", body + ".arm")
            finally:
                host("touch", body + ".release")

        def restarted():
            s = status()
            return s if (s["pod_uid"] == before["pod_uid"] and s["ready"]
                         and s["containerID"] != before["containerID"]
                         and s["restartCount"] > before["restartCount"]) else None

        after = wait(restarted, "same-PVC process replacement")
        assert host("sha256sum", f"{mount}/identity.json").split()[0] == identity
        recovered = client.request("GET")
        assert recovered[0] == 200 and recovered[2] == b"acknowledged-before-fatal", recovered
        try:
            note("reader-returned", value=reader.result())
        except Exception as error:
            note("reader-interrupted", error=repr(error))
        host("rm", "-f", *files)
        note("recovered", after=after, identity=identity, retained=len(recovered[2]))


if __name__ == "__main__":
    main()
