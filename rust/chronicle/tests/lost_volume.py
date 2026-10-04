#!/usr/bin/env python3
"""Withhold node 3's PVC, reject empty-store restart, and repair onto spare 4.

Use as a history.py one-shot hook. Leaves the old contents quarantined and node 3
unready. Restore only AFTER retained histories pass, using mode restore with the
same run name. This simulates unavailable storage, not hardware power loss.
"""
import argparse
import json
from pathlib import PurePosixPath
import re
import time

from gated_history import pod_status
from pending_placement import applied_uniform
from retirement_partition import K, api, command, wait

IMAGE = "sha256:b75eac801bbc91b4584769c840f346683c7740d72f83e630088370f8e1eb2c8a"
POD = "chronicle-2"
CLAIM = "data-chronicle-2"


def location(pod, volume, container, run):
    """Refuse any target except the named disposable PVC on its actual k3d agent."""
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", run):
        raise ValueError("invalid run name")
    agent = pod["spec"]["nodeName"]
    if not re.fullmatch(r"k3d-chronicle-rust-agent-[0-9]+(?:-[0-9]+)?", agent):
        raise ValueError("unexpected agent")
    if container["Config"]["Labels"].get("k3d.cluster") != "chronicle-rust":
        raise ValueError("not the disposable cluster")
    claim = volume["spec"]["claimRef"]
    if claim["namespace"] != "chronicle" or claim["name"] != CLAIM:
        raise ValueError("unexpected PVC")
    name = volume["metadata"]["name"]
    if not re.fullmatch(r"pvc-[0-9a-f-]{36}", name):
        raise ValueError("unexpected volume identity")
    expected = f"/var/lib/rancher/k3s/storage/{name}_chronicle_{CLAIM}"
    if volume["spec"]["local"]["path"] != expected:
        raise ValueError("unexpected volume path")
    mounts = [m for m in container["Mounts"] if m["Destination"] == "/var/lib/rancher/k3s"]
    if len(mounts) != 1 or mounts[0]["Type"] != "volume" or not re.fullmatch(r"[0-9a-f]{64}", mounts[0]["Name"]):
        raise ValueError("unexpected Docker volume")
    path = "/disk/storage/" + PurePosixPath(expected).name
    return agent, mounts[0]["Name"], path, path + ".withheld-" + run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["inject", "restore"])
    parser.add_argument("--run", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--confirm-disposable-data-loss", required=True, choices=["chronicle-rust"])
    args = parser.parse_args()
    if command(K, "config", "current-context").strip() != "k3d-chronicle-rust":
        raise RuntimeError("refusing another Kubernetes context")
    pod = json.loads(command(K, "-n", "chronicle", "get", "pod", POD, "-o", "json"))
    claims = [v["persistentVolumeClaim"]["claimName"] for v in pod["spec"]["volumes"] if v["name"] == "data"]
    if claims != [CLAIM]:
        raise RuntimeError("unexpected pod data claim")
    volume_name = command(K, "-n", "chronicle", "get", "pvc", CLAIM, "-o", "jsonpath={.spec.volumeName}")
    volume = json.loads(command(K, "get", "pv", volume_name, "-o", "json"))
    container = json.loads(command("sudo", "docker", "inspect", pod["spec"]["nodeName"]))[0]
    agent, mount, path, withheld = location(pod, volume, container, args.run)
    pods = json.loads(command(K, "-n", "chronicle", "get", "pods", "-l", "app=chronicle-raft", "-o", "json"))["items"]
    colocated = [p["metadata"]["name"] for p in pods if p["spec"].get("nodeName") == agent]
    if colocated != [POD]:
        raise RuntimeError(f"refusing to stop multiple replicas: {colocated}")
    before = next(c for c in pod["status"]["containerStatuses"] if c["name"] == "chronicle")
    # Never retain full Docker labels: k3d stores a cluster credential there.
    state = api("/admin/control")
    if args.mode == "inject":
        if not before["ready"]:
            raise RuntimeError("lost-volume target must start ready")
        if any(not p["complete"] or p["voters"] != [1, 2, 3] for p in state["placements"].values()):
            raise RuntimeError("requires stable three-seed placement")
        if not state["nodes"]["4"]["draining"] or api("/admin/retirement/4") is not True:
            raise RuntimeError("requires verified drained spare 4")
        if {id_ for id_, node in state["nodes"].items() if not node["draining"]} != {"1", "2", "3"}:
            raise RuntimeError("unexpected eligible replicas")
    else:
        if any(not p["complete"] or p["voters"] != [1, 2, 4] for p in state["placements"].values()):
            raise RuntimeError("restore requires completed replacement")
        if not state["nodes"].get("3", {}).get("draining", False):
            raise RuntimeError("restore requires the lost identity to remain quarantined")

    def cold(script):
        running = command("sudo", "docker", "inspect", agent, "--format", "{{.State.Running}}").strip()
        if running != "false":
            raise RuntimeError("refusing volume mutation while its agent runs")
        return command("sudo", "docker", "run", "--rm", "--network", "none", "--read-only",
            "--mount", f"type=volume,src={mount},dst=/disk", "--entrypoint", "/bin/sh", IMAGE,
            "-ec", script, "sh", path, withheld)

    with open(args.output, "x", encoding="utf-8") as output:
        def note(phase, **values):
            output.write(json.dumps({"phase": phase, "time_ns": time.monotonic_ns(), **values}) + "\n")
            output.flush()

        note("before", mode=args.mode, pod=POD, agent=agent, volume=volume_name,
             docker_volume=mount, path=path, withheld=withheld, state=state)
        try:
            # Whole-node stop kills every Chronicle task holding this volume.
            command("sudo", "docker", "stop", "--time", "0", agent)
            note("agent-stopped")
            if args.mode == "inject":
                listing = cold('test -f "$1/identity.json"; test ! -e "$2"; '
                    'for n in 0 1 2 3 4; do test -f "$1/group-$n.sqlite"; done; '
                    'mv "$1" "$2"; mkdir "$1"; chown 1000:1000 "$1"; '
                    'sha256sum "$2/identity.json"; ls -1 "$2"')
                note("contents-withheld", listing=listing)
            else:
                # This cannot overwrite a newly initialized database or identity.
                listing = cold('test -f "$2/identity.json"; '
                    'for f in "$1"/* "$1"/.[!.]* "$1"/..?*; do '
                    'test ! -e "$f" || test "$f" = "$1/process.lock"; done; '
                    'rm -f "$1/process.lock"; rmdir "$1"; mv "$2" "$1"; '
                    'sha256sum "$1/identity.json"')
                note("original-contents-restored-after-test", identity=listing)
        finally:
            # Restore node infrastructure, never silently restore withheld data.
            command("sudo", "docker", "start", agent)
            note("agent-started")

        if args.mode == "restore":
            # Use the command helper whose transient subprocess errors wait()
            # handles; gated_history.pod_status instead wraps them in RuntimeError.
            metrics = wait(lambda: json.loads(command(K, "get", "--raw",
                f"/api/v1/namespaces/chronicle/pods/{POD}:8080/proxy/admin/status")),
                "original-volume process restart")
            state = api("/admin/control")
            if not state["nodes"]["3"]["draining"]:
                raise RuntimeError("restored identity is no longer quarantined")
            note("original-process-recovered", metrics=metrics, state=state)
            return

        def refused():
            current = json.loads(command(K, "-n", "chronicle", "get", "pod", POD, "-o", "json"))
            c = next((c for c in current["status"].get("containerStatuses", []) if c["name"] == "chronicle"), None)
            if c is None or c["restartCount"] <= before["restartCount"] or c.get("containerID") == before["containerID"]:
                return None
            failed = c.get("lastState", {}).get("terminated", {}).get("exitCode") == 1
            failed |= c.get("state", {}).get("terminated", {}).get("exitCode") == 1
            return c if not c["ready"] and failed else None

        rejected = wait(refused, "empty-volume startup refusal")
        host_path = path.replace("/disk", "/var/lib/rancher/k3s", 1)
        listing = command("sudo", "docker", "exec", agent, "ls", "-A1", host_path).splitlines()
        if listing != ["process.lock"]:
            raise RuntimeError(f"empty-store restart created unexpected files: {listing}")
        note("empty-store-rejected", container=rejected, files=listing,
             log=command(K, "-n", "chronicle", "logs", POD, "--tail=15"))
        spare = dict(state["nodes"]["4"], draining=False)
        try:
            note("spare-eligibility-returned", result=api("/admin/register", [4, spare]))
        except Exception as error:
            note("spare-eligibility-unknown", error=repr(error))
        wait(lambda: not api("/admin/control")["nodes"]["4"]["draining"], "spare eligibility")

        def repaired():
            s = api("/admin/control")
            return s if all(p["complete"] and p["voters"] == [1, 2, 4] for p in s["placements"].values()) else None

        repaired_state = wait(repaired, "automatic whole-shard replacement")
        metrics = pod_status("chronicle-3")
        assert all(applied_uniform(m, [1, 2, 4]) for m in metrics.values()), metrics
        # Keep the lost identity quarantined on any later infrastructure repair.
        lost = dict(repaired_state["nodes"]["3"], draining=True)
        try:
            note("lost-quarantine-returned", result=api("/admin/register", [3, lost]))
        except Exception as error:
            note("lost-quarantine-unknown", error=repr(error))
        wait(lambda: api("/admin/control")["nodes"]["3"]["draining"], "lost identity quarantine")
        assert api("/admin/retirement/3") is False, "lost storage is not graceful retirement"
        note("repaired-without-original-volume", state=api("/admin/control"), spare=metrics)


if __name__ == "__main__":
    main()
