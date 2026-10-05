#!/usr/bin/env python3
"""Pause an observed preferred candidate in the disposable upgrade cluster.

The external nemesis races admission; it does not pretend to pause at an exact
Rust instruction. Record whether the durable claim actually happened. Complete
histories are checked separately with the existing Porcupine adapter.
"""
import argparse
import json
import pathlib
import random
import subprocess
import sys
import time
import urllib.request

from gated_history import group_for
from history import Client
from leadership_balance import ROOT, SERVER, api, kube


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--image", required=True, help="expected locally pinned candidate image")
    parser.add_argument("--pressure-group", required=True, type=int, choices=range(1, 5))
    parser.add_argument("--pressure-mib", type=int, default=4, choices=(4, 6, 8))
    parser.add_argument("--drain", action="store_true", help="admit retired node 4 in the target's zone, then drain the target instead of pausing it")
    args = parser.parse_args()
    args.output.mkdir()
    inspected = json.loads(subprocess.check_output(["sudo", "docker", "inspect", SERVER]))[0]
    networks = inspected["NetworkSettings"]["Networks"]
    url = f"http://{networks['k3d-chronicle-upgrade']['IPAddress']}:30644"
    pods = json.loads(kube("-n", "chronicle", "get", "pods", "-l", "app=chronicle-raft", "-o", "json"))["items"]
    if len(pods) != 5:
        raise RuntimeError("requires the isolated five-pod candidate cluster")
    for pod in pods:
        container = pod["spec"]["containers"][0]
        env = {e["name"]: e.get("value") for e in container["env"]}
        if container["image"] != args.image or env.get("CHRONICLE_LEADERSHIP_BALANCE") != "1":
            raise RuntimeError("unexpected candidate image or policy")
        if env.get("STREAM_TENANT") != "conformance-mounted":
            raise RuntimeError("unexpected mount")
    initial = api(url, "/admin/control")
    if not all(p["complete"] and p["voters"] == [1, 2, 3] for p in initial["placements"].values()):
        raise RuntimeError("requires settled seed membership")
    if args.drain and (not initial["nodes"]["4"]["draining"] or api(url, "/admin/retirement/4") is not True):
        raise RuntimeError("replacement must initially be retired")
    (args.output / "pods-before.json").write_text(json.dumps(pods, indent=2))
    for pod in pods:
        name = pod["metadata"]["name"]
        (args.output / f"{name}-metrics-before.txt").write_text(
            subprocess.check_output(["sudo", "docker", "exec", SERVER, "wget", "-qO-", "-T", "5",
                                     f"http://{pod['status']['podIP']}:8080/metrics"], text=True, timeout=10))

    def path_for(group, kind):
        for n in range(10000):
            path = f"unavailable-{args.seed}-{kind}-{n}"
            if group_for("conformance-mounted", "leadership/" + path) == group:
                return path
        raise RuntimeError("no shard mapping")

    writers, files, paused = [], [], None
    with (args.output / "nemesis.jsonl").open("x") as log:
        def note(phase, **fields):
            log.write(json.dumps({"phase": phase, "time_ns": time.monotonic_ns(), **fields}) + "\n")
            log.flush()

        pressure = Client([url], "leadership", path_for(args.pressure_group, "pressure"), 10, random.Random(args.seed))
        payload = b"U" * (512 * 1024)
        chunks = args.pressure_mib * 2
        for seq in range(-1, chunks):
            method, body = ("PUT", b"") if seq == -1 else ("POST", payload)
            note("pressure-invoke", seq=seq, bytes=len(body))
            status, headers, _, error = pressure.request(method, body, {"Content-Type": "application/octet-stream"})
            note("pressure-return", seq=seq, status=status, headers=headers, error=error)
            if status not in (200, 201, 204):
                raise RuntimeError("pressure setup did not succeed; no blind retry")
        try:
            for group in range(1, 5):
                out = (args.output / f"shard-{group}-smoke.json").open("x")
                err = (args.output / f"shard-{group}-stderr.txt").open("x")
                files.extend([out, err])
                writers.append(subprocess.Popen([sys.executable, str(ROOT / "tests/history.py"), "run",
                    "--url", url, "--tenant", "leadership", "--path", path_for(group, "history"),
                    "--seed", str(args.seed + group), "--producers", "2", "--readers", "1",
                    "--operations", "900", "--append-interval", ".15", "--read-interval", "1",
                    "--timeout", "3", "--retries", "60", "--retry-interval", ".25",
                    "--output", str(args.output / f"shard-{group}.jsonl")], stdout=out, stderr=err))
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                state = api(url, "/admin/control")
                attempt = state["leadership"]["attempt"]
                note("observe", ledger=state["leadership"])
                if attempt and attempt["id"] > initial["leadership"]["last_id"]:
                    allowed = ("Planned", "Claimed") if args.drain else ("Planned",)
                    if attempt["phase"] not in allowed:
                        raise RuntimeError("missed required pending phase; retain this run")
                    if any(p.poll() is not None for p in writers):
                        raise RuntimeError("writers ended before the fault")
                    target = attempt["proposal"]["target"]
                    if args.drain:
                        def register(identity, node):
                            note("register-invoke", identity=identity, node=node, attempt=attempt)
                            request = urllib.request.Request(url + "/admin/register", data=json.dumps([identity, node]).encode(),
                                                             headers={"Content-Type": "application/json"})
                            # A lost mutation response is unknown, never blindly replayed.
                            with urllib.request.urlopen(request, timeout=10) as response:
                                note("register-return", identity=identity, status=response.status, result=json.load(response))
                        spare = dict(state["nodes"]["4"], zone=state["nodes"][str(target)]["zone"], draining=False)
                        register(4, spare)
                        register(target, dict(state["nodes"][str(target)], draining=True))
                        note("drain-requested", target=target, replacement=4)
                        after_drain = api(url, "/admin/control")
                        note("post-drain-control", control=after_drain)
                        pending = after_drain["leadership"]["attempt"]
                        if pending["id"] != attempt["id"] or pending["phase"] not in ("Planned", "Claimed"):
                            raise RuntimeError("did not observe the targeted attempt pending after committed drain; classify from retained logs")
                        break
                    pod = next(p for p in pods if p["metadata"]["name"] == f"chronicle-{target - 1}")
                    node = pod["spec"]["nodeName"]
                    if not node.startswith("k3d-chronicle-upgrade-agent-"):
                        raise RuntimeError("refusing to pause outside the candidate agent set")
                    node_state = json.loads(subprocess.check_output(["sudo", "docker", "inspect", node]))[0]["State"]
                    if not node_state["Running"] or node_state["Paused"]:
                        raise RuntimeError("target not independently running before fault")
                    note("pause-invoke", node=node, target=target, attempt=attempt)
                    # Mark before invocation so cleanup covers uncertain transport outcomes.
                    paused = node
                    subprocess.run(["sudo", "docker", "pause", node], check=True, timeout=15)
                    note("paused", node=node)
                    time.sleep(34)  # Past the 30s intent expiry; not a durability claim.
                    subprocess.run(["sudo", "docker", "unpause", node], check=True, timeout=15)
                    paused = None
                    note("healed", node=node)
                    break
                time.sleep(.2)
            else:
                raise TimeoutError("no new leadership plan under this measured workload")
        except BaseException as error:
            note("failed", error=repr(error))
            raise
        finally:
            if paused is not None:
                subprocess.run(["sudo", "docker", "unpause", paused], check=True, timeout=15)
                note("cleanup-unpause", node=paused)
            codes = [p.wait() for p in writers]
            note("writers-finished", codes=codes)
            for file in files:
                file.close()
        if any(codes):
            raise RuntimeError("workload/smoke check failed")
        if args.drain:
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                state = api(url, "/admin/control")
                note("retirement-observe", control=state)
                if all(p["complete"] and target not in p["voters"] and 4 in p["voters"]
                       for p in state["placements"].values()) and api(url, f"/admin/retirement/{target}") is True:
                    note("retired", target=target)
                    break
                time.sleep(2)
            else:
                raise TimeoutError("membership replacement or old-owner retirement did not complete")
        final = api(url, "/admin/control")
        note("final", control=final)
        if not args.drain and final["placements"] != initial["placements"]:
            raise RuntimeError("membership changed during unavailable-target case")
        status, _, body, error = pressure.request("GET")
        if status != 200 or body != payload * chunks:
            raise RuntimeError(f"acknowledged pressure bytes lost: {status}, {error}")
        note("pressure-retained", bytes=len(body))
        after = json.loads(kube("-n", "chronicle", "get", "pods", "-l", "app=chronicle-raft", "-o", "json"))
        (args.output / "pods-after.json").write_text(json.dumps(after, indent=2))
        for pod in after["items"]:
            name = pod["metadata"]["name"]
            (args.output / f"{name}-metrics-after.txt").write_text(
                subprocess.check_output(["sudo", "docker", "exec", SERVER, "wget", "-qO-", "-T", "5",
                                         f"http://{pod['status']['podIP']}:8080/metrics"], text=True, timeout=10))
            lines = kube("-n", "chronicle", "logs", name).splitlines()
            (args.output / f"{name}-movement.jsonl").write_text(
                "\n".join(line for line in lines if "leadership" in line or "resource movement observed" in line) + "\n")


if __name__ == "__main__":
    main()
