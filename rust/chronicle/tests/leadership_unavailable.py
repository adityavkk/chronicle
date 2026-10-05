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

from gated_history import group_for
from history import Client
from leadership_balance import ROOT, SERVER, api, kube


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--pressure-group", required=True, type=int, choices=range(1, 5))
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
        if container["image"] != "chronicle-raft:leadership1" or env.get("CHRONICLE_LEADERSHIP_BALANCE") != "1":
            raise RuntimeError("unexpected candidate image or policy")
        if env.get("STREAM_TENANT") != "conformance-mounted":
            raise RuntimeError("unexpected mount")
    initial = api(url, "/admin/control")
    if not all(p["complete"] and p["voters"] == [1, 2, 3] for p in initial["placements"].values()):
        raise RuntimeError("requires settled seed membership")
    (args.output / "pods-before.json").write_text(json.dumps(pods, indent=2))

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
        for seq in range(-1, 8):
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
                    if attempt["phase"] != "Planned":
                        raise RuntimeError("missed pre-claim observation; retain this run")
                    if any(p.poll() is not None for p in writers):
                        raise RuntimeError("writers ended before the fault")
                    target = attempt["proposal"]["target"]
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
        final = api(url, "/admin/control")
        note("final", control=final)
        if final["placements"] != initial["placements"]:
            raise RuntimeError("membership changed during unavailable-target case")
        status, _, body, error = pressure.request("GET")
        if status != 200 or body != payload * 8:
            raise RuntimeError(f"acknowledged pressure bytes lost: {status}, {error}")
        note("pressure-retained", bytes=len(body))
        after = json.loads(kube("-n", "chronicle", "get", "pods", "-l", "app=chronicle-raft", "-o", "json"))
        (args.output / "pods-after.json").write_text(json.dumps(after, indent=2))


if __name__ == "__main__":
    main()
