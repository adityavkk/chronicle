#!/usr/bin/env python3
"""Qualify opt-in leadership convergence on the isolated upgrade k3d cluster.

Reuses history.py and its existing offline checkers. This coordinator does not
implement another safety checker or inject faults. It preserves all observations
and requires convergence while all four stream workloads are still active.
"""
import argparse
from collections import Counter
import hashlib
import json
import pathlib
import random
import subprocess
import sys
import time
import urllib.parse
import urllib.request

from gated_history import group_for
from history import Client

SERVER = "k3d-chronicle-upgrade-server-0"
ROOT = pathlib.Path(__file__).resolve().parents[1]


def kube(*args):
    return subprocess.check_output(["sudo", "docker", "exec", "-i", SERVER, "kubectl", *args],
                                   text=True, timeout=30)


def api(url, path):
    with urllib.request.urlopen(url + path, timeout=5) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    args.output.mkdir()  # Never overwrite an earlier failed run.
    pods = json.loads(kube("-n", "chronicle", "get", "pods", "-l", "app=chronicle-raft", "-o", "json"))["items"]
    if len(pods) != 5:
        raise RuntimeError("requires five restored candidate pods")
    for pod in pods:
        container = pod["spec"]["containers"][0]
        env = {e["name"]: e.get("value") for e in container["env"]}
        if container["image"] != args.image or env.get("CHRONICLE_LEADERSHIP_BALANCE") != "1":
            raise RuntimeError("candidate image/opt-in mismatch")
        if env.get("STREAM_TENANT") != "conformance-mounted":
            raise RuntimeError("this qualification requires the captured mounted API")
    ips = subprocess.check_output(["sudo", "docker", "inspect", SERVER,
        *[f"k3d-chronicle-upgrade-agent-{i}" for i in range(5)]], text=True)
    allowed = {net["IPAddress"] for item in json.loads(ips) for net in item["NetworkSettings"]["Networks"].values()}
    url = args.url.rstrip("/")
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "http" or parsed.hostname not in allowed or parsed.port != 30644:
        raise RuntimeError("refusing a URL outside the isolated candidate's private NodePort")
    cluster = env["CLUSTER_ID"]
    (args.output / "pods-before.json").write_text(json.dumps(pods, indent=2))
    writers, files = [], []
    with (args.output / "observations.jsonl").open("x") as output:
        def note(phase, **data):
            output.write(json.dumps({"phase": phase, "time_ns": time.monotonic_ns(), **data}) + "\n")
            output.flush()

        def resources(pod):
            ordinal = int(pod["metadata"]["name"].rsplit("-", 1)[1])
            body = subprocess.check_output(["sudo", "docker", "exec", SERVER, "wget", "-qO-", "-T", "5",
                "--header", f"x-chronicle-cluster: {cluster}", "--header", f"x-chronicle-recipient: {ordinal + 1}",
                f"http://{pod['status']['podIP']}:8080/admin/resources"], text=True, timeout=10)
            return json.loads(body)

        def leaders(loads):
            result = {}
            for node, groups in loads.items():
                for group, load in groups.items():
                    view = load.get("view")
                    if view and view["leader"]:
                        if group in result:
                            raise RuntimeError("ambiguous leadership observation")
                        result[group] = (node, view)
            if len(result) != 5:
                raise RuntimeError("missing leader observations")
            return result

        def observe():
            state = api(url, "/admin/control")
            loads = {int(p["metadata"]["name"].rsplit("-", 1)[1]) + 1: resources(p) for p in pods}
            current = leaders(loads)
            note("observe", control=state, loads=loads, leaders=current)
            return state, loads, current

        state, loads, current = observe()
        initial_attempt = state["leadership"]["last_id"]
        if {int(n) for n, v in state["nodes"].items() if not v["draining"]} != {1, 2, 3}:
            raise RuntimeError("requires eligible seed voters and retired spares")
        placements = state["placements"]
        if not all(p["complete"] and p["voters"] == [1, 2, 3] for p in placements.values()):
            raise RuntimeError("replica placement not stable before leadership qualification")
        counts = Counter(node for node, _ in current.values())
        pressure = next((int(g) for g, (node, _) in current.items() if g != "0" and counts[node] >= 2
                         and loads[node][g]["charged_bytes"] < 10 * 1024 * 1024), None)
        if pressure is None:
            raise RuntimeError("no co-located data leader with bounded pressure headroom")

        def path_for(group, kind):
            for i in range(10000):
                path = f"leadership-{args.seed}-{kind}-{i}"
                if group_for("conformance-mounted", "leadership/" + path) == group:
                    return path
            raise RuntimeError("could not map fresh stream")

        pressure_path = path_for(pressure, "pressure")
        client = Client([url], "leadership", pressure_path, 10, random.Random(args.seed))
        payload = b"P" * (512 * 1024)
        for seq in range(-1, 8):
            method, body = ("PUT", b"") if seq == -1 else ("POST", payload)
            note("pressure-invoke", seq=seq, shard=pressure, path=pressure_path, bytes=len(body))
            status, headers, _, error = client.request(method, body, {"Content-Type": "application/octet-stream"})
            note("pressure-return", seq=seq, status=status, headers=headers, error=error)
            if status not in (200, 201, 204):
                raise RuntimeError("pressure setup outcome was not successful; do not retry")
        note("pressure-ready", shard=pressure, bytes=8 * len(payload), scope="setup, not Porcupine workload")

        try:
            for group in range(1, 5):
                stdout = (args.output / f"shard-{group}-smoke.json").open("x")
                stderr = (args.output / f"shard-{group}-stderr.txt").open("x")
                files.extend([stdout, stderr])
                writers.append(subprocess.Popen([sys.executable, str(ROOT / "tests/history.py"), "run",
                    "--url", url, "--tenant", "leadership", "--path", path_for(group, "history"),
                    "--seed", str(args.seed + group), "--producers", "2", "--readers", "1",
                    "--operations", "1800", "--append-interval", ".25", "--read-interval", "1",
                    "--timeout", "3", "--retries", "30", "--retry-interval", ".25",
                    "--output", str(args.output / f"shard-{group}.jsonl")], stdout=stdout, stderr=stderr))
            deadline = time.monotonic() + 420
            signature, stable_since, moved = None, None, False
            while time.monotonic() < deadline:
                state, _, current = observe()
                if state["placements"] != placements:
                    raise RuntimeError("replica movement overlapped leadership-only qualification")
                attempt = state["leadership"]["attempt"]
                moved |= bool(attempt and attempt["id"] > initial_attempt and attempt["phase"] == "ObservedTarget")
                # Applied indices advance under load and are not leadership changes.
                value = tuple((g, n, json.dumps(v["vote"], sort_keys=True), json.dumps(v["membership"], sort_keys=True))
                              for g, (n, v) in sorted(current.items()))
                pending = attempt and attempt["phase"] in ("Planned", "Claimed")
                if value != signature or pending or stable_since is None:
                    stable_since = time.monotonic() if not pending else None
                signature = value
                if moved and stable_since is not None and time.monotonic() - stable_since >= 125:
                    if any(p.poll() is not None for p in writers):
                        raise RuntimeError("workload ended before stable qualification")
                    note("stable-under-load", seconds=time.monotonic() - stable_since, leaders=current,
                         ledger=state["leadership"])
                    break
                time.sleep(4)
            else:
                raise TimeoutError("no observed transfer followed by 125s stable loaded leadership")
        except BaseException as error:
            note("failed", error=repr(error))
            raise
        finally:
            # Keep complete outcomes, even if observation fails; no cherry-picked
            # early termination or replacement history. Workers have bounded calls.
            codes = [process.wait() for process in writers]
            note("workloads-finished", exit_codes=codes)
            for file in files:
                file.close()
        if any(codes):
            raise RuntimeError("a workload or its smoke check failed")
        final, _, last = observe()
        last_signature = tuple((g, n, json.dumps(v["vote"], sort_keys=True), json.dumps(v["membership"], sort_keys=True))
                               for g, (n, v) in sorted(last.items()))
        if last_signature != signature or final["leadership"]["last_id"] != state["leadership"]["last_id"]:
            raise RuntimeError("leadership changed after the stable observation window")
        status, _, body, error = client.request("GET")
        if status != 200 or body != payload * 8:
            raise RuntimeError(f"pressure acknowledged bytes not retained: {status}, {error}")
        note("pressure-retained", bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
        after = json.loads(kube("-n", "chronicle", "get", "pods", "-l", "app=chronicle-raft", "-o", "json"))["items"]
        (args.output / "pods-after.json").write_text(json.dumps(after, indent=2))
        def runtimes(items):
            return sorted((p["metadata"]["uid"], p["status"]["containerStatuses"][0]["containerID"]) for p in items)
        if runtimes(pods) != runtimes(after):
            raise RuntimeError("a process replacement masked convergence")


if __name__ == "__main__":
    main()
