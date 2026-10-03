#!/usr/bin/env python3
"""Interrupt a real long poll after its deadline, before its strict recheck.

Requires the feature-gated image and a direct port-forward to this shard's leader.
Only this harness owns fault timing. Records schema-3 HTTP observations and
nemesis events, not general Porcupine evidence.
"""
import argparse
import concurrent.futures
import json
import pathlib
import re
import subprocess
import time
import urllib.request

from gated_history import NAMESPACE, control, group_for, kubectl, leader_for, preflight, wait_until
from history import emit
from long_poll import Requests, expect_read

GATE = "before-live-timeout-recheck"
ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(args):
    root = pathlib.PurePosixPath(args.fault_root)
    if not root.is_relative_to("/data") or ".." in root.parts:
        raise ValueError("fault directory must be within /data")
    pods = preflight(args)
    group = group_for("live", args.path + "-gate")
    pod, observations = leader_for(group)
    with urllib.request.urlopen(args.url + "/admin/status", timeout=5) as response:
        local = json.load(response)[str(group)]
    if local["id"] != local["current_leader"] or f"chronicle-{local['id']-1}" != pod:
        raise RuntimeError("use a direct port-forward to the observed shard leader")
    node = next(p["spec"]["nodeName"] for p in pods if p["metadata"]["name"] == pod)
    ordinal = node.removeprefix("k3d-chronicle-rust-agent-")
    if not re.fullmatch(r"[0-9]+(?:-[0-9]+)?", ordinal):
        raise RuntimeError("unexpected disposable agent node")
    gate_dir = f"{root}/{f'http-live-{group}'.encode().hex()}"
    arm, reached, release, resumed = (f"{gate_dir}/{GATE}.{suffix}" for suffix in ("arm", "reached", "release", "resumed"))
    kubectl("-n", NAMESPACE, "exec", pod, "--", "mkdir", "-p", gate_dir)
    if any(control(pod, "test", p, check=False).returncode == 0 for p in (arm, reached, release, resumed)):
        raise RuntimeError("refusing pre-existing gate controls")

    with open(args.output, "x", encoding="utf-8") as output:
        requests = Requests(args, output)
        call = requests.call
        def note(phase, **value):
            emit(output, requests.lock, {"schema": 3, "type": "info", "f": "nemesis",
                                         "value": {"phase": phase, **value}})
        def partition(action):
            result = subprocess.run(["bash", str(ROOT / "ops/partition.sh"), action, ordinal],
                                    capture_output=True, text=True, timeout=30)
            note(action, returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
            result.check_returncode()
        note("setup", pod=pod, node=node, group=group, observations=observations, scenario=args.scenario)
        assert call("gate", "PUT")["status"] == 201
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        finished, partition_owned = False, False
        try:
            control(pod, "touch", arm)
            start = time.monotonic()
            future = executor.submit(call, "gate", "GET", query={"live": "long-poll", "offset": "now"})
            wait_until(lambda: control(pod, "test", reached, check=False).returncode == 0,
                       time.monotonic() + 12, "long-poll recheck gate")
            assert not future.done(), "response escaped the gate"
            # The marker is emitted only from the server's deadline branch, not
            # inferred from elapsed time observed by this slower external process.
            note("deadline-gated", elapsed_s=time.monotonic() - start)
            if args.scenario == "append":
                assert call("gate", "POST", b"after")["status"] == 204
            elif args.scenario == "recreate":
                assert call("gate", "DELETE")["status"] == 204
                assert call("gate", "PUT", b"NEW", {"stream-incarnation": "2"})["status"] == 201
            elif args.scenario == "close":
                assert call("gate", "POST", headers={"stream-closed": "true"})["status"] == 204
            else:
                probe = subprocess.run(["sudo", "docker", "exec", node, "iptables", "-S"],
                                       capture_output=True, text=True, timeout=10)
                present = "CHRONICLE_FAULT" in probe.stdout
                note("partition-preflight", returncode=probe.returncode, stderr=probe.stderr,
                     chain_present=present, filter_lines=len(probe.stdout.splitlines()))
                if probe.returncode != 0 or probe.stderr or present:
                    raise RuntimeError("refusing existing/unknown partition state")
                # The absent-chain check makes a partially applied isolate ours to heal.
                partition_owned = True
                partition("isolate")
                time.sleep(1)
            control(pod, "touch", release)
            result = future.result(timeout=22)
            if args.scenario == "append":
                expect_read(result, 200, "after", 5)
            elif args.scenario == "recreate":
                assert result["status"] == 409, result
            elif args.scenario == "close":
                expect_read(result, 204, "", 0, closed=True)
            else:
                assert result["status"] is None or result["status"] >= 500, result
            wait_until(lambda: control(pod, "test", resumed, check=False).returncode == 0,
                       time.monotonic() + 10, "live reader resumed")
            finished = True
        finally:
            try:
                control(pod, "touch", release, check=False)
                control(pod, "rm", arm, check=False)
                if partition_owned:
                    partition("heal")
                if finished:
                    for path in (reached, resumed, release):
                        control(pod, "rm", path, check=False)
                # On failure retain release until restoring the normal image stops
                # detached blocking tasks; removing it could re-trap an old reader.
            finally:
                executor.shutdown(wait=True)
        deadline = time.monotonic() + 30
        while True:
            final = call("gate", "GET")
            if final["status"] is not None and final["status"] < 500:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("strict reads did not recover; retain all attempts")
            time.sleep(.2)
        assert final["status"] == 200, final
        assert final["body"] == {"append": "after", "recreate": "NEW", "close": "", "quorum": ""}[args.scenario], final
    print(json.dumps({"result": "passed", "scenario": args.scenario, "history": args.output,
                      "scope": "gated live-read contract, not general linearizability"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--fault-root", required=True)
    parser.add_argument("--scenario", choices=["append", "recreate", "close", "quorum"], required=True)
    run(parser.parse_args())
