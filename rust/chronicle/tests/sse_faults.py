#!/usr/bin/env python3
"""Disposable k3d SSE boundaries; schema-3 observations, not linearizability.

Owns all fault timing. Never edits SQLite or its lock files. Projection truncation
targets one digest-matched disposable file. Quorum tests require a direct leader
forward so the injected inter-agent partition does not cut the client connection.
"""
import argparse
import concurrent.futures
import hashlib
import json
import pathlib
import re
import subprocess
import threading
import time
import urllib.request

from gated_history import NAMESPACE, control as gate_control, group_for, kubectl, leader_for, preflight, wait_until
from history import emit
from sse import Harness, control

ROOT = pathlib.Path(__file__).resolve().parents[1]


def slots(pod):
    text = kubectl("get", "--raw", f"/api/v1/namespaces/{NAMESPACE}/pods/{pod}:8080/proxy/metrics").stdout
    return {name: int(value) for line in text.splitlines() if not line.startswith("#")
            for name, value in [line.split(" ", 1)] if name in
            ("chronicle_live_read_available_slots", "chronicle_request_available_slots")}


def run(args):
    root = pathlib.PurePosixPath(args.fault_root)
    if not root.is_relative_to("/data") or ".." in root.parts:
        raise ValueError("fault directory must be within /data")
    pods = preflight(args)
    group = group_for("live", args.path + "-gate")
    pod, observed = leader_for(group)
    with urllib.request.urlopen(args.url.rstrip("/") + "/admin/status", timeout=5) as response:
        ingress = json.load(response)[str(group)]
    direct = ingress["id"] == ingress["current_leader"] and f"chronicle-{ingress['id']-1}" == pod
    if args.scenario in ("quorum", "open-timeout", "later-open-timeout") and not direct:
        raise RuntimeError("this scenario requires a direct port-forward to the observed leader")
    if args.scenario in ("truncate", "append", "admission") and direct:
        raise RuntimeError("use a nonleader ingress to exercise forwarding")
    node = next(p["spec"]["nodeName"] for p in pods if p["metadata"]["name"] == pod)
    ordinal = node.removeprefix("k3d-chronicle-rust-agent-")
    if not re.fullmatch(r"[0-9]+(?:-[0-9]+)?", ordinal):
        raise RuntimeError("unexpected disposable node")
    baseline = ("fixture-" + args.path).encode()
    if args.scenario in ("quorum", "later-open-timeout", "admission"):
        baseline = b""

    with open(args.output, "x", encoding="utf-8") as output:
        h = Harness(args, output)
        def note(phase, **value):
            emit(output, h.lock, {"schema": 3, "type": "info", "f": "nemesis",
                                 "value": {"phase": phase, **value}})
        note("setup", scenario=args.scenario, pod=pod, node=node, observations=observed, ingress=ingress)
        assert h.request("gate", "PUT", baseline, {"content-type": "text/plain"})[0] == 201
        assert h.request("gate", "GET")[2] == baseline
        assert slots(pod) == {"chronicle_live_read_available_slots": 32, "chronicle_request_available_slots": 128}

        if args.scenario == "admission":
            ready = [threading.Event() for _ in range(32)]
            def observe(events, _elapsed, signal):
                if len(events) == 1:
                    control(events[0], 0)
                    signal.set()
                return False
            with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
                futures = [executor.submit(h.stream, "gate", {"live": "sse", "offset": "now"},
                            lambda events, elapsed, signal=signal: observe(events, elapsed, signal)) for signal in ready]
                wait_until(lambda: all(signal.is_set() for signal in ready), time.monotonic() + 15, "32 initial controls")
                ingress_pod = f"chronicle-{ingress['id']-1}"
                for target in (pod, ingress_pod):
                    current = slots(target)
                    note("saturated", pod=target, slots=current)
                    assert current == {"chronicle_live_read_available_slots": 0, "chronicle_request_available_slots": 96}, current
                assert h.stream("gate", {"live": "sse", "offset": "now"}, lambda *_: False)[0] == 429
                assert h.request("gate", "POST", b"wake", {"content-type": "text/plain", "stream-closed": "true"})[0] == 204
                for future in futures:
                    status, _, events, error = future.result(timeout=20)
                    assert status == 200 and error is None and len(events) == 3, (status, events, error)
                    assert events[1] == {"event": "data", "data": "wake"}
                    control(events[2], 4, True)
            for target in (pod, ingress_pod):
                wait_until(lambda: slots(target) == {"chronicle_live_read_available_slots": 32,
                           "chronicle_request_available_slots": 128}, time.monotonic() + 10, "admission release")
                note("released", pod=target, slots=slots(target))
            print(json.dumps({"result": "passed", "scenario": args.scenario, "history": args.output}))
            return

        gate = None if args.scenario == "quorum" else (
            "before-projection-open" if "open-timeout" in args.scenario else "before-response-file-read")
        filename = f"group-{group}.sqlite" if gate == "before-projection-open" else "http-body"
        gate_dir = f"{root}/{filename.encode().hex()}"
        arm, reached, release, resumed = (f"{gate_dir}/{gate}.{suffix}" for suffix in ("arm", "reached", "release", "resumed"))
        if gate:
            kubectl("-n", NAMESPACE, "exec", pod, "--", "mkdir", "-p", gate_dir)
            if any(gate_control(pod, "test", path, check=False).returncode == 0 for path in (arm, reached, release, resumed)):
                raise RuntimeError("refusing pre-existing gate controls")
        target = None
        if args.scenario == "truncate":
            directory = f"/data/group-{group}.projection"
            found = kubectl("-n", NAMESPACE, "exec", pod, "--", "sh", "-c",
                'set -eu; for file in "$1"/.[!.]*; do [ -f "$file" ] || continue; '
                'if [ "$(sha256sum "$file" | cut -d " " -f 1)" = "$2" ]; then printf "%s\\n" "$file"; fi; done',
                "sh", directory, hashlib.sha256(baseline).hexdigest()).stdout.splitlines()
            if len(found) != 1 or pathlib.PurePosixPath(found[0]).parent != pathlib.PurePosixPath(directory):
                raise RuntimeError(f"expected one matching disposable projection, found {found}")
            target = found[0]

        def partition(action):
            result = subprocess.run(["bash", str(ROOT / "ops/partition.sh"), action, ordinal],
                                    capture_output=True, text=True, timeout=30)
            note(action, returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
            result.check_returncode()
        initial = threading.Event()
        def seen(events, _elapsed):
            if baseline == b"" and len(events) == 1:
                control(events[0], 0)
                initial.set()
            return False
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        finished, partition_owned = False, False
        def stream_once():
            started = time.monotonic()
            result = h.stream("gate", {"live": "sse", "offset": "-1"}, seen, 75)
            return result, time.monotonic() - started
        try:
            if gate:
                gate_control(pod, "touch", arm)
            future = executor.submit(stream_once)
            if not baseline:
                assert initial.wait(10), "initial SSE control not observed"
            if args.scenario == "later-open-timeout":
                assert h.request("gate", "POST", b"later", {"content-type": "text/plain"})[0] == 204
            if gate:
                wait_until(lambda: gate_control(pod, "test", reached, check=False).returncode == 0,
                           time.monotonic() + 15, "SSE gate")
                assert not future.done(), "SSE completed before the injected boundary"
                note("gated", gate=gate)
            if args.scenario == "truncate":
                note("truncate", target=target)
                kubectl("-n", NAMESPACE, "exec", pod, "--", "truncate", "--no-create", "-s", "0", target)
            elif args.scenario == "append":
                assert h.request("gate", "POST", b"XYZ", {"content-type": "text/plain", "stream-closed": "true"})[0] == 204
            elif args.scenario == "quorum":
                probe = subprocess.run(["sudo", "docker", "exec", node, "iptables", "-S"],
                                       capture_output=True, text=True, timeout=10)
                assert probe.returncode == 0 and not probe.stderr and "CHRONICLE_FAULT" not in probe.stdout
                partition_owned = True
                partition("isolate")
            if gate and "open-timeout" not in args.scenario:
                gate_control(pod, "touch", release)
            (status, _, events, error), elapsed = future.result(timeout=75)
            if args.scenario == "append":
                assert status == 200 and error is None and len(events) == 4, (status, events, error)
                assert events[0] == {"event": "data", "data": baseline.decode()}
                control(events[1], len(baseline))
                assert events[2] == {"event": "data", "data": "XYZ"}
                control(events[3], len(baseline) + 3, True)
            elif args.scenario == "open-timeout":
                assert status == 503 and not events and error is None, (status, events, error)
            else:
                assert status == 200 and error and "IncompleteRead" in error, (status, events, error)
                assert len(events) == (0 if args.scenario == "truncate" else 1), events
            if "open-timeout" in args.scenario:
                assert 58 <= elapsed <= 70, f"application deadline outside jitter window: {elapsed}"
                current = slots(pod)
                note("deadline-ended-with-actor-held", slots=current, elapsed_s=elapsed)
                assert current == {"chronicle_live_read_available_slots": 31, "chronicle_request_available_slots": 127}, current
            finished = True
        finally:
            try:
                if gate:
                    gate_control(pod, "touch", release, check=False)
                    gate_control(pod, "rm", arm, check=False)
                    if finished:
                        wait_until(lambda: gate_control(pod, "test", resumed, check=False).returncode == 0,
                                   time.monotonic() + 15, "SSE gate release")
                        for path in (reached, resumed, release):
                            gate_control(pod, "rm", path, check=False)
                if partition_owned:
                    partition("heal")
            finally:
                executor.shutdown(wait=True)
        expected = baseline + b"XYZ" if args.scenario == "append" else b"later" if args.scenario == "later-open-timeout" else baseline
        deadline = time.monotonic() + 30
        while True:
            status, _, body = h.request("gate", "GET")
            if status == 200:
                assert body == expected, body
                break
            assert time.monotonic() < deadline, "strict read did not recover"
            time.sleep(.2)
        wait_until(lambda: slots(pod) == {"chronicle_live_read_available_slots": 32,
                   "chronicle_request_available_slots": 128}, time.monotonic() + 10, "admission release")
        note("recovered", slots=slots(pod))
    print(json.dumps({"result": "passed", "scenario": args.scenario, "history": args.output,
                      "scope": "SSE fault-boundary contract, not linearizability or power loss"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--fault-root", required=True)
    parser.add_argument("--scenario", choices=["truncate", "append", "quorum", "open-timeout", "later-open-timeout", "admission"], required=True)
    run(parser.parse_args())
