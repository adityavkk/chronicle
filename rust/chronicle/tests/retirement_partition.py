#!/usr/bin/env python3
"""History hook: repair without an old replica, then verify retirement after healing."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
K = str(ROOT / "ops/kubectl.sh")


def command(*args):
    return subprocess.check_output(args, text=True, timeout=30)


def api(path, value=None):
    # A healthy seed, not the load-balanced service containing the isolated pod.
    if value is None:
        return json.loads(command(K, "get", "--raw",
            "/api/v1/namespaces/chronicle/pods/chronicle-0:8080/proxy" + path))
    ip = command(K, "-n", "chronicle", "get", "pod", "chronicle-0", "-o", "jsonpath={.status.podIP}")
    # kubectl create --raw does not supply the admin endpoint's JSON media type.
    # BusyBox wget in the pinned k3d server issues this POST once; no retry loop.
    return json.loads(command("sudo", "docker", "exec", "k3d-chronicle-rust-server-0",
        "wget", "-qO-", "-T", "15", "--header", "Content-Type: application/json",
        "--post-data", json.dumps(value), f"http://{ip}:8080{path}"))


def wait(predicate, label):
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (subprocess.SubprocessError, json.JSONDecodeError):
            pass  # Only read predicates are retried; mutations remain single-shot.
        time.sleep(1)
    raise TimeoutError(label)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", type=int, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.node <= 3:
        parser.error("requires a non-seed disposable replica")
    pod = f"chronicle-{args.node - 1}"
    node = command(K, "-n", "chronicle", "get", "pod", pod, "-o", "jsonpath={.spec.nodeName}")
    ordinal = node.removeprefix("k3d-chronicle-rust-agent-")
    if not re.fullmatch(r"[0-9]+(?:-[0-9]+)?", ordinal):
        raise RuntimeError("unexpected agent")
    with open(args.output, "x", encoding="utf-8") as output:
        def note(phase, **data):
            output.write(json.dumps({"phase": phase, "time_ns": time.monotonic_ns(), **data}) + "\n")
            output.flush()

        def partition(action):
            result = subprocess.run(["bash", str(ROOT / "ops/partition.sh"), action, ordinal],
                                    capture_output=True, text=True, timeout=30)
            note(action, code=result.returncode, stdout=result.stdout, stderr=result.stderr)
            result.check_returncode()

        state = api("/admin/control")
        registered = state["nodes"][str(args.node)]
        if not registered["draining"] or api(f"/admin/retirement/{args.node}") is not True:
            raise RuntimeError("requires an already verified drained replica; do not blindly repeat")
        if {int(id_) for id_, value in state["nodes"].items() if not value["draining"]} != {1, 2, 3}:
            raise RuntimeError("requires the three healthy seeds as the only eligible replicas")
        registered["draining"] = False
        api("/admin/register", [args.node, registered])
        def assigned():
            state = api("/admin/control")
            placements = state["placements"].values()
            # With three seeds and one later identity, stable rotation assigns
            # that identity to groups 1..3. Wait for all three, not a transient
            # completed placement between successive joins.
            return state if all(p["complete"] for p in placements) and all(
                args.node in state["placements"][str(g)]["voters"] for g in (1, 2, 3)) else None
        note("assigned", state=wait(assigned, "node assignment"))
        probe = command("sudo", "docker", "exec", node, "iptables", "-S")
        if "CHRONICLE_FAULT" in probe:
            raise RuntimeError("refusing existing partition")
        # From this point even a partially applied injection belongs to this test.
        try:
            partition("isolate")
            registered["draining"] = True
            api("/admin/register", [args.node, registered])
            def repaired():
                state = api("/admin/control")
                return state if all(p["complete"] and args.node not in p["voters"]
                                    for p in state["placements"].values()) else None
            state = wait(repaired, "voter repair without old replica")
            retired = api(f"/admin/retirement/{args.node}")
            note("repaired-but-unverified", state=state, retired=retired)
            assert retired is False, "unreachable replica reported gracefully retired"
        finally:
            partition("heal")
        wait(lambda: api(f"/admin/retirement/{args.node}") is True, "retirement after healing")
        note("verified-after-heal", state=api("/admin/control"))


if __name__ == "__main__":
    main()
