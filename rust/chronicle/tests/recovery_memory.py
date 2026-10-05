#!/usr/bin/env python3
"""Measure cold recovery of a stopped PVC copy, with no consensus network.

Never initializes storage or joins a cluster. The caller supplies a new output
directory and an immutable data-chronicle-0.tar.gz capture. Docker enforces the
specified memory limit; exit/OOM state and sampled cgroup peak are retained.
"""
import argparse
import json
import pathlib
import subprocess
import tarfile
import time


def docker(*args):
    return subprocess.check_output(["sudo", "docker", *args], text=True, timeout=30)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--memory", required=True, choices=("512m", "1536m"))
    args = parser.parse_args()
    args.output.mkdir()
    data = args.output.resolve() / "data"
    data.mkdir()
    with tarfile.open(args.archive) as archive:
        archive.extractall(data, filter="data")
    seeds = {str(i): {"addr": f"chronicle-{i-1}.chronicle:8080", "zone": chr(96+i), "draining": False}
             for i in range(1, 4)}
    name = "chronicle-memory-" + str(time.monotonic_ns())
    docker("create", "--name", name, "--pull=never", "--network=none", "--cpus=1",
           "--memory=" + args.memory, "--memory-swap=" + args.memory,
           "--mount", f"type=bind,src={data},dst=/data",
           "-e", "NODE_ID=1", "-e", "DATA_DIR=/data", "-e", "CLUSTER_ID=chronicle-k3d-disposable-v1",
           "-e", "CLUSTER_NODES=" + json.dumps(seeds), args.image)
    docker("cp", "/usr/bin/busybox", name + ":/tmp/busybox")
    started = time.monotonic()
    ready_at = None
    peak = 0
    try:
        with (args.output / "process.txt").open("x") as log, (args.output / "samples.jsonl").open("x") as samples:
            process = subprocess.Popen(["sudo", "docker", "start", "--attach", name], stdout=log, stderr=log)
            while time.monotonic() - started < 30:
                inspected = json.loads(docker("inspect", name))[0]
                state = inspected["State"]
                if state["Pid"]:
                    try:
                        group = pathlib.Path(f"/proc/{state['Pid']}/cgroup").read_text().strip().split("::", 1)[1]
                        peak = max(peak, int(pathlib.Path("/sys/fs/cgroup" + group + "/memory.peak").read_text()))
                    except FileNotFoundError:
                        continue  # Observe Docker's terminal state on the next iteration.
                    probe = subprocess.run(["sudo", "docker", "exec", name, "/tmp/busybox", "wget",
                        "-qO-", "-T", "1", "--header", "x-chronicle-cluster: chronicle-k3d-disposable-v1",
                        "--header", "x-chronicle-recipient: 1", "http://127.0.0.1:8080/admin/retirement-state"],
                        capture_output=True, text=True)
                    state["probe_code"] = probe.returncode
                    state["probe_error"] = probe.stderr[-500:]
                    if probe.returncode == 0 and ready_at is None:
                        ready_at = time.monotonic() - started
                        (args.output / "recovered-status.json").write_text(probe.stdout)
                samples.write(json.dumps({"elapsed_s": time.monotonic() - started, "peak_bytes": peak, "state": state}) + "\n")
                samples.flush()
                if state["Status"] == "exited" or ready_at is not None and time.monotonic() - started >= ready_at + 5:
                    break
                time.sleep(.1)
            inspected = json.loads(docker("inspect", name))[0]
            result = {"image": inspected["Image"], "limit": args.memory, "peak_bytes": peak,
                      "ready_after_s": ready_at, "state": inspected["State"],
                      "scope": "network-isolated recovery of one copied PVC, not quorum readiness or a capacity bound"}
            (args.output / "result.json").write_text(json.dumps(result, indent=2))
            print(json.dumps(result, indent=2))
            docker("stop", "--time=1", name)
            process.wait(timeout=10)
    finally:
        docker("rm", "--force", name)


if __name__ == "__main__":
    main()
