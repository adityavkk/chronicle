"""Disposable, supervised local replica processes. No cloud calls or Docker."""
import gzip
import hashlib
import http.client
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import time

ROOT = Path(__file__).resolve().parents[3]
EXPERIMENT = ROOT / "experiments/electric-replication"
BINARY = EXPERIMENT / "engine/target/release/durable-streams-server"


def source_hashes():
    paths = [EXPERIMENT / name for name in (
        "engine/Cargo.toml", "engine/Cargo.lock", "vendor/openraft/Cargo.toml",
        "vendor/openraft/LICENSE-APACHE", "vendor/openraft/LICENSE-MIT")]
    for root in ("engine/src", "vendor/openraft/src"):
        paths.extend(p for p in (EXPERIMENT / root).rglob("*") if p.is_file())
    return {str(p.relative_to(EXPERIMENT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}


class Lab:
    def __init__(self, output, replicas=3, partitions=2, port=19300, binary=BINARY):
        self.output = Path(output).resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.data = ROOT / ".tmp/electric-labs" / self.output.name
        self.data.mkdir(parents=True, exist_ok=False)
        self.cluster = self.output.name.replace("_", "-")
        self.port = port
        self.partitions = partitions
        self.binary = binary
        self.nodes = set()
        self.genesis = {str(n): {"addr": f"127.0.0.1:{port+n}"} for n in range(1, replicas+1)}

    def start(self, node, environment=None, cpus=None, fault_testing=True, stats_secs=0,
              pending_commands=256, pending_bytes=16 * 1024 * 1024, append_durability="quorum-fsync",
              heap_profile=False, workers=2):
        config = dict(cluster=self.cluster, node=node, listen=f"127.0.0.1:{self.port+node}",
                      dir=str(self.data / str(node)), partitions=self.partitions,
                      workers=workers, long_poll_ms=1000, fault_testing=fault_testing, genesis=self.genesis,
                      stats_secs=stats_secs, pending_commands=pending_commands, pending_bytes=pending_bytes,
                      append_durability=append_durability)
        path = self.output / f"node-{node}.json"
        path.write_text(json.dumps(config, indent=2) + "\n")
        with open(self.output / "config-history.jsonl", "a") as history:
            history.write(json.dumps(dict(unix_ms=time.time_ns()//1_000_000, config=config))+"\n")
        env = "env " + " ".join(shlex.quote(f"{k}={v}") for k,v in environment.items()) + " " if environment else ""
        affinity = f"taskset -c {shlex.quote(cpus)} " if cpus else ""
        profile = f"heaptrack -o {shlex.quote(str(self.output / f'heap-node-{node}'))} " if heap_profile else ""
        command = (f"ulimit -c 0; exec {env}{affinity}{profile}{shlex.quote(str(self.binary))} --cluster-config {shlex.quote(str(path))} "
                   f">> {shlex.quote(str(self.output / f'node-{node}.log'))} 2>&1")
        subprocess.run(["amp", "orb", "service", "start", f"{self.cluster}-{node}",
                        "--command", command, "--cwd", str(ROOT), "--port", str(self.port+node)],
                       check=True, stdout=subprocess.DEVNULL)
        self.nodes.add(node)
        self.wait(lambda: self.request(node, "GET", "/health")[0] == 200, "start node")
        matches = []
        for process in Path("/proc").iterdir():
            if process.name.isdecimal():
                try:
                    args = (process / "cmdline").read_bytes().split(b"\0")
                    if args[:3] == [str(self.binary).encode(), b"--cluster-config", str(path).encode()]:
                        matches.append(int(process.name))
                except (FileNotFoundError, PermissionError):
                    pass
        assert len(matches) == 1, matches
        (self.output / f"node-{node}.pid").write_text(str(matches[0])+"\n")

    def stop(self, node, crash=False):
        if crash:
            pid = int((self.output / f"node-{node}.pid").read_text())
            command = Path(f"/proc/{pid}/cmdline").read_bytes()
            assert str(self.binary).encode() in command, "refuse to kill unrelated PID"
            os.kill(pid, signal.SIGKILL)
        subprocess.run(["amp", "orb", "service", "stop", f"{self.cluster}-{node}"],
                       check=True, stdout=subprocess.DEVNULL)
        self.nodes.discard(node)

    def close(self):
        for node in sorted(self.nodes.copy()):
            self.stop(node)
        logs = {}
        for path in self.output.glob("node-*.log"):
            with open(path, "rb") as source, open(str(path)+".gz", "wb") as target:
                with gzip.GzipFile(filename="", mode="wb", fileobj=target, mtime=0) as archive:
                    shutil.copyfileobj(source, archive)
            with open(path, "rb") as source:
                logs[path.name] = hashlib.file_digest(source, "sha256").hexdigest()
            with open(str(path)+".gz", "rb") as source:
                logs[path.name+".gz"] = hashlib.file_digest(source, "sha256").hexdigest()
        (self.output / "log-sha256.json").write_text(json.dumps(logs, indent=2)+"\n")

    def request(self, node, method, path, body=b"", headers=None, timeout=12, connection=None):
        connection = connection or http.client.HTTPConnection("127.0.0.1", self.port+node, timeout=timeout)
        try:
            connection.request(method, path, body, headers or {})
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def admin(self, node, group, action, value=None):
        status, _, body = self.request(node, "POST", f"/_admin/{group}/{action}",
                                      json.dumps(value).encode(), {"x-electric-cluster": self.cluster})
        assert status == 200, (status, body)
        return json.loads(body)

    def faults(self, node, blocked=(), delay_ms=0):
        result = self.request(node, "POST", "/_admin/network",
                              json.dumps(dict(blocked=list(blocked), delay_ms=delay_ms)).encode(),
                              {"x-electric-cluster": self.cluster})
        assert result[0] == 200, result

    def leader(self, group, candidates=None):
        for node in sorted(candidates or self.nodes):
            try:
                metrics = self.admin(node, group, "metrics")
                if metrics["state"] == "Leader" and metrics["current_leader"] == node:
                    return node
            except (OSError, AssertionError, ValueError):
                continue
        return None

    @staticmethod
    def wait(predicate, description, timeout=20):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            try:
                value = predicate()
                if value:
                    return value
            except (OSError, AssertionError):
                pass
            time.sleep(0.1)
        raise AssertionError(f"timeout: {description}")

    def initialize(self):
        for group in range(self.partitions):
            node = group % len(self.genesis) + 1
            assert self.admin(node, group, "init", self.genesis) == {"Ok": None}
            self.wait(lambda: self.leader(group), "elect initial leader")


def partition(path, count):
    value = 0xCBF29CE484222325
    for byte in path.encode():
        value = ((value ^ byte) * 0x100000001B3) & ((1 << 64) - 1)
    return value % count
