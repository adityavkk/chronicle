"""Inject real file syscalls and corrupt stopped disposable replica storage.

This tests detected I/O errors and fail-closed recovery, NOT power loss, fsync
honesty, independent disks, live bitrot detection, or arbitrary torn-write repair.
The LD_PRELOAD library is test-only and is never linked into benchmark binaries.
"""
import hashlib
import json
from pathlib import Path
import resource
import subprocess
import sys
import time

from check_history import check
from lab import Lab, BINARY, EXPERIMENT, ROOT, partition, source_hashes


def run(output):
    lab = Lab(output, partitions=2, port=19700)
    interposer = ROOT / ".tmp/electric-tools/storage_faults.so"
    compile_command = ["cc", "-shared", "-fPIC", "-O2", "-Wall", "-Wextra", "-Werror",
                       "-o", str(interposer), str(Path(__file__).with_suffix(".c")), "-ldl", "-pthread"]
    subprocess.run(compile_command, check=True)
    path = next(f"/storage/{i}" for i in range(100) if partition(f"/storage/{i}", 2) == 0)
    history = []
    faults = []
    sources = source_hashes()
    (lab.output / "provenance.json").write_text(json.dumps(dict(
        binary=hashlib.sha256(BINARY.read_bytes()).hexdigest(), sources=sources,
        interposer=hashlib.sha256(interposer.read_bytes()).hexdigest(),
        interposer_source=hashlib.sha256(Path(__file__).with_suffix(".c").read_bytes()).hexdigest(),
        compile_command=compile_command, processes=3, partitions=2,
        durability="quorum-fsync", consistency="linearizable default"), indent=2)+"\n")

    def record(event):
        history.append(event)
        with open(lab.output / "history.jsonl", "a") as file:
            file.write(json.dumps(event)+"\n")

    def flag(node):
        return lab.data / f"fault-{node}"

    def start(node):
        lab.start(node, dict(LD_PRELOAD=str(interposer), DS_TEST_DATA_ROOT=str(lab.data / str(node)),
            DS_TEST_FAULT_FILE=str(flag(node)), DS_TEST_FAULT_LOG=str(lab.output / f"syscalls-{node}.jsonl")))

    def read(node, mode="linearizable"):
        event = dict(op="read", stream=path, mode=mode, node=node, start=time.monotonic_ns())
        try:
            status, headers, body = lab.request(node, "GET", path+"?offset=-1", headers={"stream-consistency":mode})
            event.update(status=status, headers=headers, records=body.decode().splitlines() if status == 200 else [])
        except OSError as error:
            event.update(status=0, records=[], error=str(error))
        event["end"] = time.monotonic_ns()
        record(event)
        return event

    def append(node, value):
        event = dict(op="append", stream=path, node=node, value=value, start=time.monotonic_ns())
        try:
            status, headers, body = lab.request(node, "POST", path, (value+"\n").encode(),
                {"content-type":"application/octet-stream", "producer-id":value, "producer-epoch":"0", "producer-seq":"0"})
            event.update(status=status, headers=headers, body=body.decode(errors="replace"))
        except OSError as error:
            event.update(status=0, error=str(error))
        event.update(end=time.monotonic_ns(), outcome="ok" if 200 <= event["status"] < 300 else "unknown")
        record(event)
        return event

    def dead(node):
        pid = int((lab.output / f"node-{node}.pid").read_text())
        proc = Path(f"/proc/{pid}/stat")
        return not proc.exists() or proc.read_text().split(") ", 1)[1].startswith("Z")

    def arm(node, mode, pattern):
        flag(node).write_text(f"{mode} {pattern}\n")
        record(dict(op="fault", kind=mode, node=node, pattern=pattern, start=time.monotonic_ns()))

    def recover(node):
        lab.wait(lambda: dead(node), "storage failure must fail-stop", timeout=25)
        lab.stop(node)
        flag(node).unlink(missing_ok=True)
        leader = lab.wait(lambda: lab.leader(0), "elect surviving majority")
        before = read(leader)
        assert before["status"] == 200
        start(node)
        lab.wait(lambda: read(node, "prefix")["records"] == before["records"], "recover committed prefix")
        return leader

    def corrupt_probe(node, target, replacement, label, expected):
        # All supervised processes are stopped before modifying authoritative files.
        original = target.read_bytes()
        target.write_bytes(replacement)
        result = dict(kind=label, path=str(target), original_sha256=hashlib.sha256(original).hexdigest(),
                      mutated_sha256=hashlib.sha256(replacement).hexdigest(), expected_error=expected)
        try:
            run = subprocess.run([str(BINARY), "--cluster-config", str(lab.output / f"node-{node}.json")],
                capture_output=True, timeout=10, preexec_fn=lambda: resource.setrlimit(resource.RLIMIT_CORE, (0, 0)))
            (lab.output / f"{label}.txt").write_bytes(run.stdout+run.stderr)
            result.update(exit_code=run.returncode, rejected=run.returncode != 0 and expected.encode() in run.stderr)
            faults.append(result)
            assert result["rejected"], result
        finally:
            target.write_bytes(original)  # Restore only this disposable fault fixture.

    try:
        for node in (1, 2, 3):
            start(node)
        lab.initialize()
        leader = lab.leader(0)
        assert lab.request(leader, "PUT", path)[0] == 201
        for i in range(5):
            assert 200 <= append(leader, f"baseline-{i}")["status"] < 300
        arm(leader, "short-write", "/0/wal/")
        assert 200 <= append(leader, "short-write-retried")["status"] < 300
        assert not flag(leader).exists(), "the short-write hook must actually fire"
        assert read(leader)["records"].count("short-write-retried") == 1
        for mode, pattern, value in [("eio-sync", "/0/wal/", "wal-sync-unknown"),
                                     ("enospc-write", "/0/wal/", "wal-space-unknown"),
                                     ("enospc-write", "/0/state/hot/streams/", "hot-space-unknown")]:
            leader = lab.wait(lambda: lab.leader(0), "fault target leader")
            arm(leader, mode, pattern)
            result = append(leader, value)
            intercepted = [json.loads(line) for line in (lab.output / f"syscalls-{leader}.jsonl").read_text().splitlines()]
            assert any(e["fault"] == mode and pattern in e["path"] for e in intercepted), "fault was not injected"
            assert not 200 <= result["status"] < 300, "storage failure produced false acknowledgement"
            recover(leader)
            leader = lab.wait(lambda: lab.leader(0), "healthy leader")
            # Retry the unknown operation with its original producer identity.
            assert 200 <= append(leader, value)["status"] < 300
            assert read(leader)["records"].count(value) == 1

        leader = lab.leader(0)
        assert lab.admin(leader, 0, "snapshot") == {"Ok":None}
        old_snapshot = lab.wait(lambda: lab.admin(leader, 0, "metrics").get("snapshot"), "durable initial snapshot")
        assert 200 <= append(leader, "after-good-snapshot")["status"] < 300
        prior = set((lab.data / str(leader) / "0/state").glob("snapshot-*"))
        arm(leader, "eio-sync", "/0/state/snapshot-")
        try:
            lab.admin(leader, 0, "snapshot")  # Scheduling success is not snapshot durability.
        except OSError:
            pass
        lab.wait(lambda: dead(leader), "snapshot fsync must fail-stop")
        failed = set((lab.data / str(leader) / "0/state").glob("snapshot-*")) - prior
        assert len(failed) == 1, failed
        failed_file = failed.pop()
        failed_file.write_bytes(b"invalid, unreferenced failed snapshot")
        recover(leader)
        assert lab.admin(leader, 0, "metrics")["snapshot"] == old_snapshot
        faults.append(dict(kind="failed-snapshot-not-published", path=str(failed_file), recovered_snapshot=old_snapshot))
        final = read(lab.leader(0))
        assert final["status"] == 200
        for node in sorted(lab.nodes.copy()):
            lab.stop(node, crash=True)

        hot = next(p for p in (lab.data / str(leader) / "0/state/hot/streams").iterdir()
                   if not p.name.startswith(".") and p.suffix != ".meta")
        original_hot = hot.read_bytes()
        wal = lab.data / str(leader) / "0/wal/1.wal"
        original = wal.read_bytes()
        for label, offset, expected in [("wal-header-crc", 4, "corrupt or non-consensus journal frame"),
                                        ("wal-payload-crc", 38, "corrupt or non-consensus journal frame")]:
            mutated = bytearray(original)
            mutated[offset] ^= 1
            corrupt_probe(leader, wal, mutated, label, expected)
        mutated = bytearray(original)
        mutated[:38] = bytes(38)
        corrupt_probe(leader, wal, mutated, "wal-zero-hole", "nonzero suffix behind journal hole")
        end = 0
        while original[end:end+38] != bytes(38):
            end += 38 + int.from_bytes(original[end:end+4], "little")
        corrupt_probe(leader, wal, original[:end-1], "wal-torn-tail", "truncated/oversize journal record")
        good_snapshot = next(p for p in prior if p != failed_file)
        mutated = bytearray(good_snapshot.read_bytes())
        mutated[-1] ^= 1
        corrupt_probe(leader, good_snapshot, mutated, "snapshot-digest", "snapshot digest mismatch")
        # A hot file is only a materialization. Startup must rebuild even bytes
        # whose length is unchanged. This does NOT assert detection while live.
        # The rejected snapshot boot already removed this disposable generation.
        hot.parent.mkdir(parents=True, exist_ok=True)
        hot.write_bytes(bytes([0xDD])*len(original_hot))
        faults.append(dict(kind="hot-rebuilt", path=str(hot), bytes=len(original_hot)))
        for node in (1, 2, 3):
            start(node)
        lab.wait(lambda: lab.leader(0), "full storage restart")
        lab.wait(lambda: read(leader, "prefix")["records"] == final["records"], "hot bytes rebuilt from authority")
        assert read(lab.leader(0))["records"] == final["records"]
        intercepted = [json.loads(line) for p in lab.output.glob("syscalls-*.jsonl") for line in p.read_text().splitlines()]
        for mode, pattern in [("short-write","/wal/"), ("eio-sync","/wal/"), ("enospc-write","/wal/"),
                              ("enospc-write","/hot/"), ("eio-sync","/snapshot-")]:
            assert any(e["fault"] == mode and pattern in e["path"] for e in intercepted), (mode, pattern)
        verdict = dict(verdict="PASS", checker=check(history), storage_checks=faults,
                       intercepted_syscalls=len(intercepted), unknown_outcomes=sum(e.get("outcome") == "unknown" for e in history),
                       scope="single-host actual native storage syscall faults and offline corruption; not power-loss durability")
        (lab.output / "result.json").write_text(json.dumps(verdict, indent=2)+"\n")
        print(json.dumps(verdict, indent=2))
    except BaseException as error:
        (lab.output / "result.json").write_text(json.dumps(dict(verdict="FAIL", error=repr(error), storage_checks=faults), indent=2)+"\n")
        raise
    finally:
        lab.close()


if __name__ == "__main__":
    run(sys.argv[1])
