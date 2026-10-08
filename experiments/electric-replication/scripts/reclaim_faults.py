"""Real checkpoint syscall failures, physical unlink and full-process restart.

The append-prefix checker is independent. Exact filler byte probes separately
exercise multi-segment storage. This is one shared disk, not power-loss testing.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

from check_history import check
from lab import Lab, BINARY, EXPERIMENT, ROOT, partition, source_hashes


def run(output):
    lab = Lab(output, port=19900)
    interposer = ROOT / ".tmp/electric-tools/reclaim_faults.so"
    source = Path(__file__).with_name("storage_faults.c")
    command = ["cc", "-shared", "-fPIC", "-O2", "-Wall", "-Wextra", "-Werror",
               "-o", str(interposer), str(source), "-ldl", "-pthread"]
    subprocess.run(command, check=True)
    (lab.output / "provenance.json").write_text(json.dumps(dict(
        binary=hashlib.sha256(BINARY.read_bytes()).hexdigest(),
        sources=source_hashes(),
        driver=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        interposer_source=hashlib.sha256(source.read_bytes()).hexdigest(),
        interposer=hashlib.sha256(interposer.read_bytes()).hexdigest(), compile_command=command,
        processes=3, partitions=2, durability="quorum-fsync", consistency="linearizable default"), indent=2)+"\n")
    paths = [f"/reclaim/{i}" for i in range(10) if partition(f"/reclaim/{i}",2) == 0]
    path, filler = paths[:2]
    payload = bytes((i*73+29) % 256 for i in range(65536))
    chunks = 0
    history, probes = [], []

    def emit(event):
        history.append(event)
        with open(lab.output / "history.jsonl","a") as file:
            file.write(json.dumps(event)+"\n")

    def flag(node):
        return lab.data / f"fault-{node}"

    def start(node):
        lab.start(node, dict(LD_PRELOAD=str(interposer), DS_TEST_DATA_ROOT=str(lab.data / str(node)),
            DS_TEST_FAULT_FILE=str(flag(node)), DS_TEST_FAULT_LOG=str(lab.output / f"syscalls-{node}.jsonl")))

    def append(node, value):
        event = dict(op="append", stream=path, value=value, node=node, start=time.monotonic_ns())
        try:
            status, headers, _ = lab.request(node,"POST",path,(value+"\n").encode(),
                {"content-type":"application/octet-stream","producer-id":value,"producer-epoch":"0","producer-seq":"0"})
            event.update(status=status,headers=headers)
        except OSError as error:
            event.update(status=0,error=repr(error))
        event.update(end=time.monotonic_ns(), outcome="ok" if 200 <= event["status"] < 300 else "unknown")
        emit(event)
        assert event["outcome"] == "ok", event

    def read(node, mode="linearizable"):
        event = dict(op="read", stream=path, node=node, mode=mode, start=time.monotonic_ns())
        status, headers, data = lab.request(node,"GET",path+"?offset=-1",headers={"stream-consistency":mode})
        event.update(status=status, headers=headers, records=data.decode().splitlines() if status == 200 else [],
                     end=time.monotonic_ns())
        emit(event)
        assert status == 200, event
        return event["records"]

    def fill(node, count):
        nonlocal chunks
        for _ in range(count):
            event = dict(op="filler-append", stream=filler, node=node, start=time.monotonic_ns(),
                         chunk=chunks, bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest())
            try:
                status, _, _ = lab.request(node,"POST",filler,payload,{"content-type":"application/octet-stream"})
                event["status"] = status
            except OSError as error:
                event.update(status=0,error=repr(error))
            event.update(end=time.monotonic_ns(), outcome="ok" if 200 <= event["status"] < 300 else "unknown")
            emit(event)
            assert event["outcome"] == "ok", event
            chunks += 1

    def probe(node):
        status, _, data = lab.request(node,"GET",filler+"?offset=-1",headers={"stream-consistency":"prefix"})
        assert status == 200 and data == payload*chunks
        probes.append(dict(node=node, bytes=len(data), sha256=hashlib.sha256(data).hexdigest()))

    def dead(node):
        proc = Path(f"/proc/{(lab.output / f'node-{node}.pid').read_text().strip()}/stat")
        return not proc.exists() or proc.read_text().split(") ",1)[1].startswith("Z")

    try:
        for node in (1,2,3):
            start(node)
        lab.initialize()
        owner = lab.leader(0)
        for name in (path,filler):
            assert lab.request(owner,"PUT",name)[0] == 201
        fill(owner,260)
        for sequence, (mode, pattern) in enumerate([
            ("enospc-write","/0/wal/journal-checkpoint-next"),
            ("eio-sync","/0/wal/journal-checkpoint-next"),
            ("eio-dir-sync","/0/wal"),
        ]):
            owner = lab.wait(lambda:lab.leader(0),"owner before checkpoint fault")
            if sequence:
                fill(owner,80)
            append(owner,f"before-checkpoint-{sequence}")
            expected = read(owner)
            old = set((lab.data / str(owner) / "0/wal").glob("*.wal"))
            assert len(old) >= 2
            emit(dict(op="fault", node=owner, kind=mode, pattern=pattern, start=time.monotonic_ns()))
            flag(owner).write_text(f"{mode} {pattern}\n")
            assert lab.admin(owner,0,"snapshot") == {"Ok":None}
            lab.wait(lambda:dead(owner),"checkpoint failure must fail-stop",timeout=25)
            assert all(p.exists() for p in old), "unlink ran despite failed checkpoint publication"
            faults = [json.loads(s) for s in (lab.output / f"syscalls-{owner}.jsonl").read_text().splitlines()]
            assert any(f["fault"] == mode and pattern in f["path"] for f in faults)
            lab.stop(owner)
            flag(owner).unlink()
            survivor = lab.wait(lambda:lab.leader(0),"surviving quorum")
            assert read(survivor) == expected
            start(owner)
            lab.wait(lambda:read(owner,"prefix") == expected,"restart through checkpoint boundary")
            probe(owner)

        owner = lab.wait(lambda:lab.leader(0),"healthy owner")
        fill(owner,160)
        append(owner,"after-checkpoint-faults")
        expected = read(owner)
        for node in (1,2,3):
            lab.wait(lambda:read(node,"prefix") == expected,"replica catchup")
            cut = lab.admin(node,0,"metrics")["last_applied"]["index"]
            assert lab.admin(node,0,"snapshot") == {"Ok":None}
            # The RPC schedules a build. Prior/restart reclamation may already
            # have removed 1.wal, so that cannot prove this build has completed.
            snapshot = lab.wait(lambda: (s := lab.admin(node,0,"metrics").get("snapshot"))
                and s["index"] >= cut and s,
                "snapshot covers current applied prefix")
            emit(dict(op="snapshot-ready", node=node, minimum_index=cut,
                      observed_index=snapshot["index"], start=time.monotonic_ns()))
            directory = lab.data / str(node) / "0/wal"
            lab.wait(lambda: (directory / "journal-checkpoint").exists() and not (directory / "1.wal").exists(),
                     "physical old-segment reclamation")
            snapshots = list((lab.data / str(node) / "0/state").glob("snapshot-*"))
            assert len(snapshots) == 1
            probe(node)
        for node in (1,2,3):
            lab.stop(node,crash=True)
        for node in (1,2,3):
            start(node)
        owner = lab.wait(lambda:lab.leader(0),"election from reclaimed WAL")
        assert read(owner) == expected
        append(owner,"after-checkpoint-faults")  # Retry deduplicates after snapshots/replay.
        assert read(owner) == expected
        for node in (1,2,3):
            probe(node)
        result = dict(verdict="PASS", checker=check(history), filler_probes=probes,
                      scope="real checkpoint write/fsync/directory-fsync errors, physical reclamation, single-host restart")
        (lab.output / "result.json").write_text(json.dumps(result,indent=2)+"\n")
        print(json.dumps(result,indent=2))
    except BaseException as error:
        (lab.output / "result.json").write_text(json.dumps(dict(verdict="FAIL",error=repr(error),filler_probes=probes),indent=2)+"\n")
        raise
    finally:
        lab.close()


if __name__ == "__main__":
    run(sys.argv[1])
