"""Independent history check with a real, delayed native maintenance fsync.

One supported Tokio worker per process makes executor starvation reproducible.
The test-only interposer blocks one snapshot or journal checkpoint descriptor,
not all node I/O. This is a single-host scheduling/recovery check, not
independent-disk durability.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import traceback

from check_history import check
from lab import BINARY, ROOT, Lab, partition, source_hashes


def check_progress(history, syscalls, checkpoint=False):
    consistency = check(history)
    begin, end = syscalls
    assert begin["fault"] == end["fault"] == "delay-sync"
    assert begin["operation"] == "fsync" and end["operation"] == "delay-end"
    target = "/0/wal/journal-checkpoint-next" if checkpoint else "/0/state/snapshot-"
    assert begin["path"] == end["path"] and target in begin["path"]
    start = begin["seconds"] * 10**9 + begin["nanoseconds"]
    finish = end["seconds"] * 10**9 + end["nanoseconds"]
    assert finish - start >= 1_900_000_000, "storage delay was not exercised"
    during = [e for e in history if e.get("phase") == "delayed" and e["op"] in ("append", "read")]
    completed = [e for e in during if start <= e["start"] <= e["end"] <= finish and e["status"] == 200]
    errors = []
    if any(e["status"] != 200 for e in during):
        errors.append("probed partition request failed during maintenance delay")
    if sum(e["op"] == "append" for e in completed) < 3 or sum(e["op"] == "read" for e in completed) < 3:
        errors.append("fewer than three complete write/read pairs inside the actual syscall delay")
    before = {(e["node"], e["group"]): e["term"] for e in history if e["op"] == "term" and e["phase"] == "before"}
    after = [e for e in history if e["op"] == "term" and e["phase"] == "after"]
    assert len(before) == len(after) == 6
    if any(e["term"] != before[e["node"], e["group"]] or e["leader"] != 1 for e in after):
        errors.append("maintenance disk delay disturbed leadership")
    return dict(verdict="FAIL" if errors else "PASS", consistency=consistency,
                delayed_requests=len(during), completed_inside_delay=len(completed),
                unknowns=sum(e.get("status") == 0 for e in history),
                delay_ns=finish-start, errors=errors)


def run(output, binary=BINARY, binary_provenance=None, same_group=False, checkpoint=False):
    lab = Lab(output, port=19600, binary=binary.resolve())
    interposer = ROOT / ".tmp/electric-tools/snapshot_progress.so"
    source = Path(__file__).with_name("storage_faults.c")
    compile_command = ["cc", "-shared", "-fPIC", "-O2", "-Wall", "-Wextra", "-Werror",
                       "-o", str(interposer), str(source), "-ldl", "-pthread"]
    subprocess.run(compile_command, check=True)
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    if binary_provenance:
        provenance = json.loads(binary_provenance.read_text())
        assert provenance["hashes"]["binary"] == digest, "baseline binary does not match its qualification"
    else:
        assert binary.resolve() == BINARY.resolve(), "alternate binary requires its source provenance"
        provenance = dict(binary=digest, sources=source_hashes())
    (lab.output / "provenance.json").write_text(json.dumps(dict(
        binary=provenance, interposer=hashlib.sha256(interposer.read_bytes()).hexdigest(),
        interposer_source=hashlib.sha256(source.read_bytes()).hexdigest(), compile_command=compile_command,
        driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), workers=1,
        processes=3, partitions=2, progress_group=0 if same_group else 1,
        delayed_target="journal-checkpoint" if checkpoint else "snapshot",
        durability="quorum-fsync", consistency="linearizable default"), indent=2)+"\n")
    paths = [next(f"/snapshot-progress/{i}" for i in range(100) if partition(f"/snapshot-progress/{i}", 2) == g) for g in range(2)]
    history = []
    syscalls = lab.output / "syscalls.jsonl"
    flag = lab.data / "delay"

    def record(event):
        history.append(event)
        with open(lab.output / "history.jsonl", "a") as out:
            out.write(json.dumps(event)+"\n")
        return event

    def start(node):
        environment = dict(LD_PRELOAD=str(interposer), DS_TEST_DATA_ROOT=str(lab.data / str(node)),
                           DS_TEST_FAULT_FILE=str(flag), DS_TEST_FAULT_LOG=str(syscalls)) if node == 1 else None
        lab.start(node, environment, workers=1)

    def operation(node, group, value=None, phase="normal", mode="linearizable"):
        event = dict(op="append" if value is not None else "read", node=node, stream=paths[group],
                     phase=phase, start=time.monotonic_ns())
        headers = {"content-type":"application/octet-stream", "stream-consistency":mode}
        if value is not None:
            event["value"] = value
            headers.update({"producer-id":hashlib.sha256(value.encode()).hexdigest(), "producer-epoch":"0", "producer-seq":"0"})
        else:
            event["mode"] = mode
        try:
            status, received, body = lab.request(node, "POST" if value is not None else "GET",
                paths[group] + ("" if value is not None else "?offset=-1"),
                (value+"\n").encode() if value is not None else b"", headers,
                timeout=0.25 if phase == "delayed" else 12)
            event.update(status=status, headers=received)
            if value is None:
                event["records"] = body.decode().splitlines() if status == 200 else []
        except OSError as error:
            event.update(status=0, headers={}, records=[], error=str(error))
        return record(dict(event, end=time.monotonic_ns()))

    def terms(phase):
        for node in (1, 2, 3):
            for group in (0, 1):
                metrics = lab.admin(node, group, "metrics")
                record(dict(op="term", node=node, group=group, phase=phase,
                            term=metrics["current_term"], leader=metrics["current_leader"]))

    result = dict(verdict="FAIL")
    try:
        for node in (1, 2, 3):
            start(node)
        for group in (0, 1):
            assert lab.admin(1, group, "init", lab.genesis) == {"Ok":None}
            lab.wait(lambda: lab.leader(group) == 1, "co-located initial leaders")
            assert lab.request(1, "PUT", paths[group])[0] == 201
            assert operation(1, group, f"before-{group}-λ")["status"] == 200
        if checkpoint:
            # A forced snapshot must cover enough logs to trigger Raft purge
            # and its native checkpoint, rather than only snapshot creation.
            for index in range(260):
                assert operation(1, 0, f"seed-{index:03}-λ", phase="seed")["status"] == 200
        terms("before")
        target = "/0/wal/journal-checkpoint-next" if checkpoint else "/0/state/snapshot-"
        flag.write_text(f"delay-sync {target}\n")
        with ThreadPoolExecutor(max_workers=1) as pool:
            snapshot = pool.submit(lab.admin, 1, 0, "snapshot")
            lab.wait(lambda: syscalls.exists() and len(syscalls.read_text().splitlines()) >= 1,
                     "actual maintenance fsync intercepted")
            index = 0
            while len(syscalls.read_text().splitlines()) == 1:
                operation(1, 0 if same_group else 1, f"during-{index:03}-λ", phase="delayed")
                operation(1, 0 if same_group else 1, phase="delayed")
                index += 1
                time.sleep(0.1)
            assert snapshot.result() == {"Ok":None}
        time.sleep(0.8)
        terms("after")
        for group in (0, 1):
            leader = lab.wait(lambda: lab.leader(group), "leader after delay")
            assert operation(leader, group, f"after-{group}")["status"] == 200
            assert operation(leader, group)["status"] == 200
        for node in (1, 2, 3):
            lab.stop(node, crash=True)
        for node in (1, 2, 3):
            start(node)
        for group in (0, 1):
            leader = lab.wait(lambda: lab.leader(group), "leader after restart")
            unknowns = [e for e in history if e["op"] == "append" and e["stream"] == paths[group] and e["status"] == 0]
            for event in unknowns:
                assert operation(leader, group, event["value"])["status"] in (200, 204)
            final = operation(leader, group)
            assert final["status"] == 200
            for node in (1, 2, 3):
                lab.wait(lambda: operation(node, group, mode="prefix").get("records") == final["records"],
                         "exact committed prefix on every restarted replica")
        result = check_progress(history, [json.loads(line) for line in syscalls.read_text().splitlines()], checkpoint)
    except BaseException as error:
        result["error"] = repr(error)
        (lab.output / "failure.txt").write_text(traceback.format_exc())
    finally:
        lab.close()
        (lab.output / "result.json").write_text(json.dumps(result, indent=2)+"\n")
    print(json.dumps(result))
    return result["verdict"] == "PASS"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    parser.add_argument("--binary", type=Path, default=BINARY)
    parser.add_argument("--binary-provenance", type=Path)
    parser.add_argument("--same-group", action="store_true", help="Probe apply in the snapshotting group, not the other group")
    parser.add_argument("--checkpoint", action="store_true", help="Delay journal checkpoint fsync instead of snapshot fsync")
    sys.exit(0 if run(**vars(parser.parse_args())) else 1)
