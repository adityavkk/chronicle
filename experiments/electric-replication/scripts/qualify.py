"""Real TCP processes, native WAL/disks, retained unknown outcomes. One host only."""
import concurrent.futures
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import threading
import time

from lab import Lab, BINARY, partition


def run(output):
    lab = Lab(output)
    history = open(lab.output / "history.jsonl", "x", buffering=1)
    lock = threading.Lock()
    paths = [next(f"/history/{i}" for i in range(100) if partition(f"/history/{i}", 2) == g) for g in range(2)]

    def emit(event):
        with lock:
            history.write(json.dumps(event) + "\n")

    def append(node, group, value):
        event = dict(op="append", node=node, stream=paths[group], value=value, start=time.monotonic_ns())
        try:
            status, headers, body = lab.request(node, "POST", paths[group], (value+"\n").encode(),
                {"content-type": "application/octet-stream", "producer-id": value, "producer-epoch": "0", "producer-seq": "0"})
            event.update(status=status, headers=headers, error=body.decode(errors="replace"))
        except OSError as exc:
            event.update(status=0, headers={}, error=str(exc))
        event["end"] = time.monotonic_ns()
        event["outcome"] = "ok" if 200 <= event["status"] < 300 else "unknown"
        emit(event)
        return event

    def read(node, group, mode="linearizable", token=None, required=()):
        headers = {"stream-consistency": mode}
        if token:
            headers["stream-session"] = token
        event = dict(op="read", node=node, stream=paths[group], mode=mode, required=list(required), start=time.monotonic_ns())
        try:
            status, response_headers, body = lab.request(node, "GET", paths[group]+"?offset=-1", headers=headers)
            event.update(status=status, records=body.decode().splitlines() if status == 200 else [], headers=response_headers)
        except OSError as exc:
            event.update(status=0, records=[], error=str(exc))
        event["end"] = time.monotonic_ns()
        emit(event)
        return event

    try:
        (lab.output / "binary.sha256").write_text(hashlib.sha256(BINARY.read_bytes()).hexdigest()+"\n")
        for node in (1, 2, 3):
            lab.start(node)
        lab.initialize()
        for group in range(2):
            leader = lab.leader(group)
            assert lab.request(leader, "PUT", paths[group])[0] == 201
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(append, lab.leader(i % 2), i % 2, f"base-{i:03d}") for i in range(24)]
            assert all(200 <= f.result()["status"] < 300 for f in futures)
        for group in range(2):
            assert read(lab.leader(group), group)["status"] == 200

        leader = lab.leader(0)
        others = lab.nodes - {leader}
        emit(dict(op="fault", kind="partition", isolated=leader, start=time.monotonic_ns()))
        lab.faults(leader, others)
        for node in others:
            lab.faults(node, [leader])
        unknown = append(leader, 0, "minority-proposal")
        assert not 200 <= unknown["status"] < 300, unknown
        assert "minority-proposal" not in read(leader, 0, "prefix")["records"]
        assert read(leader, 0)["status"] == 503
        new = lab.wait(lambda: lab.leader(0, others), "majority leader")
        ack = append(new, 0, "majority-ack")
        assert 200 <= ack["status"] < 300, ack
        token = ack["headers"]["stream-session"]
        assert read(leader, 0, "session", token)["status"] == 503
        for node in lab.nodes:
            lab.faults(node)
        emit(dict(op="fault", kind="heal", start=time.monotonic_ns()))
        lab.wait(lambda: read(leader, 0, "session", token, ["majority-ack"])["status"] == 200, "session catchup")
        assert 200 <= append(lab.leader(0), 0, "majority-ack")["status"] < 300
        # Different group/cluster and impossible future tokens fail closed.
        assert read(new, 1, "session", token)["status"] == 400
        assert read(new, 0, "session", f"{lab.cluster}:0:999999999")["status"] == 503
        for node in lab.nodes:
            lab.faults(node, delay_ms=120)
        # A delay can exceed Raft's RPC deadline and legitimately prevent ack.
        # Preserve that unknown outcome, then retry the SAME producer identity.
        append(lab.leader(0), 0, "delayed-links")
        for node in lab.nodes:
            lab.faults(node)
        lab.wait(lambda: lab.leader(0), "election after delayed links")
        lab.wait(lambda: (owner := lab.leader(0)) is not None
                 and 200 <= append(owner, 0, "delayed-links")["status"] < 300,
                 "delayed proposal resolves after heal")

        leader = lab.leader(0)
        emit(dict(op="fault", kind="SIGKILL", node=leader, start=time.monotonic_ns()))
        lab.stop(leader, crash=True)
        new = lab.wait(lambda: lab.leader(0), "leader after crash")
        assert "majority-ack" in read(new, 0)["records"]
        assert 200 <= append(new, 0, "after-crash")["status"] < 300
        lab.start(leader)
        lab.wait(lambda: "after-crash" in read(leader, 0, "prefix")["records"], "recovered committed suffix")

        # Snapshot + learner install + joint membership; group 1 is independent.
        current = lab.leader(0)
        for i in range(80):
            assert 200 <= append(current, 0, f"snapshot-{i:03d}")["status"] < 300
        assert lab.admin(current, 0, "snapshot") == {"Ok": None}
        lab.wait(lambda: lab.admin(current, 0, "metrics").get("snapshot"), "snapshot durable")
        lab.start(4)
        learner = lab.admin(current, 0, "learner", [4, {"addr": f"127.0.0.1:{lab.port+4}"}])
        assert "Ok" in learner, learner
        voters = sorted(({1, 2, 3} - {current}) | {4})
        changed = lab.admin(current, 0, "membership", voters)
        assert "Ok" in changed, changed
        new = lab.wait(lambda: lab.leader(0, set(voters)), "leader after movement")
        assert 200 <= append(new, 0, "after-move")["status"] < 300
        assert read(4, 0, "prefix")["status"] == 200
        assert read(lab.leader(1, {1, 2, 3}), 1)["status"] == 200
        final = read(new, 0)
        assert final["records"].count("majority-ack") == 1
        # Every process loses all RAM. Rebuild native files from snapshot + WAL.
        active = lab.nodes.copy()
        for node in sorted(active):
            lab.stop(node, crash=True)
        for node in sorted(active):
            lab.start(node)
        new = lab.wait(lambda: lab.leader(0, set(voters)), "full restart")
        assert read(new, 0)["records"] == final["records"]
        assert 200 <= append(new, 0, "majority-ack")["status"] < 300
        assert read(new, 0)["records"] == final["records"]
        assert read(lab.wait(lambda: lab.leader(1, {1, 2, 3}), "group 1 restart"), 1)["status"] == 200
        history.close()
        check = subprocess.run([sys.executable, str(Path(__file__).with_name("check_history.py")),
                                str(lab.output / "history.jsonl")], capture_output=True, text=True)
        (lab.output / "checker.txt").write_text(check.stdout+check.stderr)
        assert check.returncode == 0, check.stdout+check.stderr
        (lab.output / "result.json").write_text(json.dumps(dict(verdict="PASS", scope="single-host real TCP/native WAL", checker=json.loads(check.stdout)), indent=2)+"\n")
        print(check.stdout)
    except BaseException as error:
        (lab.output / "result.json").write_text(json.dumps(dict(verdict="FAIL", error=repr(error)), indent=2)+"\n")
        raise
    finally:
        history.close()
        lab.close()


if __name__ == "__main__":
    run(sys.argv[1])
