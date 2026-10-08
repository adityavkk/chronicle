"""Directed real-process receipt histories. Shared host/disks, NOT AZ/power loss."""
import concurrent.futures
import copy
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import traceback

from check_receipts import check
from lab import Lab, BINARY, EXPERIMENT, ROOT, partition, source_hashes


def run(output):
    lab = Lab(output, port=19800)
    paths = [next(f"/async/{i}" for i in range(100) if partition(f"/async/{i}", 2) == g) for g in (0, 1)]
    history = []
    lock = threading.Lock()
    paused = set()
    interposer = ROOT / ".tmp/electric-tools/storage_faults.so"
    source = Path(__file__).with_name("storage_faults.c")
    subprocess.run(["cc", "-shared", "-fPIC", "-O2", "-Wall", "-Wextra", "-Werror",
                    "-o", str(interposer), str(source), "-ldl", "-pthread"], check=True)
    hashes = source_hashes()
    (lab.output / "provenance.json").write_text(json.dumps(dict(sources=hashes,
        binary=hashlib.sha256(BINARY.read_bytes()).hexdigest(),
        interposer=hashlib.sha256(interposer.read_bytes()).hexdigest(),
        interposer_source=hashlib.sha256(source.read_bytes()).hexdigest(),
        processes="three, then four with learner", partitions=2,
        admission=dict(commands=16, bytes=2*1024*1024),
        scope="single host; real native WAL and fsync-error injection, not power-loss durability"), indent=2)+"\n")

    def emit(event):
        with lock:
            history.append(event)
            with open(lab.output / "history.jsonl", "a") as file:
                file.write(json.dumps(event)+"\n")
        return event

    def start(node, default="quorum-fsync"):
        lab.start(node, dict(LD_PRELOAD=str(interposer), DS_TEST_DATA_ROOT=str(lab.data / str(node)),
            DS_TEST_FAULT_FILE=str(lab.data / f"fault-{node}"),
            DS_TEST_FAULT_LOG=str(lab.output / f"syscalls-{node}.jsonl")),
            pending_commands=16, pending_bytes=2*1024*1024, append_durability=default)

    def pause(node, stopped):
        pid = int((lab.output / f"node-{node}.pid").read_text())
        assert str(BINARY).encode() in Path(f"/proc/{pid}/cmdline").read_bytes()
        os.kill(pid, signal.SIGSTOP if stopped else signal.SIGCONT)
        (paused.add if stopped else paused.discard)(node)

    def append(node, value, local=True, group=0, reject=False):
        headers = {"content-type":"text/plain" if reject else "application/octet-stream",
                   "producer-id":hashlib.sha256(value.encode()).hexdigest(),"producer-epoch":"0","producer-seq":"0"}
        if local is not None:
            headers["stream-durability"] = "local-fsync" if local else "quorum-fsync"
        event = dict(op="reject" if reject else "append", node=node, stream=paths[group], value=value,
                     local=local is not False, semantic_status=409, start=time.monotonic_ns())
        try:
            status, response_headers, body = lab.request(node, "POST", paths[group], (value+"\n").encode(), headers)
            event.update(status=status, headers=response_headers, body=body.decode(errors="replace"))
        except OSError as error:
            event.update(status=0, headers={}, error=str(error))
        event["end"] = time.monotonic_ns()
        return emit(event)

    def receipt(node, attempt, expected=None, wait=0):
        token = attempt["headers"]["stream-receipt"]
        event = dict(op="receipt", node=node, token=token, expected=expected, start=time.monotonic_ns())
        status, headers, body = lab.request(node, "GET", f"/_receipts/{token}?wait_ms={wait}", timeout=35)
        event.update(status=status, headers=headers, result=json.loads(body), end=time.monotonic_ns())
        emit(event)
        if expected:
            assert event["result"]["state"] == expected, (expected, event["result"])
        return event

    def read(node, group=0, mode="linearizable", session=None, forbidden=()):
        headers = {"stream-consistency":mode}
        if session: headers["stream-session"] = session
        event = dict(op="read", node=node, stream=paths[group], mode=mode, forbidden=list(forbidden), start=time.monotonic_ns())
        status, headers, body = lab.request(node, "GET", paths[group]+"?offset=-1", headers=headers)
        event.update(status=status, headers=headers, records=body.decode().splitlines() if status == 200 else [], end=time.monotonic_ns())
        emit(event)
        assert not set(forbidden) & set(event["records"])
        return event

    def live(node, offset, ready):
        connection = http.client.HTTPConnection("127.0.0.1", lab.port+node, timeout=1.5)
        event = dict(op="live", node=node, start=time.monotonic_ns())
        chunks = []
        try:
            connection.request("GET", paths[0]+f"?offset={offset}&live=sse", headers={"stream-consistency":"prefix"})
            response = connection.getresponse()
            event["status"] = response.status
            ready.set()
            until = time.monotonic()+1.5
            while time.monotonic() < until:
                chunk = response.read1(65536)
                if not chunk: break
                chunks.append(chunk)
                assert sum(map(len, chunks)) < 1024*1024
        except TimeoutError:
            event["observation_window_ended"] = True
        finally:
            connection.close()
        event.update(wire=b"".join(chunks).decode(), end=time.monotonic_ns())
        emit(event)
        assert "event:data" not in event["wire"].replace(" ", "")

    try:
        for node in (1, 2, 3): start(node)
        lab.initialize()
        for group in (0, 1): assert lab.request(lab.leader(group), "PUT", paths[group])[0] == 201
        leaders = [lab.leader(g) for g in (0, 1)]
        barrier = threading.Barrier(24)
        def burst(i):
            barrier.wait(timeout=10)
            return append(leaders[i%2], f"mixed-{i}-"+"λ"*(i%7+1), i%3!=0, i%2)
        with concurrent.futures.ThreadPoolExecutor(max_workers=24) as pool:
            mixed = list(pool.map(burst, range(24)))
        assert all(e["status"] == (202 if e["local"] else 200) for e in mixed)
        accepted = [e for e in mixed if e["status"] == 202]
        for event in accepted: receipt(event["node"], event, "committed", 1000)
        for group in (0, 1):
            expected = read(lab.leader(group), group)["records"]
            for node in lab.nodes:
                lab.wait(lambda: read(node, group, "prefix")["records"] == expected, "warm prefix catchup")
        rejected = append(lab.leader(0), "wrong-content-type", reject=True)
        assert rejected["status"] == 202
        receipt(lab.leader(0), rejected, "rejected", 1000)
        duplicate_group = partition(accepted[0]["stream"], 2)
        duplicate = append(lab.leader(duplicate_group), accepted[0]["value"], group=duplicate_group)
        assert duplicate["status"] == 202
        receipt(duplicate["node"], duplicate, "committed", 1000)
        for method, path in [("PUT", paths[0]), ("DELETE", paths[0]), ("POST", "/r/__ds/subscriptions/new/claim")]:
            assert lab.request(lab.leader(0), method, path, headers={"stream-durability":"local-fsync"})[0] == 400

        # Count/byte bounds and quorum-lease expiry are distinct admission gates.
        # Preserve all accepted and unknown operations across replacement.
        lost = []
        for bound in ("count", "bytes", "lease"):
            byte_bound = bound == "bytes"
            old = lab.leader(0)
            others = lab.nodes - {old}
            # Healing the prior partition can elect a third node. A read alone
            # does not prepare that node's write-admission epoch. Establish it
            # with a durable quorum write BEFORE deliberately removing quorum.
            assert append(old, f"prepared-{bound}", False)["status"] == 200
            before = read(old)
            tail = before["headers"]["stream-next-offset"]
            lab.faults(old, others)
            for node in others: lab.faults(node, [old])
            emit(dict(op="fault", kind="partition", isolated=old, bound=bound, start=time.monotonic_ns()))
            ready = threading.Event()
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                watcher = pool.submit(live, old, tail, ready)
                assert ready.wait(5)
                # Two strong requests must not hold the dispatch window on
                # quorum and starve following local-fsync appends.
                strong = [pool.submit(append, old, f"strong-unknown-{i}", False) for i in (0, 1)] if bound == "count" else []
                round_accepts = []
                for i in range(20):
                    value = f"lost-{bound}-{i}" + ("X"*(512*1024) if byte_bound else "")
                    attempt = append(old, value)
                    if attempt["status"] == 429: break
                    assert attempt["status"] == 202, attempt["status"]
                    round_accepts.append(attempt)
                    if bound == "lease": break  # Leave capacity to isolate the lease gate.
                else: raise AssertionError("admission failed to bound backlog")
                assert round_accepts
                state = receipt(old, round_accepts[-1], "pending", 100)
                occupancy = state["result"]["progress"]
                if bound == "lease":
                    assert 0 < occupancy["pending_commands"] < 16
                    start_wait = time.monotonic_ns()
                    time.sleep(1.1)  # Greater than the configured 700 ms lease.
                    prior = lab.admin(old, 0, "metrics")
                    assert prior["current_leader"] == old, "exercise expired lease while still leader"
                    wal = lab.data / str(old) / "0/wal"
                    wal_before = {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in wal.glob("*.wal")}
                    denied = append(old, "lease-expired-no-frame")
                    assert denied["status"] == 503 and "stream-receipt" not in denied["headers"]
                    after = lab.admin(old, 0, "metrics")
                    retained = receipt(old, round_accepts[-1], "pending")["result"]["progress"]
                    wal_after = {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in wal.glob("*.wal")}
                    emit(dict(op="lease", node=old, waited_ns=denied["start"]-start_wait,
                        before=prior, after=after, occupancy_before=occupancy, occupancy_after=retained,
                        wal_before=wal_before, wal_after=wal_after, denied=denied))
                    assert prior["last_log_index"] == after["last_log_index"] and wal_before == wal_after
                    assert retained["pending_commands"] == occupancy["pending_commands"]
                    assert retained["pending_bytes"] == occupancy["pending_bytes"]
                else:
                    assert occupancy["pending_commands"] < 16 if byte_bound else occupancy["pending_commands"] == 16
                    assert append(old, f"still-full-{bound}" + ("X"*(512*1024) if byte_bound else ""))["status"] == 429
                for future in strong: assert future.result()["status"] in (0, 503)
                watcher.result()
            lost.extend(round_accepts)
            forbidden = [e["value"] for e in lost]
            assert read(old, mode="prefix", forbidden=forbidden)["records"] == before["records"]
            assert read(old, mode="session", session=before["headers"]["stream-session"], forbidden=forbidden)["status"] == 200
            assert read(old)["status"] == 503
            assert lab.request(old,"GET",paths[0]+f"?offset={tail}&live=long-poll",headers={"stream-consistency":"prefix"})[0] == 204
            assert read(old,mode="session",session=round_accepts[0]["headers"]["stream-receipt"])["status"] == 400
            new = lab.wait(lambda: lab.leader(0, others), "majority leader")
            for i in range(24): assert append(new, f"replacement-{bound}-{i}", False)["status"] == 200
            for event in round_accepts: receipt(new, event, "invalidated")
            if bound == "count":
                # Stop peers so restarting the accepting node cannot immediately
                # learn the replacement. Its recovered local WAL must stay hidden.
                for node in others: pause(node, True)
                lab.stop(old, crash=True)
                start(old)
                state = receipt(old, round_accepts[-1], "pending")
                assert state["result"]["progress"]["recovering"]
                assert read(old, mode="prefix", forbidden=forbidden)["records"] == before["records"]
                assert append(old, "recovery-no-free-credits")["status"] in (429,503)
                for node in list(paused): pause(node, False)
            for node in lab.nodes: lab.faults(node)
            lab.wait(lambda: receipt(old, round_accepts[0])["result"]["state"] == "invalidated", "old leader learns replacement")
            assert read(lab.leader(0), forbidden=forbidden)["status"] == 200

        # A live delayed follower path must genuinely finish replication after
        # local acceptance; no receipt-polling loop is required from the caller.
        leader = lab.leader(0)
        lab.faults(leader, delay_ms=60)
        delayed = append(leader, "delayed-background-commit")
        assert delayed["status"] == 202
        script = ("import {awaitCommit} from './experiments/electric-replication/client/receipts.mjs';"
                  "const r=await awaitCommit(process.argv[1],process.argv[2],{timeoutMs:10000});"
                  "console.log(JSON.stringify({state:r.state,status:r.response?.status,session:r.session}));")
        result = subprocess.run(["node", "--input-type=module", "-e", script,
            f"http://127.0.0.1:{lab.port+leader}", delayed["headers"]["stream-receipt"]], cwd=ROOT, capture_output=True, text=True)
        (lab.output / "client-helper.txt").write_text(result.stdout+result.stderr)
        assert result.returncode == 0 and json.loads(result.stdout)["state"] == "committed"
        receipt(leader, delayed, "committed")
        lab.faults(leader)

        # Real WAL fsync failure must not return 202, even on the weaker API.
        (lab.data / f"fault-{leader}").write_text("eio-sync /0/wal/\n")
        failed = append(leader, "failed-local-fsync")
        assert failed["status"] in (0,503)
        assert "eio-sync" in (lab.output / f"syscalls-{leader}.jsonl").read_text()
        lab.stop(leader)
        (lab.data / f"fault-{leader}").unlink()
        lab.wait(lambda: lab.leader(0), "leader after local disk failure")
        start(leader)
        retry = append(lab.leader(0), "failed-local-fsync")
        assert retry["status"] == 202
        receipt(retry["node"], retry, "committed", 1000)

        # Snapshot the outcomes, not only bytes. A fresh learner initially knows
        # nothing, then installs the native snapshot and learns the exact reply.
        leader = lab.leader(0)
        for i in range(80): assert append(leader,f"snapshot-filler-{i}",False)["status"] == 200
        assert lab.admin(leader,0,"snapshot") == {"Ok":None}
        lab.wait(lambda: lab.admin(leader,0,"metrics").get("snapshot"), "receipt snapshot")
        start(4)
        receipt(4, delayed, "unknown")
        assert "Ok" in lab.admin(leader,0,"learner",[4,{"addr":f"127.0.0.1:{lab.port+4}"}])
        lab.wait(lambda: lab.admin(4,0,"metrics").get("snapshot"), "learner actually installed snapshot")
        receipt(4, delayed, "committed")
        voters = sorted(({1,2,3}-{leader}) | {4})
        assert "Ok" in lab.admin(leader,0,"membership",voters)
        new = lab.wait(lambda: lab.leader(0,set(voters)), "movement leader")
        assert append(leader,"stale-route")["status"] == 503
        moved = append(new,"after-movement")
        assert moved["status"] == 202
        receipt(new,moved,"committed",1000)
        final = [read(lab.leader(g),g)["records"] for g in (0,1)]
        active = sorted(lab.nodes)
        for node in active: lab.stop(node,crash=True)
        for node in active: start(node,default="local-fsync")
        for group in (0,1):
            leader = lab.wait(lambda: lab.leader(group), "whole-cluster restart")
            assert read(leader,group)["records"] == final[group]
        receipt(lab.leader(0),delayed,"committed")
        # Headerless clients can opt into operator-configured async appends;
        # explicit quorum overrides it, and lifecycle operations stay strong.
        configured = append(lab.leader(0),"configured-local-default",local=None)
        assert configured["status"] == 202
        receipt(configured["node"],configured,"committed",1000)
        assert append(lab.leader(0),"explicit-quorum-override",local=False)["status"] == 200
        path = "/async/default-lifecycle"
        for method in ("PUT","DELETE"):
            status, headers, _ = lab.request(lab.leader(partition(path,2)),method,path)
            assert 200 <= status < 300 and headers["stream-durability"] == "quorum-fsync"
            assert "stream-receipt" not in headers and "stream-session" in headers
        read(lab.leader(0))
        verdict = check(history)
        mutations = []
        for mode in ("202 session", "pending is committed", "SSE speculative publication", "expired lease assigns", "expiry frees debt"):
            bad = copy.deepcopy(history)
            if mode == "202 session":
                next(e for e in bad if e["op"] == "append" and e["status"] == 202)["headers"]["stream-session"] = "wrong:0:1"
            elif mode == "pending is committed":
                next(e for e in bad if e["op"] == "receipt" and e["result"]["state"] == "pending")["result"]["state"] = "committed"
            elif mode == "expired lease assigns":
                next(e for e in bad if e["op"] == "lease")["after"]["last_log_index"] += 1
            elif mode == "expiry frees debt":
                next(e for e in bad if e["op"] == "lease")["occupancy_after"]["pending_commands"] = 0
            else: next(e for e in bad if e["op"] == "live")["wire"] += "event:data\ndata:speculative\n\n"
            try: check(bad)
            except AssertionError: mutations.append(mode)
            else: raise AssertionError("checker accepted " + mode)
        verdict["negative_mutations_rejected"] = mutations
        (lab.output / "result.json").write_text(json.dumps(verdict,indent=2)+"\n")
        print(json.dumps(verdict))
    except BaseException:
        (lab.output / "failure.txt").write_text(traceback.format_exc())
        raise
    finally:
        for node in list(paused): pause(node,False)
        lab.close()


if __name__ == "__main__":
    run(sys.argv[1])
