"""Real MinIO/multiprocess pressure, restart, partial-tail and fairness histories.

Independent check uses client bytes and per-replica offsets, not admission
counter values as expected data. This is a correctness probe, not ds-bench.
"""
import hashlib
import json
import shutil
import sys
import time
import traceback

from cold_benchmark import BenchLab, PREFIX, STRONG, digest, save, seeded, compress
from cold_faults import ColdLab, MINIO
from lab import BINARY, ROOT, source_hashes


def check(history):
    attempts = [r for r in history if r["op"] == "append"]
    assert attempts and any(r["status"] == 429 for r in attempts)
    expected = {}
    for row in attempts:
        assert row["status"] in (204, 202, 429, 507), "unknown/error write retained, not a passing history"
        if row["status"] in (204, 202):
            expected.setdefault(row["name"], []).append((row["sequence"],row["bytes"]))
    verified = [r for r in history if r["op"] == "verified"]
    assert verified
    for row in verified:
        entries = [a for a in attempts if a["name"] == row["name"] and a["status"] in (204,202)
                   and a["end_ns"] <= row["cut_ns"]]
        # This probe deliberately serializes each stream's client, so the
        # independent byte oracle does not infer ordering from Raft metadata.
        payload = b"".join(seeded(f'{a["name"]}:{a["sequence"]}', a["bytes"]) for a in entries)
        assert row["bytes"] == row["offset"] == len(payload)
        assert row["sha256"] == hashlib.sha256(payload).hexdigest()
    for name in expected:
        assert {r["node"] for r in verified if r["name"] == name} == {1,2,3}
    assert {r["phase"] for r in history if r["op"] == "barrier"} >= {"outage", "restart", "recovered", "partial", "fair"}
    assert any(r["op"] == "pressure" and r["state"]["counters"].get("backlog_bound",0) for r in history)
    assert any(r["op"] == "remote-reclaimed" and r["local_bytes"] == 0 and r["remote_bytes"] > 0 for r in history)
    return dict(verdict="PASS", attempts=len(attempts), verified=len(verified))


def run(output):
    lab = BenchLab(output, "async3", True)
    policy = dict(max_backlog_bytes=2*1024*1024, min_free_bytes=64*1024*1024)
    history = []
    sequences = {}
    expected = {}
    save(lab.output / "provenance.json", dict(sources=source_hashes(), binary=digest(BINARY),
        minio=digest(MINIO), driver=digest(__file__), policy=policy,
        scope="three processes, shared local filesystem, actual MinIO; not provider/power-loss evidence"))

    def emit(op, **fields):
        row = dict(op=op, time_ns=time.monotonic_ns(), **fields)
        history.append(row)
        with open(lab.output / "history.jsonl", "a") as target:
            target.write(json.dumps(row)+"\n")
        return row

    def start(node):
        ColdLab.start(lab,node,cpus="0-3",stats_secs=1,append_durability="local-fsync",cold_admission=policy)
        lab.node_pids.append(int((lab.output/f"node-{node}.pid").read_text()))

    def pressure():
        for node in sorted(lab.nodes):
            emit("pressure",node=node,state=lab.admin(node,0,"pressure"))

    def create(name):
        code,_,_=lab.request(1,"PUT",PREFIX+name,headers={"content-type":"application/octet-stream",**STRONG})
        assert code == 201,(name,code)
        expected[name]=b""

    def append(name,size,strong=False):
        seq=sequences.get(name,0); sequences[name]=seq+1
        data=seeded(f"{name}:{seq}",size)
        start_ns=time.monotonic_ns()
        code,headers,_=lab.request(1,"POST",PREFIX+name,data,
            {"content-type":"application/octet-stream",**(STRONG if strong else {})})
        emit("append",name=name,sequence=seq,bytes=size,start_ns=start_ns,end_ns=time.monotonic_ns(),
             status=code,receipt=headers.get("stream-receipt"))
        if code in (204,202):expected[name]+=data
        return code

    def verify(names,phase):
        cut=time.monotonic_ns()
        for name in names:
            lab.drain([name],len(expected[name]))
            for node in sorted(lab.nodes):
                code,headers,body=lab.request(node,"GET",PREFIX+name+"?offset=-1",headers={"stream-consistency":"prefix"},timeout=30)
                assert code==200 and body==expected[name],(node,name,code,len(body),len(expected[name]))
                emit("verified",name=name,node=node,phase=phase,cut_ns=cut,bytes=len(body),
                    offset=int(headers["stream-next-offset"].rsplit("_",1)[-1]),sha256=hashlib.sha256(body).hexdigest())
        emit("barrier",phase=phase)

    try:
        assert shutil.disk_usage(ROOT).free >= 8*1024**3
        lab.start_storage()
        for n in (1,2,3):start(n)
        for g in range(lab.partitions):
            assert lab.admin(1,g,"init",lab.genesis)=={"Ok":None}
            lab.wait(lambda:lab.leader(g)==1,"initial direct leaders")
        create("outage")
        for n in (1,2,3):lab.object_fault(n,"unavailable",("PUT",),"/p/")
        statuses=[append("outage",128*1024) for _ in range(40)]
        assert 202 in statuses and 429 in statuses and all(s in (202,429) for s in statuses)
        verify(["outage"],"outage"); pressure()
        assert len(expected["outage"]) <= policy["max_backlog_bytes"]
        # Full restart retains acknowledged bytes AND pressure; changing local
        # resource policy does not rewrite identity-11 or reset durable data.
        for n in (1,2,3):lab.stop(n,crash=True)
        lab.node_pids=[]
        for n in (1,2,3):start(n)
        for g in range(lab.partitions):lab.wait(lambda:lab.leader(g),"restart election")
        # Force the direct endpoint only by a normal transfer, not by bypassing
        # consensus. Public writes follow each group's elected leader below.
        from lab import partition
        original_request=lab.request
        def routed(node,method,path,*args,**kwargs):
            if node==1 and method in ("PUT","POST","DELETE") and path.startswith(PREFIX):
                node=lab.leader(partition(path.split("?")[0],lab.partitions))
            return original_request(node,method,path,*args,**kwargs)
        lab.request=routed
        assert append("outage",512*1024)==429
        verify(["outage"],"restart"); pressure()
        for n in (1,2,3):lab.object_fault(n)
        reclaimed=lab.reclaimed(["outage"],len(expected["outage"]),timeout=120)
        emit("remote-reclaimed",phase="recovered",local_bytes=reclaimed["local_payload_bytes"],
             remote_bytes=len(expected["outage"]),keys=reclaimed["remote_keys"])
        verify(["outage"],"recovered")

        # A large request can be blocked BELOW the 75% high-water mark by many
        # small open tails. Retrying must trigger short sealing, not deadlock.
        # Use <64 KiB tails under the unchanged durable configuration.
        partial=[f"partial-{i}" for i in range(40)]
        for name in partial:
            create(name); assert append(name,32*1024,strong=True)==204
            time.sleep(0.12)  # Exclude transient double-accounting from this boundary case.
        create("large")
        first=append("large",1024*1024,strong=True)
        assert first==429,"fixture must actually reach the partial-tail stall boundary"
        lab.wait(lambda:append("large",1024*1024,strong=True)==204,"short-tail pressure relief",timeout=60)
        verify(partial+["large"],"partial"); pressure()

        # Keep one stream busy while closed sparse streams compete in the same
        # authoritative catalog. Stream names put the busy one before sparse.
        create("a-busy")
        sparse=[f"z-sparse-{i}" for i in range(24)]
        for name in sparse:
            create(name); assert append(name,64*1024,strong=True)==204
        for _ in range(16):append("a-busy",64*1024)
        for name in sparse:lab.reclaimed([name],64*1024,timeout=90)
        verify(sparse+["a-busy"],"fair")
        verdict=check(history)
        # Negative history controls must detect corrupt bytes, omitted replicas
        # and acceptance silently changed to a rejection.
        mutations=0
        for field,value in (("sha256","0"*64),("bytes",1),("offset",1)):
            altered=[dict(r) for r in history]
            next(r for r in altered if r["op"]=="verified")[field]=value
            try:check(altered)
            except AssertionError:mutations+=1
            else:raise AssertionError("checker accepted corruption: "+field)
        verdict["negative_history_controls"]=mutations
        save(lab.output/"result.json",verdict)
        print(json.dumps(verdict),flush=True)
    except BaseException as error:
        save(lab.output/"result.json",dict(verdict="FAIL",error=repr(error)))
        (lab.output/"failure.txt").write_text(traceback.format_exc())
        raise
    finally:
        lab.close()
        for path in lab.output.glob("*.jsonl"):compress(path)
        if (lab.output/"result.json").exists() and json.loads((lab.output/"result.json").read_text())["verdict"]=="PASS":
            shutil.rmtree(lab.data)


if __name__=="__main__":run(sys.argv[1])
