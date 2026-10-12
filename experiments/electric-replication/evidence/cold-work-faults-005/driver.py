"""Real MinIO/multiprocess pressure, restart, partial-tail and fairness histories.

Independent check uses client bytes and per-replica offsets, not admission
counter values as expected data. This is a correctness probe, not ds-bench.
"""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
import shutil
import sys
import threading
import time
import traceback

from cold_benchmark import BenchLab, PREFIX, STRONG, digest, save, seeded, compress
from cold_benchmark_check import object_window, read_lines
from cold_faults import ColdLab, MINIO
from lab import BINARY, ROOT, source_hashes


def check(history, objects):
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
    assert {r["phase"] for r in history if r["op"] == "barrier"} >= {"outage", "restart", "recovered", "partial", "fair", "concurrent", "slow", "disk"}
    assert any(r["op"] == "pressure" and r["state"]["counters"].get("backlog_bound",0) for r in history)
    assert any(r["op"] == "remote-reclaimed" and r["local_bytes"] == 0 and r["remote_bytes"] > 0 for r in history)
    assert any(r["status"] == 507 for r in attempts)
    assert any(r["op"] == "pressure" and r["state"]["counters"].get("disk_headroom",0) for r in history)
    assert all(r["status"]==201 for r in history if r["op"]=="create")
    assert all(r["status"] in (204,429) for r in history if r["op"]=="close")
    for row in history:
        if row["op"]=="remote-reclaimed":
            observed=object_window(objects,row["start_ns"],row["end_ns"],set(row["client_keys"]))
            assert observed==row["window"] and observed["client_remote_fetched_bytes"]==row["remote_bytes"]
    intervals=[r for r in history if r["op"]=="slow-provider"]
    assert len(intervals)==1
    interval=intervals[0]
    held=[e for e in objects if e["op"]=="held" and e["method"]=="PUT" and "/p/" in e["path"]
          and interval["start_ns"]<=e["time_ns"]<interval["release_ns"]]
    assert held and interval["release_ns"]-min(e["time_ns"] for e in held)>=2_000_000_000
    assert any(r["name"]=="slow" and r["status"]==429
               and min(e["time_ns"] for e in held)<r["start_ns"]<=r["end_ns"]<interval["release_ns"] for r in attempts)
    for event in held:
        puts=[e for e in objects if e["op"]=="s3" and e["method"]=="PUT" and e["path"]==event["path"] and e["status"]==200]
        assert puts and all(e["start_ns"]>=interval["release_ns"] for e in puts), "PUT completed before releasing slow provider"
    return dict(verdict="PASS", attempts=len(attempts), verified=len(verified))


def run(output):
    lab = BenchLab(output, "async3", True)
    policy = dict(max_backlog_bytes=2*1024*1024, min_free_bytes=64*1024*1024)
    history = []
    guard = threading.Lock()
    sequences = {}
    expected = {}
    shutil.copyfile(__file__,lab.output/"driver.py")
    save(lab.output / "provenance.json", dict(sources=source_hashes(), binary=digest(BINARY),
        minio=digest(MINIO), driver=digest(__file__), policy=policy,
        scope="three processes, shared local filesystem, actual MinIO; not provider/power-loss evidence"))

    def emit(op, **fields):
        row = dict(op=op, time_ns=time.monotonic_ns(), **fields)
        with guard, open(lab.output / "history.jsonl", "a") as target:
            history.append(row)
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
        emit("create",name=name,status=code)
        assert code == 201,(name,code)
        expected[name]=b""

    def append(name,size,strong=False):
        seq=sequences.get(name,0); sequences[name]=seq+1
        data=seeded(f"{name}:{seq}",size)
        start_ns=time.monotonic_ns()
        try:
            code,headers,_=lab.request(1,"POST",PREFIX+name,data,
                {"content-type":"application/octet-stream",**(STRONG if strong else {})})
        except BaseException as error:
            emit("append",name=name,sequence=seq,bytes=size,start_ns=start_ns,end_ns=time.monotonic_ns(),
                 status=0,outcome="unknown",error=type(error).__name__)
            raise
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

    def close_all():
        for name in expected:
            def close():
                code,_,_=lab.request(1,"POST",PREFIX+name,headers={"stream-closed":"true",**STRONG})
                emit("close",name=name,status=code)
                if code not in (204,429):raise ValueError(("unexpected close",code))
                return code==204
            lab.wait(close,"closed after pressure relief")
        lab.wait(lambda:all(lab.admin(n,0,"pressure")["local_backlog_bytes"]==0 for n in lab.nodes),
                 "all closed tails physically reclaimed",timeout=90)

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
        # Public writes follow each group's normally elected leader.
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
        begin=time.monotonic_ns()
        verify(["outage"],"recovered")
        end=time.monotonic_ns()
        events=[e for p in lab.output.glob("objects-*.jsonl") for e in read_lines(p)]
        keys={"/"+lab.bucket+"/"+key for key in reclaimed["listing_keys"]}
        remote=object_window(events,begin,end,keys)
        assert remote["client_remote_fetched_bytes"]>=3*len(expected["outage"])
        emit("remote-reclaimed",phase="recovered",local_bytes=reclaimed["local_payload_bytes"],
             remote_bytes=remote["client_remote_fetched_bytes"],client_keys=sorted(keys),window=remote,start_ns=begin,end_ns=end)

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
        close_all()

        # Keep a hot stream busy for the whole sparse recovery interval, with
        # actual queued sparse work (provider down during setup), not merely a
        # finite burst that may finish before observation starts.
        create("a-busy")
        for n in (1,2,3):lab.object_fault(n,"unavailable",("PUT",),"/p/")
        sparse=[f"z-sparse-{i}" for i in range(12)]
        for name in sparse:
            create(name); assert append(name,64*1024,strong=True)==204
        done=threading.Event()
        def busy():
            while not done.wait(0.05):
                assert append("a-busy",16*1024) in (202,429)
        with ThreadPoolExecutor(max_workers=1) as pool:
            writer=pool.submit(busy)
            try:
                lab.wait(lambda:len(expected["a-busy"])>=32*1024,"hot writer active")
                for n in (1,2,3):lab.object_fault(n)
                for name in sparse:lab.reclaimed([name],64*1024,timeout=90)
                emit("fairness",busy_bytes=len(expected["a-busy"]),sparse=lab.manifests(sparse))
            finally:done.set()
            writer.result()
        verify(sparse+["a-busy"],"fair")
        close_all()

        racers=[f"concurrent-{i}" for i in range(8)]
        for name in racers:create(name)
        for n in (1,2,3):lab.object_fault(n,"unavailable",("PUT",),"/p/")
        leaders=[lab.leader(g) for g in range(lab.partitions)]
        emit("admission-scope",leaders=leaders,per_originating_node_bytes=policy["max_backlog_bytes"])
        barrier=threading.Barrier(len(racers))
        def race(name):
            barrier.wait(timeout=20)
            return [append(name,128*1024) for _ in range(8)]
        with ThreadPoolExecutor(max_workers=len(racers)) as pool:
            statuses=[s for rows in pool.map(race,racers) for s in rows]
        assert 202 in statuses and 429 in statuses and set(statuses)<={202,429}
        assert leaders==[lab.leader(g) for g in range(lab.partitions)],"leadership changed: capacity observation is not a stable-originator case"
        # Followers deliberately use disk admission, not the originating cap.
        # Distinct leaders may each accept their node budget before propagation.
        assert sum(len(expected[name]) for name in racers)<=len(set(leaders))*policy["max_backlog_bytes"]
        verify(racers,"concurrent");pressure()
        for n in (1,2,3):lab.object_fault(n)
        close_all()

        # Slow success is different from an immediate unavailable response.
        # Hold an actual PUT while committed local data fills the same budget;
        # rejecting further writes must not cancel its durable Begin or bytes.
        create("slow")
        slow_start=time.monotonic_ns()
        for n in (1,2,3):lab.object_fault(n,"hold",("PUT",),"/p/")
        try:
            statuses=[append("slow",128*1024) for _ in range(32)]
            assert 202 in statuses and 429 in statuses and set(statuses)<={202,429}
            assert len(expected["slow"])<=policy["max_backlog_bytes"]
            def holding():
                return any(e["op"]=="held" and e["method"]=="PUT" and e["time_ns"]>=slow_start
                           for p in lab.output.glob("objects-*.jsonl") for e in read_lines(p))
            lab.wait(holding,"actual slow payload PUT")
            time.sleep(2.1)
            assert append("slow",128*1024)==429
            pressure()
        finally:
            release=time.monotonic_ns()
            for n in (1,2,3):lab.object_fault(n)
            emit("slow-provider",start_ns=slow_start,release_ns=release)
        lab.reclaimed(["slow"],len(expected["slow"]),timeout=90)
        verify(["slow"],"slow")
        close_all()

        # Cross a real statvfs floor with a small OWNED disposable allocation,
        # not by filling the shared filesystem or faking the free-space result.
        create("disk")
        policy["min_free_bytes"]=shutil.disk_usage(ROOT).free-32*1024*1024
        for n in (1,2,3):lab.stop(n,crash=True)
        lab.node_pids=[]
        for n in (1,2,3):start(n)
        for g in range(lab.partitions):lab.wait(lambda:lab.leader(g),"disk-test restart election")
        fill=lab.data/"owned-headroom-fixture"
        try:
            with fill.open("xb") as file:
                os.posix_fallocate(file.fileno(),0,96*1024*1024)
                os.fsync(file.fileno())
            lab.wait(lambda:all(lab.admin(n,0,"pressure")["filesystem_free_bytes"] < policy["min_free_bytes"] for n in lab.nodes),
                     "actual filesystem pressure sampled")
            assert append("disk",64*1024,strong=True)==507
            pressure()
            assert lab.heads(["disk"]) == [dict(node=n,bytes=0,offsets=[0]) for n in (1,2,3)]
        finally:fill.unlink(missing_ok=True)
        lab.wait(lambda:append("disk",64*1024,strong=True)==204,"disk headroom recovery")
        verify(["disk"],"disk")
        events=[e for p in lab.output.glob("objects-*.jsonl") for e in read_lines(p)]
        verdict=check(history,events)
        # Negative history controls must detect corrupt bytes, omitted replicas
        # and acceptance silently changed to a rejection.
        mutations=0
        for field,value in (("sha256","0"*64),("bytes",1),("offset",1)):
            altered=[dict(r) for r in history]
            next(r for r in altered if r["op"]=="verified")[field]=value
            try:check(altered,events)
            except AssertionError:mutations+=1
            else:raise AssertionError("checker accepted corruption: "+field)
        for altered in (
            [dict(r) for r in history if not (r["op"]=="verified" and r["node"]==3)],
            [dict(r, status=429) if r["op"]=="append" and r["status"]==202 else dict(r) for r in history],
        ):
            try:check(altered,events)
            except AssertionError:mutations+=1
            else:raise AssertionError("checker accepted missing replica/accepted writes")
        for altered_history,altered_events in (
            (history,[e for e in events if e["op"]!="held"]),
            ([dict(r,release_ns=r["start_ns"]) if r["op"]=="slow-provider" else dict(r) for r in history],events),
        ):
            try:check(altered_history,altered_events)
            except AssertionError:mutations+=1
            else:raise AssertionError("checker accepted an unexercised slow-provider interval")
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
        assert source_hashes()==json.loads((lab.output/"provenance.json").read_text())["sources"],"runtime changed during process test"
        if (lab.output/"result.json").exists() and json.loads((lab.output/"result.json").read_text())["verdict"]=="PASS":
            shutil.rmtree(lab.data)


if __name__=="__main__":run(sys.argv[1])
