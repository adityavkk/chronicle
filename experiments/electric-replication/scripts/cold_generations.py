"""Longer repeated-generation resource/GC probe. NOT relabelled as ds-bench.

Paced live appends overlap natural offload, stream deletion, inventories and
collection. One slow real reader intentionally blocks a replicated certificate.
Finite observations test conditional reclamation; they cannot prove an
indefinite bound or provider durability. No paid resources or external accounts.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import threading
import time
import traceback

from benchmark import sample
from cold_benchmark import BenchLab, BINARY, CLIENT, EXPERIMENT, MINIO, NATIVE, PREFIX, ROOT, SEGMENT, STRONG, archive_harness, compress, digest, save, seeded
from cold_benchmark_check import object_window, read_lines
from cold_generations_check import validate
from lab import partition, source_hashes


def collection_complete(lab, keys, since):
    if keys.intersection(o["Key"] for o in lab.objects()):
        return False
    expected = {"/"+lab.bucket+"/"+key for key in keys}
    completed = set()
    for path in lab.output.glob("objects-*.jsonl"):
        with path.open() as source:
            for line in source:
                # This is a live append-only observation, not the final audit.
                # A partial last event must wait for its writer to finish.
                if not line.endswith("\n"):
                    break
                event = json.loads(line)
                if (event.get("op") == "s3" and event["method"] == "DELETE"
                        and event["status"] == 204 and event["time_ns"] >= since):
                    completed.add(event["path"])
    return expected.issubset(completed)


def run_cell(output,arm,cold,generations,period):
    lab=BenchLab(output,arm,cold)
    result=dict(version=1,arm=arm,cold=cold,verdict="FAIL",generations=generations,period_secs=period,
                writes=[],cycles=[],object_bucket=lab.bucket,replicas=len(lab.genesis),
                cpu_hz=os.sysconf("SC_CLK_TCK"),page_bytes=os.sysconf("SC_PAGE_SIZE"))
    done=threading.Event();stop=threading.Event();writer=None;sampler=None;guard=threading.Lock()
    def emit(row):
        with guard,open(lab.output/"history.jsonl","a") as out:
            out.write(json.dumps(dict(time_ns=time.monotonic_ns(),**row))+"\n")
    def live_payload(seq):
        return seq.to_bytes(8,"big")+hashlib.sha256(f"paced-{seq}".encode()).digest()+bytes([seq%251])*216
    try:
        assert shutil.disk_usage(ROOT).free>=8*1024**3
        lab.boot();result["roles"]=dict(sut=lab.node_pids,provider=lab.provider_pids,driver=[os.getpid()])
        result["startup_resources"]=sample(lab.node_pids+lab.provider_pids,set(),lab.node_pids)
        lab.seed(["paced-live"],0)
        start=time.monotonic_ns();result["window"]=dict(start_ns=start)
        def append():
            seq=0;base=time.monotonic()
            try:
                while not done.is_set():
                    due=base+seq/64
                    if done.wait(max(0,due-time.monotonic())):break
                    row=dict(seq=seq,start_ns=time.monotonic_ns(),due_monotonic_ns=int(due*1e9),outcome="unknown")
                    result["writes"].append(row)
                    code,headers,_=lab.request(1,"POST",PREFIX+"paced-live",live_payload(seq),{"content-type":"application/octet-stream"},timeout=20)
                    row.update(end_ns=time.monotonic_ns(),status=code,outcome="accepted" if code==202 else "committed" if code in (200,204) else "rejected")
                    emit(dict(op="paced-append",**row))
                    if code not in (200,202,204):raise AssertionError(f"paced append rejected: {code}")
                    seq+=1
            except BaseException:result["writer_error"]=traceback.format_exc();done.set()
        def collect():
            try:
                with open(lab.output/"samples.jsonl","w") as out:
                    while not stop.is_set():
                        row=sample(lab.node_pids+lab.provider_pids+[os.getpid()],{str(lab.port+n) for n in lab.nodes},lab.node_pids)
                        row["state"]=lab.observe()
                        row["accepted_before"]=sum(w.get("outcome") in ("accepted","committed") for w in result["writes"])
                        row["live_committed"]=lab.heads(["paced-live"])
                        row["accepted_after"]=sum(w.get("outcome") in ("accepted","committed") for w in result["writes"])
                        out.write(json.dumps(row)+"\n");out.flush()
                        if row["state"]["free_bytes"]<6*1024**3:raise RuntimeError("6 GiB disk guard")
                        stop.wait(1)
            except BaseException:result["sampling_error"]=traceback.format_exc();done.set()
        sampler=threading.Thread(target=collect);writer=threading.Thread(target=append)
        sampler.start();writer.start()
        for generation in range(generations):
            assert not done.is_set(),"writer/sampler failed"
            cycle_start=time.monotonic()
            names=[f"generation-{generation}-{i}" for i in range(4)]
            lab.seed(names,2*SEGMENT)
            pre=lab.reclaimed(names,2*SEGMENT) if cold else None
            cycle=dict(generation=generation,start_ns=int(cycle_start*1e9),precondition=pre,reads=[])
            result["cycles"].append(cycle)
            read_start=time.monotonic_ns()
            for node in lab.nodes:
                for name in names:
                    code,headers,body=lab.request(node,"GET",PREFIX+name+"?offset=-1",headers={"stream-consistency":"prefix"})
                    expected=seeded(name,2*SEGMENT)
                    row=dict(node=node,name=name,status=code,bytes=len(body),sha256=hashlib.sha256(body).hexdigest(),
                             expected_sha256=hashlib.sha256(expected).hexdigest(),offset=int(headers.get("stream-next-offset","0").rsplit("_",1)[-1]))
                    cycle["reads"].append(row);emit(dict(op="generation-read",generation=generation,**row))
                    assert code==200 and body==expected and row["offset"]==len(expected)
            cycle["read_window"]=dict(start_ns=read_start,end_ns=time.monotonic_ns())
            # Only the replicated ownership contract promises this global pin.
            # Native standalone deletion does not implement participant certificates.
            hold=cold and arm!="native" and generation==generations//2
            pool=ThreadPoolExecutor(max_workers=1);held=None
            try:
                if hold:
                    first=pre["manifests"][0];key=first["segments"][0]["remote_key"]
                    node=max(lab.nodes);lab.object_fault(node,"hold",["GET"],key)
                    since=time.monotonic_ns()
                    held=pool.submit(lab.request,node,"GET",first["path"]+"?offset=-1",b"",{"stream-consistency":"prefix"},25)
                    lab.wait(lambda:any(e["op"]=="held" and e["time_ns"]>=since for e in read_lines(lab.output/f"objects-{node}.jsonl")),"real remote GET blocked")
                    cycle["pin_before_delete"]=lab.cold(node,partition(first["path"],2))[1]
                    assert cycle["pin_before_delete"]["oldest_pin"] is not None
                cycle["delete_start_ns"]=time.monotonic_ns()
                for name in names:
                    code,_,_=lab.request(1,"DELETE",PREFIX+name,headers=STRONG);assert code==204
                cycle["delete_end_ns"]=time.monotonic_ns()
                if hold:
                    time.sleep(7)  # More than two configured inventory periods.
                    assert key in {o["Key"] for o in lab.objects()},"live reader's object deleted"
                    cycle["pin_held_state"]=lab.observe()
                    lab.object_fault(node)
                    code,_,body=held.result(timeout=20)
                    assert code==200 and body==seeded(first["path"].removeprefix(PREFIX),2*SEGMENT)
                    cycle["pin_release_ns"]=time.monotonic_ns()
                if cold:
                    keys=set(pre["listing_keys"])
                    # Listing can observe deletion before the in-flight DELETE
                    # returns. Record collection only after both observations;
                    # do not weaken the independent completed-I/O check.
                    lab.wait(lambda:collection_complete(lab,keys,cycle["delete_start_ns"]),
                             "generation remote payload absent and DELETE completed",timeout=45)
                cycle["collected_ns"]=time.monotonic_ns()
                cycle["after_collection"]=lab.observe()
                emit(dict(op="generation-complete",generation=generation,delete_end_ns=cycle["delete_end_ns"],
                          collected_ns=cycle["collected_ns"],pin=hold,state=cycle["after_collection"]))
            finally:
                if hold:lab.object_fault(node)
                pool.shutdown(wait=True)
            # Fixed offered generation cadence where work fits. Missed deadlines
            # remain measured, never skipped generations or negative sleeps.
            cycle["overrun_secs"]=max(0,time.monotonic()-cycle_start-period)
            if done.wait(max(0,period-(time.monotonic()-cycle_start))):raise AssertionError("background failure")
        done.set();writer.join()
        assert "writer_error" not in result and "sampling_error" not in result
        target=b"".join(live_payload(w["seq"]) for w in result["writes"])
        result["live_drain"]=lab.drain(["paced-live"],len(target))
        result["live_probes"]=[]
        for node in lab.nodes:
            code,headers,body=lab.request(node,"GET",PREFIX+"paced-live?offset=-1",headers={"stream-consistency":"prefix"},timeout=40)
            probe=dict(node=node,status=code,bytes=len(body),sha256=hashlib.sha256(body).hexdigest(),
                       expected_sha256=hashlib.sha256(target).hexdigest(),offset=int(headers.get("stream-next-offset","0").rsplit("_",1)[-1]))
            result["live_probes"].append(probe);emit(dict(op="live-read",**probe))
            assert code==200 and body==target and probe["offset"]==len(target)
        result["window"]["end_ns"]=time.monotonic_ns()
        stop.set();sampler.join()
        assert "sampling_error" not in result
        assert lab.request(1,"DELETE",PREFIX+"paced-live",headers=STRONG)[0]==204
        if cold:
            def empty():
                objects=lab.objects()
                result["reclamation_objects"]=[dict(key=o["Key"],bytes=o["Size"]) for o in objects]
                if arm=="native":return not objects
                # Capture protects the active root. Stable empty state retains
                # that predecessor plus its replacement, per group, not one.
                states=[lab.cold(n,g)[1] for n in lab.nodes for g in range(lab.partitions)]
                result["reclamation_states"]=states
                return (all(s["live_objects"]==0 and not s["pending"] and s["oldest_pin"] is None for s in states)
                        and len(objects)<=2*lab.partitions and all("/i/" in o["Key"] for o in objects))
            lab.wait(empty,"all payload and obsolete inventories reclaimed",timeout=90)
        result["final_state"]=lab.observe()
        events=[e for p in lab.output.glob("objects-*.jsonl") for e in read_lines(p)]
        for cycle in result["cycles"]:
            if cold:
                keys={"/"+lab.bucket+"/"+k for k in cycle["precondition"]["listing_keys"]}
                cycle["remote_client_reads"]=object_window(events,**dict(start=cycle["read_window"]["start_ns"],end=cycle["read_window"]["end_ns"]),client_keys=keys)
                assert cycle["remote_client_reads"]["client_remote_fetched_bytes"]>=len(lab.nodes)*4*2*SEGMENT
        result["check"]=validate(result,read_lines(lab.output/"history.jsonl"),events,read_lines(lab.output/"samples.jsonl"))
        assert result["check"]["verdict"]=="PASS",result["check"]["errors"]
        result["verdict"]="PASS"
    except BaseException as error:
        result["error"]=repr(error);(lab.output/"failure.txt").write_text(traceback.format_exc())
    finally:
        done.set()
        if writer:writer.join()
        stop.set()
        if sampler:sampler.join()
        try:
            if lab.nodes:result["teardown_state"]=lab.observe()
        except BaseException as error:result["teardown_observation_error"]=repr(error)
        save(lab.output/"result.json",result)
        lab.close()
        for path in list(lab.output.glob("*.jsonl")):compress(path)
        if result["verdict"]=="PASS":shutil.rmtree(lab.data)
        else:result["retained_data"]=str(lab.data.relative_to(ROOT))
        save(lab.output/"result.json",result)
    print(json.dumps(dict(cell=output.name,verdict=result["verdict"],error=result.get("error"))),flush=True)
    return result


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    parser.add_argument("--arms",nargs="+",default=["native","raft1","quorum3","async3"])
    parser.add_argument("--tiers",nargs="+",choices=["hot","cold"],default=["hot","cold"])
    parser.add_argument("--repetitions",type=int,default=3)
    parser.add_argument("--first-repetition",type=int,default=1)
    parser.add_argument("--generations",type=int,default=24)
    parser.add_argument("--period",type=int,default=5)
    args=parser.parse_args();assert 1<=args.repetitions<=3 and 2<=args.generations<=48 and 2<=args.period<=15
    assert 1<=args.first_repetition<=3 and args.first_repetition+args.repetitions<=4
    os.sched_setaffinity(0,{4,5})
    root=Path(args.output).resolve();root.mkdir(parents=True,exist_ok=False)
    qual=json.loads((EXPERIMENT/"evidence/cold-runtime-tests-001/qualification.json").read_text())
    assert source_hashes()==qual["sources"] and digest(BINARY)==qual["release_binary_sha256"]
    save(root/"provenance.json",dict(argv=sys.argv,sources=source_hashes(),binary={str(p.relative_to(ROOT)):digest(p) for p in (BINARY,NATIVE,CLIENT,MINIO)},
        harness=archive_harness(root),
        scope="independent paced probe, not ds-bench; 64 appends/s, 256-byte asymmetric records; one shared host/disk/MinIO",
        cpus=dict(sut="0-3 aggregate",provider_observer_writer="4-5"),segment_bytes=SEGMENT,inventory_ms=3000))
    choices=[(a,t=="cold") for a in args.arms for t in args.tiers];results=[]
    for repeat in range(args.first_repetition,args.first_repetition+args.repetitions):
        k=repeat%len(choices)
        for arm,cold in choices[k:]+choices[:k]:
            name=f"{root.name}-{arm}-{'cold' if cold else 'hot'}-r{repeat}"
            row=run_cell(root/name,arm,cold,args.generations,args.period)
            results.append(dict(cell=name,verdict=row["verdict"],error=row.get("error")));save(root/"ledger.json",results)
    raise SystemExit(0 if all(r["verdict"]=="PASS" for r in results) else 1)
