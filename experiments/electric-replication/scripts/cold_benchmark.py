"""Bounded, matched ds-bench cells against the qualified engine and real MinIO.

Run with .tmp/electric-tools/cold-venv/bin/python. Never overlaps a build/test.
No server changes, no cloud calls, no credentials or raw stores in evidence.
Default: three rotated repetitions, fresh processes/data for each workload.
"""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
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
import sys
import tarfile
import threading
import time
import traceback

from benchmark import CLIENT, export_cpu_profile, export_kernel_fsync, sample
from benchmark_summary import summarize_fsync_trace
from cold_benchmark_check import listing_key, object_window, read_lines, validate
from cold_faults import ColdLab, MINIO, SERVICE
from lab import BINARY, EXPERIMENT, ROOT, Lab, matching_pids, source_hashes

NATIVE = ROOT/".tmp/electric-tools/upstream-tier-target/release/durable-streams-server"
SEGMENT = 64*1024
PREFIX = "/v1/stream/"
STRONG = {"stream-durability":"quorum-fsync"}


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source,"sha256").hexdigest()


def save(path, value):
    Path(path).write_text(json.dumps(value,indent=2)+"\n")


def archive_harness(output):
    paths=[Path(__file__).with_name(name) for name in ("cold_benchmark.py","cold_benchmark_check.py",
        "cold_generations.py","cold_generations_check.py","cold_faults.py","cold_service.py",
        "cold_benchmark_test.py","benchmark_summary_test.py","benchmark.py","benchmark_summary.py","lab.py")]
    with open(output/"harness.tar.gz","wb") as target, gzip.GzipFile(filename="",mode="wb",fileobj=target,mtime=0) as zipped:
        with tarfile.open(mode="w|",fileobj=zipped) as archive:
            for path in paths:
                info=archive.gettarinfo(str(path),arcname=path.name)
                info.uid=info.gid=info.mtime=0;info.uname=info.gname=""
                with path.open("rb") as source:archive.addfile(info,source)
    return {p.name:digest(p) for p in paths}


def seeded(name, size):
    # Distinct asymmetric bytes per stream and block, independently reproducible.
    return b"".join(hashlib.sha256(f"cold-bench-v1:{name}:{i}".encode()).digest()
                    for i in range((size+31)//32))[:size]


def write_payload(size):
    # Pinned ds-bench's public deterministic binary fixture, not server output.
    mask = (1 << 64)-1
    state = (0xC0FFEE*0x9E3779B97F4A7C15+1)&mask
    body = bytearray()
    while len(body) < size:
        state = (state*6364136223846793005+1442695040888963407)&mask
        body.extend(state.to_bytes(8,"little"))
    return bytes(body[:size])


class BenchLab(ColdLab):
    def __init__(self, output, arm, cold):
        super().__init__(output,replicas=3 if arm in ("quorum3","async3") else 1)
        self.arm, self.enabled = arm, cold
        self.cluster = "cb-"+hashlib.sha256(str(self.output).encode()).hexdigest()[:16]
        self.segment_bytes, self.inventory_ms = SEGMENT, 3000
        self.node_pids, self.provider_pids = [], []
        self.garbage_keys = set()

    def service(self, name, args, port):
        # Separate provider/proxy CPU budget from SUT and pinned client.
        super().service(name,[shutil.which("taskset"),"-c","4-5",*args],port)

    def start_storage(self):
        self.service("minio",[sys.executable,SERVICE,"exec",self.secret_file,MINIO,
            "server","--address",f"127.0.0.1:{self.port+50}",self.data/"minio"],self.port+50)
        self.wait(lambda:self.request(50,"GET","/minio/health/ready")[0]==200,"MinIO ready",timeout=40)
        self.create_bucket()
        self.provider_pids = matching_pids([str(MINIO),"server"])
        assert len(self.provider_pids) == 1
        for node in range(1,len(self.genesis)+1):
            self.object_fault(node)
            args=[sys.executable,str(SERVICE),"proxy",str(self.port+50),str(self.port+60+node),
                  str(self.data/f"object-fault-{node}.json"),str(self.output/f"objects-{node}.jsonl"),str(self.secret_file)]
            self.service(f"s3-{node}",args,self.port+60+node)
            self.wait(lambda:self.request(60+node,"GET","/minio/health/ready")[0]==200,"S3 proxy ready")
            matches=matching_pids(args)
            assert len(matches)==1
            self.provider_pids.extend(matches)

    def boot(self):
        self.start_storage()  # Idle provider also present in hot controls.
        if self.arm == "native":
            args=[str(NATIVE),"--host","127.0.0.1","--port",str(self.port+1),"--data-dir",str(self.data/"1"),
                  "--durability","wal","--worker-threads","2","--wal-shards","2",
                  "--wal-segment-bytes",str(8*1024*1024),"--stream-lanes","1","--tail-cache-bytes","0",
                  "--read-offload","tail","--wal-stats","1","--server-stats","1","--tier","s3" if self.enabled else "off"]
            if self.enabled:
                args += ["--tier-endpoint",f"http://127.0.0.1:{self.port+61}","--tier-bucket",self.bucket,
                    "--tier-region","us-east-1","--tier-allow-http","--tier-key-prefix",self.cluster+"/",
                    "--tier-segment-bytes",str(SEGMENT),"--tier-compact-bytes",str(SEGMENT)]
            command=["taskset","-c","0-3",sys.executable,str(SERVICE),"exec",str(self.secret_file),*args]
            save(self.output/"native-argv.json",command)
            subprocess.run(["amp","orb","service","start",self.cluster+"-1","--cwd",str(ROOT),
                "--command","ulimit -c 0; exec "+shlex.join(command)+" > "+shlex.quote(str(self.output/"node-1.log"))+" 2>&1",
                "--port",str(self.port+1)],check=True,stdout=subprocess.DEVNULL)
            self.nodes.add(1)
            self.wait(lambda:self.request(1,"GET","/health")[0]==200,"native ready")
            self.node_pids=matching_pids([str(NATIVE),"--host","127.0.0.1","--port",str(self.port+1)])
            assert len(self.node_pids)==1
        else:
            for node in range(1,len(self.genesis)+1):
                starter = super().start if self.enabled else lambda n,**kw:Lab.start(self,n,**kw)
                starter(node,cpus="0-3",fault_testing=True,stats_secs=1,pending_commands=1024,
                        append_durability="local-fsync" if self.arm=="async3" else "quorum-fsync")
                self.node_pids.append(int((self.output/f"node-{node}.pid").read_text()))
            for group in range(self.partitions):
                assert self.admin(1,group,"init",self.genesis)=={"Ok":None}
                self.wait(lambda:self.leader(group)==1,"direct endpoint leader")

    def manifests(self, names=None):
        rows=[]
        wanted={PREFIX+n for n in names} if names is not None else None
        for node in sorted(self.nodes):
            for path in (self.data/str(node)).rglob("*.meta"):
                try:
                    meta=json.loads(path.read_text())
                    if wanted is not None and meta["path"] not in wanted:
                        continue
                    live=Path(str(path)[:-5])
                    if not live.exists():
                        continue
                    rows.append(dict(node=node,path=meta["path"],local_bytes=live.stat().st_size,
                        file_base=meta.get("file_base"),segments=meta.get("segments",[])))
                except FileNotFoundError:
                    continue  # A concurrent atomic rename/removal; next observation retries.
        return rows

    def footprint(self):
        totals={}
        for node in sorted(self.nodes):
            values=dict(logical=0,allocated=0,files=0,payload=0,segments=0,metadata=0)
            for path in (self.data/str(node)).rglob("*"):
                try:
                    if not path.is_file():continue
                    stat=path.stat();values["logical"]+=stat.st_size;values["allocated"]+=stat.st_blocks*512;values["files"]+=1
                    if path.parent.name=="streams":
                        values["metadata" if path.name.endswith((".meta",".tmp")) else "payload"]+=stat.st_size
                    if path.parent.name=="segments":values["segments"]+=stat.st_size
                except FileNotFoundError:
                    continue
            totals[str(node)]=values
        return totals

    def observe(self):
        row=dict(time_ns=time.monotonic_ns(),free_bytes=shutil.disk_usage(ROOT).free,storage=self.footprint())
        objs=self.objects()
        row["object_store"]=dict(count=len(objs),bytes=sum(o["Size"] for o in objs))
        row["garbage_objects_remaining"]=sum(o["Key"] in self.garbage_keys for o in objs)
        if self.arm!="native":
            row["groups"]={}
            for node in sorted(self.nodes):
                for group in range(self.partitions):
                    key=f"{node}-{group}"
                    row["groups"][key]=dict(raft=self.admin(node,group,"metrics"))
                    if self.enabled:row["groups"][key]["cold"]=self.cold(node,group)[1]
        return row

    def seed(self, names, size, content_type="application/octet-stream"):
        def one(name):
            path=PREFIX+name
            status,_,_=self.request(1,"PUT",path,headers={"content-type":content_type,**STRONG})
            assert status==201,(path,status)
            payload=seeded(name,size)
            for i in range(0,size,SEGMENT):
                status,_,_=self.request(1,"POST",path,payload[i:i+SEGMENT],{"content-type":content_type,**STRONG},timeout=40)
                assert status in (200,204),(path,status)
        with ThreadPoolExecutor(max_workers=8) as pool:list(pool.map(one,names))

    def reclaimed(self, names, size, timeout=240):
        def ready():
            rows=self.manifests(names)
            if len(rows)!=len(names)*len(self.nodes):return None
            if any(r["local_bytes"] or sum(s["len"] for s in r["segments"])!=size
                   or any(not s["remote_key"] for s in r["segments"]) for r in rows):return None
            if self.arm!="native" and any(self.cold(n,g)[1]["pending"] for n in self.nodes for g in range(self.partitions)):
                return None
            return rows
        rows=self.wait(ready,"all seeded payload remote and local files reclaimed",timeout=timeout)
        keys=sorted({s["remote_key"] for r in rows for s in r["segments"]})
        mapped={listing_key(key,self.arm=="native") for key in keys}
        assert len(mapped)==len(keys)
        actual={o["Key"] for o in self.objects()}
        assert mapped.issubset(actual),"manifest objects absent from real S3 listing"
        return dict(time_ns=time.monotonic_ns(),remote_keys=keys,listing_keys=sorted(mapped),
            local_payload_bytes=sum(r["local_bytes"] for r in rows),local_segment_bytes=sum(
                s["len"] for r in rows for s in r["segments"] if s.get("local_file")),manifests=rows)

    def heads(self, names):
        rows=[]
        for node in sorted(self.nodes):
            def head(name):
                code,headers,_=self.request(node,"HEAD",PREFIX+name,headers={"stream-consistency":"prefix"})
                assert code==200,(node,name,code)
                return int(headers["stream-next-offset"].rsplit("_",1)[-1])
            with ThreadPoolExecutor(max_workers=8) as pool:offsets=list(pool.map(head,names))
            rows.append(dict(node=node,bytes=sum(offsets),offsets=offsets))
        return rows

    def drain(self,names,target):
        def ready():
            rows=self.heads(names)
            if any(r["bytes"]>target for r in rows):raise ValueError("committed bytes exceed accepted target")
            return rows if all(r["bytes"]==target for r in rows) else None
        return self.wait(ready,"exact all-replica committed drain",timeout=45)


def shape(workload, duration, connections):
    if workload in ("hot-write","many-write"):
        count=1 if workload=="hot-write" else 1024
        payload=256 if count==1 else 4096
        names=[f"s{i:08}" for i in range(count)]
        args=["multi-stream","--streams",str(count),"--connections",str(connections),"--batch","1",
              "--payload-bytes",str(payload),"--rate-per-stream","0","--setup-concurrency","16",
              "--warmup-secs","0","--settle-secs","0"]
        size=SEGMENT
    elif workload=="replay":
        names=[f"bench-reads-stream-{i}" for i in range(4)];size=1024*1024;payload=256
        args=["reads","--mode","catchup","--streams","4","--connections","4","--read-size-bytes","4096",
              "--seed-bytes","0","--warmup-secs","0","--settle-secs","0"]
    elif workload=="mixed":
        names=[f"s{i:06}" for i in range(16)];size=256*1024;payload=256
        args=["mixed","--streams","16","--writers-per-stream","1","--writer-rate","32","--readers","4",
              "--read-rate","1","--backfill-events","0","--subscribers","16","--payload-bytes",str(payload)]
    elif workload=="fanout":
        names=["fanout"];size=0;payload=256
        args=["fan-out","--stream","fanout","--subscribers","100","--writer-rate","128",
              "--payload-bytes",str(payload),"--subscriber-idle-timeout-secs","10"]
    else:raise ValueError(workload)
    args += ["--duration-secs",str(duration),"--request-timeout-secs",str(duration+60)]
    return names,size,payload,args


def probe(lab,names,size,payload,workload,drain):
    rows=[]
    for state in drain:
        def one(item):
            i,name=item
            code,headers,body=lab.request(state["node"],"GET",PREFIX+name+"?offset=-1",
                headers={"stream-consistency":"prefix"},timeout=90)
            assert code==200,(state["node"],name,code)
            initial=seeded(name,size)
            assert body[:size]==initial and len(body)==state["offsets"][i]
            suffix=body[size:]
            if workload in ("hot-write","many-write"):
                unit=write_payload(payload)
                assert len(suffix)%payload==0
                expected=initial+unit*(len(suffix)//payload)
                assert body==expected,"wrong ds-bench payload"
            elif workload in ("mixed","fanout"):
                assert len(suffix)%payload==0
                for seq,start in enumerate(range(0,len(suffix),payload)):
                    record=suffix[start:start+payload]
                    assert int(record[:16],16)==seq,"record lost, duplicated or reordered"
                    assert int(record[16:48],16)>0 and record[48:]==b" "+b"."*(payload-49)
                expected=body  # Sequence/filler independently checked; timestamps supplied by client.
            else:expected=initial
            return dict(node=state["node"],name=name,bytes=len(body),sha256=hashlib.sha256(body).hexdigest(),
                expected_sha256=hashlib.sha256(expected).hexdigest(),
                offset=int(headers["stream-next-offset"].rsplit("_",1)[-1]))
        with ThreadPoolExecutor(max_workers=4) as pool:rows.extend(pool.map(one,enumerate(names)))
    return rows


def sequence(lab, output, readers=16):
    """Separate finite byte/sequence proof; not a ds-bench throughput result."""
    name="cold-sequence";size=4*SEGMENT;initial=seeded(name,size)
    lab.seed([name],size)
    pre=lab.reclaimed([name],size) if lab.enabled else None
    catchup=threading.Barrier(readers+1)
    extra=b"".join(i.to_bytes(8,"big")+hashlib.sha256(f"live-{i}".encode()).digest() for i in range(32))
    result=dict(start_ns=time.monotonic_ns(),readers=[],precondition=pre)
    def one(index):
        con=http.client.HTTPConnection("127.0.0.1",lab.port+1,timeout=40)
        con.request("GET",PREFIX+name+"?offset=-1&live=sse")
        response=con.getresponse();assert response.status==200
        assert response.getheader("stream-sse-data-encoding")=="base64"
        collected=bytearray();buffer=bytearray();wire=0;ready=False;closed=False
        try:
            while not closed:
                chunk=response.read1(65536)
                assert chunk,"premature SSE EOF"
                wire+=len(chunk);buffer.extend(chunk)
                assert wire < 2*(size+len(extra))+65536
                while b"\n\n" in buffer:
                    event,_,remaining=buffer.partition(b"\n\n");buffer[:]=remaining
                    lines=event.splitlines()
                    data=b"\n".join(line[5:].lstrip() for line in lines if line.startswith(b"data:"))
                    if b"event: data" in lines:collected.extend(base64.b64decode(data,validate=True))
                    elif b'"streamClosed":true' in data:closed=True
                if not ready and len(collected)>=size:
                    assert bytes(collected)==initial
                    ready=True;catchup.wait(timeout=35)
            assert bytes(collected)==initial+extra
            return dict(reader=index,bytes=len(collected),sha256=hashlib.sha256(collected).hexdigest(),
                        expected_sha256=hashlib.sha256(initial+extra).hexdigest(),closed=closed)
        finally:con.close()
    with ThreadPoolExecutor(max_workers=readers) as pool:
        futures=[pool.submit(one,i) for i in range(readers)]
        catchup.wait(timeout=40)
        result["catchup_ns"]=time.monotonic_ns()
        if lab.enabled and lab.arm!="native":result["pins_during_live"]=lab.observe()
        # Strong close also drains previously accepted async writes; expected
        # sequence and byte concatenation are independent of response batching.
        code,_,_=lab.request(1,"POST",PREFIX+name,extra,{"content-type":"application/octet-stream","stream-closed":"true",**STRONG})
        assert code==204
        result["readers"]=[f.result() for f in futures]
    result["end_ns"]=time.monotonic_ns()
    result["drain"]=lab.drain([name],size+len(extra))
    save(output/"sequence.json",result)
    return result


def compress(path):
    with open(path,"rb") as source,open(str(path)+".gz","wb") as target:
        with gzip.GzipFile(filename="",mode="wb",fileobj=target,mtime=0) as out:shutil.copyfileobj(source,out)
    path.unlink()


def cell(output,arm,cold,workload,repetition,duration,connections,profile="none"):
    lab=BenchLab(output,arm,cold)
    names,size,payload,args=shape(workload,duration,connections)
    result=dict(version=1,arm=arm,cold=cold,workload=workload,repetition=repetition,replicas=len(lab.genesis),
        streams=len(names),seed_bytes=size,payload_bytes=payload,verdict="FAIL",duration_secs=duration,object_bucket=lab.bucket,
        cpu_hz=os.sysconf("SC_CLK_TCK"),page_bytes=os.sysconf("SC_PAGE_SIZE"),profile=profile,
        source_commit=subprocess.check_output(["/usr/bin/git","rev-parse","HEAD"],text=True).strip(),
        binary_sha256=digest(NATIVE if arm=="native" else BINARY),client_sha256=digest(CLIENT))
    stop=threading.Event();sampler=None;process=None;trace=None;trace_log=None
    kernel_path=ROOT/".tmp/electric-profiles"/(lab.output.name+".kernel.perf.data")
    def stop_trace():
        nonlocal trace
        if trace is None:return
        early=trace.poll() is not None
        if not early:trace.send_signal(signal.SIGINT)
        trace.wait(timeout=15);trace_log.close();trace=None
        assert not early,"profiler ended before client; capture invalid"
        if profile=="kernel":
            result["fsync"]=export_kernel_fsync(lab.output,kernel_path,lab.node_pids+lab.provider_pids)
            assert result["fsync"]["successful"] and not result["fsync"]["errors"]
        elif profile=="fsync":
            with open(lab.output/"fsync-trace.log") as events:
                result["fsync"]=summarize_fsync_trace(events)
            assert result["fsync"]["successful"] and not result["fsync"]["errors"]
        elif profile=="cpu":
            with open(lab.output/"cpu-profile.txt","w") as report:
                result["profile_report_exit_code"]=subprocess.run(
                    ["perf","report","--stdio","--no-children","--percent-limit","0.2",
                     "-i",str(lab.output/"perf.data")],stdout=report,stderr=subprocess.STDOUT).returncode
            assert export_cpu_profile(lab.output)==0
            assert result["profile_report_exit_code"]==0
        else:
            assert (lab.output/"syscall-profile.txt").stat().st_size>0
    try:
        result["disk_free_before"]=shutil.disk_usage(ROOT).free
        assert result["disk_free_before"]>=8*1024**3,"8 GiB preflight failed; no SUT started"
        start=time.monotonic();lab.boot();result["startup_seconds"]=time.monotonic()-start
        result["roles"]=dict(sut=lab.node_pids,provider=lab.provider_pids)
        result["startup_resources"]=sample(lab.node_pids+lab.provider_pids,set(),lab.node_pids)
        lab.seed(names,size,"text/plain" if workload=="fanout" else "application/octet-stream")
        # The same extra stream lifecycle occurs in hot controls. In cold arms
        # these 32 objects retire during the measured workload, not in warmup.
        garbage=[f"gc-prefill-{i}" for i in range(8)]
        lab.seed(garbage,4*SEGMENT)
        if cold:
            gc_pre=lab.reclaimed(garbage,4*SEGMENT)
            lab.garbage_keys=set(gc_pre["listing_keys"])
            result["garbage_keys"]=sorted(lab.garbage_keys)
            result["precondition"]=lab.reclaimed(names,size) if size else gc_pre
            result["client_object_keys"]=["/"+lab.bucket+"/"+key for key in result["precondition"]["listing_keys"]] if size else []
            save(lab.output/"precondition.json",dict(client=result["precondition"],garbage=gc_pre))
        else:
            result["precondition"]=dict(time_ns=time.monotonic_ns(),remote_keys=[],local_payload_bytes=len(names)*size,
                                        local_segment_bytes=0,manifests=lab.manifests(names))
            result["client_object_keys"]=[]
        result["before_client"]=lab.observe()
        env={k:v for k,v in os.environ.items() if not k.startswith("DS_BENCH_") and k!="LD_PRELOAD"}
        (lab.output/"hdr").mkdir();env["DS_BENCH_HDR_OUT"]=str(lab.output/"hdr")
        command=["taskset","-c","6-7",str(CLIENT),*args,"--target",f"http://127.0.0.1:{lab.port+1}","--api-style","durable"]
        save(lab.output/"client-argv.json",command)
        if profile!="none":
            assert duration<=30 and os.uname().machine=="x86_64"
            result["profiler_version"]=subprocess.check_output(
                ["strace" if profile in ("fsync","syscalls") else "perf","--version"],text=True).strip()
            pids=",".join(map(str,lab.node_pids+lab.provider_pids))
            if profile=="kernel":
                kernel_path.parent.mkdir(parents=True,exist_ok=True);assert not kernel_path.exists()
                command_profile=["sudo","-n","perf","record","--clockid","mono","--max-size","128M",
                    "-e","raw_syscalls:sys_enter","--filter","id == 74 || id == 75",
                    "-e","raw_syscalls:sys_exit","--filter","id == 74 || id == 75","-o",str(kernel_path),"-p",pids]
            elif profile=="cpu":
                command_profile=["perf","record","-F","99","--max-size","128M","-e","cpu-clock:u","--call-graph","dwarf,16384",
                    "-o",str(lab.output/"perf.data"),"-p",pids]
            elif profile=="fsync":
                # Restrict trace arguments to file descriptors and return codes.
                command_profile=["strace","-f","-qq","-ttt","-T","-e","trace=fsync,fdatasync",
                                 "-e","signal=none","-o",str(lab.output/"fsync-trace.log"),"-p",pids]
            else:
                # Counts only, never syscall arguments or process memory.
                command_profile=["strace","-f","-c","-w","-o",str(lab.output/"syscall-profile.txt")]
                for pid in lab.node_pids:command_profile += ["-p",str(pid)]
            save(lab.output/"profile-argv.json",command_profile)
            trace_log=open(lab.output/"profile.log","w")
            trace=subprocess.Popen(command_profile,stdout=trace_log,stderr=trace_log)
            time.sleep(0.3);assert trace.poll() is None,"profiler attach failed"
        def collect():
            try:
                with open(lab.output/"samples.jsonl","w") as out:
                    while not stop.is_set():
                        pids=lab.node_pids+lab.provider_pids+([process.pid] if process else [])
                        row=sample(pids,{str(lab.port+n) for n in lab.nodes},lab.node_pids)
                        row["state"]=lab.observe()
                        out.write(json.dumps(row)+"\n");out.flush()
                        if row["state"]["free_bytes"]<6*1024**3:
                            if process and process.poll() is None:process.terminate()
                            raise RuntimeError("disk guard: client stopped at 6 GiB free; evidence retained")
                        stop.wait(1)
            except BaseException:result["sampling_error"]=traceback.format_exc()
        sampler=threading.Thread(target=collect);sampler.start()
        with open(lab.output/"client.json","w") as out,open(lab.output/"client.log","w") as err:
            result["client_window"]=dict(start_ns=time.monotonic_ns(),start_unix_ms=time.time_ns()//1_000_000,
                scope="outer invocation; pinned reads/mixed/fanout lack exact monotonic drive timestamps; warmup/settle/seed are zero")
            process=subprocess.Popen(command,stdout=out,stderr=err,env=env)
            result["roles"]["client"]=[process.pid]
            time.sleep(1)  # Inside the counted window, never an uncounted warmup.
            result["gc_delete_window"]=dict(start_ns=time.monotonic_ns(),start_unix_ms=time.time_ns()//1_000_000)
            for name in garbage:
                code,_,_=lab.request(1,"DELETE",PREFIX+name,headers=STRONG)
                assert code==204
            result["gc_delete_window"]["end_ns"]=time.monotonic_ns()
            try:process.wait(timeout=duration+120)
            except subprocess.TimeoutExpired:
                process.kill();process.wait();raise
            finally:
                result["client_exit_code"]=process.returncode
                result["client_window"].update(end_ns=time.monotonic_ns(),end_unix_ms=time.time_ns()//1_000_000)
        stop.set();sampler.join()
        stop_trace()
        assert process.returncode==0,"ds-bench failed"
        raw=json.loads((lab.output/"client.json").read_text())
        if workload in ("hot-write","many-write"):
            assert raw["measure_start_unix_ms"] <= result["gc_delete_window"]["start_unix_ms"] < raw["measure_end_unix_ms"], "GC stimulus outside measured write window"
        result["after_client"]=lab.observe()
        ok=raw.get("ok_total_all_phases",raw.get("write_counts",raw.get("append_counts",{})).get("ok",0))
        result["target_bytes"]=len(names)*size+ok*payload
        start=time.monotonic();result["drain"]=lab.drain(names,result["target_bytes"]);result["drain_seconds"]=time.monotonic()-start
        result["probes"]=probe(lab,names,size,payload,workload,result["drain"])
        if workload=="fanout":
            result["sequence"]=sequence(lab,lab.output)
        events=[e for p in lab.output.glob("objects-*.jsonl") for e in read_lines(p)]
        if "sequence" in result:
            seq=result["sequence"]
            keys={"/"+lab.bucket+"/"+key for key in seq["precondition"]["listing_keys"]} if cold else set()
            seq["objects"]=object_window(events,seq["start_ns"],seq["end_ns"],keys)
            if cold:assert seq["objects"]["client_remote_fetched_bytes"]>=16*4*SEGMENT
            save(lab.output/"sequence.json",seq)
        if cold:
            lab.wait(lambda:not lab.garbage_keys.intersection(o["Key"] for o in lab.objects()),"retired payloads collected after clients release pins",timeout=45)
            result["gc_observed_complete_ns"]=time.monotonic_ns()
        samples=read_lines(lab.output/"samples.jsonl")
        result["check"]=validate(result,raw,events,samples)
        assert result["check"]["verdict"]=="PASS",result["check"]["errors"]
        result["verdict"]="PASS"
    except BaseException as error:
        result["error"]=repr(error);(lab.output/"failure.txt").write_text(traceback.format_exc())
    finally:
        if process and process.poll() is None:process.terminate();process.wait(timeout=15)
        stop.set()
        if sampler:sampler.join()
        try:stop_trace()
        except BaseException as error:
            result["verdict"]="FAIL";result["profile_error"]=repr(error)
        try:
            if lab.nodes:result["final_state"]=lab.observe()
        except BaseException as error:result["final_observation_error"]=repr(error)
        save(lab.output/"result.json",result)  # Preserve the outcome even if teardown itself fails.
        lab.close()
        result["data_files"]={str(p.relative_to(lab.data)):p.stat().st_size for p in lab.data.rglob("*") if p.is_file()}
        for path in list(lab.output.glob("*.jsonl")):
            compress(path)
        for path in lab.output.glob("*.log"):
            if not Path(str(path)+".gz").exists():compress(path)
        # Only this driver's freshly created data; sanitized histories/configs,
        # object events survive. Failed raw stores also stay local for diagnosis.
        if result["verdict"]=="PASS":shutil.rmtree(lab.data)
        else:result["retained_data"]=str(lab.data.relative_to(ROOT))
        save(lab.output/"result.json",result)
    print(json.dumps(dict(cell=lab.output.name,verdict=result["verdict"],error=result.get("error"))),flush=True)
    return result


def run(args):
    os.sched_setaffinity(0,{4,5})  # Observer never competes for the SUT/client CPU pins.
    output=Path(args.output).resolve();output.mkdir(parents=True,exist_ok=False)
    qualification=json.loads((EXPERIMENT/"evidence/cold-runtime-tests-001/qualification.json").read_text())
    assert source_hashes()==qualification["sources"],"runtime changed: requalify before benchmarking"
    assert digest(BINARY)==qualification["release_binary_sha256"]
    for folder,pin in (("electric","88793e76595d69be300731b9b25c58538923a53b"),("ds-bench","93a1a066a511ad2ce5114dc429afb1fd0f6d99bf")):
        repo=ROOT/".tmp/electric-sources"/folder
        assert subprocess.check_output(["/usr/bin/git","-C",str(repo),"rev-parse","HEAD"],text=True).strip()==pin
        assert not subprocess.check_output(["/usr/bin/git","-C",str(repo),"status","--porcelain"])
    plan=dict(version=1,argv=sys.argv,source_commit=subprocess.check_output(["/usr/bin/git","rev-parse","HEAD"],text=True).strip(),
        sources=source_hashes(),binaries={str(p.relative_to(ROOT)):digest(p) for p in (BINARY,NATIVE,CLIENT,MINIO)},
        harness=archive_harness(output),
        native_source="88793e76595d69be300731b9b25c58538923a53b",client_source="93a1a066a511ad2ce5114dc429afb1fd0f6d99bf",
        native_build="cargo build --locked --release --features tier (same native binary hot/cold)",
        arms=args.arms,tiers=args.tiers,workloads=args.workloads,repetitions=args.repetitions,first_repetition=args.first_repetition,
        duration=args.duration,profile=args.profile,
        cpus=dict(sut="0-3 aggregate",provider_and_proxies="4-5",client="6-7"),memory="shared 16 GiB; RSS excludes kernel page cache",
        storage="one shared orb root device; NOT canonical split-NVMe upstream capacity or independent fault domains",
        payloads=dict(hot_write=256,many_write=4096,mixed=256,fanout=256),segment_bytes=SEGMENT,compact_bytes=SEGMENT,
        wal_segment_bytes=8*1024*1024,partitions_or_wal_shards=2,workers_per_process=2,tail_cache_bytes=0,
        cold_inventory_ms=3000,min_free_before=8*1024**3,stop_below=6*1024**3,
        caveats=["Short 64 KiB segments deliberately expose lifecycle work; not upstream 8 MiB default capacity.",
            "Cold tier remains qualification-only, trusted loopback, shared MinIO; no production/provider durability claim.",
            "202 is accepted, not semantic success; stock client discards receipts. Independent all-replica bytes/sequence drain follows.",
            "Reads/mixed/fanout have no exact monotonic drive timestamps. S3 and resources use explicit outer invocation windows.",
            "Pure fanout starts at now: cold-enabled live interference, not forced cold catch-up. Separate finite probe covers that.",
            "Pre-existing published keys cannot be re-uploaded by offloader; no learner/recovery/fork/probe reads during drive.",
            "Remote fetched bytes include whole-object hash verification and are NOT equal to HTTP client bytes.",
            "Metrics/S3 logging overhead is present in all matched cells; not independent calibration or maximum capacity."])
    save(output/"provenance.json",plan)
    results=[]
    choices=[(a,t=="cold") for a in args.arms for t in args.tiers]
    for repetition in range(args.first_repetition,args.first_repetition+args.repetitions):
        for wi,workload in enumerate(args.workloads):
            k=(repetition+wi-1)%len(choices)
            for arm,cold in choices[k:]+choices[:k]:
                name=f"{output.name}-{workload}-{arm}-{'cold' if cold else 'hot'}-r{repetition}"
                result=cell(output/name,arm,cold,workload,repetition,args.duration,args.connections,args.profile)
                results.append(dict(cell=name,verdict=result["verdict"],error=result.get("error")))
                save(output/"ledger.json",results)
    return all(r["verdict"]=="PASS" for r in results)


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    parser.add_argument("--arms",nargs="+",choices=["native","raft1","quorum3","async3"],default=["native","raft1","quorum3","async3"])
    parser.add_argument("--tiers",nargs="+",choices=["hot","cold"],default=["hot","cold"])
    parser.add_argument("--workloads",nargs="+",choices=["hot-write","many-write","replay","fanout","mixed"],default=["hot-write","many-write","replay","fanout","mixed"])
    parser.add_argument("--repetitions",type=int,default=3)
    parser.add_argument("--first-repetition",type=int,default=1,help="retain the logical repetition number when rerunning an affected matched pair")
    parser.add_argument("--duration",type=int,default=24)
    parser.add_argument("--connections",type=int,default=16)
    parser.add_argument("--profile",choices=["none","kernel","fsync","cpu","syscalls"],default="none",help="separate perturbed diagnostics, never capacity results")
    args=parser.parse_args()
    assert 1<=args.repetitions<=3 and 4<=args.duration<=180 and 1<=args.connections<=256
    assert 1<=args.first_repetition<=3 and args.first_repetition+args.repetitions<=4
    raise SystemExit(0 if run(args) else 1)
