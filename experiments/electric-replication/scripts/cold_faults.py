"""Actual multiprocess native cold qualification against pinned local MinIO.

Run with .tmp/electric-tools/cold-venv/bin/python; see CONTRACT.md for setup.
No cloud calls. Process crashes share one Linux host/disk. Fault proxies do not
implement S3 storage: every successful request reaches the separate MinIO server.
"""
import base64
import concurrent.futures
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import shlex
import subprocess
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from lab import Lab, BINARY, EXPERIMENT, ROOT, partition, source_hashes

MINIO = ROOT / ".tmp/electric-tools/minio"
SERVICE = Path(__file__).with_name("cold_service.py")


class ColdLab(Lab):
    def __init__(self, output, **kwargs):
        super().__init__(output, port=20600, **kwargs)
        self.services = []
        self.secret_file = self.data / "local-object-credentials.json"
        keys = dict(access="fixture-"+secrets.token_hex(8), secret=secrets.token_hex(32))
        with os.fdopen(os.open(self.secret_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "w") as file:
            json.dump(keys, file)
        self.s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{self.port+50}", region_name="us-east-1",
                              aws_access_key_id=keys["access"], aws_secret_access_key=keys["secret"],
                              config=Config(retries={"max_attempts": 0}, connect_timeout=2, read_timeout=10))
        self.bucket = "cold-fixture"
        self.segment_bytes = 16 * 1024
        self.inventory_ms = 1500

    def service(self, name, args, port):
        name = self.cluster+"-"+name
        log = self.data / (name+".log")  # Provider diagnostics may contain credentials.
        command = f"ulimit -c 0; exec {shlex.join(map(str,args))} >> {shlex.quote(str(log))} 2>&1"
        subprocess.run(["amp","orb","service","start",name,"--command",command,
                        "--cwd",str(ROOT),"--port",str(port)],check=True,stdout=subprocess.DEVNULL)
        self.services.append(name)

    def create_bucket(self):
        # This MinIO pin's /ready returns 200 even with an uninitialized
        # object layer. Signed ListBuckets exercises the same nil-layer guard
        # as CreateBucket, without retrying a mutating request on unknown I/O.
        observations=[]
        def ready():
            try:
                self.s3.list_buckets()
            except ClientError as error:
                code=error.response["Error"]["Code"]
                observations.append(dict(time_ns=time.monotonic_ns(),code=code))
                if code!="XMinioServerNotInitialized":raise
                return False
            observations.append(dict(time_ns=time.monotonic_ns(),code="ready"))
            return True
        try:
            self.wait(ready,"MinIO S3 object layer ready",timeout=40)
            self.s3.create_bucket(Bucket=self.bucket)  # Exactly one mutating attempt.
        finally:
            (self.output/"storage-startup.json").write_text(json.dumps(observations,indent=2)+"\n")

    def start_storage(self):
        self.service("minio", [sys.executable,SERVICE,"exec",self.secret_file,MINIO,
                              "server","--address",f"127.0.0.1:{self.port+50}",self.data/"minio"],self.port+50)
        def healthy(port):
            connection = http.client.HTTPConnection("127.0.0.1",port,timeout=2)
            try:
                connection.request("GET","/minio/health/ready")
                return connection.getresponse().status == 200
            finally:
                connection.close()
        self.wait(lambda:healthy(self.port+50),"MinIO ready",timeout=40)
        self.create_bucket()
        for node in (1,2,3,4):
            self.object_fault(node)
            self.service(f"s3-{node}",[sys.executable,SERVICE,"proxy",self.port+50,self.port+60+node,
                         self.data/f"object-fault-{node}.json",self.output/f"objects-{node}.jsonl",self.secret_file],self.port+60+node)
            self.wait(lambda:healthy(self.port+60+node),"S3 fault proxy ready")

    def start(self, node, environment=None, **kwargs):
        super().start(node, environment=environment, cold=dict(backend="s3",bucket=self.bucket,
            endpoint=f"http://127.0.0.1:{self.port+60+node}",region="us-east-1",allow_http=True,
            segment_bytes=self.segment_bytes,inventory_interval_ms=self.inventory_ms),
            cold_credentials=self.secret_file, **kwargs)

    def object_fault(self, node, mode="", methods=(), contains=""):
        path = self.data/f"object-fault-{node}.json"
        tmp = path.with_suffix(".next")
        tmp.write_text(json.dumps(dict(mode=mode,methods=list(methods),contains=contains)))
        tmp.replace(path)

    def cold(self, node, group=0, call="Status"):
        status, _, data = self.request(node,"POST",f"/_admin/{group}/cold",json.dumps(call).encode(),
                                      {"x-electric-cluster":self.cluster},timeout=35)
        if call == "Status":
            assert status == 200, (status,data)
        return status, json.loads(data) if data[:1] in (b"{",b"[",b"t",b"f") else data.decode()

    def checkpoint(self, node, group=0):
        cut = self.admin(node,group,"metrics")["last_applied"]["index"]
        assert self.admin(node,group,"snapshot") == {"Ok":None}
        self.wait(lambda: (s := self.admin(node,group,"metrics").get("snapshot")) and s["index"] >= cut,
                  "durable cold snapshot")

    def objects(self):
        return [o for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket)
                for o in page.get("Contents",[])]

    def caught_up(self, group=0):
        cut = self.admin(self.leader(group),group,"metrics")["last_applied"]["index"]
        for node in self.nodes:
            self.wait(lambda: (s := self.admin(node,group,"metrics").get("last_applied")) and s["index"] >= cut,
                      "replica applied prefix")
        return cut

    def close(self):
        try:
            super().close()
        finally:
            for name in reversed(self.services):
                subprocess.run(["amp","orb","service","stop",name],check=True,stdout=subprocess.DEVNULL)
            self.services.clear()
            self.secret_file.unlink(missing_ok=True)  # Retire fixture credentials, never publish them.


def run(output):
    lab = ColdLab(output)
    provenance = dict(binary=hashlib.sha256(BINARY.read_bytes()).hexdigest(),sources=source_hashes(),
        driver=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        service=hashlib.sha256(SERVICE.read_bytes()).hexdigest(),
        minio=dict(repository="https://github.com/minio/minio",commit="0d7408fc9969caf07de6a8c3a84f9fbb10a6739e",
                   tag="RELEASE.2025-04-22T22-12-26Z",license="AGPL-3.0",binary=hashlib.sha256(MINIO.read_bytes()).hexdigest()),
        processes=3,partitions=2,consistency="linearizable default",durability="quorum-fsync default",
        scope="single-host separate S3 process; not provider, independent-disk or power-loss evidence")
    (lab.output/"provenance.json").write_text(json.dumps(provenance,indent=2)+"\n")
    history = []

    def emit(event):
        event = dict(version=1,time_ns=time.monotonic_ns(),**event)
        history.append(event)
        with (lab.output/"history.jsonl").open("a") as target:
            target.write(json.dumps(event)+"\n")

    def state(node, group=0):
        value = lab.cold(node,group)[1]
        emit(dict(op="state",node=node,group=group,state=value))
        return value

    def request(node, method, path, body=b"", headers=None):
        event = dict(op="http",node=node,method=method,path=path,start=time.monotonic_ns(),
                     input_bytes=len(body),input_sha256=hashlib.sha256(body).hexdigest(),
                     input_b64=base64.b64encode(body).decode(),request_headers=headers or {})
        try:
            status, returned, data = lab.request(node,method,path,body,headers,timeout=35)
            event.update(status=status,headers=returned,bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
        except (OSError,http.client.HTTPException) as error:
            emit(dict(**event,status=0,error=type(error).__name__,outcome="unknown",end=time.monotonic_ns()))
            return 0,{},b""
        emit(dict(**event,end=time.monotonic_ns()))
        return status,returned,data

    def read(node, path, expected, mode="prefix"):
        status, headers, data = request(node,"GET",path+"?offset=-1",headers={"stream-consistency":mode})
        emit(dict(op="byte-probe",node=node,path=path,status=status,expected_bytes=len(expected),
                  expected_sha256=hashlib.sha256(expected).hexdigest(),bytes=len(data),sha256=hashlib.sha256(data).hexdigest()))
        assert status == 200 and data == expected, (status,len(data),len(expected))
        return headers

    def leader(group=0):
        return lab.wait(lambda:lab.leader(group),"current group leader")

    def object_events(node):
        return [json.loads(line) for line in (lab.output/f"objects-{node}.jsonl").read_text().splitlines()]

    def gone(keys):
        return not set(keys).intersection(o["Key"] for o in lab.objects())

    def choose(name, group):
        return next(f"/{name}/{i}" for i in range(16) if partition(f"/{name}/{i}",2) == group)

    def force_seal(path):
        group = partition(path,2)
        status, value = lab.cold(leader(group),group,{"Seal":{"path":path}})
        emit(dict(op="seal",path=path,status=status,value=value))
        assert status == 200, (status,value)

    def replace_inventory(group=0):
        before = state(leader(group),group)["floor"]
        lab.cold(leader(group),group,"Inventory")  # Pending/certification can return 503.
        lab.wait(lambda: state(leader(group),group)["floor"] > before,"inventory replacement",timeout=45)

    def pause_snapshots(node, paused):
        status, _, _ = lab.request(node,"POST","/_admin/network",
            json.dumps(dict(blocked=[],delay_ms=0,pause_snapshots=paused)).encode(),
            {"x-electric-cluster":lab.cluster})
        assert status == 200

    def add_learner(node):
        return lab.admin(leader(),0,"learner",[node,{"addr":f"127.0.0.1:{lab.port+node}"}])

    try:
        lab.start_storage()
        for node in (1,2,3):
            lab.start(node)
        lab.initialize()
        parent = next(f"/cold/{i}" for i in range(10) if partition(f"/cold/{i}",2) == 0)
        owner = lab.leader(0)
        # Different bytes in every segment expose duplication, misrouting and
        # range-offset mistakes that a repeating 256-byte pattern cannot. Three
        # SSE windows distinguish bounded catch-up from whole-prefix buffering.
        payload = b"".join(hashlib.sha256(f"cold-byte-block-{i}".encode()).digest() for i in range(6144))
        assert request(owner,"PUT",parent,payload)[0] == 201
        lab.wait(lambda: len(state(owner)["objects"]) >= len(payload)//lab.segment_bytes and state(owner)["local_payload_bytes"] == 0,
                 "quorum cold publication and actual hot reclamation",timeout=40)
        lab.caught_up()
        for node in (1,2,3):
            files = [p for p in (lab.data/str(node)/"0/state/hot/streams").rglob("*")
                     if p.is_file() and "." not in p.name]
            assert len(files) == 1 and files[0].stat().st_size == 0
            emit(dict(op="hot-reclaimed",node=node,files=[dict(path=str(p.relative_to(lab.data)),
                size=p.stat().st_size,allocated=p.stat().st_blocks*512) for p in files]))
        for node in (1,2,3):
            lab.checkpoint(node)
        for node in (1,2,3):
            lab.stop(node,crash=True)
        for node in (1,2,3):
            lab.start(node)
        owner = lab.wait(lambda:lab.leader(0),"whole cluster certified restart")
        for node in (1,2,3):
            read(node,parent,payload)
        lab.wait(lambda: state(owner)["floor"] > 0,"certified inventory activation",timeout=40)

        # Binary fork sub-offsets cross remote segment boundaries too. Derive
        # expected bytes from the client invocation, not the native resolver.
        binary_child = choose("binary-child",1)
        binary_cut = 2*16384+137
        h = {"stream-forked-from":parent,"stream-fork-offset":"-1","stream-fork-sub-offset":str(binary_cut)}
        assert request(leader(1),"PUT",binary_child,b"fork-tail",h)[0] == 201
        lab.caught_up(1)
        for node in (1,2,3):
            read(node,binary_child,payload[:binary_cut]+b"fork-tail")
        assert request(leader(1),"DELETE",binary_child)[0] == 204

        # A real SSE catch-up must fetch the reclaimed binary prefix, then
        # deliver a new committed append exactly once and observe closure.
        sse_start = time.monotonic_ns()
        connection = http.client.HTTPConnection("127.0.0.1",lab.port+owner,timeout=10)
        connection.request("GET",parent+"?offset=-1&live=sse")
        response = connection.getresponse()
        assert response.status == 200 and response.getheader("stream-sse-data-encoding") == "base64"
        wire, buffer, streamed = bytearray(), bytearray(), bytearray()
        def sse_until(length):
            while len(streamed) < length:
                chunk = response.read1(65536)
                assert chunk, "cold SSE ended before the committed prefix"
                wire.extend(chunk); buffer.extend(chunk)
                assert len(wire) < 1024*1024
                while b"\n\n" in buffer:
                    event, _, rest = buffer.partition(b"\n\n")
                    buffer[:] = rest
                    lines = event.splitlines()
                    if b"event: data" in lines:
                        streamed.extend(base64.b64decode(b"\n".join(l[5:].lstrip() for l in lines if l.startswith(b"data:")),validate=True))
        sse_until(len(payload))
        assert bytes(streamed) == payload
        catchup_end = time.monotonic_ns()
        extra_live = b"committed-after-cold-catchup"
        assert request(owner,"POST",parent,extra_live,
                       {"stream-closed":"true","content-type":"application/octet-stream"})[0] == 204
        parent_bytes = payload+extra_live
        sse_until(len(parent_bytes))
        assert bytes(streamed) == parent_bytes
        wire.extend(response.read(1024*1024))
        assert b'"streamClosed":true' in wire and len(wire) < 1024*1024
        response.close(); connection.close()
        emit(dict(op="cold-sse",node=owner,path=parent,start=sse_start,catchup_end=catchup_end,
                  wire_b64=base64.b64encode(wire).decode()))

        # Native JSON framing and cross-partition fork sub-offsets, including
        # a value spanning the configured segment boundary.
        jp, child = choose("json-source",0), choose("json-child",1)
        values = [{"text":"escaped\\\"\n"*2600,"n":19}, [41,"last"]]
        json_bytes = json.dumps(values,separators=(",",":")).encode()
        assert request(leader(),"PUT",jp,json_bytes,{"content-type":"application/json"})[0] == 201
        force_seal(jp)
        for node in (1,2,3):
            lab.checkpoint(node)
        reader = next(n for n in (1,2,3) if n != leader())
        lab.stop(reader,crash=True)
        lab.start(reader)
        read(reader,jp,json_bytes)
        fork_headers = {"stream-forked-from":jp,"stream-fork-offset":"-1","stream-fork-sub-offset":"1"}
        assert request(leader(1),"PUT",child,b"[399]",fork_headers)[0] == 201
        expected_child = json.dumps([values[0],399],separators=(",",":")).encode()
        lab.caught_up(1)
        for node in (1,2,3):
            read(node,child,expected_child)
        assert request(leader(),"DELETE",jp)[0] == 204
        assert request(leader(1),"PUT",child,b"[777]",fork_headers)[0] == 200
        read(leader(1),child,expected_child)

        # Hold an actual object GET while the original stream retires. This is
        # a live HTTP reader, not a test-only pin in a metadata fixture.
        keys = [o["key"] for o in state(leader())["objects"] if o["len"] == 16384]
        assert len(keys) == len(payload)//lab.segment_bytes
        reader = next(n for n in (1,2,3) if n != leader())
        lab.object_fault(reader,"hold",["GET"],keys[0])
        start = time.monotonic_ns()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            held = pool.submit(request,reader,"GET",parent+"?offset=-1",headers={"stream-consistency":"prefix"})
            lab.wait(lambda:any(e["op"] == "held" and e["time_ns"] >= start for e in object_events(reader)),"real cold GET held")
            pin = state(reader)["oldest_pin"]
            assert pin is not None
            assert request(leader(),"DELETE",parent)[0] == 204
            lab.wait(lambda: not set(keys).intersection(o["key"] for o in state(leader())["objects"]),"last binary owner retired")
            lab.cold(leader(),0,"Inventory")
            pending = lab.wait(lambda: next((o for o in state(leader())["pending"].values()
                if "Inventory" in o["kind"] and o["kind"]["Inventory"]["cut"] > pin),None),"new cut blocked by reader")
            cut = pending["kind"]["Inventory"]["cut"]
            assert state(leader())["floor"] <= pin
            assert all(key in {o["Key"] for o in lab.objects()} for key in keys)
            emit(dict(op="reader-fence",node=reader,pin=pin,cut=cut,keys=keys))
            lab.object_fault(reader)
            status, _, observed = held.result(timeout=20)
            assert status == 200 and observed == parent_bytes
            emit(dict(op="retired-reader-complete",node=reader,bytes=len(observed),sha256=hashlib.sha256(observed).hexdigest()))
        lab.wait(lambda:gone(keys),"retired binary objects reclaimed after reader drainage",timeout=45)
        emit(dict(op="reclaimed",keys=keys))

        # Provider corruption and outage must fail the HTTP body, never emit a
        # clean-but-truncated success. Snapshot/restart removes the hot cache.
        damaged = choose("damaged",0)
        assert request(leader(),"PUT",damaged,payload)[0] == 201
        lab.wait(lambda:len([o for o in state(leader())["objects"] if o["len"] == 16384]) >= len(payload)//lab.segment_bytes,"second cold payload")
        lab.checkpoint(reader)
        lab.stop(reader,crash=True)
        lab.start(reader)
        read(reader,damaged,payload)
        candidate = next(o["key"] for o in state(leader())["objects"] if o["len"] == 16384)
        original = lab.s3.get_object(Bucket=lab.bucket,Key=candidate)["Body"].read()
        lab.s3.put_object(Bucket=lab.bucket,Key=candidate,Body=bytes([original[0]^1])+original[1:])
        assert request(reader,"GET",damaged+"?offset=-1",headers={"stream-consistency":"prefix"})[0] == 0
        emit(dict(op="corruption-rejected",key=candidate,node=reader))
        lab.s3.put_object(Bucket=lab.bucket,Key=candidate,Body=original)
        lab.object_fault(reader,"unavailable",["GET"],"/p/")
        assert request(reader,"GET",damaged+"?offset=-1",headers={"stream-consistency":"prefix"})[0] == 0
        lab.object_fault(reader)
        read(reader,damaged,payload)

        # Unknown short PUT followed by append: retry must use the original
        # descriptor, not the now-larger tail. The actual queued write is later
        # delivered to MinIO after its client has already seen an error.
        short = choose("short",0)
        owner = leader()
        lab.object_fault(owner,"queue-put",["PUT"],"/p/")
        initial, extra = b"short-range-"*23, b"later-append-"*31
        assert request(owner,"PUT",short,initial)[0] == 201
        lab.cold(owner,0,{"Seal":{"path":short}})
        pending = next(o for o in state(owner)["pending"].values() if o["kind"].get("Payload",{}).get("path") == short)
        assert pending["len"] == len(initial)
        headers = {"content-type":"application/octet-stream","producer-id":"cold-retry","producer-epoch":"0","producer-seq":"0"}
        assert request(owner,"POST",short,extra,headers)[0] == 200
        assert request(owner,"POST",short,b"must-not-append",headers)[0] == 204
        lab.object_fault(owner)
        lab.wait(lambda:str(pending["birth"]) not in state(leader())["pending"],"exact immutable-range retry")
        read(leader(),short,initial+extra)
        connection = http.client.HTTPConnection("127.0.0.1",lab.port+60+owner,timeout=15)
        connection.request("POST","/_fault/release")
        assert connection.getresponse().status == 200
        connection.close()
        read(leader(),short,initial+extra)

        # Abort and retire another unknown upload, collect through a newer
        # inventory, THEN deliver the old PUT. Cumulative scans must wrap back.
        late = choose("late",0)
        owner = leader()
        lab.object_fault(owner,"queue-put",["PUT"],"/p/")
        assert request(owner,"PUT",late,initial)[0] == 201
        lab.cold(owner,0,{"Seal":{"path":late}})
        pending = next(o for o in state(owner)["pending"].values() if o["kind"].get("Payload",{}).get("path") == late)
        born = pending["birth"]
        assert lab.cold(owner,0,{"Abort":{"birth":born}})[0] == 204
        assert request(owner,"DELETE",late)[0] == 204
        lab.object_fault(owner)
        replace_inventory()
        replace_inventory()
        before = {o["Key"] for o in lab.objects()}
        connection = http.client.HTTPConnection("127.0.0.1",lab.port+60+owner,timeout=15)
        connection.request("POST","/_fault/release")
        response = connection.getresponse()
        assert response.status == 200 and json.loads(response.read())["released"] >= 1
        connection.close()
        late_keys = [e["path"].split("/",2)[2] for e in object_events(owner)
                     if e["op"] == "late-put" and f"/{born:020}/" in e["path"]]
        assert late_keys and not set(late_keys).intersection(before)
        assert lab.cold(leader(),0,{"Resume":{"birth":born}})[0] == 409
        lab.wait(lambda:gone(late_keys),"late physical resurrection reclaimed on repeated scan",timeout=45)
        emit(dict(op="late-reclaimed",birth=born,keys=late_keys))

        # Inventory bytes at an old cut cannot be reconstructed from a newer
        # state. Lose the uploader after real multipart parts reach MinIO,
        # recover by aborting the Begin, then complete the old PUT anyway.
        replace_inventory()
        owner = leader()
        lab.object_fault(owner,"queue-inventory",["POST","DELETE"],"/i/")
        start = time.monotonic_ns()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            upload = pool.submit(lab.cold,owner,0,"Inventory")
            queued = lab.wait(lambda:next((e for e in object_events(owner)
                if e["op"] == "queued-inventory" and e["time_ns"] >= start
                and f"/{lab.cluster}/0/" in e["path"]),None),"actual inventory multipart completion held",timeout=30)
            key = queued["path"].split("/",2)[2]
            born = int(key.split("/")[3])
            assert str(born) in state(owner)["pending"] and gone([key])
            lab.stop(owner,crash=True)
            try:
                outcome = upload.result(timeout=35)[0]
            except (OSError,http.client.HTTPException):
                outcome = 0
            emit(dict(op="inventory-upload-crash",node=owner,birth=born,key=key,outcome=outcome))
        lab.wait(lambda:str(born) not in state(leader())["pending"],"new owner aborts unreconstructible inventory",timeout=45)
        lab.object_fault(owner)
        lab.start(owner)
        lab.caught_up()
        replace_inventory()
        replace_inventory()
        connection = http.client.HTTPConnection("127.0.0.1",lab.port+60+owner,timeout=15)
        connection.request("POST","/_fault/release")
        response = connection.getresponse()
        assert response.status == 200 and json.loads(response.read())["inventories"] >= 1
        connection.close()
        assert any(e["op"] == "late-inventory" and e["path"].endswith(key) for e in object_events(owner))
        assert lab.cold(leader(),0,{"Resume":{"birth":born}})[0] == 409
        lab.wait(lambda:gone([key]),"late multipart inventory reclaimed after crash/replacement",timeout=45)
        emit(dict(op="late-inventory-reclaimed",node=owner,birth=born,key=key))

        # Repeated replacements/retirements must not accumulate one tombstone
        # per object. Unreachable registered participants are tested below.
        for generation in range(12):
            path = choose(f"generation-{generation}",0)
            assert request(leader(),"PUT",path,payload[:16384])[0] == 201
            force_seal(path)
            assert request(leader(),"DELETE",path)[0] == 204
            replace_inventory()
        replace_inventory()
        emit(dict(op="generation-bound",generations=12,state=state(leader()),physical_objects=len(lab.objects())))

        # PUT completion alone cannot publish a cold manifest or reclaim the
        # local payload on a leader that has lost its quorum. Peers cannot
        # verify the object until the object fault is lifted separately.
        isolated = choose("isolated-offload",0)
        old = leader()
        peers = {1,2,3}-{old}
        assert request(old,"PUT",isolated,initial)[0] == 201
        lab.object_fault(old,"hold",["PUT"],"/p/")
        for node in peers:
            lab.object_fault(node,"unavailable",["HEAD","GET","PUT"],"/p/")
        start = time.monotonic_ns()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            offload = pool.submit(lab.cold,old,0,{"Seal":{"path":isolated}})
            lab.wait(lambda:any(e["op"] == "held" and e["method"] == "PUT" and e["time_ns"] >= start
                               for e in object_events(old)),"durable Begin followed by held upload")
            pending = next(o for o in state(old)["pending"].values() if o["kind"].get("Payload",{}).get("path") == isolated)
            lab.faults(old,blocked=peers)
            for node in peers:
                lab.faults(node,blocked=[old])
            lab.wait(lambda:lab.leader(0,peers),"new majority leader while old uploader is isolated")
            lab.object_fault(old)
            assert offload.result(timeout=20)[0] == 503
            stale = state(old)
            assert str(pending["birth"]) in stale["pending"]
            assert pending["birth"] not in {o["birth"] for o in stale["objects"]}
            emit(dict(op="quorum-publication-fence",node=old,birth=pending["birth"],state=stale))
        for node in (1,2,3):
            lab.faults(node)
            lab.object_fault(node)
        lab.wait(lambda:any(o["birth"] == pending["birth"] for o in state(leader())["objects"]),"quorum publication after healing",timeout=45)
        lab.caught_up()
        read(old,isolated,initial)

        # An actual OpenRaft snapshot remains in flight while a JSON fork and
        # both deletions commit after its cut. Replaying that suffix on the new
        # learner requires the retired source bytes, not just snapshot metadata.
        moving, moved_child = choose("transfer-json",0), choose("transfer-child",0)
        prior = {o["key"] for o in state(leader())["objects"]}
        assert request(leader(),"PUT",moving,json_bytes,{"content-type":"application/json"})[0] == 201
        force_seal(moving)
        moving_keys = {o["key"] for o in state(leader())["objects"]} - prior
        assert moving_keys
        lab.caught_up()
        lab.start(4)
        source = leader()
        pause_snapshots(source,True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            transfer = pool.submit(add_learner,4)
            def held_snapshot():
                events = [json.loads(line) for line in (lab.output/f"node-{source}.log").read_text().splitlines()
                          if line.startswith("{")]
                return next((e for e in events if e.get("event") == "fault.snapshot-held"
                             and e["group"] == 0 and e["target"] == 4),None)
            sent = lab.wait(held_snapshot,"actual paused snapshot RPC, not merely a builder/GC pin")
            pin = sent["index"]
            assert "4" in state(source)["participants"] and state(source)["oldest_pin"] <= pin
            # OpenRaft's blocking add_learner permits its configured lag (5000
            # by default). Its response is NOT an installed-snapshot barrier.
            assert state(4)["applied"] is None
            transfer_start = time.monotonic_ns()
            h = {"stream-forked-from":moving,"stream-fork-offset":"-1","stream-fork-sub-offset":"1"}
            assert request(source,"PUT",moved_child,b"[73]",h)[0] == 201
            assert request(source,"DELETE",moving)[0] == 204
            assert request(source,"DELETE",moved_child)[0] == 204
            lab.wait(lambda: not moving_keys.intersection(o["key"] for o in state(source)["objects"]),"transfer source retired")
            lab.cold(source,0,"Inventory")
            assert state(source)["floor"] <= state(source)["participants"]["4"] <= pin
            assert moving_keys.issubset(o["Key"] for o in lab.objects())
            emit(dict(op="transfer-fence",node=source,learner=4,pin=pin,keys=sorted(moving_keys)))
            pause_snapshots(source,False)
            result = transfer.result(timeout=25)
            assert "Ok" in result, result
        lab.caught_up()
        assert lab.admin(4,0,"metrics")["snapshot"] is not None
        remote = [e for e in object_events(4) if e["op"] == "s3" and e["method"] == "GET"
                  and e["time_ns"] >= transfer_start and any(k in e["path"] for k in moving_keys)]
        assert remote, "learner must replay the JSON fork through real cold objects"
        emit(dict(op="transfer-complete",node=4,remote_gets=len(remote),keys=sorted(moving_keys)))
        lab.wait(lambda:gone(moving_keys),"transfer dependencies reclaimed after completed replay/purge",timeout=45)

        # Voting removal is NOT storage drainage. The removed replica may still
        # serve its stale committed view and needs those exact objects on reboot.
        blocked = choose("removed-owner",0)
        prior = {o["key"] for o in state(leader())["objects"]}
        assert request(leader(),"PUT",blocked,payload[:16384])[0] == 201
        force_seal(blocked)
        protected = {o["key"] for o in state(leader())["objects"]} - prior
        assert protected
        lab.caught_up()
        removed = next(n for n in (1,2,3) if n != leader())
        voters = sorted({1,2,3,4}-{removed})
        assert "Ok" in lab.admin(leader(),0,"membership",voters)
        removal_floor = state(leader())["participants"][str(removed)]
        assert request(leader(),"DELETE",blocked)[0] == 204
        lab.wait(lambda:not protected.intersection(o["key"] for o in state(leader())["objects"]),"removed participant's source retired")
        read(removed,blocked,payload[:16384])
        lab.stop(removed,crash=True)
        for _ in range(4):
            lab.cold(leader(),0,"Inventory")
            assert state(leader())["floor"] <= removal_floor
            assert protected.issubset(o["Key"] for o in lab.objects())
            time.sleep(0.5)
        emit(dict(op="removed-fence",node=removed,floor=removal_floor,keys=sorted(protected),voters=voters))
        lab.start(removed)
        read(removed,blocked,payload[:16384])
        assert "Ok" in add_learner(removed)
        lab.caught_up()
        replace_inventory()
        lab.wait(lambda:gone(protected),"rejoined removed participant certifies completed recovery",timeout=45)
        emit(dict(op="removed-recovered",node=removed,keys=sorted(protected)))

        # A fresh scanner after full restart has no verified local inventory.
        # A corrupt remote inventory grants no delete authority; payload reads
        # remain available because they use a separate object namespace.
        survivor = choose("survivor",0)
        assert request(leader(),"PUT",survivor,payload)[0] == 201
        lab.wait(lambda:state(leader())["local_payload_bytes"] < len(payload),"surviving payload compacted")
        for node in sorted(lab.nodes):
            lab.checkpoint(node)
            lab.object_fault(node,"corrupt",["GET"],"/i/")
        active = sorted(lab.nodes)
        for node in active:
            lab.stop(node,crash=True)
        for node in active:
            lab.start(node)
        owner = leader()
        for key in late_keys:
            lab.s3.put_object(Bucket=lab.bucket,Key=key,Body=initial)
        for node in active:
            read(node,survivor,payload)
        lab.wait(lambda:state(owner)["counters"].get("inventory_read_errors",0) >= 3,"corrupt inventory rejected after restart")
        assert set(late_keys).issubset(o["Key"] for o in lab.objects())
        emit(dict(op="inventory-fail-closed",keys=late_keys,live_path=survivor))
        for node in active:
            lab.object_fault(node)
        lab.wait(lambda:gone(late_keys),"valid replacement inventory restores collection",timeout=45)
        # S3 completes DELETE but the proxy loses its successful reply.
        owner = leader()
        lab.object_fault(owner,"unknown-delete",["DELETE"],late_keys[0])
        start = time.monotonic_ns()
        lab.s3.put_object(Bucket=lab.bucket,Key=late_keys[0],Body=initial)
        lab.wait(lambda:any(e["op"] == "s3" and e["method"] == "DELETE" and e["fault"] == "unknown-delete"
                            and e["time_ns"] >= start for e in object_events(owner)),"actual unknown DELETE")
        assert gone(late_keys)
        lab.object_fault(owner)
        read(owner,survivor,payload)
        emit(dict(op="unknown-delete-reconciled",keys=late_keys))

        # Apply/recovery can need remote JSON bytes. A failed provider read must
        # stop that replica, not turn one committed command into divergent data.
        recovering, recovered_child = choose("replay-source",0), choose("replay-child",0)
        assert request(leader(),"PUT",recovering,json_bytes,{"content-type":"application/json"})[0] == 201
        force_seal(recovering)
        lab.caught_up()
        target = next(n for n in sorted(lab.nodes) if n != leader())
        lab.checkpoint(target)
        lab.stop(target,crash=True)
        lab.start(target)
        lab.object_fault(target,"unavailable",["GET"],"/p/")
        h = {"stream-forked-from":recovering,"stream-fork-offset":"-1","stream-fork-sub-offset":"1"}
        assert request(leader(),"PUT",recovered_child,b"[83]",h)[0] == 201

        def dead(node):
            pid = int((lab.output/f"node-{node}.pid").read_text())
            proc = Path(f"/proc/{pid}/stat")
            return not proc.exists() or proc.read_text().split(") ",1)[1].startswith("Z")

        lab.wait(lambda:dead(target),"remote committed apply failure must fail-stop",timeout=45)
        emit(dict(op="remote-apply-fail-stop",node=target,path=recovered_child))
        lab.stop(target)
        lab.object_fault(target)
        lab.start(target)
        lab.caught_up()
        read(target,recovered_child,json.dumps([values[0],83],separators=(",",":")).encode())

        # Actual native fsync errors: an unsynced snapshot/certificate must not
        # publish a recovery floor. These are real syscalls, not storage mocks.
        interposer = ROOT/".tmp/electric-tools/storage_faults.so"
        c_source = Path(__file__).with_name("storage_faults.c")
        subprocess.run(["cc","-shared","-fPIC","-O2","-Wall","-Wextra","-Werror",
                        "-o",str(interposer),str(c_source),"-ldl","-pthread"],check=True)
        provenance.update(interposer_sha256=hashlib.sha256(interposer.read_bytes()).hexdigest(),
                          interposer_source_sha256=hashlib.sha256(c_source.read_bytes()).hexdigest())
        (lab.output/"provenance.json").write_text(json.dumps(provenance,indent=2)+"\n")
        flag = lab.data/f"storage-fault-{target}"
        environment = dict(LD_PRELOAD=str(interposer),DS_TEST_DATA_ROOT=str(lab.data/str(target)),
                           DS_TEST_FAULT_FILE=str(flag),DS_TEST_FAULT_LOG=str(lab.output/f"syscalls-{target}.jsonl"))
        lab.stop(target)
        lab.start(target,environment=environment)
        for label, pattern in (("certificate","/0/state/cold-certificate.next"),("snapshot","/0/state/snapshot-")):
            flag.write_text(f"eio-sync {pattern}\n")
            path = choose("fsync-"+label,0)
            assert request(leader(),"PUT",path,b"durable before local failure")[0] == 201
            cut = lab.admin(leader(),0,"metrics")["last_applied"]["index"]
            def drive_failure():
                if dead(target):
                    return True
                lab.cold(target,0,{"Certificate":{"floor":cut}})
                return False
            lab.wait(drive_failure,"native fsync failure must fail-stop",timeout=45)
            intercepted = [json.loads(line) for line in (lab.output/f"syscalls-{target}.jsonl").read_text().splitlines()]
            assert any(e["fault"] == "eio-sync" and pattern in e["path"] for e in intercepted)
            emit(dict(op="storage-fail-stop",node=target,kind=label,cut=cut))
            lab.stop(target)
            flag.unlink()
            lab.start(target,environment=environment)
            lab.caught_up()
            read(target,survivor,payload)
        emit(dict(op="storage-recovered",node=target))

        # Exceed the collector's 128-key logical page with simultaneously live
        # immutable objects, then retire them while other cold streams survive.
        before = {o["Key"] for o in lab.objects() if "/p/" in o["Key"]}
        baseline = state(leader())["live_objects"]
        bulk = [choose(f"bulk-{i}",0) for i in range(160)]
        for path in bulk:
            assert request(leader(),"PUT",path,payload[:1024])[0] == 201
            force_seal(path)
        keys = {o["Key"] for o in lab.objects() if "/p/" in o["Key"]} - before
        assert len(keys) == len(bulk)
        emit(dict(op="bulk-live",objects=len(keys),keys=sorted(keys),baseline_live=baseline))
        for path in bulk:
            assert request(leader(),"DELETE",path)[0] == 204
        lab.wait(lambda:lab.cold(leader())[1]["live_objects"] <= baseline,"bulk ownership retirement",timeout=90)
        lab.wait(lambda:gone(keys),"multi-page cumulative collection",timeout=90)
        final = state(leader())
        assert final["live_objects"] <= baseline
        read(leader(),survivor,payload)
        emit(dict(op="bulk-reclaimed",objects=len(keys),live_objects=final["live_objects"],keys=sorted(keys)))
        # Initial membership must obey the same bound as later registrations.
        invalid = json.loads((lab.output/"node-1.json").read_text())
        invalid["dir"] = str(lab.data/"invalid-genesis")
        invalid["genesis"] = {str(n):{"addr":f"127.0.0.1:{21000+n}"} for n in range(1,66)}
        config = lab.output/"invalid-genesis.json"
        config.write_text(json.dumps(invalid,indent=2)+"\n")
        rejected = subprocess.run(["bash","-c",'ulimit -c 0; exec "$@"',"admission-check",str(BINARY),
            "--cluster-config",str(config)],capture_output=True,timeout=10)
        (lab.output/"invalid-genesis.txt").write_bytes(rejected.stdout+rejected.stderr)
        assert rejected.returncode != 0 and b"cold genesis exceeds the storage participant bound" in rejected.stderr
        assert not Path(invalid["dir"]).exists()
        emit(dict(op="genesis-bound-rejected",participants=65,status=rejected.returncode))
        result = dict(verdict="PASS",phase="real-S3 publication/read/reader/transfer/removed-participant/restart/object-fault campaign",
                      events=len(history),objects=len(lab.objects()))
        (lab.output/"result.json").write_text(json.dumps(result,indent=2)+"\n")
        print(json.dumps(result))
    except BaseException as error:
        (lab.output/"result.json").write_text(json.dumps(dict(verdict="FAIL",error=repr(error)),indent=2)+"\n")
        raise
    finally:
        lab.close()


if __name__ == "__main__":
    run(sys.argv[1])
