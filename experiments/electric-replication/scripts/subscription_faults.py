"""Real HTTP, native WAL and process faults for partition-owned subscriptions.

Claims/cursors are checked independently from this driver. The receiver records
raw signed deliveries, including duplicates and failed responses. Loopback-only,
single orb: no claim of independent disks, network hosts or power-loss durability.
"""
import base64
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import http.client
import json
import shlex
import subprocess
import sys
import threading
import time
import traceback
from urllib.parse import quote

from lab import Lab, BINARY, EXPERIMENT, ROOT, partition
from check_subscriptions import check


def run(output):
    lab = Lab(output, partitions=2, port=19600)
    receiver = lab.cluster+"-receiver"
    receiver_port = lab.port+10
    lock = threading.Lock()
    history = []
    provenance = dict(binary=hashlib.sha256(BINARY.read_bytes()).hexdigest(), partitions=2,
        processes=3, consistency="linearizable default", durability="quorum-fsync default",
        faults="SIGKILL, full restart, lost/delayed webhook replies, failed dispatch, claim races",
        sources={str(p.relative_to(EXPERIMENT)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in (EXPERIMENT / "engine/src").rglob("*.rs")})
    (lab.output / "provenance.json").write_text(json.dumps(provenance,indent=2)+"\n")

    def record(event):
        with lock:
            history.append(event)
            with open(lab.output / "history.jsonl", "a") as file:
                file.write(json.dumps(event)+"\n")

    def route(path):
        path = path.split("?")[0]
        if "/__ds/subscriptions/" in path:
            mount, tail = path.split("/__ds/subscriptions/",1)
            path = mount+"/__ds/subscriptions/"+tail.split("/")[0]
        return partition(path,2)

    def request(op, method, path, body=b"", headers=None, node=None, lose_response=False):
        if node is None:
            node = lab.wait(lambda: lab.leader(route(path)), "find request owner")
        if not isinstance(body, bytes):
            body = json.dumps(body).encode()
            headers = {"content-type":"application/json", **(headers or {})}
        event = dict(op=op, method=method, path=path, body=base64.b64encode(body).decode(),
                     headers=headers or {}, node=node, start=time.monotonic())
        try:
            if lose_response:
                connection = http.client.HTTPConnection("127.0.0.1",lab.port+node,timeout=8)
                connection.request(method,path,body,headers or {})
                time.sleep(0.25)
                connection.close()  # No response observed; it may have committed.
                event.update(status=0,result="",outcome="unknown",error="client deliberately dropped response")
            else:
                status, response_headers, data = lab.request(node,method,path,body,headers)
                event.update(status=status,response_headers=response_headers,result=base64.b64encode(data).decode())
        except (OSError, TimeoutError) as error:
            event.update(status=0,result="",outcome="unknown",error=str(error))
        event["end"] = time.monotonic()
        record(event)
        return event

    def value(event):
        return json.loads(base64.b64decode(event["result"]))

    def ok(event, status):
        assert event["status"] == status, (event["op"],event["path"],event["status"],base64.b64decode(event["result"]))
        return event

    def receiver_call(method, path, body=None):
        c = http.client.HTTPConnection("127.0.0.1",receiver_port,timeout=5)
        try:
            c.request(method,path,json.dumps(body) if body is not None else None)
            r = c.getresponse()
            data = r.read()
            assert r.status == 200, (r.status,data)
            return json.loads(data)
        finally:
            c.close()

    def policy(path, mode):
        receiver_call("PUT","/_policy",dict(path=path,mode=mode))

    def received(path):
        return [e for e in receiver_call("GET","/_records") if e["op"] == "webhook" and e["path"] == path]

    def wait_delivery(path, count):
        return lab.wait(lambda: (events if len(events := received(path)) >= count else None),
                        f"receive {count} deliveries on {path}",timeout=35)[count-1]

    def release_delivery(event, mode):
        receiver_call("PUT",f"/_reply/{event['id']}",dict(mode=mode))

    def chosen(prefix, group):
        return next(f"/r/{prefix}-{n}" for n in range(100) if partition(f"/r/{prefix}-{n}",2) == group)

    def kill(node):
        start = time.monotonic()
        lab.stop(node,crash=True)
        record(dict(op="SIGKILL",node=node,start=start,end=time.monotonic()))
        for group in (0,1):
            lab.wait(lambda: lab.leader(group),"elect after SIGKILL")

    def admin(group, operation):
        node = lab.leader(group)
        return ok(request("admin","POST",f"/_admin/{group}/{operation}",None,
                          {"x-electric-cluster":lab.cluster},node),200)

    def read_sub(path):
        return value(ok(request("sub-read","GET",path),200))

    def cursor(path, stream):
        return next(s["acked_offset"] for s in read_sub(path)["streams"] if s["path"] == stream)

    def ack(path, claim, acks=(), done=False, operation="ack", token=None):
        return request(operation,"POST",path+"/"+operation,
                       dict(wake_id=claim["wake_id"],generation=claim["generation"],acks=list(acks),done=done),
                       {"authorization":"Bearer "+(token if token is not None else claim.get("token",claim.get("callback_token")))})

    def claim(path, worker):
        return value(ok(request("claim","POST",path+"/claim",dict(worker=worker)),200))

    def all_acks(claim):
        return [dict(stream=s["path"],offset=s["tail_offset"]) for s in claim["streams"]]

    pull = chosen("__ds/subscriptions/pull",0)
    a, b, manual = chosen("events/a",1), chosen("events/b",0), chosen("manual/x",1)
    wake = chosen("wake/pool",1)
    relative = lambda p: p.removeprefix("/r/")
    cfg = dict(type="pull-wake",pattern="events/**",streams=[relative(a)],
               wake_stream=relative(wake),lease_ttl_ms=8000)
    receiver_started = False
    try:
        command = f"exec python3 {shlex.quote(str(EXPERIMENT/'scripts/subscription_receiver.py'))} {shlex.quote(str(lab.output))} {receiver_port}"
        subprocess.run(["amp","orb","service","start",receiver,"--command",command,"--cwd",str(ROOT),"--port",str(receiver_port)],
                       check=True,stdout=subprocess.DEVNULL)
        receiver_started = True
        lab.wait(lambda: receiver_call("GET","/health") == {},"receiver ready")
        for node in (1,2,3):
            lab.start(node)
        lab.initialize()
        assert lab.leader(0) != lab.leader(1), "different group leaders required"
        ok(request("stream-create","PUT",a,b"before"),201)
        ok(request("stream-create","PUT",manual,b"historic-manual"),201)
        ok(request("sub-create","PUT",pull,cfg),201)
        ok(request("sub-create","PUT",pull,cfg),200)
        initial = cursor(pull,relative(a))
        first = ok(request("stream-append","POST",a,b"new1",{"content-type":"application/octet-stream"}),204)
        first_tail = first["response_headers"]["stream-next-offset"]
        ok(request("stream-create","PUT",b,b"late-discovery"),201)
        lab.wait(lambda: any(s["path"] == relative(b) for s in read_sub(pull)["streams"]),"repair pattern discovery")
        # No wake stream exists: failed dispatch must not lose pending work.
        with ThreadPoolExecutor(2) as pool:
            futures = [pool.submit(request,"claim","POST",pull+"/claim",dict(worker=f"worker-{n}")) for n in (1,2)]
            claims = [f.result() for f in futures]
        assert sorted(c["status"] for c in claims) == [200,409]
        first_claim = value(next(c for c in claims if c["status"] == 200))
        assert next(s for s in first_claim["streams"] if s["path"] == relative(b))["acked_offset"].endswith("0000000000000000")
        ok(ack(pull,first_claim),200)  # Heartbeat without consumption.
        bad = [dict(stream=relative(a),offset=first_tail),dict(stream=relative(b),offset="0000000000000000_0000000000999999")]
        ok(ack(pull,first_claim,bad),400)
        assert cursor(pull,relative(a)) == initial, "invalid batch partially acked"
        for group in (0,1):
            admin(group,"snapshot")
            lab.wait(lambda: lab.admin(lab.leader(group),group,"metrics").get("snapshot") is not None,"snapshot ready")
        killed = lab.leader(0)
        kill(killed)
        ok(request("claim","POST",pull+"/claim",dict(worker="rival-after-failover")),409)
        ok(ack(pull,first_claim),200)
        ok(request("stream-append","POST",a,b"later-suffix",{"content-type":"application/octet-stream"}),204)
        ok(ack(pull,first_claim,all_acks(first_claim),done=True),200)
        second = claim(pull,"second")
        ok(ack(pull,first_claim,all_acks(second),done=True),409)
        ok(ack(pull,second,operation="release"),204)
        third = claim(pull,"third")
        ok(request("stream-create","PUT",wake,b"",{"content-type":"application/json"}),201)
        lab.wait(lambda: value(ok(request("wake-read","GET",wake+"?offset=-1"),200)),"recover pull notification")
        # Explicit overlay/removal leaves the glob link and its cursor intact.
        ok(request("link","POST",pull+"/streams",dict(streams=[relative(b),relative(manual)])),204)
        assert next(s for s in read_sub(pull)["streams"] if s["path"] == relative(b))["link_type"] == "explicit"
        ok(request("unlink","DELETE",pull+"/streams/"+quote(relative(b),safe="")),204)
        assert next(s for s in read_sub(pull)["streams"] if s["path"] == relative(b))["link_type"] == "glob"
        # An old stream snapshot cannot ack a newly-created stream at that path.
        ok(request("stream-delete","DELETE",a),204)
        ok(request("stream-create","PUT",a,b"new-incarnation-is-longer"),201)
        later_b = ok(request("stream-append","POST",b,b"++",{"content-type":"application/octet-stream"}),204)
        stale_batch = [dict(stream=relative(b),offset=later_b["response_headers"]["stream-next-offset"]),
                       dict(stream=relative(a),offset=first_tail)]
        previous_b = next(s for s in third["streams"] if s["path"] == relative(b))["acked_offset"]
        ok(ack(pull,third,stale_batch),409)
        assert cursor(pull,relative(b)) == previous_b
        ok(ack(pull,third,operation="release"),204)
        fourth = claim(pull,"fourth")
        ok(ack(pull,fourth,all_acks(fourth),done=True),200)
        # Unknown claim reply followed by leader loss. It may hold a lease;
        # eventual expiry, not blind retry success, permits the next worker.
        ok(request("stream-append","POST",a,b"unknown-claim-work",{"content-type":"application/octet-stream"}),204)
        request("claim","POST",pull+"/claim",dict(worker="lost-reply"),lose_response=True)
        lab.start(killed)
        kill(lab.leader(0))
        recovered = lab.wait(lambda: (e if (e := request("claim","POST",pull+"/claim",dict(worker="recovered")))["status"] == 200 else None),
                             "claim after unknown outcome/lease expiry",timeout=20)
        fifth = value(recovered)
        ok(ack(pull,fifth,token=fifth["token"]+"tampered"),401)
        ok(request("sub-delete","DELETE",pull),204)
        ok(ack(pull,fifth,all_acks(fifth),done=True),404)
        ok(request("sub-create","PUT",pull,cfg),201)
        ok(request("stream-append","POST",a,b"after-sub-recreate",{"content-type":"application/octet-stream"}),204)
        sixth = claim(pull,"new-incarnation")
        ok(ack(pull,fifth,all_acks(sixth),done=True),409)
        ok(ack(pull,sixth,all_acks(sixth),done=True),200)

        # Webhook snapshots and signatures, across a different owner group.
        hook = chosen("__ds/subscriptions/hook",1)
        source = chosen("hook-data/source",0)
        ok(request("stream-create","PUT",source,b"prior"),201)
        hook_cfg = dict(type="webhook",streams=[relative(source)],
                        webhook=dict(url=f"http://127.0.0.1:{receiver_port}/snapshot"),lease_ttl_ms=30000)
        policy("/snapshot","hold")
        ok(request("sub-create","PUT",hook,hook_cfg),201)
        ok(request("keys","GET","/r/__ds/jwks.json",node=lab.leader(0)),200)
        cut = ok(request("stream-append","POST",source,b"first",{"content-type":"application/octet-stream"}),204)["response_headers"]["stream-next-offset"]
        one = wait_delivery("/snapshot",1)
        ok(request("stream-append","POST",source,b"unseen-suffix",{"content-type":"application/octet-stream"}),204)
        release_delivery(one,"done")
        two = wait_delivery("/snapshot",2)
        assert cursor(hook,relative(source)) == cut, "auto-done consumed beyond its snapshot"
        release_delivery(two,"async")
        envelope = json.loads(base64.b64decode(two["body"]))
        ok(ack(hook,envelope,all_acks(envelope),done=True,operation="callback"),200)
        old = json.loads(base64.b64decode(one["body"]))
        ok(ack(hook,old,all_acks(old),done=True,operation="callback"),409)
        ok(request("sub-delete","DELETE",hook),204)

        # The external effect occurs, but its response is lost when the leader
        # dies. A retry may duplicate delivery, not cursor consumption.
        lost = chosen("__ds/subscriptions/lost",0)
        lost_cfg = dict(hook_cfg,webhook=dict(url=f"http://127.0.0.1:{receiver_port}/lost"))
        policy("/lost","hold")
        ok(request("sub-create","PUT",lost,lost_cfg),201)
        ok(request("stream-append","POST",source,b"lost-response",{"content-type":"application/octet-stream"}),204)
        one = wait_delivery("/lost",1)
        missing = ({1,2,3}-lab.nodes).pop()
        lab.start(missing)
        kill(lab.leader(0))
        release_delivery(one,"drop")
        two = wait_delivery("/lost",2)
        assert json.loads(base64.b64decode(one["body"]))["generation"] == json.loads(base64.b64decode(two["body"]))["generation"]
        policy("/lost","done")
        release_delivery(two,"done")
        tail = json.loads(base64.b64decode(two["body"]))["streams"][0]["tail_offset"]
        lab.wait(lambda: cursor(lost,relative(source)) == tail,"recovered dispatch completion")
        ok(request("sub-delete","DELETE",lost),204)

        retry = chosen("__ds/subscriptions/retry",0)
        retry_cfg = dict(hook_cfg,webhook=dict(url=f"http://127.0.0.1:{receiver_port}/retry"))
        policy("/retry","fail")
        ok(request("sub-create","PUT",retry,retry_cfg),201)
        ok(request("stream-append","POST",source,b"retry-work",{"content-type":"application/octet-stream"}),204)
        first_retry = wait_delivery("/retry",1)
        second_retry = wait_delivery("/retry",2)
        assert second_retry["start"] - first_retry["start"] >= 0.95
        lab.wait(lambda: read_sub(retry)["status"] == "failed","durable failed-delivery state")
        missing = ({1,2,3}-lab.nodes).pop()
        lab.start(missing)
        kill(lab.leader(0))
        third_retry = wait_delivery("/retry",3)
        assert third_retry["start"] - second_retry["start"] >= 1.95, "restart reset durable retry backoff"
        policy("/retry","done")
        lab.wait(lambda: cursor(retry,relative(source)) == json.loads(base64.b64decode(third_retry["body"]))["streams"][0]["tail_offset"],
                 "retry eventually consumes its issued snapshot",timeout=20)
        ok(request("sub-delete","DELETE",retry),204)

        for i,url in enumerate(["https://10.0.0.1/", "https://169.254.169.254/", "http://8.8.8.8/", "https://[::ffff:127.0.0.1]/"]):
            ok(request("sub-create","PUT",chosen(f"__ds/subscriptions/ssrf{i}",0),dict(hook_cfg,webhook=dict(url=url))),400)
        redirect = chosen("__ds/subscriptions/redirect",0)
        policy("/redirect","redirect")
        ok(request("sub-create","PUT",redirect,dict(hook_cfg,webhook=dict(url=f"http://127.0.0.1:{receiver_port}/redirect"))),201)
        ok(request("stream-append","POST",source,b"redirect-work",{"content-type":"application/octet-stream"}),204)
        wait_delivery("/redirect",1)
        lab.wait(lambda: read_sub(redirect)["status"] == "failed","redirect rejected")
        assert not received("/forbidden")
        # Freeze external work before checking durable cursors/key identity.
        for p in (hook,lost,retry,redirect):
            ok(request("sub-delete","DELETE",p),204)
        before = read_sub(pull)
        keys_before = value(ok(request("keys","GET","/r/__ds/jwks.json",node=lab.leader(0)),200))
        for group in (0,1):
            admin(group,"snapshot")
            lab.wait(lambda: lab.admin(lab.leader(group),group,"metrics").get("snapshot") is not None,"final snapshot")
        live = sorted(lab.nodes)
        for node in live:
            lab.stop(node,crash=True)
        record(dict(op="whole-cluster-SIGKILL",start=time.monotonic()))
        for node in live:
            lab.start(node)
        for group in (0,1):
            lab.wait(lambda: lab.leader(group),"restart leaders")
        assert read_sub(pull) == before
        assert value(ok(request("keys","GET","/r/__ds/jwks.json",node=lab.leader(0)),200)) == keys_before
        ok(request("wake-read","GET",wake+"?offset=-1"),200)
        deliveries = [json.loads(line) for line in (lab.output/"deliveries.jsonl").read_text().splitlines()]
        verdict = check(history,deliveries)
        mutations = []
        for name in ("stale ack accepted","cursor advanced without ack","forged webhook"):
            h, d = copy.deepcopy(history), copy.deepcopy(deliveries)
            if name == "stale ack accepted":
                next(e for e in h if e["op"] == "ack" and e["status"] == 409)["status"] = 200
            elif name == "cursor advanced without ack":
                e = next(e for e in h if e["op"] == "sub-read" and e["status"] == 200)
                v = value(e)
                v["streams"][0]["acked_offset"] = "0000000000000000_0000000000999999"
                e["result"] = base64.b64encode(json.dumps(v).encode()).decode()
            else:
                e = next(e for e in d if e["op"] == "webhook")
                e["body"] = base64.b64encode(base64.b64decode(e["body"])+b" ").decode()
            try:
                check(h,d)
            except AssertionError:
                mutations.append(name)
            else:
                raise AssertionError(f"checker accepted {name}")
        verdict["negative_mutations_rejected"] = mutations
        (lab.output/"verdict.json").write_text(json.dumps(verdict,indent=2)+"\n")
        print(json.dumps(verdict))
    except Exception:
        (lab.output/"failure.txt").write_text(traceback.format_exc())
        raise
    finally:
        lab.close()
        if receiver_started:
            subprocess.run(["amp","orb","service","stop",receiver],check=True,stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    run(sys.argv[1])
