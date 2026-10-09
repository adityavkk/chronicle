"""Three-process, two-owner fault history on real native WAL/wire files.

Explicit import pause is a scheduling fault hook, not a packet partition. SIGKILL
and whole-process restart are real; this single-host run proves no independent
disk, power-loss or AZ guarantee. All HTTP unknowns and administrative events stay
in history.jsonl. The checker imports neither this driver nor the server.
"""
import base64
import hashlib
import json
import sys
import time
import traceback

from lab import Lab, BINARY, EXPERIMENT, partition, source_hashes
from check_forks import check


def run(output):
    lab = Lab(output, partitions=2, port=19500)
    history = []
    provenance = dict(binary=hashlib.sha256(BINARY.read_bytes()).hexdigest(), partitions=2,
                      processes=3, maximum_processes=5, storage="native WAL + wire files",
                      faults="import pause, SIGKILL, snapshot learners, full restarts, delayed retired grants, 320 retirement cycles, source-only owner outage with destination retries",
                      sources=source_hashes())
    (lab.output / "provenance.json").write_text(json.dumps(provenance, indent=2)+"\n")

    def record(event):
        history.append(event)
        with open(lab.output / "history.jsonl", "a") as file:
            file.write(json.dumps(event)+"\n")

    def request(op, method, path, body=b"", headers=None, node=None, **metadata):
        group = partition(path.split("?")[0], 2)
        if node is None:
            node = lab.wait(lambda: lab.leader(group), "find request owner")
        event = dict(op=op, path=path, node=node, method=method, body=base64.b64encode(body).decode(),
                     headers=headers or {}, start=time.monotonic(), **metadata)
        try:
            status, response_headers, data = lab.request(node, method, path, body, headers)
            event.update(status=status, response_headers=response_headers, result=base64.b64encode(data).decode())
        except (OSError, TimeoutError) as error:
            event.update(status=0, result="", error=str(error), outcome="unknown")
        event["end"] = time.monotonic()
        record(event)
        return event

    def read(path, mode="linearizable", node=None):
        return request("read", "GET", path+"?offset=-1", headers={"stream-consistency": mode}, node=node, mode=mode)

    def reput(path, headers):
        return request("reput", "PUT", path, b"ignored-on-retry", headers)

    def control(node, group, action, value=None):
        return request("inspect" if action == "fork-state" else "admin", "POST", f"/_admin/{group}/{action}",
                       json.dumps(value).encode(), {"x-electric-cluster": lab.cluster}, node)

    def settled(group, destinations=0, decisions=0, node=None):
        def ready():
            event = control(node or lab.leader(group), group, "fork-stats")
            assert event["status"] == 200
            stats = json.loads(base64.b64decode(event["result"]))["forks"]
            return stats if stats["destinations"] == destinations and stats["decisions"] == decisions else None
        return lab.wait(ready, "terminal metadata reclaimed", timeout=45)

    def retention(group, node):
        return request("retention", "POST", f"/_admin/{group}/fork-stats",
                       b"null", {"x-electric-cluster": lab.cluster}, node)

    def pause(node, enabled):
        event = request("fault", "POST", "/_admin/network",
                        json.dumps(dict(blocked=[], delay_ms=0, pause_fork_import=enabled)).encode(),
                        {"x-electric-cluster": lab.cluster}, node)
        assert event["status"] == 200

    def kill(node):
        start = time.monotonic()
        lab.stop(node, crash=True)
        record(dict(op="SIGKILL", node=node, start=start, end=time.monotonic()))

    def path(prefix, group):
        return next(f"/r/{prefix}-{i}" for i in range(100) if partition(f"/r/{prefix}-{i}",2) == group)

    source, child, descendant = path("source",0), path("child",1), path("descendant",0)
    data = bytes((i*31+17) % 256 for i in range(190003))
    cut = 170011
    fork_headers = {"stream-forked-from":source,"stream-fork-offset":f"0000000000000000_{cut:016d}"}
    try:
        for node in (1,2,3):
            lab.start(node)
            pause(node, True)
        lab.initialize()
        assert lab.leader(0) != lab.leader(1), "campaign requires different owner leaders"
        assert request("create","PUT",source,data,{"stream-ttl":"3600"})["status"] == 201
        fillers = [path("snapshot-filler",group) for group in (0,1)]
        for filler in fillers:
            assert request("create","PUT",filler,b"initial")["status"] == 201
            for i in range(350):
                assert request("append","POST",filler,i.to_bytes(4,"big"),
                               {"content-type":"application/octet-stream"})["status"] == 204
        probe = control(lab.leader(0), 0, "fork", {"Probe":{"headers":list(fork_headers.items())}})
        probe_reply = json.loads(base64.b64decode(probe["result"]))
        assert probe_reply["status"] == 200
        source_id, source_config = json.loads(bytes(probe_reply["body"]))
        unknown = request("fork","PUT",child,b"child-initial",fork_headers,source=source,cut=cut)
        assert unknown["status"] == 503, unknown

        def granted():
            result = control(lab.leader(1),1,"fork-state")
            transactions = json.loads(base64.b64decode(result["result"]))["transactions"]
            return next((t for t in transactions if t["path"] == child and t["granted"]), None)

        transaction = lab.wait(granted, "source grant recorded at destination")
        assert transaction["created"] is None and transaction["imported"] == 0

        def stale_grant():
            call = {"Apply":{"path":source,"action":{"Grant":{"tx":transaction["tx"],
                    "source_id":source_id,"headers":list(fork_headers.items()),"expected":source_config}}}}
            return request("retired-grant", "POST", "/_admin/0/fork", json.dumps(call).encode(),
                           {"x-electric-cluster":lab.cluster}, lab.leader(0))

        for mode in ("linearizable", "prefix"):
            assert read(child,mode)["status"] == 503
        assert request("delete","DELETE",source)["status"] == 204
        assert read(source)["status"] == 410
        kill(lab.leader(1))
        lab.wait(lambda: lab.leader(1), "destination failover")
        assert read(child)["status"] == 503
        # Snapshot both a retained source and a still-hidden destination. A
        # learner must acquire the control state AND exact wire-file prefix.
        for group in (0,1):
            leader = lab.leader(group)
            assert control(leader,group,"snapshot")["status"] == 200
            lab.wait(lambda: lab.admin(leader,group,"metrics").get("snapshot") is not None, "snapshot ready")
        lab.start(4)
        pause(4, True)
        for group in (0,1):
            leader = lab.leader(group)
            value = [4,{"addr":f"127.0.0.1:{lab.port+4}"}]
            reply = control(leader,group,"learner",value)
            assert reply["status"] == 200 and "Ok" in json.loads(base64.b64decode(reply["result"]))
            lab.wait(lambda: lab.admin(4,group,"metrics").get("snapshot") is not None,
                     "learner actually installed snapshot, not just log replay")
        assert read(child,"prefix",4)["status"] == 503
        for node in sorted(lab.nodes):
            pause(node,False)
        lab.wait(lambda: read(child)["status"] == 200, "resume native prefix import")
        assert reput(child,fork_headers)["status"] == 200
        equivalent = {**fork_headers,"stream-ttl":"3600",
                      "content-type":"APPLICATION/OCTET-STREAM; charset=binary","stream-fork-sub-offset":"0"}
        assert reput(child,equivalent)["status"] == 200
        assert reput(child,{**fork_headers,"stream-ttl":"7200"})["status"] == 409
        assert read(child)["status"] == 200
        # A second remote fork crosses back to the original source owner.
        child_tail = cut + len(b"child-initial")
        grand_headers = {"stream-forked-from":child,"stream-fork-offset":f"0000000000000000_{child_tail:016d}"}
        assert request("fork","PUT",descendant,b"grandchild",grand_headers,source=child,cut=child_tail)["status"] == 201
        assert request("delete","DELETE",child)["status"] == 204
        assert read(child)["status"] == 410
        assert read(source)["status"] == 410
        assert read(descendant)["status"] == 200
        for group in (0,1):
            leader = lab.leader(group)
            members = sorted(lab.nodes)
            reply = control(leader,group,"membership",members)
            assert reply["status"] == 200 and "Ok" in json.loads(base64.b64decode(reply["result"]))
        # Stop every node after acknowledged descendants/soft deletes; no hot
        # file can be treated as authoritative during recovery.
        live = sorted(lab.nodes)
        for node in live:
            kill(node)
        for node in live:
            lab.start(node)
        for group in (0,1):
            lab.wait(lambda: lab.leader(group), "whole-cluster restart leader")
        for filler in fillers:
            assert read(filler)["status"] == 200
        assert read(descendant)["status"] == 200
        assert read(source)["status"] == 410
        assert request("delete","DELETE",descendant)["status"] == 204
        lab.wait(lambda: read(child)["status"] == 404, "collect retained child")
        lab.wait(lambda: read(source)["status"] == 404, "cascade remote source release")
        assert request("create","PUT",source,b"new-incarnation",{"stream-ttl":"3600"})["status"] == 201
        # Delayed duplicate releases are transaction-fenced, not path-based.
        stale = {"Apply":{"path":source,"action":{"Release":{"tx":transaction["tx"]}}}}
        assert control(lab.leader(0),0,"fork",stale)["status"] == 200
        assert read(source)["status"] == 200
        for group in (0,1):
            settled(group)
            retention(group, lab.leader(group))
        stale_grant()

        # A long-lived low ID must not hold all later terminal tombstones. Keep
        # one native child, churn more than the reply-cache bound, then require
        # exact reclamation while preserving that child's source reference.
        anchor = path("anchor",1)
        headers = {"stream-forked-from":source,"stream-ttl":"7200"}
        assert request("fork","PUT",anchor,b"anchor",headers,source=source,cut=15)["status"] == 201
        for i in range(320):
            temporary = path(f"retirement-{i}",1)
            assert request("fork","PUT",temporary,i.to_bytes(4,"big"),headers,source=source,cut=15)["status"] == 201
            assert read(temporary)["status"] == 200
            assert request("delete","DELETE",temporary)["status"] == 204
        settled(0, decisions=1)
        held = settled(1, destinations=1)
        assert held["results"] == 256
        assert request("delete","DELETE",source)["status"] == 204
        assert read(source)["status"] == 410
        assert read(anchor)["status"] == 200
        assert reput(anchor,headers)["status"] == 200
        assert reput(anchor,{"stream-forked-from":source})["status"] == 409
        # Move only the source group to its existing caught-up learner, then
        # crash its sole voter. The destination keeps a three-voter membership.
        # This changes the fixture's source durability class, not the default.
        reply = control(lab.leader(0),0,"membership",[4])
        assert reply["status"] == 200 and "Ok" in json.loads(base64.b64decode(reply["result"]))
        lab.wait(lambda: lab.leader(0) == 4, "source-only placement")
        kill(4)
        leader = lab.wait(lambda: lab.leader(1), "destination quorum without source")
        assert read(source,node=leader)["status"] == 503
        assert reput(anchor,headers)["status"] == 200
        assert reput(anchor,{"stream-forked-from":source})["status"] == 409
        cut_index = lab.admin(leader,1,"metrics")["last_applied"]["index"]
        assert control(leader,1,"snapshot")["status"] == 200
        lab.wait(lambda: (lab.admin(leader,1,"metrics").get("snapshot") or {}).get("index",-1) >= cut_index,
                 "original source defaults in durable destination snapshot")
        kill(leader)
        lab.start(leader)
        lab.wait(lambda: lab.leader(1), "destination restart while source unavailable")
        assert reput(anchor,{**headers,"content-type":"APPLICATION/OCTET-STREAM; charset=binary"})["status"] == 200
        assert reput(anchor,{"stream-forked-from":source})["status"] == 409
        assert read(anchor)["status"] == 200
        assert request("delete","DELETE",anchor)["status"] == 204
        assert reput(anchor,headers)["status"] == 503
        assert read(anchor)["status"] == 404
        lab.start(4)
        lab.wait(lambda: lab.leader(0) == 4, "source owner restored")
        lab.wait(lambda: read(source)["status"] == 404, "last live hole releases source")
        for group in (0,1):
            settled(group)
            retention(group, lab.leader(group))
        stale_grant()

        # Install snapshots containing the compacted fences into a fresh learner,
        # not just a snapshot taken before compaction plus subsequent log replay.
        for group in (0,1):
            leader = lab.leader(group)
            cut_index = lab.admin(leader,group,"metrics")["last_applied"]["index"]
            assert control(leader,group,"snapshot")["status"] == 200
            lab.wait(lambda: (lab.admin(leader,group,"metrics").get("snapshot") or {}).get("index",-1) >= cut_index,
                     "fences included in durable snapshot")
        lab.start(5)
        for group in (0,1):
            reply = control(lab.leader(group),group,"learner",[5,{"addr":f"127.0.0.1:{lab.port+5}"}])
            assert reply["status"] == 200 and "Ok" in json.loads(base64.b64decode(reply["result"]))
            lab.wait(lambda: lab.admin(5,group,"metrics").get("snapshot") is not None,
                     "new learner installed fence snapshot")
            settled(group,node=5)
            retention(group,5)
        live = sorted(lab.nodes)
        for node in live:
            kill(node)
        for node in live:
            lab.start(node)
        for group in (0,1):
            lab.wait(lambda: lab.leader(group), "restart with compacted fences")
            settled(group)
            retention(group,lab.leader(group))
        stale_grant()
        assert request("create","PUT",source,b"third-incarnation")["status"] == 201
        stale_grant()
        assert read(source)["status"] == 200
        verdict = check(history)
        # The independent checker must reject plausible broken implementations.
        import copy
        bad = copy.deepcopy(history)
        next(e for e in bad if e["op"] == "read" and e["path"].startswith(descendant) and e["status"] == 200)["result"] = base64.b64encode(b"truncated").decode()
        try:
            check(bad)
        except AssertionError:
            pass
        else:
            raise AssertionError("checker accepted truncated fork")
        bad = copy.deepcopy(history)
        next(e for e in bad if e["op"] == "read" and e["path"].startswith(child) and e["status"] == 503)["status"] = 404
        try:
            check(bad)
        except AssertionError:
            pass
        else:
            raise AssertionError("checker accepted false absence")
        bad = copy.deepcopy(history)
        event = next(e for e in bad if e["op"] == "retired-grant")
        reply = json.loads(base64.b64decode(event["result"]))
        reply["status"] = 200
        event["result"] = base64.b64encode(json.dumps(reply).encode()).decode()
        try:
            check(bad)
        except AssertionError:
            pass
        else:
            raise AssertionError("checker accepted reopened terminal grant")
        bad = copy.deepcopy(history)
        event = next(e for e in bad if e["op"] == "retention")
        stats = json.loads(base64.b64decode(event["result"]))
        stats["forks"]["decisions"] = 1
        event["result"] = base64.b64encode(json.dumps(stats).encode()).decode()
        try:
            check(bad)
        except AssertionError:
            pass
        else:
            raise AssertionError("checker accepted unreclaimed terminal records")
        for original, changed in ((200,409),(409,200)):
            bad = copy.deepcopy(history)
            next(e for e in bad if e["op"] == "reput" and e["status"] == original)["status"] = changed
            try:
                check(bad)
            except AssertionError:
                pass
            else:
                raise AssertionError("checker accepted wrong existing-fork configuration result")
        verdict["negative_mutations_rejected"] = ["truncated fork bytes", "false absence after grant",
                                                  "reopened terminal grant", "unreclaimed terminal records",
                                                  "rejected matching fork", "accepted conflicting fork defaults"]
        (lab.output / "verdict.json").write_text(json.dumps(verdict,indent=2)+"\n")
        print(json.dumps(verdict))
    except Exception:
        (lab.output / "failure.txt").write_text(traceback.format_exc())
        raise
    finally:
        lab.close()


if __name__ == "__main__":
    run(sys.argv[1])
