"""Independent checker for the ordered cold campaign (not general linearizability).

Reconstruct payloads/forks/retries from HTTP invocations, never byte-probe expected
fields. Check observed participant/pin floors, immutable retirement, real S3 GETs,
and deletion eligibility against complete inventory bytes observed by the proxy.
Unknown HTTP outcomes remain unknown. Prefix reads may return older committed
versions, including a removed replica's pre-delete view. Model checks and WAL
tests, not this sampled history, establish correctness between observations.
"""
import base64
import copy
import hashlib
import json
from pathlib import Path
import sys


def sha(data):
    return hashlib.sha256(data).hexdigest()


def check(history, objects):
    resources, producers = {}, set()
    reads = retries = unknown = 0
    for event in sorted((e for e in history if e["op"] == "http"), key=lambda e:e["start"]):
        status, method = event["status"], event["method"]
        path = event["path"].split("?")[0]
        body = base64.b64decode(event["input_b64"],validate=True)
        assert sha(body) == event["input_sha256"] and len(body) == event["input_bytes"], "invocation evidence corrupt"
        headers = event["request_headers"]
        if status == 0:
            unknown += 1
            continue
        if method == "PUT" and status == 201:
            source = resources.get(headers.get("stream-forked-from"))
            is_json = headers.get("content-type", "application/json" if source and source[0] else "") == "application/json"
            data = json.loads(body) if is_json else body
            if source:
                assert headers.get("stream-fork-offset") == "-1", "unsupported fixture fork offset"
                data = source[1][-1][:int(headers["stream-fork-sub-offset"])] + data
            resources[path] = (is_json, [data])
        elif method == "POST" and status in (200,204):
            is_json, versions = resources[path]
            producer = (path,headers.get("producer-id"),headers.get("producer-epoch"),headers.get("producer-seq"))
            if producer[1] is not None and producer in producers:
                retries += 1
            else:
                versions.append(versions[-1] + (json.loads(body) if is_json else body))
                producers.add(producer)
        elif method == "GET" and status == 200:
            is_json, versions = resources[path]
            wire = [json.dumps(v,separators=(",",":")).encode() if is_json else v for v in versions]
            assert any(event["bytes"] <= len(v) and sha(v[:event["bytes"]]) == event["sha256"] for v in wire), \
                "read differs from committed client prefix (including fork/retry semantics)"
            reads += 1
        # DELETE and matching PUT do not erase historical committed prefixes.
    assert reads and retries and unknown, "missing read/retry/unknown-outcome coverage"

    feeds = [e for e in history if e["op"] == "cold-sse"]
    assert feeds, "missing cold live-feed coverage"
    for feed in feeds:
        wire = base64.b64decode(feed["wire_b64"],validate=True)
        expected = resources[feed["path"]][1][-1]
        initial = resources[feed["path"]][1][0]
        data, closed = bytearray(), False
        for event in wire.split(b"\n\n"):
            lines = event.splitlines()
            value = b"\n".join(line[5:].lstrip() for line in lines if line.startswith(b"data:"))
            if b"event: data" in lines:
                chunk = base64.b64decode(value,validate=True)
                assert len(chunk) <= 64*1024, "cold binary SSE catch-up exceeded its frame bound"
                assert not closed, "SSE data after closure"
                data.extend(chunk)
            elif b"event: control" in lines:
                control = json.loads(value)
                assert int(control["streamNextOffset"].split("_")[1]) == len(data), "SSE advertised a skipped offset"
                if control.get("upToDate",False):
                    assert len(data) in (len(initial),len(expected)), "SSE advertised a partial catch-up as complete"
                if control.get("streamClosed",False):
                    assert bytes(data) == expected, "SSE closed before its complete committed prefix"
                    closed = True
        assert not resources[feed["path"]][0] and closed
        assert bytes(data) == expected, "cold SSE differs from exact committed client bytes"
        assert any(e["op"] == "s3" and e.get("method") == "GET" and "/p/" in e["path"]
                   and e["status"] in (200,206) and e["node"] == feed["node"]
                   and feed["start"] <= e["time_ns"] <= feed["catchup_end"] for e in objects), \
            "SSE catch-up has no real cold GET observation"

    previous, retired = {}, {}
    for event in (e for e in history if e["op"] == "state"):
        key, state = (event["node"],event["group"]), event["state"]
        old = previous.get(key)
        assert len(state["pending"]) <= 64 and len(state["participants"]) <= 64, "metadata admission bound exceeded"
        assert state["floor"] <= min(state["participants"].values()), "GC floor exceeds participant certificate"
        if state["oldest_pin"] is not None:
            assert state["certificate"]["floor"] <= state["oldest_pin"], "certificate bypassed a live pin"
        assert not state["objects_truncated"], "campaign too large for complete sampled state evidence"
        known = {o["birth"] for o in state["objects"]} | {int(b) for b in state["pending"]}
        if state["inventory"]:
            known.add(state["inventory"]["birth"])
        if old:
            assert state["floor"] >= old[0]["floor"], "inventory floor regressed"
            assert state["certificate"]["floor"] >= old[0]["certificate"]["floor"], "restart certificate regressed"
            assert old[0]["participants"].keys() <= state["participants"].keys(), "membership forgot a storage participant"
            retired.setdefault(key,set()).update(old[1] - known)
            assert not known.intersection(retired[key]), "retired Begin identity reattached"
        previous[key] = state, known

    inventories, deletes, gets = {}, [], []
    for event in sorted(objects,key=lambda e:e["time_ns"]):
        if event["op"] != "s3":
            continue
        key = event["path"].split("/",2)[-1]
        fields = key.split("/")
        if len(fields) != 9 or fields[0] != "electric":
            continue
        prefix, birth = "/".join(fields[:3]), int(fields[3])
        if "inventory" in event:
            inventory = event["inventory"]
            encoded = b"ERIC0001" + inventory["cut"].to_bytes(8,"big") + b"".join(
                r["birth"].to_bytes(8,"big") + bytes.fromhex(r["key_sha256"]) for r in inventory["records"])
            assert len(encoded) == int(fields[7]) and sha(encoded) == fields[8] == event["sha256"], "inventory evidence checksum mismatch"
            births = [r["birth"] for r in inventory["records"]]
            assert births == sorted(set(births)) and all(b <= inventory["cut"] for b in births), "inventory cut/order violation"
            assert birth > inventory["cut"], "inventory Begin must follow its exact cut"
            if not event["fault"]:
                inventories.setdefault(prefix, {})[birth] = inventory
        if event["method"] == "GET" and fields[4] == "p" and event["status"] in (200,206):
            gets.append(event)
        if event["method"] == "DELETE" and event["status"] == 204:
            assert any(birth <= i["cut"] and sha(key.encode()) not in {r["key_sha256"] for r in i["records"]}
                       for i in inventories.get(prefix,{}).values()), "delete without observed inventory authority"
            deletes.append(event)
    assert gets and deletes and inventories, "missing real object read/collection evidence"

    # Cross-check deliberate fencing windows against actual object-server IO,
    # independently of the driver's state assertions or expected byte hashes.
    by_op = {}
    for event in history:
        by_op.setdefault(event["op"], []).append(event)
    for begin, end in (("reader-fence","retired-reader-complete"), ("transfer-fence","transfer-complete"),
                       ("removed-fence","removed-recovered")):
        start, finish = by_op[begin][0], by_op[end][0]
        # End is an observation after drainage, not its precise instant: only
        # the fence instant is guaranteed protected. The held reader's request
        # start supplies the wider, independently observed reader interval.
        low = start["time_ns"]
        if begin == "reader-fence":
            low = max(e["start"] for e in history if e["op"] == "http" and e["method"] == "GET"
                      and e["start"] < low < e["end"])
        assert not any(low <= d["time_ns"] <= start["time_ns"] and
                       d["path"].split("/",2)[-1] in start["keys"] for d in deletes), "premature deletion in protected interval"
        assert finish["time_ns"] > start["time_ns"]
    transfer = by_op["transfer-complete"][0]
    assert any(g["node"] == transfer["node"] and g["path"].split("/",2)[-1] in transfer["keys"] for g in gets), \
        "learner has no observed remote replay reads"
    generations = by_op["generation-bound"][0]
    created = [e for e in by_op["http"] if e["method"] == "PUT" and e["status"] == 201
               and e["path"].startswith("/generation-") and e["end"] < generations["time_ns"]]
    paths = {e["path"] for e in created}
    assert len(paths) == generations["generations"] >= 12
    assert paths <= {e["path"] for e in by_op["http"] if e["method"] == "DELETE" and e["status"] == 204
                     and e["end"] < generations["time_ns"]}, "generation deletion coverage incomplete"
    # Bound growth relative to the pre-cycle live set, not the unrelated count
    # of generations: enlarging a surviving stream legitimately adds segments.
    before = max((e["state"] for e in by_op["state"] if e["group"] == 0
                  and e["time_ns"] < min(c["start"] for c in created)), key=lambda s:s["applied"])
    after = generations["state"]
    assert after["live_objects"] == before["live_objects"] and \
        {o["key"] for o in after["objects"]} == {o["key"] for o in before["objects"]}, \
        "retired generations changed the baseline live metadata set"
    assert not any(o["kind"].get("Payload",{}).get("path") in paths for o in after["pending"].values()), \
        "retired generation left pending payload metadata"
    assert {e["node"] for e in by_op["hot-reclaimed"]} == {1,2,3}
    assert all(e["files"] and all(f["size"] == 0 for f in e["files"]) for e in by_op["hot-reclaimed"]), \
        "remote-read campaign did not reclaim actual native hot files"
    quorum = by_op["quorum-publication-fence"][0]
    assert str(quorum["birth"]) in quorum["state"]["pending"] and quorum["birth"] not in {o["birth"] for o in quorum["state"]["objects"]}, \
        "minority uploader published without quorum"
    assert {e["kind"] for e in by_op["storage-fail-stop"]} == {"certificate","snapshot"}
    assert by_op["remote-apply-fail-stop"] and by_op["storage-recovered"]
    bulk, collected = by_op["bulk-live"][0], by_op["bulk-reclaimed"][0]
    assert bulk["objects"] == collected["objects"] == len(bulk["keys"]) == 160
    assert collected["live_objects"] <= bulk["baseline_live"], "bulk retirement retained live metadata"
    deleted = {e["path"].split("/",2)[-1] for e in deletes if e["time_ns"] >= bulk["time_ns"]}
    assert set(bulk["keys"]) <= deleted, "bulk reclamation has no actual S3 DELETE evidence"
    crashed, reclaimed = by_op["inventory-upload-crash"][0], by_op["late-inventory-reclaimed"][0]
    assert crashed["key"] == reclaimed["key"] and crashed["time_ns"] < reclaimed["time_ns"]
    late = next((e for e in objects if e["op"] == "late-inventory" and e["path"].endswith(crashed["key"])),None)
    assert late and crashed["time_ns"] < late["time_ns"] < reclaimed["time_ns"], "no real late inventory completion across crash"
    assert any(e["path"].endswith(crashed["key"]) and late["time_ns"] <= e["time_ns"] <= reclaimed["time_ns"]
               for e in deletes), "late inventory completion was not subsequently deleted"
    assert any(e.get("list_after") for e in objects), "collector never traversed a noninitial page"
    assert max(len(i["records"]) for group in inventories.values() for i in group.values()) > 128
    assert by_op["genesis-bound-rejected"][0]["participants"] == 65
    return dict(verdict="PASS",reads=reads,sse_reads=len(feeds),retries=retries,unknown_http_outcomes=unknown,
                observed_object_gets=len(gets),observed_deletes=len(deletes),
                observed_inventories=sum(map(len,inventories.values())),state_observations=sum(e["op"]=="state" for e in history))


def main(directory):
    directory = Path(directory)
    history = [json.loads(line) for line in (directory/"history.jsonl").read_text().splitlines()]
    objects = [json.loads(line) for p in directory.glob("objects-*.jsonl") for line in p.read_text().splitlines()]
    result = check(history,objects)
    controls = []
    for name in ("wrong-read-prefix", "lost-participant", "inventory-corruption", "delete-held-reader", "miss-late-inventory", "duplicate-sse", "premature-sse-offset", "retained-generation"):
        events, remote = copy.deepcopy(history), copy.deepcopy(objects)
        if name == "wrong-read-prefix":
            next(e for e in events if e["op"] == "http" and e["method"] == "GET" and e["status"] == 200)["sha256"] = "00"*32
        elif name == "lost-participant":
            state = next(e["state"] for e in reversed(events) if e["op"] == "state" and len(e["state"]["participants"]) == 4)
            state["participants"].pop("4")
        elif name == "inventory-corruption":
            next(e for e in remote if "inventory" in e)["inventory"]["cut"] += 1
        elif name == "miss-late-inventory":
            key = next(e["key"] for e in events if e["op"] == "inventory-upload-crash")
            remote = [e for e in remote if not (e.get("method") == "DELETE" and e["path"].endswith(key))]
        elif name == "duplicate-sse":
            feed = next(e for e in events if e["op"] == "cold-sse")
            feed["wire_b64"] = base64.b64encode(base64.b64decode(feed["wire_b64"])*2).decode()
        elif name == "premature-sse-offset":
            feed = next(e for e in events if e["op"] == "cold-sse")
            wire = base64.b64decode(feed["wire_b64"])
            start = wire.index(b'data:{"streamNextOffset":') + len(b"data:")
            end = wire.index(b"\n",start)
            control = json.loads(wire[start:end])
            control["streamNextOffset"] = "0000000000000000_9999999999999999"
            feed["wire_b64"] = base64.b64encode(wire[:start]+json.dumps(control).encode()+wire[end:]).decode()
        elif name == "retained-generation":
            state = next(e["state"] for e in events if e["op"] == "generation-bound")
            state["live_objects"] += 1
        else:
            fence = next(e for e in events if e["op"] == "reader-fence")
            remote.append(dict(op="s3",method="DELETE",status=204,fault="",node=fence["node"],
                               time_ns=fence["time_ns"],path="/cold-fixture/"+fence["keys"][0]))
        try:
            check(events,remote)
        except AssertionError as error:
            controls.append(dict(mutation=name,detected=str(error)))
        else:
            raise AssertionError(f"negative control survived: {name}")
    result.update(negative_controls=controls, checker_sha256=sha(Path(__file__).read_bytes()))
    (directory/"checker.json").write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main(sys.argv[1])
