"""Independent checker for the ordered two-owner subscription fault fixture.

Uses client requests, responses and external receiver records only. It does not
import the driver, Rust implementation, storage or expected-result annotations.
Stream mutations are ordered; claim races and external deliveries may overlap.
Unknown outcomes remain unknown. This is not arbitrary-history linearizability.
"""
import base64
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.parse import unquote


def decode(value):
    return base64.b64decode(value, validate=True)


def offset(value):
    if value == "-1":
        return 0
    first, last = value.split("_")
    assert first == "0000000000000000" and len(last) == 16 and last.isdecimal()
    return int(last)


def signature(receipt, keys):
    header = {k.lower(): v for k, v in receipt["headers"].items()}["webhook-signature"]
    fields = dict(part.split("=", 1) for part in header.split(","))
    key = keys[fields["kid"]]
    assert key["kty"] == "OKP" and key["crv"] == "Ed25519" and key["alg"] == "EdDSA"
    assert abs(int(fields["t"]) - receipt["wall"]) < 300, "signature outside replay window"
    raw = lambda s: base64.urlsafe_b64decode(s + "="*((-len(s)) % 4))
    public = raw(key["x"])
    assert len(public) == 32
    with tempfile.TemporaryDirectory() as directory:
        d = Path(directory)
        # RFC 8410 SubjectPublicKeyInfo: Ed25519 OID, 32-byte raw public key.
        (d / "key.der").write_bytes(bytes.fromhex("302a300506032b6570032100") + public)
        (d / "signature").write_bytes(raw(fields["ed25519"]))
        (d / "message").write_bytes(fields["t"].encode()+b"."+decode(receipt["body"]))
        result = subprocess.run(["openssl", "pkeyutl", "-verify", "-pubin", "-keyform", "DER",
                                 "-inkey", str(d / "key.der"), "-rawin", "-in", str(d / "message"),
                                 "-sigfile", str(d / "signature")], capture_output=True)
        assert result.returncode == 0, "Ed25519 signature verification failed"


def check(history, deliveries):
    keys = {}
    for e in history:
        if e["op"] == "keys" and e["status"] == 200:
            for key in json.loads(decode(e["result"]))["keys"]:
                assert "d" not in key, "private signing material exposed"
                assert key["kid"] not in keys or keys[key["kid"]] == key, "key changed after recovery"
                keys[key["kid"]] = key
    receipts = {e["id"]: e for e in deliveries if e["op"] == "webhook"}
    for receipt in receipts.values():
        assert receipt["path"] != "/forbidden", "webhook followed a redirect"
        signature(receipt, keys)

    streams, subs, versions, tokens, envelopes = {}, {}, {}, {}, {}
    checks, unknowns, rejected, claimed, wake_reads = 0, 0, 0, 0, 0

    def matches(sub, p):
        # The fixture deliberately uses this one protocol pattern.
        pattern = sub["config"].get("pattern")
        assert pattern in (None, "events/**")
        return pattern == "events/**" and p.startswith("events/")

    def linked(sub, p, initial=False, explicit=False):
        s = streams.get("/r/"+p, dict(inc=0, size=0, alive=False))
        cursor = s["size"] if initial and s["alive"] else 0
        return dict(inc=s["inc"] if s["alive"] else 0, floor=cursor, ceiling=cursor,
                    explicit=explicit, refresh=False)

    def snapshot(sub, value, exact=True):
        assert {l["path"] for l in value} == set(sub["links"]), "claim lost a link"
        for entry in value:
            p = entry["path"]
            link = sub["links"][p]
            source = streams.get("/r/"+p, dict(size=0, alive=False))
            assert link["floor"] <= offset(entry["acked_offset"]) <= link["ceiling"], "unacknowledged cursor"
            tail = source["size"] if source["alive"] else link["floor"]
            observed = offset(entry["tail_offset"])
            assert observed == tail if exact else observed <= tail, "invalid snapshot tail"
            assert entry["has_pending"] == (observed > offset(entry["acked_offset"]))

    for e in sorted(history + deliveries, key=lambda e: e["start"]):
        op = e["op"]
        if op == "webhook":
            value = json.loads(decode(e["body"]))
            path = "/r/__ds/subscriptions/"+value["subscription_id"]
            sub = subs[path]
            token = value["callback_token"]
            if token not in tokens:
                snapshot(sub, value["streams"], exact=False)
                tokens[token] = dict(path=path, version=sub["version"], value=value,
                                     expires_latest=e["start"]+sub["config"]["lease_ttl_ms"]/1000,
                                     inc={p:l["inc"] for p,l in sub["links"].items()})
            else:
                assert tokens[token]["value"] == value, "retry changed a durable wake snapshot"
            sub["generation"] = max(sub["generation"], value["generation"])
            envelopes[e["id"]] = tokens[token]
            continue
        if op == "webhook_reply":
            issued = envelopes[e["id"]]
            sub = subs.get(issued["path"])
            if e["mode"] == "done" and sub and sub["version"] == issued["version"]:
                for entry in issued["value"]["streams"]:
                    p = entry["path"]
                    link = sub["links"].get(p)
                    if link and link["inc"] == issued["inc"][p]:
                        link["ceiling"] = max(link["ceiling"], offset(entry["tail_offset"]))
            continue
        if "status" not in e:
            continue
        status = e["status"]
        if status in (0, 503):
            unknowns += 1
            continue
        path = e.get("path", "").split("?")[0]
        successful = 200 <= status < 300
        checks += 1
        if op in ("stream-create", "stream-append", "stream-delete") and successful:
            if op == "stream-create":
                previous = streams.get(path, dict(inc=0))
                streams[path] = dict(inc=previous["inc"]+1, size=len(decode(e["body"])), alive=True)
            elif op == "stream-append":
                streams[path]["size"] += len(decode(e["body"]))
            else:
                streams[path]["alive"] = False
            p = path.removeprefix("/r/")
            for sub in subs.values():
                if p not in sub["links"] and matches(sub, p) and op != "stream-delete":
                    sub["links"][p] = linked(sub, p)
                elif p in sub["links"] and op != "stream-append":
                    old = sub["links"][p]
                    sub["links"][p] = linked(sub, p, explicit=old["explicit"])
                    sub["links"][p]["refresh"] = True
            continue
        if op == "sub-create" and status == 201:
            config = json.loads(decode(e["body"]))
            versions[path] = versions.get(path, 0)+1
            sub = dict(config=config, links={}, version=versions[path], held=None, generation=0)
            for full, source in streams.items():
                p = full.removeprefix("/r/")
                if source["alive"] and matches(sub, p):
                    sub["links"][p] = linked(sub, p, initial=True)
            for p in config.get("streams", []):
                sub["links"][p] = linked(sub, p, initial=True, explicit=True)
            subs[path] = sub
            continue
        if op == "sub-delete" and successful:
            subs.pop(path, None)
            continue
        if op == "wake-read" and status == 200:
            values = json.loads(decode(e["result"]))
            identities = [(v["subscription_id"], v["generation"], v["ts"]) for v in values]
            assert len(set(identities)) == len(identities), "duplicate effect of pull-wake dispatch retry"
            assert all(v["type"] == "wake" for v in values)
            wake_reads += 1
            continue
        base = path.split("/streams/")[0] if op == "unlink" else path.rsplit("/",1)[0]
        if op == "sub-read":
            sub = subs.get(path)
            if status == 404:
                assert sub is None, "lost durable subscription"
            elif status == 200:
                assert sub is not None, "deleted subscription resurrected"
                value = json.loads(decode(e["result"]))
                for entry in value["streams"]:
                    link = sub["links"][entry["path"]]
                    if not link["refresh"]:
                        cursor = offset(entry["acked_offset"])
                        assert link["floor"] <= cursor <= link["ceiling"], "cursor lost or consumed unauthorized bytes"
                        link["floor"] = cursor
                        assert entry["link_type"] == ("explicit" if link["explicit"] else "glob")
            continue
        if op not in ("claim", "ack", "callback", "release", "link", "unlink"):
            continue
        sub = subs.get(base)
        if not sub:
            assert not successful, "operation on deleted subscription succeeded"
            rejected += 1
            continue
        for link in sub["links"].values():
            link["refresh"] = False  # These endpoints capture an authoritative catalog.
        body = json.loads(decode(e["body"])) if e["body"] else None
        if op == "claim" and status == 200:
            value = json.loads(decode(e["result"]))
            if sub["held"]:
                assert e["end"] >= sub["held"]["expires_earliest"], "two unexpired successful claims"
            assert value["generation"] > sub["generation"], "wake generation reused"
            snapshot(sub, value["streams"])
            issued = dict(path=base, version=sub["version"], value=value,
                          expires_earliest=e["start"]+value["lease_ttl_ms"]/1000,
                          expires_latest=e["end"]+value["lease_ttl_ms"]/1000,
                          inc={p:l["inc"] for p,l in sub["links"].items()})
            tokens[value["token"]] = issued
            sub["held"] = issued
            sub["generation"] = value["generation"]
            claimed += 1
        elif op in ("ack", "callback", "release"):
            token = e["headers"].get("authorization", "").removeprefix("Bearer ")
            issued = tokens.get(token)
            valid = issued and issued["path"] == base and issued["version"] == sub["version"]
            valid = valid and body["generation"] == issued["value"]["generation"] and body["wake_id"] == issued["value"]["wake_id"]
            valid = valid and body["generation"] == sub["generation"] and e["start"] < issued["expires_latest"]
            if op != "callback":
                valid = valid and sub["held"] is issued
            acks = body.get("acks", [])
            for ack in acks:
                p = ack["stream"]
                link = sub["links"].get(p)
                valid = valid and link and issued["inc"].get(p) == link["inc"]
                valid = valid and offset(ack["offset"]) <= streams["/r/"+p]["size"]
            if successful:
                assert valid, "stale/invalid callback, ack or release accepted"
                for ack in acks:
                    link = sub["links"][ack["stream"]]
                    link["floor"] = max(link["floor"], offset(ack["offset"]))
                    link["ceiling"] = max(link["ceiling"], link["floor"])
                if op == "release" or body.get("done"):
                    sub["held"] = None
                else:
                    issued["expires_earliest"] = e["start"]+sub["config"]["lease_ttl_ms"]/1000
                    issued["expires_latest"] = e["end"]+sub["config"]["lease_ttl_ms"]/1000
            else:
                rejected += 1
        elif op == "link" and successful:
            for p in body["streams"]:
                if p not in sub["links"]:
                    sub["links"][p] = linked(sub,p,initial=True,explicit=True)
                sub["links"][p]["explicit"] = True
        elif op == "unlink" and successful:
            p = unquote(path.split("/streams/",1)[1])
            if matches(sub,p):
                sub["links"][p]["explicit"] = False
            else:
                sub["links"].pop(p,None)
    assert claimed >= 5 and len(receipts) >= 5 and wake_reads, "incomplete fixture"
    return dict(verdict="PASS", operations=checks, unknown_outcomes=unknowns,
                successful_claims=claimed, rejected_workers=rejected,
                signatures_verified=len(receipts), wake_reads=wake_reads,
                scope="ordered client fixture: cursor authorization, incarnation/generation fencing, claim exclusion, independent Ed25519 and dispatch dedup")


if __name__ == "__main__":
    print(json.dumps(check([json.loads(s) for s in open(sys.argv[1])],
                           [json.loads(s) for s in open(sys.argv[2])]), indent=2))
