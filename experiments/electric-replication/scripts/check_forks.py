"""Independent checker for ordered binary fork/lifecycle fixture histories.

Reconstruct bytes from client invocations, not storage or the driver's expected
values. Unknown fork outcomes may take effect. A recorded grant forbids a false
404 while materialization is pending. Native descendant retention forbids early
parent collection. This is not a general concurrent lifecycle linearizability
checker; the driver deliberately orders source mutations around an explicit cut.
"""
import base64
import json
import sys


def decode(value):
    return base64.b64decode(value, validate=True)


def configuration(headers, source=None):
    """Wire rules, derived from requests rather than server descriptors."""
    content_type = headers.get("content-type", source[0] if source else "application/octet-stream")
    ttl = int(headers["stream-ttl"]) if "stream-ttl" in headers else None
    expiry = headers.get("stream-expires-at")
    if ttl is None and expiry is None and source:
        ttl, expiry = source[1:3]
    return (content_type.split(";")[0].strip().lower(), ttl, expiry,
            headers.get("stream-forked-from"), headers.get("stream-fork-offset"),
            int(headers.get("stream-fork-sub-offset", 0)), headers.get("stream-closed") == "true")


def check(events):
    resources = {}
    transactions = {}
    fork_resources = []
    checked = 0
    forks = 0
    pending_reads = 0
    retired_grants = 0
    reclamation_observations = 0
    reconfirmations = 0

    def retained(resource):
        return any(child["granted"] and (not child["deleted"] or retained(child))
                   for child in resource["children"])

    for event in sorted(events, key=lambda e: e["start"]):
        op, status = event["op"], event.get("status", 0)
        if op == "inspect" and status == 200:
            for transaction in json.loads(decode(event["result"]))["transactions"]:
                if transaction["path"] in resources and transaction["granted"]:
                    resources[transaction["path"]]["granted"] = True
                    transactions.setdefault(transaction["tx"], resources[transaction["path"]])
            continue
        if op == "retired-grant":
            call = json.loads(decode(event["body"]))["Apply"]["action"]["Grant"]
            resource = transactions[call["tx"]]
            assert resource["deleted"] and not retained(resource), "grant tested before retirement"
            assert status == 200 and json.loads(decode(event["result"]))["status"] == 410, \
                "compacted transaction reacquired a grant or forgot its terminal fence"
            retired_grants += 1
            continue
        if op == "retention":
            assert all(r["deleted"] and not retained(r) for r in fork_resources), "premature quiescence claim"
            assert status == 200
            stats = json.loads(decode(event["result"]))["forks"]
            assert stats["schema"] == 1 and stats["transaction_limit"] == 4096 and stats["result_limit"] == 256
            assert all(stats[k] == 0 for k in ("destinations", "decisions", "pending", "mirrors",
                                              "terminal_destinations", "terminal_decisions")), "unreclaimed fork records"
            assert stats["results"] <= 256 and stats["retired_ranges"] <= 1, "history-sized terminal state"
            reclamation_observations += 1
            continue
        if op not in ("create", "append", "fork", "delete", "read", "reput"):
            continue
        checked += 1
        path = event["path"].split("?")[0]
        resource = resources.get(path)
        if op == "create" and status == 201:
            assert not resource or (resource["deleted"] and not retained(resource)), "recreated retained path"
            resources[path] = dict(data=decode(event["body"]), deleted=False, children=[], granted=True,
                                   config=configuration(event["headers"]), source_config=None)
        elif op == "append" and status in (200, 204):
            assert resource and not resource["deleted"], "append to absent/deleted stream"
            resource["data"] += decode(event["body"])
        elif op == "fork" and status in (0, 200, 201, 503):
            source = resources[event["source"]]
            cut = event["cut"]
            assert 0 <= cut <= len(source["data"]), "fork cut exceeds source"
            if resource:
                assert resource["data"].startswith(source["data"][:cut]), "retry changed inherited prefix"
            else:
                resource = dict(data=source["data"][:cut] + decode(event["body"]),
                                deleted=False, children=[], granted=status in (200, 201),
                                config=configuration(event["headers"], source["config"]),
                                source_config=source["config"])
                resources[path] = resource
                source["children"].append(resource)
                fork_resources.append(resource)
                forks += 1
        elif op == "reput":
            assert resource, "reconfirmation fixture has no prior incarnation"
            if resource["deleted"]:
                assert status not in (200, 201), "reconfirmation resurrected deleted child"
            else:
                matching = configuration(event["headers"], resource["source_config"]) == resource["config"]
                assert status == (200 if matching else 409), "existing child configuration/availability mismatch"
                if matching:
                    assert not decode(event["result"]), "reconfirmation must not return initial data"
                    offset = event["response_headers"]["stream-next-offset"].split("_")[-1]
                    assert int(offset) == len(resource["data"]), "retry changed child tail"
            reconfirmations += 1
        elif op == "delete" and status == 204:
            assert resource and not resource["deleted"], "successful delete of absent stream"
            resource["deleted"] = True
        elif op == "read":
            if status == 200:
                assert resource and not resource["deleted"], "resurrected or phantom resource"
                observed = decode(event["result"])
                if event.get("mode", "linearizable") == "prefix":
                    assert resource["data"].startswith(observed), "incompatible committed prefix"
                else:
                    assert observed == resource["data"], "fork bytes differ from granted prefix + child appends"
                resource["granted"] = True
            elif status == 404 and resource:
                assert not retained(resource), "parent collected with living descendant"
                assert resource["deleted"] or not resource["granted"], "false absence after durable grant"
            elif status == 410:
                assert resource and resource["deleted"], "unexpected soft deletion"
            elif status == 503:
                pending_reads += 1
    assert forks > 0 and checked > 0, "empty fork history"
    return dict(verdict="PASS", operations=checked, fork_resources=forks, unavailable_reads=pending_reads,
                retired_grants=retired_grants, reclamation_observations=reclamation_observations,
                reconfirmations=reconfirmations,
                unknown_mutations=sum(e["op"] in ("create", "append", "fork", "delete", "reput") and e.get("status") in (0,503)
                                      for e in events),
                scope="ordered binary fixture: exact fork bytes, retry identity, pending visibility, descendant retention, terminal fences")


if __name__ == "__main__":
    print(json.dumps(check([json.loads(line) for line in open(sys.argv[1])]), indent=2))
