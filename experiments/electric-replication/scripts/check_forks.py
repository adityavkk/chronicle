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


def check(events):
    resources = {}
    checked = 0
    forks = 0
    pending_reads = 0

    def retained(resource):
        return any(child["granted"] and (not child["deleted"] or retained(child))
                   for child in resource["children"])

    for event in sorted(events, key=lambda e: e["start"]):
        op, status = event["op"], event.get("status", 0)
        if op == "inspect" and status == 200:
            for transaction in json.loads(decode(event["result"]))["transactions"]:
                if transaction["path"] in resources and transaction["granted"]:
                    resources[transaction["path"]]["granted"] = True
            continue
        if op not in ("create", "append", "fork", "delete", "read"):
            continue
        checked += 1
        path = event["path"].split("?")[0]
        resource = resources.get(path)
        if op == "create" and status == 201:
            assert not resource or (resource["deleted"] and not retained(resource)), "recreated retained path"
            resources[path] = dict(data=decode(event["body"]), deleted=False, children=[], granted=True)
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
                                deleted=False, children=[], granted=status in (200, 201))
                resources[path] = resource
                source["children"].append(resource)
                forks += 1
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
                scope="ordered binary fixture: exact fork bytes, retry identity, pending visibility, descendant retention")


if __name__ == "__main__":
    print(json.dumps(check([json.loads(line) for line in open(sys.argv[1])]), indent=2))
