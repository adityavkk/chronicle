"""Independent receipt/append fixture checker, not a Rust import or Raft proof.

202 is an UNKNOWN append, never a successful append. Only a terminal receipt
refines its interval to a committed result. Reuse the byte/real-time checker on
those observations. Explicit fault-phase expectations check the directed fixture;
this is not an arbitrary-history checker for lifecycle or membership operations.
"""
import base64
from collections import Counter
import json
import sys

from check_history import check as check_appends


def identity(token):
    version, data = token.split(".")
    assert version == "er1"
    result = json.loads(base64.urlsafe_b64decode(data + "=" * (-len(data) % 4)))
    assert set(result) == {"cluster", "group", "position"}
    assert 0 <= result["position"]["ordinal"] < 64
    assert set(result["position"]["log_id"]) == {"leader_id", "index"}
    assert set(result["position"]["log_id"]["leader_id"]) == {"term", "node_id"}
    return result


def check(history):
    accepted = {}
    observed = {}
    projected = []
    states = Counter()
    for event in history:
        op = event["op"]
        if op in ("append", "reject"):
            if event["status"] == 202:
                headers = event["headers"]
                assert "stream-session" not in headers and "stream-next-offset" not in headers, "202 is not a session"
                assert event["local"] and headers["stream-durability"] == "local-fsync"
                token = headers["stream-receipt"]
                assert headers["location"] == "/_receipts/" + token
                assert json.loads(event["body"]) == dict(state="accepted", receipt=token, location=headers["location"])
                assert token not in accepted, "different attempts share a receipt"
                identity(token)
                accepted[token] = event
            if op == "append":
                projected.append(dict(event, status=0 if event["status"] == 202 else event["status"]))
        elif op == "read":
            assert not set(event.get("forbidden", [])) & set(event["records"]), "speculative or invalidated data visible"
            projected.append(event)
        elif op == "live":
            assert event["status"] == 200
            assert "event:data" not in event["wire"].replace(" ", ""), "uncommitted SSE wake/data"
        elif op == "lease":
            assert event["waited_ns"] > 700_000_000
            assert event["before"]["current_leader"] == event["node"]
            denied = event["denied"]
            assert denied["status"] == 503 and "stream-receipt" not in denied["headers"]
            assert event["before"]["last_log_index"] == event["after"]["last_log_index"], "expired lease assigned an index"
            assert event["wal_before"] and event["wal_before"] == event["wal_after"], "expired lease changed native WAL"
            before, after = event["occupancy_before"], event["occupancy_after"]
            assert 0 < before["pending_commands"] < before["max_pending_commands"]
            assert before["pending_bytes"] < before["max_pending_bytes"]
            assert after["pending_commands"] == before["pending_commands"], "expiry freed unresolved credit"
            assert after["pending_bytes"] == before["pending_bytes"], "expiry freed unresolved bytes"
        elif op == "receipt":
            token = event["token"]
            assert token in accepted, "unissued fixture receipt"
            result = event["result"]
            state = result["state"]
            states[state] += 1
            if event.get("expected"):
                assert state == event["expected"], "receipt contradicts directed fault-phase contract"
            assert event["status"] == dict(pending=202, committed=200, rejected=200, unknown=404, invalidated=410)[state]
            progress = result["progress"]
            assert 0 <= progress["pending_commands"] <= progress["max_pending_commands"]
            assert 0 <= progress["pending_bytes"] <= progress["max_pending_bytes"]
            prior = observed.get(token)
            if state in ("committed", "rejected", "invalidated"):
                assert prior in (None, state), "conflicting terminal outcomes"
                observed[token] = state
            if state != "committed":
                assert result["session"] is None and "stream-session" not in event["headers"]
            if state in ("committed", "rejected"):
                attempt = accepted[token]
                position = identity(token)
                index = position["position"]["log_id"]["index"]
                assert progress["applied"]["index"] >= index
                reply = result["response"]
                if state == "rejected":
                    assert attempt["op"] == "reject" and reply["status"] == attempt["semantic_status"]
                else:
                    assert attempt["op"] == "append" and reply["status"] in (200, 204)
                    session = f'{position["cluster"]}:{position["group"]}:{index}'
                    assert result["session"] == event["headers"]["stream-session"] == session
                    projected.append(dict(attempt, status=reply["status"], headers=dict(reply["headers"]), end=event["end"]))
            else:
                assert result["response"] is None
    verdict = check_appends(projected)
    assert accepted and all(token in observed for token in accepted), "fixture must account for every 202"
    return dict(verdict="PASS", operations=len(history), accepted=len(accepted),
                terminal_receipts=dict(Counter(observed.values())), observations=dict(states),
                lease_expirations_checked=sum(e["op"] == "lease" for e in history),
                unknown_requests=sum(e["op"] in ("append", "reject") and e["status"] in (0,503) for e in history),
                append_checker=verdict, scope="directed receipt fixture plus independent byte-prefix/real-time graph; one host")


if __name__ == "__main__":
    print(json.dumps(check([json.loads(line) for line in open(sys.argv[1])]), indent=2))
