#!/usr/bin/env python3
"""Schema-3 checks for fail-closed headers and committed write status mapping."""
import argparse
import json

from long_poll import Requests


def run(args):
    with open(args.output, "x", encoding="utf-8") as output:
        requests = Requests(args, output)

        def call(*positional, **named):
            result = requests.call(*positional, **named)
            assert result["headers"].get("x-content-type-options") == "nosniff", result
            assert result["headers"].get("cross-origin-resource-policy") == "cross-origin", result
            return result

        for name, value in (
            ("stream-forked-from", "/v1/stream/source"),
            ("stream-fork-offset", "0000000000000000_0000000000000000"),
            ("stream-fork-sub-offset", "0"),
            ("stream-expires-at", "2099-01-01T00:00:00Z"),
        ):
            rejected = call(name, "PUT", b"must-not-exist", {name: value})
            assert rejected["status"] == 501 and rejected["body"] == f"unsupported header: {name}", rejected
            assert rejected["headers"].get("x-request-id") and rejected["headers"].get("traceparent"), rejected
            assert call(name, "GET")["status"] == 404
            # Unknown application headers remain allowed, rather than a blanket ban.
            created = call(name, "PUT", b"accepted", {"x-application-tag": "fixture"})
            assert created["status"] == 201 and created["headers"]["content-type"] == "application/octet-stream", created
            retried = call(name, "PUT", b"different initial bytes")
            assert retried["status"] == 200 and retried["headers"]["content-type"] == "application/octet-stream", retried
            assert retried["headers"]["stream-duplicate"] == "true", retried
            conflict = call(name, "PUT", b"different type", {"content-type": "text/plain"})
            assert conflict["status"] == 409, conflict
            read = call(name, "GET")
            assert read["status"] == 200 and read["body"] == "accepted", read

        assert call("seq", "PUT", b"before")["status"] == 201
        producer = {"producer-id": "header-test", "producer-epoch": "0", "producer-seq": "0"}
        assert call("seq", "POST", b"wrong", {"stream-seq": "\u0080"})["status"] == 400
        assert call("seq", "POST", b"x", {"stream-seq": ""})["status"] == 204
        rejected = call("seq", "POST", b"wrong", {**producer, "stream-seq": ""})
        assert rejected["status"] == 409 and rejected["body"] == "StreamSequenceConflict", rejected
        assert rejected["headers"]["stream-next-offset"] == f"{0:016d}_{7:016d}", rejected
        assert call("seq", "GET")["body"] == "beforex"
        # Rejection must not consume the producer tuple or produce a cached success.
        accepted = call("seq", "POST", b"after", {**producer, "stream-seq": "10"})
        assert accepted["status"] == 200 and accepted["headers"]["stream-duplicate"] == "false", accepted
        assert call("seq", "POST", b"!", {**producer, "producer-seq": "1", "stream-seq": "2"})["status"] == 200
        retry = call("seq", "POST", b"ignored", {**producer, "stream-seq": "zz"})
        assert retry["status"] == 204 and retry["headers"]["stream-next-offset"] == f"{0:016d}_{12:016d}", retry
        assert call("seq", "POST", b"wrong", {**producer, "producer-epoch": "1", "stream-seq": "10"})["status"] == 409
        assert call("seq", "POST", b"?", {**producer, "producer-seq": "2"})["status"] == 200
        assert call("seq", "POST", b"wrong", {**producer, "producer-seq": "3", "stream-seq": "2"})["status"] == 409
        assert call("seq", "POST", b"!", {**producer, "producer-seq": "3", "stream-seq": "3"})["status"] == 200
        assert call("seq", "POST", headers={**producer, "producer-seq": "4", "stream-seq": "4", "stream-closed": "true"})["status"] == 204
        assert call("seq", "POST", headers={"stream-seq": "1", "stream-closed": "true"})["status"] == 204
        read = call("seq", "GET")
        assert read["status"] == 200 and read["body"] == "beforexafter!?!", read
        for name, body, headers, expected in (
            ("ordinary", b"BC", {}, 204),
            ("producer", b"BC", producer, 200),
            ("close", b"", {"stream-closed": "true"}, 204),
            ("producer-close", b"", {**producer, "stream-closed": "true"}, 204),
        ):
            assert call(name, "PUT", b"A")["status"] == 201
            appended = call(name, "POST", body, headers)
            assert appended["status"] == expected and appended["body"] == "", appended
            end = len(b"A" + body)
            assert appended["headers"]["stream-next-offset"] == f"{0:016d}_{end:016d}", appended
            assert appended["headers"].get("stream-closed") == headers.get("stream-closed"), appended
            if "producer-id" in headers:
                assert appended["headers"]["producer-epoch"] == "0" and appended["headers"]["producer-seq"] == "0", appended
                # Changed close intent cannot change the cached tuple's effect.
                duplicate = call(name, "POST", body, {**headers, "stream-closed": "TRUE"})
                assert duplicate["status"] == 204 and duplicate["headers"]["stream-duplicate"] == "true", duplicate
                assert duplicate["headers"]["stream-next-offset"] == appended["headers"]["stream-next-offset"], duplicate
                assert duplicate["headers"].get("stream-closed") == headers.get("stream-closed"), duplicate
                for retry_headers in (producer, {**producer, "stream-closed": "false"}):
                    empty_retry = call(name, "POST", headers=retry_headers)
                    assert empty_retry["status"] == 204 and empty_retry["headers"]["stream-duplicate"] == "true", empty_retry
                    assert empty_retry["headers"].get("stream-closed") == headers.get("stream-closed"), empty_retry
                    assert empty_retry["headers"]["stream-next-offset"] == appended["headers"]["stream-next-offset"], empty_retry
            read = call(name, "GET")
            assert read["status"] == 200 and read["body"] == (b"A" + body).decode(), read
            if "stream-closed" in headers:
                assert read["headers"]["stream-closed"] == "true", read
                repeated = call(name, "POST", headers={"stream-closed": "TrUe", "content-type": "text/plain"})
                assert repeated["status"] == 204 and repeated["headers"]["stream-closed"] == "true", repeated
                rejected = call(name, "POST", b"not applied")
                assert rejected["status"] == 409 and rejected["headers"]["stream-closed"] == "true", rejected
                assert rejected["headers"]["stream-next-offset"] == f"{0:016d}_{end:016d}", rejected
            elif name == "producer":
                closing = call(name, "POST", b"D", {**producer, "producer-seq": "1", "stream-closed": "TRUE"})
                assert closing["status"] == 200 and closing["headers"]["stream-closed"] == "true", closing
                old = call(name, "POST", b"ignored", producer)
                assert old["status"] == 204 and old["headers"]["stream-closed"] == "true", old
                assert old["headers"]["stream-next-offset"] == f"{0:016d}_{3:016d}", old
                assert old["headers"]["producer-epoch"] == "0" and old["headers"]["producer-seq"] == "1", old
                assert call(name, "GET")["body"] == "ABCD"
        for closed in (False, True):
            name = f"create-closed-{closed}"
            headers = {"stream-closed": "TrUe" if closed else "yes"}
            created = call(name, "PUT", b"created", headers)
            assert created["status"] == 201 and created["headers"].get("stream-closed") == ("true" if closed else None), created
            assert call(name, "PUT", headers=headers)["status"] == 200
            opposite = {"stream-closed": "false" if closed else "true"}
            assert call(name, "PUT", headers=opposite)["status"] == 409
        assert call("empty", "PUT")["status"] == 201
        assert call("empty", "POST", headers=producer)["status"] == 400
        assert call("empty", "POST", b"accepted", producer)["status"] == 200
        assert call("empty", "GET")["body"] == "accepted"
        json_producer = {**producer, "content-type": "application/json"}
        assert call("empty-json", "PUT", headers=json_producer)["status"] == 201
        # The pinned protocol rejects empty JSON appends before Raft proposal.
        assert call("empty-json", "POST", b"[]", json_producer)["status"] == 400
        assert call("empty-json", "POST", b"[1]", json_producer)["status"] == 200
        assert call("empty-json", "GET")["body"] == "[1]"
        assert call("empty-json", "HEAD")["status"] == 200
        assert call("empty-json", "DELETE")["status"] == 204
        assert call("producer-position", "PUT")["status"] == 201
        gap = call("producer-position", "POST", b"wrong", {**producer, "producer-seq": "3"})
        assert gap["status"] == 409 and gap["headers"]["producer-expected-seq"] == "0", gap
        assert gap["headers"]["producer-received-seq"] == "3" and "producer-epoch" not in gap["headers"], gap
        assert call("producer-position", "POST", b"A", producer)["status"] == 200
        gap = call("producer-position", "POST", b"C", {**producer, "producer-seq": "2"})
        assert gap["status"] == 409 and gap["headers"]["producer-expected-seq"] == "1", gap
        assert gap["headers"]["producer-received-seq"] == "2" and gap["headers"]["producer-seq"] == "0", gap
        assert call("producer-position", "POST", b"BB", {**producer, "producer-seq": "1"})["status"] == 200
        assert call("producer-position", "POST", b"C", {**producer, "producer-seq": "2"})["status"] == 200
        old = call("producer-position", "POST", b"ignored", producer)
        assert old["status"] == 204 and old["headers"]["producer-seq"] == "2", old
        assert old["headers"]["stream-next-offset"] == f"{0:016d}_{1:016d}", old
        invalid_epoch = call("producer-position", "POST", b"wrong", {**producer, "producer-epoch": "7", "producer-seq": "3"})
        assert invalid_epoch["status"] == 400 and invalid_epoch["headers"]["producer-expected-seq"] == "0", invalid_epoch
        assert invalid_epoch["headers"]["producer-epoch"] == "0" and invalid_epoch["headers"]["producer-seq"] == "2", invalid_epoch
        upgraded = call("producer-position", "POST", b"D", {**producer, "producer-epoch": "7"})
        assert upgraded["status"] == 200 and upgraded["headers"]["producer-epoch"] == "7", upgraded
        assert upgraded["headers"]["producer-seq"] == "0", upgraded
        fenced = call("producer-position", "POST", b"ignored", producer)
        assert fenced["status"] == 403 and fenced["headers"]["producer-epoch"] == "7", fenced
        assert fenced["headers"]["producer-seq"] == "0", fenced
        assert call("producer-position", "GET")["body"] == "ABBCD"
    print(json.dumps({"result": "passed", "history": args.output,
                      "scope": "unsupported-header and committed write status contract, not full conformance"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--path", required=True, help="unused disposable path prefix")
    parser.add_argument("--output", required=True)
    run(parser.parse_args())
