#!/usr/bin/env python3
"""Schema-3 checks for fail-closed headers and committed write status mapping."""
import argparse
import json

from long_poll import Requests


def run(args):
    with open(args.output, "x", encoding="utf-8") as output:
        call = Requests(args, output).call
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
        rejected = call("seq", "POST", b"wrong", {**producer, "stream-seq": "A"})
        assert rejected["status"] == 501 and rejected["body"] == "unsupported header: stream-seq", rejected
        assert call("seq", "GET")["body"] == "before"
        # Rejection must not consume the producer tuple or produce a cached success.
        accepted = call("seq", "POST", b"after", producer)
        assert accepted["status"] == 200 and accepted["headers"]["stream-duplicate"] == "false", accepted
        read = call("seq", "GET")
        assert read["status"] == 200 and read["body"] == "beforeafter", read
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
            if "producer-id" in headers:
                duplicate = call(name, "POST", body, headers)
                assert duplicate["status"] == 204 and duplicate["headers"]["stream-duplicate"] == "true", duplicate
                assert duplicate["headers"]["stream-next-offset"] == appended["headers"]["stream-next-offset"], duplicate
            read = call(name, "GET")
            assert read["status"] == 200 and read["body"] == (b"A" + body).decode(), read
            if "stream-closed" in headers:
                assert read["headers"]["stream-closed"] == "true", read
    print(json.dumps({"result": "passed", "history": args.output,
                      "scope": "unsupported-header and committed write status contract, not full conformance"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--path", required=True, help="unused disposable path prefix")
    parser.add_argument("--output", required=True)
    run(parser.parse_args())
