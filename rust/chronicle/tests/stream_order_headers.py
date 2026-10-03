#!/usr/bin/env python3
"""Raw repeated-header regression on pinned-pod URLs; not a concurrency checker."""
import argparse
import hashlib
import http.client
import itertools
import json
import threading
import urllib.parse

from history import emit


def run(args):
    ids, lock, routes = itertools.count(), threading.Lock(), set()
    with open(args.output, "x", encoding="utf-8") as output:
        def call(base, method, path, headers=(), body=b""):
            url = urllib.parse.urlsplit(base)
            if url.scheme != "http" or url.path not in ("", "/"):
                raise ValueError("use a private HTTP pinned-pod URL")
            op = str(next(ids))
            headers = [("content-type", "application/octet-stream"),
                       ("x-request-id", f"raw-{args.path}-{op}"), *headers]
            common = {"schema": 3, "id": op, "process": "raw-headers", "f": "http"}
            emit(output, lock, {**common, "type": "invoke", "value": {
                "base": base, "method": method, "path": path,
                "headers": headers, "body": body.decode()}})
            connection = http.client.HTTPConnection(url.hostname, url.port, timeout=15)
            try:
                connection.putrequest(method, path)
                for name, value in headers:
                    connection.putheader(name, value)
                connection.putheader("content-length", str(len(body)))
                connection.endheaders(body)
                response = connection.getresponse()
                value = {"status": response.status, "headers": dict(response.getheaders()),
                         "body": response.read().decode()}
                emit(output, lock, {**common, "type": "unknown" if response.status >= 500 else "ok",
                                    "value": value})
                return value
            except (OSError, http.client.HTTPException) as error:
                emit(output, lock, {**common, "type": "unknown", "value": {"error": repr(error)}})
                raise
            finally:
                connection.close()

        for ingress, base in enumerate(args.url):
            before = json.loads(call(base, "GET", "/admin/status")["body"])
            paths = {}
            for i in range(10000):
                path = f"{args.path}-{ingress}-{i}"
                shard = int.from_bytes(hashlib.sha256(("3:raw" + path).encode()).digest()[:8], "big") % 4 + 1
                paths.setdefault(shard, "/v1/stream/raw/" + path)
                if len(paths) == 4:
                    break
            assert len(paths) == 4
            for shard, path in paths.items():
                assert call(base, "PUT", path)["status"] == 201
                for seq, tokens in enumerate((("z", "\u0080"), ("z", "a"), ("z", "z"))):
                    producer = [("producer-id", "raw"), ("producer-epoch", "0"), ("producer-seq", str(seq))]
                    rejected = call(base, "POST", path, [*producer, *[("stream-seq", t) for t in tokens]], b"wrong")
                    assert rejected["status"] == 400, rejected
                    # Same tuple still appends; the rejected z cannot advance the token.
                    accepted = call(base, "POST", path, [*producer, ("stream-seq", str(seq))], b"x")
                    assert accepted["status"] == 200, accepted
                assert call(base, "GET", path)["body"] == "xxx"
                # A comma is part of one opaque token, not a list delimiter.
                assert call(base, "POST", path, [("stream-seq", "3,4")], b"y")["status"] == 204
                assert call(base, "GET", path)["body"] == "xxxy"
            after = json.loads(call(base, "GET", "/admin/status")["body"])
            for shard in paths:
                old, new = before[str(shard)], after[str(shard)]
                assert (old["id"], old["current_leader"]) == (new["id"], new["current_leader"]), "unstable ingress/leader; retain run"
                assert new["current_leader"] is not None
                routes.add("direct" if new["id"] == new["current_leader"] else "forwarded")
        assert routes == {"direct", "forwarded"}, routes
    print(json.dumps({"result": "passed", "routes": sorted(routes), "history": args.output,
                      "scope": "sequential raw HTTP header contract; routing classified from stable observed status"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", action="append", required=True, help="private pinned-pod ingress URL")
    parser.add_argument("--path", required=True, help="unused disposable path prefix")
    parser.add_argument("--output", required=True)
    run(parser.parse_args())
