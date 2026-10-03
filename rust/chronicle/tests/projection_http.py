#!/usr/bin/env python3
"""Real HTTP checks for projection framing, headers, cursors and HEAD (not full conformance)."""
import argparse
import json
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    args = parser.parse_args()
    checks = []
    for kind, payload, expected, end in [
        ("application/octet-stream", b"abcde", b"abcde", 5),
        ("application/json", b'["a,b",true]', b'["a,b",true]', 11),
    ]:
        url = args.url.rstrip("/") + "/v1/stream/projection/" + uuid.uuid4().hex

        def request(method, suffix="", data=None):
            req = urllib.request.Request(url + suffix, data=data, method=method,
                                         headers={"Content-Type": kind})
            try:
                response = urllib.request.urlopen(req, timeout=15)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                return response.status, response.headers, response.read()

        assert request("PUT", data=payload)[0] == 201
        for method in ["GET", "HEAD"]:
            status, headers, data = request(method)
            assert status == 200
            assert int(headers["Content-Length"]) == len(expected)
            assert int(headers["stream-next-offset"].split("_")[1]) == end
            assert data == (expected if method == "GET" else b"")
            checks.append(f"{kind} {method} length/frontier/body")
        status, headers, data = request("GET", "?offset=now")
        assert status == 200 and data == (b"[]" if kind == "application/json" else b"")
        assert int(headers["Content-Length"]) == len(data)
        if kind == "application/json":
            assert request("GET", "?offset=0000000000000000_0000000000000003")[0] == 400
            assert request("GET", "?offset=0000000000000000_0000000000000006")[2] == b"[true]"
            checks.append("JSON embedded comma rejected; complete value cursor accepted")
        checks.append(f"{kind} empty tail")
    print(json.dumps({"checks": checks, "result": "passed"}, indent=2))


if __name__ == "__main__":
    main()
