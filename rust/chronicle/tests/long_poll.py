#!/usr/bin/env python3
"""Live HTTP contract checks, not full conformance or a linearizability checker.

Retains schema-3 HTTP invoke/completion observations (including rejected and
unknown results). These are not input to the existing schema-1/2 Porcupine adapter.
"""
import argparse
import concurrent.futures
import itertools
import json
import random
import threading
import time
import urllib.request

from history import Client, emit


class Requests:
    def __init__(self, args, output):
        self.args, self.output = args, output
        self.lock, self.ids = threading.Lock(), itertools.count()

    def call(self, suffix, method, data=b"", headers=None, query=None):
        with self.lock:
            op_id = str(next(self.ids))
        path = self.args.path + "-" + suffix
        common = {"schema": 3, "id": op_id, "process": threading.current_thread().name, "f": "http"}
        headers = {"content-type": "application/octet-stream", **(headers or {})}
        headers["x-request-id"] = f"live-{self.args.path}-{op_id}"
        emit(self.output, self.lock, {**common, "type": "invoke", "value": {
            "tenant": "live", "path": path, "method": method, "headers": headers,
            "query": query, "body": data.decode()}})
        client = Client([self.args.url], "live", path, 20, random.Random(0))
        started = time.monotonic()
        status, response_headers, body, error = client.request(method, data, headers, query=query)
        result = {"status": status, "headers": {k.lower(): v for k, v in response_headers.items()},
                  "body": body.decode(), "error": error, "elapsed_s": time.monotonic() - started}
        emit(self.output, self.lock, {**common, "type": "unknown" if status is None or status >= 500 else "ok",
                                      "value": result})
        return result


def expect_read(result, status, body, end, closed=False):
    assert result["status"] == status, result
    assert result["body"] == body, result
    h = result["headers"]
    assert int(h["stream-next-offset"].split("_")[1]) == end, result
    assert h["stream-up-to-date"] == "true" and h["stream-consistency"] == "strict", result
    assert int(h["stream-cursor"]) >= 0, result
    assert (h.get("stream-closed") == "true") == closed, result


def offset(value):
    return f"0000000000000000_{value:016d}"


def run(args):
    with open(args.output, "x", encoding="utf-8") as output:
        requests = Requests(args, output)
        call = requests.call
        live = {"live": "long-poll", "offset": "-1"}
        assert call("binary", "PUT", b"abc")["status"] == 201
        expect_read(call("binary", "GET", query=live), 200, "abc", 3)
        for method, query in [("HEAD", live), ("GET", {"live": "long-poll"}),
                              ("GET", {**live, "consistency": "stale"}),
                              ("GET", {**live, "live": "invalid"})]:
            assert call("binary", method, query=query)["status"] == 400
        quiet = call("binary", "GET", query={**live, "offset": offset(3), "cursor": "1000000000"})
        expect_read(quiet, 204, "", 3)
        assert quiet["elapsed_s"] >= 4, quiet  # Distinguish waiting from immediate catch-up.
        assert int(quiet["headers"]["stream-cursor"]) > 1_000_000_000
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(call, "binary", "GET", query={**live, "offset": offset(3)})
            time.sleep(.2)
            assert call("binary", "POST", b"WXYZ")["status"] == 200
            expect_read(future.result(), 200, "WXYZ", 7)
        assert call("binary", "POST", headers={"stream-closed": "true"})["status"] == 200
        for cursor in ["now", offset(99)]:
            eof = call("binary", "GET", query={**live, "offset": cursor})
            expect_read(eof, 204, "", 7, closed=True)
            assert eof["elapsed_s"] < 4, eof
        assert call("binary", "GET", query={**live, "cursor": str(2**64-1)})["status"] == 400

        assert call("json", "PUT", b'["a,b",true]', {"content-type": "application/json"})["status"] == 201
        expect_read(call("json", "GET", query={**live, "offset": offset(6)}), 200, "[true]", 11)
        expect_read(call("json", "GET", query={**live, "offset": "now"}), 204, "", 11)
        assert call("expiry", "PUT", headers={"stream-ttl": "1"})["status"] == 201
        assert call("expiry", "GET", query=live)["status"] == 404

        # The dedicated live limit must leave room for a writer that wakes everyone.
        assert call("admission", "PUT")["status"] == 201
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            futures = [executor.submit(call, "admission", "GET", query=live) for _ in range(32)]
            deadline = time.monotonic() + 3
            while True:
                with urllib.request.urlopen(args.url + "/metrics", timeout=2) as response:
                    metrics = response.read().decode()
                if "chronicle_live_read_available_slots 0\n" in metrics:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("32 live waiters did not acquire admission")
                time.sleep(.025)
            emit(output, requests.lock, {"schema": 3, "type": "info", "f": "admission", "value": {"available_slots": 0}})
            assert call("admission", "GET", query=live)["status"] == 429
            assert call("admission", "POST", b"wake")["status"] == 200
            for future in futures:
                expect_read(future.result(), 200, "wake", 4)
    print(json.dumps({"result": "passed", "history": args.output,
                      "scope": "long-poll HTTP contract; no fault or linearizability claim"}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--path", required=True, help="new unused path prefix")
    parser.add_argument("--output", required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
