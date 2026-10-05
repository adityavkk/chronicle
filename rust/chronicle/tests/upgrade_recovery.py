#!/usr/bin/env python3
"""Check restored history bytes and old producer frontiers, then make new writes.

This is a migration regression, not a concurrent linearizability checker. Use
only an isolated copy of the captured cluster; it appends one record per stream.
"""
import argparse
import gzip
import hashlib
import json
import random
import threading

from history import Client, emit, record


def check(url, source, output):
    with gzip.open(source, "rt") as history:
        events = [json.loads(line) for line in history]
    run = events[0]["value"]
    final = next(event["value"] for event in reversed(events)
                 if event["id"] == "final-read" and event["type"] == "ok")
    expected = "".join(final["records"]).encode("ascii")
    client = Client([url], run["tenant"], run["path"], 10, random.Random(run["seed"]))

    def request(method, body=b"", headers=None):
        status, response, data, error = client.request(method, body, headers)
        assert status is not None and 200 <= status < 300, (status, error)
        return {key.lower(): value for key, value in response.items()}, data

    _, actual = request("GET")
    assert actual == expected, "recovered committed bytes differ from old final strict read"
    for producer in range(run["producers"]):
        identity = f"p{producer}-0"
        original = next(event["value"] for event in events
                        if event.get("id") == identity and event["type"] == "ok")
        headers, _ = request("POST", record(producer, 0, run["seed"]), {
            "Content-Type": "application/octet-stream",
            "producer-id": f"history-{run['seed']}-{producer}",
            "producer-epoch": "0", "producer-seq": "0"})
        assert headers["stream-next-offset"] == original["stream-next-offset"]
        assert headers["stream-duplicate"] == "true"
    _, actual = request("GET")
    assert actual == expected, "old sequence retry changed recovered bytes"
    appended = record(2, 0, run["seed"] + 1_000_000)
    request_headers = {"Content-Type": "application/octet-stream",
                       "producer-id": f"upgrade-{run['seed']}",
                       "producer-epoch": "0", "producer-seq": "0"}
    first, _ = request("POST", appended, request_headers)
    assert first["stream-duplicate"] == "false"
    retry, _ = request("POST", appended, request_headers)
    assert retry["stream-duplicate"] == "true"
    assert retry["stream-next-offset"] == first["stream-next-offset"]
    _, actual = request("GET")
    assert actual == expected + appended, "new append or its retry changed the committed prefix"
    emit(output, threading.Lock(), {"type": "info", "f": "upgrade-recovery", "value": {
        "history": str(source), "original_records": len(final["records"]),
        "original_sha256": hashlib.sha256(expected).hexdigest(),
        "old_sequence_frontiers_checked": run["producers"], "new_records": 1,
        "recovered_sha256": hashlib.sha256(actual).hexdigest()}})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("histories", nargs="+")
    args = parser.parse_args()
    with open(args.output, "x") as output:
        for source in args.histories:
            check(args.url, source, output)
