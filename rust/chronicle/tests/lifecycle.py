#!/usr/bin/env python3
"""Execute the schema-2 retry/lifecycle contract against a real HTTP cluster.

This emits observations, not expected results. The existing offline Go Porcupine
model decides correctness. It is sequential contract coverage, not a fault test.
"""
import argparse
import json
import random
import threading

from history import Client, emit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", action="append", required=True)
    parser.add_argument("--path", required=True, help="new, unused stream path")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    client = Client(args.url, "contract", args.path, 10, random.Random(0))
    lock = threading.Lock()
    operations = [
        ("create", {"data": "abc"}),
        ("append", {"data": "A", "producer": "p", "seq": 0}),
        ("append", {"data": "gap", "producer": "p", "seq": 2}),
        ("append", {"data": "BB", "producer": "p", "seq": 1}),
        ("append", {"data": "CC", "producer": "p", "seq": 2}),
        ("append", {"data": "different", "producer": "p", "seq": 0}),
        ("read", {}),
        ("append", {"data": "DDD", "producer": "p", "epoch": 1, "seq": 0}),
        ("append", {"data": "fenced", "producer": "p", "seq": 2}),
        ("append", {"close": True, "producer": "p", "epoch": 1, "seq": 1}),
        ("append", {"data": "retry after close", "producer": "p", "epoch": 1, "seq": 0}),
        ("append", {"data": "closed", "producer": "p", "epoch": 1, "seq": 2}),
        ("read", {}),
        ("delete", {}),
        ("create", {}),
        ("create", {"expected_incarnation": 2, "data": "Q"}),
        ("append", {"data": "old incarnation", "producer": "p", "seq": 0}),
        ("append", {"incarnation": 2, "data": "Z", "producer": "p", "seq": 0}),
        ("read", {}),
        ("delete", {"incarnation": 2}),
        ("delete", {"incarnation": 2}),
    ]
    with open(args.output, "x", encoding="utf-8") as output:
        for index, (kind, values) in enumerate(operations):
            value = {"tenant": "contract", "path": args.path,
                     "incarnation": 1, "content_type": "application/octet-stream", **values}
            common = {"schema": 2, "process": "lifecycle", "id": str(index), "f": kind}
            emit(output, lock, {**common, "type": "invoke", "value": value})
            headers = {"content-type": value["content_type"],
                       "stream-incarnation": str(value.get("expected_incarnation", value["incarnation"]))}
            if "producer" in value:
                headers.update({"producer-id": value["producer"],
                                "producer-epoch": str(value.get("epoch", 0)),
                                "producer-seq": str(value["seq"])})
            if value.get("close"):
                headers["stream-closed"] = "true"
            status, response_headers, body, error = client.request(
                {"create": "PUT", "append": "POST", "read": "GET", "delete": "DELETE"}[kind],
                value.get("data", "").encode(), headers)
            if status is None or status >= 500:
                emit(output, lock, {**common, "type": "unknown", "value": {"error": error}})
                raise RuntimeError("transport outcome unknown; retain this history")
            if status >= 400:
                result = {"error": "Missing" if status == 404 else body.decode()}
                typ = "fail"
            else:
                h = {k.lower(): v for k, v in response_headers.items()}
                result = {"end": int(h["stream-next-offset"].split("_")[1]),
                          "incarnation": int(h["stream-incarnation"])}
                if kind == "read":
                    result.update(data=body.decode(), closed=h.get("stream-closed") == "true")
                else:
                    result["duplicate"] = status == 204 and kind != "delete"
                typ = "ok"
            emit(output, lock, {**common, "type": typ, "value": result})
    print(json.dumps({"history": args.output, "completed_operations": len(operations)}))


if __name__ == "__main__":
    main()
