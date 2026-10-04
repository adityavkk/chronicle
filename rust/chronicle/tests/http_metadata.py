#!/usr/bin/env python3
"""Supplemental framing/Location observations; upstream suite remains unchanged."""
import argparse

from long_poll import Requests, offset


def run(args):
    with open(args.output, "x", encoding="utf-8") as output:
        call = Requests(args, output).call
        json_type = {"content-type": "APPLICATION/JSON; charset=utf-8"}
        created = call("json", "PUT", b'["a","b"]', json_type)
        assert created["status"] == 201, created
        assert created["headers"]["location"] == args.url + "/v1/stream/live/" + args.path + "-json", created
        read = call("json", "GET")
        assert read["status"] == 200 and read["body"] == '["a","b"]', read
        assert read["headers"]["content-type"] == json_type["content-type"], read
        ensured = call("json", "PUT", headers={"content-type": "application/json;charset=ascii"})
        assert ensured["status"] == 200 and ensured["headers"]["content-type"] == json_type["content-type"], ensured
        assert call("json", "POST", b'{"c":1}', {"content-type": "application/json"})["status"] == 204
        read = call("json", "GET", query={"offset": offset(8)})
        assert read["status"] == 200 and read["body"] == '[{"c":1}]', read
        assert call("json", "GET", query={"offset": offset(1)})["status"] == 400
        assert call("json", "POST", b"mismatch", {"content-type": "text/plain"})["status"] == 409
        assert call("json", "POST", headers={"content-type": "unrelated", "stream-closed": "true"})["status"] == 204
        assert call("opaque", "PUT", b"raw", {"content-type": "application/jsonp"})["status"] == 201
        read = call("opaque", "GET")
        assert read["status"] == 200 and read["body"] == "raw", read
        assert call("empty-type", "PUT")["status"] == 201
        producer = {"producer-id": "p", "producer-epoch": "0", "producer-seq": "0"}
        assert call("empty-type", "POST", b"bad", {**producer, "content-type": ""})["status"] == 400
        assert call("empty-type", "POST", b"good", producer)["status"] == 200
        read = call("empty-type", "GET")
        assert read["body"] == "good", read
        print("HTTP metadata: preserved authority/type, case-insensitive JSON, exact framing and ranges: PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--output", required=True)
    run(parser.parse_args())
