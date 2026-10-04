#!/usr/bin/env python3
"""Implicit URL lifecycle and explicit retry fences; schema-3 HTTP observations."""
import argparse
import time

from long_poll import Requests


def run(args):
    with open(args.output, "x", encoding="utf-8") as output:
        call = Requests(args, output).call
        first = call("normal", "PUT")
        assert first["status"] == 201, first
        old = first["headers"]["stream-incarnation"]
        producer = {"producer-id": "p", "producer-epoch": "0", "producer-seq": "0"}
        assert call("normal", "POST", b"old", producer)["status"] == 200
        assert call("normal", "DELETE")["status"] == 204
        fresh = call("normal", "PUT", b"new")
        assert fresh["status"] == 201 and int(fresh["headers"]["stream-incarnation"]) == int(old) + 1, fresh
        assert call("normal", "POST", b"old", {**producer, "stream-incarnation": old})["status"] == 409
        assert call("normal", "DELETE", headers={"stream-incarnation": old})["status"] == 409
        assert call("normal", "PUT", headers={"stream-incarnation": old})["status"] == 409
        assert call("normal", "POST", b"fresh", producer)["status"] == 200
        read = call("normal", "GET")
        assert read["status"] == 200 and read["body"] == "newfresh", read
        assert call("normal", "POST", b"bad", {"stream-incarnation": ""})["status"] == 400
        assert call("normal", "DELETE")["status"] == 204
        expired = call("expiry", "PUT", b"expired", {"stream-ttl": "1"})
        assert expired["status"] == 201, expired
        time.sleep(1.1)
        renewed = call("expiry", "PUT", b"replacement")
        assert renewed["status"] == 201 and int(renewed["headers"]["stream-incarnation"]) == int(expired["headers"]["stream-incarnation"]) + 1, renewed
        assert call("expiry", "GET")["body"] == "replacement"
        print("implicit deletion/TTL recreation and producer reset; explicit stale create/append/delete: PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--output", required=True)
    run(parser.parse_args())
