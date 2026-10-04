#!/usr/bin/env python3
"""Conditional-read HTTP checks; schema-3 observations, not Porcupine evidence."""
import argparse
import time

from long_poll import Requests, offset


def run(args):
    with open(args.output, "x", encoding="utf-8") as output:
        call = Requests(args, output).call
        assert call("binary", "PUT", b"abc")["status"] == 201
        first = call("binary", "GET")
        assert first["status"] == 200 and first["body"] == "abc", first
        tag = first["headers"]["etag"]
        for validator in [tag, f'"other", W/{tag}', "*"]:
            same = call("binary", "GET", headers={"if-none-match": validator})
            assert same["status"] == 304 and same["body"] == "", same
        beyond = call("binary", "GET", headers={"if-none-match": "*"}, query={"offset": offset(4)})
        assert beyond["status"] == 416, beyond
        assert call("binary", "POST", b"d")["status"] == 204
        changed = call("binary", "GET", headers={"if-none-match": tag})
        assert changed["status"] == 200 and changed["body"] == "abcd", changed
        assert changed["headers"]["etag"] != tag
        assert call("binary", "DELETE")["status"] == 204
        assert call("binary", "GET", headers={"if-none-match": "*"})["status"] == 404
        assert call("binary", "PUT", b"xyz")["status"] == 201
        fresh = call("binary", "GET", headers={"if-none-match": tag})
        assert fresh["status"] == 200 and fresh["headers"]["etag"] != tag, fresh
        assert call("json", "PUT", b'[12,345]', {"content-type": "application/json"})["status"] == 201
        invalid = call("json", "GET", headers={"if-none-match": "*"}, query={"offset": offset(1)})
        assert invalid["status"] == 400, invalid
        assert call("ttl", "PUT", b"ttl", {"stream-ttl": "2"})["status"] == 201
        time.sleep(1.2)
        renewed = call("ttl", "GET", headers={"if-none-match": "*"})
        assert renewed["status"] == 304, renewed
        time.sleep(1.2)
        assert call("ttl", "HEAD")["status"] == 200
        time.sleep(1.0)
        assert call("ttl", "GET", headers={"if-none-match": "*"})["status"] == 404
        print("ETag revalidation, append/recreation, invalid ranges, expiry and 304 TTL renewal: PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--output", required=True)
    run(parser.parse_args())
