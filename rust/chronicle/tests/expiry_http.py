#!/usr/bin/env python3
"""Live expiry selection checks; schema-3 observations, not a linearizability proof."""
import argparse
import concurrent.futures
import time

from long_poll import Requests


def run(args):
    with open(args.output, "x", encoding="utf-8") as output:
        call = Requests(args, output).call
        cases = [
            ("read", "GET", None, 200, True),
            ("head", "HEAD", None, 200, False),
            ("stale", "GET", {"consistency": "stale"}, 200, False),
            ("ensure", "PUT", None, 200, False),
            ("invalid-range", "GET", {"offset": "0000000000000000_0000000000000099"}, 416, True),
        ]
        for name, _, _, _, _ in cases:
            assert call(name, "PUT", b"abc", {"stream-ttl": "2"})["status"] == 201
        time.sleep(1.2)
        for name, method, query, status, _ in cases:
            result = call(name, method, headers={"stream-ttl": "2"} if method == "PUT" else None, query=query)
            assert result["status"] == status, result
        time.sleep(1.2)
        for name, _, _, _, renewed in cases:
            result = call(name, "HEAD")
            assert result["status"] == (200 if renewed else 404), (name, result)
            if renewed:
                assert result["headers"]["stream-ttl"] == "2", result

        # The initial long poll renews. Its subsequent timeout/rechecks must not.
        assert call("live", "PUT", headers={"stream-ttl": "2"})["status"] == 201
        time.sleep(1.2)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(call, "live", "GET", query={"offset": "-1", "live": "long-poll"})
            time.sleep(1.2)
            assert call("live", "HEAD")["status"] == 200
            time.sleep(1.2)
            assert call("live", "HEAD")["status"] == 404
            assert pending.result()["status"] == 404
        print("expiry selection: strict GET/error renewal; HEAD/stale/PUT nonrenewal; initial-only long poll: PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--output", required=True)
    run(parser.parse_args())
