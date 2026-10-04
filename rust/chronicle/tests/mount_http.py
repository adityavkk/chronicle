#!/usr/bin/env python3
"""Two-phase, stop-all mount switch regression against the same persistent data."""
import argparse
import json
import time
import urllib.error
import urllib.request


def run(args):
    with open(args.output, "x", encoding="utf-8") as output:
        def request(method, path, body=b""):
            record = {"schema": 3, "method": method, "path": path,
                      "invoke_mono_ns": time.monotonic_ns()}
            req = urllib.request.Request(args.url + path, data=body if method == "PUT" else None,
                                         method=method, headers={"content-type": "text/plain"})
            output.write(json.dumps({**record, "type": "invoke"}) + "\n")
            output.flush()
            try:
                try:
                    response = urllib.request.urlopen(req, timeout=20)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    record.update(type="unknown" if response.status >= 500 else "ok",
                                  status=response.status, body=response.read().decode())
            except Exception as error:
                record.update(type="unknown", error=repr(error))
                raise
            finally:
                record["complete_mono_ns"] = time.monotonic_ns()
                output.write(json.dumps(record) + "\n")
                output.flush()
            return record

        if args.phase == "prepare":
            for tenant, body in [(args.tenant, b"mounted-visible"), (args.tenant + "-other", b"isolated")]:
                result = request("PUT", f"/v1/stream/{tenant}/{args.path}", body)
                assert result["status"] == 201, result
        else:
            result = request("GET", f"/v1/stream/{args.path}")
            assert result["status"] == 200 and result["body"] == "mounted-visible", result
            result = request("GET", f"/v1/stream/{args.tenant}-other/{args.path}")
            assert result["status"] == 404, result
            result = request("PUT", f"/v1/stream/{args.path}-new", b"new")
            assert result["status"] == 201, result
            result = request("GET", f"/v1/stream/{args.path}-new")
            assert result["status"] == 200 and result["body"] == "new", result
        print(f"mount {args.phase}: PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--phase", choices=["prepare", "verify"], required=True)
    parser.add_argument("--output", required=True)
    run(parser.parse_args())
