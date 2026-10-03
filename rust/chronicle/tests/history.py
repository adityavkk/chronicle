#!/usr/bin/env python3
"""Small, dependency-free Chronicle history generator and offline checker."""

import argparse
import concurrent.futures
import datetime
import http.client
import json
import os
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

RECORD_SIZE = 96


def emit(fp, lock, event):
    event = {"schema": 1, "time_ns": time.monotonic_ns(), **event}
    with lock:
        fp.write(json.dumps(event, sort_keys=True) + "\n")
        fp.flush()


def record(producer, seq, seed):
    prefix = f"R|{seed:016x}|{producer:04d}|{seq:012d}|"
    return (prefix + "x" * (RECORD_SIZE - len(prefix) - 1) + "\n").encode()


def split_records(body):
    if not body:
        return []
    if len(body) % RECORD_SIZE:
        raise ValueError(f"body length {len(body)} is not a multiple of {RECORD_SIZE}")
    out = [body[i:i + RECORD_SIZE].decode("ascii") for i in range(0, len(body), RECORD_SIZE)]
    if any(not x.endswith("\n") or not x.startswith("R|") for x in out):
        raise ValueError("malformed fixed-width record")
    return out


class Client:
    def __init__(self, urls, tenant, path, timeout, rng):
        self.urls, self.tenant, self.path, self.timeout, self.rng = urls, tenant, path, timeout, rng

    def request(self, method, body=b"", headers=None, stale=False, query=None):
        base = self.rng.choice(self.urls).rstrip("/")
        path = "/v1/stream/{}/{}".format(urllib.parse.quote(self.tenant), urllib.parse.quote(self.path, safe="/"))
        parameters = dict(query or {})
        if stale:
            parameters["consistency"] = "stale"
        if parameters:
            path += "?" + urllib.parse.urlencode(parameters)
        req = urllib.request.Request(base + path, data=body if method in ("PUT", "POST") else None,
                                     headers=headers or {}, method=method)
        try:
            detail = None
            try:
                response = urllib.request.urlopen(req, timeout=self.timeout)
            except urllib.error.HTTPError as error:
                response, detail = error, str(error)
            with response as r:
                return r.status, dict(r.headers.items()), r.read(), detail
        except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as e:
            return None, {}, b"", repr(e)


def operation(fp, lock, process, op_id, function, call):
    emit(fp, lock, {"type": "invoke", "process": process, "id": op_id, "f": function})
    started = time.monotonic()
    status, headers, body, error = call()
    value = {"status": status, "latency_ms": round((time.monotonic() - started) * 1000, 3)}
    value.update({k.lower(): v for k, v in headers.items() if k.lower().startswith("stream-")})
    if function == "read" and status == 200:
        try:
            value["records"] = split_records(body)
        except ValueError as e:
            value["decode_error"] = str(e)
    if error:
        value["error"] = error
    typ = "ok" if status is not None and 200 <= status < 300 else ("fail" if status is not None and status < 500 else "unknown")
    emit(fp, lock, {"type": typ, "process": process, "id": op_id, "f": function, "value": value})
    return typ, value


def run_workload(a):
    rng = random.Random(a.seed)
    lock, stop = threading.Lock(), threading.Event()
    os.makedirs(os.path.dirname(os.path.abspath(a.output)), exist_ok=True)
    with open(a.output, "x", encoding="utf-8") as fp:
        emit(fp, lock, {"type": "info", "f": "run", "value": {"seed": a.seed, "urls": a.url,
             "tenant": a.tenant, "path": a.path, "record_size": RECORD_SIZE,
             "producers": a.producers, "reads": a.readers, "operations_per_producer": a.operations,
             "append_interval": a.append_interval, "retry_interval": a.retry_interval,
             "payload": "fixed-width ASCII record", "consistency": "strict except labelled stale reads"}})
        c = Client(a.url, a.tenant, a.path, a.timeout, rng)
        operation(fp, lock, "setup", "create", "create", lambda: c.request("PUT", b"", {"Content-Type": "application/octet-stream"}))

        def producer(pid):
            local = Client(a.url, a.tenant, a.path, a.timeout, random.Random(a.seed + pid + 1))
            for seq in range(a.operations):
                data, oid = record(pid, seq, a.seed), f"p{pid}-{seq}"
                attempts = 0
                def attempt():
                    nonlocal attempts
                    attempts += 1
                    return local.request("POST", data, {"Content-Type": "application/octet-stream",
                        "producer-id": f"history-{a.seed}-{pid}", "producer-epoch": "0", "producer-seq": str(seq)})
                typ, value = operation(fp, lock, f"producer-{pid}", oid, "append", attempt)
                while typ == "unknown" and attempts <= a.retries:
                    time.sleep(a.retry_interval)
                    typ, value = operation(fp, lock, f"producer-{pid}", oid, "append-retry", attempt)
                value["record"] = data.decode("ascii")  # completion was already emitted; add authoritative metadata.
                emit(fp, lock, {"type": "info", "f": "record", "id": oid,
                                "value": {"record": data.decode("ascii"), "terminal": typ, "attempts": attempts}})
                if typ != "ok":
                    # Do not advance an unresolved sequence and turn a transient
                    # outage into permanent client-generated gap rejections.
                    return
                if a.append_interval > 0:
                    stop.wait(a.append_interval)

        def reader(rid):
            local = Client(a.url, a.tenant, a.path, a.timeout, random.Random(a.seed + 10000 + rid))
            n = 0
            while not stop.is_set():
                stale = a.stale_fraction > 0 and local.rng.random() < a.stale_fraction
                operation(fp, lock, f"reader-{rid}", f"r{rid}-{n}", "stale-read" if stale else "read",
                          lambda: local.request("GET", stale=stale))
                n += 1
                stop.wait(a.read_interval)

        with concurrent.futures.ThreadPoolExecutor(max_workers=a.producers + a.readers + 1) as ex:
            readers = [ex.submit(reader, i) for i in range(a.readers)]
            nemesis = ex.submit(run_nemesis, a, fp, lock) if a.nemesis != "none" else None
            futures = [ex.submit(producer, i) for i in range(a.producers)]
            for f in futures: f.result()
            stop.set()
            for f in readers: f.result()
            if nemesis: nemesis.result()
        operation(fp, lock, "final", "final-read", "read", lambda: c.request("GET"))
    return check_file(a.output)


def run_nemesis(a, fp, lock):
    time.sleep(a.nemesis_delay)
    hooks = dict(x.split("=", 1) for x in a.hook)
    defaults = {
        "leader-kill:start": "kubectl -n chronicle delete pod chronicle-0 --wait=false",
        "snapshot-crash:start": "kubectl -n chronicle exec chronicle-0 -- sh -c 'wget -qO- --post-data= http://127.0.0.1:8080/admin/snapshot/1; kill 1'",
        "node-drain:start": "kubectl drain k3d-chronicle-rust-agent-0 --ignore-daemonsets --delete-emptydir-data",
        "node-drain:heal": "kubectl uncordon k3d-chronicle-rust-agent-0",
    }
    phases = ["start", "heal"] if a.nemesis in ("minority-partition", "majority-partition", "drop-delay", "node-drain") else ["start"]
    for phase in phases:
        key, cmd = f"{a.nemesis}:{phase}", hooks.get(f"{a.nemesis}:{phase}", defaults.get(f"{a.nemesis}:{phase}"))
        if not cmd:
            raise RuntimeError(f"{key} needs --hook '{key}=COMMAND' (environment-specific network injection)")
        emit(fp, lock, {"type": "info", "f": "nemesis", "value": {"scenario": a.nemesis, "phase": phase, "command": cmd}})
        p = subprocess.run(cmd, shell=True, text=True, capture_output=True, timeout=a.hook_timeout)
        emit(fp, lock, {"type": "ok" if p.returncode == 0 else "fail", "f": "nemesis",
             "value": {"scenario": a.nemesis, "phase": phase, "returncode": p.returncode,
                       "stdout": p.stdout[-2000:], "stderr": p.stderr[-2000:]}})
        if p.returncode: raise RuntimeError(f"nemesis hook failed: {key}")
        if phase == "start" and len(phases) > 1: time.sleep(a.nemesis_duration)


def check_events(events):
    invokes, completions, records = {}, [], {}
    for i, e in enumerate(events):
        e["_line"] = i + 1
        if e.get("type") == "invoke": invokes[(e.get("process"), e.get("id"), e.get("f"))] = e
        elif e.get("f") == "record": records[e["id"]] = e["value"]
        elif e.get("type") in ("ok", "fail", "unknown"):
            # retry function shares the logical id but has its own invocation.
            inv = invokes.get((e.get("process"), e.get("id"), e.get("f")))
            if inv: completions.append((inv, e))
    strict = [(a, b) for a, b in completions if b.get("type") == "ok" and b.get("f") == "read"]
    errors = {"duplicate": [], "committed-prefix": [], "acked-retention": [], "linearizability": []}
    reads = []
    for inv, done in strict:
        rs = done.get("value", {}).get("records")
        if rs is None:
            errors["committed-prefix"].append(f"line {done['_line']}: undecodable successful read")
            continue
        if len(rs) != len(set(rs)): errors["duplicate"].append(f"line {done['_line']}: duplicate record")
        reads.append((inv, done, rs))
    canonical = max((x[2] for x in reads), key=len, default=[])
    for _, done, rs in reads:
        if canonical[:len(rs)] != rs: errors["committed-prefix"].append(f"line {done['_line']}: read is not a committed prefix")
    positions = {r: i for i, r in enumerate(canonical)}
    appends = []
    for oid, meta in records.items():
        rec, terminal = meta["record"], meta["terminal"]
        matching = [(x, y) for x, y in completions if y.get("id") == oid and y.get("f") in ("append", "append-retry")]
        if matching:
            start = min(x["time_ns"] for x, _ in matching)
            successes = [y["time_ns"] for _, y in matching if y["type"] == "ok"]
            # A transport timeout does not bound the server's possible execution interval.
            end = min(successes) if successes else float("inf")
            appends.append((oid, rec, terminal, start, end))
    for oid, rec, terminal, start, end in appends:
        if terminal == "ok":
            for inv, done, rs in reads:
                if end < inv["time_ns"] and rec not in rs:
                    errors["acked-retention"].append(f"{oid} absent from later read line {done['_line']}")
        # Unknown writes are allowed either absent or present; if present they participate in ordering.
        if rec in positions:
            for _, other, _, ostart, oend in appends:
                if other in positions and end < ostart and positions[rec] >= positions[other]:
                    errors["linearizability"].append(f"real-time order violated: {oid} before later append")
            for rinv, rdone, rs in reads:
                if end < rinv["time_ns"] and rec not in rs:
                    errors["linearizability"].append(f"{oid} completed before read line {rdone['_line']}")
                if start > rdone["time_ns"] and rec in rs:
                    errors["linearizability"].append(f"{oid} appears before invocation")
    if not reads:
        errors["acked-retention"].append("no successful strict read; retention was not tested")
    return {"valid": not any(errors.values()), "checks": {k: {"valid": not v, "errors": v} for k, v in errors.items()},
            "strict_reads": len(reads), "canonical_records": len(canonical), "note": "smoke/prefix checker, not general linearizability; unknown appends remain pending through history end"}


def check_file(path):
    with open(path, encoding="utf-8") as f: events = [json.loads(x) for x in f if x.strip()]
    result = check_events(events)
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command", required=True)
    c = sub.add_parser("check"); c.add_argument("history")
    r = sub.add_parser("run")
    r.add_argument("--url", action="append", required=True); r.add_argument("--seed", type=int, required=True)
    r.add_argument("--output"); r.add_argument("--tenant", default="jepsen"); r.add_argument("--path", default="history")
    r.add_argument("--producers", type=int, default=4); r.add_argument("--readers", type=int, default=2)
    r.add_argument("--operations", type=int, default=100); r.add_argument("--retries", type=int, default=3)
    r.add_argument("--timeout", type=float, default=10); r.add_argument("--read-interval", type=float, default=.05)
    r.add_argument("--append-interval", type=float, default=0, help="per-producer pause after each logical append, including retries")
    r.add_argument("--retry-interval", type=float, default=.1, help="pause before retrying an unknown append; exhaustion stops that producer")
    r.add_argument("--stale-fraction", type=float, default=0)
    r.add_argument("--nemesis", choices=["none", "leader-kill", "minority-partition", "majority-partition", "drop-delay", "snapshot-crash", "node-join", "node-drain"], default="none")
    r.add_argument("--nemesis-delay", type=float, default=2); r.add_argument("--nemesis-duration", type=float, default=5)
    r.add_argument("--hook", action="append", default=[]); r.add_argument("--hook-timeout", type=float, default=120)
    a = p.parse_args()
    if a.command == "check": return 0 if check_file(a.history)["valid"] else 1
    if not a.output: a.output = "history-{}-seed-{}.jsonl".format(datetime.datetime.now().strftime("%Y%m%dT%H%M%S"), a.seed)
    print(f"history: {a.output}", file=sys.stderr)
    return 0 if run_workload(a)["valid"] else 1


if __name__ == "__main__": raise SystemExit(main())
