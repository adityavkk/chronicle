"""Independent checks for cold benchmark evidence, not a server implementation.

An S3 GET is not automatically a client read. Only immutable, fully published
preconditioning keys, during an otherwise quiescent client invocation, qualify.
New-key verification, inventories, recovery and probes are excluded. No request
spanning a window boundary is assigned an invented partial byte count.
"""
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import re
import statistics
import subprocess
import sys

from benchmark_summary import check_clock, summarize_write_outcomes


def read_lines(path):
    path = Path(path)
    if not path.exists() and path.suffix == ".gz":
        path = path.with_suffix(".zst")
    if path.suffix == ".zst":
        with subprocess.Popen(["zstd", "-dq", "-c", "--", str(path)], stdout=subprocess.PIPE, text=True) as process:
            try:
                rows = [json.loads(line) for line in process.stdout if line.strip()]
            finally:
                process.stdout.close()
            if process.wait():
                raise ValueError("zstd evidence decoding failed")
        return rows
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt") as source:
        return [json.loads(line) for line in source if line.strip()]


def read_object_events(path):
    files = sorted(Path(path).glob("objects-*.jsonl.*"))
    if len({p.stem for p in files}) != len(files) or any(p.suffix not in (".gz", ".zst") for p in files):
        raise ValueError("ambiguous object evidence encoding")
    return [event for file in files for event in read_lines(file)]


def listing_key(key, native):
    # object_store 0.11.2 Path::from encodes ~, preserves +, then the S3
    # transport URL-encodes again. Restrict to this harness's generated grammar;
    # never recursively unquote opaque keys (a%7Eb and a~b are different).
    if not re.fullmatch(r"[A-Za-z0-9_+./~-]+", key) or any(not p or p in (".","..") for p in key.split("/")):
        raise ValueError("key outside the benchmark's canonical grammar")
    return key.replace("~","%7E") if native else key


def object_window(events, start, end, client_keys):
    if end <= start:
        raise ValueError("invalid measurement window")
    methods, statuses = Counter(), Counter()
    client_bytes = client_gets = uploaded = downloaded = boundary = errors = 0
    for event in events:
        if event.get("op") != "s3":
            if start <= event["time_ns"] <= end:
                errors += 1
            continue
        if event["time_ns"] < event["start_ns"]:
            raise ValueError("object clock reversed")
        if event["time_ns"] < start or event["start_ns"] > end:
            continue
        if event["start_ns"] < start or event["time_ns"] > end:
            boundary += 1
            continue
        method, status = event["method"], event["status"]
        methods[method] += 1
        statuses[str(status)] += 1
        uploaded += event["request_bytes"]
        downloaded += event["bytes"]
        # A missing object HEAD is part of upstream offload existence checks.
        if status >= 400 and not (method == "HEAD" and status == 404):
            errors += 1
        if method == "GET" and status in (200, 206) and event["path"] in client_keys:
            client_gets += 1
            client_bytes += event["bytes"]
    return dict(methods=dict(methods), statuses=dict(statuses), request_body_bytes=uploaded,
                response_body_bytes=downloaded, client_payload_gets=client_gets,
                client_remote_fetched_bytes=client_bytes, boundary_requests_excluded=boundary,
                errors=errors, scope="whole completed S3 requests inside client invocation; remote fetch bytes, not HTTP emitted bytes")


def validate(result, raw, objects, samples):
    errors = []
    clock = check_clock(samples)
    if clock["status"] != "STABLE":
        errors.append("unstable or insufficient paired clock samples")
    if result.get("client_exit_code") != 0:
        errors.append("client did not exit successfully")
    workload = result["workload"]
    counts = [raw[k] for k in ("counts", "write_counts", "read_counts", "append_counts") if k in raw]
    if not counts or any(c["other_err"] or c["backpressure"] for c in counts):
        errors.append("client errors or rejections; not a zero-error operating point")
    if raw.get("subscriber_errors", 0) or raw.get("lazy_creates", 0):
        errors.append("subscriber errors or lazy stream creation")
    if not result.get("drain") or not all(r["bytes"] == result["target_bytes"] for r in result["drain"]):
        errors.append("all-replica committed drain mismatch")
    if len(result.get("drain", [])) != result["replicas"]:
        errors.append("missing replica drain")
    nodes = set(range(1,result["replicas"]+1))
    if {r["node"] for r in result.get("drain", [])} != nodes:
        errors.append("wrong or duplicated drain identities")
    if result.get("drain") and any(r["offsets"]!=result["drain"][0]["offsets"] for r in result["drain"]):
        errors.append("replicas disagree on per-stream committed offsets")
    probes = result.get("probes", [])
    if len(probes) != result["replicas"] * result["streams"]:
        errors.append("missing independent byte probes")
    names = {p["name"] for p in probes}
    if len(names)!=result["streams"] or {(p["node"],p["name"]) for p in probes}!={(n,s) for n in nodes for s in names}:
        errors.append("wrong or duplicated probe identities")
    hashes = {}
    for probe in probes:
        hashes.setdefault(probe["name"],set()).add(probe["sha256"])
    if any(len(values)!=1 for values in hashes.values()):
        errors.append("replicas disagree on exact stream bytes")
    if any(p["sha256"] != p["expected_sha256"] or p["offset"] != p["bytes"] for p in probes):
        errors.append("byte/offset mismatch")
    if workload == "replay" and raw.get("bytes_read_total") != raw["counts"]["ok"] * result["seed_bytes"]:
        errors.append("replay totals disagree with independently seeded bytes")
    if result.get("sampling_error"):
        errors.append("resource sampler failed")
    start, end = result["client_window"]["start_ns"], result["client_window"]["end_ns"]
    evidence = object_window(objects, start, end, set(result.get("client_object_keys", [])))
    if evidence["errors"]:
        errors.append("object service errors or unknowns in measurement window")
    if result["cold"]:
        pre = result["precondition"]
        if not pre["remote_keys"] or pre["local_payload_bytes"] or pre["local_segment_bytes"]:
            errors.append("cold precondition did not reclaim all seeded local payload")
        mapped = {listing_key(key,result["arm"]=="native") for key in pre["remote_keys"]}
        if mapped != set(pre["listing_keys"]):
            errors.append("published manifest does not match actual object keys")
        expected_keys={"/"+result["object_bucket"]+"/"+key for key in mapped} if result["seed_bytes"] else set()
        if expected_keys != set(result["client_object_keys"]):
            errors.append("client attribution contains keys outside its immutable precondition")
        if workload in ("replay", "mixed") and not evidence["client_remote_fetched_bytes"]:
            errors.append("no attributable client cold-read bytes in measurement")
    elif evidence["client_remote_fetched_bytes"]:
        errors.append("hot control unexpectedly used cold objects")
    if workload=="fanout":
        sequence=result.get("sequence")
        if not sequence:
            errors.append("missing independent fanout sequence probe")
        else:
            # Independent reconstruction of the public asymmetric byte fixture,
            # including every live record, rather than trusting driver hashes.
            digest=hashlib.sha256()
            for i in range(8192):digest.update(hashlib.sha256(f"cold-bench-v1:cold-sequence:{i}".encode()).digest())
            for i in range(32):digest.update(i.to_bytes(8,"big")+hashlib.sha256(f"live-{i}".encode()).digest())
            readers=sequence["readers"]
            if (len(readers)!=16 or {r["reader"] for r in readers}!=set(range(16))
                    or any(r["bytes"]!=263424 or not r["closed"] or r["sha256"]!=digest.hexdigest() for r in readers)):
                errors.append("fanout byte/sequence/closure mismatch")
            if not sequence["start_ns"] < sequence["catchup_ns"] < sequence["end_ns"]:
                errors.append("fanout observation clock invalid")
            if result["cold"]:
                pre=sequence["precondition"]
                if pre["local_payload_bytes"] or pre["local_segment_bytes"]:
                    errors.append("fanout catch-up was not reclaimed")
                mapped={listing_key(k,result["arm"]=="native") for k in pre["remote_keys"]}
                if mapped!=set(pre["listing_keys"]):errors.append("fanout object mapping invalid")
                keys={"/"+result["object_bucket"]+"/"+k for k in mapped}
                fetched=object_window(objects,sequence["start_ns"],sequence["end_ns"],keys)
                if fetched["errors"] or fetched["client_remote_fetched_bytes"]<16*262144:
                    errors.append("fanout lacks attributable remote catch-up")
    return dict(verdict="FAIL" if errors else "PASS", errors=errors, clock=clock, objects=evidence)


def resources(samples, window, roles, cpu_hz, page_bytes):
    # The filesystem/admin observation starts after all process samples. Use
    # that marker as a conservative upper bound, not a made-up instant.
    selected = [s for s in samples if window["start_ns"] <= s["monotonic_ns"]
                and s["state"]["time_ns"] <= window["end_ns"]]
    out = {}
    for role, pids in roles.items():
        values = []
        for s in selected:
            observations = [s["processes"][str(pid)] for pid in pids if str(pid) in s["processes"]]
            if len(observations) == len(pids):
                values.append((s["monotonic_ns"], observations))
        if len(values) < 2:
            out[role] = dict(status="INSUFFICIENT", complete_samples=len(values))
            continue
        seconds = (values[-1][0] - values[0][0])/1e9
        first, last = values[0][1], values[-1][1]
        out[role] = dict(status="MEASURED", complete_samples=len(values), sampled_seconds=seconds,
            cpu_cores=sum(b["cpu_ticks"]-a["cpu_ticks"] for a,b in zip(first,last))/cpu_hz/seconds,
            peak_rss_bytes=max(sum(p["rss_pages"] for p in ps)*page_bytes for _,ps in values),
            io_delta={k:sum(b["io"][k]-a["io"][k] for a,b in zip(first,last)) for k in first[0]["io"]})
    return out


def diagnostics(path, result, raw, samples, objects):
    window=result["client_window"]
    # Next iteration begins strictly after this iteration's admin/listing work.
    # Older captures lack an end marker, so exclude their unbounded last point.
    selected=[s for s,next_sample in zip(samples,samples[1:])
              if window["start_ns"]<=s["monotonic_ns"] and next_sample["monotonic_ns"]<=window["end_ns"]]
    states=[s["state"] for s in selected]
    sockets={}
    gaps={}
    for sample in selected:
        for socket in sample["sockets"]:
            if socket["replication"]:
                key=(socket["local"],socket["peer"],tuple(socket["owners"]))
                sent=socket["counters"].get("bytes_sent",0)
                first,last=sockets.setdefault(key,(sent,sent))
                sockets[key]=(first,max(last,sent))
        for key, group in sample["state"].get("groups",{}).items():
            metrics=group["raft"]
            if metrics.get("last_log_index") is not None and metrics.get("last_applied"):
                row=gaps.setdefault(key,dict(unapplied=[],uncommitted=[],matched_gap=[]))
                row["unapplied"].append(metrics["last_log_index"]-metrics["last_applied"]["index"])
                if metrics.get("committed"):
                    row["uncommitted"].append(metrics["last_log_index"]-metrics["committed"]["index"])
                row["matched_gap"].extend(metrics["last_log_index"]-m["index"] for m in (metrics.get("replication") or {}).values() if m)
    begin=raw.get("measure_start_unix_ms",window["start_unix_ms"])
    end=raw.get("measure_end_unix_ms",window["end_unix_ms"])
    stages={}
    for log in path.glob("node-*.log.gz"):
        with gzip.open(log,"rt") as source:
            values=[json.loads(line.removeprefix("RAFT_WRITE_OUTCOMES ")) for line in source if line.startswith("RAFT_WRITE_OUTCOMES ")]
        if values:
            stages[log.name]=summarize_write_outcomes(values,begin,end)
            inside=[v for v in values if begin<=v["unix_ms"]<=end]
            stages[log.name]["unapplied_locally_durable_commands"]=[dict(unix_ms=v["unix_ms"],
                commands=v["cumulative"].get("locally_durable_commands",0)-v["cumulative"].get("applied_commands",0)) for v in inside]
    garbage={"/"+result["object_bucket"]+"/"+k for k in result.get("garbage_keys",[])}
    deleted={e["path"]:e["time_ns"] for e in objects if e.get("op")=="s3" and e["method"]=="DELETE"
             and e["status"]==204 and e["path"] in garbage and e["time_ns"]>=result["gc_delete_window"]["start_ns"]}
    puts=[e for e in objects if e.get("op")=="s3" and e["method"]=="PUT" and e["status"]==200
          and "/i/" not in e["path"] and window["start_ns"]<=e["start_ns"]<=e["time_ns"]<=window["end_ns"]]
    startup=result["startup_resources"]["processes"]
    cold_progress={}
    if len(states)>=2:
        for key in states[-1].get("groups",{}):
            values=[s["groups"][key]["cold"] for s in states if "cold" in s["groups"][key]]
            if not values:continue
            first,last=values[0],values[-1]
            before,after=first["counters"],last["counters"]
            reset=any(after.get(k,0)<v for k,v in before.items())
            cold_progress[key]=dict(counter_reset_detected=reset,
                counter_delta=None if reset else {k:v-before.get(k,0) for k,v in after.items()},
                live_objects=dict(first=first["live_objects"],last=last["live_objects"]),
                pending=dict(first=len(first["pending"]),last=len(last["pending"])),
                local_payload_bytes=dict(first=first["local_payload_bytes"],last=last["local_payload_bytes"],
                                         maximum=max(v["local_payload_bytes"] for v in values)))
    return dict(startup_seconds=result["startup_seconds"],
        startup_rss_bytes={role:sum(startup[str(pid)]["rss_pages"] for pid in pids if str(pid) in startup)*result["page_bytes"]
                           for role,pids in result["roles"].items() if role!="client"},
        node_socket_sent_bytes_lower_bound=sum(last-first for first,last in sockets.values()),
        raft_entry_gaps={key:{field:dict(first=values[0],last=values[-1],maximum=max(values)) if values else None
                             for field,values in row.items()} for key,row in gaps.items()},
        stages=stages,cold_progress=cold_progress,
        peak_object_count=max((s["object_store"]["count"] for s in states),default=None),
        peak_object_bytes=max((s["object_store"]["bytes"] for s in states),default=None),
        peak_local_allocated_bytes=max((sum(n["allocated"] for n in s["storage"].values()) for s in states),default=None),
        peak_local_metadata_bytes=max((sum(n["metadata"] for n in s["storage"].values()) for s in states),default=None),
        peak_pending_replicated_descriptors=max((sum(len(g.get("cold",{}).get("pending",{})) for g in s.get("groups",{}).values()) for s in states),default=None),
        payload_upload_parts_in_window=len(puts),payload_upload_bytes_in_window=sum(e["request_bytes"] for e in puts),
        garbage_objects=len(garbage),observed_garbage_deletes=len(deleted),
        garbage_deletes_in_window=sum(t<=window["end_ns"] for t in deleted.values()),
        garbage_reclamation_seconds=(max(deleted.values())-result["gc_delete_window"]["start_ns"])/1e9 if garbage and set(deleted)==garbage else None,
        scope="Resources/S3 use outer invocation. Non-atomic admin observations finish before the next included sample. Log-stage counters include internal metadata, not HTTP acks; first/last samples inside reported client bounds. Entry gaps are not command/byte lag. Stage gaps include unresolved unknowns, not just backlog. Socket deltas miss closed sockets; sampled peaks can miss transients. Payload PUT classification excludes /i/ inventory keys in this fixture's grammar; parts are not publication acknowledgements. GC interval runs from first application DELETE to last completed remote DELETE.")


def summarize(root):
    rows = []
    for path in sorted(Path(root).glob("*/result.json")):
        row = json.loads(path.read_text())
        identity = dict(cell=path.parent.name, arm=row["arm"], cold=row["cold"],
                        workload=row["workload"], repetition=row["repetition"])
        raw_path = path.parent/"client.json"
        has_samples = any((path.parent/("samples.jsonl"+suffix)).exists() for suffix in (".gz", ".zst"))
        if not raw_path.exists() or "end_ns" not in row.get("client_window", {}) or not has_samples:
            rows.append(dict(identity, verdict="FAIL", error=row.get("error")))
            continue
        try:
            raw = json.loads(raw_path.read_text())
        except json.JSONDecodeError:
            rows.append(dict(identity, verdict="FAIL", error="no valid client output"))
            continue
        objects = read_object_events(path.parent)
        samples = read_lines(path.parent/"samples.jsonl.gz")
        checked = validate(row,raw,objects,samples)
        # Never turn an earlier failed assertion into a clean run.
        if row["verdict"] != "PASS":
            checked["verdict"] = "FAIL"
            checked["errors"].append(row.get("error", "driver failed"))
        counts = raw.get("counts",raw.get("write_counts",raw.get("append_counts")))
        latency = raw.get("latency_ms",raw.get("write_latency_ms",raw.get("fan_out_latency_ms",{})))
        secs = raw.get("drive_secs",raw.get("elapsed_secs"))
        pre = row["precondition"]
        # Full manifests and per-object descriptors remain in result.json;
        # copying them here obscures the measurements and bloats the summary.
        pre_summary = {k:pre[k] for k in ("time_ns", "local_payload_bytes", "local_segment_bytes")}
        pre_summary.update(remote_objects=len(pre["remote_keys"]), manifests=len(pre["manifests"]))
        rows.append(dict(identity,verdict=checked["verdict"],errors=checked["errors"],
            counts=counts,ops_per_sec=raw.get("aggregate_ops_per_sec",counts["ok"]/secs),
            bytes_per_sec=raw.get("bytes_per_sec"),latency=latency,
            write_latency=raw.get("write_latency_ms"),read_latency=raw.get("read_latency_ms"),
            objects=checked["objects"],resources=resources(samples,row["client_window"],row["roles"],row["cpu_hz"],row["page_bytes"]),
            diagnostics=diagnostics(path.parent,row,raw,samples,objects),
            drain_seconds=row.get("drain_seconds"),sequence=row.get("sequence"),
            precondition=pre_summary,
            after_client={k:v for k,v in row.get("after_client",{}).items()
                          if k in ("time_ns", "storage", "object_store", "garbage_objects_remaining")}))
    groups = {}
    for row in rows:
        if "arm" in row:
            groups.setdefault((row["arm"],row["cold"],row["workload"]),[]).append(row)
    aggregates = []
    for (arm,cold,workload),cells in groups.items():
        valid = [r for r in cells if r["verdict"] == "PASS"]
        aggregates.append(dict(arm=arm,cold=cold,workload=workload,cells=len(cells),passed=len(valid),
            median_ops_per_sec=statistics.median(r["ops_per_sec"] for r in valid) if valid else None,
            min_ops_per_sec=min((r["ops_per_sec"] for r in valid),default=None),
            max_ops_per_sec=max((r["ops_per_sec"] for r in valid),default=None)))
    result=dict(cells=rows,aggregates=aggregates,passed=sum(r["verdict"]=="PASS" for r in rows),total=len(rows),
                checker_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                helper_sha256=hashlib.sha256(Path(__file__).with_name("benchmark_summary.py").read_bytes()).hexdigest())
    (Path(root)/"summary.json").write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps(dict(passed=result["passed"],total=result["total"],aggregates=aggregates),indent=2))
    return result


if __name__ == "__main__":
    result = summarize(sys.argv[1])
    raise SystemExit(0 if result["total"] and result["passed"] == result["total"] else 1)
