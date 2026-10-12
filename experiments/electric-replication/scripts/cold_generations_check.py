"""Offline checks for the paced generation probe; these are not ds-bench metrics.

The public fixture grammar fixes every byte before the server runs. Histories,
HTTP hashes/offsets, filesystem observations and S3 events are cross-checked;
driver PASS alone is never sufficient. Finite reclamation is not boundedness.
"""
from collections import Counter
import copy
import hashlib
import json
import math
from pathlib import Path
import sys

from benchmark_summary import check_clock
from cold_benchmark_check import listing_key, object_window, read_lines, read_object_events, resources


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return None
    return dict(count=len(ordered), p50=ordered[math.ceil(len(ordered)*0.5)-1],
                p99=ordered[math.ceil(len(ordered)*0.99)-1], max=ordered[-1])


def validate(result, history, objects, samples):
    errors = []
    writes = result.get("writes", [])
    if not writes or [w["seq"] for w in writes] != list(range(len(writes))):
        errors.append("missing or reordered append sequence")
    expected_outcome = "accepted" if result["arm"] == "async3" else "committed"
    expected_status = (202,) if result["arm"] == "async3" else (200, 204)
    if any(w.get("outcome") != expected_outcome or w.get("status") not in expected_status
           or w.get("end_ns", 0) < w["start_ns"] for w in writes):
        errors.append("write rejection, unknown, wrong acknowledgement mode or clock")
    keys = ("seq", "start_ns", "end_ns", "outcome", "status")
    observed = [tuple(w.get(k) for k in keys) for w in history if w["op"] == "paced-append"]
    if observed != [tuple(w.get(k) for k in keys) for w in writes]:
        errors.append("result and append history disagree")
    target = len(writes)*256
    digest = hashlib.sha256()
    for seq in range(len(writes)):
        digest.update(seq.to_bytes(8, "big"))
        digest.update(hashlib.sha256(f"paced-{seq}".encode()).digest())
        digest.update(bytes([seq % 251])*216)
    drains, probes = result.get("live_drain", []), result.get("live_probes", [])
    nodes = set(range(1, result["replicas"]+1))
    if ({d["node"] for d in drains} != nodes or len(drains) != len(nodes)
            or any(d["bytes"] != target or d["offsets"] != [target] for d in drains)):
        errors.append("all-replica committed drain mismatch")
    if ({p["node"] for p in probes} != nodes or len(probes) != len(nodes)
            or any(p["bytes"] != target or p["offset"] != target or p["status"] != 200
                   or p["sha256"] != digest.hexdigest() for p in probes)):
        errors.append("live byte/offset/sequence mismatch")
    if [h["sha256"] for h in history if h["op"] == "live-read"] != [p["sha256"] for p in probes]:
        errors.append("live read history disagreement")
    cycles = result.get("cycles", [])
    if [c["generation"] for c in cycles] != list(range(result["generations"])):
        errors.append("missing generation")
    if [h["generation"] for h in history if h["op"] == "generation-complete"] != list(range(result["generations"])):
        errors.append("missing completed generation history")
    remote_bytes = 0
    for cycle in cycles:
        generation = cycle["generation"]
        expected_names = {f"generation-{generation}-{n}" for n in range(4)}
        reads = cycle["reads"]
        if (len(reads) != len(nodes)*4
                or {(r["node"], r["name"]) for r in reads} != {(n, s) for n in nodes for s in expected_names}):
            errors.append("incomplete generation reads")
        for read in reads:
            expected = hashlib.sha256(b"".join(hashlib.sha256(
                f"cold-bench-v1:{read['name']}:{i}".encode()).digest() for i in range(4096))).hexdigest()
            if (read["status"] != 200 or read["sha256"] != expected
                    or read["bytes"] != 131072 or read["offset"] != 131072):
                errors.append("generation byte/offset mismatch")
        if cycle["collected_ns"] < cycle["delete_end_ns"]:
            errors.append("collection clock reversed")
        if result["cold"]:
            pre = cycle["precondition"]
            mapped = {listing_key(k, result["arm"] == "native") for k in pre["remote_keys"]}
            if (not mapped or mapped != set(pre["listing_keys"])
                    or pre["local_payload_bytes"] or pre["local_segment_bytes"]):
                errors.append("cold precondition not proven")
            keys = {"/"+result["object_bucket"]+"/"+k for k in mapped}
            window = cycle["read_window"]
            fetched = object_window(objects, window["start_ns"], window["end_ns"], keys)
            if fetched["errors"] or fetched["client_remote_fetched_bytes"] < len(nodes)*4*131072:
                errors.append("no complete attributable remote reads")
            remote_bytes += fetched["client_remote_fetched_bytes"]
            # Every retired payload needs a successful external delete after
            # the application DELETE, not an aggregate GC counter increment.
            deleted = {e["path"] for e in objects if e.get("op") == "s3" and e["method"] == "DELETE"
                       and e["status"] == 204 and cycle["delete_start_ns"] <= e["time_ns"] <= cycle["collected_ns"]}
            if not keys.issubset(deleted):
                errors.append("retired object has no observed completed deletion")
        if "pin_release_ns" in cycle:
            if (cycle["pin_before_delete"]["oldest_pin"] is None
                    or cycle["pin_release_ns"]-cycle["delete_end_ns"] < 7_000_000_000
                    or cycle["collected_ns"] < cycle["pin_release_ns"]):
                errors.append("reader pin interval not exercised")
            held = [e for e in objects if e.get("op") == "held"
                    and cycle["start_ns"] <= e["time_ns"] <= cycle["delete_start_ns"]]
            if len(held) != 1:
                errors.append("missing unique held remote request")
            else:
                key, still_held = held[0]["path"], cycle["pin_held_state"]["time_ns"]
                if any(e.get("op") == "s3" and e["method"] == "DELETE" and e["status"] == 204
                       and e["path"] == key and cycle["delete_start_ns"] <= e["time_ns"] <= still_held for e in objects):
                    errors.append("reader object deleted while pinned")
                if not any(e.get("op") == "s3" and e["method"] == "GET" and e["status"] in (200, 206)
                           and e["path"] == key and e.get("fault") == "hold" and e["start_ns"] >= still_held for e in objects):
                    errors.append("held reader never fetched its protected object")
    should_pin = result["cold"] and result["arm"] != "native"
    if sum("pin_release_ns" in c for c in cycles) != int(should_pin):
        errors.append("missing or unexpected reader pin")
    if result["cold"]:
        final_objects = result.get("reclamation_objects", [])
        if result["arm"] == "native":
            if final_objects:
                errors.append("native payloads not reclaimed")
        else:
            states = result.get("reclamation_states", [])
            if (len(states) != result["replicas"]*2
                    or any(s["live_objects"] or s["pending"] or s["oldest_pin"] is not None for s in states)
                    or len(final_objects) > 4 or any("/i/" not in o["key"] for o in final_objects)):
                errors.append("empty cluster retained payloads, pending operations or excess roots")
    if result.get("writer_error") or result.get("sampling_error") or check_clock(samples)["status"] != "STABLE":
        errors.append("background failure or unstable observation clock")
    faults = [e for e in objects if e.get("op") != "s3"]
    if len(faults) != int(should_pin) or any(e["op"] != "held" for e in faults):
        errors.append("unexpected object fault or missing hold")
    actual_errors = [e for e in objects if e.get("op") == "s3" and e["status"] >= 400
                     and not (e["method"] == "HEAD" and e["status"] == 404)]
    if actual_errors:
        errors.append("object HTTP failures")
    return dict(verdict="FAIL" if errors else "PASS", errors=sorted(set(errors)),
                writes=len(writes), read_remote_bytes=remote_bytes, cycles=len(cycles))


def analyze(path):
    result = json.loads((path/"result.json").read_text())
    if result["verdict"] != "PASS":
        return dict(cell=path.name, verdict="FAIL", errors=[result.get("error", "driver failed")])
    samples = read_lines(path/"samples.jsonl.gz")
    objects = read_object_events(path)
    history = read_lines(path/"history.jsonl.gz")
    checked = validate(result, history, objects, samples)
    writes = result["writes"]
    states = [s["state"] for s in samples]
    cycles = result["cycles"]
    checked.update(cell=path.name, arm=result["arm"], cold=result["cold"],
        elapsed_seconds=(result["window"]["end_ns"]-result["window"]["start_ns"])/1e9,
        completion_ops_per_second=len(writes)/((writes[-1]["end_ns"]-writes[0]["start_ns"])/1e9),
        latency_ms=distribution((w["end_ns"]-w["start_ns"])/1e6 for w in writes),
        schedule_lateness_ms=distribution((w["start_ns"]-w["due_monotonic_ns"])/1e6 for w in writes),
        gc_seconds=distribution((c["collected_ns"]-c["delete_end_ns"])/1e9 for c in cycles),
        gc_unpinned_seconds=distribution((c["collected_ns"]-c["delete_end_ns"])/1e9 for c in cycles if "pin_release_ns" not in c),
        gc_pinned_seconds=distribution((c["collected_ns"]-c["delete_end_ns"])/1e9 for c in cycles if "pin_release_ns" in c),
        generation_overrun_seconds=distribution(c["overrun_secs"] for c in cycles),
        resources=resources(samples, result["window"], result["roles"], result["cpu_hz"], result["page_bytes"]),
        peak_objects=max(s["object_store"]["count"] for s in states),
        peak_object_bytes=max(s["object_store"]["bytes"] for s in states),
        peak_local_allocated_bytes=max(sum(n["allocated"] for n in s["storage"].values()) for s in states),
        final_objects=result["final_state"]["object_store"],
        gc_cycle_objects=[c["after_collection"]["object_store"] for c in cycles],
        peak_pending_replicated_descriptors=max(sum(len(g.get("cold", {}).get("pending", {}))
            for g in s.get("groups", {}).values()) for s in states),
        methods=dict(Counter(e["method"] for e in objects if e.get("op") == "s3")))
    # HEADs and ack observations are non-atomic. Bracket lag instead of
    # inventing exact replication latency or treating accepted bytes as applied.
    gaps = []
    for sample in samples:
        for head in sample["live_committed"]:
            gaps.append(dict(node=head["node"], time_ns=sample["monotonic_ns"],
                lower=max(0, sample["accepted_before"]-head["bytes"]//256),
                upper=max(0, sample["accepted_after"]+1-head["bytes"]//256)))
    checked["sampled_commit_gap_max_appends"] = dict(lower=max(g["lower"] for g in gaps), upper=max(g["upper"] for g in gaps))
    if checked["verdict"] != "PASS":
        checked["negative_controls_status"] = "NOT_RUN: unmodified evidence already fails"
        return checked
    # Mutate actual real-process evidence, not an all-green toy fixture.
    mutations = {
        "missing-drain": lambda r: r["live_drain"].pop(),
        "corrupt-live": lambda r: r["live_probes"][0].update(sha256="corrupt"),
        "corrupt-generation": lambda r: r["cycles"][0]["reads"][0].update(offset=131071),
        "missing-generation": lambda r: r["cycles"].pop(),
        "unknown-write": lambda r: r["writes"][0].update(outcome="unknown"),
    }
    if result["cold"]:
        mutations["local-not-reclaimed"] = lambda r: r["cycles"][0]["precondition"].update(local_payload_bytes=1)
        mutations["wrong-remote-key"] = lambda r: r["cycles"][0]["precondition"].update(listing_keys=["wrong"])
        mutations["retained-payload"] = lambda r: r["reclamation_objects"].append(dict(key="retained/p/payload", bytes=131072))
    if result["cold"] and result["arm"] != "native":
        mutations["no-reader-pin"] = lambda r: next(c for c in r["cycles"] if "pin_release_ns" in c)["pin_before_delete"].update(oldest_pin=None)
    controls = {}
    for name, mutation in mutations.items():
        altered = copy.deepcopy(result)
        mutation(altered)
        controls[name] = validate(altered, history, objects, samples)["verdict"] == "FAIL"
    if result["cold"]:
        cycle = cycles[0]
        key = "/"+result["object_bucket"]+"/"+cycle["precondition"]["listing_keys"][0]
        def target(event):
            return event.get("op") == "s3" and event["method"] == "DELETE" and event["path"] == key
        missing = [e for e in objects if not target(e)]
        controls["missing-completed-delete"] = "retired object has no observed completed deletion" in validate(
            result, history, missing, samples)["errors"]
        late = copy.deepcopy(objects)
        for event in late:
            if target(event):
                event["time_ns"] = cycle["collected_ns"]+1
        controls["collection-before-completion"] = "retired object has no observed completed deletion" in validate(
            result, history, late, samples)["errors"]
    if result["cold"] and result["arm"] != "native":
        cycle = next(c for c in cycles if "pin_release_ns" in c)
        key = next(e["path"] for e in objects if e.get("op") == "held")
        altered = copy.deepcopy(objects)
        deletion = next(e for e in altered if e.get("op") == "s3" and e["method"] == "DELETE" and e["path"] == key)
        deletion.update(start_ns=cycle["pin_held_state"]["time_ns"]-2, time_ns=cycle["pin_held_state"]["time_ns"]-1)
        controls["early-pinned-delete"] = "reader object deleted while pinned" in validate(result, history, altered, samples)["errors"]
    checked["negative_controls"] = controls
    if not all(controls.values()):
        checked["verdict"] = "FAIL"
        checked["errors"].append("negative control escaped")
    return checked


if __name__ == "__main__":
    root = Path(sys.argv[1])
    rows = [analyze(path.parent) for path in sorted(root.glob("*/result.json"))]
    result = dict(passed=sum(r["verdict"] == "PASS" for r in rows), total=len(rows), cells=rows,
        checker_sources={name:hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                         for name in ("cold_generations_check.py", "cold_benchmark_check.py", "benchmark_summary.py")})
    (root/"checked.json").write_text(json.dumps(result, indent=2)+"\n")
    print(json.dumps(dict(passed=result["passed"], total=len(rows),
                         failures=[r for r in rows if r["verdict"] != "PASS"]), indent=2))
    raise SystemExit(0 if rows and result["passed"] == len(rows) else 1)
