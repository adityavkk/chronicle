"""Summarize retained local samples without merging unlike measurement windows.

Write cells expose exact client measure timestamps. Other pinned client modes
do not: their resource samples use the outer client invocation, explicitly so
labelled. Socket deltas omit traffic before the first/after the last sample of
each connection and are lower bounds. RSS excludes the shared kernel page cache.
"""
import gzip
import hashlib
import json
from pathlib import Path
import re
import sys


def summarize_phase_timings(observations, begin, end):
    """Difference cumulative counters; never sum successive cumulative samples."""
    phases = {}
    for observation in observations:
        stats = observation["cumulative"]
        assert sum(stats["buckets"]) == stats["count"], "inconsistent timing histogram"
        phases.setdefault(observation["phase"], []).append(observation)
    result = {}
    for phase, values in phases.items():
        reset = any(any(b["cumulative"][k] < a["cumulative"][k] for k in ("count", "total_ns", "bytes"))
                    for a, b in zip(values, values[1:]))
        row = dict(counter_reset_detected=reset, observations=len(values),
                   whole_invocation=None if reset else values[-1]["cumulative"], sampled_window=None)
        inside = [v for v in values if begin <= v["unix_ms"] <= end]
        if not reset and len(inside) >= 2:
            first, last = inside[0], inside[-1]
            delta = {k:last["cumulative"][k]-first["cumulative"][k] for k in ("count", "total_ns", "bytes")}
            delta["buckets"] = [b-a for a,b in zip(first["cumulative"]["buckets"], last["cumulative"]["buckets"])]
            assert sum(delta["buckets"]) == delta["count"] and all(n >= 0 for n in delta["buckets"])
            # A lifetime maximum cannot be differenced into a window maximum.
            row["sampled_window"] = dict(start_unix_ms=first["unix_ms"], end_unix_ms=last["unix_ms"], **delta)
        result[phase] = row
    return result


def summarize_progress(samples, begin, end):
    nodes, groups = {}, {}
    for sample in samples:
        for observation in sample.get("replicas", []):
            when = observation["end_unix_ms"]
            if not begin <= observation["unix_ms"] <= when <= end:
                continue
            node = str(observation["node"])
            row = nodes.setdefault(node, dict(heads=[], errors=0))
            row["errors"] += int("error" in observation or observation.get("head_status") != 200)
            if "committed_bytes" in observation:
                row["heads"].append(dict(unix_ms=when, bytes=observation["committed_bytes"],
                                        collection_ms=when-observation["unix_ms"]))
            for group, metrics in enumerate(observation.get("groups", [])):
                if metrics.get("last_log_index") is None or metrics.get("last_applied") is None:
                    continue
                lag = metrics["last_log_index"]-metrics["last_applied"]["index"]
                state = groups.setdefault(f"{node}/{group}", dict(unapplied_entries=[], matched_gap_entries=[], snapshots=set()))
                state["unapplied_entries"].append(lag)
                for matched in (metrics.get("replication") or {}).values():
                    if matched is not None:
                        state["matched_gap_entries"].append(metrics["last_log_index"]-matched["index"])
                snapshot = metrics.get("snapshot")
                if snapshot is not None:
                    state["snapshots"].add(snapshot["index"])
    for row in nodes.values():
        heads = row.pop("heads")
        row["samples"] = len(heads)
        if heads:
            row.update(first=heads[0], last=heads[-1], max_collection_ms=max(h["collection_ms"] for h in heads))
        if len(heads) >= 2:
            seconds = (heads[-1]["unix_ms"]-heads[0]["unix_ms"])/1000
            if seconds > 0:
                row["committed_bytes_per_second"] = (heads[-1]["bytes"]-heads[0]["bytes"])/seconds
    for state in groups.values():
        for key in ("unapplied_entries", "matched_gap_entries"):
            values = state[key]
            state[key] = dict(samples=len(values), first=values[0], last=values[-1],
                              minimum=min(values), maximum=max(values)) if values else None
        state["observed_snapshot_indices"] = sorted(state.pop("snapshots"))
    return dict(nodes=nodes, groups=groups,
        scope="Committed HEAD byte rates between individual observation completions, not exact client-window acks. "
              "Raft entry gaps are not commands or bytes, and last_log_index is not a commit index. "
              "Sequential node samples cannot establish an instantaneous cross-node gap. Missed peaks are possible.")


def summarize(directory):
    root = Path(directory)
    provenance = json.loads((root / "provenance.json").read_text())
    rows = []
    for result_file in sorted(root.glob("*/result.json")):
        cell = result_file.parent
        result = json.loads(result_file.read_text())
        client_file = cell / "client.json"
        raw = json.loads(client_file.read_text()) if client_file.exists() and client_file.stat().st_size else {}
        row = dict(cell=cell.name, arm=result["arm"], workload=result["workload"], profile=result["profile"],
                   verdict=result["verdict"], error=result.get("error"), raw_client=raw)
        outer = result.get("client_window",{})
        begin = raw.get("measure_start_unix_ms",outer.get("start_unix_ms",0))
        end = raw.get("measure_end_unix_ms",outer.get("end_unix_ms",0))
        row["sample_window"] = dict(start_unix_ms=begin,end_unix_ms=end,
            scope="exact client measurement" if "measure_start_unix_ms" in raw else "outer client invocation; includes non-measure work")
        nodes = set(map(str,result.get("node_pids",[])))
        first, last, sockets = {}, {}, {}
        peaks = dict(sut_rss_bytes=0,client_rss_bytes=0)
        window = []
        progress = []
        samples = cell / "samples.jsonl.gz"
        if samples.exists():
            with gzip.open(samples,"rt") as file:
                for line in file:
                    sample = json.loads(line)
                    if not begin <= sample["unix_ms"] <= end:
                        continue
                    window.append(sample["unix_ms"])
                    if "replicas" in sample:
                        progress.append(sample)
                    for pid, process in sample["processes"].items():
                        first.setdefault(pid,process)
                        last[pid] = process
                    for name, selected in [("sut_rss_bytes",nodes),("client_rss_bytes",{str(result.get("client_pid"))})]:
                        value = sum(p["rss_pages"]*provenance["page_bytes"] for pid,p in sample["processes"].items() if pid in selected)
                        peaks[name] = max(peaks[name],value)
                    for socket in sample["sockets"]:
                        if socket["replication"]:
                            key = (socket["local"],socket["peer"],tuple(socket["owners"]))
                            sent = socket["counters"].get("bytes_sent",0)
                            initial, maximum = sockets.setdefault(key,(sent,sent))
                            sockets[key] = (initial,max(maximum,sent))
        resources = dict(peak_sampled=peaks,samples=len(window),
                         observed_replication_sent_bytes_lower_bound=sum(b-a for a,b in sockets.values()))
        if len(window) >= 2:
            seconds = (window[-1]-window[0])/1000
            resources.update(sampled_seconds=seconds,
                average_sut_cpu_cores=sum(last[p]["cpu_ticks"]-first[p]["cpu_ticks"] for p in nodes if p in first)/provenance["cpu_hz"]/seconds,
                sut_write_bytes=sum(last[p]["io"]["write_bytes"]-first[p]["io"]["write_bytes"] for p in nodes if p in first),
                sut_read_bytes=sum(last[p]["io"]["read_bytes"]-first[p]["io"]["read_bytes"] for p in nodes if p in first))
        row["resources"] = resources
        if result.get("progress_sampling"):
            row["progress"] = summarize_progress(progress, begin, end)
            row["drain"] = result.get("drain")
        storage_file = cell / "storage-bytes.json"
        if storage_file.exists():
            storage = json.loads(storage_file.read_text())
            row["final_storage_bytes"] = dict(total=sum(storage.values()),
                wal=sum(v for k,v in storage.items() if k.endswith(".wal")),
                snapshots=sum(v for k,v in storage.items() if Path(k).name.startswith("snapshot-")))
        if result.get("diagnostics") and raw.get("ok_total_all_phases"):
            # Both pinned engines print WAL deltas divided by the CONFIGURED
            # interval (exactly 1s here), so these sums recover counter totals.
            # SRV_STATS uses actual elapsed time instead: do not sum its rates
            # and call them append counts. Use the client's all-phase acks.
            staged, syncs = 0, 0
            phase_timings = {}
            for log in cell.glob("node-*.log.gz"):
                observations = []
                with gzip.open(log, "rt") as file:
                    for line in file:
                        match = re.search(r"WAL_CONT staged/s=(\d+) fsync/s=(\d+)", line)
                        if match:
                            staged += int(match[1])
                            syncs += int(match[2])
                        if line.startswith("RAFT_TIMING "):
                            observations.append(json.loads(line.removeprefix("RAFT_TIMING ")))
                if observations:
                    phase_timings[log.name] = summarize_phase_timings(observations, begin, end)
            if phase_timings:
                row["phase_timings"] = dict(nodes=phase_timings,
                    scope="Cumulative completed-attempt wall times, including setup/warmup for whole_invocation. "
                          "Sampled windows use first/last observations INSIDE the client window, not exact client boundaries. "
                          "Nested/overlapping phases are NOT additive CPU or pure fsync syscall time.")
            acknowledgements = raw["ok_total_all_phases"]
            row["wal_diagnostics"] = dict(staged_records=staged, fsyncs=syncs,
                client_acks_all_phases=acknowledgements,
                fsyncs_per_ack=syncs/acknowledgements, records_per_ack=staged/acknowledgements,
                final_counter_interval_captured=result.get("diagnostic_tail_captured", result["verdict"] == "PASS"),
                scope="aggregate native WAL counters over whole invocation, including setup/warmup; not measure-window or all filesystem fsyncs. "
                      "If final_counter_interval_captured is false, counters and ratios are lower bounds only.")
        rows.append(row)
    output = dict(provenance_sha256=hashlib.sha256((root/"provenance.json").read_bytes()).hexdigest(),
                  analyzer_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), rows=rows,
                  scope="single-host resource qualification; profile cells are perturbed and never capacity rows")
    (root/"summary.json").write_text(json.dumps(output,indent=2)+"\n")
    for row in rows:
        if not row["profile"]:
            resource = row["resources"]
            print(row["arm"],row["workload"],row["verdict"],
                  "cpu",round(resource.get("average_sut_cpu_cores",0),2),
                  "RSS MiB",round(resource["peak_sampled"]["sut_rss_bytes"]/2**20,1),
                  "replication MiB lower bound",round(resource["observed_replication_sent_bytes_lower_bound"]/2**20,1))


if __name__ == "__main__":
    summarize(sys.argv[1])
