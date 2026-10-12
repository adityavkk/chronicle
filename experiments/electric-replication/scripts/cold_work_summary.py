"""Recheck matched cells and keep baseline/candidate aggregates separate.

Run offline, not alongside a measurement. Original failed cells never turn into
passes, and medians include every available observation, not just green cells.
"""
from collections import defaultdict
from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import statistics
import sys

from cold_benchmark_check import read_lines, summarize


def trend(points):
    if len(points)<2:return None
    t0=points[0][0]
    xs=[(t-t0)/1e9 for t,_ in points]
    ys=[y for _,y in points]
    mx,my=statistics.mean(xs),statistics.mean(ys)
    divisor=sum((x-mx)**2 for x in xs)
    return sum((x-mx)*(y-my) for x,y in zip(xs,ys))/divisor if divisor else None


def distribution(values):
    values=[x for x in values if x is not None]
    return dict(observations=len(values),median=statistics.median(values),minimum=min(values),maximum=max(values)) if values else None


def run(root):
    root=Path(root)
    # Reuse all byte/offset/window checks, but do not present the historical
    # summarizer's non-variant aggregates as a before/after comparison.
    with redirect_stdout(io.StringIO()):
        result=summarize(root)
    groups=defaultdict(list)
    for row in result["cells"]:
        path=root/row["cell"]
        raw=json.loads((path/"result.json").read_text())
        row["variant"]=raw["variant"]
        groups[row["variant"],row["arm"],row["cold"],row["workload"]].append(row)
        window=raw.get("client_window",{})
        if "end_ns" not in window or "diagnostics" not in row:continue
        samples=read_lines(path/"samples.jsonl.gz")
        # Observation completion is bounded by the next sample, not its own
        # start timestamp. Both variants have the same observer RPC count.
        states=[s["state"] for s,n in zip(samples,samples[1:])
                if window["start_ns"]<=s["monotonic_ns"] and n["monotonic_ns"]<=window["end_ns"]]
        middle=(window["start_ns"]+window["end_ns"])/2
        node_trends={node:trend([(s["time_ns"],s["storage"][node]["payload"]) for s in states if s["time_ns"]>=middle])
                     for node in raw["after_client"]["storage"]}
        pressure=[p for s in states for p in s.get("pressure",{}).values() if p]
        eligibility={}
        for key in raw["after_client"].get("groups",{}):
            if not key.startswith("1-"):continue
            values=[(s["time_ns"],s["groups"][key]) for s in states
                    if "eligible_payload_bytes" in s["groups"][key].get("cold",{})]
            if len(values)<2 or any(g["raft"].get("current_leader")!=1 for _,g in values):continue
            first,last=values[0][1]["cold"],values[-1][1]["cold"]
            seconds=(values[-1][0]-values[0][0])/1e9
            published=last["counters"].get("published_payload_bytes",0)-first["counters"].get("published_payload_bytes",0)
            if published<0 or seconds<=0:raise ValueError("invalid eligibility observation interval")
            delta=last["eligible_payload_bytes"]-first["eligible_payload_bytes"]
            forced=last["counters"].get("pressure_seals",0)-first["counters"].get("pressure_seals",0)
            eligibility[key]=dict(sampled_seconds=seconds,first=first["eligible_payload_bytes"],last=last["eligible_payload_bytes"],
                maximum=max(g["cold"]["eligible_payload_bytes"] for _,g in values),
                published_bytes_per_sec=published/seconds,pressure_seals=forced,
                eligible_arrival_estimate_bytes_per_sec=(published+delta)/seconds if forced==0 else None)
        row["offload"]=dict(
            per_node_second_half_local_payload_slope_bytes_per_sec=node_trends,
            per_node_after_client_payload_bytes={n:s["payload"] for n,s in raw["after_client"]["storage"].items()},
            leader_full_or_closed_eligibility=eligibility,
            eligibility_scope="Candidate-only sampled full/closed tails, including pending work; pressure-forced short tails are not this gauge. Without pressure seals, arrival is estimated as published bytes plus gauge change over the same interval, not HTTP throughput. Nodes other than the direct leader do not increment its publication counter. Non-atomic observations, no deletions of the benchmark's live streams.",
            payload_upload_bytes_per_sec=row["diagnostics"]["payload_upload_bytes_in_window"]/((window["end_ns"]-window["start_ns"])/1e9),
            max_pending_per_group=max((len(g["cold"]["pending"]) for s in states for g in s.get("groups",{}).values()
                                       if g.get("cold") is not None and "pending" in g["cold"]),default=None),
            max_active_backlog_credit=max((p["active_credits"]["backlog"] for p in pressure),default=None),
            max_spent_backlog_credit=max((p["spent_credits"]["backlog"] for p in pressure),default=None),
            max_unapplied_bytes=max((p["unapplied_bytes"] for p in pressure),default=None),
            minimum_filesystem_free_bytes=min((s["free_bytes"] for s in states),default=None),
            scope="Finite second-half least-squares slope; small partial tails oscillate. Physical payload files exclude WAL/metadata and pinned unlinked inodes; allocated totals and statvfs are separate. Not an indefinite bound. Pressure observations are candidate-only, not invented baseline zeroes.")
    aggregates=[]
    for (variant,arm,cold,workload),rows in groups.items():
        def stat(get):return distribution(get(r) for r in rows)
        aggregates.append(dict(variant=variant,arm=arm,cold=cold,workload=workload,
            total=len(rows),passing=sum(r["verdict"]=="PASS" for r in rows),
            acks_per_sec=stat(lambda r:r.get("ops_per_sec")),
            p50_ms=stat(lambda r:(r.get("latency") or {}).get("p50_ms")),
            p99_ms=stat(lambda r:(r.get("latency") or {}).get("p99_ms")),
            payload_puts=stat(lambda r:r.get("diagnostics",{}).get("payload_upload_parts_in_window")),
            uploaded_bytes=stat(lambda r:r.get("diagnostics",{}).get("payload_upload_bytes_in_window")),
            leader_local_payload_bytes=stat(lambda r:r.get("offload",{}).get("per_node_after_client_payload_bytes",{}).get("1")),
            leader_backlog_slope=stat(lambda r:r.get("offload",{}).get("per_node_second_half_local_payload_slope_bytes_per_sec",{}).get("1")),
            sut_cpu_cores=stat(lambda r:r.get("resources",{}).get("sut",{}).get("cpu_cores")),
            sut_peak_rss_bytes=stat(lambda r:r.get("resources",{}).get("sut",{}).get("peak_rss_bytes")),
            garbage_reclamation_seconds=stat(lambda r:r.get("diagnostics",{}).get("garbage_reclamation_seconds"))))
    result["aggregates"]=aggregates
    result["variant_checker_sha256"]=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    result["comparison_scope"]="Same workload/settings and pinned native/provider/client; local shared host. Async throughput is local acceptance, NOT committed operations/s. All-replica drain and sampled Raft entry progress remain explicit. Missing observations and failed runs remain in counts."
    (root/"summary.json").write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps(dict(passed=result["passed"],total=result["total"],aggregates=aggregates),indent=2))
    return result["total"]>0 and result["passed"]==result["total"]


if __name__=="__main__":raise SystemExit(0 if run(sys.argv[1]) else 1)
