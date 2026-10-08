"""Matched forced-snapshot diagnostic, separate from ds-bench capacity runs.

Three untraced pairs and one separately traced pair, with rotating arm order.
Real single-member Raft, native WAL, four SUT CPUs, fresh identical payloads.
No concurrent client load: this isolates snapshot work, not write throughput.
Every stream is checked after SIGKILL/restart; traces perturb timing.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import traceback

from benchmark_summary import summarize_fsync_trace
from lab import BINARY, ROOT, Lab, source_hashes


def cell(output, binary, streams, trace):
    lab = Lab(output, replicas=1, partitions=1, port=19600, binary=binary)
    result = dict(verdict="FAIL", traced=trace, streams=streams,
                  binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
    profiler = None
    paths = [f"/snapshot-cost/{i}" for i in range(streams)]
    # Asymmetric binary bytes, independently determined from the request input.
    payload = bytes((i*73+17) % 256 for i in range(2048))
    tail = b"\x02\xfd\x41"
    try:
        lab.start(1, cpus="0-3", fault_testing=False, stats_secs=1)
        lab.initialize()
        for path in paths:
            assert lab.request(1, "PUT", path, payload)[0] == 201
            status, _, _ = lab.request(1, "POST", path, tail,
                {"content-type":"application/octet-stream", "producer-id":"snapshot-cost",
                 "producer-epoch":"3", "producer-seq":"0"})
            assert status == 200
        before = lab.admin(1, 0, "metrics")
        assert before["snapshot"] is None, "setup crossed the automatic snapshot threshold"
        if trace:
            pid = int((lab.output / "node-1.pid").read_text())
            command = ["strace", "-f", "-qq", "-ttt", "-T", "-yy", "-e", "trace=fsync,fdatasync",
                       "-o", str(lab.output / "fsync-trace.txt"), "-p", str(pid)]
            (lab.output / "trace-command.json").write_text(json.dumps(command)+"\n")
            with open(lab.output / "trace-stderr.txt", "w") as log:
                profiler = subprocess.Popen(command, stdout=log, stderr=log)
            time.sleep(0.3)
            assert profiler.poll() is None, "profiler attachment failed"
        start = time.monotonic_ns()
        assert lab.admin(1, 0, "snapshot") == {"Ok":None}
        while True:
            after = lab.admin(1, 0, "metrics")
            if after["snapshot"] is not None:
                break
            if time.monotonic_ns()-start > 30_000_000_000:
                raise TimeoutError("snapshot did not complete")
            time.sleep(0.001)
        result.update(completion_observed_ns=time.monotonic_ns()-start,
                      before=before, after=after)
        assert after["snapshot"]["index"] >= before["last_applied"]["index"]
        assert after["current_term"] == before["current_term"]
        # Include the final cumulative phase record. This wait is outside the
        # measured trigger-to-observation window and before restart/shutdown.
        time.sleep(1.1)
        if profiler:
            profiler.send_signal(signal.SIGINT)
            profiler.wait(timeout=10)
            profiler = None
            raw = (lab.output / "fsync-trace.txt").read_text()
            stats = summarize_fsync_trace(raw.splitlines())
            result["fsync_trace"] = stats
            result["sidecar_sync_calls"] = sum(".meta.tmp>" in line for line in raw.splitlines())
            assert stats["successful"] and not stats["errors"]
            assert not stats["incomplete_by_tid"] and not stats["resumed_without_start"]
        phases = [json.loads(line.removeprefix("RAFT_TIMING ")) for line in
                  (lab.output / "node-1.log").read_text().splitlines() if line.startswith("RAFT_TIMING ")]
        snapshots = [p for p in phases if p["phase"] == "snapshot"]
        result["snapshot_phase"] = snapshots[-1]["cumulative"]
        assert result["snapshot_phase"]["count"] == 1
        result["archive_bytes"] = sum(p.stat().st_size for p in (lab.data / "1/0/state").glob("snapshot-*"))
        lab.stop(1, crash=True)
        lab.start(1, cpus="0-3", fault_testing=False)
        lab.wait(lambda: lab.leader(0) == 1, "single-member restart")
        expected = payload+tail
        probes = []
        for path in paths:
            status, headers, body = lab.request(1, "GET", path+"?offset=-1")
            assert status == 200 and body == expected
            assert int(headers["stream-next-offset"].rsplit("_",1)[-1]) == len(expected)
            status, _, _ = lab.request(1, "POST", path, b"wrong duplicate payload",
                {"content-type":"application/octet-stream", "producer-id":"snapshot-cost",
                 "producer-epoch":"3", "producer-seq":"0"})
            assert status == 204
            status, _, body = lab.request(1, "GET", path+"?offset=-1")
            assert status == 200 and body == expected
            probes.append(dict(path=path, expected_bytes=len(expected),
                               expected_sha256=hashlib.sha256(expected).hexdigest(),
                               observed_sha256=hashlib.sha256(body).hexdigest(), retry_status=204))
        (lab.output / "restart-probes.json").write_text(json.dumps(probes, indent=2)+"\n")
        result["verdict"] = "PASS"
    except BaseException as error:
        result["error"] = repr(error)
        (lab.output / "failure.txt").write_text(traceback.format_exc())
    finally:
        if profiler and profiler.poll() is None:
            profiler.send_signal(signal.SIGINT)
            profiler.wait(timeout=10)
        lab.close()
        (lab.output / "result.json").write_text(json.dumps(result, indent=2)+"\n")
        # Dedicated measurement fixtures contain no signing keys or histories.
        # Keep failed data for investigation; successful cells retain hashes/logs.
        if result["verdict"] == "PASS":
            shutil.rmtree(lab.data)
    print(json.dumps(result), flush=True)
    return result


def run(output, baseline_binary, baseline_provenance, streams=1024):
    baseline_binary = baseline_binary.resolve()
    provenance = json.loads(baseline_provenance.read_text())
    assert hashlib.sha256(baseline_binary.read_bytes()).hexdigest() == provenance["hashes"]["binary"]
    assert 1 <= streams <= 2048, "bound this local diagnostic below automatic snapshot cadence"
    output.mkdir(parents=True, exist_ok=False)
    (output / "provenance.json").write_text(json.dumps(dict(
        baseline=provenance, current=dict(sources=source_hashes(), binary=hashlib.sha256(BINARY.read_bytes()).hexdigest()),
        driver=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), streams=streams, payload_bytes=2051,
        scope="isolated forced snapshot, real one-member consensus, four SUT CPUs, shared host/disk; not capacity",
        clock="monotonic trigger-to-observation (1ms polling); native phase includes durable WAL reference",
        traced_timing="perturbed and separate from the three untraced repetitions"), indent=2)+"\n")
    rows = []
    for repeat in range(4):
        arms = [("before", baseline_binary), ("after", BINARY)]
        for name, binary in arms[::1 if repeat % 2 == 0 else -1]:
            rows.append(cell(output / f"{output.name}-{name}-{repeat+1}", binary, streams, repeat == 3))
    (output / "results.json").write_text(json.dumps(rows, indent=2)+"\n")
    return all(r["verdict"] == "PASS" for r in rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--baseline-binary", type=Path, required=True)
    parser.add_argument("--baseline-provenance", type=Path, required=True)
    parser.add_argument("--streams", type=int, default=1024)
    sys.exit(0 if run(**vars(parser.parse_args())) else 1)
