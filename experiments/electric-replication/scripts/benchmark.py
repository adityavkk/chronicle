"""Matched local ds-bench qualification, not published cloud capacity numbers.

Unmodified pinned client and native baseline. Every cell has fresh data, four
aggregate SUT CPU affinities, four separate client CPUs, raw windows/results,
exact write/seed byte probes, and per-process samples. Shared disk/page cache and
host memory are NOT isolated; no independent-disk or production performance claim.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback

from benchmark_summary import kernel_fsync_event, summarize_fsync_trace, summarize_kernel_fsync
from lab import Lab, BINARY, EXPERIMENT, ROOT, matching_pids, source_hashes

CLIENT = ROOT / ".tmp/electric-tools/bench-target/release/ds-bench"
NATIVE = ROOT / ".tmp/electric-tools/upstream-target/release/durable-streams-server"


def workload_streams(workload):
    """Exact names used by the unchanged pinned client, not routing guesses."""
    if workload[0] == "write":
        return [f"s{i:08}" for i in range(workload[1])]
    if workload[0] == "reads":
        return [f"bench-reads-stream-{i}" for i in range(4)]
    if workload[0] == "mixed":
        return [f"s{i:06}" for i in range(16)]
    if workload[0] == "fanout":
        return ["fanout"]
    raise ValueError(f"unknown workload: {workload}")


def committed_target(workload, raw):
    """Seed bytes plus acknowledged appends; never use server offsets as oracle."""
    if workload[0] == "write":
        return raw["ok_total_all_phases"] * 256
    if workload[0] == "reads":
        return 4 * 4 * 1024 * 1024
    if workload[0] == "mixed":
        return 16 * 256 * 1024 + raw["write_counts"]["ok"] * 256
    if workload[0] == "fanout":
        return raw["append_counts"]["ok"] * 256
    raise ValueError(f"unknown workload: {workload}")


def export_cpu_profile(output):
    """Retain symbolized samples; raw DWARF stack memory stays local-only."""
    source = output / "perf.data"
    local = ROOT / ".tmp/electric-profiles" / f"{output.name}.perf.data.gz"
    local.parent.mkdir(parents=True, exist_ok=True)
    with open(output / "perf-script.log", "w") as errors, gzip.open(output / "cpu-samples.txt.gz", "wb") as samples:
        process = subprocess.Popen(["perf", "script", "--no-inline", "-F", "comm,pid,tid,time,event,ip,sym,dso",
                                    "-i", str(source)], stdout=subprocess.PIPE, stderr=errors)
        shutil.copyfileobj(process.stdout, samples)
        process.stdout.close()
        result = process.wait()
    with open(source, "rb") as data, open(local, "wb") as compressed:
        with gzip.GzipFile(filename="", mode="wb", fileobj=compressed, mtime=0) as archive:
            shutil.copyfileobj(data, archive)
    (output / "cpu-profile-provenance.json").write_text(json.dumps(dict(
        raw_local_path=str(local.relative_to(ROOT)), raw_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        archive_sha256=hashlib.sha256(local.read_bytes()).hexdigest(), script_exit_code=result,
        scope="99 Hz user-space CPU samples, whole client invocation; kernel/blocked time excluded. "
              "Raw DWARF process-memory captures are deliberately not version-controlled; symbolized samples are retained."), indent=2)+"\n")
    source.unlink()
    return result


def export_kernel_fsync(output, source, pids):
    # Raw tracepoints include unused syscall argument registers. Keep the root-
    # readable capture local-only; publish only fd/result/identity/timestamp.
    raw_hash = subprocess.check_output(["sudo", "-n", "sha256sum", str(source)], text=True).split()[0]
    (output / "kernel-profile-provenance.json").write_text(json.dumps(dict(
        raw_local_path=str(source.relative_to(ROOT)), raw_sha256=raw_hash, raw_bytes=source.stat().st_size,
        clock="CLOCK_MONOTONIC", node_pids=pids, max_capture_bytes=128*1024**2,
        scope="Linux x86-64, PID-scoped fsync/fdatasync raw tracepoints. No stacks or memory samples. "
              "Unused argument registers stay in root-readable local capture, never exported."), indent=2)+"\n")
    target = output / "kernel-fsync-events.jsonl.gz"
    with open(output / "kernel-script.log", "w") as errors, gzip.open(target, "wt") as out:
        process = subprocess.Popen(["sudo", "-n", "perf", "script", "--ns", "--show-lost-events",
            "-F", "trace:pid,tid,time,event,trace", "-i", str(source)], stdout=subprocess.PIPE,
            stderr=errors, text=True)
        try:
            for line in process.stdout:
                if not line.strip():
                    continue
                row = kernel_fsync_event(line)
                if row["pid"] not in pids:
                    raise ValueError("kernel trace includes a process outside this cell")
                out.write(json.dumps(row)+"\n")
        finally:
            process.stdout.close()
            code = process.wait(timeout=10)
        if code:
            raise ValueError("kernel trace export failed; raw capture retained")
    with gzip.open(target, "rt") as events:
        return summarize_kernel_fsync(json.loads(line) for line in events)


def preserve_incomplete_heap_profiles(output):
    """Keep interrupted/supervisor-restarted traces without publishing opaque tails."""
    rows = []
    for source in sorted(output.glob("heap-node-*.zst")):
        check = subprocess.run(["zstd", "-tq", str(source)], capture_output=True)
        if check.returncode == 0:
            continue
        local = ROOT / ".tmp/electric-profiles" / output.name / source.name
        local.parent.mkdir(parents=True, exist_ok=True)
        assert not local.exists(), "never replace an earlier partial trace"
        rows.append(dict(original_file=source.name, raw_local_path=str(local.relative_to(ROOT)),
                         sha256=hashlib.sha256(source.read_bytes()).hexdigest(), bytes=source.stat().st_size,
                         decoder_exit_code=check.returncode, decoder_error=check.stderr.decode(errors="replace")))
        source.rename(local)
    if rows:
        (output / "incomplete-heap-profiles.json").write_text(json.dumps(rows, indent=2)+"\n")


def sample(pids, ports, node_pids):
    processes = {}
    gaps = []
    for pid in pids:
        try:
            proc = Path(f"/proc/{pid}")
            stat = (proc / "stat").read_text().split(") ", 1)[1].split()
            io = {k:int(v) for k,v in (line.split(":") for line in (proc / "io").read_text().splitlines())}
            processes[str(pid)] = dict(cpu_ticks=int(stat[11])+int(stat[12]), rss_pages=int(stat[21]), io=io)
        except (FileNotFoundError, ProcessLookupError, PermissionError) as error:
            # /proc can disappear or refuse io access during process exit.
            # Preserve the missing observation; never invent a zero counter.
            gaps.append(dict(pid=pid,error=repr(error)))
    # Only record sockets owned by these disposable processes, never unrelated
    # orb traffic. Outbound node-to-node sockets estimate replication traffic;
    # already closed sockets can be missed, so this is an observed lower bound.
    lines = subprocess.run(["ss", "-Htinp"], capture_output=True, text=True, check=True).stdout.splitlines()
    sockets = []
    for i, line in enumerate(lines[:-1]):
        owners = set(map(int, re.findall(r"pid=(\d+)", line)))
        if owners.intersection(pids):
            parts = line.split()
            counters = {k:int(v) for k,v in re.findall(r"(bytes_sent|bytes_acked|bytes_received):(\d+)", lines[i+1])}
            sockets.append(dict(local=parts[3], peer=parts[4], owners=sorted(owners), counters=counters,
                                replication=bool(owners.intersection(node_pids)) and parts[4].rsplit(":",1)[-1] in ports))
    return dict(unix_ms=time.time_ns()//1_000_000, monotonic_ns=time.monotonic_ns(), processes=processes, sockets=sockets, process_gaps=gaps)


def cell(output, arm, workload, profile=False, diagnostics=False, pending_commands=256, duration=8, progress=False,
         profiler="strace", binary=BINARY, write_warmup_secs=3):
    replicas = 3 if arm.startswith("raft3") else 1
    lab = Lab(output, replicas=replicas, partitions=2, port=19800, binary=binary)
    lab.cluster = "bench-"+hashlib.sha256(str(lab.output).encode()).hexdigest()[:16]
    native = arm == "native"
    local_ack = arm.removesuffix("-baseline") == "raft3-local"
    node_pids = []
    trace = None
    sampler = None
    stop = threading.Event()
    result = dict(arm=arm, workload=workload, profile=profile, profiler=profiler if profile else None,
                  diagnostics=diagnostics, verdict="FAIL",
                  binary_sha256=hashlib.sha256((NATIVE if native else binary).read_bytes()).hexdigest(),
                  ack_contract="local-fsync acceptance, not commitment" if local_ack else "applied after durable quorum" if not native else "native local-fsync",
                  pending_commands=pending_commands, progress_sampling=progress)
    env = {k:v for k,v in os.environ.items() if not k.startswith("DS_BENCH_") and k != "LD_PRELOAD"}
    env["DS_BENCH_HDR_OUT"] = str(lab.output / "hdr")
    (lab.output / "hdr").mkdir()
    heap_profile = profile and profiler == "heaptrack"
    kernel_profile = profile and profiler == "kernel-fsync"
    kernel_capture = ROOT / ".tmp/electric-profiles" / f"{output.name}.kernel.perf.data"
    if profile:
        result["profiler_version"] = subprocess.check_output([
            "perf" if kernel_profile else "strace" if profiler == "fsync" else profiler, "--version"], text=True).strip()

    def stop_profile():
        nonlocal trace
        if trace is None:
            return
        stopped_early = trace.poll() is not None
        if trace.poll() is None:
            trace.send_signal(signal.SIGINT)
        trace.wait(timeout=10)
        trace_log.close()
        trace = None
        if kernel_profile:
            result["kernel_trace_stopped_early"] = stopped_early
            try:
                stats = export_kernel_fsync(lab.output, kernel_capture, node_pids)
                result["kernel_fsync_summary"] = stats
                result["profile_script_exit_code"] = int(stopped_early or not stats["successful"] or bool(stats["errors"]))
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                result["profile_script_exit_code"] = 1
                result["profile_error"] = str(error)
        elif profiler == "perf":
            with open(lab.output / "cpu-profile.txt", "w") as report:
                result["profile_report_exit_code"] = subprocess.run(
                    ["perf", "report", "--stdio", "--no-children", "--percent-limit", "0.2",
                     "-i", str(lab.output / "perf.data")], stdout=report, stderr=subprocess.STDOUT).returncode
            if (lab.output / "perf.data").exists():
                result["profile_script_exit_code"] = export_cpu_profile(lab.output)
        elif profiler == "fsync":
            try:
                with open(lab.output / "fsync-trace.log") as data:
                    result["fsync_trace_summary"] = summarize_fsync_trace(data)
                stats = result["fsync_trace_summary"]
                result["profile_script_exit_code"] = int(not stats["successful"] or bool(stats["errors"]))
            except (OSError, ValueError) as error:
                result["profile_script_exit_code"] = 1
                result["profile_error"] = str(error)

    def run_client(args, name):
        command = ["taskset", "-c", "4-7", str(CLIENT), *args, "--target", f"http://127.0.0.1:{lab.port+1}", "--api-style", "durable"]
        (lab.output / f"{name}-argv.json").write_text(json.dumps(command, indent=2)+"\n")
        with open(lab.output / f"{name}.json", "w") as out, open(lab.output / f"{name}.log", "w") as err:
            begin = time.time_ns()//1_000_000
            proc = subprocess.Popen(command, stdout=out, stderr=err, env=env)
            result[name+"_pid"] = proc.pid
            try:
                proc.wait(timeout=max(240, duration+120))
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                raise
            finally:
                result[name+"_window"] = dict(start_unix_ms=begin, end_unix_ms=time.time_ns()//1_000_000, exit_code=proc.returncode)
        assert proc.returncode == 0, f"{name} exited {proc.returncode}"
        return json.loads((lab.output / f"{name}.json").read_text())

    def seed(names, size):
        payload = bytes((i*73+17) % 256 for i in range(65536))
        def one(name):
            path = "/v1/stream/"+name
            assert lab.request(1, "PUT", path)[0] == 201
            for offset in range(0,size,len(payload)):
                chunk = payload[:min(len(payload),size-offset)]
                response = lab.request(1, "POST", path, chunk,
                                       {"content-type":"application/octet-stream", "stream-durability":"quorum-fsync"})
                assert 200 <= response[0] < 300, (path, offset, response)
            status, headers, data = lab.request(1,"GET",path+"?offset=-1")
            expected = (payload*((size+len(payload)-1)//len(payload)))[:size]
            assert status == 200 and data == expected and int(headers["stream-next-offset"].rsplit("_",1)[-1]) == size
            return dict(stream=name, bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        with ThreadPoolExecutor(max_workers=8) as pool:
            probes = list(pool.map(one, names))
        (lab.output / "seed-probes.json").write_text(json.dumps(probes, indent=2)+"\n")

    try:
        # Local harness headroom, not a server storage requirement or capacity
        # guarantee. Keep raw failure evidence if the shared disk is too full
        # to measure this cell without risking another truncated sample file.
        result["disk_preflight"] = dict(free_bytes=shutil.disk_usage(ROOT).free,
                                        minimum_free_bytes=8 * 1024**3)
        if result["disk_preflight"]["free_bytes"] < result["disk_preflight"]["minimum_free_bytes"]:
            raise RuntimeError("benchmark requires 8 GiB free before starting a cell; no SUT started")
        if native:
            command = ["taskset", "-c", "0-3", str(NATIVE), "--host", "127.0.0.1", "--port", str(lab.port+1),
                       "--data-dir", str(lab.data / "1"), "--durability", "wal", "--worker-threads", "2",
                       "--wal-shards", "2", "--wal-segment-bytes", str(8*1024*1024), "--stream-lanes", "1",
                       "--tier", "off", "--tail-cache-bytes", "0", "--read-offload", "tail"]
            if diagnostics:
                command += ["--wal-stats", "1", "--server-stats", "1"]
            if heap_profile:
                command[3:3] = ["heaptrack", "-o", str(lab.output / "heap-node-1")]
            (lab.output / "native-argv.json").write_text(json.dumps(command, indent=2)+"\n")
            subprocess.run(["amp", "orb", "service", "start", lab.cluster+"-1", "--cwd", str(ROOT),
                            "--command", "ulimit -c 0; exec "+shlex.join(command)+" > "+shlex.quote(str(lab.output / "node-1.log"))+" 2>&1",
                            "--port", str(lab.port+1)], check=True, stdout=subprocess.DEVNULL)
            lab.nodes.add(1)
            lab.wait(lambda: lab.request(1,"GET","/health")[0] == 200, "native ready")
            node_pids = matching_pids([str(NATIVE)])
            assert len(node_pids) == 1
        else:
            for node in range(1,replicas+1):
                lab.start(node, cpus="0-3", fault_testing=False, stats_secs=int(diagnostics),
                          pending_commands=pending_commands,
                          append_durability="local-fsync" if local_ack else "quorum-fsync", heap_profile=heap_profile)
                node_pids.append(int((lab.output / f"node-{node}.pid").read_text()))
            for group in range(2):
                assert lab.admin(1, group, "init", lab.genesis) == {"Ok":None}
                lab.wait(lambda: lab.leader(group) == 1, "direct endpoint leader")
        result["node_pids"] = node_pids
        result["affinity"] = {str(p):sorted(os.sched_getaffinity(p)) for p in node_pids}
        names = workload_streams(workload)
        if workload[0] == "reads":
            seed(names, 4*1024*1024)
            args = ["reads", "--mode", "catchup", "--streams", "4", "--connections", "16", "--read-size-bytes", "4096",
                    "--seed-bytes", "0", "--warmup-secs", "3", "--settle-secs", "1", "--duration-secs", str(duration)]
        elif workload[0] == "mixed":
            seed(names, 256*1024)
            args = ["mixed", "--streams", "16", "--writers-per-stream", "1", "--writer-rate", "0", "--readers", "16",
                    "--read-rate", "1", "--backfill-events", "0", "--subscribers", "0", "--duration-secs", str(duration), "--payload-bytes", "256"]
        elif workload[0] == "fanout":
            args = ["fan-out", "--stream", "fanout", "--subscribers", str(workload[1]), "--writer-rate", "50",
                    "--duration-secs", str(duration), "--payload-bytes", "256", "--subscriber-idle-timeout-secs", "10"]
        else:
            _, streams, concurrency = workload
            args = ["multi-stream", "--streams", str(streams), "--connections", str(concurrency), "--batch", "1",
                    "--rate-per-stream", "0", "--payload-bytes", "256", "--setup-concurrency", "32",
                    "--warmup-secs", str(write_warmup_secs), "--settle-secs", "1", "--duration-secs", str(duration)]
        # reqwest's timeout spans the entire SSE body, starting before the
        # writer barrier. It must outlive the drive window plus setup/drain.
        args += ["--request-timeout-secs", str(duration + 30 if workload[0] == "fanout" else 30)]
        if profile and not heap_profile:
            if kernel_profile:
                if os.uname().machine != "x86_64" or duration > 60:
                    raise ValueError("kernel fsync profiler requires x86-64 and at most 60 seconds")
                kernel_capture.parent.mkdir(parents=True, exist_ok=True)
                assert not kernel_capture.exists(), "never replace an earlier kernel capture"
                command = ["sudo", "-n", "perf", "record", "--clockid", "mono", "--max-size", "128M",
                    "-e", "raw_syscalls:sys_enter", "--filter", "id == 74 || id == 75",
                    "-e", "raw_syscalls:sys_exit", "--filter", "id == 74 || id == 75",
                    "-o", str(kernel_capture), "-p", ",".join(map(str, node_pids))]
            elif profiler == "perf":
                command = ["perf", "record", "-F", "99", "-e", "cpu-clock:u", "--call-graph", "dwarf,16384",
                           "-o", str(lab.output / "perf.data"), "-p", ",".join(map(str, node_pids))]
            else:
                assert profiler in ("strace", "fsync"), profiler
                command = (["strace", "-f", "-qq", "-ttt", "-T", "-e", "trace=fsync,fdatasync", "-e", "signal=none",
                            "-o", str(lab.output / "fsync-trace.log")] if profiler == "fsync" else
                           ["strace", "-f", "-c", "-w", "-o", str(lab.output / "syscall-profile.txt")])
                for pid in node_pids:
                    command += ["-p", str(pid)]
            (lab.output / "profile-argv.json").write_text(json.dumps(command, indent=2)+"\n")
            trace_log = open(lab.output / f"{profiler}.log", "w")
            trace = subprocess.Popen(command, stdout=trace_log, stderr=trace_log)
            time.sleep(0.5)
            assert trace.poll() is None, "profiler attach failed; see retained log"

        def collect():
            try:
                with open(lab.output / "samples.jsonl", "w") as out:
                    while not stop.is_set():
                        pids = node_pids + ([result["client_pid"]] if "client_pid" in result else [])
                        observation = sample(pids, {str(lab.port+n) for n in lab.nodes}, node_pids)
                        result.setdefault("process_sample_gaps",[]).extend(observation["process_gaps"])
                        if progress:
                            observation["replicas"] = []
                            for node in sorted(lab.nodes):
                                row = dict(node=node, unix_ms=time.time_ns()//1_000_000)
                                try:
                                    if not native:
                                        row["groups"] = [lab.admin(node,g,"metrics") for g in range(2)]
                                    # Bound observer load. Never extrapolate these sampled
                                    # prefixes into a many-stream total; final drain is exhaustive.
                                    row["sampled_stream_indices"] = sorted({i*(len(names)-1)//15 for i in range(16)})
                                    row["total_streams"] = len(names)
                                    row["sampled_stream_names"] = [names[i] for i in row["sampled_stream_indices"]]
                                    total = 0
                                    for name in row["sampled_stream_names"]:
                                        status, headers, _ = lab.request(node,"HEAD",f"/v1/stream/{name}",
                                                                       headers={"stream-consistency":"prefix"})
                                        row["head_status"] = status
                                        if status != 200:
                                            break
                                        total += int(headers["stream-next-offset"].rsplit("_",1)[-1])
                                    else:
                                        row["committed_bytes"] = total
                                except (OSError, AssertionError, KeyError, ValueError) as error:
                                    row["error"] = repr(error)
                                row["end_unix_ms"] = time.time_ns()//1_000_000
                                observation["replicas"].append(row)
                        out.write(json.dumps(observation)+"\n")
                        out.flush()
                        stop.wait(0.5)
            except Exception:
                result["sampling_error"] = traceback.format_exc()
        sampler = threading.Thread(target=collect)
        sampler.start()
        raw = run_client(args,"client")
        if progress:
            # Stock ds-bench discards receipts. Fresh uniform batch=1 streams
            # permit exact aggregate reconciliation, not per-attempt proof.
            target = committed_target(workload, raw)
            started = time.monotonic()
            def drained():
                rows = []
                for node in sorted(lab.nodes):
                    total = 0
                    for name in names:
                        status, headers, _ = lab.request(node,"HEAD",f"/v1/stream/{name}",
                                                       headers={"stream-consistency":"prefix"})
                        assert status == 200
                        total += int(headers["stream-next-offset"].rsplit("_",1)[-1])
                    rows.append(dict(node=node,committed_bytes=total))
                with open(lab.output / "drain.jsonl","a") as out:
                    out.write(json.dumps(dict(unix_ms=time.time_ns()//1_000_000,target=target,replicas=rows))+"\n")
                if any(r["committed_bytes"] > target for r in rows):
                    raise ValueError("committed bytes exceed seed plus acknowledgements; unknown outcome or duplicate")
                return rows if all(r["committed_bytes"] == target for r in rows) else None
            rows = lab.wait(drained,"all accepted bytes committed and applied on every replica",timeout=30)
            result["drain"] = dict(observed_ms=(time.monotonic()-started)*1000,target_bytes=target,replicas=rows,
                                   scope="post-client-exit observation upper bound; every replica, no per-attempt receipts")
        stop.set()
        sampler.join()
        assert "sampling_error" not in result, result.get("sampling_error")
        stop_profile()
        assert result.get("profile_report_exit_code", 0) == 0, "profiler report failed; raw output retained"
        assert result.get("profile_script_exit_code", 0) == 0, "profiler sample export failed; raw output retained"
        if workload[0] == "write":
            verify = run_client(["verify-offsets", "--streams", str(workload[1]), "--payload-bytes", "256", "--concurrency", "32"], "verify")
            assert verify["total_records"] == raw["ok_total_all_phases"] and verify["bytes_divide_exactly"]
            assert verify["streams_found"] == workload[1] and verify["streams_missing"] == verify["head_errors"] == 0
            assert raw["lazy_creates"] == raw["counts"]["other_err"] == raw["counts"]["backpressure"] == 0
        elif workload[0] == "fanout":
            assert raw["append_counts"]["other_err"] == raw["append_counts"]["backpressure"] == raw["subscriber_errors"] == 0
            assert raw["events_sent"] == raw["append_counts"]["ok"] > 0
            result["delivery_fraction"] = raw["events_received"] / (raw["events_sent"]*workload[1])
            # The upstream counter counts frames, can coalesce records, and
            # tolerates EOF. Do not reinterpret it as a per-reader drain proof.
            if progress and workload[1] == 1000:
                command = ["taskset", "-c", "4-7", "node", str(EXPERIMENT / "scripts/fanout_drain.mjs"),
                           f"http://127.0.0.1:{lab.port+1}/v1/stream", str(lab.output / "finite-fanout.json")]
                (lab.output / "finite-fanout-argv.json").write_text(json.dumps(command, indent=2)+"\n")
                with open(lab.output / "finite-fanout.log", "w") as out:
                    checked = subprocess.run(command, stdout=out, stderr=subprocess.STDOUT, timeout=90)
                result["finite_fanout_exit_code"] = checked.returncode
                assert checked.returncode == 0, "independent live sequence probe failed; ledgers retained"
        elif workload[0] == "reads":
            assert raw["counts"]["other_err"] == raw["counts"]["backpressure"] == 0
            assert raw["bytes_read_total"] == raw["counts"]["ok"]*4*1024*1024
        else:
            for kind in ("write_counts", "read_counts"):
                assert raw[kind]["other_err"] == raw[kind]["backpressure"] == 0
        result["verdict"] = "PASS"
    except BaseException as error:
        result["error"] = repr(error)
        (lab.output / "failure.txt").write_text(traceback.format_exc())
    finally:
        stop.set()
        if sampler:
            sampler.join()
        stop_profile()
        if diagnostics:
            # Keep the final partial counter interval even when validation
            # failed. This wait is outside the measurement/sample windows.
            time.sleep(2)
            result["diagnostic_tail_captured"] = bool(node_pids) and all(Path(f"/proc/{pid}").exists() for pid in node_pids)
        if not native:
            for node in sorted(lab.nodes):
                for group in range(2):
                    try:
                        (lab.output / f"final-metrics-{node}-{group}.json").write_text(json.dumps(lab.admin(node,group,"metrics"),indent=2)+"\n")
                    except (OSError, AssertionError):
                        pass
        # Retain sizes, not benchmark payload copies. Data is disposable and
        # per-cell cleanup prevents a long sweep exhausting the shared disk.
        if heap_profile:
            # Let the profiler's interpreter drain EOF before stopping its
            # supervising service/process group. Only our recorded SUT PIDs.
            for node, pid in enumerate(node_pids, 1):
                # The installed wrapper leaves custom %p names literal. Move
                # the still-open trace first so a supervisor restart cannot
                # overwrite it. The interpreter keeps writing the same inode.
                for suffix in (".gz", ".zst"):
                    heap = lab.output / f"heap-node-{node}{suffix}"
                    if heap.exists():
                        heap.rename(lab.output / f"heap-node-{node}-{pid}{suffix}")
                cmdline = Path(f"/proc/{pid}/cmdline")
                if cmdline.exists() and cmdline.read_bytes().split(b"\0")[0] in (str(NATIVE).encode(), str(binary).encode()):
                    os.kill(pid, signal.SIGTERM)
            time.sleep(2)
        lab.close()
        if heap_profile:
            reports = {}
            for heap in lab.output.glob("heap-node-*.*"):
                if heap.suffix not in (".gz", ".zst") or not any(heap.stem.endswith(f"-{pid}") for pid in node_pids):
                    continue
                with open(heap.with_suffix(".txt"), "w") as report:
                    reports[heap.name] = subprocess.run(["heaptrack_print", "-f", str(heap), "-n", "20"],
                        stdout=report, stderr=subprocess.STDOUT).returncode
                if not re.search(r"calls to allocation functions: [1-9][0-9]*", heap.with_suffix(".txt").read_text()):
                    reports[heap.name] = -1
            result["heap_report_exit_codes"] = reports
            if len(reports) != replicas or any(reports.values()):
                result["verdict"] = "FAIL"
                result["profile_error"] = "Missing or unreadable heap profile; partial outputs retained"
            preserve_incomplete_heap_profiles(lab.output)
        files = {str(p.relative_to(lab.data)):p.stat().st_size for p in lab.data.rglob("*") if p.is_file()}
        (lab.output / "storage-bytes.json").write_text(json.dumps(files,indent=2)+"\n")
        for path in lab.output.glob("*.log"):
            if not path.with_suffix(".log.gz").exists():
                with open(str(path)+".gz","wb") as target, gzip.GzipFile(filename="",mode="wb",fileobj=target,mtime=0) as archive:
                    archive.write(path.read_bytes())
        samples = lab.output / "samples.jsonl"
        if samples.exists():
            with open(samples,"rb") as source, open(str(samples)+".gz","wb") as target:
                with gzip.GzipFile(filename="",mode="wb",fileobj=target,mtime=0) as archive:
                    shutil.copyfileobj(source, archive)
            samples.unlink()
        (lab.output / "result.json").write_text(json.dumps(result,indent=2)+"\n")
        shutil.rmtree(lab.data)
    print(json.dumps(result), flush=True)
    return result


def run(output, smoke=False, reads_only=False, write_diagnostics=False, write_profiles=False, async_writes=False,
        cpu_profiles=False, heap_profiles=False, baseline_binary=None, baseline_provenance=None, duration_secs=None,
        write_streams=1, write_connections=256, write_warmup_secs=3, read_diagnostics=False, fsync_profiles=False,
        no_stats=False, fsync_profiler="strace"):
    if read_diagnostics and any((smoke, reads_only, write_diagnostics, write_profiles, async_writes, cpu_profiles, heap_profiles, fsync_profiles)):
        raise ValueError("read diagnostics is a separate repeated four-arm matrix")
    if sum((write_profiles, cpu_profiles, heap_profiles, fsync_profiles)) > 1:
        raise ValueError("select one profiler per campaign")
    if fsync_profiler not in ("strace", "kernel") or (fsync_profiler == "kernel" and (
            not fsync_profiles or os.uname().machine != "x86_64" or (duration_secs or 0) > 60)):
        raise ValueError("kernel fsync profiler requires fsync profiles on x86-64, at most 60 seconds")
    if (baseline_binary is None) != (baseline_provenance is None):
        raise ValueError("baseline comparison requires both binary and conformance provenance")
    baseline = None
    if baseline_binary is not None:
        baseline_binary = Path(baseline_binary).resolve()
        baseline_provenance = Path(baseline_provenance).resolve()
        baseline = json.loads(baseline_provenance.read_text())
        if hashlib.sha256(baseline_binary.read_bytes()).hexdigest() != baseline["hashes"]["binary"]:
            raise ValueError("baseline binary does not match its retained conformance provenance")
    if duration_secs is not None and duration_secs <= 0:
        raise ValueError("duration must be positive")
    if write_streams <= 0 or write_connections <= 0 or write_warmup_secs < 0:
        raise ValueError("write streams/connections must be positive and warmup nonnegative")
    if (write_streams, write_connections, write_warmup_secs) != (1, 256, 3) and not any(
            (write_diagnostics, write_profiles, async_writes, cpu_profiles, heap_profiles, fsync_profiles)):
        raise ValueError("write controls require an explicit write diagnostic/profile/async matrix")
    progress = async_writes or read_diagnostics
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    sources = source_hashes()
    provenance = dict(native_source="88793e76595d69be300731b9b25c58538923a53b", client_source="93a1a066a511ad2ce5114dc429afb1fd0f6d99bf",
        client_unmodified=True, native_unmodified=True, adaptation_sources=sources,
        driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        lab_driver_sha256=hashlib.sha256(Path(__file__).with_name("lab.py").read_bytes()).hexdigest(),
        finite_fanout_driver_sha256=hashlib.sha256(Path(__file__).with_name("fanout_drain.mjs").read_bytes()).hexdigest(),
        analyzer_sha256=hashlib.sha256(Path(__file__).with_name("benchmark_summary.py").read_bytes()).hexdigest(),
        binaries={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in (CLIENT,NATIVE,BINARY)},
        sut_cpus="0-3 aggregate across replicas", client_cpus="4-7", memory="shared 16 GiB host, no separate quota",
        disk="shared orb root filesystem; same 8 MiB WAL segments, no cold tier", workers_per_process=2,
        wal_shards_or_partitions=2, cpu_hz=os.sysconf("SC_CLK_TCK"), page_bytes=os.sysconf("SC_PAGE_SIZE"),
        native_durability="local fsync", raft1_durability="one-member local fsync", raft3_durability="quorum fsync on shared host",
        raft3_local_durability="local-fsync202 acceptance; all reads committed-only; background durable-quorum replication",
        disable_optional_stats=no_stats,
        pending_commands=1024 if progress else 256, pending_bytes=16*1024*1024,
        progress_sampling="prefix HEAD for at most 16 declared stream indices per replica and group Raft metrics, then 0.5s idle; adds observation work" if progress else "none",
        client_limits="202 is success; receipt headers discarded; 429 and 503 combined as measured backpressure, not timed; warmup errors not counted",
        read_client_limits="Fanout counts frames and tolerates EOF/join failure; separate finite sequence probe is not a proof about omitted frames. "
                           "Fanout, reads and mixed expose no exact wall-clock drive window; resources use outer client invocation. "
                           "Mixed ignores task join failures; committed-byte drain is checked independently.",
        write_load="closed-loop concurrency, not paced; the pinned pool ignores rate-per-stream",
        write_warmup_secs=write_warmup_secs,
        read_consistency="replicated default linearizable", tail_cache_bytes=0,
        qualification="short local windows, client not independently calibrated; no cloud capacity headline")
    if baseline is not None:
        provenance["baseline"] = dict(binary=str(baseline_binary.relative_to(ROOT)),
            conformance_provenance=str(baseline_provenance.relative_to(ROOT)),
            conformance_provenance_sha256=hashlib.sha256(baseline_provenance.read_bytes()).hexdigest(),
            **baseline)
        provenance["binaries"][str(baseline_binary.relative_to(ROOT))] = baseline["hashes"]["binary"]
    (output / "provenance.json").write_text(json.dumps(provenance,indent=2)+"\n")
    (output / "host.txt").write_text(subprocess.run(["uname","-a"],capture_output=True,text=True,check=True).stdout+
                                     Path("/proc/cpuinfo").read_text()+Path("/proc/meminfo").read_text())
    read_workloads = [("fanout",n) for n in (1,100,1000)] + [("reads",), ("mixed",)]
    workloads = [("write",n,c) for n in (1,1024) for c in (4,16,64,256)] + read_workloads
    if smoke:
        workloads = [("write",1,4)]
    if reads_only:
        workloads = [("reads",), ("mixed",)]
    if write_diagnostics or async_writes:
        workloads = [("write",write_streams,write_connections)] * 3
    if write_profiles or cpu_profiles or heap_profiles or fsync_profiles:
        workloads = [("write",write_streams,write_connections)]
    if read_diagnostics:
        workloads = read_workloads * 3
    results = []
    for i, workload in enumerate(workloads):
        # Rotate arm order to reduce a systematic page-cache/time-order bias.
        arms = ["native","raft1","raft3-local","raft3"] if progress else ["native","raft1"] if write_diagnostics else ["native","raft1","raft3"]
        if baseline is not None:
            arms = [candidate for arm in arms for candidate in ([arm] if arm == "native" else [arm+"-baseline",arm])]
        arms = arms[i%len(arms):]+arms[:i%len(arms)]
        for arm in arms:
            repeat = i//len(read_workloads)+1 if read_diagnostics else i+1
            name = f"{output.name}-{arm}-"+"-".join(map(str,workload))+(f"-repeat{repeat}" if write_diagnostics or progress else "")
            results.append(cell(output / name, arm, workload, profile=write_profiles or cpu_profiles or heap_profiles or fsync_profiles,
                                diagnostics=not no_stats and (write_diagnostics or write_profiles or progress or cpu_profiles or heap_profiles or fsync_profiles),
                                pending_commands=1024 if progress else 256,
                                duration=duration_secs if duration_secs is not None else 30 if progress else 8, progress=progress,
                                binary=baseline_binary if arm.endswith("-baseline") else BINARY,
                                write_warmup_secs=write_warmup_secs,
                                profiler="heaptrack" if heap_profiles else "perf" if cpu_profiles else
                                    ("kernel-fsync" if fsync_profiler == "kernel" else "fsync") if fsync_profiles else "strace"))
    if not smoke and not write_diagnostics and not write_profiles and not progress and not cpu_profiles and not heap_profiles and not fsync_profiles:
        for arm in ("native","raft1","raft3"):
            for workload in (("reads",),) if reads_only else (("write",1024,64),("reads",)):
                name = f"{output.name}-{arm}-profile-"+"-".join(map(str,workload))
                results.append(cell(output / name,arm,workload,profile=True,
                                    duration=duration_secs if duration_secs is not None else 8))
    (output / "results.json").write_text(json.dumps(results,indent=2)+"\n")
    return all(r["verdict"] == "PASS" for r in results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    for name in ("smoke", "reads-only", "write-diagnostics", "write-profiles", "async-writes", "cpu-profiles", "heap-profiles", "fsync-profiles", "read-diagnostics"):
        parser.add_argument("--"+name, action="store_true")
    parser.add_argument("--baseline-binary", type=Path)
    parser.add_argument("--baseline-provenance", type=Path)
    parser.add_argument("--duration-secs", type=int)
    parser.add_argument("--write-streams", type=int, default=1)
    parser.add_argument("--write-connections", type=int, default=256)
    parser.add_argument("--write-warmup-secs", type=int, default=3,
                        help="Use 0 for the reject-free envelope: the pinned client does not count warmup errors")
    parser.add_argument("--fsync-profiler", choices=("strace", "kernel"), default="strace",
                        help="Kernel tracepoints require local sudo/perf; at most 60s and 128MiB, raw registers stay local-only")
    parser.add_argument("--no-stats", action="store_true",
                        help="Disable optional server/WAL probes; keep identical workload, bounds and external observations")
    sys.exit(0 if run(**vars(parser.parse_args())) else 1)
