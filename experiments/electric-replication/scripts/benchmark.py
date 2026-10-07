"""Matched local ds-bench qualification, not published cloud capacity numbers.

Unmodified pinned client and native baseline. Every cell has fresh data, four
aggregate SUT CPU affinities, four separate client CPUs, raw windows/results,
exact write/seed byte probes, and per-process samples. Shared disk/page cache and
host memory are NOT isolated; no independent-disk or production performance claim.
"""
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

from lab import Lab, BINARY, EXPERIMENT, ROOT

CLIENT = ROOT / ".tmp/electric-tools/bench-target/release/ds-bench"
NATIVE = ROOT / ".tmp/electric-tools/upstream-target/release/durable-streams-server"


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


def cell(output, arm, workload, profile=False):
    replicas = 3 if arm == "raft3" else 1
    lab = Lab(output, replicas=replicas, partitions=2, port=19800)
    lab.cluster = "bench-"+hashlib.sha256(str(lab.output).encode()).hexdigest()[:16]
    native = arm == "native"
    node_pids = []
    trace = None
    sampler = None
    stop = threading.Event()
    result = dict(arm=arm, workload=workload, profile=profile, verdict="FAIL")
    env = {k:v for k,v in os.environ.items() if not k.startswith("DS_BENCH_") and k != "LD_PRELOAD"}
    env["DS_BENCH_HDR_OUT"] = str(lab.output / "hdr")
    (lab.output / "hdr").mkdir()

    def run_client(args, name):
        command = ["taskset", "-c", "4-7", str(CLIENT), *args, "--target", f"http://127.0.0.1:{lab.port+1}", "--api-style", "durable"]
        (lab.output / f"{name}-argv.json").write_text(json.dumps(command, indent=2)+"\n")
        with open(lab.output / f"{name}.json", "w") as out, open(lab.output / f"{name}.log", "w") as err:
            begin = time.time_ns()//1_000_000
            proc = subprocess.Popen(command, stdout=out, stderr=err, env=env)
            result[name+"_pid"] = proc.pid
            try:
                proc.wait(timeout=240)
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
                response = lab.request(1, "POST", path, chunk, {"content-type":"application/octet-stream"})
                assert 200 <= response[0] < 300, (path, offset, response)
            status, headers, data = lab.request(1,"GET",path+"?offset=-1")
            expected = (payload*((size+len(payload)-1)//len(payload)))[:size]
            assert status == 200 and data == expected and int(headers["stream-next-offset"].rsplit("_",1)[-1]) == size
            return dict(stream=name, bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        with ThreadPoolExecutor(max_workers=8) as pool:
            probes = list(pool.map(one, names))
        (lab.output / "seed-probes.json").write_text(json.dumps(probes, indent=2)+"\n")

    try:
        if native:
            command = ["taskset", "-c", "0-3", str(NATIVE), "--host", "127.0.0.1", "--port", str(lab.port+1),
                       "--data-dir", str(lab.data / "1"), "--durability", "wal", "--worker-threads", "2",
                       "--wal-shards", "2", "--wal-segment-bytes", str(8*1024*1024), "--stream-lanes", "1",
                       "--tier", "off", "--tail-cache-bytes", "0", "--read-offload", "tail"]
            (lab.output / "native-argv.json").write_text(json.dumps(command, indent=2)+"\n")
            subprocess.run(["amp", "orb", "service", "start", lab.cluster+"-1", "--cwd", str(ROOT),
                            "--command", "ulimit -c 0; exec "+shlex.join(command)+" > "+shlex.quote(str(lab.output / "node-1.log"))+" 2>&1",
                            "--port", str(lab.port+1)], check=True, stdout=subprocess.DEVNULL)
            lab.nodes.add(1)
            lab.wait(lambda: lab.request(1,"GET","/health")[0] == 200, "native ready")
            for proc in Path("/proc").iterdir():
                try:
                    if proc.name.isdecimal() and str(NATIVE).encode() in (proc / "cmdline").read_bytes().split(b"\0"):
                        node_pids.append(int(proc.name))
                except FileNotFoundError:
                    pass
            assert len(node_pids) == 1
        else:
            for node in range(1,replicas+1):
                lab.start(node, cpus="0-3", fault_testing=False)
                node_pids.append(int((lab.output / f"node-{node}.pid").read_text()))
            for group in range(2):
                assert lab.admin(1, group, "init", lab.genesis) == {"Ok":None}
                lab.wait(lambda: lab.leader(group) == 1, "direct endpoint leader")
        result["node_pids"] = node_pids
        result["affinity"] = {str(p):sorted(os.sched_getaffinity(p)) for p in node_pids}
        if workload[0] == "reads":
            seed([f"bench-reads-stream-{i}" for i in range(4)], 4*1024*1024)
            args = ["reads", "--mode", "catchup", "--streams", "4", "--connections", "16", "--read-size-bytes", "4096",
                    "--seed-bytes", "0", "--warmup-secs", "3", "--settle-secs", "1", "--duration-secs", "8"]
        elif workload[0] == "mixed":
            seed([f"s{i:06}" for i in range(16)], 256*1024)
            args = ["mixed", "--streams", "16", "--writers-per-stream", "1", "--writer-rate", "0", "--readers", "16",
                    "--read-rate", "1", "--backfill-events", "0", "--subscribers", "0", "--duration-secs", "10", "--payload-bytes", "256"]
        elif workload[0] == "fanout":
            args = ["fan-out", "--stream", "fanout", "--subscribers", str(workload[1]), "--writer-rate", "50",
                    "--duration-secs", "10", "--payload-bytes", "256", "--subscriber-idle-timeout-secs", "10"]
        else:
            _, streams, concurrency = workload
            args = ["multi-stream", "--streams", str(streams), "--connections", str(concurrency), "--batch", "1",
                    "--rate-per-stream", "0", "--payload-bytes", "256", "--setup-concurrency", "32",
                    "--warmup-secs", "3", "--settle-secs", "1", "--duration-secs", "8"]
        args += ["--request-timeout-secs", "30"]
        if profile:
            command = ["strace", "-f", "-c", "-w", "-o", str(lab.output / "syscall-profile.txt")]
            for pid in node_pids:
                command += ["-p", str(pid)]
            trace_log = open(lab.output / "strace.log", "w")
            trace = subprocess.Popen(command, stdout=trace_log, stderr=trace_log)
            time.sleep(0.5)
            assert trace.poll() is None, "strace attach failed; see retained log"

        def collect():
            try:
                with open(lab.output / "samples.jsonl", "w") as out:
                    while not stop.is_set():
                        pids = node_pids + ([result["client_pid"]] if "client_pid" in result else [])
                        observation = sample(pids, {str(lab.port+n) for n in lab.nodes}, node_pids)
                        result.setdefault("process_sample_gaps",[]).extend(observation["process_gaps"])
                        out.write(json.dumps(observation)+"\n")
                        out.flush()
                        stop.wait(0.5)
            except Exception:
                result["sampling_error"] = traceback.format_exc()
        sampler = threading.Thread(target=collect)
        sampler.start()
        raw = run_client(args,"client")
        stop.set()
        sampler.join()
        assert "sampling_error" not in result, result.get("sampling_error")
        if trace:
            trace.send_signal(signal.SIGINT)
            trace.wait(timeout=10)
            trace_log.close()
            trace = None
        if workload[0] == "write":
            verify = run_client(["verify-offsets", "--streams", str(workload[1]), "--payload-bytes", "256", "--concurrency", "32"], "verify")
            assert verify["total_records"] == raw["ok_total_all_phases"] and verify["bytes_divide_exactly"]
            assert verify["streams_found"] == workload[1] and verify["streams_missing"] == verify["head_errors"] == 0
            assert raw["lazy_creates"] == raw["counts"]["other_err"] == raw["counts"]["backpressure"] == 0
        elif workload[0] == "fanout":
            assert raw["append_counts"]["other_err"] == raw["append_counts"]["backpressure"] == raw["subscriber_errors"] == 0
            result["delivery_fraction"] = raw["events_received"] / (raw["events_sent"]*workload[1])
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
        if trace and trace.poll() is None:
            trace.send_signal(signal.SIGINT)
            trace.wait(timeout=10)
            trace_log.close()
        if not native:
            for node in sorted(lab.nodes):
                for group in range(2):
                    try:
                        (lab.output / f"final-metrics-{node}-{group}.json").write_text(json.dumps(lab.admin(node,group,"metrics"),indent=2)+"\n")
                    except (OSError, AssertionError):
                        pass
        # Retain sizes, not benchmark payload copies. Data is disposable and
        # per-cell cleanup prevents a long sweep exhausting the shared disk.
        lab.close()
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


def run(output, smoke=False, reads_only=False):
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    sources = {str(p.relative_to(EXPERIMENT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in (EXPERIMENT / "engine/src").rglob("*.rs")}
    provenance = dict(native_source="88793e76595d69be300731b9b25c58538923a53b", client_source="93a1a066a511ad2ce5114dc429afb1fd0f6d99bf",
        client_unmodified=True, native_unmodified=True, adaptation_sources=sources,
        driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        binaries={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in (CLIENT,NATIVE,BINARY)},
        sut_cpus="0-3 aggregate across replicas", client_cpus="4-7", memory="shared 16 GiB host, no separate quota",
        disk="shared orb root filesystem; same 8 MiB WAL segments, no cold tier", workers_per_process=2,
        wal_shards_or_partitions=2, cpu_hz=os.sysconf("SC_CLK_TCK"), page_bytes=os.sysconf("SC_PAGE_SIZE"),
        native_durability="local fsync", raft1_durability="one-member local fsync", raft3_durability="quorum fsync on shared host",
        read_consistency="replicated default linearizable", tail_cache_bytes=0,
        qualification="short local windows, client not independently calibrated; no cloud capacity headline")
    (output / "provenance.json").write_text(json.dumps(provenance,indent=2)+"\n")
    (output / "host.txt").write_text(subprocess.run(["uname","-a"],capture_output=True,text=True,check=True).stdout+
                                     Path("/proc/cpuinfo").read_text()+Path("/proc/meminfo").read_text())
    workloads = [("write",n,c) for n in (1,1024) for c in (4,16,64,256)]
    workloads += [("fanout",n) for n in (1,100,1000)] + [("reads",), ("mixed",)]
    if smoke:
        workloads = [("write",1,4)]
    if reads_only:
        workloads = [("reads",), ("mixed",)]
    results = []
    for i, workload in enumerate(workloads):
        # Rotate arm order to reduce a systematic page-cache/time-order bias.
        arms = ["native","raft1","raft3"]
        arms = arms[i%3:]+arms[:i%3]
        for arm in arms:
            name = f"{output.name}-{arm}-"+"-".join(map(str,workload))
            results.append(cell(output / name, arm, workload))
    if not smoke:
        for arm in ("native","raft1","raft3"):
            for workload in (("reads",),) if reads_only else (("write",1024,64),("reads",)):
                name = f"{output.name}-{arm}-profile-"+"-".join(map(str,workload))
                results.append(cell(output / name,arm,workload,profile=True))
    (output / "results.json").write_text(json.dumps(results,indent=2)+"\n")
    return all(r["verdict"] == "PASS" for r in results)


if __name__ == "__main__":
    sys.exit(0 if run(sys.argv[1], "--smoke" in sys.argv[2:], "--reads-only" in sys.argv[2:]) else 1)
