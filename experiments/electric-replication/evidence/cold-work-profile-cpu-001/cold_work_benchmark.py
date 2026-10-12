"""Matched before/after cold scheduler cells using the existing pinned ds-bench driver.

Does not weaken cold_benchmark.run's historical source gate. This campaign owns
separate manifests and explicitly selects its baseline or candidate executable.
All original cell checks, failed histories and all-replica drains remain active.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import cold_benchmark as bench
from lab import ROOT, EXPERIMENT, source_hashes

BASELINE = ROOT / ".tmp/electric-tools/cold-schedule-baseline/durable-streams-server"
CANDIDATE = bench.BINARY
ORIGINAL_LAB = bench.BenchLab


def run(args):
    os.sched_setaffinity(0, {4, 5})
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    previous = json.loads((EXPERIMENT / "evidence/cold-runtime-tests-001/qualification.json").read_text())
    assert bench.digest(BASELINE) == previous["release_binary_sha256"], "baseline executable changed"
    for repository, pin in (("electric", "88793e76595d69be300731b9b25c58538923a53b"),
                            ("ds-bench", "93a1a066a511ad2ce5114dc429afb1fd0f6d99bf")):
        path = ROOT / ".tmp/electric-sources" / repository
        assert subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip() == pin
        assert not subprocess.check_output(["git", "-C", str(path), "status", "--porcelain"])
    bench.SEGMENT = args.segment_bytes
    shutil.copyfile(__file__, output / Path(__file__).name)
    provenance = dict(version=1, argv=sys.argv, base_commit="a75aa98c59c27aa4cf8f7df3ade56986665d55e5",
        runtime_sources=dict(baseline=previous["sources"], candidate=source_hashes()),
        binaries={str(p.relative_to(ROOT)): bench.digest(p) for p in (BASELINE, CANDIDATE, bench.NATIVE, bench.CLIENT, bench.MINIO)},
        harness=bench.archive_harness(output), driver_sha256=bench.digest(__file__),
        segment_bytes=args.segment_bytes, duration=args.duration, connections=args.connections,
        profile=args.profile,
        cpus=dict(sut="0-3 aggregate", provider="4-5", client="6-7"),
        scope="local shared-host MinIO; diagnostic, not independent-host or provider durability; no profiling overlap")
    bench.save(output / "provenance.json", provenance)
    results = []
    choices = [(variant, arm, tier == "cold") for variant in args.variants for arm in args.arms for tier in args.tiers]
    for repetition in range(1, args.repetitions + 1):
        for wi, workload in enumerate(args.workloads):
            rotate = (repetition + wi - 1) % len(choices)
            for variant, arm, cold in choices[rotate:] + choices[:rotate]:
                executable = BASELINE if variant == "baseline" else CANDIDATE

                class SelectedLab(ORIGINAL_LAB):
                    def __init__(self, *pos, **kw):
                        super().__init__(*pos, **kw)
                        self.binary = executable

                    def seed(self, names, size, content_type="application/octet-stream"):
                        if args.segment_bytes == 64*1024:
                            return super().seed(names, size, content_type)
                        # A large storage segment is not a large protocol POST.
                        # Identical bounded chunks on native/baseline/candidate.
                        def one(name):
                            path = bench.PREFIX + name
                            assert self.request(1,"PUT",path,headers={"content-type":content_type,**bench.STRONG})[0] == 201
                            payload = bench.seeded(name,size)
                            for i in range(0,size,512*1024):
                                assert self.request(1,"POST",path,payload[i:i+512*1024],
                                    {"content-type":content_type,**bench.STRONG},timeout=40)[0] in (200,204)
                        with bench.ThreadPoolExecutor(max_workers=8) as pool:
                            list(pool.map(one,names))

                    def observe(self):
                        row = super().observe()
                        if self.arm != "native":
                            # Same number of observer RPCs on both binaries.
                            row["pressure"] = {}
                            for n in sorted(self.nodes):
                                status, _, body = self.request(n, "POST", "/_admin/0/pressure", b"null",
                                    {"x-electric-cluster":self.cluster})
                                assert status == (200 if variant == "candidate" else 404)
                                row["pressure"][str(n)] = json.loads(body) if status == 200 else None
                        return row

                bench.BenchLab = SelectedLab
                bench.BINARY = executable
                name = f"{output.name}-{variant}-{workload}-{arm}-{'cold' if cold else 'hot'}-r{repetition}"
                result = bench.cell(output / name, arm, cold, workload, repetition, args.duration, args.connections, args.profile)
                result["variant"] = variant
                bench.save(output / name / "result.json", result)
                results.append(dict(cell=name, variant=variant, arm=arm, workload=workload,
                    verdict=result["verdict"], error=result.get("error")))
                bench.save(output / "ledger.json", results)
    assert source_hashes() == provenance["runtime_sources"]["candidate"], "source changed during campaign"
    return all(r["verdict"] == "PASS" for r in results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    parser.add_argument("--variants", nargs="+", choices=["baseline", "candidate"], default=["baseline", "candidate"])
    parser.add_argument("--arms", nargs="+", choices=["native", "raft1", "quorum3", "async3"], default=["raft1", "quorum3", "async3"])
    parser.add_argument("--tiers", nargs="+", choices=["hot", "cold"], default=["cold"])
    parser.add_argument("--workloads", nargs="+", choices=["hot-write", "many-write", "replay", "fanout", "mixed"], default=["hot-write", "many-write"])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--duration", type=int, default=24)
    parser.add_argument("--connections", type=int, default=4)
    parser.add_argument("--segment-bytes", type=int, default=64*1024)
    parser.add_argument("--profile", choices=["none", "cpu", "fsync", "kernel", "syscalls"], default="none")
    options = parser.parse_args()
    assert 1 <= options.repetitions <= 3 and 4 <= options.duration <= 180 and 1 <= options.connections <= 256
    assert options.segment_bytes in (64*1024, 8*1024*1024)
    assert options.segment_bytes == 64*1024 or options.workloads == ["hot-write"], "8 MiB high-cardinality prefill exceeds this orb's bounded data budget"
    raise SystemExit(0 if run(options) else 1)
