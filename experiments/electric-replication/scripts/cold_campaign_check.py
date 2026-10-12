"""Recheck this retained cold campaign, including its deliberately non-green ledger.

Run after all workloads stop. Exit zero means the evidence agrees with the
reported outcomes, NOT that performance acceptance passed. No server is started.
The selected-pair map preserves all original failed and superseded cells.
"""
import copy
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys

from cold_benchmark_check import read_lines, read_object_events, validate
from lab import BINARY, EXPERIMENT, source_hashes


def sha(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2)+"\n")


def check(output):
    evidence = EXPERIMENT/"evidence"
    output.mkdir(parents=True, exist_ok=False)
    expected = {
        "cold-bench-001": (118, 120),
        "cold-bench-repair-001": (2, 2),
        "cold-bench-envelope-001": (6, 6),
        "cold-profile-kernel-001": (7, 8),
        "cold-profile-kernel-repair-001": (1, 2),
        "cold-profile-cpu-001": (8, 8),
        "cold-profile-syscalls-001": (8, 8),
        "cold-profile-fsync-001": (4, 8),
        "cold-profile-fsync-002": (8, 8),
        "cold-generations-001": (22, 24),
        "cold-generations-repair-001": (4, 4),
    }
    runs, commands, inputs = {}, [], {}
    for name, counts in expected.items():
        generation = name.startswith("cold-generations-")
        script = "cold_generations_check.py" if generation else "cold_benchmark_check.py"
        command = [sys.executable, str(Path(__file__).with_name(script)), str(evidence/name)]
        with open(output/(name+".txt"), "w") as log:
            code = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT).returncode
        commands.append(dict(argv=command, exit_code=code))
        save(output/"commands.json", commands)
        path = evidence/name/("checked.json" if generation else "summary.json")
        value = json.loads(path.read_text())
        assert (value["passed"], value["total"]) == counts, (name, "unexpected ledger")
        assert code == int(counts[0] != counts[1]), (name, "unexpected checker exit")
        runs[name] = value
        inputs[str(path.relative_to(evidence))] = sha(path)

    baseline = json.loads((evidence/"cold-runtime-tests-001/qualification.json").read_text())
    assert source_hashes() == baseline["sources"]
    assert sha(BINARY) == baseline["release_binary_sha256"]
    binaries = None
    for name in runs:
        path = evidence/name/"provenance.json"
        provenance = json.loads(path.read_text())
        assert provenance["sources"] == baseline["sources"], name
        current = provenance.get("binaries", provenance.get("binary"))
        assert current and (binaries is None or current == binaries), name
        binaries = current
        inputs[str(path.relative_to(evidence))] = sha(path)
    conformance = {}
    for name in ("conformance-036", "conformance-cold-004"):
        counts = json.loads((evidence/name/"counts.json").read_text())
        assert counts == dict(numTotalTests=332, numPassedTests=332, numFailedTests=0,
                              numPendingTests=0, numTodoTests=0), name
        provenance = json.loads((evidence/name/"provenance.json").read_text())
        assert provenance["subscriptions"] and provenance["suite"] == "0.3.5"
        assert all(provenance["hashes"][k] == v for k, v in baseline["sources"].items())
        assert provenance["hashes"]["binary"] == baseline["release_binary_sha256"]
        old = json.loads((evidence/("conformance-cold-003" if "cold" in name else "conformance-035")/"provenance.json").read_text())
        assert {k:v for k,v in provenance["hashes"].items() if k.startswith("suite/")} == {
            k:v for k,v in old["hashes"].items() if k.startswith("suite/")}
        conformance[name] = counts

    selected = {(r["arm"], r["cold"], r["workload"], r["repetition"]): r
                for r in runs["cold-bench-001"]["cells"]}
    for row in runs["cold-bench-repair-001"]["cells"]:
        key = row["arm"], row["cold"], row["workload"], row["repetition"]
        assert key in selected and key[0] == "native" and key[2:] == ("many-write", 2)
        selected[key] = row
    assert len(selected) == 120 and sum(r["verdict"] == "PASS" for r in selected.values()) == 119
    failed = next(r for r in selected.values() if r["verdict"] != "PASS")
    assert (failed["arm"], failed["cold"], failed["workload"], failed["repetition"]) == ("async3", False, "many-write", 3)
    assert failed["counts"]["backpressure"] == 10 and failed["counts"]["other_err"] == 0

    generations = {r["cell"].removeprefix("cold-generations-001-"): r
                   for r in runs["cold-generations-001"]["cells"]}
    for row in runs["cold-generations-repair-001"]["cells"]:
        key = row["cell"].removeprefix("cold-generations-repair-001-")
        assert key in generations and key in {a+"-"+t+"-r2" for a in ("quorum3", "async3") for t in ("hot", "cold")}
        generations[key] = row
    assert len(generations) == 24 and all(r["verdict"] == "PASS" for r in generations.values())
    assert all(all(r["negative_controls"].values()) for r in generations.values())

    # Reject actual captured outcomes after changing one independently checked
    # fact. These are in addition to generated unit checks and generation mutations.
    controls = {}
    for workload in ("replay", "mixed", "fanout"):
        name = "cold-bench-001-"+workload+"-quorum3-cold-r1"
        path = evidence/"cold-bench-001"/name
        row = json.loads((path/"result.json").read_text())
        raw = json.loads((path/"client.json").read_text())
        samples = read_lines(path/"samples.jsonl.gz")
        objects = read_object_events(path)
        assert validate(row, raw, objects, samples)["verdict"] == "PASS"
        mutations = {
            "missing-replica": lambda r: r["drain"].pop(),
            "corrupt-byte-hash": lambda r: r["probes"][0].update(sha256="corrupt"),
            "offset-off-by-one": lambda r: r["probes"][0].update(offset=r["probes"][0]["bytes"]-1),
            "not-reclaimed": lambda r: r["precondition"].update(local_payload_bytes=1),
            "wrong-attribution": lambda r: r.update(client_object_keys=["/not-the-client-object"]),
        }
        if workload == "fanout":
            mutations["false-closure"] = lambda r: r["sequence"]["readers"][0].update(closed=False)
            mutations["duplicate-reader"] = lambda r: r["sequence"]["readers"][1].update(reader=0)
        for kind, mutation in mutations.items():
            altered = copy.deepcopy(row)
            mutation(altered)
            verdict = validate(altered, raw, objects, samples)
            assert verdict["verdict"] == "FAIL", (name, kind)
            controls[name+"/"+kind] = verdict["errors"]

    groups = {}
    for row in selected.values():
        groups.setdefault((row["arm"], row["cold"], row["workload"]), []).append(row)
    aggregates = []
    for (arm, cold, workload), rows in sorted(groups.items()):
        passed = sum(r["verdict"] == "PASS" for r in rows)
        aggregates.append(dict(arm=arm, cold=cold, workload=workload, passed=passed, total=len(rows),
            median_ops_per_sec=statistics.median(r["ops_per_sec"] for r in rows) if passed == len(rows) else None,
            cells=[r["cell"] for r in rows],
            ops_per_sec=[r["ops_per_sec"] for r in rows],
            latency=[r["latency"] for r in rows],
            peak_sut_rss_bytes=[r["resources"]["sut"]["peak_rss_bytes"] for r in rows],
            peak_provider_rss_bytes=[r["resources"]["provider"]["peak_rss_bytes"] for r in rows],
            sut_cpu_cores=[r["resources"]["sut"]["cpu_cores"] for r in rows],
            client_remote_fetched_bytes=[r["objects"]["client_remote_fetched_bytes"] for r in rows]))
    result = dict(evidence_consistency="PASS", performance_acceptance="NOT_MET", runtime_changed=False,
        binary_sha256=sha(BINARY), source_inputs=len(baseline["sources"]), conformance=conformance,
        original_ledgers={n:dict(passed=v["passed"], total=v["total"]) for n,v in runs.items()},
        selected_benchmark=dict(passed=119,total=120,cells=[r["cell"] for r in selected.values()]),
        selected_generations=dict(passed=24,total=24,cells=[r["cell"] for r in generations.values()],
            negative_controls=sum(len(r["negative_controls"]) for r in generations.values())),
        actual_benchmark_mutations=controls, aggregates=aggregates, input_reports_sha256=inputs,
        checker_sha256={p.name:sha(p) for p in (Path(__file__), Path(__file__).with_name("cold_benchmark_check.py"),
                            Path(__file__).with_name("cold_generations_check.py"), Path(__file__).with_name("benchmark_summary.py"))},
        limitations=["Selected replacements are complete hot/cold pairs; originals remain failures, not erased.",
            "c4 envelope and perturbed profiles are separate, never replacements for c16 capacity.",
            "Zero API errors and exact replication drain do not establish sustainable cold offload.",
            "Read/mixed/fanout remote observations use invocation windows; ds-bench lacks exact drive bounds.",
            "All processes and MinIO share one host/disk; no independent-provider/power-loss claim."])
    save(output/"checked.json", result)
    print(json.dumps({k:result[k] for k in ("evidence_consistency", "performance_acceptance", "source_inputs", "original_ledgers")}, indent=2))


if __name__ == "__main__":
    check(Path(sys.argv[1]).resolve())
