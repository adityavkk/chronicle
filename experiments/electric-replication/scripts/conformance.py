"""Run the unchanged 0.3.5 suite, including its optional subscription block.

Install pinned suite first:
  pnpm --dir .tmp/electric-conformance add --save-exact @durable-streams/server-conformance-tests@0.3.5
No cloud, test filters, custom skips, or protocol proxy. Three real replicas;
two independent partitions, with leaders initially co-located for the suite's
single direct HTTP endpoint. Fault campaigns exercise independent leader placement.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

from lab import Lab, ROOT, EXPERIMENT, BINARY, source_hashes


def run(output, partitions=2, cold=False):
    package = (ROOT / ".tmp/electric-conformance/node_modules/@durable-streams/server-conformance-tests").resolve()
    metadata = json.loads((package / "package.json").read_text())
    assert metadata["version"] == "0.3.5"
    vitest = package.parent.parent / "vitest/vitest.mjs"
    if cold:
        from cold_faults import ColdLab, MINIO
        lab = ColdLab(output, partitions=partitions)
        lab.segment_bytes = 1024
    else:
        lab = Lab(output, partitions=partitions, port=19400)
    hashes = source_hashes()
    hashes["binary"] = hashlib.sha256(BINARY.read_bytes()).hexdigest()
    if cold:
        hashes["minio"] = hashlib.sha256(MINIO.read_bytes()).hexdigest()
    for path in sorted((package / "dist").glob("*.js")):
        hashes["suite/" + path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    hashes["suite/package.json"] = hashlib.sha256((package / "package.json").read_bytes()).hexdigest()
    hashes["lockfile"] = hashlib.sha256((EXPERIMENT / "engine/Cargo.lock").read_bytes()).hexdigest()
    (lab.output / "provenance.json").write_text(json.dumps(dict(suite=metadata["version"],
        subscriptions=True, processes=3, partitions=partitions, initial_leaders=1,
        cold="local MinIO S3, 1024-byte segments" if cold else "disabled (hot-only)",
        consistency="linearizable (default)", durability="quorum-fsync (default)", hashes=hashes), indent=2)+"\n")
    try:
        if cold:
            lab.start_storage()
        for node in (1, 2, 3):
            lab.start(node)
        for group in range(partitions):
            assert lab.admin(1, group, "init", lab.genesis) == {"Ok": None}
            lab.wait(lambda: lab.leader(group) == 1, "elect co-located initial leaders")
        leader = lab.leader(0)
        env = dict(os.environ, CONFORMANCE_TEST_URL=f"http://127.0.0.1:{lab.port+leader}")
        command = ["node", str(vitest), "run", str(EXPERIMENT / "conformance.test.ts"),
                   "--reporter=default", "--reporter=json", f"--outputFile={lab.output / 'suite.json'}"]
        (lab.output / "command.json").write_text(json.dumps(command)+"\n")
        with open(lab.output / "suite.txt", "w") as log:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        if cold:
            states = []
            for n in (1,2,3):
                for g in range(partitions):
                    try:
                        states.append(dict(node=n,group=g,state=lab.cold(n,g)[1]))
                    except Exception as error:
                        # Preserve the full suite ledger even if a replica died.
                        states.append(dict(node=n,group=g,error=type(error).__name__))
            (lab.output / "cold-coverage.json").write_text(json.dumps(dict(states=states,
                note="HTTP Channel counters distinguish native cold bodies from offloader verification reads; suite is unchanged, not forced onto cold for every case."),indent=2)+"\n")
        if (lab.output / "suite.json").exists():
            data = json.loads((lab.output / "suite.json").read_text())
            counts = {k: data.get(k) for k in ("numTotalTests", "numPassedTests", "numFailedTests", "numPendingTests", "numTodoTests")}
            (lab.output / "counts.json").write_text(json.dumps(counts, indent=2)+"\n")
            ledger = [dict(name=test["fullName"], status=test["status"], reasons=test.get("failureMessages", []))
                      for suite in data["testResults"] for test in suite["assertionResults"]
                      if test["status"] != "passed"]
            (lab.output / "failing-ledger.json").write_text(json.dumps(ledger, indent=2)+"\n")
            print(json.dumps(counts))
            if counts != dict(numTotalTests=332, numPassedTests=332, numFailedTests=0,
                              numPendingTests=0, numTodoTests=0):
                return result.returncode or 1
        else:
            return result.returncode or 1
        return result.returncode
    finally:
        lab.close()


if __name__ == "__main__":
    sys.exit(run(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 2,
                 cold=len(sys.argv) > 3 and sys.argv[3] == "cold"))
