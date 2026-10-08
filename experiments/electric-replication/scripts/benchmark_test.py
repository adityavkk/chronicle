import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from benchmark import cell, committed_target, run, workload_streams


class WorkloadReconciliation(unittest.TestCase):
    def test_oracle_includes_seed_and_successes_not_attempts_or_observed_offsets(self):
        raw = dict(ok_total_all_phases=11, counts=dict(ok=7, backpressure=3, other_err=1),
                   write_counts=dict(ok=19, backpressure=2, other_err=5),
                   append_counts=dict(ok=23, backpressure=7, other_err=2),
                   events_sent=32, events_received=100, committed_bytes=999999)
        self.assertEqual(committed_target(("write", 3, 4), raw), 2816)
        self.assertEqual(committed_target(("mixed",), raw), 4199168)
        self.assertEqual(committed_target(("fanout", 1000), raw), 5888)
        self.assertEqual(committed_target(("reads",), raw), 16777216)
        self.assertEqual(workload_streams(("write", 3, 4)), ["s00000000", "s00000001", "s00000002"])
        self.assertEqual(workload_streams(("mixed",))[15], "s000015")
        self.assertEqual(workload_streams(("reads",)), ["bench-reads-stream-0", "bench-reads-stream-1",
                                                       "bench-reads-stream-2", "bench-reads-stream-3"])
        self.assertEqual(workload_streams(("fanout", 1000)), ["fanout"])
        with self.assertRaises(KeyError):
            committed_target(("write", 3, 4), {"counts": {"ok": 7}})
        for callback in (workload_streams, lambda workload: committed_target(workload, raw)):
            with self.assertRaises(ValueError):
                callback(("unknown",))


class DiskPreflight(unittest.TestCase):
    def test_headroom_boundary_precedes_startup_and_preserves_failure(self):
        minimum = 8 * 1024**3
        for free in (minimum - 1, minimum):
            with self.subTest(free=free), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / Path(directory).name
                binary = Path(directory) / "native"
                binary.write_bytes(b"fixture-not-executable")
                with patch("benchmark.NATIVE", binary), \
                     patch("benchmark.shutil.disk_usage", return_value=SimpleNamespace(free=free)), \
                     patch("benchmark.subprocess.run", side_effect=RuntimeError("SUT startup intercepted")) as start, \
                     contextlib.redirect_stdout(io.StringIO()):
                    result = cell(output, "native", ("write", 1, 4))
                self.assertEqual(result["verdict"], "FAIL")
                self.assertEqual(result["disk_preflight"]["free_bytes"], free)
                self.assertTrue((output / "failure.txt").exists())
                if free < minimum:
                    start.assert_not_called()
                    self.assertIn("no SUT started", result["error"])
                    self.assertFalse((output / "native-argv.json").exists())
                else:
                    start.assert_called_once()
                    self.assertIn("SUT startup intercepted", result["error"])
                    self.assertTrue((output / "native-argv.json").exists())


class BaselineComparison(unittest.TestCase):
    def test_hash_fence_and_exact_binary_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = {name: root / name for name in ("native", "client", "current", "baseline")}
            for name, path in binaries.items():
                path.write_bytes(name.encode())
            provenance = root / "baseline.json"
            provenance.write_text(json.dumps({"hashes": {"binary": hashlib.sha256(b"current").hexdigest()}}))
            with self.assertRaisesRegex(ValueError, "does not match"):
                run(root / "bad", baseline_binary=binaries["baseline"], baseline_provenance=provenance)
            self.assertFalse((root / "bad").exists())
            provenance.write_text(json.dumps({"hashes": {"binary": hashlib.sha256(b"baseline").hexdigest()}}))
            with patch("benchmark.ROOT", root), patch("benchmark.NATIVE", binaries["native"]), \
                 patch("benchmark.CLIENT", binaries["client"]), patch("benchmark.BINARY", binaries["current"]), \
                 patch("benchmark.source_hashes", return_value={}), \
                 patch("benchmark.cell", return_value={"verdict": "PASS"}) as execute:
                self.assertTrue(run(root / "good", write_diagnostics=True, duration_secs=17,
                    write_streams=1024, write_connections=23, write_warmup_secs=0,
                    baseline_binary=binaries["baseline"], baseline_provenance=provenance))
            arms = [call.args[1] for call in execute.call_args_list]
            self.assertEqual(arms, ["native", "raft1-baseline", "raft1", "raft1-baseline", "raft1", "native",
                                    "raft1", "native", "raft1-baseline"])
            for call in execute.call_args_list:
                expected = binaries["baseline"] if call.args[1].endswith("-baseline") else binaries["current"]
                self.assertEqual(call.kwargs["binary"], expected)
                self.assertEqual(call.kwargs["duration"], 17)
                self.assertEqual(call.args[2], ("write", 1024, 23))
                self.assertEqual(call.kwargs["write_warmup_secs"], 0)
            recorded = json.loads((root / "good/provenance.json").read_text())
            self.assertEqual(recorded["baseline"]["hashes"]["binary"], hashlib.sha256(b"baseline").hexdigest())
            self.assertEqual(recorded["binaries"]["current"], hashlib.sha256(b"current").hexdigest())
            self.assertEqual(recorded["write_warmup_secs"], 0)

    def test_read_matrix_keeps_all_four_arms_repeats_domains_and_bounds(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "binary"
            binary.write_bytes(b"not-executable")
            with patch("benchmark.ROOT", root), patch("benchmark.NATIVE", binary), \
                 patch("benchmark.CLIENT", binary), patch("benchmark.BINARY", binary), \
                 patch("benchmark.source_hashes", return_value={}), \
                 patch("benchmark.cell", return_value={"verdict": "PASS"}) as execute:
                self.assertTrue(run(root / "read", read_diagnostics=True, duration_secs=37))
            calls = execute.call_args_list
            self.assertEqual(len(calls), 60)
            self.assertEqual(len({call.args[0] for call in calls}), 60)
            for arm in ("native", "raft1", "raft3-local", "raft3"):
                for workload in (("fanout", 1), ("fanout", 100), ("fanout", 1000), ("reads",), ("mixed",)):
                    chosen = [call for call in calls if call.args[1:3] == (arm, workload)]
                    self.assertEqual(len(chosen), 3)
                    self.assertEqual({call.args[0].name.rsplit("-", 1)[-1] for call in chosen},
                                     {"repeat1", "repeat2", "repeat3"})
            for call in calls:
                self.assertEqual(call.kwargs["duration"], 37)
                self.assertTrue(call.kwargs["progress"])
                self.assertTrue(call.kwargs["diagnostics"])
                self.assertFalse(call.kwargs["profile"])
                self.assertEqual(call.kwargs["pending_commands"], 1024)
            self.assertNotEqual([call.args[1] for call in calls[:4]], [call.args[1] for call in calls[20:24]])

    def test_invalid_or_ignored_write_controls_fail_before_creating_a_run(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "not-started"
            for arguments in (dict(async_writes=True, write_connections=0),
                              dict(async_writes=True, write_streams=0),
                              dict(async_writes=True, write_warmup_secs=-1),
                              dict(reads_only=True, write_connections=17),
                              dict(read_diagnostics=True, async_writes=True),
                              dict(read_diagnostics=True, write_connections=17)):
                with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                    run(output, **arguments)
                self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
