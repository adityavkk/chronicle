import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from benchmark import cell, run


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

    def test_invalid_or_ignored_write_controls_fail_before_creating_a_run(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "not-started"
            for arguments in (dict(async_writes=True, write_connections=0),
                              dict(async_writes=True, write_streams=0),
                              dict(async_writes=True, write_warmup_secs=-1),
                              dict(reads_only=True, write_connections=17)):
                with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                    run(output, **arguments)
                self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
