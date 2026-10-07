import contextlib
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from benchmark import cell


class DiskPreflight(unittest.TestCase):
    def test_headroom_boundary_precedes_startup_and_preserves_failure(self):
        minimum = 8 * 1024**3
        for free in (minimum - 1, minimum):
            with self.subTest(free=free), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / Path(directory).name
                with patch("benchmark.shutil.disk_usage", return_value=SimpleNamespace(free=free)), \
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


if __name__ == "__main__":
    unittest.main()
