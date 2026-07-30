from __future__ import annotations

import csv
import importlib.util
from pathlib import Path
import tempfile
import unittest


MODULE_PATH = Path(__file__).with_name("aggregate_chronicle_samples.py")
SPEC = importlib.util.spec_from_file_location("aggregate_chronicle_samples", MODULE_PATH)
assert SPEC and SPEC.loader
aggregate_samples = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(aggregate_samples)


class TestAggregateChronicleSamples(unittest.TestCase):
    def test_sums_process_and_pod_values_but_counts_device_writes_once(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            samples = root / "raw"
            samples.mkdir()
            header = "ts_ms,rss_bytes,cpu_ticks,write_bytes,pod_ws_bytes\n"
            (samples / "chronicle-a.csv").write_text(
                header
                + "1001,10,1,100,20\n"
                + "2001,12,3,200,22\n"
                + "3001,14,5,300,24\n"
            )
            (samples / "redis-a.csv").write_text(
                header
                + "1050,30,2,100,40\n"
                + "2050,32,6,200,42\n"
                + "3050,34,10,300,44\n"
            )
            output = root / "samples.csv"
            count = aggregate_samples.aggregate(samples, output)

            self.assertEqual(count, 3)
            with output.open(newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(rows[1]["rss_bytes"], "44")
            self.assertEqual(rows[1]["cpu_ticks"], "9")
            self.assertEqual(rows[1]["write_bytes"], "200")
            self.assertEqual(rows[1]["pod_ws_bytes"], "64")

    def test_requires_overlapping_buckets(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            header = "ts_ms,rss_bytes,cpu_ticks,write_bytes,pod_ws_bytes\n"
            (root / "a.csv").write_text(header + "1000,1,1,1,1\n")
            (root / "b.csv").write_text(header + "2000,1,1,1,1\n")
            with self.assertRaises(ValueError):
                aggregate_samples.aggregate(root, root / "out.csv")


if __name__ == "__main__":
    unittest.main()
