import unittest

from benchmark_summary import summarize_progress


class ProgressSummary(unittest.TestCase):
    def test_commit_windows_are_per_observation_and_entry_gaps_are_not_acks(self):
        def observation(start, end, size, last, applied, matched, snapshot):
            return dict(node=1, unix_ms=start, end_unix_ms=end, head_status=200,
                        committed_bytes=size, groups=[dict(last_log_index=last,
                        last_applied=dict(index=applied), snapshot=dict(index=snapshot),
                        replication={"1": dict(index=last), "2": dict(index=matched), "3": None})])
        samples = [dict(replicas=[observation(1100, 1200, 256, 40, 35, 34, 32)]),
                   dict(replicas=[observation(2000, 2200, 1280, 50, 42, 47, 42),
                       dict(node=2, unix_ms=2200, end_unix_ms=2210, error="unavailable")]),
                   # Collection begins inside the window but finishes outside it.
                   dict(replicas=[observation(2800, 3100, 100000, 1000, 42, 42, 42)])]
        result = summarize_progress(samples, 1000, 3000)
        node = result["nodes"]["1"]
        self.assertEqual(node["samples"], 2)
        self.assertEqual(node["committed_bytes_per_second"], 1024)
        self.assertEqual(node["max_collection_ms"], 200)
        self.assertEqual(result["nodes"]["2"], dict(errors=1, samples=0))
        group = result["groups"]["1/0"]
        self.assertEqual(group["unapplied_entries"], dict(samples=2, first=5, last=8, minimum=5, maximum=8))
        self.assertEqual(group["matched_gap_entries"], dict(samples=4, first=0, last=3, minimum=0, maximum=6))
        self.assertEqual(group["observed_snapshot_indices"], [32, 42])

    def test_one_head_or_missing_metrics_does_not_invent_a_rate(self):
        result = summarize_progress([dict(replicas=[dict(node=4, unix_ms=10, end_unix_ms=11,
            head_status=200, committed_bytes=512, groups=[dict(last_log_index=None, last_applied=None)])])], 0, 20)
        self.assertEqual(result["nodes"]["4"]["samples"], 1)
        self.assertNotIn("committed_bytes_per_second", result["nodes"]["4"])
        self.assertEqual(result["groups"], {})


if __name__ == "__main__":
    unittest.main()
