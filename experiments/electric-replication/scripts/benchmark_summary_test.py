import unittest

from benchmark_summary import summarize_phase_timings, summarize_progress, summarize_sync_timings


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


class PhaseTimingSummary(unittest.TestCase):
    def observation(self, when, count, ns, first_bucket):
        return dict(unix_ms=when, phase="read", cumulative=dict(count=count, total_ns=ns,
            max_ns=2000, bytes=count*11, buckets=[first_bucket, count-first_bucket]))

    def test_cumulative_samples_are_differenced_only_inside_the_window(self):
        values = [self.observation(50, 3, 3000, 2), self.observation(125, 5, 5000, 3),
                  self.observation(175, 9, 12000, 4), self.observation(225, 20, 28000, 7)]
        row = summarize_phase_timings(values, 100, 200)["read"]
        self.assertEqual(row["whole_invocation"], values[-1]["cumulative"])
        self.assertEqual(row["sampled_window"], dict(start_unix_ms=125, end_unix_ms=175,
            count=4, total_ns=7000, bytes=44, buckets=[1, 3]))
        self.assertFalse(row["counter_reset_detected"])
        self.assertIsNone(summarize_phase_timings(values, 126, 200)["read"]["sampled_window"])

    def test_a_process_counter_reset_is_not_a_negative_duration_or_whole_run_total(self):
        values = [self.observation(125, 50, 50000, 30), self.observation(175, 2, 2500, 1)]
        row = summarize_phase_timings(values, 100, 200)["read"]
        self.assertTrue(row["counter_reset_detected"])
        self.assertIsNone(row["whole_invocation"])
        self.assertIsNone(row["sampled_window"])


class SyncTimingSummary(unittest.TestCase):
    def test_loops_and_calls_are_distinct_and_lifetime_max_is_not_a_window_max(self):
        values = [dict(unix_ms=50, count=4, calls=7, total_ns=1000, max_ns=700),
                  dict(unix_ms=110, count=8, calls=11, total_ns=2000, max_ns=700),
                  dict(unix_ms=190, count=13, calls=20, total_ns=3300, max_ns=700),
                  dict(unix_ms=220, count=17, calls=24, total_ns=5300, max_ns=900)]
        row = summarize_sync_timings(values, 100, 200)
        self.assertEqual(row["whole_invocation"], values[-1])
        self.assertFalse(row["counter_reset_detected"])
        self.assertEqual(row["sampled_window"], dict(start_unix_ms=110, end_unix_ms=190,
                                                   count=5, calls=9, total_ns=1300))
        self.assertIsNone(summarize_sync_timings(values, 120, 200)["sampled_window"])
        reset = [values[2], values[0]]
        row = summarize_sync_timings(reset, 0, 200)
        self.assertTrue(row["counter_reset_detected"])
        self.assertIsNone(row["whole_invocation"])
        self.assertIsNone(row["sampled_window"])


if __name__ == "__main__":
    unittest.main()
