import unittest

from history_stats import quantiles, summarize


def invoke(identity, ms, function="append"):
    return {"type": "invoke", "id": identity, "f": function, "time_ns": ms * 1_000_000}


def complete(identity, ms, kind, latency, function="append"):
    return {"type": kind, "id": identity, "f": function, "time_ns": ms * 1_000_000,
            "value": {"latency_ms": latency}}


class StatsTest(unittest.TestCase):
    def test_rank_is_not_mean_and_empty_sample_is_not_zero(self):
        self.assertEqual(quantiles([100, 1, 3, 2]), {"p50": 2, "p95": 100, "p99": 100, "max": 100})
        self.assertTrue(all(value is None for value in quantiles([]).values()))

    def test_retries_keep_logical_wait_and_separate_histories_can_reuse_ids(self):
        result = summarize([
            [invoke("a", 1000), complete("a", 2000, "unknown", 1000),
             invoke("a", 2500, "append-retry"), complete("a", 3000, "ok", 500, "append-retry")],
            [invoke("a", 1000), complete("a", 1100, "ok", 100),
             invoke("b", 2000), complete("b", 2100, "fail", 100)],
        ])
        self.assertEqual(result["acknowledged_logical_appends"], 2)
        self.assertEqual(result["attempt_outcomes"], {"ok": 2, "fail": 1, "unknown": 1})
        self.assertEqual(result["append_window_s"], 2)
        self.assertEqual(result["acknowledged_appends_per_s"], 1)
        self.assertEqual(result["attempt_latency_ms_including_errors"]["p99"], 1000)
        self.assertEqual(result["acknowledged_logical_latency_ms_including_retries"]["p99"], 2000)

    def test_unknown_only_does_not_report_successful_latency(self):
        result = summarize([[invoke("a", 0), complete("a", 1000, "unknown", 1000)]])
        self.assertEqual(result["acknowledged_appends_per_s"], 0)
        self.assertIsNone(result["acknowledged_logical_latency_ms_including_retries"]["p99"])
