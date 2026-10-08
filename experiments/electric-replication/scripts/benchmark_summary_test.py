import contextlib
import gzip
import io
import json
from pathlib import Path
import random
import tempfile
import unittest

from benchmark_summary import check_clock, kernel_fsync_event, summarize, summarize_fsync_trace, summarize_kernel_fsync, summarize_phase_timings, summarize_progress, summarize_sync_timings, summarize_write_outcomes


class KernelFsyncTrace(unittest.TestCase):
    def test_redaction_keeps_only_fd_not_unused_syscall_registers(self):
        enter = kernel_fsync_event("21/37 80.123456789: raw_syscalls:sys_enter: NR 74 (a, deadbeef, 1234, 5678, 0, 0)")
        self.assertEqual(enter, dict(pid=21, tid=37, monotonic_ns=80_123_456_789,
                                    phase="enter", syscall="fsync", fd=10))
        self.assertEqual(kernel_fsync_event("21/37 81.000000001: raw_syscalls:sys_exit: NR 75 = -5"),
                         dict(pid=21, tid=37, monotonic_ns=81_000_000_001,
                              phase="exit", syscall="fdatasync", result=-5))
        for line in ("LOST 17 events", "21/37 1.000000001: raw_syscalls:sys_enter: NR 1 (a, 0)",
                     "21/37 1.0001: raw_syscalls:sys_exit: NR 74 = 0", "sensitive unexpected line"):
            with self.assertRaisesRegex(ValueError, "invalid kernel fsync event") as raised:
                kernel_fsync_event(line)
            self.assertNotIn(line, str(raised.exception))

    def test_generated_interleavings_match_per_thread_not_neighbouring_events(self):
        for seed in range(48):
            rng = random.Random(seed)
            events, durations, errors = [], [], {}
            for tid in range(6):
                start = rng.randrange(1_000_000)
                for _ in range(30):
                    duration = rng.choice([0, 99_999_999, 100_000_000, rng.randrange(200_000_000)])
                    code = rng.choice([0, 0, 0, -5, -28])
                    syscall = rng.choice(["fsync", "fdatasync"])
                    base = dict(pid=tid//2+1, tid=tid+10, syscall=syscall)
                    events.append(dict(base, monotonic_ns=start, phase="enter", fd=3))
                    events.append(dict(base, monotonic_ns=start+duration, phase="exit", result=code))
                    start += duration+rng.randrange(1, 1_000_000)
                    if code: errors[str(code)] = errors.get(str(code), 0)+1
                    else: durations.append(duration)
            events.sort(key=lambda row: row["monotonic_ns"])
            summary = summarize_kernel_fsync(events)
            ordered = sorted(durations)
            self.assertEqual(summary["started"], 180)
            self.assertEqual(summary["matched"], 180)
            self.assertEqual(summary["incomplete"], [])
            self.assertEqual(summary["unmatched_exits"], 0)
            self.assertEqual(summary["errors"], errors)
            self.assertEqual(summary["successful"], dict(count=len(durations), total_ns=sum(durations),
                p50_ns=ordered[(len(ordered)+1)//2-1], p99_ns=ordered[(len(ordered)*99+99)//100-1],
                max_ns=max(durations), at_least_100ms=sum(n >= 100_000_000 for n in durations)))

    def test_partial_capture_is_not_invented_duration_and_conflicts_fail(self):
        base = dict(pid=7, tid=11, syscall="fsync")
        enter = dict(base, phase="enter", monotonic_ns=50, fd=8)
        end = dict(base, phase="exit", monotonic_ns=60, result=0)
        partial = summarize_kernel_fsync([end, dict(enter, monotonic_ns=70)])
        self.assertEqual(partial["unmatched_exits"], 1)
        self.assertEqual(partial["incomplete"], [dict(enter, monotonic_ns=70)])
        self.assertIsNone(partial["successful"])
        for rows in ([enter, enter], [enter, dict(end, syscall="fdatasync")],
                     [enter, dict(end, monotonic_ns=49)]):
            with self.assertRaises(ValueError):
                summarize_kernel_fsync(rows)


class FsyncTrace(unittest.TestCase):
    def test_interleaved_resumes_errors_and_partial_tail_keep_distinct_outcomes(self):
        lines = ["81 10.000001 fsync(3 <unfinished ...>",
                 "82 10.000002 fdatasync(4) = 0 <0.099999>",
                 "81 10.000003 <... fsync resumed>) = 0 <0.100000>",
                 "82 10.000004 fsync(4) = -1 EIO (Input/output error) <0.700000>",
                 "81 10.000005 fsync(3) = 0 <0.000017>",
                 "82 10.000006 fdatasync(4 <unfinished ...>"]
        result = summarize_fsync_trace(lines)
        self.assertEqual(result["started"], 5)
        self.assertEqual(result["completed"], 4)
        self.assertEqual(result["incomplete_by_tid"], {"82": "fdatasync"})
        self.assertEqual(result["resumed_without_start"], 0)
        self.assertEqual(result["errors"], {"EIO": 1})
        self.assertEqual(result["successful"], dict(count=3, total_us=200016, mean_us=66672,
            p50_us=99999, p99_us=100000, max_us=100000, at_least_100ms=1))
        unmatched = summarize_fsync_trace(["81 10.001000 <... fsync resumed>) = 0 <0.000100>"])
        self.assertEqual(unmatched["started"], 0)
        self.assertEqual(unmatched["completed"], 1)
        self.assertEqual(unmatched["resumed_without_start"], 1)

    def test_bad_trace_does_not_silently_discard_missing_or_conflicting_calls(self):
        for lines in (["garbage"], ["81 10.1 fsync(3) = 0"],
                      ["81 10.1 fsync(3 <unfinished ...>", "81 10.2 fdatasync(4 <unfinished ...>"],
                      ["81 10.1 fsync(3 <unfinished ...>", "81 10.2 <... fdatasync resumed>) = 0 <0.000001>"],
                      ["81 10.1 fsync(3 <unfinished ...>", "81 10.2 fsync(4) = 0 <0.000001>"]):
            with self.subTest(lines=lines), self.assertRaises(ValueError):
                summarize_fsync_trace(lines)
        self.assertIsNone(summarize_fsync_trace(["81 10.1 fsync(3) = -1 EINVAL (Invalid argument) <0.000001>"])["successful"])


class ClockIntegrity(unittest.TestCase):
    def test_steps_in_either_direction_and_reversed_steps_are_not_hidden_by_endpoints(self):
        base = [dict(unix_ms=1000+i*500, monotonic_ns=7_000_000_000+i*500_000_000) for i in range(6)]
        self.assertEqual(check_clock(base)["status"], "STABLE")
        for start in range(1, 5):
            for end in range(start+1, 7):
                for jump in (-75428, -101, 101, 75428):
                    values = [dict(row, unix_ms=row["unix_ms"]+(jump if start <= i < end else 0))
                              for i, row in enumerate(base)]
                    with self.subTest(start=start, end=end, jump=jump):
                        result = check_clock(values)
                        self.assertEqual(result["status"], "INVALID")
                        self.assertEqual(result["discontinuities"][0]["offset_drift_ms"], jump)
        # A missing/regressed monotonic clock must not be interpreted as a
        # zero-duration or a trustworthy wall-only rate.
        self.assertEqual(check_clock([])["status"], "UNVERIFIED")
        self.assertEqual(check_clock([base[0]])["status"], "UNVERIFIED")
        self.assertEqual(check_clock([base[0], {"unix_ms": 2000}])["status"], "UNVERIFIED")
        self.assertEqual(check_clock([base[1], base[0]])["status"], "INVALID")

    def test_invalid_clock_omits_rates_and_aligned_windows_but_retains_results(self):
        for jump in (0, 75428, -75428):
            with self.subTest(jump=jump), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                cell = root / "clock"
                cell.mkdir()
                (root / "provenance.json").write_text(json.dumps(dict(page_bytes=4096, cpu_hz=100)))
                (cell / "result.json").write_text(json.dumps(dict(arm="raft1", workload=["mixed"],
                    profile=False, diagnostics=True, verdict="PASS", progress_sampling=True, node_pids=[1],
                    client_window=dict(start_unix_ms=1000, end_unix_ms=4000), drain=dict(target_bytes=768))))
                raw = dict(write_counts=dict(ok=3), drive_secs=2)
                (cell / "client.json").write_text(json.dumps(raw))
                with gzip.open(cell / "samples.jsonl.gz", "wt") as out:
                    for i, elapsed in enumerate((0, 500, 2000)):
                        wall = 1100+elapsed+(jump if i == 1 else 0)
                        sample = dict(unix_ms=wall, monotonic_ns=7_000_000_000+elapsed*1_000_000,
                            processes={"1": dict(cpu_ticks=10+elapsed//20, rss_pages=7,
                                io=dict(write_bytes=elapsed*2, read_bytes=0))}, sockets=[],
                            replicas=[dict(node=1, unix_ms=wall, end_unix_ms=wall+1,
                                head_status=200, committed_bytes=i*384)])
                        out.write(json.dumps(sample)+'\n')
                with gzip.open(cell / "node-1.log.gz", "wt") as out:
                    for when, count in ((1100, 9), (3100, 17)):
                        out.write('RAFT_WRITE_OUTCOMES '+json.dumps(dict(schema_version=1,
                            unix_ms=when, cumulative=dict(applied_commands=count)))+'\n')
                with contextlib.redirect_stdout(io.StringIO()):
                    summarize(root)
                row = json.loads((root / "summary.json").read_text())["rows"][0]
                self.assertEqual(row["verdict"], "PASS")  # original protocol check, not timing qualification
                self.assertEqual(row["raw_client"], raw)
                self.assertEqual(row["drain"], dict(target_bytes=768))
                outcome = row["write_outcomes"]["nodes"]["node-1.log.gz"]
                self.assertEqual(outcome["whole_invocation"], dict(applied_commands=17))
                if jump:
                    self.assertEqual(row["clock_integrity"]["status"], "INVALID")
                    self.assertNotIn("sampled_seconds", row["resources"])
                    self.assertNotIn("average_sut_cpu_cores", row["resources"])
                    self.assertEqual(row["progress"]["nodes"], {})
                    self.assertIsNone(outcome["sampled_window"])
                else:
                    self.assertEqual(row["clock_integrity"]["status"], "STABLE")
                    self.assertEqual(row["resources"]["sampled_seconds"], 2)
                    self.assertEqual(row["resources"]["average_sut_cpu_cores"], 0.5)
                    self.assertEqual(row["progress"]["nodes"]["1"]["committed_bytes_per_second"], 384)
                    self.assertEqual(outcome["sampled_window"]["cumulative_delta"], dict(applied_commands=8))


class ProgressSummary(unittest.TestCase):
    def test_commit_windows_are_per_observation_and_entry_gaps_are_not_acks(self):
        def observation(start, end, size, last, applied, matched, snapshot):
            return dict(node=1, unix_ms=start, end_unix_ms=end, head_status=200,
                        sampled_stream_indices=[0, 1023], total_streams=1024,
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
        self.assertEqual(node["sampled_stream_indices"], [0, 1023])
        self.assertEqual(node["total_streams"], 1024)
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

    def test_stable_indices_cannot_hide_changed_stream_names(self):
        rows = [dict(node=1, unix_ms=10, end_unix_ms=11, head_status=200,
                     sampled_stream_indices=[0], total_streams=1, sampled_stream_names=[name])
                for name in ("fanout", "s00000000")]
        with self.assertRaisesRegex(AssertionError, "stream names changed"):
            summarize_progress([dict(replicas=rows)], 0, 20)


class NonWriteDiagnostics(unittest.TestCase):
    def test_mixed_keeps_counters_without_fabricating_all_phase_counts_or_windows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cell = root / "mixed"
            cell.mkdir()
            (root / "provenance.json").write_text("{}")
            (cell / "result.json").write_text(json.dumps(dict(arm="raft3-local", workload=["mixed"],
                profile=False, diagnostics=True, verdict="PASS", diagnostic_tail_captured=True,
                client_window=dict(start_unix_ms=100, end_unix_ms=300))))
            (cell / "client.json").write_text(json.dumps(dict(write_counts=dict(ok=100), drive_secs=1)))
            with gzip.open(cell / "node-1.log.gz", "wt") as log:
                log.write('WAL_CONT staged/s=17 fsync/s=9\n')
                log.write('RAFT_WRITE_OUTCOMES '+json.dumps(dict(schema_version=1, unix_ms=120,
                    cumulative=dict(applied_commands=71, lease_expired=4)))+'\n')
            with contextlib.redirect_stdout(io.StringIO()):
                summarize(root)
            row = json.loads((root / "summary.json").read_text())["rows"][0]
            self.assertEqual(row["wal_diagnostics"]["fsyncs"], 9)
            self.assertEqual(row["wal_diagnostics"]["staged_records"], 17)
            self.assertIsNone(row["wal_diagnostics"]["client_acks_all_phases"])
            self.assertIsNone(row["wal_diagnostics"]["fsyncs_per_ack"])
            self.assertIsNone(row["wal_diagnostics"]["records_per_ack"])
            self.assertEqual(row["write_outcomes"]["nodes"]["node-1.log.gz"]["whole_invocation"],
                             dict(applied_commands=71, lease_expired=4))
            self.assertEqual(row["sample_window"], dict(start_unix_ms=100, end_unix_ms=300,
                             scope="outer client invocation; includes non-measure work"))


class WriteOutcomeSummary(unittest.TestCase):
    def test_sparse_counter_deltas_keep_stage_units_and_window_boundaries(self):
        def row(when, **counters):
            return dict(schema_version=1, unix_ms=when, cumulative=counters)
        values = [row(90, lease_expired=2, applied_commands=70),
                  row(120, lease_expired=3, applied_commands=101),
                  row(230, lease_expired=7, applied_commands=179, byte_bound=8),
                  row(310, lease_expired=70, applied_commands=200, byte_bound=9)]
        result = summarize_write_outcomes(values, 100, 300)
        self.assertFalse(result["counter_reset_detected"])
        self.assertEqual(result["sampled_window"], dict(start_unix_ms=120, end_unix_ms=230,
            cumulative_delta=dict(lease_expired=4, applied_commands=78, byte_bound=8)))
        self.assertEqual(result["whole_invocation"], dict(lease_expired=70, applied_commands=200, byte_bound=9))
        self.assertIsNone(summarize_write_outcomes(values, 121, 300)["sampled_window"])

    def test_counter_reset_or_unknown_schema_cannot_produce_a_successful_total(self):
        values = [dict(schema_version=1, unix_ms=120, cumulative=dict(applied_commands=900, count_bound=3)),
                  dict(schema_version=1, unix_ms=230, cumulative=dict(applied_commands=4))]
        result = summarize_write_outcomes(values, 100, 300)
        self.assertTrue(result["counter_reset_detected"])
        self.assertIsNone(result["whole_invocation"])
        self.assertIsNone(result["sampled_window"])
        values[1]["schema_version"] = 2
        with self.assertRaises(AssertionError):
            summarize_write_outcomes(values, 100, 300)


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
