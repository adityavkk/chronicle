from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


MODULE_PATH = Path(__file__).resolve().parents[1] / "dsbench.py"
SPEC = importlib.util.spec_from_file_location("chronicle_dsbench", MODULE_PATH)
assert SPEC and SPEC.loader
dsbench = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = dsbench
SPEC.loader.exec_module(dsbench)


def write_calibration_results(root: Path, throughputs: dict[str, list[float]]) -> None:
    for label, values in throughputs.items():
        target = root / label
        target.mkdir(parents=True)
        cell = {
            "status": "ok",
            "saturated": True,
            "reason": "plateau",
            "pinned_pods": 1,
            "throughput": values[-1],
            "confirmed_throughputs": values,
        }
        (target / "cells.json").write_text(json.dumps({"cells": {"10000": cell}}))
        for index, value in enumerate(values, 1):
            rep = target / "cells" / "chronicle" / "n10000" / f"p1-r{index}"
            fleet = rep / "fleet"
            fleet.mkdir(parents=True)
            (rep / "merged.json").write_text(
                json.dumps(
                    {
                        "aggregate_ops_per_sec": value,
                        "windows_aligned": True,
                        "pods_reported": 1,
                    }
                )
            )
            (fleet / "write-0.json").write_text(
                json.dumps(
                    {
                        "counts": {"other_err": 0, "backpressure": 0},
                        "lazy_creates": 0,
                    }
                )
            )


class TestResources(unittest.TestCase):
    def test_fixed_four_cpu_sixteen_gib_budget(self):
        dsbench.ChronicleSplit(2_000, 2_000).validate()

    def test_rejects_cpu_overcommit(self):
        with self.assertRaises(dsbench.HarnessError):
            dsbench.ChronicleSplit(3_000, 2_000).validate()

    def test_rejects_memory_overcommit(self):
        with self.assertRaises(dsbench.HarnessError):
            dsbench.ChronicleSplit(2_000, 2_000, 8_192, 12_288).validate()

    def test_smoke_suite_is_valid(self):
        suite = MODULE_PATH.parent / "overlay" / "suites" / "chronicle-smoke.json"
        dsbench.validate_chronicle_suite(suite)

    def test_parses_fixed_budget_shared_topology(self):
        topology = dsbench.parse_topology_args(
            "always:2:1:2000:2000:4096:12288:legacy"
        )
        self.assertIsNotNone(topology)
        assert topology is not None
        self.assertEqual(topology.kind, "shared")
        self.assertEqual(topology.chronicle_per_pod.cpu_millis, 1000)
        self.assertEqual(topology.redis_per_pod.cpu_millis, 2000)
        self.assertEqual(topology.effective_cpu_millis, 4000)

    def test_parses_cluster_topology_without_rounding_up(self):
        topology = dsbench.parse_topology_args(
            "always:4:3:2000:2000:4096:12288:persistent"
        )
        self.assertIsNotNone(topology)
        assert topology is not None
        self.assertEqual(topology.kind, "cluster")
        self.assertEqual(topology.redis_per_pod.cpu_millis, 666)
        self.assertEqual(topology.effective_cpu_millis, 3998)
        self.assertEqual(topology.effective_memory_mib, 16384)
        self.assertEqual(topology.sse_wait_mode, "persistent")

    def test_rejects_invalid_topology_shape_or_budget(self):
        invalid = (
            "always:3:1:2000:2000:4096:12288:legacy",
            "always:2:2:2000:2000:4096:12288:legacy",
            "always:2:1:2500:2000:4096:12288:legacy",
            "always:2:1:2000:2000:4096:12288:unknown",
        )
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(dsbench.HarnessError):
                dsbench.parse_topology_args(value)

    def test_legacy_config_is_not_reinterpreted_as_topology(self):
        self.assertIsNone(dsbench.parse_topology_args("always:2:2"))


class TestQuotaChecks(unittest.TestCase):
    def test_quota_requirement_accounts_for_live_usage(self):
        requirement = dsbench._quota_requirement(
            "N2D_CPUS",
            64,
            limit=80,
            usage=20,
            dimensions={"region": "europe-west4"},
        )
        self.assertFalse(requirement["ok"])
        self.assertEqual(requirement["available"], 60)

    def test_unlimited_quota_passes(self):
        requirement = dsbench._quota_requirement(
            "LOCAL_SSD_TOTAL_GB",
            375,
            limit=-1,
        )
        self.assertTrue(requirement["ok"])
        self.assertEqual(requirement["available"], "unlimited")

    def test_dimension_limit_distinguishes_absent_quota(self):
        info = {
            "dimensionsInfos": [
                {
                    "dimensions": {"region": "europe-west4", "vm_family": "C4D"},
                    "details": {"value": "24"},
                },
                {
                    "dimensions": {"region": "europe-west4", "vm_family": "C4"},
                    "details": {},
                },
            ]
        }
        self.assertEqual(
            dsbench._dimension_limit(
                info,
                {"region": "europe-west4", "vm_family": "C4D"},
            ),
            24,
        )
        self.assertIsNone(
            dsbench._dimension_limit(
                info,
                {"region": "europe-west4", "vm_family": "C4"},
            )
        )


class TestImageReuse(unittest.TestCase):
    def test_reuses_only_matching_source_and_registry_digest(self):
        source = {"commit": "a" * 40, "diff_sha256": "b" * 64}
        digest = "sha256:" + "c" * 64
        registry = "europe-west1-docker.pkg.dev/example/ds-bench"
        candidate = {
            "reference": f"{registry}/chronicle@{digest}",
            "digest": digest,
            "source": source,
        }
        self.assertTrue(
            dsbench._reusable_image(candidate, source, registry=registry)
        )
        self.assertFalse(
            dsbench._reusable_image(
                candidate,
                {**source, "diff_sha256": "d" * 64},
                registry=registry,
            )
        )
        self.assertFalse(
            dsbench._reusable_image(candidate, source, registry="other.example/repo")
        )

    def test_chronicle_cloud_context_contains_only_runtime_packages(self):
        with tempfile.TemporaryDirectory() as raw:
            context = dsbench._temporary_chronicle_build_context(
                dsbench.REPO_ROOT, Path(raw)
            )
            self.assertTrue((context / "Dockerfile").is_file())
            self.assertTrue((context / "cmd" / "chronicle" / "main.go").is_file())
            self.assertTrue((context / "webhook" / "scripts" / "ack.lua").is_file())
            self.assertTrue((context / "store" / "redis" / "scripts" / "append.lua").is_file())
            for excluded in (
                ".copybara",
                "benchmarks",
                "docs",
                "deploy",
                "Dockerfile.chronicle",
                "kitt.yml",
            ):
                self.assertFalse((context / excluded).exists())

    def test_generic_cloud_context_identity_matches_uploaded_files(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "source"
            source.mkdir()
            (source / "src").mkdir()
            (source / "src" / "lib.rs").write_text("pub fn value() -> u8 { 1 }\n")
            (source / "Cargo.toml").write_text("[package]\nname = \"fixture\"\n")
            (source / "target").mkdir()
            (source / "target" / "ignored").write_text("first\n")
            dockerfile = root / "fixture.Dockerfile"
            dockerfile.write_text("FROM scratch\n")

            first = dsbench.docker_build_context_identity(source, dockerfile)
            self.assertEqual(first["build_context_file_count"], 4)

            (source / "target" / "ignored").write_text("second\n")
            self.assertEqual(
                dsbench.docker_build_context_identity(source, dockerfile),
                first,
            )

            (source / "src" / "lib.rs").write_text("pub fn value() -> u8 { 2 }\n")
            self.assertNotEqual(
                dsbench.docker_build_context_identity(source, dockerfile)[
                    "build_context_sha256"
                ],
                first["build_context_sha256"],
            )


class TestVerdicts(unittest.TestCase):
    @staticmethod
    def nonwrite_suite(workload, **config):
        section = "mixed" if workload.startswith("mixed-") else "reads"
        return {
            "benchmark_meta": {"workload": workload},
            section: config,
        }

    def test_valid_plateau(self):
        verdict = dsbench.validate_write_cell(
            {
                "status": "ok",
                "saturated": True,
                "reason": "plateau",
                "errors": 0,
                "lazy_creates": 0,
                "windows_aligned": True,
                "client_bound": False,
            }
        )
        self.assertTrue(verdict.valid)

    def test_rejects_every_headline_disqualifier(self):
        verdict = dsbench.validate_write_cell(
            {
                "status": "error",
                "saturated": False,
                "reason": "ladder_exhausted",
                "errors": 2,
                "lazy_creates": 1,
                "windows_aligned": False,
                "client_bound": True,
            }
        )
        self.assertFalse(verdict.valid)
        self.assertEqual(verdict.errors, 2)
        self.assertEqual(verdict.lazy_creates, 1)
        self.assertIn("client_bound", verdict.reasons)
        self.assertIn("windows_not_aligned", verdict.reasons)

    def test_raw_write_evidence_is_required_and_checked(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            rep = (
                root
                / "chronicle-redis-aof-always"
                / "cells"
                / "chronicle"
                / "n10000"
                / "p2-r1"
            )
            fleet = rep / "fleet"
            fleet.mkdir(parents=True)
            (rep / "merged.json").write_text(
                json.dumps({"windows_aligned": True, "pods_reported": 2})
            )
            for index in range(2):
                (fleet / f"write-{index}.json").write_text(
                    json.dumps(
                        {
                            "counts": {"other_err": 0, "backpressure": 0},
                            "lazy_creates": 0,
                        }
                    )
                )
            cell = {
                "status": "ok",
                "saturated": True,
                "reason": "plateau",
                "pinned_pods": 2,
            }
            verdict = dsbench.validate_write_artifacts(
                root,
                label="chronicle-redis-aof-always",
                mode="chronicle",
                stream_count=10000,
                cell=cell,
                required_repeats=1,
            )
            self.assertTrue(verdict.valid)

            bad = json.loads((fleet / "write-0.json").read_text())
            bad["lazy_creates"] = 1
            (fleet / "write-0.json").write_text(json.dumps(bad))
            verdict = dsbench.validate_write_artifacts(
                root,
                label="chronicle-redis-aof-always",
                mode="chronicle",
                stream_count=10000,
                cell=cell,
                required_repeats=1,
            )
            self.assertFalse(verdict.valid)
            self.assertEqual(verdict.lazy_creates, 1)

    def test_headline_requires_three_confirmation_repeats(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            rep = (
                root
                / "chronicle-redis-aof-always"
                / "cells"
                / "chronicle"
                / "n10000"
                / "p1-r1"
            )
            fleet = rep / "fleet"
            fleet.mkdir(parents=True)
            (rep / "merged.json").write_text(
                json.dumps({"windows_aligned": True, "pods_reported": 1})
            )
            (fleet / "write-0.json").write_text(
                json.dumps(
                    {
                        "counts": {"other_err": 0, "backpressure": 0},
                        "lazy_creates": 0,
                    }
                )
            )
            verdict = dsbench.validate_write_artifacts(
                root,
                label="chronicle-redis-aof-always",
                mode="chronicle",
                stream_count=10000,
                cell={
                    "status": "ok",
                    "saturated": True,
                    "reason": "plateau",
                    "pinned_pods": 1,
                },
            )
            self.assertFalse(verdict.valid)
            self.assertIn("confirmation_reps=1/3", verdict.reasons)

    def test_catchup_requires_exact_seed_bytes_per_operation(self):
        suite = self.nonwrite_suite("reads-catchup", seed_bytes=16_777_216)
        valid = {
            "status": "ok",
            "ops_per_sec": 10.0,
            "bytes_per_sec": 167_772_160.0,
        }
        classification, reasons, _, _ = dsbench.classify_nonwrite_row(suite, valid)
        self.assertEqual(classification, "result")
        self.assertEqual(reasons, [])

        short = {**valid, "bytes_per_sec": 167_731_200.0}
        classification, reasons, _, _ = dsbench.classify_nonwrite_row(suite, short)
        self.assertEqual(classification, "invalid")
        self.assertIn("catchup_seed_bytes_per_op", reasons[0])

    def test_catchup_prefers_exact_post_write_seed_verification(self):
        seed_bytes = 16_777_216
        suite = self.nonwrite_suite("reads-catchup", seed_bytes=seed_bytes)
        row = {
            "status": "error",
            "stream_count": 10,
            "ops_per_sec": 10.0,
            "bytes_per_sec": 167_567_360.0,
            "p99": 100.0,
        }
        evidence = [
            {
                "seed_bytes": seed_bytes,
                "seed_verified_streams": 10,
                "seed_verified_min_bytes": seed_bytes,
                "seed_verified_max_bytes": seed_bytes,
            }
        ]

        classification, reasons, _, _ = dsbench.classify_nonwrite_row(
            suite,
            row,
            catchup_seed_evidence=evidence,
        )
        self.assertEqual(classification, "result")
        self.assertEqual(reasons, [])

    def test_catchup_exact_seed_does_not_hide_missing_reads(self):
        seed_bytes = 16_777_216
        suite = self.nonwrite_suite("reads-catchup", seed_bytes=seed_bytes)
        row = {
            "status": "error",
            "stream_count": 10,
            "ops_per_sec": 0.0,
            "bytes_per_sec": 0.0,
            "p99": None,
        }
        evidence = [
            {
                "seed_bytes": seed_bytes,
                "seed_verified_streams": 10,
                "seed_verified_min_bytes": seed_bytes,
                "seed_verified_max_bytes": seed_bytes,
            }
        ]

        classification, reasons, _, _ = dsbench.classify_nonwrite_row(
            suite,
            row,
            catchup_seed_evidence=evidence,
        )
        self.assertEqual(classification, "invalid")
        self.assertIn("status=error", reasons)

    def test_under_offered_or_error_row_is_valid_overload_observation(self):
        suite = self.nonwrite_suite(
            "reads-sse",
            append_rate_per_sec=50,
        )
        row = {
            "status": "ok",
            "connections": 100,
            "ops_per_sec": 1_250,
            "other_err": 2,
        }
        classification, reasons, offered, ratio = dsbench.classify_nonwrite_row(
            suite, row
        )
        self.assertEqual(classification, "overload")
        self.assertEqual(offered, 5_000)
        self.assertEqual(ratio, 0.25)
        self.assertIn("errors=2", reasons)
        self.assertIn("completion_ratio=0.2500<0.9800", reasons)

    def test_non_ok_row_is_invalid(self):
        suite = self.nonwrite_suite(
            "mixed-writes",
            writers_per_stream=1,
            writer_rate=5,
        )
        row = {
            "status": "error",
            "stream_count": 10_000,
            "writer_rate": 5,
            "write_ops_per_sec": 50_000,
        }
        classification, reasons, _, _ = dsbench.classify_nonwrite_row(suite, row)
        self.assertEqual(classification, "invalid")
        self.assertIn("status=error", reasons)


class TestCalibration(unittest.TestCase):
    def write_results(self, root: Path, throughputs: dict[str, list[float]]) -> None:
        write_calibration_results(root, throughputs)

    def test_selects_highest_valid_median(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write_results(
                root,
                {
                    "chronicle-cal-1-3": [80, 85, 90],
                    "chronicle-cal-2-2": [100, 105, 110],
                    "chronicle-cal-3-1": [90, 95, 100],
                },
            )
            selected = dsbench.select_calibration(root)
            self.assertEqual(selected["selected"]["split"], "2:2")

    def test_rejects_tie(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write_results(
                root,
                {
                    "chronicle-cal-1-3": [100, 100, 100],
                    "chronicle-cal-2-2": [100, 100, 100],
                    "chronicle-cal-3-1": [90, 90, 90],
                },
            )
            with self.assertRaises(dsbench.HarnessError):
                dsbench.select_calibration(root)

    def test_uses_raw_confirmation_repeats_for_median(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write_results(
                root,
                {
                    "chronicle-cal-1-3": [1, 1, 1],
                    "chronicle-cal-2-2": [1, 1, 1],
                    "chronicle-cal-3-1": [1, 1, 1],
                },
            )
            values = {
                "chronicle-cal-1-3": [80, 90, 85],
                "chronicle-cal-2-2": [100, 110, 105],
                "chronicle-cal-3-1": [90, 95, 92],
            }
            for label, samples in values.items():
                cells_path = root / label / "cells.json"
                data = json.loads(cells_path.read_text())
                data["cells"]["10000"]["pinned_pods"] = 4
                data["cells"]["10000"].pop("confirmed_throughputs")
                cells_path.write_text(json.dumps(data))
                reps = root / label / "cells" / "chronicle" / "n10000"
                reps.mkdir(parents=True, exist_ok=True)
                for index, value in enumerate(samples, 1):
                    target = reps / f"p4-r{index}"
                    target.mkdir()
                    (target / "merged.json").write_text(
                        json.dumps(
                            {
                                "aggregate_ops_per_sec": value,
                                "windows_aligned": True,
                                "pods_reported": 4,
                            }
                        )
                    )
                    fleet = target / "fleet"
                    fleet.mkdir()
                    for pod in range(4):
                        (fleet / f"write-{pod}.json").write_text(
                            json.dumps(
                                {
                                    "counts": {"other_err": 0, "backpressure": 0},
                                    "lazy_creates": 0,
                                }
                            )
                        )
            selected = dsbench.select_calibration(root)
            self.assertEqual(selected["selected"]["split"], "2:2")
            self.assertEqual(selected["selected"]["median_throughput"], 105)


class TestCampaign(unittest.TestCase):
    def test_paid_screen_matches_current_definitions_and_build_contexts(self):
        screen = json.loads((MODULE_PATH.parent / "paid-screen.json").read_text())
        definitions = {
            "fixed-budget-topology-screen": dsbench.SCALING_FILE,
            "sse-wait-diagnostic": dsbench.SSE_DIAGNOSTIC_FILE,
            "sse-hub-p0-validation-4cpu": (
                MODULE_PATH.parent / "sse-hub-validation.json"
            ),
        }
        for campaign in screen["campaigns"]:
            self.assertEqual(
                campaign["definition_sha256"],
                dsbench.sha256_file(definitions[campaign["name"]]),
            )
        self.assertEqual(
            screen["image_build"]["ds_bench_adapter_sha256"],
            dsbench.adapter_digest(),
        )
        self.assertEqual(
            screen["image_build"]["chronicle_build_context_sha256"],
            dsbench.chronicle_build_identity(dsbench.REPO_ROOT)[
                "build_context_sha256"
            ],
        )

    def test_resolves_six_topology_screen_suites(self):
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "resolved"
            resolved = dsbench.resolve_scaling_campaign(output)
            index = json.loads((resolved / "index.json").read_text())
            self.assertEqual(index["profile"], "chronicle-topology-screen-4cpu")
            self.assertEqual(index["suite_count"], 6)
            self.assertIsNone(index["chronicle_split"])
            for record in index["suites"]:
                suite_path = resolved / record["path"]
                dsbench.validate_chronicle_suite(suite_path)
                suite = json.loads(suite_path.read_text())
                configs = suite["server_configs"]["chronicle"]
                self.assertEqual(len(configs), 6)
                for config, metadata in zip(
                    configs, suite["benchmark_meta"]["configs"], strict=True
                ):
                    topology = dsbench.parse_topology_args(config["args"])
                    self.assertIsNotNone(topology)
                    assert topology is not None
                    self.assertEqual(
                        metadata["topology"]["effective"]["cpu_millis"],
                        topology.effective_cpu_millis,
                    )
                    self.assertEqual(
                        metadata["topology"]["per_pod"]["redis"]["cpu_millis"],
                        topology.redis_per_pod.cpu_millis,
                    )

    def test_resolves_separate_sse_wait_diagnostic(self):
        with tempfile.TemporaryDirectory() as raw:
            resolved = dsbench.resolve_sse_diagnostic(Path(raw) / "resolved")
            index = json.loads((resolved / "index.json").read_text())
            self.assertEqual(index["suite_count"], 2)
            self.assertEqual(index["profile"], "chronicle-sse-wait-diagnostic-4cpu")
            for record in index["suites"]:
                suite = json.loads((resolved / record["path"]).read_text())
                self.assertEqual(
                    [
                        config["label"]
                        for config in suite["server_configs"]["chronicle"]
                    ],
                    [
                        "chronicle-c1-r1-legacy",
                        "chronicle-c1-r1-sse-persistent",
                    ],
                )

    def test_resolves_larger_cpu_campaigns_from_explicit_definitions(self):
        definitions = (
            ("scaleup-8.json", 8_000, "chronicle-c1-r1-cpu8"),
            ("scaleup-16.json", 16_000, "chronicle-c1-r1-cpu16"),
        )
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            for filename, expected_cpu, expected_label in definitions:
                campaign_file = MODULE_PATH.parent / filename
                resolved = dsbench.resolve_scaling_campaign(
                    root / filename.removesuffix(".json"),
                    campaign_file=campaign_file,
                )
                index = json.loads((resolved / "index.json").read_text())
                self.assertEqual(index["suite_count"], 5)
                for record in index["suites"]:
                    suite = json.loads((resolved / record["path"]).read_text())
                    self.assertEqual(
                        suite["benchmark_meta"]["resource_budget"]["cpu_millis"],
                        expected_cpu,
                    )
                    self.assertEqual(
                        suite["cluster"]["server_cpus"],
                        expected_cpu // 1000,
                    )
                    self.assertEqual(
                        suite["server_configs"]["chronicle"][0]["label"],
                        expected_label,
                    )
                    topology = dsbench.parse_topology_args(
                        suite["server_configs"]["chronicle"][0]["args"],
                        dsbench.ResourceBudget(
                            **suite["benchmark_meta"]["resource_budget"]
                        ),
                    )
                    self.assertIsNotNone(topology)

    def test_scaling_manifest_freezes_targets_and_reference_seals(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            campaign = json.loads(dsbench.SCALING_FILE.read_text())
            references = []
            for index in range(2):
                archive = root / f"reference-{index}"
                archive.mkdir()
                (archive / "manifest.json").write_text(
                    json.dumps({"campaign_id": f"reference-{index}"}) + "\n"
                )
                (archive / "evidence.json").write_text(
                    json.dumps({"reference": index}) + "\n"
                )
                seal = dsbench.seal_archive(archive)
                references.append(
                    {
                        "path": str(archive),
                        "evidence_tree_sha256": seal["tree_sha256"],
                    }
                )
            campaign["reference_archives"] = references
            campaign_file = root / "scaling.json"
            campaign_file.write_text(json.dumps(campaign))
            resolved = dsbench.resolve_scaling_campaign(
                root / "resolved",
                campaign_file=campaign_file,
            )
            digest = "sha256:" + "a" * 64
            client_source = dsbench.dsbench_build_identity(
                dsbench.prepare_checkout()
            )
            images = {
                "schema": "chronicle-ds-bench-images-v1",
                "target": "remote",
                "project": "example-project",
                "images": {
                    name: {
                        "reference": f"registry.example/{name}@{digest}",
                        "digest": digest,
                        **(
                            {
                                "source": dsbench.chronicle_build_identity(
                                    dsbench.REPO_ROOT
                                )
                            }
                            if name == "chronicle"
                            else {"source": client_source}
                            if name == "ds-bench"
                            else {}
                        ),
                    }
                    for name in (
                        "chronicle",
                        "ds-bench",
                        "rust",
                        "node",
                        "ursula",
                        "redis",
                    )
                },
            }
            images_path = root / "images.json"
            images_path.write_text(json.dumps(images))
            manifest = dsbench.create_scaling_manifest(
                resolved,
                images_path,
                output=root / "manifest.json",
                campaign_file=campaign_file,
            )
            self.assertEqual(manifest["execution_kind"], "scaling")
            self.assertNotIn("calibration", manifest["campaign"])
            self.assertEqual(len(manifest["campaign"]["candidate_order"]), 6)
            self.assertEqual(len(manifest["campaign"]["reference_archives"]), 2)
            self.assertTrue(
                all(
                    item["topology"]["effective"]["cpu_millis"] >= 3998
                    for item in manifest["campaign"]["candidate_order"]
                )
            )

    def test_scaling_evaluator_selects_smallest_candidate_that_meets_all_gates(self):
        with tempfile.TemporaryDirectory() as raw:
            archive = Path(raw)

            def topology(replicas: int) -> dict[str, object]:
                return {
                    "kind": "shared",
                    "chronicle_replicas": replicas,
                    "redis_masters": 1,
                    "requested": {
                        "chronicle_cpu_millis": 2000,
                        "redis_cpu_millis": 2000,
                        "chronicle_memory_mib": 4096,
                        "redis_memory_mib": 12288,
                    },
                    "effective": {
                        "cpu_millis": 4000,
                        "memory_mib": 16384,
                    },
                }

            manifest = {
                "execution_kind": "scaling",
                "campaign_id": "scale-test",
                "campaign": {
                    "target": {
                        "throughput_ratio": 0.8,
                        "completion_ratio": 0.98,
                        "latency_ratio": 2.0,
                        "memory_ratio": 2.0,
                    },
                    "reference_archives": [
                        {"path": "/reference", "campaign_id": "reference"}
                    ],
                    "candidate_order": [
                        {
                            "label": "c1",
                            "args": "always:1:1:2000:2000:4096:12288:legacy",
                            "topology": topology(1),
                        },
                        {
                            "label": "c2",
                            "args": "always:2:1:2000:2000:4096:12288:legacy",
                            "topology": topology(2),
                        },
                    ],
                    "suites": [],
                },
            }
            (archive / "manifest.json").write_text(json.dumps(manifest))
            candidate_cells = {
                "cells": [
                    {
                        "system": "chronicle",
                        "label": "c1",
                        "workload": "write",
                        "stream_count": 100000,
                        "classification": "headline",
                        "throughput": 80.0,
                        "p50_ms": 2.0,
                        "p99_ms": 10.0,
                        "pod_memory_peak_mib": 100.0,
                    },
                    {
                        "system": "chronicle",
                        "label": "c2",
                        "workload": "write",
                        "stream_count": 100000,
                        "classification": "headline",
                        "throughput": 70.0,
                        "p50_ms": 2.0,
                        "p99_ms": 10.0,
                        "pod_memory_peak_mib": 100.0,
                    },
                ]
            }
            reference_cells = {
                "cells": [
                    {
                        "system": "rust",
                        "label": "rust-wal",
                        "workload": "write",
                        "stream_count": 100000,
                        "classification": "headline",
                        "throughput": 100.0,
                        "p50_ms": 1.0,
                        "p99_ms": 5.0,
                        "pod_memory_peak_mib": 60.0,
                    },
                    {
                        "system": "ursula",
                        "label": "ursula-disk",
                        "workload": "write",
                        "stream_count": 100000,
                        "classification": "headline",
                        "throughput": 50.0,
                        "p50_ms": 2.0,
                        "p99_ms": 8.0,
                        "pod_memory_peak_mib": 80.0,
                    },
                ]
            }
            with (
                mock.patch.object(
                    dsbench,
                    "validate_archive",
                    return_value=candidate_cells,
                ),
                mock.patch.object(
                    dsbench,
                    "combine_archive_validations",
                    return_value=reference_cells,
                ),
                mock.patch.object(
                    dsbench,
                    "_scaling_expected_cells",
                    return_value=[
                        {
                            "suite": "write",
                            "workload": "write",
                            "stream_count": 100000,
                            "level": None,
                        }
                    ],
                ),
            ):
                evaluation = dsbench.evaluate_scaling_archive(archive)
                report = dsbench.render_scaling_report(archive)
            self.assertEqual(evaluation["minimal_qualifying"], "c1")
            self.assertTrue(evaluation["candidates"][0]["qualifies"])
            self.assertFalse(evaluation["candidates"][1]["qualifies"])
            self.assertIn("Direct cell comparison", report.read_text())

    def test_campaign_cli_forwards_workload_selection(self):
        with mock.patch.object(
            dsbench,
            "execute_campaign",
            return_value={"complete": True},
        ) as execute:
            result = dsbench.main(
                [
                    "campaign",
                    "--manifest",
                    "manifest.json",
                    "--output-root",
                    "results",
                    "--workload",
                    "reads-catchup",
                ]
            )
        self.assertEqual(result, 0)
        self.assertEqual(execute.call_args.kwargs["workloads"], ["reads-catchup"])

    def test_calibration_cli_does_not_pass_campaign_workloads(self):
        with mock.patch.object(
            dsbench,
            "execute_calibration",
            return_value={"complete": True},
        ) as execute:
            result = dsbench.main(
                [
                    "calibrate",
                    "--images",
                    "images.json",
                    "--output-root",
                    "results",
                ]
            )
        self.assertEqual(result, 0)
        self.assertNotIn("workloads", execute.call_args.kwargs)

    def test_resolves_every_system_workload_pair_once(self):
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "resolved"
            first = dsbench.resolve_campaign(dsbench.parse_split("2:2"), output_dir=output)
            second = dsbench.resolve_campaign(dsbench.parse_split("2:2"), output_dir=output)
            self.assertEqual(first, second)
            index = json.loads((output / "index.json").read_text())
            self.assertEqual(index["suite_count"], 24)
            pairs = [(item["system"], item["workload"]) for item in index["suites"]]
            self.assertEqual(len(pairs), len(set(pairs)))
            self.assertEqual(
                set(system for system, _ in pairs),
                {"rust", "node", "ursula", "chronicle"},
            )
            self.assertEqual(
                set(workload for _, workload in pairs),
                {
                    "write",
                    "blog-sse",
                    "reads-sse",
                    "reads-catchup",
                    "mixed-writes",
                    "mixed-delivery",
                },
            )

            chronicle_suites = [
                output / item["path"]
                for item in index["suites"]
                if item["system"] == "chronicle"
            ]
            self.assertEqual(len(chronicle_suites), 6)
            for path in chronicle_suites:
                suite = json.loads(path.read_text())
                args = [
                    config["args"]
                    for config in suite["server_configs"]["chronicle"]
                ]
                self.assertEqual(args, ["always:2:2", "everysec:2:2"])
                self.assertEqual(
                    suite["benchmark_meta"]["resource_budget"]["cpu_millis"], 4000
                )

    def test_manifest_requires_digest_pinned_images(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            resolved = dsbench.resolve_campaign(
                dsbench.parse_split("2:2"), output_dir=root / "resolved"
            )
            images = {
                "schema": "chronicle-ds-bench-images-v1",
                "images": {
                    name: {
                        "reference": f"registry.example/{name}:tag",
                        "digest": "sha256:" + "a" * 64,
                    }
                    for name in ("chronicle", "ds-bench", "rust", "node", "ursula", "redis")
                },
            }
            images_path = root / "images.json"
            images_path.write_text(json.dumps(images))
            with self.assertRaises(dsbench.HarnessError):
                dsbench.create_campaign_manifest(
                    resolved,
                    images_path,
                    root / "missing-calibration",
                    output=root / "manifest.json",
                )

    def test_manifest_freezes_portable_cluster_targets(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            resolved = dsbench.resolve_campaign(
                dsbench.parse_split("2:2"), output_dir=root / "resolved"
            )
            digest = "sha256:" + "a" * 64
            client_source = dsbench.dsbench_build_identity(
                dsbench.prepare_checkout()
            )
            images = {
                "schema": "chronicle-ds-bench-images-v1",
                "target": "remote",
                "project": "example-project",
                "images": {
                    name: {
                        "reference": f"registry.example/{name}@{digest}",
                        "digest": digest,
                        **(
                            {
                                "source": dsbench.chronicle_build_identity(
                                    dsbench.REPO_ROOT
                                )
                            }
                            if name == "chronicle"
                            else {"source": client_source}
                            if name == "ds-bench"
                            else {}
                        ),
                    }
                    for name in ("chronicle", "ds-bench", "rust", "node", "ursula", "redis")
                },
            }
            images_path = root / "images.json"
            images_path.write_text(json.dumps(images))
            calibration = root / "calibration"
            write_calibration_results(
                calibration,
                {
                    "chronicle-cal-1-3": [80, 85, 90],
                    "chronicle-cal-2-2": [100, 105, 110],
                    "chronicle-cal-3-1": [90, 95, 100],
                },
            )
            manifest = dsbench.create_campaign_manifest(
                resolved,
                images_path,
                calibration,
                output=root / "manifest.json",
            )
            for record in manifest["campaign"]["suites"]:
                record["absolute_path"] = "/path/that/does/not/exist"
            plan = dsbench.campaign_plan(manifest)
            self.assertEqual(len(plan), 24)
            self.assertTrue(all(item["cluster"].startswith("chdb-") for item in plan))

    def test_teardown_plan_is_exact_name_scoped(self):
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "resolved"
            dsbench.resolve_campaign(dsbench.parse_split("2:2"), output_dir=output)
            index = json.loads((output / "index.json").read_text())
            manifest = {
                "schema": "chronicle-ds-bench-manifest-v1",
                "campaign": {
                    "suites": [
                        {
                            **record,
                            "absolute_path": str((output / record["path"]).resolve()),
                        }
                        for record in index["suites"]
                    ]
                },
            }
            plan = dsbench.campaign_plan(manifest)
            self.assertEqual(len(plan), 24)
            self.assertEqual(len({item["cluster"] for item in plan}), 24)
            for item in plan:
                self.assertEqual(item["teardown_filter"], f"^{item['cluster']}$")

    def test_campaign_plan_can_select_one_workload(self):
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "resolved"
            dsbench.resolve_campaign(dsbench.parse_split("2:2"), output_dir=output)
            index = json.loads((output / "index.json").read_text())
            manifest = {
                "schema": "chronicle-ds-bench-manifest-v1",
                "campaign": {
                    "suites": [
                        {
                            **record,
                            "absolute_path": str((output / record["path"]).resolve()),
                        }
                        for record in index["suites"]
                    ]
                },
            }
            plan = dsbench.select_campaign_plan(manifest, ["reads-catchup"])
            self.assertEqual(len(plan), 4)
            self.assertEqual(
                {item["system"] for item in plan},
                {"rust", "node", "ursula", "chronicle"},
            )
            self.assertEqual({item["workload"] for item in plan}, {"reads-catchup"})
            with self.assertRaises(dsbench.HarnessError):
                dsbench.select_campaign_plan(manifest, ["not-a-workload"])

    def test_calibration_uses_safe_single_suite_manifest(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            digest = "sha256:" + "a" * 64
            current_source = dsbench.chronicle_build_identity(dsbench.REPO_ROOT)
            client_source = dsbench.dsbench_build_identity(
                dsbench.prepare_checkout()
            )
            images = {
                "schema": "chronicle-ds-bench-images-v1",
                "target": "remote",
                "project": "example-project",
                "images": {
                    "chronicle": {
                        "reference": f"registry.example/chronicle@{digest}",
                        "digest": digest,
                        "source": current_source,
                    },
                    "ds-bench": {
                        "reference": f"registry.example/ds-bench@{digest}",
                        "digest": digest,
                        "source": client_source,
                    },
                    "redis": {
                        "reference": f"registry.example/redis@{digest}",
                        "digest": digest,
                    },
                },
            }
            images_path = root / "images.json"
            images_path.write_text(json.dumps(images))
            manifest = dsbench.create_calibration_manifest(images_path)
            self.assertEqual(manifest["execution_kind"], "calibration")
            plan = dsbench.campaign_plan(manifest)
            self.assertEqual(len(plan), 1)
            self.assertEqual(plan[0]["suite"], "chronicle-calibration")
            self.assertEqual(plan[0]["cluster"], "chdb-cal")
            self.assertEqual(plan[0]["teardown_filter"], "^chdb-cal$")


class TestSourcePins(unittest.TestCase):
    def test_source_commits_are_full_shas(self):
        sources = json.loads((MODULE_PATH.parent / "sources.json").read_text())
        for name in ("rust", "node"):
            commit = sources[name]["commit"]
            self.assertEqual(len(commit), 40)
            self.assertTrue(all(character in "0123456789abcdef" for character in commit))


class TestSourceIdentity(unittest.TestCase):
    def test_client_image_match_uses_exact_build_context_not_runtime_adapter(self):
        source = {
            "commit": "a" * 40,
            "adapter_sha256": "b" * 64,
            "build_context_sha256": "c" * 64,
            "build_context_file_count": 24,
        }
        runtime_only_change = {
            **source,
            "adapter_sha256": "d" * 64,
        }
        self.assertTrue(
            dsbench.dsbench_image_source_matches(source, runtime_only_change)
        )
        changed_context = {
            **runtime_only_change,
            "build_context_sha256": "e" * 64,
        }
        self.assertFalse(
            dsbench.dsbench_image_source_matches(source, changed_context)
        )

    def test_generated_campaign_results_are_not_chronicle_source(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            subprocess.run(["git", "init", "--quiet"], cwd=root, check=True)
            subprocess.run(
                ["git", "config", "user.name", "Fixture"], cwd=root, check=True
            )
            subprocess.run(
                ["git", "config", "user.email", "fixture@example.test"],
                cwd=root,
                check=True,
            )
            results = root / "docs" / "benchmarks" / "ds-bench" / "results"
            results.mkdir(parents=True)
            (root / "base.txt").write_text("base\n")
            (results / "tracked.json").write_text("{}\n")
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            subprocess.run(
                ["git", "commit", "--quiet", "-m", "fixture"], cwd=root, check=True
            )

            (results / "tracked.json").write_text('{"changed": true}\n')
            (results / "raw.json").write_text("{}\n")
            identity = dsbench.git_worktree_identity(root)
            self.assertFalse(identity["dirty"])
            self.assertEqual(identity["untracked_files"], [])

            (root / "runtime.go").write_text("package fixture\n")
            identity = dsbench.git_worktree_identity(root)
            self.assertTrue(identity["dirty"])
            self.assertEqual(identity["untracked_files"], ["runtime.go"])


class TestImageBuild(unittest.TestCase):
    def test_reference_reuse_builds_only_chronicle_and_client(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            output = root / "images.json"
            registry = "europe-west1-docker.pkg.dev/fixture-project/ds-bench"
            sealed = {}
            for index, name in enumerate(sorted(dsbench.SUT_IMAGE_NAMES), start=1):
                digest = f"sha256:{index:064x}"
                sealed[name] = {
                    "reference": f"{registry}/{name}@{digest}",
                    "digest": digest,
                    "source": {"sealed": name},
                }
            source_provenance = {
                "schema": "chronicle-ds-bench-sut-reuse-v1",
                "archive": str(root / "sealed"),
                "campaign_id": "fixture-primary",
                "evidence_tree_sha256": "a" * 64,
            }

            def resolved(tag, _project):
                name = "chronicle" if "/chronicle:" in tag else "ds-bench"
                digest = f"sha256:{90 if name == 'chronicle' else 91:064x}"
                return {
                    "reference": f"{tag.split(':', 1)[0]}@{digest}",
                    "digest": digest,
                    "tag_reference": tag,
                }

            with (
                mock.patch.object(
                    dsbench,
                    "preflight",
                    return_value={"ok": True, "required_failures": []},
                ),
                mock.patch.object(dsbench, "prepare_checkout", return_value=root),
                mock.patch.object(
                    dsbench.UpstreamPin,
                    "load",
                    return_value=dsbench.UpstreamPin("fixture", "1" * 40),
                ),
                mock.patch.object(dsbench, "adapter_digest", return_value="2" * 64),
                mock.patch.object(
                    dsbench,
                    "git_worktree_identity",
                    return_value={"commit": "3" * 40, "diff_sha256": "4" * 64},
                ),
                mock.patch.object(
                    dsbench,
                    "chronicle_build_identity",
                    return_value={"source": "chronicle"},
                ),
                mock.patch.object(
                    dsbench,
                    "dsbench_build_identity",
                    return_value={"source": "ds-bench"},
                ),
                mock.patch.object(
                    dsbench,
                    "load_reused_sut_images",
                    return_value=(sealed, source_provenance),
                ),
                mock.patch.object(
                    dsbench,
                    "_temporary_chronicle_build_context",
                    return_value=root,
                ),
                mock.patch.object(
                    dsbench,
                    "_temporary_build_context",
                    return_value=root,
                ),
                mock.patch.object(dsbench, "_run") as run,
                mock.patch.object(
                    dsbench,
                    "_resolve_artifact_registry_image",
                    side_effect=resolved,
                ),
            ):
                manifest = dsbench.build_images(
                    "remote",
                    output=output,
                    project="fixture-project",
                    reuse_reference_suts_from_archive=root / "sealed",
                )

            uploads = [
                call
                for call in run.call_args_list
                if call.args[0][:3] == ["gcloud", "builds", "submit"]
            ]
            self.assertEqual(len(uploads), 2)
            self.assertEqual(
                {call.args[0][7].rsplit("/", 1)[-1].split(":", 1)[0] for call in uploads},
                {"chronicle", "ds-bench"},
            )
            self.assertEqual(manifest["images"]["redis"], sealed["redis"])
            self.assertIsNotNone(manifest["reference_suts_reused_from"])


class TestArchiveSeal(unittest.TestCase):
    def test_seal_ignores_regenerated_reports_and_rejects_raw_changes(self):
        with tempfile.TemporaryDirectory() as raw:
            archive = Path(raw)
            evidence = archive / "runs" / "fixture" / "raw"
            evidence.mkdir(parents=True)
            source = evidence / "aggregate.json"
            source.write_text("[]\n")
            (archive / "manifest.json").write_text("{}\n")

            seal = dsbench.seal_archive(archive)
            self.assertEqual(seal["file_count"], 2)
            self.assertTrue(dsbench.verify_archive_seal(archive)["verified"])

            (archive / "report.md").write_text("# regenerated\n")
            (archive / "validation.json").write_text("{}\n")
            self.assertTrue(dsbench.verify_archive_seal(archive)["verified"])

            source.write_text("[1]\n")
            with self.assertRaises(dsbench.HarnessError):
                dsbench.verify_archive_seal(archive)
            with self.assertRaises(dsbench.HarnessError):
                dsbench.seal_archive(archive)

    def test_sut_reuse_keeps_exact_images_from_a_sealed_archive(self):
        with tempfile.TemporaryDirectory() as raw:
            archive = Path(raw)
            project = "fixture-project"
            registry = "europe-west1-docker.pkg.dev/fixture-project/ds-bench"
            entries = {}
            for index, name in enumerate(sorted(dsbench.SUT_IMAGE_NAMES), start=1):
                digest = f"sha256:{index:064x}"
                entries[name] = {
                    "reference": f"{registry}/{name}@{digest}",
                    "digest": digest,
                    "source": {"fixture": name},
                }
            images = {
                "schema": "chronicle-ds-bench-images-v1",
                "target": "remote",
                "project": project,
                "registry": registry,
                "images": entries,
            }
            chronicle_source = {
                "commit": "a" * 40,
                "diff_sha256": "b" * 64,
            }
            manifest = {
                "campaign_id": "fixture-primary",
                "chronicle_source": chronicle_source,
                "images": images,
            }
            (archive / "manifest.json").write_text(json.dumps(manifest))
            (archive / "images.json").write_text(json.dumps(images))
            dsbench.seal_archive(archive)

            selected, provenance = dsbench.load_reused_sut_images(
                archive,
                project=project,
                registry=registry,
            )
            self.assertEqual(selected, entries)
            derived = json.loads(json.dumps(images))
            derived["sut_reused_from"] = provenance
            primary = dsbench.verify_reused_sut_images(derived)
            self.assertEqual(primary["chronicle_source"], chronicle_source)

            derived["images"]["chronicle"]["digest"] = f"sha256:{99:064x}"
            with self.assertRaises(dsbench.HarnessError):
                dsbench.verify_reused_sut_images(derived)

            reference_derived = json.loads(json.dumps(images))
            chronicle_digest = f"sha256:{100:064x}"
            reference_derived["images"]["chronicle"] = {
                "reference": f"{registry}/chronicle@{chronicle_digest}",
                "digest": chronicle_digest,
                "source": {"fixture": "new-chronicle"},
            }
            reference_derived["reference_suts_reused_from"] = {
                **provenance,
                "schema": "chronicle-ds-bench-reference-sut-reuse-v1",
                "images": sorted(dsbench.SUT_IMAGE_NAMES - {"chronicle"}),
            }
            primary = dsbench.verify_reused_reference_images(reference_derived)
            self.assertEqual(primary["chronicle_source"], chronicle_source)

            reference_derived["images"]["redis"]["digest"] = f"sha256:{101:064x}"
            with self.assertRaises(dsbench.HarnessError):
                dsbench.verify_reused_reference_images(reference_derived)

    def test_full_and_reference_only_reuse_are_exclusive(self):
        with self.assertRaisesRegex(dsbench.HarnessError, "exclusive"):
            dsbench.build_images(
                "remote",
                output=Path("unused.json"),
                reuse_suts_from_archive=Path("full"),
                reuse_reference_suts_from_archive=Path("references"),
            )


class TestWatchdogJoin(unittest.TestCase):
    def test_waits_for_watchdog_to_flush_before_sealing(self):
        watchdog = mock.Mock()
        watchdog.wait.return_value = 0

        self.assertTrue(dsbench._join_completed_watchdog(watchdog))
        watchdog.wait.assert_called_once_with(timeout=dsbench.WATCHDOG_JOIN_SECONDS)

    def test_stops_watchdog_that_does_not_join(self):
        watchdog = mock.Mock()
        watchdog.wait.side_effect = [
            subprocess.TimeoutExpired("watchdog", dsbench.WATCHDOG_JOIN_SECONDS),
            0,
        ]

        self.assertFalse(dsbench._join_completed_watchdog(watchdog))
        watchdog.terminate.assert_called_once_with()
        watchdog.kill.assert_not_called()


class TestReport(unittest.TestCase):
    @staticmethod
    def write_reads_archive(root, campaign_id, bytes_per_sec, *, supplement):
        root.mkdir()
        suite = {
            "suite": "fixture-reads",
            "cluster": {
                "cluster_name": "chdb-fixture-reads",
                "zone": "europe-west4-b",
            },
            "modes": ["wal"],
            "server_configs": {
                "wal": [{"label": "rust-wal", "args": ""}]
            },
            "stream_counts": [10],
            "reads": {"mode": "catchup", "seed_bytes": 16_777_216},
            "benchmark_meta": {
                "system": "rust",
                "workload": "reads-catchup",
                "configs": [{"label": "rust-wal", "durability": "local-wal"}],
            },
        }
        suite_source = root / "source-suite.json"
        suite_source.write_text(json.dumps(suite))
        manifest = {
            "schema": "chronicle-ds-bench-manifest-v1",
            "campaign_id": campaign_id,
            "campaign": {
                "suites": [
                    {
                        "suite": suite["suite"],
                        "system": "rust",
                        "workload": "reads-catchup",
                        "absolute_path": str(suite_source),
                        "cluster": "chdb-fixture-reads",
                        "zone": "europe-west4-b",
                    }
                ]
            },
        }
        (root / "manifest.json").write_text(json.dumps(manifest))
        execution = {
            "complete": True,
            "runs": [{"suite": suite["suite"], "returncode": 0}],
        }
        if supplement:
            execution["selected_suites"] = [suite["suite"]]
        (root / "execution.json").write_text(json.dumps(execution))
        run = root / "runs" / suite["suite"]
        raw = run / "raw"
        raw.mkdir(parents=True)
        (run / "suite.json").write_text(json.dumps(suite))
        (raw / "aggregate.json").write_text(
            json.dumps(
                [
                    {
                        "mode": "rust-wal",
                        "stream_count": 10,
                        "connections": 8,
                        "ops_per_sec": 10.0,
                        "bytes_per_sec": bytes_per_sec,
                        "status": "ok",
                    }
                ]
            )
        )
        verified_bytes = 16_777_216 if supplement else 16_773_120
        fleet = (
            raw
            / "rust-wal"
            / "cells"
            / "wal"
            / "n10-c8"
            / "p1-r1"
            / "fleet"
        )
        fleet.mkdir(parents=True)
        (fleet / "reads-0.json").write_text(
            json.dumps(
                {
                    "scenario": "reads",
                    "seed_bytes": 16_777_216,
                    "seed_verified_streams": 10,
                    "seed_verified_min_bytes": verified_bytes,
                    "seed_verified_max_bytes": verified_bytes,
                }
            )
        )

    def test_supplement_replaces_a_complete_system_workload_slice(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            primary = root / "primary"
            supplement = root / "supplement"
            self.write_reads_archive(
                primary,
                "primary",
                (16_777_216 - 4_096) * 10.0,
                supplement=False,
            )
            self.write_reads_archive(
                supplement,
                "supplement",
                16_777_216 * 10.0,
                supplement=True,
            )
            combined = dsbench.combine_archive_validations(
                primary, [supplement]
            )
            self.assertTrue(combined["complete"])
            self.assertEqual(combined["invalid_or_gap_count"], 0)
            self.assertEqual(len(combined["cells"]), 1)
            self.assertEqual(
                combined["cells"][0]["source_campaign_id"], "supplement"
            )

    def test_validates_raw_fixture_and_renders_headline(self):
        with tempfile.TemporaryDirectory() as raw:
            archive = Path(raw)
            suite = {
                "suite": "fixture-write",
                "cluster": {
                    "cluster_name": "chdb-fixture-rust",
                    "zone": "europe-west4-b",
                },
                "modes": ["wal"],
                "server_configs": {
                    "wal": [{"label": "rust-wal", "args": ""}]
                },
                "stream_counts": [10000],
                "benchmark_meta": {
                    "system": "rust",
                    "workload": "write",
                    "configs": [
                        {"label": "rust-wal", "durability": "local-wal"}
                    ],
                },
            }
            suite_source = archive / "source-suite.json"
            suite_source.write_text(json.dumps(suite))
            manifest = {
                "schema": "chronicle-ds-bench-manifest-v1",
                "campaign_id": "fixture",
                "chronicle_source": {
                    "commit": "a" * 40,
                    "diff_sha256": "b" * 64,
                },
                "ds_bench": {
                    "commit": "c" * 40,
                    "adapter_sha256": "d" * 64,
                },
                "campaign": {
                    "chronicle_split": "2:2",
                    "suites": [
                        {
                            "suite": "fixture-write",
                            "absolute_path": str(suite_source),
                            "cluster": "chdb-fixture-rust",
                            "zone": "europe-west4-b",
                        }
                    ],
                },
            }
            (archive / "manifest.json").write_text(json.dumps(manifest))
            (archive / "execution.json").write_text(
                json.dumps(
                    {
                        "complete": True,
                        "runs": [{"suite": "fixture-write", "returncode": 0}],
                    }
                )
            )
            run = archive / "runs" / "fixture-write"
            run.mkdir(parents=True)
            (run / "suite.json").write_text(json.dumps(suite))
            raw_root = run / "raw"
            label = raw_root / "rust-wal"
            label.mkdir(parents=True)
            (label / "cells.json").write_text(
                json.dumps(
                    {
                        "cells": {
                            "10000": {
                                "status": "ok",
                                "saturated": True,
                                "reason": "plateau",
                                "pinned_pods": 1,
                                "throughput": 123456,
                                "p50": 1.2,
                                "p99": 4.5,
                                "pod_mem_mb": 200,
                            }
                        }
                    }
                )
            )
            for repeat in range(1, 4):
                rep = label / "cells" / "wal" / "n10000" / f"p1-r{repeat}"
                fleet = rep / "fleet"
                fleet.mkdir(parents=True)
                (rep / "merged.json").write_text(
                    json.dumps({"windows_aligned": True, "pods_reported": 1})
                )
                (fleet / "write-0.json").write_text(
                    json.dumps(
                        {
                            "counts": {"other_err": 0, "backpressure": 0},
                            "lazy_creates": 0,
                        }
                    )
                )

            validation = dsbench.validate_archive(archive)
            self.assertTrue(validation["complete"])
            self.assertEqual(validation["cells"][0]["classification"], "headline")
            report = dsbench.render_report(archive)
            self.assertIn("123.5k", report.read_text())

            stored = json.loads((label / "cells.json").read_text())
            stored["cells"]["10000"]["saturated"] = False
            stored["cells"]["10000"]["reason"] = "ladder_exhausted"
            (label / "cells.json").write_text(json.dumps(stored))
            first_merged = (
                label
                / "cells"
                / "wal"
                / "n10000"
                / "p1-r1"
                / "merged.json"
            )
            first_merged.write_text(
                json.dumps({"windows_aligned": True, "pods_reported": 0})
            )
            invalid_lower_bound = dsbench.validate_archive(archive)
            self.assertEqual(
                invalid_lower_bound["cells"][0]["classification"], "invalid"
            )
            self.assertIn(
                "p1-r1:partial_fleet",
                invalid_lower_bound["cells"][0]["reasons"],
            )


class TestPrepare(unittest.TestCase):
    def test_prepare_is_pinned_and_idempotent(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            origin = root / "origin"
            origin.mkdir()
            subprocess.run(["git", "init", "--quiet"], cwd=origin, check=True)
            subprocess.run(
                ["git", "config", "user.name", "Fixture"], cwd=origin, check=True
            )
            subprocess.run(
                ["git", "config", "user.email", "fixture@example.test"],
                cwd=origin,
                check=True,
            )
            (origin / "base.txt").write_text("base\n")
            subprocess.run(["git", "add", "base.txt"], cwd=origin, check=True)
            subprocess.run(["git", "commit", "--quiet", "-m", "fixture"], cwd=origin, check=True)
            commit = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=origin,
                check=True,
                text=True,
                stdout=subprocess.PIPE,
            ).stdout.strip()
            overlay = root / "overlay"
            overlay.mkdir()
            (overlay / "added.txt").write_text("overlay\n")
            pin = dsbench.UpstreamPin(str(origin), commit)
            destination = root / "prepared"

            first = dsbench.prepare_checkout(
                pin=pin,
                destination_root=destination,
                patch_file=None,
                overlay_dir=overlay,
            )
            second = dsbench.prepare_checkout(
                pin=pin,
                destination_root=destination,
                patch_file=None,
                overlay_dir=overlay,
            )

            self.assertEqual(first, second)
            self.assertEqual((first / "added.txt").read_text(), "overlay\n")
            head = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=first,
                check=True,
                text=True,
                stdout=subprocess.PIPE,
            ).stdout.strip()
            self.assertEqual(head, commit)


if __name__ == "__main__":
    unittest.main()
