#!/usr/bin/env python3
"""Revision-pinned Chronicle adapter for Electric's ds-bench harness."""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from typing import Any, Iterable, Mapping, Sequence
import urllib.error
import urllib.parse
import urllib.request


ADAPTER_DIR = Path(__file__).resolve().parent
REPO_ROOT = ADAPTER_DIR.parents[1]
PIN_FILE = ADAPTER_DIR / "upstream.json"
CAMPAIGN_FILE = ADAPTER_DIR / "campaign.json"
SCALING_FILE = ADAPTER_DIR / "scaling.json"
SSE_DIAGNOSTIC_FILE = ADAPTER_DIR / "sse-diagnostic.json"
SOURCES_FILE = ADAPTER_DIR / "sources.json"
PATCH_FILE = ADAPTER_DIR / "patches" / "chronicle.patch"
OVERLAY_DIR = ADAPTER_DIR / "overlay"
PREPARED_ROOT = REPO_ROOT / ".tmp" / "ds-bench"
MARKER_FILE = ".chronicle-adapter.json"
SOURCE_IDENTITY_EXCLUDES = ("docs/benchmarks/ds-bench/results/",)
ARCHIVE_GENERATED_FILES = {
    "combined-validation.json",
    "evidence-checksums.json",
    "report.md",
    "scaling-evaluation.json",
    "scaling-report.md",
    "validation.json",
}
SUT_IMAGE_NAMES = frozenset({"chronicle", "rust", "node", "ursula", "redis"})
WATCHDOG_JOIN_SECONDS = 65


class HarnessError(RuntimeError):
    """A user-actionable harness failure."""


@dataclasses.dataclass(frozen=True)
class UpstreamPin:
    url: str
    commit: str

    @classmethod
    def load(cls, path: Path = PIN_FILE) -> "UpstreamPin":
        data = json.loads(path.read_text())
        commit = str(data["commit"])
        if len(commit) != 40 or any(c not in "0123456789abcdef" for c in commit):
            raise HarnessError(f"{path}: commit must be a full lowercase Git SHA")
        return cls(url=str(data["url"]), commit=commit)


@dataclasses.dataclass(frozen=True)
class ResourceBudget:
    cpu_millis: int = 4_000
    memory_mib: int = 16 * 1024
    local_ssds: int = 1


@dataclasses.dataclass(frozen=True)
class ChronicleSplit:
    chronicle_cpu_millis: int
    redis_cpu_millis: int
    chronicle_memory_mib: int = 4 * 1024
    redis_memory_mib: int = 12 * 1024

    @property
    def label(self) -> str:
        return (
            f"{self.chronicle_cpu_millis // 1000}:"
            f"{self.redis_cpu_millis // 1000}"
        )

    def validate(self, budget: ResourceBudget = ResourceBudget()) -> None:
        values = dataclasses.astuple(self)
        if any(value <= 0 for value in values):
            raise HarnessError("Chronicle and Redis resource allocations must be positive")
        if self.chronicle_cpu_millis + self.redis_cpu_millis != budget.cpu_millis:
            raise HarnessError("Chronicle and Redis CPU allocations must equal 4 vCPUs")
        if self.chronicle_memory_mib + self.redis_memory_mib != budget.memory_mib:
            raise HarnessError("Chronicle and Redis memory allocations must equal 16 GiB")


@dataclasses.dataclass(frozen=True)
class PerPodResources:
    cpu_millis: int
    memory_mib: int


@dataclasses.dataclass(frozen=True)
class ChronicleTopology:
    chronicle_replicas: int
    redis_masters: int
    chronicle_cpu_millis: int
    redis_cpu_millis: int
    chronicle_memory_mib: int
    redis_memory_mib: int
    sse_wait_mode: str = "legacy"
    appendfsync: str = "always"

    @property
    def kind(self) -> str:
        return "shared" if self.redis_masters == 1 else "cluster"

    @property
    def label(self) -> str:
        suffix = "" if self.sse_wait_mode == "legacy" else "-sse-persistent"
        return (
            f"c{self.chronicle_replicas}-r{self.redis_masters}"
            f"-cpu{self.chronicle_cpu_millis}-{self.redis_cpu_millis}"
            f"-mem{self.chronicle_memory_mib}-{self.redis_memory_mib}{suffix}"
        )

    @property
    def chronicle_per_pod(self) -> PerPodResources:
        return PerPodResources(
            self.chronicle_cpu_millis // self.chronicle_replicas,
            self.chronicle_memory_mib // self.chronicle_replicas,
        )

    @property
    def redis_per_pod(self) -> PerPodResources:
        return PerPodResources(
            self.redis_cpu_millis // self.redis_masters,
            self.redis_memory_mib // self.redis_masters,
        )

    @property
    def effective_cpu_millis(self) -> int:
        return (
            self.chronicle_per_pod.cpu_millis * self.chronicle_replicas
            + self.redis_per_pod.cpu_millis * self.redis_masters
        )

    @property
    def effective_memory_mib(self) -> int:
        return (
            self.chronicle_per_pod.memory_mib * self.chronicle_replicas
            + self.redis_per_pod.memory_mib * self.redis_masters
        )

    def validate(self, budget: ResourceBudget = ResourceBudget()) -> None:
        if self.appendfsync not in {"always", "everysec", "no"}:
            raise HarnessError(f"unsupported Redis appendfsync: {self.appendfsync}")
        if self.chronicle_replicas not in {1, 2, 4}:
            raise HarnessError("Chronicle replicas must be one, two, or four")
        if self.redis_masters not in {1, 3}:
            raise HarnessError("Redis masters must be one or three")
        if self.sse_wait_mode not in {"legacy", "persistent"}:
            raise HarnessError("SSE wait mode must be legacy or persistent")
        requested = (
            self.chronicle_cpu_millis,
            self.redis_cpu_millis,
            self.chronicle_memory_mib,
            self.redis_memory_mib,
        )
        if any(value <= 0 for value in requested):
            raise HarnessError("Chronicle topology resource allocations must be positive")
        if self.chronicle_cpu_millis + self.redis_cpu_millis != budget.cpu_millis:
            raise HarnessError("Chronicle topology CPU must equal the SUT budget")
        if self.chronicle_memory_mib + self.redis_memory_mib != budget.memory_mib:
            raise HarnessError("Chronicle topology memory must equal the SUT budget")
        per_pod = (self.chronicle_per_pod, self.redis_per_pod)
        if any(value.cpu_millis <= 0 or value.memory_mib <= 0 for value in per_pod):
            raise HarnessError("Chronicle topology gives a pod no resources")
        if budget.cpu_millis - self.effective_cpu_millis >= 3:
            raise HarnessError("Chronicle topology leaves three or more millicores unused")
        if budget.memory_mib - self.effective_memory_mib >= 3:
            raise HarnessError("Chronicle topology leaves three or more MiB unused")

    def to_args(self) -> str:
        return ":".join(
            (
                self.appendfsync,
                str(self.chronicle_replicas),
                str(self.redis_masters),
                str(self.chronicle_cpu_millis),
                str(self.redis_cpu_millis),
                str(self.chronicle_memory_mib),
                str(self.redis_memory_mib),
                self.sse_wait_mode,
            )
        )

    def to_metadata(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "chronicle_replicas": self.chronicle_replicas,
            "redis_masters": self.redis_masters,
            "appendfsync": self.appendfsync,
            "sse_wait_mode": self.sse_wait_mode,
            "requested": {
                "chronicle_cpu_millis": self.chronicle_cpu_millis,
                "redis_cpu_millis": self.redis_cpu_millis,
                "chronicle_memory_mib": self.chronicle_memory_mib,
                "redis_memory_mib": self.redis_memory_mib,
            },
            "per_pod": {
                "chronicle": dataclasses.asdict(self.chronicle_per_pod),
                "redis": dataclasses.asdict(self.redis_per_pod),
            },
            "effective": {
                "cpu_millis": self.effective_cpu_millis,
                "memory_mib": self.effective_memory_mib,
            },
        }


@dataclasses.dataclass(frozen=True)
class CellVerdict:
    valid: bool
    status: str
    reasons: tuple[str, ...]
    errors: int
    lazy_creates: int
    windows_aligned: bool
    client_bound: bool
    plateau: bool


CALIBRATION_SPLITS: Mapping[str, ChronicleSplit] = {
    "chronicle-cal-1-3": ChronicleSplit(1_000, 3_000),
    "chronicle-cal-2-2": ChronicleSplit(2_000, 2_000),
    "chronicle-cal-3-1": ChronicleSplit(3_000, 1_000),
}

SERVER_APP_BY_MODE: Mapping[str, str] = {
    "wal": "durable-streams",
    "node": "durable-node",
    "ursula": "ursula",
    "chronicle": "chronicle",
}


def _run(
    args: Sequence[str | os.PathLike[str]],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    command = [os.fspath(arg) for arg in args]
    try:
        return subprocess.run(
            command,
            cwd=cwd,
            env=dict(env) if env is not None else None,
            check=True,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
        )
    except FileNotFoundError as exc:
        raise HarnessError(f"required command not found: {command[0]}") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()
        suffix = f": {detail}" if detail else ""
        raise HarnessError(f"command failed ({' '.join(command)}){suffix}") from exc


def _probe_command(
    name: str,
    args: Sequence[str],
    *,
    required: bool = True,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    executable = shutil.which(args[0])
    if executable is None:
        return {
            "name": name,
            "ok": False,
            "required": required,
            "command": list(args),
            "detail": f"{args[0]} is not installed",
        }
    completed = subprocess.run(
        list(args),
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=dict(env) if env is not None else None,
    )
    output = (completed.stdout or completed.stderr).strip()
    return {
        "name": name,
        "ok": completed.returncode == 0,
        "required": required,
        "command": list(args),
        "detail": output[-4_000:],
    }


def _probe_iam_permissions(project: str) -> dict[str, Any]:
    permissions = [
        "artifactregistry.repositories.downloadArtifacts",
        "artifactregistry.repositories.uploadArtifacts",
        "cloudbuild.builds.create",
        "cloudbuild.builds.get",
        "compute.instances.create",
        "compute.instances.delete",
        "container.clusters.create",
        "container.clusters.delete",
        "container.clusters.get",
        "container.clusters.list",
        "serviceusage.services.use",
    ]
    token_result = subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if token_result.returncode != 0:
        return {
            "name": "project IAM permissions",
            "ok": False,
            "required": True,
            "command": ["gcloud", "auth", "print-access-token"],
            "detail": token_result.stderr.strip()[-4_000:],
        }
    request = urllib.request.Request(
        f"https://cloudresourcemanager.googleapis.com/v1/projects/{project}:testIamPermissions",
        data=json.dumps({"permissions": permissions}).encode(),
        headers={
            "Authorization": f"Bearer {token_result.stdout.strip()}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            data = json.load(response)
    except (OSError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
        return {
            "name": "project IAM permissions",
            "ok": False,
            "required": True,
            "command": ["Cloud Resource Manager", "projects.testIamPermissions"],
            "detail": str(exc),
        }
    granted = set(data.get("permissions", []))
    missing = sorted(set(permissions) - granted)
    return {
        "name": "project IAM permissions",
        "ok": not missing,
        "required": True,
        "command": ["Cloud Resource Manager", "projects.testIamPermissions"],
        "detail": {"granted": sorted(granted), "missing": missing},
    }


def _gcloud_json(
    args: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
) -> tuple[Any | None, str | None]:
    completed = subprocess.run(
        ["gcloud", *args],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=dict(env) if env is not None else None,
    )
    if completed.returncode != 0:
        return None, (completed.stderr or completed.stdout).strip()[-4_000:]
    try:
        return json.loads(completed.stdout), None
    except json.JSONDecodeError:
        return None, f"gcloud returned invalid JSON: {completed.stdout[-1_000:]}"


def _quota_row(rows: Sequence[Mapping[str, Any]], metric: str) -> Mapping[str, Any] | None:
    return next((row for row in rows if row.get("metric") == metric), None)


def _quota_requirement(
    quota: str,
    required: int,
    *,
    limit: float | int | None,
    usage: float | int | None = 0,
    dimensions: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    numeric_limit = float(limit) if limit is not None else None
    numeric_usage = float(usage) if usage is not None else None
    unlimited = numeric_limit == -1
    available = (
        None
        if numeric_limit is None or numeric_usage is None
        else numeric_limit - numeric_usage
    )
    ok = unlimited or (available is not None and available >= required)
    return {
        "quota": quota,
        "dimensions": dict(dimensions or {}),
        "required": required,
        "limit": limit,
        "usage": usage,
        "available": "unlimited" if unlimited else available,
        "ok": ok,
    }


def _dimension_limit(
    quota_info: Mapping[str, Any],
    dimensions: Mapping[str, str],
) -> int | None:
    for item in quota_info.get("dimensionsInfos", []):
        if all(item.get("dimensions", {}).get(key) == value for key, value in dimensions.items()):
            raw = item.get("details", {}).get("value")
            return int(raw) if raw is not None else None
    return None


def _probe_campaign_compute_capacity(
    project: str,
    region: str,
    zone: str,
    *,
    env: Mapping[str, str],
    campaign_file: Path = CAMPAIGN_FILE,
) -> dict[str, Any]:
    """Check the exact VM topology, including modern VM-family quotas."""
    campaign = json.loads(campaign_file.read_text())
    cluster = campaign["cluster"]
    if region != cluster["region"] or zone != cluster["zone"]:
        return {
            "name": "campaign compute capacity",
            "ok": False,
            "required": True,
            "command": ["gcloud", "compute", "quotas"],
            "detail": {
                "error": "preflight location differs from the frozen campaign",
                "requested": {"region": region, "zone": zone},
                "campaign": {
                    "region": cluster["region"],
                    "zone": cluster["zone"],
                },
            },
        }

    commands = {
        "server_machine": [
            "compute",
            "machine-types",
            "describe",
            cluster["server_machine"],
            "--zone",
            zone,
            "--project",
            project,
            "--format=json",
        ],
        "client_machine": [
            "compute",
            "machine-types",
            "describe",
            cluster["client_machine"],
            "--zone",
            zone,
            "--project",
            project,
            "--format=json",
        ],
        "project_quotas": [
            "compute",
            "project-info",
            "describe",
            "--project",
            project,
            "--format=json",
        ],
        "regional_quotas": [
            "compute",
            "regions",
            "describe",
            region,
            "--project",
            project,
            "--format=json",
        ],
        "server_family_quota": [
            "beta",
            "quotas",
            "info",
            "describe",
            "CPUS-PER-VM-FAMILY-per-project-region",
            "--service=compute.googleapis.com",
            "--project",
            project,
            "--format=json",
        ],
        "preemptible_quota": [
            "beta",
            "quotas",
            "info",
            "describe",
            "PREEMPTIBLE-CPUS-per-project-region",
            "--service=compute.googleapis.com",
            "--project",
            project,
            "--format=json",
        ],
    }
    results: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for name, command in commands.items():
        value, error = _gcloud_json(command, env=env)
        if error is not None:
            errors[name] = error
        else:
            results[name] = value
    if errors:
        return {
            "name": "campaign compute capacity",
            "ok": False,
            "required": True,
            "command": ["gcloud", "compute", "quotas"],
            "detail": {"errors": errors},
        }

    server_cpus = int(results["server_machine"]["guestCpus"])
    client_cpus = int(results["client_machine"]["guestCpus"])
    client_nodes = int(cluster["client_nodes"])
    total_nodes = 1 + client_nodes
    total_cpus = server_cpus + client_cpus * client_nodes
    project_quotas = results["project_quotas"].get("quotas", [])
    regional_quotas = results["regional_quotas"].get("quotas", [])
    server_family = str(cluster["server_machine"]).split("-", 1)[0].upper()
    client_family = str(cluster["client_machine"]).split("-", 1)[0].upper()

    global_cpus = _quota_row(project_quotas, "CPUS_ALL_REGIONS")
    server_family_limit = _dimension_limit(
        results["server_family_quota"],
        {"region": region, "vm_family": server_family},
    )
    preemptible_limit = _dimension_limit(
        results["preemptible_quota"],
        {"region": region},
    )
    client_metric = f"{client_family}_CPUS"
    client_quota = _quota_row(regional_quotas, client_metric)
    # If a project has never received separate preemptible CPU quota, Google
    # charges Spot VMs to their standard family quota. A numeric preemptible
    # limit means the separate pool is active and must be checked instead.
    if preemptible_limit is not None:
        client_requirement = _quota_requirement(
            "PREEMPTIBLE-CPUS-per-project-region",
            client_cpus * client_nodes,
            limit=preemptible_limit,
            usage=0,
            dimensions={"region": region},
        )
    else:
        client_requirement = _quota_requirement(
            client_metric,
            client_cpus * client_nodes,
            limit=client_quota.get("limit") if client_quota else None,
            usage=client_quota.get("usage") if client_quota else None,
            dimensions={"region": region},
        )

    instances = _quota_row(regional_quotas, "INSTANCES")
    addresses = _quota_row(regional_quotas, "IN_USE_ADDRESSES")
    disks = _quota_row(regional_quotas, "DISKS_TOTAL_GB")
    local_ssd = _quota_row(regional_quotas, "LOCAL_SSD_TOTAL_GB")
    requirements = [
        _quota_requirement(
            "CPUS-ALL-REGIONS-per-project",
            total_cpus,
            limit=global_cpus.get("limit") if global_cpus else None,
            usage=global_cpus.get("usage") if global_cpus else None,
        ),
        _quota_requirement(
            "CPUS-PER-VM-FAMILY-per-project-region",
            server_cpus,
            limit=server_family_limit,
            # Cloud Quotas exposes this allocation limit but not live usage.
            # The preflight separately proves no owned GKE cluster exists.
            usage=0,
            dimensions={"region": region, "vm_family": server_family},
        ),
        client_requirement,
        _quota_requirement(
            "INSTANCES",
            total_nodes,
            limit=instances.get("limit") if instances else None,
            usage=instances.get("usage") if instances else None,
            dimensions={"region": region},
        ),
        _quota_requirement(
            "IN_USE_ADDRESSES",
            total_nodes,
            limit=addresses.get("limit") if addresses else None,
            usage=addresses.get("usage") if addresses else None,
            dimensions={"region": region},
        ),
        _quota_requirement(
            "DISKS_TOTAL_GB",
            total_nodes * 100,
            limit=disks.get("limit") if disks else None,
            usage=disks.get("usage") if disks else None,
            dimensions={"region": region},
        ),
        _quota_requirement(
            "LOCAL_SSD_TOTAL_GB",
            375,
            limit=local_ssd.get("limit") if local_ssd else None,
            usage=local_ssd.get("usage") if local_ssd else None,
            dimensions={"region": region},
        ),
    ]
    failures = [item for item in requirements if not item["ok"]]
    return {
        "name": "campaign compute capacity",
        "ok": not failures,
        "required": True,
        "command": ["gcloud", "compute", "quotas"],
        "detail": {
            "topology": {
                "server": {
                    "machine": cluster["server_machine"],
                    "nodes": 1,
                    "vcpus_per_node": server_cpus,
                },
                "clients": {
                    "machine": cluster["client_machine"],
                    "nodes": client_nodes,
                    "vcpus_per_node": client_cpus,
                    "purchasing_model": "spot",
                },
                "total_nodes": total_nodes,
                "total_vcpus": total_cpus,
            },
            "requirements": requirements,
            "failures": failures,
        },
    }


def preflight(
    target: str,
    *,
    project: str = "adityavkk-prototyping",
    region: str = "europe-west4",
    zone: str = "europe-west4-b",
    ar_location: str = "europe-west1",
    ar_repo: str = "ds-bench",
    phase: str = "campaign",
    campaign_file: Path = CAMPAIGN_FILE,
) -> dict[str, Any]:
    if phase not in {"build", "campaign"}:
        raise HarnessError(f"unknown preflight phase: {phase}")
    checks: list[dict[str, Any]] = []
    for executable in ("git", "python3", "kubectl"):
        checks.append(
            {
                "name": f"{executable} installed",
                "ok": shutil.which(executable) is not None,
                "required": True,
                "command": ["which", executable],
                "detail": shutil.which(executable) or "not found",
            }
        )
    if target == "local":
        checks.extend(
            [
                _probe_command("Docker daemon", ["docker", "info", "--format", "{{.ServerVersion}}"]),
                _probe_command("kind", ["kind", "version"]),
                _probe_command("kind cluster list", ["kind", "get", "clusters"], required=False),
            ]
        )
    elif target == "remote":
        env = os.environ.copy()
        env["CLOUDSDK_CORE_PROJECT"] = project
        checks.extend(
            [
                _probe_command(
                    "active gcloud account",
                    ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"],
                    env=env,
                ),
                _probe_command(
                    "billing enabled",
                    ["gcloud", "billing", "projects", "describe", project, "--format=json"],
                    env=env,
                ),
                _probe_command(
                    "required APIs enabled",
                    [
                        "gcloud",
                        "services",
                        "list",
                        "--enabled",
                        "--project",
                        project,
                        "--filter=name:(artifactregistry.googleapis.com cloudbuild.googleapis.com compute.googleapis.com container.googleapis.com)",
                        "--format=value(name)",
                    ],
                    env=env,
                ),
                _probe_command(
                    "benchmarking VPC",
                    [
                        "gcloud",
                        "compute",
                        "networks",
                        "describe",
                        "benchmarking",
                        "--project",
                        project,
                        "--format=json",
                    ],
                    env=env,
                ),
                _probe_command(
                    "benchmarking subnet",
                    [
                        "gcloud",
                        "compute",
                        "networks",
                        "subnets",
                        "describe",
                        "benchmarking",
                        "--region",
                        region,
                        "--project",
                        project,
                        "--format=json",
                    ],
                    env=env,
                ),
                _probe_command(
                    "regional quota access",
                    [
                        "gcloud",
                        "compute",
                        "regions",
                        "describe",
                        region,
                        "--project",
                        project,
                        "--format=json",
                    ],
                    env=env,
                ),
                _probe_command(
                    "GKE list access",
                    [
                        "gcloud",
                        "container",
                        "clusters",
                        "list",
                        "--project",
                        project,
                        "--zone",
                        zone,
                        "--format=json",
                    ],
                    env=env,
                ),
                _probe_command(
                    "Artifact Registry repository",
                    [
                        "gcloud",
                        "artifacts",
                        "repositories",
                        "describe",
                        ar_repo,
                        "--location",
                        ar_location,
                        "--project",
                        project,
                        "--format=json",
                    ],
                    env=env,
                ),
                _probe_command(
                    "Artifact Registry image list",
                    [
                        "gcloud",
                        "artifacts",
                        "docker",
                        "images",
                        "list",
                        f"{ar_location}-docker.pkg.dev/{project}/{ar_repo}",
                        "--project",
                        project,
                        "--limit=1",
                        "--format=json",
                    ],
                    env=env,
                ),
            ]
        )
        if shutil.which("gcloud"):
            checks.append(_probe_iam_permissions(project))
            if phase == "campaign":
                checks.append(
                    _probe_campaign_compute_capacity(
                        project,
                        region,
                        zone,
                        env=env,
                        campaign_file=campaign_file,
                    )
                )
    else:
        raise HarnessError(f"unknown preflight target: {target}")

    for check in checks:
        if check["name"] == "active gcloud account" and check["ok"]:
            check["ok"] = bool(str(check["detail"]).strip())
        if check["name"] == "billing enabled" and check["ok"]:
            try:
                billing = json.loads(check["detail"])
                check["ok"] = billing.get("billingEnabled") is True
            except json.JSONDecodeError:
                check["ok"] = False
        if check["name"] == "required APIs enabled" and check["ok"]:
            required_apis = {
                "artifactregistry.googleapis.com",
                "cloudbuild.googleapis.com",
                "compute.googleapis.com",
                "container.googleapis.com",
            }
            enabled = {
                line.rsplit("/", 1)[-1]
                for line in str(check["detail"]).splitlines()
                if line.strip()
            }
            missing = sorted(required_apis - enabled)
            check["ok"] = not missing
            check["detail"] = {"enabled": sorted(enabled), "missing": missing}

    required_failures = [
        check["name"] for check in checks if check["required"] and not check["ok"]
    ]
    return {
        "schema": "chronicle-ds-bench-preflight-v1",
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "target": target,
        "phase": phase,
        "project": project if target == "remote" else None,
        "region": region if target == "remote" else None,
        "zone": zone if target == "remote" else None,
        "artifact_registry_location": ar_location if target == "remote" else None,
        "artifact_registry_repository": ar_repo if target == "remote" else None,
        "ok": not required_failures,
        "required_failures": required_failures,
        "checks": checks,
    }


def _sha256_parts(parts: Iterable[tuple[str, bytes]]) -> str:
    digest = hashlib.sha256()
    for name, content in parts:
        encoded = name.encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha256_tree(root: Path) -> str:
    if not root.is_dir():
        raise HarnessError(f"directory not found: {root}")
    return _sha256_parts(
        (
            path.relative_to(root).as_posix(),
            path.read_bytes(),
        )
        for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file())
    )


def _archive_evidence_records(archive: Path) -> list[dict[str, Any]]:
    records = []
    for path in sorted(
        candidate
        for candidate in archive.rglob("*")
        if candidate.is_file()
        and candidate.relative_to(archive).as_posix()
        not in ARCHIVE_GENERATED_FILES
    ):
        records.append(
            {
                "path": path.relative_to(archive).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return records


def seal_archive(archive: Path) -> dict[str, Any]:
    records = _archive_evidence_records(archive)
    if not records:
        raise HarnessError(f"{archive}: no campaign evidence to seal")
    payload = {
        "schema": "chronicle-ds-bench-evidence-v1",
        "file_count": len(records),
        "tree_sha256": _sha256_parts(
            (record["path"], bytes.fromhex(record["sha256"]))
            for record in records
        ),
        "files": records,
    }
    path = archive / "evidence-checksums.json"
    if path.is_file():
        try:
            existing = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise HarnessError(f"{path}: invalid existing evidence seal") from exc
        if existing != payload:
            raise HarnessError(
                f"{archive}: raw campaign evidence changed after it was sealed"
            )
        return existing
    path.write_text(json.dumps(payload, indent=2) + "\n")
    return payload


def verify_archive_seal(archive: Path) -> dict[str, Any]:
    path = archive / "evidence-checksums.json"
    try:
        expected = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessError(f"{archive}: missing or invalid evidence seal") from exc
    if expected.get("schema") != "chronicle-ds-bench-evidence-v1":
        raise HarnessError(f"{archive}: unsupported evidence seal schema")
    actual_records = _archive_evidence_records(archive)
    actual = {
        "schema": "chronicle-ds-bench-evidence-v1",
        "file_count": len(actual_records),
        "tree_sha256": _sha256_parts(
            (record["path"], bytes.fromhex(record["sha256"]))
            for record in actual_records
        ),
        "files": actual_records,
    }
    if actual != expected:
        raise HarnessError(f"{archive}: evidence seal verification failed")
    return {
        "schema": expected["schema"],
        "file_count": expected["file_count"],
        "tree_sha256": expected["tree_sha256"],
        "verified": True,
    }


def load_reused_sut_images(
    archive: Path,
    *,
    project: str,
    registry: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Load exact server images from a sealed campaign archive."""
    seal = verify_archive_seal(archive)
    try:
        manifest = json.loads((archive / "manifest.json").read_text())
        images = json.loads((archive / "images.json").read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessError(f"{archive}: invalid SUT reuse archive") from exc
    if manifest.get("images") != images:
        raise HarnessError(f"{archive}: manifest and archived images do not match")
    if (
        images.get("schema") != "chronicle-ds-bench-images-v1"
        or images.get("target") != "remote"
        or images.get("project") != project
        or images.get("registry") != registry
    ):
        raise HarnessError(f"{archive}: SUT images do not match the remote build target")
    entries = images.get("images", {})
    missing = sorted(SUT_IMAGE_NAMES - set(entries))
    if missing:
        raise HarnessError(
            f"{archive}: SUT image archive is missing: {', '.join(missing)}"
        )
    selected: dict[str, dict[str, Any]] = {}
    for name in SUT_IMAGE_NAMES:
        entry = entries[name]
        digest = str(entry.get("digest", ""))
        reference = str(entry.get("reference", ""))
        if (
            re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None
            or f"@{digest}" not in reference
        ):
            raise HarnessError(f"{archive}: {name} image is not digest pinned")
        selected[name] = dict(entry)
    return selected, {
        "schema": "chronicle-ds-bench-sut-reuse-v1",
        "archive": str(archive.resolve()),
        "campaign_id": manifest["campaign_id"],
        "evidence_tree_sha256": seal["tree_sha256"],
    }


def verify_reused_sut_images(images: Mapping[str, Any]) -> dict[str, Any] | None:
    """Verify a derived image manifest against its sealed source campaign."""
    provenance = images.get("sut_reused_from")
    if provenance is None:
        return None
    if not isinstance(provenance, Mapping):
        raise HarnessError("SUT reuse provenance must be an object")
    try:
        archive = Path(str(provenance["archive"]))
        project = str(images["project"])
        registry = str(images["registry"])
    except KeyError as exc:
        raise HarnessError("SUT reuse provenance is incomplete") from exc
    expected_entries, expected_provenance = load_reused_sut_images(
        archive,
        project=project,
        registry=registry,
    )
    if dict(provenance) != expected_provenance:
        raise HarnessError("SUT reuse provenance does not match its sealed archive")
    current_entries = images.get("images", {})
    for name, expected in expected_entries.items():
        if current_entries.get(name) != expected:
            raise HarnessError(f"reused {name} image differs from its sealed archive")
    return json.loads((archive / "manifest.json").read_text())


def verify_reused_reference_images(
    images: Mapping[str, Any],
) -> dict[str, Any] | None:
    provenance = images.get("reference_suts_reused_from")
    if provenance is None:
        return None
    if not isinstance(provenance, Mapping):
        raise HarnessError("reference SUT reuse provenance must be an object")
    try:
        archive = Path(str(provenance["archive"]))
        project = str(images["project"])
        registry = str(images["registry"])
    except KeyError as exc:
        raise HarnessError("reference SUT reuse provenance is incomplete") from exc
    expected, source_provenance = load_reused_sut_images(
        archive,
        project=project,
        registry=registry,
    )
    expected_provenance = {
        **source_provenance,
        "schema": "chronicle-ds-bench-reference-sut-reuse-v1",
        "images": sorted(SUT_IMAGE_NAMES - {"chronicle"}),
    }
    if dict(provenance) != expected_provenance:
        raise HarnessError(
            "reference SUT reuse provenance does not match its sealed archive"
        )
    for name in SUT_IMAGE_NAMES - {"chronicle"}:
        if images.get("images", {}).get(name) != expected[name]:
            raise HarnessError(
                f"reused reference image {name} differs from its sealed archive"
            )
    return json.loads((archive / "manifest.json").read_text())


def adapter_digest(
    pin_file: Path = PIN_FILE,
    patch_file: Path = PATCH_FILE,
    overlay_dir: Path = OVERLAY_DIR,
) -> str:
    parts: list[tuple[str, bytes]] = [
        ("upstream.json", pin_file.read_bytes()),
        ("patches/chronicle.patch", patch_file.read_bytes()),
    ]
    for path in sorted(p for p in overlay_dir.rglob("*") if p.is_file()):
        parts.append((f"overlay/{path.relative_to(overlay_dir).as_posix()}", path.read_bytes()))
    return _sha256_parts(parts)


def git_worktree_identity(repo: Path) -> dict[str, Any]:
    commit = _run(["git", "rev-parse", "HEAD"], cwd=repo, capture=True).stdout.strip()
    diff_command: list[str | Path] = ["git", "diff", "--binary", "HEAD", "--", "."]
    diff_command.extend(
        f":(exclude){prefix.rstrip('/')}" for prefix in SOURCE_IDENTITY_EXCLUDES
    )
    diff = _run(diff_command, cwd=repo, capture=True).stdout.encode()
    untracked_output = _run(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"],
        cwd=repo,
        capture=True,
    ).stdout
    untracked = sorted(
        path
        for path in untracked_output.split("\0")
        if path
        and not any(path.startswith(prefix) for prefix in SOURCE_IDENTITY_EXCLUDES)
    )
    parts: list[tuple[str, bytes]] = [("tracked.diff", diff)]
    for relative in untracked:
        path = repo / relative
        if path.is_file():
            parts.append((f"untracked/{relative}", path.read_bytes()))
    return {
        "commit": commit,
        "dirty": bool(diff or untracked),
        "diff_sha256": _sha256_parts(parts),
        "untracked_files": untracked,
    }


def prepare_sources(
    *,
    sources_file: Path = SOURCES_FILE,
    destination_root: Path = PREPARED_ROOT / "sources",
) -> dict[str, Any]:
    sources = json.loads(sources_file.read_text())
    prepared: dict[str, Any] = {}
    for name in ("rust", "node"):
        spec = sources[name]
        commit = str(spec["commit"])
        if len(commit) != 40:
            raise HarnessError(f"{sources_file}: {name} commit must be a full Git SHA")
        target = destination_root / f"{name}-{commit[:12]}"
        marker_data = {
            "schema": "chronicle-ds-bench-source-v1",
            "name": name,
            "url": spec["url"],
            "commit": commit,
            "subdirectory": spec["subdirectory"],
        }
        if target.exists():
            if _read_marker(target) != marker_data:
                raise HarnessError(f"refusing to replace incomplete source checkout: {target}")
        else:
            destination_root.mkdir(parents=True, exist_ok=True)
            temporary = Path(tempfile.mkdtemp(prefix=f".source-{name}-", dir=destination_root))
            try:
                _run(["git", "init", "--quiet"], cwd=temporary)
                _run(["git", "remote", "add", "origin", spec["url"]], cwd=temporary)
                if spec["subdirectory"] != ".":
                    _run(["git", "sparse-checkout", "init", "--cone"], cwd=temporary)
                    _run(
                        ["git", "sparse-checkout", "set", spec["subdirectory"]],
                        cwd=temporary,
                    )
                _run(
                    [
                        "git",
                        "fetch",
                        "--quiet",
                        "--depth=1",
                        "--filter=blob:none",
                        "origin",
                        commit,
                    ],
                    cwd=temporary,
                )
                _run(["git", "checkout", "--quiet", "--detach", "FETCH_HEAD"], cwd=temporary)
                head = _run(
                    ["git", "rev-parse", "HEAD"], cwd=temporary, capture=True
                ).stdout.strip()
                dirty = _run(
                    ["git", "status", "--porcelain"], cwd=temporary, capture=True
                ).stdout
                if head != commit or dirty:
                    raise HarnessError(f"{name} did not resolve to a clean pinned source")
                (temporary / MARKER_FILE).write_text(
                    json.dumps(marker_data, indent=2) + "\n"
                )
                os.replace(temporary, target)
            except BaseException:
                shutil.rmtree(temporary, ignore_errors=True)
                raise
        source_path = (
            target if spec["subdirectory"] == "." else target / spec["subdirectory"]
        )
        if not source_path.is_dir():
            raise HarnessError(f"prepared source subdirectory is missing: {source_path}")
        prepared[name] = {
            **marker_data,
            "checkout": str(target),
            "build_context": str(source_path),
            "provenance": spec["provenance"],
        }
    prepared["ursula"] = sources["ursula"]
    prepared["redis"] = sources["redis"]
    return prepared


def _registry_request(url: str, *, token: str | None = None) -> urllib.request.Request:
    headers = {
        "Accept": (
            "application/vnd.oci.image.index.v1+json,"
            "application/vnd.docker.distribution.manifest.list.v2+json,"
            "application/vnd.oci.image.manifest.v1+json,"
            "application/vnd.docker.distribution.manifest.v2+json"
        )
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return urllib.request.Request(url, headers=headers)


def resolve_registry_image(image: str) -> dict[str, str]:
    """Resolve a public Docker Hub or GHCR tag to an immutable manifest digest."""
    if "@" in image:
        base, digest = image.rsplit("@", 1)
        if re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            return {"reference": image, "digest": digest, "tag_reference": image}
        raise HarnessError(f"invalid digest-pinned image: {image}")
    last = image.rsplit("/", 1)[-1]
    if ":" in last:
        base, tag = image.rsplit(":", 1)
    else:
        base, tag = image, "latest"
    if "/" not in base or base.split("/", 1)[0] not in {"ghcr.io", "docker.io"}:
        registry = "registry-1.docker.io"
        repository = base if "/" in base else f"library/{base}"
        display_base = base
    else:
        host, repository = base.split("/", 1)
        registry = "registry-1.docker.io" if host == "docker.io" else host
        display_base = base
    url = f"https://{registry}/v2/{repository}/manifests/{tag}"
    token: str | None = None
    try:
        response = urllib.request.urlopen(_registry_request(url), timeout=30)
    except urllib.error.HTTPError as exc:
        if exc.code != 401:
            raise HarnessError(f"cannot resolve image {image}: HTTP {exc.code}") from exc
        challenge = exc.headers.get("WWW-Authenticate", "")
        values = dict(re.findall(r'([a-z]+)="([^"]+)"', challenge))
        realm = values.get("realm")
        if not realm:
            raise HarnessError(f"registry did not provide an auth realm for {image}") from exc
        query = urllib.parse.urlencode(
            {
                "service": values.get("service", registry),
                "scope": values.get("scope", f"repository:{repository}:pull"),
            }
        )
        try:
            with urllib.request.urlopen(f"{realm}?{query}", timeout=30) as token_response:
                token_data = json.load(token_response)
            token = token_data.get("token") or token_data["access_token"]
            response = urllib.request.urlopen(_registry_request(url, token=token), timeout=30)
        except (OSError, KeyError, json.JSONDecodeError) as token_exc:
            raise HarnessError(f"cannot authenticate to resolve image {image}") from token_exc
    try:
        body = response.read()
        digest = response.headers.get("Docker-Content-Digest")
    finally:
        response.close()
    if digest is None:
        digest = f"sha256:{hashlib.sha256(body).hexdigest()}"
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise HarnessError(f"registry returned an invalid digest for {image}: {digest}")
    return {
        "reference": f"{display_base}@{digest}",
        "digest": digest,
        "tag_reference": image,
    }


def _find_digest(value: Any) -> str | None:
    if isinstance(value, str):
        match = re.search(r"sha256:[0-9a-f]{64}", value)
        return match.group(0) if match else None
    if isinstance(value, Mapping):
        for child in value.values():
            digest = _find_digest(child)
            if digest:
                return digest
    if isinstance(value, list):
        for child in value:
            digest = _find_digest(child)
            if digest:
                return digest
    return None


def _resolve_artifact_registry_image(tag_reference: str, project: str) -> dict[str, str]:
    result = _run(
        [
            "gcloud",
            "artifacts",
            "docker",
            "images",
            "describe",
            tag_reference,
            "--project",
            project,
            "--format=json",
        ],
        capture=True,
    )
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise HarnessError(f"gcloud returned invalid image metadata for {tag_reference}") from exc
    digest = _find_digest(data)
    if digest is None:
        raise HarnessError(f"no manifest digest found for {tag_reference}")
    untagged = tag_reference.rsplit(":", 1)[0]
    return {
        "reference": f"{untagged}@{digest}",
        "digest": digest,
        "tag_reference": tag_reference,
    }


def _temporary_build_context(source: Path, dockerfile: Path, root: Path) -> Path:
    target = root / source.name
    shutil.copytree(
        source,
        target,
        ignore=shutil.ignore_patterns(
            ".git",
            "target",
            "node_modules",
            "dist",
            ".tmp",
        ),
    )
    shutil.copy2(dockerfile, target / "Dockerfile")
    (target / ".gcloudignore").write_text(
        ".git/\ntarget/\n**/target/\nnode_modules/\n**/node_modules/\ndist/\n**/dist/\n"
    )
    return target


def _temporary_chronicle_build_context(source: Path, root: Path) -> Path:
    """Copy only packages needed to build cmd/chronicle into an upload context."""
    target = root / "chronicle"
    target.mkdir()
    for name in ("Dockerfile", "go.mod", "go.sum"):
        shutil.copy2(source / name, target / name)
    for path in source.glob("*.go"):
        if not path.name.endswith("_test.go"):
            shutil.copy2(path, target / path.name)
    ignored = shutil.ignore_patterns(
        "*_test.go",
        "testdata",
        "leanoracle",
        "__pycache__",
    )
    for relative in (
        "auth",
        "cmd/chronicle",
        "internal",
        "metrics",
        "protocol",
        "store",
        "webhook",
    ):
        shutil.copytree(source / relative, target / relative, ignore=ignored)
    (target / ".gcloudignore").write_text(
        ".git/\n**/*_test.go\n**/testdata/\n**/leanoracle/\n"
    )
    return target


def docker_build_context_identity(source: Path, dockerfile: Path) -> dict[str, Any]:
    """Identify the exact generic Docker context sent to a remote builder."""
    with tempfile.TemporaryDirectory(prefix="docker-build-identity-") as raw:
        context = _temporary_build_context(source, dockerfile, Path(raw))
        files = [
            path
            for path in context.rglob("*")
            if path.is_file()
        ]
        return {
            "build_context_sha256": sha256_tree(context),
            "build_context_file_count": len(files),
        }


def dsbench_build_identity(checkout: Path) -> dict[str, Any]:
    """Identify the patched client source and exact upload context."""
    return {
        "commit": UpstreamPin.load().commit,
        "adapter_sha256": adapter_digest(),
        **docker_build_context_identity(
            checkout / "ds-bench",
            checkout / "dockerfiles" / "ds-bench.Dockerfile",
        ),
    }


def dsbench_image_source_matches(
    actual: Mapping[str, Any] | None,
    expected: Mapping[str, Any],
) -> bool:
    """Match the bytes sent to Cloud Build, independent of runtime-only adapter files."""
    if actual is None:
        return False
    return all(
        actual.get(key) == expected.get(key)
        for key in (
            "commit",
            "build_context_sha256",
            "build_context_file_count",
        )
    )


def chronicle_build_identity(source: Path) -> dict[str, Any]:
    """Identify the exact minimal context that produces the Chronicle image."""
    commit = _run(
        ["git", "rev-parse", "HEAD"], cwd=source, capture=True
    ).stdout.strip()
    with tempfile.TemporaryDirectory(prefix="chronicle-build-identity-") as raw:
        context = _temporary_chronicle_build_context(source, Path(raw))
        files = sorted(
            path.relative_to(context).as_posix()
            for path in context.rglob("*")
            if path.is_file()
        )
        return {
            "schema": "chronicle-image-source-v1",
            "commit": commit,
            "build_context_sha256": sha256_tree(context),
            "build_context_file_count": len(files),
        }


def _reusable_image(
    candidate: Mapping[str, Any] | None,
    expected_source: Mapping[str, Any],
    *,
    registry: str,
) -> bool:
    if candidate is None or candidate.get("source") != expected_source:
        return False
    reference = str(candidate.get("reference", ""))
    digest = str(candidate.get("digest", ""))
    return (
        reference.startswith(f"{registry}/")
        and f"@{digest}" in reference
        and re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is not None
    )


def build_images(
    target: str,
    *,
    output: Path,
    project: str = "adityavkk-prototyping",
    ar_location: str = "europe-west1",
    ar_repo: str = "ds-bench",
    reuse: Path | None = None,
    reuse_suts_from_archive: Path | None = None,
    reuse_reference_suts_from_archive: Path | None = None,
) -> dict[str, Any]:
    """Build changed images and resolve every campaign image to a digest."""
    if reuse_suts_from_archive is not None and target != "remote":
        raise HarnessError("sealed SUT reuse is supported only for remote builds")
    if reuse_reference_suts_from_archive is not None and target != "remote":
        raise HarnessError(
            "sealed reference SUT reuse is supported only for remote builds"
        )
    if (
        reuse_suts_from_archive is not None
        and reuse_reference_suts_from_archive is not None
    ):
        raise HarnessError("full and reference-only sealed SUT reuse are exclusive")
    report = preflight(
        target,
        project=project,
        ar_location=ar_location,
        ar_repo=ar_repo,
        phase="build",
    )
    preflight_path = output.with_name(f"{output.stem}-preflight.json")
    preflight_path.parent.mkdir(parents=True, exist_ok=True)
    preflight_path.write_text(json.dumps(report, indent=2) + "\n")
    if not report["ok"]:
        raise HarnessError(
            "preflight failed before image build: " + ", ".join(report["required_failures"])
        )

    checkout = prepare_checkout()
    dsbench_tag = f"{UpstreamPin.load().commit[:12]}-{adapter_digest()[:12]}"
    registry = (
        f"{ar_location}-docker.pkg.dev/{project}/{ar_repo}"
        if target == "remote"
        else None
    )
    build_specs: dict[str, tuple[Path, Path]] = {
        "ds-bench": (
            checkout / "ds-bench",
            checkout / "dockerfiles" / "ds-bench.Dockerfile",
        ),
    }
    source_identities: dict[str, dict[str, Any]] = {
        "ds-bench": dsbench_build_identity(checkout),
    }
    image_entries: dict[str, Any] = {}
    external_images: dict[str, Any] = {}
    sut_reuse: dict[str, Any] | None = None
    reference_sut_reuse: dict[str, Any] | None = None

    if reuse_suts_from_archive is not None:
        assert registry is not None
        image_entries, sut_reuse = load_reused_sut_images(
            reuse_suts_from_archive,
            project=project,
            registry=registry,
        )
    else:
        chronicle_identity = git_worktree_identity(REPO_ROOT)
        chronicle_tag = (
            f"{chronicle_identity['commit'][:12]}-"
            f"{chronicle_identity['diff_sha256'][:12]}"
        )
        build_specs.update(
            {
                "chronicle": (REPO_ROOT, REPO_ROOT / "Dockerfile"),
            }
        )
        source_identities.update(
            {
                "chronicle": chronicle_build_identity(REPO_ROOT),
            }
        )
        if reuse_reference_suts_from_archive is not None:
            assert registry is not None
            sealed_images, source_provenance = load_reused_sut_images(
                reuse_reference_suts_from_archive,
                project=project,
                registry=registry,
            )
            for name in SUT_IMAGE_NAMES - {"chronicle"}:
                image_entries[name] = sealed_images[name]
            reference_sut_reuse = {
                **source_provenance,
                "schema": "chronicle-ds-bench-reference-sut-reuse-v1",
                "images": sorted(SUT_IMAGE_NAMES - {"chronicle"}),
            }
        else:
            sources = prepare_sources()
            build_specs.update(
                {
                    "rust": (
                        Path(sources["rust"]["build_context"]),
                        checkout / "dockerfiles" / "durable-streams.Dockerfile",
                    ),
                    "node": (
                        Path(sources["node"]["build_context"]),
                        checkout / "dockerfiles" / "durable-node.Dockerfile",
                    ),
                }
            )
            source_identities.update(
                {
                    "rust": {
                        "commit": sources["rust"]["commit"],
                        "provenance": sources["rust"]["provenance"],
                        **docker_build_context_identity(
                            build_specs["rust"][0],
                            build_specs["rust"][1],
                        ),
                    },
                    "node": {
                        "commit": sources["node"]["commit"],
                        "provenance": sources["node"]["provenance"],
                        **docker_build_context_identity(
                            build_specs["node"][0],
                            build_specs["node"][1],
                        ),
                    },
                }
            )
            source_specs = json.loads(SOURCES_FILE.read_text())
            external_images = {
                "ursula": resolve_registry_image(source_specs["ursula"]["image"]),
                "redis": resolve_registry_image(source_specs["redis"]["image"]),
            }

    if target == "remote":
        assert registry is not None
        tags = {
            "ds-bench": f"{registry}/ds-bench:{dsbench_tag}",
        }
        if reuse_suts_from_archive is None:
            tags["chronicle"] = f"{registry}/chronicle:{chronicle_tag}"
            if "rust" in source_identities:
                tags["rust"] = (
                    f"{registry}/durable-streams:"
                    f"{source_identities['rust']['commit'][:12]}"
                )
            if "node" in source_identities:
                tags["node"] = (
                    f"{registry}/durable-node:"
                    f"{source_identities['node']['commit'][:12]}"
                )
    else:
        tags = {
            "ds-bench": f"ds-bench:{dsbench_tag}",
            "chronicle": f"chronicle:{chronicle_tag}",
            "rust": f"durable-streams:{source_identities['rust']['commit'][:12]}",
            "node": f"durable-node:{source_identities['node']['commit'][:12]}",
        }

    reusable_entries: Mapping[str, Any] = {}
    if reuse is not None:
        try:
            previous = json.loads(reuse.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise HarnessError(f"cannot reuse image manifest {reuse}: {exc}") from exc
        if (
            previous.get("schema") != "chronicle-ds-bench-images-v1"
            or previous.get("target") != target
            or previous.get("project") != (project if target == "remote" else None)
        ):
            raise HarnessError(f"{reuse}: image manifest does not match this build target")
        reusable_entries = previous.get("images", {})

    if target == "remote":
        assert registry is not None
        for name, (context, dockerfile) in build_specs.items():
            candidate = reusable_entries.get(name)
            if _reusable_image(
                candidate,
                source_identities[name],
                registry=registry,
            ):
                image_entries[name] = dict(candidate)
                continue
            temporary_context = tempfile.TemporaryDirectory(prefix=f"dsbench-{name}-")
            if name == "chronicle":
                build_context = _temporary_chronicle_build_context(
                    context, Path(temporary_context.name)
                )
            else:
                build_context = _temporary_build_context(
                    context, dockerfile, Path(temporary_context.name)
                )
            try:
                _run(
                    [
                        "gcloud",
                        "builds",
                        "submit",
                        build_context,
                        "--project",
                        project,
                        "--tag",
                        tags[name],
                    ]
                )
            finally:
                if temporary_context is not None:
                    temporary_context.cleanup()
            image_entries[name] = _resolve_artifact_registry_image(tags[name], project)
    else:
        for name, (context, dockerfile) in build_specs.items():
            _run(["docker", "build", "-t", tags[name], "-f", dockerfile, context])
            inspect = _run(
                ["docker", "image", "inspect", tags[name], "--format", "{{.Id}}"],
                capture=True,
            ).stdout.strip()
            image_entries[name] = {
                "reference": tags[name],
                "digest": inspect,
                "tag_reference": tags[name],
            }
        cluster_name = os.environ.get("KIND_CLUSTER", "ds-bench")
        local_env = os.environ.copy()
        local_env.update({"DS_TARGET": "local", "KIND_CLUSTER": cluster_name})
        _run(
            ["bash", checkout / "scripts" / "cluster-up.sh"],
            cwd=checkout,
            env=local_env,
        )
        _run(
            [
                "kind",
                "load",
                "docker-image",
                *tags.values(),
                "--name",
                cluster_name,
            ]
        )

    image_entries.update(external_images)
    for name, identity in source_identities.items():
        image_entries[name]["source"] = identity
    manifest = {
        "schema": "chronicle-ds-bench-images-v1",
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "target": target,
        "project": project if target == "remote" else None,
        "registry": registry,
        "preflight": str(preflight_path.resolve()),
        "reused_from": str(reuse.resolve()) if reuse is not None else None,
        "sut_reused_from": sut_reuse,
        "reference_suts_reused_from": reference_sut_reuse,
        "images": image_entries,
    }
    output.write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def _marker(pin: UpstreamPin, digest: str) -> dict[str, str]:
    return {
        "schema": "chronicle-ds-bench-prepared-v1",
        "upstream_url": pin.url,
        "upstream_commit": pin.commit,
        "adapter_sha256": digest,
    }


def _read_marker(path: Path) -> dict[str, Any] | None:
    marker = path / MARKER_FILE
    if not marker.is_file():
        return None
    try:
        return json.loads(marker.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def prepare_checkout(
    *,
    pin: UpstreamPin | None = None,
    destination_root: Path = PREPARED_ROOT,
    patch_file: Path | None = PATCH_FILE,
    overlay_dir: Path | None = OVERLAY_DIR,
    identity_digest: str | None = None,
) -> Path:
    """Prepare the exact upstream revision plus adapter, atomically and idempotently."""
    pin = pin or UpstreamPin.load()
    if identity_digest is None:
        if patch_file == PATCH_FILE and overlay_dir == OVERLAY_DIR:
            identity_digest = adapter_digest()
        else:
            parts: list[tuple[str, bytes]] = []
            if patch_file is not None:
                parts.append(("patch", patch_file.read_bytes()))
            if overlay_dir is not None:
                for path in sorted(p for p in overlay_dir.rglob("*") if p.is_file()):
                    parts.append((path.relative_to(overlay_dir).as_posix(), path.read_bytes()))
            identity_digest = _sha256_parts(parts)
    expected = _marker(pin, identity_digest)
    target = destination_root / f"{pin.commit[:12]}-{identity_digest[:12]}"

    if target.exists():
        if _read_marker(target) != expected:
            raise HarnessError(f"refusing to replace incomplete prepared checkout: {target}")
        head = _run(["git", "rev-parse", "HEAD"], cwd=target, capture=True).stdout.strip()
        if head != pin.commit:
            raise HarnessError(f"prepared checkout HEAD changed: {target}")
        return target

    destination_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".prepare-", dir=destination_root))
    try:
        _run(["git", "init", "--quiet"], cwd=temporary)
        _run(["git", "remote", "add", "origin", pin.url], cwd=temporary)
        _run(["git", "fetch", "--quiet", "--depth=1", "origin", pin.commit], cwd=temporary)
        _run(["git", "checkout", "--quiet", "--detach", "FETCH_HEAD"], cwd=temporary)
        head = _run(["git", "rev-parse", "HEAD"], cwd=temporary, capture=True).stdout.strip()
        dirty = _run(["git", "status", "--porcelain"], cwd=temporary, capture=True).stdout
        if head != pin.commit or dirty:
            raise HarnessError("upstream checkout did not resolve to a clean pinned commit")

        if patch_file is not None:
            _run(["git", "apply", "--check", patch_file], cwd=temporary)
            _run(["git", "apply", patch_file], cwd=temporary)
        if overlay_dir is not None:
            shutil.copytree(overlay_dir, temporary, dirs_exist_ok=True)

        (temporary / MARKER_FILE).write_text(json.dumps(expected, indent=2) + "\n")
        os.replace(temporary, target)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return target


def load_suite(path: Path) -> dict[str, Any]:
    try:
        suite = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessError(f"cannot load suite {path}: {exc}") from exc
    required = ("suite", "cluster", "modes", "stream_counts")
    missing = [key for key in required if key not in suite]
    if missing:
        raise HarnessError(f"{path}: missing fields: {', '.join(missing)}")
    return suite


def validate_chronicle_suite(path: Path, budget: ResourceBudget | None = None) -> None:
    suite = load_suite(path)
    if "chronicle" not in suite["modes"]:
        return
    metadata_budget = suite.get("benchmark_meta", {}).get("resource_budget")
    if budget is None and isinstance(metadata_budget, dict):
        budget = ResourceBudget(**metadata_budget)
    if budget is None:
        server_cpus = int(suite["cluster"].get("server_cpus", 4))
        budget = ResourceBudget(
            cpu_millis=server_cpus * 1000,
            memory_mib=16 * 1024 if server_cpus == 4 else 2 * 1024,
            local_ssds=1,
        )
    entries = suite.get("server_configs", {}).get("chronicle", [])
    if not entries:
        raise HarnessError(f"{path}: Chronicle mode requires explicit server configs")
    for entry in entries:
        raw = str(entry.get("args", ""))
        fields = raw.split(":")
        if fields[0] not in {"always", "everysec", "no"}:
            raise HarnessError(f"{path}: invalid Chronicle config {entry!r}")
        topology = parse_topology_args(raw, budget)
        if topology is not None:
            continue
        if len(fields) != 3:
            raise HarnessError(f"{path}: invalid Chronicle config {entry!r}")
        try:
            split = ChronicleSplit(
                int(fields[1]) * 1000,
                int(fields[2]) * 1000,
                4 * 1024 if budget.memory_mib == 16 * 1024 else budget.memory_mib // 2,
                12 * 1024 if budget.memory_mib == 16 * 1024 else budget.memory_mib // 2,
            )
        except ValueError as exc:
            raise HarnessError(f"{path}: nonnumeric Chronicle CPU split") from exc
        split.validate(budget)


def _sum_int_fields(data: Mapping[str, Any], names: Sequence[str]) -> int:
    total = 0
    for name in names:
        value = data.get(name, 0)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            total += int(value)
    return total


def validate_write_cell(cell: Mapping[str, Any]) -> CellVerdict:
    reasons: list[str] = []
    status = str(cell.get("status", "missing"))
    errors = _sum_int_fields(
        cell,
        ("errors", "error_count", "other_err", "write_err", "read_err"),
    )
    lazy_creates = _sum_int_fields(
        cell,
        ("lazy_creates", "lazy_create_count", "streams_created_during_measurement"),
    )
    windows_aligned = bool(cell.get("windows_aligned", True))
    client_bound = bool(cell.get("client_bound", False))
    plateau = bool(cell.get("saturated", False)) and cell.get("reason") == "plateau"

    if status != "ok":
        reasons.append(f"status={status}")
    if errors:
        reasons.append(f"errors={errors}")
    if lazy_creates:
        reasons.append(f"lazy_creates={lazy_creates}")
    if not windows_aligned:
        reasons.append("windows_not_aligned")
    if client_bound:
        reasons.append("client_bound")
    if not plateau:
        reasons.append(str(cell.get("reason", "ceiling_not_proven")))
    return CellVerdict(
        valid=not reasons,
        status=status,
        reasons=tuple(reasons),
        errors=errors,
        lazy_creates=lazy_creates,
        windows_aligned=windows_aligned,
        client_bound=client_bound,
        plateau=plateau,
    )


def validate_write_artifacts(
    raw_root: Path,
    *,
    label: str,
    mode: str,
    stream_count: int,
    cell: Mapping[str, Any],
    required_repeats: int = 3,
) -> CellVerdict:
    """Validate a write headline against archived per-pod evidence."""
    if required_repeats < 1:
        raise HarnessError("required write confirmation repeats must be positive")
    reasons: list[str] = []
    status = str(cell.get("status", "missing"))
    plateau = bool(cell.get("saturated", False)) and cell.get("reason") == "plateau"
    pinned_pods = cell.get("pinned_pods")
    errors = 0
    lazy_creates = 0
    windows_aligned = True
    client_bound = not plateau
    if status != "ok":
        reasons.append(f"status={status}")
    if not isinstance(pinned_pods, int) or pinned_pods < 1:
        reasons.append("missing_pinned_pods")
        rep_dirs: list[Path] = []
    else:
        rep_dirs = sorted(
            (
                raw_root
                / label
                / "cells"
                / mode
                / f"n{stream_count}"
            ).glob(f"p{pinned_pods}-r*")
        )
    if len(rep_dirs) != required_repeats:
        reasons.append(f"confirmation_reps={len(rep_dirs)}/{required_repeats}")
    for rep_dir in rep_dirs:
        try:
            merged = json.loads((rep_dir / "merged.json").read_text())
        except (OSError, json.JSONDecodeError):
            reasons.append(f"{rep_dir.name}:missing_merged")
            windows_aligned = False
            continue
        if merged.get("windows_aligned") is not True:
            windows_aligned = False
            reasons.append(f"{rep_dir.name}:windows_not_aligned")
        if merged.get("pods_reported") != pinned_pods:
            reasons.append(f"{rep_dir.name}:partial_fleet")
        pod_files = sorted((rep_dir / "fleet").glob("*.json"))
        if len(pod_files) != pinned_pods:
            reasons.append(f"{rep_dir.name}:raw_pod_count={len(pod_files)}")
        for pod_file in pod_files:
            try:
                pod = json.loads(pod_file.read_text())
            except (OSError, json.JSONDecodeError):
                reasons.append(f"{rep_dir.name}:{pod_file.name}:invalid_json")
                continue
            counts = pod.get("counts", {})
            errors += _sum_int_fields(
                counts, ("other_err", "backpressure", "errors", "error_count")
            )
            lazy_creates += int(pod.get("lazy_creates", 0) or 0)
    if errors:
        reasons.append(f"errors={errors}")
    if lazy_creates:
        reasons.append(f"lazy_creates={lazy_creates}")
    if not plateau:
        reasons.append(str(cell.get("reason", "ceiling_not_proven")))
    return CellVerdict(
        valid=not reasons,
        status=status,
        reasons=tuple(dict.fromkeys(reasons)),
        errors=errors,
        lazy_creates=lazy_creates,
        windows_aligned=windows_aligned,
        client_bound=client_bound,
        plateau=plateau,
    )


def _durability_for_suite(suite: Mapping[str, Any]) -> dict[str, str]:
    return {
        str(config["label"]): str(config["durability"])
        for config in suite.get("benchmark_meta", {}).get("configs", [])
    }


def _positive_float(data: Mapping[str, Any], key: str) -> float | None:
    value = data.get(key)
    if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        return None
    value = float(value)
    return value if value > 0 else None


def _catchup_seed_evidence(
    raw_root: Path,
    suite: Mapping[str, Any],
    row: Mapping[str, Any],
) -> list[dict[str, Any]]:
    label = str(row.get("mode", suite["modes"][0]))
    stream_count = row.get("stream_count")
    connections = row.get("connections")
    if not isinstance(stream_count, int) or not isinstance(connections, int):
        return []
    mode = str(suite["modes"][0])
    cell = (
        raw_root
        / label
        / "cells"
        / mode
        / f"n{stream_count}-c{connections}"
    )
    evidence = []
    for path in sorted(cell.glob("p*-r*/fleet/*.json")):
        try:
            record = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if record.get("scenario") == "reads":
            evidence.append(record)
    return evidence


def _exact_catchup_seed_reasons(
    suite: Mapping[str, Any],
    row: Mapping[str, Any],
    evidence: Sequence[Mapping[str, Any]],
) -> list[str]:
    expected_bytes = int(suite.get("reads", {}).get("seed_bytes", 0))
    expected_streams = row.get("stream_count")
    if expected_bytes <= 0 or not isinstance(expected_streams, int):
        return ["missing_catchup_seed_inputs"]
    if not evidence:
        return ["missing_catchup_seed_evidence"]
    reasons = []
    for index, record in enumerate(evidence):
        prefix = f"catchup_seed[{index}]"
        if record.get("seed_bytes") != expected_bytes:
            reasons.append(
                f"{prefix}.target={record.get('seed_bytes')},expected={expected_bytes}"
            )
        if record.get("seed_verified_streams") != expected_streams:
            reasons.append(
                f"{prefix}.streams={record.get('seed_verified_streams')},"
                f"expected={expected_streams}"
            )
        for key in ("seed_verified_min_bytes", "seed_verified_max_bytes"):
            if record.get(key) != expected_bytes:
                reasons.append(
                    f"{prefix}.{key}={record.get(key)},expected={expected_bytes}"
                )
    return reasons


def _nonwrite_pod_memory_peak_mib(
    raw_root: Path,
    suite: Mapping[str, Any],
    row: Mapping[str, Any],
) -> float | None:
    label = str(row.get("mode", suite["modes"][0]))
    stream_count = row.get("stream_count")
    if not isinstance(stream_count, int):
        return None
    workload = str(suite["benchmark_meta"]["workload"])
    if workload in {"blog-sse", "reads-sse", "reads-catchup"}:
        level = row.get("connections")
        cell_name = f"n{stream_count}-c{level}"
    else:
        level = row.get("level")
        cell_name = f"n{stream_count}-l{level}"
    if not isinstance(level, int):
        return None

    peaks = []
    mode = str(suite["modes"][0])
    for path in (
        raw_root / label / "cells" / mode / cell_name
    ).glob("p*-r*/samples.csv"):
        try:
            with path.open(newline="") as stream:
                reader = csv.DictReader(stream)
                values = [
                    int(record["pod_ws_bytes"])
                    for record in reader
                    if record.get("pod_ws_bytes")
                ]
        except (OSError, ValueError, KeyError):
            continue
        if values:
            peaks.append(max(values) / (1024 * 1024))
    return max(peaks) if peaks else None


def classify_nonwrite_row(
    suite: Mapping[str, Any],
    row: Mapping[str, Any],
    *,
    catchup_seed_evidence: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[str, list[str], float | None, float | None]:
    """Separate invalid setup from valid overload observations."""
    workload = str(suite["benchmark_meta"]["workload"])
    invalid_reasons: list[str] = []
    overload_reasons: list[str] = []
    offered_rate: float | None = None
    completion_ratio: float | None = None

    status_ok = row.get("status") == "ok"

    error_count = _sum_int_fields(
        row,
        (
            "other_err",
            "backpressure",
            "write_err",
            "write_bp",
            "read_err",
            "read_bp",
        ),
    )
    if error_count:
        overload_reasons.append(f"errors={error_count}")

    if workload == "reads-catchup":
        seed_bytes = int(suite.get("reads", {}).get("seed_bytes", 0))
        if catchup_seed_evidence is not None:
            invalid_reasons.extend(
                _exact_catchup_seed_reasons(
                    suite,
                    row,
                    catchup_seed_evidence,
                )
            )
            completed_reads = all(
                _positive_float(row, key) is not None
                for key in ("ops_per_sec", "bytes_per_sec", "p99")
            )
            if not status_ok and (invalid_reasons or not completed_reads):
                invalid_reasons.insert(0, f"status={row.get('status')}")
        else:
            ops_per_sec = _positive_float(row, "ops_per_sec")
            bytes_per_sec = _positive_float(row, "bytes_per_sec")
            actual_bytes = (
                bytes_per_sec / ops_per_sec
                if ops_per_sec is not None and bytes_per_sec is not None
                else None
            )
            if (
                seed_bytes <= 0
                or actual_bytes is None
                or not math.isclose(
                    actual_bytes,
                    float(seed_bytes),
                    rel_tol=0.0,
                    abs_tol=0.5,
                )
            ):
                actual = "missing" if actual_bytes is None else f"{actual_bytes:.3f}"
                invalid_reasons.append(
                    f"catchup_seed_bytes_per_op={actual},expected={seed_bytes}"
                )
            if not status_ok:
                invalid_reasons.insert(0, f"status={row.get('status')}")
    elif workload in {"blog-sse", "reads-sse"}:
        if not status_ok:
            invalid_reasons.append(f"status={row.get('status')}")
        reads = suite.get("reads", {})
        connections = row.get("connections")
        append_rate = reads.get("append_rate_per_sec")
        if not isinstance(connections, (int, float)) or not isinstance(
            append_rate, (int, float)
        ):
            invalid_reasons.append("missing_offered_rate_inputs")
        else:
            offered_rate = float(connections) * float(append_rate)
            achieved = _positive_float(row, "ops_per_sec")
            if achieved is None:
                invalid_reasons.append("missing_rate_metric=ops_per_sec")
            elif offered_rate > 0:
                completion_ratio = achieved / offered_rate
    elif workload in {"mixed-writes", "mixed-delivery"}:
        if not status_ok:
            invalid_reasons.append(f"status={row.get('status')}")
        mixed = suite.get("mixed", {})
        stream_count = row.get("stream_count")
        writers_per_stream = mixed.get("writers_per_stream")
        writer_rate = row.get("writer_rate", mixed.get("writer_rate"))
        if not all(
            isinstance(value, (int, float))
            for value in (stream_count, writers_per_stream, writer_rate)
        ):
            invalid_reasons.append("missing_offered_rate_inputs")
        elif float(writer_rate) > 0:
            offered_rate = (
                float(stream_count)
                * float(writers_per_stream)
                * float(writer_rate)
            )
            achieved = _positive_float(row, "write_ops_per_sec")
            if achieved is None:
                invalid_reasons.append("missing_rate_metric=write_ops_per_sec")
            else:
                completion_ratio = achieved / offered_rate
    elif not status_ok:
        invalid_reasons.append(f"status={row.get('status')}")

    if completion_ratio is not None and completion_ratio < 0.98:
        overload_reasons.append(
            f"completion_ratio={completion_ratio:.4f}<0.9800"
        )

    if invalid_reasons:
        return (
            "invalid",
            list(dict.fromkeys([*invalid_reasons, *overload_reasons])),
            offered_rate,
            completion_ratio,
        )
    if overload_reasons:
        return (
            "overload",
            list(dict.fromkeys(overload_reasons)),
            offered_rate,
            completion_ratio,
        )
    return "result", [], offered_rate, completion_ratio


def validation_plan(
    manifest: Mapping[str, Any],
    execution: Mapping[str, Any],
) -> list[dict[str, str]]:
    plan = campaign_plan(manifest)
    selected = execution.get("selected_suites")
    if selected is None:
        return plan
    if not isinstance(selected, list) or any(not isinstance(item, str) for item in selected):
        raise HarnessError("execution selected_suites must be a list of suite names")
    selected_set = set(selected)
    known = {item["suite"] for item in plan}
    unknown = selected_set - known
    if unknown:
        raise HarnessError(
            "execution selects suites absent from its manifest: "
            + ", ".join(sorted(unknown))
        )
    return [item for item in plan if item["suite"] in selected_set]


def validate_archive(archive: Path) -> dict[str, Any]:
    try:
        manifest = json.loads((archive / "manifest.json").read_text())
        execution = json.loads((archive / "execution.json").read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessError(f"invalid campaign archive {archive}: {exc}") from exc
    evidence_seal = (
        verify_archive_seal(archive)
        if (archive / "evidence-checksums.json").is_file()
        else None
    )
    run_status = {run["suite"]: run for run in execution.get("runs", [])}
    cells: list[dict[str, Any]] = []
    for planned in validation_plan(manifest, execution):
        run_dir = archive / "runs" / planned["suite"]
        suite_path = run_dir / "suite.json"
        if not suite_path.is_file():
            cells.append(
                {
                    "suite": planned["suite"],
                    "classification": "gap",
                    "reasons": ["suite_not_executed"],
                }
            )
            continue
        suite = load_suite(suite_path)
        meta = suite["benchmark_meta"]
        raw_root = run_dir / "raw"
        workload = meta["workload"]
        durability = _durability_for_suite(suite)
        if workload == "write":
            mode = suite["modes"][0]
            for config in suite["server_configs"][mode]:
                label = config["label"]
                cells_path = raw_root / label / "cells.json"
                try:
                    stored = json.loads(cells_path.read_text())["cells"]
                except (OSError, KeyError, json.JSONDecodeError):
                    stored = {}
                for stream_count in suite["stream_counts"]:
                    cell = stored.get(str(stream_count))
                    if not isinstance(cell, Mapping):
                        cells.append(
                            {
                                "suite": planned["suite"],
                                "system": meta["system"],
                                "workload": workload,
                                "label": label,
                                "durability": durability[label],
                                "stream_count": stream_count,
                                "classification": "gap",
                                "reasons": ["missing_cell"],
                            }
                        )
                        continue
                    required_repeats = int(
                        suite.get("saturation", {}).get("repeats", 3)
                    )
                    verdict = validate_write_artifacts(
                        raw_root,
                        label=label,
                        mode=mode,
                        stream_count=stream_count,
                        cell=cell,
                        required_repeats=required_repeats,
                    )
                    lower_bound_only_reasons = {
                        str(cell.get("reason", "ceiling_not_proven")),
                        *(
                            f"confirmation_reps={repeat}/{required_repeats}"
                            for repeat in range(1, required_repeats)
                        ),
                    }
                    classification = (
                        "headline"
                        if verdict.valid
                        else "lower_bound"
                        if (
                            verdict.status == "ok"
                            and cell.get("reason") == "ladder_exhausted"
                            and verdict.errors == 0
                            and verdict.lazy_creates == 0
                            and verdict.windows_aligned
                            and set(verdict.reasons) <= lower_bound_only_reasons
                        )
                        else "invalid"
                    )
                    cells.append(
                        {
                            "suite": planned["suite"],
                            "system": meta["system"],
                            "workload": workload,
                            "label": label,
                            "durability": durability[label],
                            "stream_count": stream_count,
                            "classification": classification,
                            "reasons": list(verdict.reasons),
                            "throughput": cell.get("throughput"),
                            "p50_ms": cell.get("p50"),
                            "p99_ms": cell.get("p99"),
                            "pod_memory_peak_mib": cell.get("pod_mem_mb"),
                            "pod_memory_p50_mib": cell.get("pod_mem_p50_mb"),
                            "walk": cell.get("walk"),
                        }
                    )
        else:
            aggregate_path = raw_root / "aggregate.json"
            try:
                aggregate = json.loads(aggregate_path.read_text())
            except (OSError, json.JSONDecodeError):
                aggregate = []
            if not aggregate:
                cells.append(
                    {
                        "suite": planned["suite"],
                        "system": meta["system"],
                        "workload": workload,
                        "classification": "gap",
                        "reasons": ["missing_aggregate"],
                    }
                )
                continue
            for row in aggregate:
                label = str(row.get("mode", suite["modes"][0]))
                catchup_evidence = (
                    _catchup_seed_evidence(raw_root, suite, row)
                    if workload == "reads-catchup"
                    else None
                )
                if catchup_evidence:
                    row = {
                        **row,
                        "seed_append_attempts": sum(
                            int(record.get("seed_append_attempts", 0) or 0)
                            for record in catchup_evidence
                        ),
                        "seed_append_failures": sum(
                            int(record.get("seed_append_failures", 0) or 0)
                            for record in catchup_evidence
                        ),
                        "seed_retry_rounds": sum(
                            int(record.get("seed_retry_rounds", 0) or 0)
                            for record in catchup_evidence
                        ),
                        "seed_verified_streams": min(
                            int(record.get("seed_verified_streams", 0) or 0)
                            for record in catchup_evidence
                        ),
                        "seed_verified_min_bytes": min(
                            int(record.get("seed_verified_min_bytes", 0) or 0)
                            for record in catchup_evidence
                        ),
                        "seed_verified_max_bytes": max(
                            int(record.get("seed_verified_max_bytes", 0) or 0)
                            for record in catchup_evidence
                        ),
                    }
                classification, reasons, offered_rate, completion_ratio = (
                    classify_nonwrite_row(
                        suite,
                        row,
                        catchup_seed_evidence=catchup_evidence,
                    )
                )
                cells.append(
                    {
                        "suite": planned["suite"],
                        "system": meta["system"],
                        "workload": workload,
                        "label": label,
                        "durability": durability.get(label, "unknown"),
                        "stream_count": row.get("stream_count"),
                        "classification": classification,
                        "reasons": reasons,
                        "offered_rate": offered_rate,
                        "completion_ratio": completion_ratio,
                        "pod_memory_peak_mib": _nonwrite_pod_memory_peak_mib(
                            raw_root,
                            suite,
                            row,
                        ),
                        "metrics": row,
                    }
                )
        if planned["suite"] in run_status and run_status[planned["suite"]]["returncode"] != 0:
            cells.append(
                {
                    "suite": planned["suite"],
                    "system": meta["system"],
                    "workload": workload,
                    "classification": "gap",
                    "reasons": [f"run_returncode={run_status[planned['suite']]['returncode']}"],
                }
            )
    invalid = [
        cell for cell in cells if cell["classification"] in {"invalid", "gap"}
    ]
    overload = [cell for cell in cells if cell["classification"] == "overload"]
    validation = {
        "schema": "chronicle-ds-bench-validation-v1",
        "campaign_id": manifest["campaign_id"],
        "complete": execution.get("complete", False) and not invalid,
        "cell_count": len(cells),
        "invalid_or_gap_count": len(invalid),
        "overload_count": len(overload),
        "evidence_seal": evidence_seal,
        "cells": cells,
    }
    (archive / "validation.json").write_text(json.dumps(validation, indent=2) + "\n")
    return validation


def _format_number(value: Any, digits: int = 1) -> str:
    if not isinstance(value, (int, float)):
        return "—"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.1f}k"
    return f"{value:.{digits}f}"


def combine_archive_validations(
    archive: Path,
    supplements: Sequence[Path] = (),
) -> dict[str, Any]:
    """Replace complete system/workload slices with corrected rerun archives."""
    primary = validate_archive(archive)
    cells = [{**cell, "source_campaign_id": primary["campaign_id"]} for cell in primary["cells"]]
    source_archives = [
        {
            "campaign_id": primary["campaign_id"],
            "archive": str(archive.resolve()),
            "role": "primary",
            "evidence_seal": primary["evidence_seal"],
        }
    ]
    executions = [json.loads((archive / "execution.json").read_text())]

    for supplement in supplements:
        validation = validate_archive(supplement)
        execution = json.loads((supplement / "execution.json").read_text())
        manifest = json.loads((supplement / "manifest.json").read_text())
        selected_suites = set(execution.get("selected_suites", []))
        if not selected_suites:
            raise HarnessError(
                f"{supplement}: a supplement must record selected_suites"
            )
        pairs = {
            (item["system"], item["workload"])
            for item in campaign_plan(manifest)
            if item["suite"] in selected_suites
        }
        if any(not system or not workload for system, workload in pairs):
            raise HarnessError(
                f"{supplement}: selected suites lack system/workload metadata"
            )
        cells = [
            cell
            for cell in cells
            if (cell.get("system"), cell.get("workload")) not in pairs
        ]
        cells.extend(
            {
                **cell,
                "source_campaign_id": validation["campaign_id"],
            }
            for cell in validation["cells"]
        )
        executions.append(execution)
        source_archives.append(
            {
                "campaign_id": validation["campaign_id"],
                "archive": str(supplement.resolve()),
                "role": "supplement",
                "evidence_seal": validation["evidence_seal"],
                "replaces": [
                    {"system": system, "workload": workload}
                    for system, workload in sorted(pairs)
                ],
            }
        )

    invalid = [
        cell for cell in cells if cell["classification"] in {"invalid", "gap"}
    ]
    overload = [cell for cell in cells if cell["classification"] == "overload"]
    return {
        "schema": "chronicle-ds-bench-combined-validation-v1",
        "campaign_id": primary["campaign_id"],
        "complete": all(execution.get("complete") for execution in executions)
        and not invalid,
        "cell_count": len(cells),
        "invalid_or_gap_count": len(invalid),
        "overload_count": len(overload),
        "source_archives": source_archives,
        "cells": cells,
    }


def _cell_level(cell: Mapping[str, Any]) -> Any:
    metrics = cell.get("metrics", {})
    return metrics.get("connections", metrics.get("level"))


def _find_result_cell(
    cells: Sequence[Mapping[str, Any]],
    *,
    workload: str,
    system: str,
    label: str,
    stream_count: int,
    level: int | None = None,
) -> Mapping[str, Any] | None:
    for cell in cells:
        if (
            cell.get("workload") == workload
            and cell.get("system") == system
            and cell.get("label") == label
            and cell.get("stream_count") == stream_count
            and (level is None or _cell_level(cell) == level)
        ):
            return cell
    return None


_DURABLE_REFERENCE_CONFIGS = (
    ("rust", "rust-wal"),
    ("ursula", "ursula-disk"),
)


def _scaling_cell_metric_fields(
    workload: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if workload == "write":
        return ("throughput",), ("p50_ms", "p99_ms")
    if workload in {"blog-sse", "reads-sse"}:
        return ("ops_per_sec",), ("p50", "p99")
    if workload == "reads-catchup":
        return ("bytes_per_sec", "ops_per_sec"), ("p50", "p99")
    if workload == "mixed-writes":
        return (
            "write_ops_per_sec",
            "read_mib_per_sec",
        ), (
            "write_p50",
            "write_p99",
            "read_p50",
            "read_p99",
        )
    if workload == "mixed-delivery":
        return (
            "write_ops_per_sec",
            "events_per_sec",
        ), (
            "write_p50",
            "write_p99",
            "delivery_p50",
            "delivery_p99",
        )
    raise HarnessError(f"unsupported scaling workload: {workload}")


def _scaling_cell_metric(
    cell: Mapping[str, Any],
    name: str,
) -> float | None:
    source = cell if name in cell else cell.get("metrics", {})
    value = source.get(name) if isinstance(source, Mapping) else None
    if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        return None
    return float(value)


def _scaling_expected_cells(
    archive: Path,
    manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    expected = []
    for record in manifest["campaign"]["suites"]:
        archived = archive / "runs" / str(record["suite"]) / "suite.json"
        source = archived if archived.is_file() else Path(str(record["absolute_path"]))
        suite = load_suite(source)
        workload = str(suite["benchmark_meta"]["workload"])
        for stream_count in suite["stream_counts"]:
            if workload == "write":
                levels: Sequence[int | None] = (None,)
            elif workload in {"blog-sse", "reads-sse", "reads-catchup"}:
                levels = tuple(int(value) for value in suite["reads"]["connection_levels"])
            else:
                levels = tuple(int(value) for value in suite["mixed"]["levels"])
            for level in levels:
                expected.append(
                    {
                        "suite": record["suite"],
                        "workload": workload,
                        "stream_count": int(stream_count),
                        "level": level,
                    }
                )
    return expected


def _matching_scaling_cell(
    cells: Sequence[Mapping[str, Any]],
    *,
    workload: str,
    stream_count: int,
    level: int | None,
    system: str,
    label: str,
) -> Mapping[str, Any] | None:
    return _find_result_cell(
        cells,
        workload=workload,
        system=system,
        label=label,
        stream_count=stream_count,
        level=level,
    )


def evaluate_scaling_archive(archive: Path) -> dict[str, Any]:
    manifest = json.loads((archive / "manifest.json").read_text())
    if manifest.get("execution_kind") != "scaling":
        raise HarnessError(f"{archive}: not a scaling campaign")
    scaling = validate_archive(archive)
    references = manifest["campaign"].get("reference_archives", [])
    if not references:
        raise HarnessError("scaling manifest has no durable reference archives")
    reference_validation = combine_archive_validations(
        Path(str(references[0]["path"])),
        [Path(str(item["path"])) for item in references[1:]],
    )
    target = manifest["campaign"]["target"]
    expected = _scaling_expected_cells(archive, manifest)
    candidates = []

    for declared in manifest["campaign"]["candidate_order"]:
        label = str(declared["label"])
        topology = declared["topology"]
        cell_results = []
        passed_checks = 0
        total_checks = 0
        throughput_ratios = []
        for identity in expected:
            workload = identity["workload"]
            stream_count = identity["stream_count"]
            level = identity["level"]
            candidate = _matching_scaling_cell(
                scaling["cells"],
                workload=workload,
                stream_count=stream_count,
                level=level,
                system="chronicle",
                label=label,
            )
            durable_cells = [
                cell
                for system, reference_label in _DURABLE_REFERENCE_CONFIGS
                if (
                    cell := _matching_scaling_cell(
                        reference_validation["cells"],
                        workload=workload,
                        stream_count=stream_count,
                        level=level,
                        system=system,
                        label=reference_label,
                    )
                )
                is not None
                and cell.get("classification")
                in {"headline", "lower_bound", "result"}
            ]
            failures = []
            checks = []
            observed: dict[str, float | None] = {}
            reference_targets: dict[str, float] = {}
            limits: dict[str, float] = {}
            if candidate is None:
                failures.append("missing_candidate_cell")
            if not durable_cells:
                failures.append("missing_valid_durable_reference")

            if candidate is not None:
                total_checks += 1
                if candidate.get("classification") in {"headline", "result"}:
                    passed_checks += 1
                    checks.append("valid_setup")
                else:
                    failures.append(
                        f"classification={candidate.get('classification', 'missing')}"
                    )

                completion = candidate.get("completion_ratio")
                if completion is not None:
                    observed["completion_ratio"] = (
                        float(completion)
                        if isinstance(completion, (int, float))
                        else None
                    )
                    limits["completion_ratio_min"] = float(
                        target["completion_ratio"]
                    )
                    total_checks += 1
                    if (
                        isinstance(completion, (int, float))
                        and float(completion) >= float(target["completion_ratio"])
                    ):
                        passed_checks += 1
                        checks.append("completion")
                    else:
                        failures.append(
                            f"completion={completion}<"
                            f"{target['completion_ratio']}"
                        )

            throughput_fields, latency_fields = _scaling_cell_metric_fields(workload)
            if candidate is not None and durable_cells:
                for field in throughput_fields:
                    reference_values = [
                        value
                        for cell in durable_cells
                        if (value := _scaling_cell_metric(cell, field)) is not None
                        and value > 0
                    ]
                    if not reference_values:
                        continue
                    total_checks += 1
                    reference_value = max(reference_values)
                    candidate_value = _scaling_cell_metric(candidate, field)
                    observed[field] = candidate_value
                    reference_targets[field] = reference_value
                    ratio = (
                        candidate_value / reference_value
                        if candidate_value is not None
                        else 0.0
                    )
                    throughput_ratios.append(ratio)
                    if ratio >= float(target["throughput_ratio"]):
                        passed_checks += 1
                        checks.append(f"{field}_ratio={ratio:.4f}")
                    else:
                        failures.append(
                            f"{field}_ratio={ratio:.4f}<"
                            f"{target['throughput_ratio']:.4f}"
                        )

                for field in latency_fields:
                    reference_values = [
                        value
                        for cell in durable_cells
                        if (value := _scaling_cell_metric(cell, field)) is not None
                        and value > 0
                    ]
                    if not reference_values:
                        continue
                    total_checks += 1
                    reference_value = min(reference_values)
                    candidate_value = _scaling_cell_metric(candidate, field)
                    limit = reference_value * float(target["latency_ratio"])
                    observed[field] = candidate_value
                    reference_targets[field] = reference_value
                    limits[field] = limit
                    if candidate_value is not None and candidate_value <= limit:
                        passed_checks += 1
                        checks.append(f"{field}<={limit:.4f}")
                    else:
                        failures.append(
                            f"{field}={candidate_value}>limit={limit:.4f}"
                        )

                reference_memory = [
                    value
                    for cell in durable_cells
                    if (
                        value := _scaling_cell_metric(
                            cell,
                            "pod_memory_peak_mib",
                        )
                    )
                    is not None
                    and value > 0
                ]
                if reference_memory:
                    total_checks += 1
                    candidate_memory = _scaling_cell_metric(
                        candidate,
                        "pod_memory_peak_mib",
                    )
                    reference_limit = min(reference_memory) * float(
                        target["memory_ratio"]
                    )
                    declared_limit = float(topology["effective"]["memory_mib"])
                    memory_limit = min(reference_limit, declared_limit)
                    observed["pod_memory_peak_mib"] = candidate_memory
                    reference_targets["pod_memory_peak_mib"] = min(
                        reference_memory
                    )
                    limits["pod_memory_peak_mib"] = memory_limit
                    if (
                        candidate_memory is not None
                        and candidate_memory <= memory_limit
                    ):
                        passed_checks += 1
                        checks.append(f"memory<={memory_limit:.1f}MiB")
                    else:
                        failures.append(
                            f"memory={candidate_memory}>limit={memory_limit:.1f}MiB"
                        )

            cell_results.append(
                {
                    **identity,
                    "checks": checks,
                    "failures": failures,
                    "observed": observed,
                    "reference_targets": reference_targets,
                    "limits": limits,
                }
            )

        requested = topology["requested"]
        candidates.append(
            {
                "label": label,
                "args": declared["args"],
                "topology": topology,
                "qualifies": all(not cell["failures"] for cell in cell_results),
                "passed_checks": passed_checks,
                "total_checks": total_checks,
                "coverage_ratio": (
                    passed_checks / total_checks if total_checks else 0.0
                ),
                "worst_throughput_ratio": (
                    min(throughput_ratios) if throughput_ratios else 0.0
                ),
                "minimal_order": [
                    int(requested["chronicle_cpu_millis"])
                    + int(requested["redis_cpu_millis"]),
                    int(requested["chronicle_memory_mib"])
                    + int(requested["redis_memory_mib"]),
                    int(topology["redis_masters"]),
                    int(topology["chronicle_replicas"]),
                ],
                "cells": cell_results,
            }
        )

    for candidate in candidates:
        dominators = []
        for other in candidates:
            if other is candidate:
                continue
            no_more_resources = all(
                left <= right
                for left, right in zip(
                    other["minimal_order"],
                    candidate["minimal_order"],
                    strict=True,
                )
            )
            no_worse_performance = (
                other["coverage_ratio"] >= candidate["coverage_ratio"]
                and other["worst_throughput_ratio"]
                >= candidate["worst_throughput_ratio"]
            )
            strictly_better = (
                other["minimal_order"] != candidate["minimal_order"]
                or other["coverage_ratio"] > candidate["coverage_ratio"]
                or other["worst_throughput_ratio"]
                > candidate["worst_throughput_ratio"]
            )
            if no_more_resources and no_worse_performance and strictly_better:
                dominators.append(other["label"])
        candidate["dominated_by"] = sorted(dominators)

    qualifying = sorted(
        (candidate for candidate in candidates if candidate["qualifies"]),
        key=lambda candidate: tuple(candidate["minimal_order"]),
    )
    non_dominated = [
        candidate for candidate in candidates if not candidate["dominated_by"]
    ]
    closest = sorted(
        non_dominated or candidates,
        key=lambda candidate: (
            -float(candidate["coverage_ratio"]),
            -float(candidate["worst_throughput_ratio"]),
            tuple(candidate["minimal_order"]),
        ),
    )
    result = {
        "schema": "chronicle-ds-bench-scaling-evaluation-v1",
        "campaign_id": manifest["campaign_id"],
        "target": target,
        "reference_campaigns": [
            item["campaign_id"] for item in references
        ],
        "candidate_count": len(candidates),
        "minimal_qualifying": qualifying[0]["label"] if qualifying else None,
        "closest_nonqualifying": (
            None if qualifying or not closest else closest[0]["label"]
        ),
        "candidates": candidates,
    }
    (archive / "scaling-evaluation.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    return result


def render_scaling_report(archive: Path) -> Path:
    evaluation = evaluate_scaling_archive(archive)
    lines = [
        "# Chronicle configuration scaling report",
        "",
        (
            f"Minimal qualifying configuration: "
            f"`{evaluation['minimal_qualifying']}`."
            if evaluation["minimal_qualifying"]
            else (
                "No evaluated configuration met every gate. "
                f"Closest candidate: `{evaluation['closest_nonqualifying']}`."
            )
        ),
        "",
        "| configuration | Chronicle replicas | Redis masters | checks passed | worst throughput ratio | qualifies | dominated by |",
        "|---|---:|---:|---:|---:|---|---|",
    ]
    for candidate in evaluation["candidates"]:
        topology = candidate["topology"]
        lines.append(
            f"| {candidate['label']} | {topology['chronicle_replicas']} | "
            f"{topology['redis_masters']} | {candidate['passed_checks']}/"
            f"{candidate['total_checks']} | "
            f"{candidate['worst_throughput_ratio']:.1%} | "
            f"{'yes' if candidate['qualifies'] else 'no'} | "
            f"{', '.join(candidate['dominated_by']) or 'none'} |"
        )
    lines.extend(
        [
            "",
            "## Direct cell comparison",
            "",
            "| configuration | workload | streams and level | throughput observed / target | latency observed / limit | memory observed / limit | cell passes |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for candidate in evaluation["candidates"]:
        for cell in candidate["cells"]:
            throughput_fields, latency_fields = _scaling_cell_metric_fields(
                cell["workload"]
            )
            observed = cell["observed"]
            targets = cell["reference_targets"]
            limits = cell["limits"]
            throughput_parts = []
            for field in throughput_fields:
                if field not in targets:
                    continue
                value = observed.get(field)
                target_value = targets[field]
                ratio = (
                    value / target_value
                    if isinstance(value, (int, float)) and target_value > 0
                    else None
                )
                throughput_parts.append(
                    f"{field}={_format_number(value)}/"
                    f"{_format_number(target_value)} "
                    f"({_percent(ratio)})"
                )
            latency_parts = [
                f"{field}={_format_number(observed.get(field))}/"
                f"{_format_number(limits[field])}"
                for field in latency_fields
                if field in limits
            ]
            memory = (
                f"{_format_number(observed.get('pod_memory_peak_mib'))}/"
                f"{_format_number(limits['pod_memory_peak_mib'])} MiB"
                if "pod_memory_peak_mib" in limits
                else "not recorded"
            )
            level = (
                str(cell["stream_count"])
                if cell["level"] is None
                else f"{cell['stream_count']} / {cell['level']}"
            )
            lines.append(
                f"| {candidate['label']} | {cell['workload']} | {level} | "
                f"{'<br>'.join(throughput_parts) or 'not recorded'} | "
                f"{'<br>'.join(latency_parts) or 'not recorded'} | "
                f"{memory} | {'yes' if not cell['failures'] else 'no'} |"
            )
    lines.extend(["", "## Failed gates", ""])
    for candidate in evaluation["candidates"]:
        failed = [
            cell
            for cell in candidate["cells"]
            if cell["failures"]
        ]
        if not failed:
            continue
        lines.append(f"### {candidate['label']}")
        lines.append("")
        for cell in failed:
            level = "" if cell["level"] is None else f", level {cell['level']}"
            lines.append(
                f"- {cell['workload']}, {cell['stream_count']} streams{level}: "
                + "; ".join(cell["failures"])
            )
        lines.append("")
    lines.extend(
        [
            "## Downside",
            "",
            "All Redis masters share one server node and one local SSD. This report "
            "measures software sharding and process scaling, not added disks, "
            "machines, replication, or availability.",
        ]
    )
    path = archive / "scaling-report.md"
    path.write_text("\n".join(lines) + "\n")
    return path


def _verdict_text(cell: Mapping[str, Any]) -> str:
    verdict = str(cell.get("classification", "unknown"))
    reasons = cell.get("reasons", [])
    if reasons:
        verdict += f" ({', '.join(map(str, reasons))})"
    return verdict


def _percent(value: Any, digits: int = 1) -> str:
    if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        return "—"
    return f"{float(value) * 100:.{digits}f}%"


def _machine_vcpus(machine: str) -> int:
    match = re.search(r"-(\d+)(?:-[a-z0-9]+)?$", machine)
    if match is None:
        raise HarnessError(f"cannot infer vCPU count from machine type: {machine}")
    return int(match.group(1))


def archive_runtime_summary(archive: Path) -> dict[str, Any]:
    manifest = json.loads((archive / "manifest.json").read_text())
    execution = json.loads((archive / "execution.json").read_text())
    elapsed: list[float] = []
    timing_source = "execution"
    for run in execution.get("runs", []):
        seconds = run.get("elapsed_seconds")
        if isinstance(seconds, (int, float)) and seconds >= 0:
            elapsed.append(float(seconds))
            continue
        timing_source = "filesystem-mtime"
        run_dir = archive / "runs" / str(run["suite"])
        start = run_dir / "pre-run-cluster-check.json"
        end = run_dir / "teardown-proof.json"
        if not end.is_file():
            end = run_dir / "teardown-proof-after-retry.json"
        if start.is_file() and end.is_file():
            elapsed.append(max(0.0, end.stat().st_mtime - start.stat().st_mtime))
    hardware = manifest.get("hardware", {})
    cluster_hours = sum(elapsed) / 3600.0
    if all(
        key in hardware
        for key in ("server_machine", "client_machine", "client_nodes")
    ):
        server_vcpus = _machine_vcpus(str(hardware["server_machine"]))
        client_vcpus = _machine_vcpus(str(hardware["client_machine"]))
        client_nodes = int(hardware["client_nodes"])
    else:
        server_vcpus = client_vcpus = client_nodes = 0
    return {
        "campaign_id": manifest["campaign_id"],
        "suite_count": len(elapsed),
        "timing_source": timing_source,
        "cluster_hours": cluster_hours,
        "server_vcpu_hours": cluster_hours * server_vcpus,
        "spot_client_vcpu_hours": cluster_hours * client_vcpus * client_nodes,
        "total_vcpu_hours": cluster_hours
        * (server_vcpus + client_vcpus * client_nodes),
    }


def render_report(archive: Path, supplements: Sequence[Path] = ()) -> Path:
    validation = combine_archive_validations(archive, supplements)
    (archive / "combined-validation.json").write_text(
        json.dumps(validation, indent=2) + "\n"
    )
    manifest = json.loads((archive / "manifest.json").read_text())
    source_manifests = [
        (
            source,
            json.loads((Path(source["archive"]) / "manifest.json").read_text()),
        )
        for source in validation["source_archives"]
    ]
    seal_repairs = []
    for source in validation["source_archives"]:
        repair_path = Path(source["archive"]) / "seal-repair.json"
        if repair_path.is_file():
            seal_repairs.append(
                (source["campaign_id"], json.loads(repair_path.read_text()))
            )
    runtime_summaries = [
        archive_runtime_summary(Path(source["archive"]))
        for source in validation["source_archives"]
    ]
    write_cells = [cell for cell in validation["cells"] if cell.get("workload") == "write"]
    other_cells = [
        cell
        for cell in validation["cells"]
        if cell.get("workload") not in {None, "write"}
    ]
    rust_wal_100k = _find_result_cell(
        write_cells,
        workload="write",
        system="rust",
        label="rust-wal",
        stream_count=100_000,
    )
    chronicle_always_100k = _find_result_cell(
        write_cells,
        workload="write",
        system="chronicle",
        label="chronicle-redis-aof-always",
        stream_count=100_000,
    )
    ursula_disk_100k = _find_result_cell(
        write_cells,
        workload="write",
        system="ursula",
        label="ursula-disk",
        stream_count=100_000,
    )
    durable_finding = (
        "The 100,000-stream durable write result is unavailable."
    )
    if rust_wal_100k and chronicle_always_100k and ursula_disk_100k:
        rust_rate = rust_wal_100k.get("throughput")
        chronicle_rate = chronicle_always_100k.get("throughput")
        ursula_rate = ursula_disk_100k.get("throughput")
        if all(
            isinstance(value, (int, float))
            for value in (rust_rate, chronicle_rate, ursula_rate)
        ):
            durable_finding = (
                f"At 100,000 streams, Chronicle AOF `always` reached "
                f"{_format_number(chronicle_rate)} append/s. Rust WAL reached at "
                f"least {_format_number(rust_rate)} append/s because its load "
                f"ladder ended before a plateau. Ursula disk reached "
                f"{_format_number(ursula_rate)} append/s. Chronicle was no more "
                f"than {float(chronicle_rate) / float(rust_rate):.2f} times the Rust "
                f"lower bound and {float(chronicle_rate) / float(ursula_rate):.2f} "
                "times Ursula disk."
            )

    blog_chronicle = [
        cell
        for cell in other_cells
        if cell.get("workload") == "blog-sse"
        and cell.get("system") == "chronicle"
        and _cell_level(cell) == 1_000
    ]
    blog_completion = [
        cell["completion_ratio"]
        for cell in blog_chronicle
        if isinstance(cell.get("completion_ratio"), (int, float))
    ]
    fanout_finding = (
        "The 1,000-subscriber Chronicle fanout result is unavailable."
    )
    if blog_completion:
        fanout_finding = (
            "At 1,000 subscribers and 50 appends/s, Chronicle delivered "
            f"{_percent(min(blog_completion))} to {_percent(max(blog_completion))} "
            "of the declared event rate. The detailed table keeps this as an "
            "overload observation rather than a completed 50,000-event/s cell."
        )

    catchup_512 = [
        cell
        for cell in other_cells
        if cell.get("workload") == "reads-catchup"
        and _cell_level(cell) == 512
    ]
    valid_catchup_512 = [
        cell
        for cell in catchup_512
        if cell.get("classification") not in {"invalid", "gap"}
    ]
    if catchup_512 and len(valid_catchup_512) == len(catchup_512):
        chronicle_catchup = [
            cell
            for cell in valid_catchup_512
            if cell.get("system") == "chronicle"
        ]
        rates = [
            cell.get("metrics", {}).get("mib_per_sec")
            for cell in chronicle_catchup
            if isinstance(cell.get("metrics", {}).get("mib_per_sec"), (int, float))
        ]
        catchup_cells = [
            cell
            for cell in other_cells
            if cell.get("workload") == "reads-catchup"
        ]
        chronicle_seed_attempts = sum(
            int(cell.get("metrics", {}).get("seed_append_attempts", 0) or 0)
            for cell in catchup_cells
            if cell.get("system") == "chronicle"
        )
        chronicle_seed_failures = sum(
            int(cell.get("metrics", {}).get("seed_append_failures", 0) or 0)
            for cell in catchup_cells
            if cell.get("system") == "chronicle"
        )
        other_seed_failures = sum(
            int(cell.get("metrics", {}).get("seed_append_failures", 0) or 0)
            for cell in catchup_cells
            if cell.get("system") != "chronicle"
        )
        seed_note = ""
        if chronicle_seed_attempts:
            seed_note = (
                f" Chronicle recovered {_format_number(chronicle_seed_failures)} "
                f"failed seed appends out of {_format_number(chronicle_seed_attempts)} "
                f"attempts ({chronicle_seed_failures / chronicle_seed_attempts:.3%}) "
                "before measurement."
            )
            if other_seed_failures == 0:
                seed_note += (
                    " Rust, Node, and Ursula recorded no failed seed appends."
                )
        catchup_finding = (
            "Every 512-reader catchup row passed exact seed validation. Chronicle "
            + (
                f"replayed {_format_number(min(rates))} to "
                f"{_format_number(max(rates))} MiB/s across its tested arms and "
                "stream cardinalities."
                if rates
                else "has no numeric catchup rate."
            )
            + seed_note
        )
    else:
        catchup_finding = (
            f"Catchup is not yet publishable: {len(catchup_512) - len(valid_catchup_512)} "
            "of the 512-reader rows are invalid or missing. Corrected supplement "
            "archives must replace the whole catchup slice."
        )

    lines = [
        f"# Chronicle ds-bench comparison: {manifest['campaign_id']}",
        "",
        "This report compares Chronicle with the official Rust durable-streams server, "
        "the Node reference server, and Ursula. Every result uses the same pinned "
        "upstream ds-bench commit and single-node 4 vCPU, 16 GiB, one-local-NVMe "
        "SUT budget. Corrected adapter revisions are recorded per archive. Chronicle "
        "and Redis share that budget.",
        "",
        "Durability classes are reported separately. Redis AOF `everysec` and all "
        "memory arms are weaker than the local-WAL or Redis AOF `always` arms.",
        "",
        "Corrected supplement archives replace the complete matching system and "
        "workload slice. They never fill individual missing cells from an older run.",
        "",
        "## Executive findings",
        "",
        f"- {durable_finding}",
        "",
        f"- {fanout_finding}",
        "",
        f"- {catchup_finding}",
        "",
        f"- The combined dataset contains {validation['overload_count']} valid "
        "overload observations and "
        f"{validation['invalid_or_gap_count']} invalid or missing rows. Overload "
        "is measured behavior. Invalid setup is not a performance result.",
        "",
        "## Write saturation",
        "",
        "| system | configuration | durability | streams | append/s | p50 ms | p99 ms | peak MiB | verdict |",
        "|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for cell in write_cells:
        verdict = cell["classification"]
        if cell.get("reasons"):
            verdict += f" ({', '.join(cell['reasons'])})"
        lines.append(
            "| {system} | {label} | {durability} | {streams} | {throughput} | "
            "{p50} | {p99} | {memory} | {verdict} |".format(
                system=cell.get("system", "—"),
                label=cell.get("label", "—"),
                durability=cell.get("durability", "—"),
                streams=cell.get("stream_count", "—"),
                throughput=_format_number(cell.get("throughput")),
                p50=_format_number(cell.get("p50_ms")),
                p99=_format_number(cell.get("p99_ms")),
                memory=_format_number(cell.get("pod_memory_peak_mib"), 0),
                verdict=verdict,
            )
        )
    lines.extend(
        [
            "",
            "`lower_bound` means the offered-load ladder ended before a plateau. "
            "It is not presented as a measured ceiling.",
            "",
            "## Blog fanout at 1,000 subscribers",
            "",
            "| system | configuration | delivered events/s | completion | p50 ms | p99 ms | verdict |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
    )
    for cell in sorted(
        (
            cell
            for cell in other_cells
            if cell.get("workload") == "blog-sse"
            and _cell_level(cell) == 1_000
        ),
        key=lambda cell: (str(cell.get("system")), str(cell.get("label"))),
    ):
        metrics = cell.get("metrics", {})
        lines.append(
            f"| {cell.get('system', '—')} | {cell.get('label', '—')} | "
            f"{_format_number(metrics.get('ops_per_sec'))} | "
            f"{_percent(cell.get('completion_ratio'))} | "
            f"{_format_number(metrics.get('p50'))} | "
            f"{_format_number(metrics.get('p99'))} | "
            f"{_verdict_text(cell)} |"
        )

    lines.extend(
        [
            "",
            "The declared offered rate is 50,000 event deliveries per second. "
            "Completion below 98 percent is labeled overload.",
            "",
            "## SSE scale at 2,048 connections",
            "",
            "| system | configuration | streams | delivered events/s | completion | p99 ms | verdict |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
    )
    for cell in sorted(
        (
            cell
            for cell in other_cells
            if cell.get("workload") == "reads-sse"
            and _cell_level(cell) == 2_048
        ),
        key=lambda cell: (
            str(cell.get("system")),
            str(cell.get("label")),
            int(cell.get("stream_count", 0)),
        ),
    ):
        metrics = cell.get("metrics", {})
        lines.append(
            f"| {cell.get('system', '—')} | {cell.get('label', '—')} | "
            f"{cell.get('stream_count', '—')} | "
            f"{_format_number(metrics.get('ops_per_sec'))} | "
            f"{_percent(cell.get('completion_ratio'))} | "
            f"{_format_number(metrics.get('p99'))} | "
            f"{_verdict_text(cell)} |"
        )

    lines.extend(
        [
            "",
            "The declared offered rate is 102,400 event deliveries per second.",
            "",
            "## Catchup at 512 readers",
            "",
            "| system | configuration | streams | full replays/s | MiB/s | p50 ms | p99 ms | verdict |",
            "|---|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for cell in sorted(
        (
            cell
            for cell in other_cells
            if cell.get("workload") == "reads-catchup"
            and _cell_level(cell) == 512
        ),
        key=lambda cell: (
            str(cell.get("system")),
            str(cell.get("label")),
            int(cell.get("stream_count", 0)),
        ),
    ):
        metrics = cell.get("metrics", {})
        lines.append(
            f"| {cell.get('system', '—')} | {cell.get('label', '—')} | "
            f"{cell.get('stream_count', '—')} | "
            f"{_format_number(metrics.get('ops_per_sec'))} | "
            f"{_format_number(metrics.get('mib_per_sec'))} | "
            f"{_format_number(metrics.get('p50'))} | "
            f"{_format_number(metrics.get('p99'))} | "
            f"{_verdict_text(cell)} |"
        )

    lines.extend(
        [
            "",
            "One operation replays a stream whose stored size was verified at exactly "
            "16 MiB before measurement. MiB/s counts response payload bytes, which "
            "can exclude server-specific record framing.",
            "",
            "## Mixed writes with 100,000 readers",
            "",
            "| system | configuration | writes/s | baseline retained | read MiB/s | write p99 ms | errors | verdict |",
            "|---|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    mixed_write_cells = [
        cell for cell in other_cells if cell.get("workload") == "mixed-writes"
    ]
    for cell in sorted(
        (cell for cell in mixed_write_cells if _cell_level(cell) == 100_000),
        key=lambda cell: (str(cell.get("system")), str(cell.get("label"))),
    ):
        metrics = cell.get("metrics", {})
        baseline = next(
            (
                candidate
                for candidate in mixed_write_cells
                if candidate.get("system") == cell.get("system")
                and candidate.get("label") == cell.get("label")
                and _cell_level(candidate) == 0
            ),
            None,
        )
        baseline_rate = (
            baseline.get("metrics", {}).get("write_ops_per_sec")
            if baseline is not None
            else None
        )
        rate = metrics.get("write_ops_per_sec")
        retained = (
            float(rate) / float(baseline_rate)
            if isinstance(rate, (int, float))
            and isinstance(baseline_rate, (int, float))
            and baseline_rate
            else None
        )
        errors = _sum_int_fields(
            metrics, ("write_err", "write_bp", "read_err", "read_bp")
        )
        lines.append(
            f"| {cell.get('system', '—')} | {cell.get('label', '—')} | "
            f"{_format_number(rate)} | {_percent(retained)} | "
            f"{_format_number(metrics.get('read_mib_per_sec'))} | "
            f"{_format_number(metrics.get('write_p99'))} | {errors} | "
            f"{_verdict_text(cell)} |"
        )

    lines.extend(
        [
            "",
            "Baseline retained compares each arm with its own zero-reader cell. "
            "Every arm was offered 50,000 writes per second.",
            "",
            "## Mixed SSE delivery ceiling",
            "",
            "| system | configuration | highest clean offered writes/s | observed writes/s | delivery p99 ms there | unthrottled writes/s | unthrottled events/s | verdict |",
            "|---|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    delivery_cells = [
        cell for cell in other_cells if cell.get("workload") == "mixed-delivery"
    ]
    delivery_configs = sorted(
        {
            (str(cell.get("system")), str(cell.get("label")))
            for cell in delivery_cells
        }
    )
    for system, label in delivery_configs:
        group = [
            cell
            for cell in delivery_cells
            if cell.get("system") == system and cell.get("label") == label
        ]
        clean = [
            cell
            for cell in group
            if _cell_level(cell) not in {None, 0}
            and cell.get("classification") == "result"
        ]
        highest = max(clean, key=lambda cell: int(_cell_level(cell))) if clean else None
        unthrottled = next(
            (cell for cell in group if _cell_level(cell) == 0),
            None,
        )
        highest_metrics = highest.get("metrics", {}) if highest else {}
        unthrottled_metrics = (
            unthrottled.get("metrics", {}) if unthrottled else {}
        )
        lines.append(
            f"| {system} | {label} | "
            f"{_format_number(highest.get('offered_rate') if highest else None)} | "
            f"{_format_number(highest_metrics.get('write_ops_per_sec'))} | "
            f"{_format_number(highest_metrics.get('delivery_p99'))} | "
            f"{_format_number(unthrottled_metrics.get('write_ops_per_sec'))} | "
            f"{_format_number(unthrottled_metrics.get('events_per_sec'))} | "
            f"{_verdict_text(unthrottled) if unthrottled else 'missing'} |"
        )

    lines.extend(
        [
            "",
            "`writer_rate=0` is the upstream unthrottled sentinel. It does not mean "
            "zero writes.",
            "",
            "## Complete read and mixed appendix",
            "",
            "| workload | system | configuration | streams | level | primary rate | p99 ms | verdict |",
            "|---|---|---|---:|---:|---:|---:|---|",
        ]
    )
    for cell in other_cells:
        metrics = cell.get("metrics", {})
        level = metrics.get("connections", metrics.get("level", "—"))
        rate = metrics.get(
            "ops_per_sec",
            metrics.get(
                "write_ops_per_sec",
                metrics.get("events_per_sec"),
            ),
        )
        p99 = metrics.get(
            "p99",
            metrics.get("write_p99", metrics.get("delivery_p99")),
        )
        verdict = cell["classification"]
        if cell.get("reasons"):
            verdict += f" ({', '.join(cell['reasons'])})"
        lines.append(
            f"| {cell.get('workload', '—')} | {cell.get('system', '—')} | "
            f"{cell.get('label', '—')} | {cell.get('stream_count', '—')} | "
            f"{level} | {_format_number(rate)} | {_format_number(p99)} | {verdict} |"
        )
    lines.extend(
        [
            "",
            "## Run inventory and cost boundary",
            "",
            "| campaign | suites | cluster hours | server vCPU-hours | Spot client vCPU-hours | total vCPU-hours | timing source |",
            "|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for runtime in runtime_summaries:
        lines.append(
            f"| {runtime['campaign_id']} | {runtime['suite_count']} | "
            f"{runtime['cluster_hours']:.2f} | "
            f"{runtime['server_vcpu_hours']:.1f} | "
            f"{runtime['spot_client_vcpu_hours']:.1f} | "
            f"{runtime['total_vcpu_hours']:.1f} | "
            f"{runtime['timing_source']} |"
        )
    lines.extend(
        [
            "",
            "This inventory bounds billable machine use. It is not a dollar invoice. "
            "Google Cloud SKU prices, Spot discounts, control-plane charges, taxes, "
            "and billing credits can change, so the authoritative dollar cost must "
            "come from the project billing export.",
            "",
            "## Provenance and validity",
            "",
            "| campaign | role | upstream commit | adapter | Chronicle image | client image | Chronicle build context | client build context |",
            "|---|---|---|---|---|---|---|---|",
        ]
    )
    for source, source_manifest in source_manifests:
        images = source_manifest.get("images", {}).get("images", {})
        chronicle_image = images.get("chronicle", {})
        client_image = images.get("ds-bench", {})
        chronicle_image_source = chronicle_image.get("source", {})
        client_image_source = client_image.get("source", {})
        context_digest = chronicle_image_source.get(
            "build_context_sha256",
            f"legacy worktree {chronicle_image_source.get('diff_sha256', 'unknown')}",
        )
        client_context_digest = client_image_source.get(
            "build_context_sha256",
            f"adapter {client_image_source.get('adapter_sha256', 'unknown')}",
        )
        lines.append(
            f"| {source['campaign_id']} | {source['role']} | "
            f"`{source_manifest['ds_bench']['commit'][:12]}` | "
            f"`{source_manifest['ds_bench']['adapter_sha256'][:12]}` | "
            f"`{str(chronicle_image.get('digest', 'unknown'))[:19]}` | "
            f"`{str(client_image.get('digest', 'unknown'))[:19]}` | "
            f"`{str(context_digest)[:19]}` | "
            f"`{str(client_context_digest)[:19]}` |"
        )
    sealed_sources = [
        source
        for source in validation["source_archives"]
        if source.get("evidence_seal") is not None
    ]
    if sealed_sources:
        lines.extend(["", "Evidence seals verified before report generation:", ""])
        for source in sealed_sources:
            seal = source["evidence_seal"]
            lines.append(
                f"- `{source['campaign_id']}`: `{seal['file_count']}` raw files; "
                f"tree SHA-256 `{seal['tree_sha256']}`."
            )
    if seal_repairs:
        lines.extend(["", "Recorded seal repairs:", ""])
        for campaign_id, repair in seal_repairs:
            lines.append(
                f"- `{campaign_id}`: {repair['reason']} "
                f"{repair['scope']}"
            )
    reused_sut_sources = [
        (source, source_manifest.get("images", {}).get("sut_reused_from"))
        for source, source_manifest in source_manifests
        if source_manifest.get("images", {}).get("sut_reused_from") is not None
    ]
    if reused_sut_sources:
        lines.extend(["", "Server image reuse:"])
        for source, reuse_provenance in reused_sut_sources:
            lines.append(
                f"- `{source['campaign_id']}` uses the exact SUT image digests from "
                f"`{reuse_provenance['campaign_id']}`."
            )
    sut_source = manifest.get("sut_chronicle_source", manifest["chronicle_source"])
    lines.extend(
        [
            "",
            f"- Chronicle SUT source: `{sut_source['commit']}`; worktree "
            f"diff digest `{sut_source['diff_sha256']}`.",
            f"- ds-bench: `{manifest['ds_bench']['commit']}`; adapter "
            f"`{manifest['ds_bench']['adapter_sha256']}`.",
            f"- Chronicle to Redis CPU split: `{manifest['campaign']['chronicle_split']}`.",
            f"- Invalid or missing result records: `{validation['invalid_or_gap_count']}`.",
            f"- Valid overload observations: `{validation['overload_count']}`.",
            "- Data archives: "
            + "; ".join(
                f"`{source['campaign_id']}` ({source['role']})"
                for source in validation["source_archives"]
            )
            + ".",
            "- Raw per-pod JSON, HDR input, merged JSON, samples, resolved suites, logs, "
            "image digests, and teardown proofs are retained in this archive.",
            "",
            "## Threats to validity",
            "",
            "- This is a single-node throughput comparison, not an availability or "
            "replication comparison.",
            "- Chronicle includes an in-pod Redis process. The report counts both "
            "processes and the whole pod working set.",
            "- The official harness runs MinIO outside the 4 vCPU primary SUT budget. "
            "Rust and Ursula can use it as a cold tier, while Chronicle and Node do "
            "not. Server and MinIO logs are retained so overlapping cold-tier work "
            "can be audited.",
            "- Local WAL, Redis AOF `always`, Redis AOF `everysec`, and memory modes "
            "have different acknowledgement guarantees. Their numbers are not merged.",
            "- A result marked as a gap or invalid is not treated as zero.",
        ]
    )
    report_path = archive / "report.md"
    report_path.write_text("\n".join(lines) + "\n")
    return report_path


def select_calibration(results_dir: Path, stream_count: int = 10_000) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for label, split in CALIBRATION_SPLITS.items():
        split.validate()
        path = results_dir / label / "cells.json"
        try:
            cell = json.loads(path.read_text())["cells"][str(stream_count)]
        except (OSError, KeyError, json.JSONDecodeError) as exc:
            raise HarnessError(f"missing calibration cell: {path}") from exc
        verdict = validate_write_artifacts(
            results_dir,
            label=label,
            mode="chronicle",
            stream_count=stream_count,
            cell=cell,
        )
        throughput_samples = cell.get("confirmed_throughputs")
        raw_values: list[float] = []
        if cell.get("pinned_pods") is not None:
            raw_files = sorted(
                (
                    results_dir
                    / label
                    / "cells"
                    / "chronicle"
                    / f"n{stream_count}"
                ).glob(f"p{int(cell['pinned_pods'])}-r*/merged.json")
            )
            for raw_file in raw_files:
                try:
                    raw = json.loads(raw_file.read_text())
                    value = raw["aggregate_ops_per_sec"]
                except (OSError, KeyError, json.JSONDecodeError, TypeError):
                    continue
                if isinstance(value, (int, float)):
                    raw_values.append(float(value))
            if raw_values:
                throughput_samples = raw_values
        if throughput_samples is None:
            throughput_samples = [cell.get("throughput")]
        if (
            not isinstance(throughput_samples, list)
            or not throughput_samples
            or any(not isinstance(value, (int, float)) for value in throughput_samples)
        ):
            raise HarnessError(f"{path}: no numeric throughput evidence")
        reasons = list(verdict.reasons)
        if len(raw_values) != 3:
            reasons.append(f"confirmation_reps={len(raw_values)}")
        candidates.append(
            {
                "label": label,
                "split": split.label,
                "median_throughput": statistics.median(throughput_samples),
                "valid": verdict.valid and not reasons,
                "reasons": reasons,
                "source": str(path),
            }
        )

    valid = [candidate for candidate in candidates if candidate["valid"]]
    if not valid:
        raise HarnessError("no valid Chronicle calibration split")
    best_value = max(candidate["median_throughput"] for candidate in valid)
    winners = [candidate for candidate in valid if candidate["median_throughput"] == best_value]
    if len(winners) != 1:
        labels = ", ".join(candidate["label"] for candidate in winners)
        raise HarnessError(f"calibration tie requires a new measurement: {labels}")
    return {"selected": winners[0], "candidates": candidates}


def parse_split(value: str) -> ChronicleSplit:
    try:
        chronicle_cpu, redis_cpu = (int(part) * 1000 for part in value.split(":"))
    except (TypeError, ValueError) as exc:
        raise HarnessError(f"invalid Chronicle split {value!r}; expected C:R cores") from exc
    split = ChronicleSplit(chronicle_cpu, redis_cpu)
    split.validate()
    if split.label not in {candidate.label for candidate in CALIBRATION_SPLITS.values()}:
        raise HarnessError(f"split {value!r} was not part of the predeclared calibration")
    return split


def parse_topology_args(
    value: str,
    budget: ResourceBudget = ResourceBudget(),
) -> ChronicleTopology | None:
    """Parse an extended topology config, or return None for the legacy syntax."""
    fields = value.split(":")
    if len(fields) == 3:
        return None
    if len(fields) != 8:
        raise HarnessError(
            "invalid Chronicle topology config; expected "
            "fsync:chronicle_replicas:redis_masters:chronicle_cpu_millis:"
            "redis_cpu_millis:chronicle_memory_mib:redis_memory_mib:sse_wait_mode"
        )
    try:
        topology = ChronicleTopology(
            appendfsync=fields[0],
            chronicle_replicas=int(fields[1]),
            redis_masters=int(fields[2]),
            chronicle_cpu_millis=int(fields[3]),
            redis_cpu_millis=int(fields[4]),
            chronicle_memory_mib=int(fields[5]),
            redis_memory_mib=int(fields[6]),
            sse_wait_mode=fields[7],
        )
    except ValueError as exc:
        raise HarnessError("Chronicle topology contains a nonnumeric resource") from exc
    topology.validate(budget)
    return topology


def resolve_campaign(
    split: ChronicleSplit | None,
    *,
    campaign_file: Path = CAMPAIGN_FILE,
    output_dir: Path | None = None,
) -> Path:
    """Resolve the selected split into one immutable suite per workload and system."""
    if split is not None:
        split.validate()
    campaign_bytes = campaign_file.read_bytes()
    campaign = json.loads(campaign_bytes)
    if campaign.get("schema") != "chronicle-ds-bench-campaign-v1":
        raise HarnessError(f"{campaign_file}: unsupported campaign schema")
    budget = ResourceBudget(**campaign["resource_budget"])
    if split is not None and budget != ResourceBudget():
        raise HarnessError("headline campaign must use the fixed 4 vCPU, 16 GiB, one SSD budget")
    if min(budget.cpu_millis, budget.memory_mib, budget.local_ssds) <= 0:
        raise HarnessError("campaign resource budget values must be positive")
    if int(campaign["cluster"]["server_cpus"]) * 1000 != budget.cpu_millis:
        raise HarnessError("campaign server CPU allocation must equal the SUT CPU budget")

    systems = campaign["systems"]
    workloads = campaign["workloads"]
    system_keys = [system["key"] for system in systems]
    workload_keys = [workload["key"] for workload in workloads]
    if len(system_keys) != len(set(system_keys)):
        raise HarnessError("campaign system keys must be unique")
    if len(workload_keys) != len(set(workload_keys)):
        raise HarnessError("campaign workload keys must be unique")

    identity = _sha256_parts(
        [
            ("campaign.json", campaign_bytes),
            ("chronicle_split", (split.label if split is not None else "none").encode()),
        ]
    )
    if output_dir is None:
        suffix = split.label.replace(":", "-") if split is not None else "topologies"
        output_dir = PREPARED_ROOT / "resolved" / f"{identity[:12]}-{suffix}"
    split_label = split.label if split is not None else None
    expected_index = {
        "schema": "chronicle-ds-bench-resolved-v1",
        "campaign_sha256": hashlib.sha256(campaign_bytes).hexdigest(),
        "profile": campaign["profile"],
        "chronicle_split": split_label,
        "suite_count": len(systems) * len(workloads),
    }
    index_path = output_dir / "index.json"
    if output_dir.exists():
        try:
            existing = json.loads(index_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise HarnessError(f"refusing to replace incomplete resolved campaign: {output_dir}") from exc
        for key, value in expected_index.items():
            if existing.get(key) != value:
                raise HarnessError(f"resolved campaign identity mismatch: {output_dir}")
        return output_dir

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".resolve-", dir=output_dir.parent))
    suites_dir = temporary / "suites"
    suites_dir.mkdir()
    suite_records: list[dict[str, Any]] = []
    try:
        for workload in workloads:
            for system in systems:
                suite_name = f"parity-{workload['key']}-{system['key']}"
                cluster_name = f"chdb-{workload['key'][:10]}-{system['key']}"
                if len(cluster_name) > 40:
                    raise HarnessError(f"generated GKE cluster name is too long: {cluster_name}")
                cluster = dict(campaign["cluster"])
                cluster["cluster_name"] = cluster_name
                configs = []
                config_meta = []
                for config in system["configs"]:
                    format_values = {}
                    if split is not None:
                        format_values = {
                            "chronicle_cpu": split.chronicle_cpu_millis // 1000,
                            "redis_cpu": split.redis_cpu_millis // 1000,
                        }
                    try:
                        args = config["args"].format(**format_values)
                    except KeyError as exc:
                        raise HarnessError(
                            f"{campaign_file}: config requires an unresolved field {exc}"
                        ) from exc
                    configs.append({"label": config["label"], "args": args})
                    topology_meta = config.get("topology")
                    if system["key"] == "chronicle":
                        parsed_topology = parse_topology_args(args, budget)
                        if parsed_topology is not None:
                            parsed_topology.validate(budget)
                            declared_shape = {
                                key: topology_meta.get(key)
                                for key in (
                                    "chronicle_replicas",
                                    "redis_masters",
                                    "sse_wait_mode",
                                )
                            } if isinstance(topology_meta, Mapping) else {}
                            actual_shape = {
                                "chronicle_replicas": parsed_topology.chronicle_replicas,
                                "redis_masters": parsed_topology.redis_masters,
                                "sse_wait_mode": parsed_topology.sse_wait_mode,
                            }
                            if declared_shape and declared_shape != actual_shape:
                                raise HarnessError(
                                    f"{campaign_file}: topology metadata differs from {args}"
                                )
                            topology_meta = parsed_topology.to_metadata()
                    config_meta.append(
                        {
                            "label": config["label"],
                            "durability": config["durability"],
                            **(
                                {"topology": topology_meta}
                                if topology_meta is not None
                                else {}
                            ),
                        }
                    )
                suite: dict[str, Any] = {
                    "suite": suite_name,
                    "_doc": (
                        "Resolved Chronicle ds-bench parity suite. Generated from "
                        "benchmarks/ds-bench/campaign.json; do not edit."
                    ),
                    "cluster": cluster,
                    "modes": [system["mode"]],
                    "server_configs": {system["mode"]: configs},
                    "stream_counts": workload["stream_counts"],
                    "benchmark_meta": {
                        "profile": campaign["profile"],
                        "system": system["key"],
                        "workload": workload["key"],
                        "resource_budget": campaign["resource_budget"],
                        "configs": config_meta,
                        "chronicle_split": (
                            split.label
                            if split is not None and system["key"] == "chronicle"
                            else None
                        ),
                    },
                }
                for key in ("workload", "saturation", "reads", "mixed", "pod_ladder"):
                    if key in workload:
                        suite[key] = workload[key]
                suite_path = suites_dir / f"{suite_name}.json"
                suite_path.write_text(json.dumps(suite, indent=2) + "\n")
                if system["key"] == "chronicle":
                    validate_chronicle_suite(suite_path, budget)
                suite_records.append(
                    {
                        "suite": suite_name,
                        "system": system["key"],
                        "workload": workload["key"],
                        "path": f"suites/{suite_path.name}",
                        "sha256": sha256_file(suite_path),
                    }
                )
        index = dict(expected_index)
        index["suites"] = suite_records
        index_path_temp = temporary / "index.json"
        index_path_temp.write_text(json.dumps(index, indent=2) + "\n")
        os.replace(temporary, output_dir)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return output_dir


def resolve_scaling_campaign(
    output_dir: Path | None = None,
    *,
    campaign_file: Path = SCALING_FILE,
) -> Path:
    return resolve_campaign(
        None,
        campaign_file=campaign_file,
        output_dir=output_dir,
    )


def resolve_sse_diagnostic(output_dir: Path | None = None) -> Path:
    return resolve_campaign(
        None,
        campaign_file=SSE_DIAGNOSTIC_FILE,
        output_dir=output_dir,
    )


def create_campaign_manifest(
    resolved_dir: Path,
    images_file: Path,
    calibration_results: Path | None,
    *,
    output: Path,
    campaign_file: Path = CAMPAIGN_FILE,
    execution_kind: str = "campaign",
) -> dict[str, Any]:
    if execution_kind not in {"campaign", "scaling"}:
        raise HarnessError(f"unsupported manifest execution kind: {execution_kind}")
    if execution_kind == "campaign" and calibration_results is None:
        raise HarnessError("parity campaign requires calibration results")
    if execution_kind == "scaling" and calibration_results is not None:
        raise HarnessError("scaling campaign must not use calibration results")
    try:
        index = json.loads((resolved_dir / "index.json").read_text())
        images = json.loads(images_file.read_text())
        campaign = json.loads(campaign_file.read_text())
        sources = json.loads(SOURCES_FILE.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessError(f"cannot build campaign manifest: {exc}") from exc
    if index.get("schema") != "chronicle-ds-bench-resolved-v1":
        raise HarnessError("resolved suite index has an unsupported schema")
    if images.get("schema") != "chronicle-ds-bench-images-v1":
        raise HarnessError("image manifest has an unsupported schema")
    if index.get("campaign_sha256") != sha256_file(campaign_file):
        raise HarnessError("resolved suite index does not match the campaign definition")

    required_images = {"chronicle", "ds-bench", "rust", "node", "ursula", "redis"}
    missing_images = sorted(required_images - set(images.get("images", {})))
    if missing_images:
        raise HarnessError(f"image manifest is missing: {', '.join(missing_images)}")
    for name in required_images:
        entry = images["images"][name]
        digest = str(entry.get("digest", ""))
        reference = str(entry.get("reference", ""))
        if not digest.startswith("sha256:") or "@sha256:" not in reference:
            raise HarnessError(f"{name} image is not pinned by manifest digest")

    current_source = git_worktree_identity(REPO_ROOT)
    reused_sut_manifest = verify_reused_sut_images(images)
    verify_reused_reference_images(images)
    if (
        reused_sut_manifest is None
        and images["images"]["chronicle"].get("source")
        != chronicle_build_identity(REPO_ROOT)
    ):
        raise HarnessError(
            "Chronicle image build context changed after the remote image was built"
        )
    sut_chronicle_source = (
        reused_sut_manifest["chronicle_source"]
        if reused_sut_manifest is not None
        else current_source
    )
    checkout = prepare_checkout()
    if not dsbench_image_source_matches(
        images["images"]["ds-bench"].get("source"),
        dsbench_build_identity(checkout),
    ):
        raise HarnessError(
            "ds-bench client build context changed after the remote image was built"
        )

    calibration: dict[str, Any] | None = None
    if execution_kind == "campaign":
        assert calibration_results is not None
        calibration = select_calibration(calibration_results)
        if calibration["selected"]["split"] != index["chronicle_split"]:
            raise HarnessError(
                "resolved campaign split does not match the archived calibration winner"
            )

    reference_archives: list[dict[str, Any]] = []
    if execution_kind == "scaling":
        for declared in campaign.get("reference_archives", []):
            raw_path = Path(str(declared["path"]))
            archive = raw_path if raw_path.is_absolute() else REPO_ROOT / raw_path
            seal = verify_archive_seal(archive)
            expected_tree = str(declared["evidence_tree_sha256"])
            if seal["tree_sha256"] != expected_tree:
                raise HarnessError(
                    f"{archive}: reference evidence tree differs from scaling definition"
                )
            reference_archives.append(
                {
                    "path": str(archive.resolve()),
                    "campaign_id": json.loads(
                        (archive / "manifest.json").read_text()
                    )["campaign_id"],
                    "evidence_tree_sha256": expected_tree,
                    "file_count": seal["file_count"],
                }
            )
        if not reference_archives:
            raise HarnessError("scaling campaign requires sealed reference archives")

    suite_records = []
    for record in index["suites"]:
        path = resolved_dir / record["path"]
        actual = sha256_file(path)
        if actual != record["sha256"]:
            raise HarnessError(f"resolved suite changed after indexing: {path}")
        suite = load_suite(path)
        suite_records.append(
            {
                **record,
                "absolute_path": str(path.resolve()),
                "cluster": suite["cluster"]["cluster_name"],
                "zone": suite["cluster"]["zone"],
            }
        )

    manifest = {
        "schema": "chronicle-ds-bench-manifest-v1",
        "execution_kind": execution_kind,
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "campaign_id": (
            f"{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-"
            f"{index['campaign_sha256'][:8]}"
        ),
        "profile": index["profile"],
        "chronicle_source": current_source,
        "sut_chronicle_source": sut_chronicle_source,
        "ds_bench": {
            "url": UpstreamPin.load().url,
            "commit": UpstreamPin.load().commit,
            "adapter_sha256": adapter_digest(),
        },
        "comparison_sources": sources,
        "campaign": {
            "definition": str(campaign_file),
            "definition_sha256": sha256_file(campaign_file),
            "resolved_index": str((resolved_dir / "index.json").resolve()),
            "chronicle_split": index["chronicle_split"],
            "suites": suite_records,
        },
        "hardware": {
            **campaign["cluster"],
            "resource_budget": campaign["resource_budget"],
            "disk_mode": "one local-NVMe-backed ephemeral-storage filesystem",
            "server_purchasing_model": "on-demand",
            "client_purchasing_model": "spot",
        },
        "redis": {
            "appendonly": "yes",
            "appendfsync_arms": (
                ["always", "everysec"] if execution_kind == "campaign" else ["always"]
            ),
            "maxmemory": (
                "10gb"
                if execution_kind == "campaign"
                else "five-sixths of each declared Redis pod memory limit"
            ),
            "maxmemory_policy": "noeviction",
            "chronicle_pool_size": 1024 if execution_kind == "campaign" else 4096,
        },
        "images": images,
        "images_file": str(images_file.resolve()),
        "validity_policy": {
            "headline_requires": [
                "status=ok",
                "zero errors",
                "zero lazy stream creation during measurement",
                "aligned fleet measurement windows",
                "no client bottleneck",
                "observed plateau",
            ],
            "ladder_exhausted": "lower_bound_only",
            "durability_classes_are_separate": True,
        },
    }
    if execution_kind == "campaign":
        assert calibration_results is not None and calibration is not None
        manifest["campaign"]["calibration"] = {
            "results_path": str(calibration_results.resolve()),
            "results_sha256": sha256_tree(calibration_results),
            **calibration,
        }
    else:
        candidate_order = []
        scaling_budget = ResourceBudget(**campaign["resource_budget"])
        for system in campaign["systems"]:
            for config in system["configs"]:
                topology = parse_topology_args(config["args"], scaling_budget)
                if topology is None:
                    raise HarnessError(
                        f"scaling candidate is not a topology: {config['label']}"
                    )
                candidate_order.append(
                    {
                        "label": config["label"],
                        "args": config["args"],
                        "topology": topology.to_metadata(),
                    }
                )
        manifest["campaign"]["target"] = campaign["target"]
        manifest["campaign"]["reference_archives"] = reference_archives
        manifest["campaign"]["candidate_order"] = candidate_order
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def create_scaling_manifest(
    resolved_dir: Path,
    images_file: Path,
    *,
    output: Path,
    campaign_file: Path = SCALING_FILE,
) -> dict[str, Any]:
    return create_campaign_manifest(
        resolved_dir,
        images_file,
        None,
        output=output,
        campaign_file=campaign_file,
        execution_kind="scaling",
    )


def campaign_plan(manifest: Mapping[str, Any]) -> list[dict[str, str]]:
    if manifest.get("schema") != "chronicle-ds-bench-manifest-v1":
        raise HarnessError("campaign manifest has an unsupported schema")
    plan: list[dict[str, str]] = []
    seen_suites: set[str] = set()
    seen_clusters: set[str] = set()
    for record in manifest["campaign"]["suites"]:
        suite_path = Path(record["absolute_path"])
        if suite_path.is_file():
            suite = load_suite(suite_path)
            suite_name = str(suite["suite"])
            cluster = str(suite["cluster"]["cluster_name"])
            zone = str(suite["cluster"]["zone"])
            metadata = suite.get("benchmark_meta", {})
            system = str(metadata.get("system", record.get("system", "")))
            workload = str(metadata.get("workload", record.get("workload", "")))
        else:
            suite_name = str(record["suite"])
            cluster = str(record["cluster"])
            zone = str(record["zone"])
            system = str(record.get("system", ""))
            workload = str(record.get("workload", ""))
        if not re.fullmatch(r"chdb-[a-z0-9-]+", cluster):
            raise HarnessError(f"unsafe campaign cluster name: {cluster}")
        if suite_name in seen_suites or cluster in seen_clusters:
            raise HarnessError(f"campaign suite or cluster is not unique: {suite_name}")
        seen_suites.add(suite_name)
        seen_clusters.add(cluster)
        plan.append(
            {
                "suite": suite_name,
                "suite_path": str(suite_path),
                "cluster": cluster,
                "zone": zone,
                "system": system,
                "workload": workload,
                "teardown_filter": f"^{cluster}$",
            }
        )
    return plan


def select_campaign_plan(
    manifest: Mapping[str, Any],
    workloads: Iterable[str] | None = None,
) -> list[dict[str, str]]:
    plan = campaign_plan(manifest)
    if workloads is None:
        return plan
    selected = {str(workload) for workload in workloads}
    if not selected:
        raise HarnessError("at least one workload must be selected")
    known = {item["workload"] for item in plan if item["workload"]}
    unknown = selected - known
    if unknown:
        raise HarnessError(
            "unknown campaign workload: " + ", ".join(sorted(unknown))
        )
    filtered = [item for item in plan if item["workload"] in selected]
    if not filtered:
        raise HarnessError("workload selection produced an empty campaign")
    return filtered


def create_calibration_manifest(images_file: Path) -> dict[str, Any]:
    """Freeze the predeclared calibration as a one-suite safe execution."""
    try:
        images = json.loads(images_file.read_text())
        campaign = json.loads(CAMPAIGN_FILE.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessError(f"cannot create calibration manifest: {exc}") from exc
    if (
        images.get("schema") != "chronicle-ds-bench-images-v1"
        or images.get("target") != "remote"
        or not images.get("project")
    ):
        raise HarnessError("calibration requires a remote image manifest")
    required_images = {"chronicle", "ds-bench", "redis"}
    missing = sorted(required_images - set(images.get("images", {})))
    if missing:
        raise HarnessError(f"calibration image manifest is missing: {', '.join(missing)}")
    for name in required_images:
        reference = str(images["images"][name].get("reference", ""))
        if "@sha256:" not in reference:
            raise HarnessError(f"calibration image {name} is not digest pinned")

    current_source = git_worktree_identity(REPO_ROOT)
    image_source = images["images"]["chronicle"].get("source")
    if image_source != chronicle_build_identity(REPO_ROOT):
        raise HarnessError(
            "Chronicle image build context changed after the remote image was built"
        )

    checkout = prepare_checkout()
    if not dsbench_image_source_matches(
        images["images"]["ds-bench"].get("source"),
        dsbench_build_identity(checkout),
    ):
        raise HarnessError(
            "ds-bench client build context changed after the remote image was built"
        )
    suite_path = checkout / "suites" / "chronicle-calibration.json"
    validate_chronicle_suite(suite_path)
    suite = load_suite(suite_path)
    cluster = suite["cluster"]
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest = {
        "schema": "chronicle-ds-bench-manifest-v1",
        "execution_kind": "calibration",
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "campaign_id": f"{timestamp}-cal-{sha256_file(suite_path)[:8]}",
        "profile": "chronicle-cpu-split-calibration",
        "chronicle_source": current_source,
        "ds_bench": {
            "url": UpstreamPin.load().url,
            "commit": UpstreamPin.load().commit,
            "adapter_sha256": adapter_digest(),
        },
        "campaign": {
            "suites": [
                {
                    "suite": suite["suite"],
                    "system": "chronicle",
                    "workload": "calibration",
                    "path": str(suite_path),
                    "absolute_path": str(suite_path.resolve()),
                    "sha256": sha256_file(suite_path),
                    "cluster": cluster["cluster_name"],
                    "zone": cluster["zone"],
                }
            ]
        },
        "hardware": {
            **campaign["cluster"],
            "resource_budget": campaign["resource_budget"],
            "disk_mode": "one local-NVMe-backed ephemeral-storage filesystem",
            "server_purchasing_model": "on-demand",
            "client_purchasing_model": "spot",
        },
        "images": images,
    }
    return manifest


def _capture_command(
    args: Sequence[str | os.PathLike[str]],
    output: Path,
    *,
    env: Mapping[str, str] | None = None,
) -> int:
    with output.open("w") as stream:
        completed = subprocess.run(
            [os.fspath(arg) for arg in args],
            check=False,
            text=True,
            stdout=stream,
            stderr=subprocess.STDOUT,
            env=dict(env) if env is not None else None,
        )
    return completed.returncode


def _cluster_absence_proof(
    cluster: str,
    *,
    project: str,
    zone: str,
    output: Path,
) -> bool:
    completed = subprocess.run(
        [
            "gcloud",
            "container",
            "clusters",
            "list",
            "--project",
            project,
            "--zone",
            zone,
            f"--filter=name={cluster}",
            "--format=json",
        ],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    proof = {
        "cluster": cluster,
        "project": project,
        "zone": zone,
        "command_returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "absent": False,
    }
    if completed.returncode == 0:
        try:
            proof["absent"] = json.loads(completed.stdout) == []
        except json.JSONDecodeError:
            pass
    output.write_text(json.dumps(proof, indent=2) + "\n")
    return bool(proof["absent"])


def _delete_exact_cluster(cluster: str, *, project: str, zone: str, log: Path) -> bool:
    with log.open("a") as stream:
        for attempt in range(1, 11):
            completed = subprocess.run(
                [
                    "gcloud",
                    "container",
                    "clusters",
                    "delete",
                    cluster,
                    "--project",
                    project,
                    "--zone",
                    zone,
                    "--quiet",
                ],
                check=False,
                text=True,
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
            if completed.returncode == 0:
                return True
            stream.write(f"exact delete retry {attempt} failed\n")
            if attempt < 10:
                import time

                time.sleep(20)
    return False


def _final_cluster_proof(
    plan: Sequence[Mapping[str, str]],
    *,
    project: str,
    output: Path,
) -> bool:
    completed = subprocess.run(
        [
            "gcloud",
            "container",
            "clusters",
            "list",
            "--project",
            project,
            "--format=json",
        ],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    expected = {item["cluster"] for item in plan}
    present: list[dict[str, Any]] = []
    if completed.returncode == 0:
        try:
            all_clusters = json.loads(completed.stdout)
            present = [
                cluster for cluster in all_clusters if cluster.get("name") in expected
            ]
        except json.JSONDecodeError:
            pass
    proof = {
        "project": project,
        "expected_campaign_clusters": sorted(expected),
        "present_campaign_clusters": present,
        "command_returncode": completed.returncode,
        "stderr": completed.stderr,
        "all_absent": completed.returncode == 0 and not present,
    }
    output.write_text(json.dumps(proof, indent=2) + "\n")
    return bool(proof["all_absent"])


def _join_completed_watchdog(
    watchdog: subprocess.Popen,
    *,
    timeout: int = WATCHDOG_JOIN_SECONDS,
) -> bool:
    """Wait until a completed suite's watchdog has flushed its terminal log line."""
    try:
        return watchdog.wait(timeout=timeout) == 0
    except subprocess.TimeoutExpired:
        watchdog.terminate()
        try:
            watchdog.wait(timeout=10)
        except subprocess.TimeoutExpired:
            watchdog.kill()
            watchdog.wait()
        return False


def execute_campaign(
    manifest_file: Path,
    *,
    output_root: Path,
    deadline_secs: int = 4 * 60 * 60,
    keep_failed_cluster: bool = False,
    workloads: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Run suites sequentially with an exact-name detached teardown watchdog."""
    output_root = output_root.resolve()
    manifest = json.loads(manifest_file.read_text())
    execution_kind = manifest.get("execution_kind", "campaign")
    if execution_kind not in {"campaign", "calibration", "scaling"}:
        raise HarnessError(f"unsupported execution kind: {execution_kind}")
    if execution_kind == "calibration" and workloads is not None:
        raise HarnessError("workload selection is not supported for calibration")
    plan = select_campaign_plan(manifest, workloads)
    if deadline_secs < 600:
        raise HarnessError("campaign deadline must be at least 600 seconds per suite")
    project = manifest["images"]["project"]
    if not project:
        raise HarnessError("publication campaigns require remote images")
    region = manifest["hardware"]["region"]
    campaign_file = Path(str(manifest["campaign"]["definition"]))
    if not campaign_file.is_absolute():
        campaign_file = REPO_ROOT / campaign_file
    preflight_report = preflight(
        "remote",
        project=project,
        region=region,
        phase="campaign",
        campaign_file=campaign_file,
    )
    if not preflight_report["ok"]:
        raise HarnessError(
            "remote preflight failed before campaign: "
            + ", ".join(preflight_report["required_failures"])
        )
    current_source = git_worktree_identity(REPO_ROOT)
    if current_source != manifest["chronicle_source"]:
        raise HarnessError("Chronicle worktree changed after the campaign manifest was frozen")

    campaign_id = manifest["campaign_id"]
    archive = output_root / campaign_id
    if archive.exists():
        raise HarnessError(f"refusing to replace an existing campaign archive: {archive}")
    archive.mkdir(parents=True)
    (archive / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (archive / "preflight.json").write_text(json.dumps(preflight_report, indent=2) + "\n")
    (archive / "selection.json").write_text(
        json.dumps(
            {
                "schema": "chronicle-ds-bench-selection-v1",
                "selected_workloads": sorted(
                    {item["workload"] for item in plan if item["workload"]}
                ),
                "selected_suites": [
                    {
                        "suite": item["suite"],
                        "system": item["system"],
                        "workload": item["workload"],
                        "cluster": item["cluster"],
                    }
                    for item in plan
                ],
            },
            indent=2,
        )
        + "\n"
    )
    images_file = archive / "images.json"
    images_file.write_text(json.dumps(manifest["images"], indent=2) + "\n")
    if execution_kind == "campaign":
        calibration_source = Path(manifest["campaign"]["calibration"]["results_path"])
        if (
            sha256_tree(calibration_source)
            != manifest["campaign"]["calibration"]["results_sha256"]
        ):
            raise HarnessError("calibration results changed after the campaign manifest was frozen")
        shutil.copytree(calibration_source, archive / "calibration")

    prepared = prepare_checkout()
    runtime = PREPARED_ROOT / "runs" / campaign_id
    runtime.parent.mkdir(parents=True, exist_ok=True)
    if runtime.exists():
        raise HarnessError(f"campaign runtime already exists: {runtime}")
    shutil.copytree(
        prepared,
        runtime,
        ignore=shutil.ignore_patterns(".git", ".bench-state", "results", "results-*"),
    )
    results: list[dict[str, Any]] = []
    base_env = _bench_env("remote", images_file)
    base_env["PROJECT"] = project
    base_env["CLOUDSDK_CORE_PROJECT"] = project
    base_env["PULL_POLICY"] = "IfNotPresent"
    # The campaign wrapper must archive server and MinIO logs before teardown.
    # It owns deletion and has an exact-name watchdog, so suppress upstream's
    # clean-run auto-teardown for this execution path.
    base_env["BENCH_KEEP_CLUSTER"] = "1"

    for item in plan:
        suite_started_at = dt.datetime.now(dt.timezone.utc)
        suite_started_monotonic = time.monotonic()
        suite_path = Path(item["suite_path"])
        suite_archive = archive / "runs" / item["suite"]
        suite_archive.mkdir(parents=True)
        copied_suite = suite_archive / "suite.json"
        shutil.copy2(suite_path, copied_suite)
        suite = load_suite(copied_suite)
        modes = suite.get("modes", [])
        if len(modes) != 1 or modes[0] not in SERVER_APP_BY_MODE:
            raise HarnessError(
                f"{copied_suite}: campaign suites must contain one supported mode"
            )
        server_app = SERVER_APP_BY_MODE[str(modes[0])]
        done_marker = suite_archive / "watchdog.done"
        watchdog_log = suite_archive / "watchdog.log"
        run_log = suite_archive / "run.log"
        state_path = runtime / ".bench-state" / f"{item['suite']}.json"
        raw_source = runtime / "results" / item["suite"]
        run_command = [runtime / "scripts" / "bench", copied_suite, "run"]
        rc = 1
        timed_out = False
        watchdog = None

        # Never adopt an existing same-name cluster.
        if not _cluster_absence_proof(
            item["cluster"],
            project=project,
            zone=item["zone"],
            output=suite_archive / "pre-run-cluster-check.json",
        ):
            raise HarnessError(f"refusing to adopt existing cluster {item['cluster']}")

        if not keep_failed_cluster:
            watchdog_env = dict(base_env)
            watchdog_env.update(
                {
                    "DEADLINE_SECS": str(deadline_secs),
                    "DONE_MARKER": str(done_marker),
                    "CLUSTER_FILTER": item["teardown_filter"],
                    "POLL_SECS": "30",
                }
            )
            watchdog_stream = watchdog_log.open("w")
            watchdog = subprocess.Popen(
                ["bash", runtime / "scripts" / "teardown-watchdog.sh"],
                cwd=runtime,
                env=watchdog_env,
                stdout=watchdog_stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            watchdog_stream.close()

        try:
            with run_log.open("w") as stream:
                try:
                    completed = subprocess.run(
                        run_command,
                        cwd=runtime,
                        env=base_env,
                        check=False,
                        text=True,
                        stdout=stream,
                        stderr=subprocess.STDOUT,
                        timeout=deadline_secs,
                    )
                    rc = completed.returncode
                except subprocess.TimeoutExpired:
                    timed_out = True
                    rc = 124
                    stream.write("\nchronicle adapter: suite deadline exceeded\n")
            if raw_source.is_dir():
                shutil.copytree(raw_source, suite_archive / "raw")
            if state_path.is_file():
                shutil.copy2(state_path, suite_archive / "cluster-state.json")
            kubeconfig = runtime / ".bench-state" / f"kc-{item['cluster']}"
            diagnostic_env = dict(base_env)
            diagnostic_env["KUBECONFIG"] = str(kubeconfig)
            _capture_command(
                [
                    "kubectl",
                    "--context",
                    f"gke_{project}_{item['zone']}_{item['cluster']}",
                    "-n",
                    "ds-bench",
                    "get",
                    "all,events",
                    "-o",
                    "wide",
                ],
                suite_archive / "cluster-diagnostics.txt",
                env=diagnostic_env,
            )
            for app, output_name in (
                (server_app, "server.log"),
                ("minio", "minio.log"),
            ):
                _capture_command(
                    [
                        "kubectl",
                        "--context",
                        f"gke_{project}_{item['zone']}_{item['cluster']}",
                        "-n",
                        "ds-bench",
                        "logs",
                        "-l",
                        f"app={app}",
                        "--all-containers=true",
                        "--prefix=true",
                        "--timestamps=true",
                        "--tail=-1",
                    ],
                    suite_archive / output_name,
                    env=diagnostic_env,
                )
        finally:
            should_keep = keep_failed_cluster and rc != 0
            if not should_keep:
                teardown_env = dict(base_env)
                teardown_env["BENCH_DRYRUN"] = ""
                with (suite_archive / "teardown.log").open("w") as stream:
                    subprocess.run(
                        [runtime / "scripts" / "bench", copied_suite, "teardown"],
                        cwd=runtime,
                        env=teardown_env,
                        check=False,
                        text=True,
                        stdout=stream,
                        stderr=subprocess.STDOUT,
                    )
                absent = _cluster_absence_proof(
                    item["cluster"],
                    project=project,
                    zone=item["zone"],
                    output=suite_archive / "teardown-proof.json",
                )
                if absent:
                    done_marker.touch()
                else:
                    _delete_exact_cluster(
                        item["cluster"],
                        project=project,
                        zone=item["zone"],
                        log=suite_archive / "teardown.log",
                    )
                    absent = _cluster_absence_proof(
                        item["cluster"],
                        project=project,
                        zone=item["zone"],
                        output=suite_archive / "teardown-proof-after-retry.json",
                    )
                    if absent:
                        done_marker.touch()
                    else:
                        rc = 125
            else:
                absent = False
            if watchdog is not None and done_marker.is_file():
                if not _join_completed_watchdog(watchdog) and rc == 0:
                    rc = 126
        results.append(
            {
                **item,
                "started_at": suite_started_at.isoformat(),
                "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "elapsed_seconds": time.monotonic() - suite_started_monotonic,
                "returncode": rc,
                "timed_out": timed_out,
                "cluster_absent": absent,
                "kept_for_debugging": keep_failed_cluster and rc != 0,
                "command": [os.fspath(value) for value in run_command],
                "environment": {
                    "DS_TARGET": base_env["DS_TARGET"],
                    "PROJECT": base_env["PROJECT"],
                    "PULL_POLICY": base_env["PULL_POLICY"],
                    "BENCH_KEEP_CLUSTER": base_env["BENCH_KEEP_CLUSTER"],
                },
            }
        )
        if rc != 0:
            break

    final_clusters_absent = _final_cluster_proof(
        plan,
        project=project,
        output=archive / "final-cluster-proof.json",
    )
    summary = {
        "schema": "chronicle-ds-bench-execution-v1",
        "execution_kind": execution_kind,
        "campaign_id": campaign_id,
        "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "suite_deadline_seconds": deadline_secs,
        "complete": (
            len(results) == len(plan)
            and all(item["returncode"] == 0 for item in results)
            and final_clusters_absent
        ),
        "planned_suites": len(plan),
        "executed_suites": len(results),
        "selected_workloads": sorted({item["workload"] for item in plan if item["workload"]}),
        "selected_suites": [item["suite"] for item in plan],
        "all_campaign_clusters_absent": final_clusters_absent,
        "runs": results,
    }
    (archive / "execution.json").write_text(json.dumps(summary, indent=2) + "\n")
    if execution_kind in {"campaign", "scaling"} and summary["complete"]:
        seal_archive(archive)
    return summary


def execute_calibration(
    images_file: Path,
    *,
    output_root: Path,
    deadline_secs: int = 4 * 60 * 60,
    keep_failed_cluster: bool = False,
) -> dict[str, Any]:
    """Run calibration through the same watchdog and archive path as a campaign."""
    manifest = create_calibration_manifest(images_file)
    output_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="dsbench-calibration-") as raw:
        manifest_file = Path(raw) / "manifest.json"
        manifest_file.write_text(json.dumps(manifest, indent=2) + "\n")
        summary = execute_campaign(
            manifest_file,
            output_root=output_root,
            deadline_secs=deadline_secs,
            keep_failed_cluster=keep_failed_cluster,
        )
    archive = output_root / summary["campaign_id"]
    results_path = archive / "runs" / "chronicle-calibration" / "raw"
    summary["archive"] = str(archive.resolve())
    summary["results_path"] = str(results_path.resolve())
    (archive / "execution.json").write_text(json.dumps(summary, indent=2) + "\n")
    if summary["complete"]:
        seal_archive(archive)
    return summary


def _resolve_suite(checkout: Path, suite_arg: str) -> Path:
    requested = Path(suite_arg)
    candidates = [requested]
    if not requested.is_absolute():
        candidates.extend(
            [
                checkout / requested,
                checkout / "suites" / requested,
                checkout / "suites" / f"{requested}.json",
            ]
        )
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise HarnessError(f"suite not found: {suite_arg}")


def run_upstream_tests(checkout: Path) -> None:
    shell_files = [
        checkout / "scripts" / "lib-bench.sh",
        checkout / "scripts" / "target-env.sh",
        checkout / "deploy" / "metrics" / "poller.sh",
    ]
    _run(["bash", "-n", *shell_files], cwd=checkout)
    for test in sorted((checkout / "scripts").glob("*_test.py")):
        _run([sys.executable, test], cwd=checkout)
    for test in sorted((checkout / "scripts").glob("*_test.sh")):
        _run(["bash", test], cwd=checkout)
    _run(
        [
            "cargo",
            "test",
            "--locked",
            "--manifest-path",
            checkout / "ds-bench" / "Cargo.toml",
        ],
        cwd=checkout,
    )
    validate_chronicle_suite(checkout / "suites" / "chronicle-smoke.json")


def _bench_env(
    target: str,
    images_file: Path | None = None,
    *,
    require_images: bool = True,
) -> dict[str, str]:
    env = os.environ.copy()
    env["DS_TARGET"] = target
    if images_file is not None:
        images = json.loads(images_file.read_text())
        if images.get("schema") != "chronicle-ds-bench-images-v1":
            raise HarnessError(f"{images_file}: unsupported image manifest")
        entries = images["images"]
        mapping = {
            "IMG_CHRONICLE": "chronicle",
            "IMG_REDIS": "redis",
            "IMG_SERVER": "rust",
            "IMG_NODE": "node",
            "IMG_URSULA": "ursula",
            "IMG_DSBENCH": "ds-bench",
            "IMG_METRICS": "ds-bench",
        }
        for variable, name in mapping.items():
            env[variable] = entries[name]["reference"]
    elif target == "remote" and require_images:
        raise HarnessError("remote runs require --images with digest-pinned references")
    else:
        env.setdefault("IMG_CHRONICLE", "chronicle:ds-bench")
        env.setdefault("IMG_REDIS", "redis:8-alpine")
    if target == "remote":
        env.setdefault("PROJECT", "adityavkk-prototyping")
        env.setdefault("AR_LOCATION", "europe-west1")
    return env


def invoke_bench(
    checkout: Path,
    suite: Path,
    command: str,
    target: str,
    images_file: Path | None = None,
) -> None:
    env = _bench_env(target, images_file, require_images=command == "run")
    _run([checkout / "scripts" / "bench", suite, command], cwd=checkout, env=env)


def _print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("prepare", help="prepare and print the pinned upstream checkout")
    subparsers.add_parser(
        "prepare-sources", help="prepare the pinned Rust and Node comparison sources"
    )
    subparsers.add_parser("test", help="run adapter and pinned upstream tests")

    preflight_command = subparsers.add_parser(
        "preflight", help="check all local or remote prerequisites before any spend"
    )
    preflight_command.add_argument("--target", choices=("local", "remote"), required=True)
    preflight_command.add_argument("--project", default="adityavkk-prototyping")
    preflight_command.add_argument("--region", default="europe-west4")
    preflight_command.add_argument("--zone", default="europe-west4-b")
    preflight_command.add_argument("--ar-location", default="europe-west1")
    preflight_command.add_argument("--ar-repo", default="ds-bench")
    preflight_command.add_argument(
        "--phase",
        choices=("build", "campaign"),
        default="campaign",
        help="campaign additionally proves the frozen VM topology fits current quota",
    )
    preflight_command.add_argument(
        "--campaign-file",
        type=Path,
        default=CAMPAIGN_FILE,
        help="campaign definition whose frozen VM topology should be checked",
    )
    preflight_command.add_argument("--output", type=Path)

    build = subparsers.add_parser(
        "build", help="build all benchmark images after a successful preflight"
    )
    build.add_argument("--target", choices=("local", "remote"), required=True)
    build.add_argument("--project", default="adityavkk-prototyping")
    build.add_argument("--ar-location", default="europe-west1")
    build.add_argument("--ar-repo", default="ds-bench")
    build.add_argument("--output", type=Path, required=True)
    build.add_argument(
        "--reuse",
        type=Path,
        help="reuse digest-pinned images whose recorded source identity is unchanged",
    )
    build.add_argument(
        "--reuse-suts-from-archive",
        type=Path,
        help=(
            "reuse exact server image digests from a sealed remote campaign and "
            "build only the changed client image"
        ),
    )
    build.add_argument(
        "--reuse-reference-suts-from-archive",
        type=Path,
        help=(
            "reuse exact Rust, Node, Ursula, and Redis image digests from a sealed "
            "remote campaign while rebuilding Chronicle and the client"
        ),
    )

    run = subparsers.add_parser("run", help="run one ds-bench suite")
    run.add_argument("suite", help="suite name or JSON path")
    run.add_argument("--target", choices=("local", "remote"), default="local")
    run.add_argument("--images", type=Path)
    teardown = subparsers.add_parser("teardown", help="tear down one ds-bench suite")
    teardown.add_argument("suite", help="suite name or JSON path")
    teardown.add_argument("--target", choices=("local", "remote"), default="local")

    calibrate = subparsers.add_parser(
        "calibrate",
        help="run the remote Chronicle CPU-split calibration with safe teardown",
    )
    calibrate.add_argument("--images", type=Path, required=True)
    calibrate.add_argument("--output-root", type=Path, required=True)
    calibrate.add_argument("--deadline-secs", type=int, default=4 * 60 * 60)
    calibrate.add_argument("--keep-failed-cluster", action="store_true")

    calibration = subparsers.add_parser(
        "select-calibration", help="select a Chronicle CPU split from completed cells"
    )
    calibration.add_argument("results_dir", type=Path)
    calibration.add_argument("--stream-count", type=int, default=10_000)

    resolve = subparsers.add_parser(
        "resolve", help="resolve the immutable parity suite matrix after calibration"
    )
    source = resolve.add_mutually_exclusive_group(required=True)
    source.add_argument("--split", help="selected Chronicle to Redis CPU split, such as 2:2")
    source.add_argument(
        "--calibration",
        type=Path,
        help="calibration results directory from which to select the split",
    )
    resolve.add_argument("--output-dir", type=Path)

    scaling_resolve = subparsers.add_parser(
        "resolve-scaling",
        help="resolve an immutable Chronicle scaling campaign",
    )
    scaling_resolve.add_argument("--output-dir", type=Path)
    scaling_resolve.add_argument(
        "--campaign-file",
        type=Path,
        default=SCALING_FILE,
    )

    sse_resolve = subparsers.add_parser(
        "resolve-sse-diagnostic",
        help="resolve the legacy versus persistent SSE wait diagnostic",
    )
    sse_resolve.add_argument("--output-dir", type=Path)

    manifest = subparsers.add_parser(
        "manifest", help="freeze source, image, hardware, and suite provenance"
    )
    manifest.add_argument("resolved_dir", type=Path)
    manifest.add_argument("--images", type=Path, required=True)
    manifest.add_argument("--calibration-results", type=Path, required=True)
    manifest.add_argument("--output", type=Path, required=True)

    scaling_manifest = subparsers.add_parser(
        "manifest-scaling",
        help="freeze a Chronicle scaling campaign",
    )
    scaling_manifest.add_argument("resolved_dir", type=Path)
    scaling_manifest.add_argument("--images", type=Path, required=True)
    scaling_manifest.add_argument("--output", type=Path, required=True)
    scaling_manifest.add_argument(
        "--campaign-file",
        type=Path,
        default=SCALING_FILE,
    )

    sse_manifest = subparsers.add_parser(
        "manifest-sse-diagnostic",
        help="freeze the separate Chronicle SSE wait diagnostic",
    )
    sse_manifest.add_argument("resolved_dir", type=Path)
    sse_manifest.add_argument("--images", type=Path, required=True)
    sse_manifest.add_argument("--output", type=Path, required=True)

    campaign = subparsers.add_parser(
        "campaign", help="run the frozen remote suite matrix sequentially"
    )
    campaign.add_argument("--manifest", type=Path, required=True)
    campaign.add_argument("--output-root", type=Path, required=True)
    campaign.add_argument("--deadline-secs", type=int, default=4 * 60 * 60)
    campaign.add_argument("--keep-failed-cluster", action="store_true")
    campaign.add_argument(
        "--workload",
        action="append",
        help="run only this workload from the frozen matrix; repeat to select more",
    )

    validate = subparsers.add_parser(
        "validate", help="validate archived cells against raw evidence"
    )
    validate.add_argument("archive", type=Path)
    seal = subparsers.add_parser(
        "seal", help="write an immutable SHA-256 inventory of raw archive evidence"
    )
    seal.add_argument("archive", type=Path)
    verify_seal = subparsers.add_parser(
        "verify-seal", help="verify every raw archive file against its SHA-256 inventory"
    )
    verify_seal.add_argument("archive", type=Path)
    report = subparsers.add_parser(
        "report", help="render the comparison report from a validated archive"
    )
    report.add_argument("archive", type=Path)
    report.add_argument(
        "--supplement",
        action="append",
        type=Path,
        default=[],
        help="replace matching system/workload slices from a corrected rerun archive",
    )
    scaling_report = subparsers.add_parser(
        "report-scaling",
        help="evaluate and render a Chronicle topology scaling archive",
    )
    scaling_report.add_argument("archive", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "prepare":
        checkout = prepare_checkout()
        _print_json(
            {
                "checkout": str(checkout),
                "upstream_commit": UpstreamPin.load().commit,
                "adapter_sha256": adapter_digest(),
            }
        )
        return 0
    if args.command == "prepare-sources":
        _print_json(prepare_sources())
        return 0
    if args.command == "preflight":
        report = preflight(
            args.target,
            project=args.project,
            region=args.region,
            zone=args.zone,
            ar_location=args.ar_location,
            ar_repo=args.ar_repo,
            phase=args.phase,
            campaign_file=args.campaign_file,
        )
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
        _print_json(report)
        return 0 if report["ok"] else 2
    if args.command == "build":
        _print_json(
            build_images(
                args.target,
                output=args.output,
                project=args.project,
                ar_location=args.ar_location,
                ar_repo=args.ar_repo,
                reuse=args.reuse,
                reuse_suts_from_archive=args.reuse_suts_from_archive,
                reuse_reference_suts_from_archive=(
                    args.reuse_reference_suts_from_archive
                ),
            )
        )
        return 0
    if args.command == "test":
        checkout = prepare_checkout()
        _run(
            [
                sys.executable,
                "-m",
                "unittest",
                "discover",
                "-s",
                ADAPTER_DIR / "tests",
                "-p",
                "test_*.py",
            ],
            cwd=REPO_ROOT,
        )
        run_upstream_tests(checkout)
        _print_json({"status": "ok", "checkout": str(checkout)})
        return 0
    if args.command in {"run", "teardown"}:
        checkout = prepare_checkout()
        suite = _resolve_suite(checkout, args.suite)
        validate_chronicle_suite(suite)
        invoke_bench(
            checkout,
            suite,
            args.command,
            args.target,
            getattr(args, "images", None),
        )
        return 0
    if args.command == "calibrate":
        summary = execute_calibration(
            args.images,
            output_root=args.output_root,
            deadline_secs=args.deadline_secs,
            keep_failed_cluster=args.keep_failed_cluster,
        )
        _print_json(summary)
        return 0 if summary["complete"] else 1
    if args.command == "select-calibration":
        _print_json(select_calibration(args.results_dir, args.stream_count))
        return 0
    if args.command == "resolve":
        if args.calibration:
            selected = select_calibration(args.calibration)["selected"]["split"]
            split = parse_split(selected)
        else:
            split = parse_split(args.split)
        output = resolve_campaign(split, output_dir=args.output_dir)
        _print_json(json.loads((output / "index.json").read_text()))
        return 0
    if args.command == "resolve-scaling":
        output = resolve_scaling_campaign(
            args.output_dir,
            campaign_file=args.campaign_file,
        )
        _print_json(json.loads((output / "index.json").read_text()))
        return 0
    if args.command == "resolve-sse-diagnostic":
        output = resolve_sse_diagnostic(args.output_dir)
        _print_json(json.loads((output / "index.json").read_text()))
        return 0
    if args.command == "manifest":
        _print_json(
            create_campaign_manifest(
                args.resolved_dir,
                args.images,
                args.calibration_results,
                output=args.output,
            )
        )
        return 0
    if args.command == "manifest-scaling":
        _print_json(
            create_scaling_manifest(
                args.resolved_dir,
                args.images,
                output=args.output,
                campaign_file=args.campaign_file,
            )
        )
        return 0
    if args.command == "manifest-sse-diagnostic":
        _print_json(
            create_scaling_manifest(
                args.resolved_dir,
                args.images,
                output=args.output,
                campaign_file=SSE_DIAGNOSTIC_FILE,
            )
        )
        return 0
    if args.command == "campaign":
        summary = execute_campaign(
            args.manifest,
            output_root=args.output_root,
            deadline_secs=args.deadline_secs,
            keep_failed_cluster=args.keep_failed_cluster,
            workloads=args.workload,
        )
        _print_json(summary)
        return 0 if summary["complete"] else 1
    if args.command == "validate":
        validation = validate_archive(args.archive)
        _print_json(validation)
        return 0 if validation["complete"] else 1
    if args.command == "seal":
        seal = seal_archive(args.archive)
        _print_json(
            {
                "schema": seal["schema"],
                "file_count": seal["file_count"],
                "tree_sha256": seal["tree_sha256"],
                "seal": str(args.archive / "evidence-checksums.json"),
            }
        )
        return 0
    if args.command == "verify-seal":
        _print_json(verify_archive_seal(args.archive))
        return 0
    if args.command == "report":
        path = render_report(args.archive, args.supplement)
        _print_json(
            {
                "report": str(path),
                "validation": str(args.archive / "combined-validation.json"),
            }
        )
        return 0
    if args.command == "report-scaling":
        path = render_scaling_report(args.archive)
        _print_json(
            {
                "report": str(path),
                "evaluation": str(args.archive / "scaling-evaluation.json"),
            }
        )
        return 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except HarnessError as exc:
        print(f"dsbench: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
