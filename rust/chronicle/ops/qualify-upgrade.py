#!/usr/bin/env python3
"""Restore stopped baseline PVC copies into a separate, disposable k3d network.

Never initializes an identity or writes to the baseline cluster. Refuses an
existing destination cluster; retain a failed destination for diagnosis.
"""
import argparse
import hashlib
import json
import pathlib
import shutil
import subprocess

CLUSTER = "chronicle-upgrade"
BASELINE = "chronicle-rust"


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def kube(cluster, *args, **kwargs):
    return run("sudo", "docker", "exec", "-i", f"k3d-{cluster}-server-0",
               "kubectl", *args, **kwargs)


def read_baseline(kind, name):
    return json.loads(kube(BASELINE, "-n", "chronicle", "get", kind, name,
                           "-o", "json", capture_output=True).stdout)


def apply(value):
    kube(CLUSTER, "apply", "-f", "-", input=json.dumps(value).encode())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=pathlib.Path)
    parser.add_argument("image", help="already built local candidate image")
    args = parser.parse_args()
    capture = args.capture.resolve(strict=True)
    verified = set()
    for line in (capture / "SHA256SUMS").read_text().splitlines():
        digest, original = line.split(maxsplit=1)
        name = pathlib.Path(original).name
        with (capture / name).open("rb") as source:
            if hashlib.file_digest(source, "sha256").hexdigest() != digest:
                parser.error(f"capture checksum mismatch: {name}")
        verified.add(name)
        print(f"Verified {name}: {digest}", flush=True)
    for ordinal in range(5):
        if f"data-chronicle-{ordinal}.tar.gz" not in verified:
            parser.error("requires all five stopped-process PVC captures")
    disk = shutil.disk_usage(capture)
    if disk.used / disk.total >= 0.8:
        parser.error("free generated build artifacts first; image GC threshold is 85%")
    version = run("sudo", "k3d", "version", capture_output=True, text=True).stdout
    if "k3d version v5.8.3" not in version:
        parser.error("requires k3d v5.8.3")
    clusters = json.loads(run("sudo", "k3d", "cluster", "list", "-o", "json",
                             capture_output=True).stdout)
    if any(cluster["name"] == CLUSTER for cluster in clusters):
        parser.error("destination exists; inspect it instead of overwriting test evidence")
    run("sudo", "docker", "image", "inspect", args.image, "--format", "{{.Id}}")
    template = read_baseline("statefulset", "chronicle")
    services = [read_baseline("service", name) for name in
                ["chronicle", "chronicle-http", "chronicle-conformance"]]
    run("sudo", "k3d", "cluster", "create", CLUSTER, "--servers", "1", "--agents", "5",
        "--image", "rancher/k3s:v1.32.5-k3s1", "--wait", "--timeout", "180s",
        "--kubeconfig-update-default=false")
    run("sudo", "k3d", "image", "import", "-c", CLUSTER, args.image)
    apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "chronicle"}})
    for service in services:
        spec = service["spec"]
        for field in ["clusterIP", "clusterIPs", "ipFamilies", "ipFamilyPolicy"]:
            if spec.get(field) != "None":
                spec.pop(field, None)
        apply({"apiVersion": "v1", "kind": "Service", "metadata": {
            "name": service["metadata"]["name"], "namespace": "chronicle"}, "spec": spec})
    spec = template["spec"]
    spec["replicas"] = 0
    spec["template"]["spec"]["containers"][0]["image"] = args.image
    apply({"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": {
        "name": "chronicle", "namespace": "chronicle"}, "spec": spec})
    for ordinal in range(5):
        claim = f"data-chronicle-{ordinal}"
        name = f"restore-{ordinal}"
        apply({"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {
            "name": claim, "namespace": "chronicle"}, "spec": {
                "accessModes": ["ReadWriteOnce"], "storageClassName": "local-path",
                "resources": {"requests": {"storage": "1Gi"}}}})
        apply({"apiVersion": "v1", "kind": "Pod", "metadata": {
            "name": name, "namespace": "chronicle"}, "spec": {
                "nodeSelector": {"kubernetes.io/hostname": f"k3d-{CLUSTER}-agent-{ordinal}"},
                "containers": [{"name": "restore", "image": args.image,
                    "imagePullPolicy": "Never", "command": ["sleep", "600"],
                    "volumeMounts": [{"name": "data", "mountPath": "/data"}]}],
                "securityContext": {"runAsUser": 0}, "restartPolicy": "Never",
                "terminationGracePeriodSeconds": 1,
                "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": claim}}]}})
        kube(CLUSTER, "-n", "chronicle", "wait", "--for=condition=Ready", f"pod/{name}", "--timeout=120s")
        kube(CLUSTER, "-n", "chronicle", "exec", name, "--", "sh", "-ec",
             'test -z "$(ls -A /data)"')
        with (capture / f"{claim}.tar.gz").open("rb") as archive:
            kube(CLUSTER, "-n", "chronicle", "exec", "-i", name, "--",
                 "tar", "-xzf", "-", "-C", "/data", stdin=archive)
        kube(CLUSTER, "-n", "chronicle", "delete", "pod", name, "--wait=true")
    kube(CLUSTER, "-n", "chronicle", "scale", "statefulset/chronicle", "--replicas=5")
    kube(CLUSTER, "-n", "chronicle", "rollout", "status", "statefulset/chronicle", "--timeout=180s")
    print("Candidate recovered in isolated chronicle-upgrade; readiness alone is not qualification.")


if __name__ == "__main__":
    main()
