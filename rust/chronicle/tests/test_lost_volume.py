import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import lost_volume
from lost_volume import location


class LocationTest(unittest.TestCase):
    def test_only_exact_disposable_pvc_and_named_volume_are_accepted(self):
        pod = {"spec": {"nodeName": "k3d-chronicle-rust-agent-3-0"}}
        name = "pvc-12345678-1234-1234-1234-123456789abc"
        path = f"/var/lib/rancher/k3s/storage/{name}_chronicle_data-chronicle-2"
        volume = {"metadata": {"name": name}, "spec": {
            "claimRef": {"name": "data-chronicle-2", "namespace": "chronicle"},
            "local": {"path": path}}}
        container = {"Config": {"Labels": {"k3d.cluster": "chronicle-rust"}},
                     "Mounts": [{"Destination": "/var/lib/rancher/k3s", "Type": "volume", "Name": "a" * 64}]}
        agent, mount, source, destination = location(pod, volume, container, "test-1")
        self.assertEqual(agent, "k3d-chronicle-rust-agent-3-0")
        self.assertEqual(mount, "a" * 64)
        self.assertEqual(source, f"/disk/storage/{name}_chronicle_data-chronicle-2")
        self.assertEqual(destination, source + ".withheld-test-1")
        for target, keys, value in (
            (0, ["spec", "nodeName"], "production-agent"),
            (1, ["spec", "claimRef", "name"], "data-chronicle-1"),
            (1, ["spec", "claimRef", "namespace"], "other"),
            (1, ["spec", "local", "path"], path + "/../other"),
            (2, ["Config", "Labels", "k3d.cluster"], "other"),
            (2, ["Mounts", 0, "Type"], "bind"),
            (2, ["Mounts", 0, "Name"], "another-volume"),
        ):
            with self.subTest(keys=keys, value=value):
                inputs = copy.deepcopy([pod, volume, container])
                obj = inputs[target]
                for key in keys[:-1]:
                    obj = obj[key]
                obj[keys[-1]] = value
                with self.assertRaises(ValueError):
                    location(*inputs, "test-1")
        for run in ("", "../other", "x" * 65):
            with self.assertRaises(ValueError):
                location(pod, volume, container, run)

    def test_restore_requires_quarantine_before_any_docker_mutation(self):
        pod = {"metadata": {"name": "chronicle-2"}, "spec": {
            "nodeName": "k3d-chronicle-rust-agent-3-0", "volumes": [
                {"name": "data", "persistentVolumeClaim": {"claimName": "data-chronicle-2"}}]},
            "status": {"containerStatuses": [{"name": "chronicle", "ready": False}]}}
        for nodes in ({}, {"3": {"draining": False}}):
            state = {"nodes": nodes, "placements": {str(g): {"complete": True, "voters": [1, 2, 4]} for g in range(5)}}
            reads = ["k3d-chronicle-rust", json.dumps(pod), "volume", "{}", "[{}]", json.dumps({"items": [pod]})]
            with self.subTest(nodes=nodes), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "events.jsonl"
                argv = ["lost_volume.py", "restore", "--run", "test", "--output", str(output),
                        "--confirm-disposable-data-loss", "chronicle-rust"]
                with patch("sys.argv", argv), patch.object(lost_volume, "command", side_effect=reads) as commands, \
                        patch.object(lost_volume, "api", return_value=state), \
                        patch.object(lost_volume, "location", return_value=(pod["spec"]["nodeName"], "a" * 64, "/source", "/withheld")):
                    with self.assertRaisesRegex(RuntimeError, "quarantined"):
                        lost_volume.main()
                self.assertEqual(len(commands.call_args_list), len(reads))
                self.assertFalse(output.exists())

    def test_restore_retries_status_reads_without_repeating_volume_mutation(self):
        pod = {"metadata": {"name": "chronicle-2"}, "spec": {
            "nodeName": "k3d-chronicle-rust-agent-3-0", "volumes": [
                {"name": "data", "persistentVolumeClaim": {"claimName": "data-chronicle-2"}}]},
            "status": {"containerStatuses": [{"name": "chronicle", "ready": False}]}}
        state = {"nodes": {"3": {"draining": True}},
                 "placements": {str(g): {"complete": True, "voters": [1, 2, 4]} for g in range(5)}}
        results = ["k3d-chronicle-rust", json.dumps(pod), "volume", "{}", "[{}]", json.dumps({"items": [pod]}),
                   "stopped", "false", "identity-hash", "started", subprocess.CalledProcessError(1, "read-status"), '{"0":{"id":3}}']
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "events.jsonl"
            argv = ["lost_volume.py", "restore", "--run", "test", "--output", str(output),
                    "--confirm-disposable-data-loss", "chronicle-rust"]
            with patch("sys.argv", argv), patch.object(lost_volume, "command", side_effect=results) as commands, \
                    patch.object(lost_volume, "api", return_value=state), \
                    patch("retirement_partition.time.sleep"), \
                    patch.object(lost_volume, "location", return_value=(pod["spec"]["nodeName"], "a" * 64, "/source", "/withheld")):
                lost_volume.main()
            calls = [c.args for c in commands.call_args_list]
            self.assertEqual(sum(c[:4] == ("sudo", "docker", "run", "--rm") for c in calls), 1)
            self.assertEqual(sum(c[1:3] == ("get", "--raw") for c in calls), 2)
            events = [json.loads(line) for line in output.read_text().splitlines()]
            self.assertEqual(events[-1]["phase"], "original-process-recovered")
            self.assertTrue(events[-1]["state"]["nodes"]["3"]["draining"])


if __name__ == "__main__":
    unittest.main()
