import copy
import unittest

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


if __name__ == "__main__":
    unittest.main()
