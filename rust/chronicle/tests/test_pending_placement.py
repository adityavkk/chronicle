import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pending_placement as harness


class CleanupTest(unittest.TestCase):
    def test_crash_needs_runtime_replacement_not_only_ready_or_command_success(self):
        before = {"metadata": {"uid": "same-pod"}, "status": {"containerStatuses": [{
            "name": "chronicle", "containerID": "containerd://" + "a" * 64,
            "restartCount": 0, "ready": True, "lastState": {},
        }]}}
        for changed in ({}, {"containerID": "containerd://" + "a" * 64},
                        {"restartCount": 0}, {"ready": False},
                        {"lastState": {"terminated": {"exitCode": 1}}}):
            with self.subTest(changed=changed):
                after = copy.deepcopy(before)
                after["status"]["containerStatuses"][0].update({
                    "containerID": "containerd://" + "b" * 64, "restartCount": 1,
                    "lastState": {"terminated": {"exitCode": 137}}, **changed,
                })

                def wait(predicate, label):
                    result = predicate()
                    if result is None:
                        raise TimeoutError(label)
                    return result

                with patch.object(harness, "command", side_effect=[json.dumps(before), json.dumps(after)]), \
                        patch.object(harness, "wait", side_effect=wait), \
                        patch.object(harness.subprocess, "run", return_value=SimpleNamespace(
                            returncode=0, stdout="", stderr="")) as signal:
                    if changed:
                        with self.assertRaises((TimeoutError, RuntimeError)):
                            harness.crash_gated("pod", "agent", Mock())
                    else:
                        harness.crash_gated("pod", "agent", Mock())
                self.assertEqual(signal.call_args.args[0][-3:], ["--signal", "SIGKILL", "a" * 64])

    def test_verified_restart_does_not_wait_for_a_dead_actor_to_resume(self):
        files = {s: f"gate.{s}" for s in ("arm", "release", "reached", "resumed")}
        with patch.object(harness, "control") as control, patch.object(harness, "wait") as wait, \
                patch.object(harness, "command") as command:
            errors = harness.cleanup("pod", None, [files], files, True, Mock(), Mock(), restarted=True)
        self.assertEqual(errors, [])
        wait.assert_not_called()
        self.assertEqual([call.args[1] for call in control.call_args_list], ["rm", "touch"])
        self.assertEqual(command.call_count, 3)

    def test_cleanup_failures_do_not_skip_release_or_heal(self):
        files = [{s: f"{i}.{s}" for s in ("arm", "release", "reached", "resumed")} for i in range(2)]
        calls, note, partition = [], Mock(), Mock()

        def control(pod, action, path):
            calls.append((action, path))
            if path in ("0.arm", "0.release"):
                raise TimeoutError(path)

        with patch.object(harness, "control", side_effect=control), \
                patch.object(harness, "quarantine", side_effect=TimeoutError("quarantine")), \
                patch.object(harness, "command") as command:
            errors = harness.cleanup("pod", 5, files, files[0], True, partition, note)
        self.assertEqual(calls, [("rm", "0.arm"), ("rm", "1.arm"),
                                 ("touch", "0.release"), ("touch", "1.release")])
        self.assertEqual(len(errors), 3)
        partition.assert_called_once_with("heal")
        command.assert_not_called()  # Uncertain controls remain for inspection.

    def test_uncertain_creation_and_admission_are_owned_before_remote_mutation(self):
        for failure in ("arm", "admission"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                state = {"nodes": {str(i): {"addr": f"node-{i}", "zone": "a", "draining": i == 5}
                                   for i in (1, 2, 3, 5)}}
                mutations = []

                def api(path, value=None):
                    if path == "/admin/control":
                        return copy.deepcopy(state)
                    if path == "/admin/retirement/5":
                        return True
                    self.assertEqual(path, "/admin/register")
                    mutations.append(copy.deepcopy(value))
                    state["nodes"]["5"] = copy.deepcopy(value[1])
                    if not value[1]["draining"]:
                        raise TimeoutError("admission response lost after execution")

                def command(*args):
                    if "jsonpath={.spec.nodeName}" in args:
                        return "k3d-chronicle-rust-agent-4-0"
                    if failure == "arm" and 'set -C; : > "$1"' in args:
                        raise TimeoutError("arm response lost after execution")
                    return ""

                def control(pod, action, path, check=True):
                    return SimpleNamespace(returncode=1 if action == "test" else 0)

                output = str(Path(directory) / "events.jsonl")
                with patch("sys.argv", ["pending_placement.py", "--node", "5", "--output", output]), \
                        patch.object(harness, "preflight"), \
                        patch.object(harness, "api", side_effect=api), \
                        patch.object(harness, "command", side_effect=command), \
                        patch.object(harness, "control", side_effect=control) as controls:
                    with self.assertRaises(TimeoutError):
                        harness.main()
                releases = [c.args[2] for c in controls.call_args_list if c.args[1] == "touch"]
                self.assertEqual(len(releases), 1 if failure == "arm" else 2)
                self.assertTrue(state["nodes"]["5"]["draining"])
                self.assertEqual([v[1]["draining"] for v in mutations],
                                 [] if failure == "arm" else [False, True])
                events = [json.loads(line) for line in Path(output).read_text().splitlines()]
                self.assertEqual(next(e for e in events if e["phase"] == "cleanup")["errors"], [])
                self.assertTrue(any(e["phase"] == "failed" for e in events))


if __name__ == "__main__":
    unittest.main()
