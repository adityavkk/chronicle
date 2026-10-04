import copy
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import leader_drain
import retirement_partition


spec = importlib.util.spec_from_file_location("drain", Path(__file__).parents[1] / "ops/drain.py")
drain = importlib.util.module_from_spec(spec)
spec.loader.exec_module(drain)


def repaired():
    return {
        "nodes": {str(i): {"draining": False} for i in range(1, 5)},
        "placements": {str(g): {"complete": True, "voters": [1, 2, 3]} for g in range(5)},
    }


class DrainTest(unittest.TestCase):
    def test_leader_selected_before_admission_and_rechecked_before_drain(self):
        for changed_role in (False, True):
            with self.subTest(changed_role=changed_role), tempfile.TemporaryDirectory() as directory:
                state = repaired()
                state["nodes"]["4"]["draining"] = True
                mutations, observations = [], []

                def api(path, value=None):
                    if path == "/admin/control":
                        return copy.deepcopy(state)
                    if path.startswith("/admin/retirement/"):
                        return True
                    node, registration = value
                    mutations.append((node, registration["draining"]))
                    state["nodes"][str(node)] = copy.deepcopy(registration)

                def status(pod):
                    observations.append(list(mutations))
                    role = "Learner" if state["nodes"]["1"]["draining"] else "Leader"
                    if changed_role and mutations:
                        role = "Follower"
                    return {str(g): {"id": 1, "state": role} for g in range(5)}

                def wait(predicate, label):
                    result = predicate()
                    self.assertTrue(result, label)
                    return result

                output = str(Path(directory) / "events.jsonl")
                with patch("sys.argv", ["leader_drain.py", "--output", output]), \
                        patch.object(leader_drain, "command", return_value="k3d-chronicle-rust"), \
                        patch.object(leader_drain, "processes", return_value={"pod": ["uid", "container", 0]}), \
                        patch.object(leader_drain, "pod_status", side_effect=status), \
                        patch.object(leader_drain, "api", side_effect=api), \
                        patch.object(leader_drain, "wait", side_effect=wait):
                    if changed_role:
                        with self.assertRaisesRegex(RuntimeError, "no longer leads"):
                            leader_drain.main()
                    else:
                        leader_drain.main()
                self.assertEqual(observations[:2], [[], [(4, False)]])
                self.assertEqual(mutations, [(4, False), (4, True)] if changed_role else
                                 [(4, False), (1, True), (1, False), (4, True)])
                events = [json.loads(line) for line in Path(output).read_text().splitlines()]
                self.assertEqual(next(e for e in events if e["phase"] == "leader-before-drain")["valid"],
                                 not changed_role)

    def test_partition_accepts_any_single_group_but_requires_unverified_retirement(self):
        for group, premature in ((0, False), (4, False), (0, True)):
            with self.subTest(group=group, premature=premature), tempfile.TemporaryDirectory() as directory:
                state = repaired()
                state["nodes"]["4"]["draining"] = True
                actions, mutations = [], []

                def api(path, value=None):
                    if path == "/admin/control":
                        return copy.deepcopy(state)
                    if path == "/admin/retirement/4":
                        return premature or not actions or actions[-1] == "heal"
                    node, registration = value
                    mutations.append((node, registration["draining"]))
                    state["nodes"][str(node)] = copy.deepcopy(registration)
                    state["placements"][str(group)]["voters"] = [1, 2, 3] if registration["draining"] else [1, 2, 4]

                def wait(predicate, label):
                    result = predicate()
                    self.assertTrue(result, label)
                    return result

                def command(*args):
                    return "k3d-chronicle-rust-agent-3-0" if "jsonpath={.spec.nodeName}" in args else ""

                def partition(args, **kwargs):
                    actions.append(args[-2])
                    return SimpleNamespace(returncode=0, stdout="", stderr="", check_returncode=lambda: None)

                output = str(Path(directory) / "events.jsonl")
                with patch("sys.argv", ["retirement_partition.py", "--node", "4", "--output", output]), \
                        patch.object(retirement_partition, "command", side_effect=command), \
                        patch.object(retirement_partition, "api", side_effect=api), \
                        patch.object(retirement_partition, "wait", side_effect=wait), \
                        patch.object(retirement_partition.subprocess, "run", side_effect=partition):
                    if premature:
                        with self.assertRaisesRegex(AssertionError, "unreachable replica"):
                            retirement_partition.main()
                    else:
                        retirement_partition.main()
                self.assertEqual(actions, ["isolate", "heal"])
                self.assertEqual(mutations, [(4, False), (4, True)])
                events = [json.loads(line) for line in Path(output).read_text().splitlines()]
                self.assertEqual(next(e for e in events if e["phase"] == "assigned")["groups"], [group])
                self.assertEqual(any(e["phase"] == "verified-after-heal" for e in events), not premature)

    def test_voter_repair_does_not_skip_old_process_verification(self):
        state = repaired()
        with patch("sys.argv", ["drain", "--url", "http://test", "--node", "4"]), \
             patch.object(drain, "request", side_effect=[state, {}, state, False, state, True]) as request, \
             patch.object(drain.time, "sleep"), patch("builtins.print") as report:
            drain.main()
        urls = [call.args[0] for call in request.call_args_list]
        self.assertEqual(urls.count("http://test/admin/register"), 1)
        self.assertEqual(urls.count("http://test/admin/retirement/4"), 2)
        report.assert_called_once()

    def test_unverified_old_process_is_not_reported_as_gracefully_drained(self):
        state = repaired()
        with patch("sys.argv", ["drain", "--url", "http://test", "--node", "4"]), \
             patch.object(drain, "request", side_effect=[state, {}, state, False]), \
             patch.object(drain.time, "monotonic", side_effect=[0, 0, 121]), \
             patch.object(drain.time, "sleep"), patch("builtins.print") as report:
            with self.assertRaisesRegex(TimeoutError, "retirement unverified"):
                drain.main()
        report.assert_not_called()


if __name__ == "__main__":
    unittest.main()
