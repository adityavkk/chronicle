import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import resource_balance as harness


class ResourceBalanceTest(unittest.TestCase):
    def test_observes_stability_and_restores_after_invalid_domains_or_unknown_admission(self):
        for failure in (None, "domains", "response"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                state = {
                    "nodes": {str(i): {"zone": zone, "draining": i == 4}
                              for i, zone in ((1, "a"), (2, "b"), (3, "c"), (4, "unknown"))},
                    "placements": {str(g): {"voters": [1, 2, 3], "generation": 1, "complete": True}
                                   for g in range(5)},
                }
                mutations, clock = [], [0]

                def api(path, value=None):
                    if path == "/admin/control":
                        return copy.deepcopy(state)
                    if path == "/admin/retirement/4":
                        return True
                    node, registration = value
                    mutations.append(registration["draining"])
                    state["nodes"][str(node)] = copy.deepcopy(registration)
                    state["placements"]["1"]["generation"] += 1
                    state["placements"]["1"]["voters"] = ([1, 2, 3] if registration["draining"]
                        else [1, 3, 4] if failure == "domains" else [2, 3, 4])
                    if failure == "response" and not registration["draining"]:
                        raise TimeoutError("admission executed but response lost")

                def sleep(seconds):
                    clock[0] += seconds

                output = Path(directory) / "events.jsonl"
                with patch("sys.argv", ["resource_balance.py", "--output", str(output),
                                       "--stable-seconds", "6", "--timeout", "20"]), \
                        patch.object(harness, "api", side_effect=api), \
                        patch.object(harness, "command", return_value="k3d-chronicle-rust"), \
                        patch.object(harness, "processes", return_value={"node": "same-process"}), \
                        patch.object(harness.time, "monotonic", side_effect=lambda: clock[0]), \
                        patch.object(harness.time, "sleep", side_effect=sleep):
                    if failure:
                        with self.assertRaises((TimeoutError, RuntimeError)):
                            harness.main()
                    else:
                        harness.main()
                self.assertEqual(mutations, [False, True])
                self.assertTrue(state["nodes"]["4"]["draining"])
                events = [json.loads(line) for line in output.read_text().splitlines()]
                self.assertEqual(any(e["phase"] == "stable" for e in events), failure is None)
                if failure is None:
                    self.assertEqual(next(e for e in events if e["phase"] == "stable")["seconds"], 10)
                    self.assertEqual(events[-1]["phase"], "restored")
