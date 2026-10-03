import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("drain", Path(__file__).parents[1] / "ops/drain.py")
drain = importlib.util.module_from_spec(spec)
spec.loader.exec_module(drain)


def repaired():
    return {
        "nodes": {str(i): {"draining": False} for i in range(1, 5)},
        "placements": {str(g): {"complete": True, "voters": [1, 2, 3]} for g in range(5)},
    }


class DrainTest(unittest.TestCase):
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
