import unittest
from unittest.mock import patch

import gated_history


class GateControlTest(unittest.TestCase):
    def test_file_probe_tests_existence_not_nonempty_path_string(self):
        with patch.object(gated_history, "kubectl") as command:
            gated_history.control("chronicle-0", "test", "/data/gates/reached", check=False)
        self.assertEqual(command.call_args.args[-3:], ("test", "-f", "/data/gates/reached"))

    def test_control_path_is_one_argument_without_shell_interpolation(self):
        with patch.object(gated_history, "kubectl") as command:
            gated_history.control("chronicle-0", "touch", "/data/gates/with space.arm")
        self.assertEqual(command.call_args.args[-2:], ("touch", "/data/gates/with space.arm"))
        self.assertIn("--", command.call_args.args)

    def test_unicode_tenant_uses_rust_utf8_byte_length(self):
        # Independent SHA256 fixture for the exact wire key 2:ép begins
        # 0a3bf702d1d2899b; its low two bits are 3, hence virtual shard 4.
        self.assertEqual(gated_history.group_for("é", "p"), 4)


if __name__ == "__main__":
    unittest.main()
