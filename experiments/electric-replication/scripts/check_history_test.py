import copy
import unittest

from check_history import check


def append(value, start, end, status=204):
    return dict(op="append", stream="/s", value=value, start=start, end=end, status=status)


def read(records, start, end, mode="linearizable", required=()):
    return dict(op="read", stream="/s", records=records, start=start, end=end,
                mode=mode, required=required, status=200)


class CheckerTest(unittest.TestCase):
    def test_accepts_concurrency_unknown_commit_and_duplicate_retry(self):
        history = [append("a", 1, 7), append("b", 2, 3, 503),
                   read(["b"], 2, 5), append("b", 8, 9), read(["b", "a"], 10, 11)]
        self.assertEqual(check(history)["operations"], 5)

    def test_rejects_ack_loss_phantom_duplicate_and_real_time_reorder(self):
        good = [append("a", 1, 2), append("b", 3, 4), read(["a", "b"], 5, 6)]
        check(good)
        for result in (["a"], ["a", "b", "c"], ["a", "a", "b"], ["b", "a"]):
            bad = copy.deepcopy(good)
            bad[-1]["records"] = result
            with self.subTest(result=result), self.assertRaises(AssertionError):
                check(bad)

    def test_strict_read_must_contain_prior_ack_but_prefix_may_lag(self):
        history = [append("a", 1, 2), read([], 3, 4), read(["a"], 5, 6)]
        with self.assertRaises(AssertionError):
            check(history)
        history[1]["mode"] = "prefix"
        check(history)
        history[1].update(mode="session", required=["a"])
        with self.assertRaises(AssertionError):
            check(history)

    def test_read_cannot_observe_future_or_divergent_unknown_effect(self):
        for history in (
            [read(["a"], 1, 2), append("a", 3, 4, 503)],
            [append("a", 1, 2, 503), append("b", 1, 2, 503),
             read(["a"], 3, 4, "prefix"), read(["b"], 3, 4, "prefix")],
        ):
            with self.assertRaises(AssertionError):
                check(history)


if __name__ == "__main__":
    unittest.main()
