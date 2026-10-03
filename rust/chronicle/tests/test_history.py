import importlib.util
import http.client
import pathlib
import random
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch

P = pathlib.Path(__file__).with_name("history.py")
S = importlib.util.spec_from_file_location("history", P); H = importlib.util.module_from_spec(S); S.loader.exec_module(H)

def history(reads, writes=()):
    ev = []
    for oid, rec, typ, start, end in writes:
        ev += [{"type":"invoke","process":oid,"id":oid,"f":"append","time_ns":start},
               {"type":typ,"process":oid,"id":oid,"f":"append","time_ns":end,"value":{}},
               {"type":"info","f":"record","id":oid,"time_ns":end+1,"value":{"record":rec,"terminal":typ,"attempts":1}}]
    for n, (start, end, rs) in enumerate(reads):
        ev += [{"type":"invoke","process":"r","id":str(n),"f":"read","time_ns":start},
               {"type":"ok","process":"r","id":str(n),"f":"read","time_ns":end,"value":{"records":rs}}]
    return ev

class CheckerTest(unittest.TestCase):
    def test_query_encoding_preserves_inputs_and_explicit_stale_mode(self):
        client = H.Client(["http://unused"], "t", "p", 1, random.Random(0))
        query = {"offset": "a b&c", "consistency": "strict"}
        with patch.object(H.urllib.request, "urlopen") as open_url:
            client.request("GET", stale=True, query=query)
        actual = H.urllib.parse.parse_qs(H.urllib.parse.urlsplit(open_url.call_args.args[0].full_url).query)
        self.assertEqual(actual, {"offset": ["a b&c"], "consistency": ["stale"]})
        self.assertEqual(query, {"offset": "a b&c", "consistency": "strict"})

    def test_unknown_retry_is_paced_and_never_skips_a_sequence(self):
        for retries, expected in [(1, ["0", "0"]), (2, ["0", "0", "0", "1"])]:
            with tempfile.TemporaryDirectory() as directory:
                args = types.SimpleNamespace(seed=1, output=str(pathlib.Path(directory) / "history"),
                    url=["http://unused"], tenant="t", path="p", timeout=1, producers=1,
                    readers=0, operations=2, append_interval=0, retry_interval=.25,
                    retries=retries, nemesis="none")
                seen = []
                def request(method, data=None, headers=None):
                    if method == "POST":
                        seen.append(headers["producer-seq"])
                        if len(seen) <= 2:
                            return None, {}, b"", "timeout"
                    return 200, {}, b"", None
                with patch.object(H.Client, "request", side_effect=request), \
                     patch.object(H.time, "sleep") as pause, \
                     patch.object(H, "check_file", return_value={"valid": True}):
                    H.run_workload(args)
                self.assertEqual(seen, expected)
                self.assertEqual(pause.call_count, retries)
                pause.assert_called_with(.25)

    def test_workload_never_overwrites_retained_history(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "failed.jsonl"
            path.write_text("retained counterexample\n")
            with self.assertRaises(FileExistsError):
                H.run_workload(types.SimpleNamespace(seed=1, output=str(path)))
            self.assertEqual(path.read_text(), "retained counterexample\n")

    def test_truncated_success_or_error_body_is_unknown(self):
        client = H.Client(["http://unused"], "tenant", "path", 1, random.Random(0))
        response = MagicMock()
        response.__enter__.return_value = response
        response.closed = False
        response.status, response.headers = 200, {}
        response.read.side_effect = http.client.IncompleteRead(b"partial", 20)
        for failure in [None, H.urllib.error.HTTPError("http://unused", 409, "rejected", {}, response)]:
            with patch.object(H.urllib.request, "urlopen", return_value=response, side_effect=failure):
                status, _, data, error = client.request("POST", b"mutation")
            self.assertIsNone(status)
            self.assertEqual(data, b"")
            self.assertIn("IncompleteRead", error)

    def test_falsified_retention(self):
        self.assertFalse(H.check_events(history([(3,4,[])], [("w","a","ok",1,2)]))["checks"]["acked-retention"]["valid"])
    def test_duplicates(self):
        self.assertFalse(H.check_events(history([(1,2,["a","a"])]))["checks"]["duplicate"]["valid"])
    def test_non_prefix(self):
        self.assertFalse(H.check_events(history([(1,2,["a"]),(3,4,["b"])]))["checks"]["committed-prefix"]["valid"])
    def test_real_time_violation(self):
        x = history([(9,10,["b","a"])], [("a","a","ok",1,2),("b","b","ok",3,4)])
        self.assertFalse(H.check_events(x)["checks"]["linearizability"]["valid"])
    def test_unknown_accepted_write(self):
        x = history([(3,4,["a"])], [("a","a","unknown",1,2)])
        self.assertTrue(H.check_events(x)["valid"])

if __name__ == "__main__": unittest.main()
