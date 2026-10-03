import importlib.util
import http.client
import pathlib
import random
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
