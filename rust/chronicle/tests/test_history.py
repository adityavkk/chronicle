import importlib.util
import pathlib
import unittest

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
