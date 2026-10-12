"""Harness regressions only; real MinIO cells provide the integration evidence."""
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from botocore.exceptions import ClientError
from cold_faults import ColdLab
from cold_generations import collection_complete
from lab import Lab


class StartupS3:
    def __init__(self, delays=0, error=None, create_unknown=False):
        self.delays, self.error, self.create_unknown = delays, error, create_unknown
        self.buckets = set()

    def list_buckets(self):
        if self.error or self.delays:
            self.delays -= 1
            raise ClientError({"Error": {"Code": self.error or "XMinioServerNotInitialized"}}, "ListBuckets")
        return {"Buckets": []}

    def create_bucket(self, Bucket):
        if self.error:
            raise ClientError({"Error": {"Code": self.error}}, "CreateBucket")
        if self.delays:
            raise ClientError({"Error": {"Code": "XMinioServerNotInitialized"}}, "CreateBucket")
        self.buckets.add(Bucket)
        if self.create_unknown:
            self.create_unknown = False
            raise TimeoutError("response lost after mutation")


class StorageReadiness(unittest.TestCase):
    def test_delayed_object_layer_is_ready_before_bucket_creation(self):
        for delays in (0, 1, 3):
            with self.subTest(delays=delays), tempfile.TemporaryDirectory() as directory:
                provider = StartupS3(delays=delays)
                lab = SimpleNamespace(s3=provider, bucket="fixture", output=Path(directory), wait=Lab.wait)
                ColdLab.create_bucket(lab)
                self.assertEqual(provider.buckets, {"fixture"})

    def test_permanent_read_errors_and_unknown_mutations_are_not_retried(self):
        for code in ("AccessDenied", "InvalidAccessKeyId", "InternalError"):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                provider = StartupS3(error=code)
                lab = SimpleNamespace(s3=provider, bucket="fixture", output=Path(directory), wait=Lab.wait)
                with self.assertRaises(ClientError):
                    ColdLab.create_bucket(lab)
                self.assertEqual(provider.buckets, set())
        with tempfile.TemporaryDirectory() as directory:
            provider = StartupS3(create_unknown=True)
            lab = SimpleNamespace(s3=provider, bucket="fixture", output=Path(directory), wait=Lab.wait)
            with self.assertRaises(TimeoutError):
                ColdLab.create_bucket(lab)
            self.assertEqual(provider.buckets, {"fixture"})


class CollectionObservation(unittest.TestCase):
    def test_absence_is_not_completed_io_and_partial_events_must_wait(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            listed = []
            lab = SimpleNamespace(output=Path(directory), bucket="fixture", objects=lambda:listed)
            path = lab.output/"objects-1.jsonl"
            keys = {"first", "second"}
            good = dict(op="s3", method="DELETE", status=204, time_ns=101, path="/fixture/first")
            self.assertFalse(collection_complete(lab, keys, 100))
            for wrong in (dict(good, method="GET"), dict(good, status=503), dict(good, time_ns=99),
                          dict(good, path="/fixture/other")):
                path.write_text(json.dumps(wrong)+"\n"+json.dumps(dict(good,path="/fixture/second"))+"\n")
                self.assertFalse(collection_complete(lab, keys, 100))
            path.write_text(json.dumps(good)+"\n"+json.dumps(dict(good,path="/fixture/second")))
            self.assertFalse(collection_complete(lab, keys, 100))
            with path.open("a") as output:
                output.write("\n")
            self.assertTrue(collection_complete(lab, keys, 100))
            listed.append(dict(Key="first"))
            self.assertFalse(collection_complete(lab, keys, 100))


if __name__ == "__main__":
    unittest.main()
