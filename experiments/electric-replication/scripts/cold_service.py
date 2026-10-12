"""Local MinIO launcher and bounded fault proxy, never a replacement object store.

The proxy forwards real S3 requests to MinIO. Fault queues and credentials stay
under disposable .tmp data. Evidence records method/key/hash/outcome, never auth
headers, signing keys or payloads. No portal or non-loopback listener is used.
"""
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import threading
import time
from urllib.parse import parse_qs, unquote, urlsplit
import xml.etree.ElementTree as ET


def credentials(path):
    data = json.loads(Path(path).read_text())
    assert set(data) == {"access", "secret"}
    return data


def execute(path, args):
    keys = credentials(path)
    os.environ.update(ELECTRIC_COLD_ACCESS_KEY_ID=keys["access"], ELECTRIC_COLD_SECRET_ACCESS_KEY=keys["secret"],
                      DS_S3_ACCESS_KEY_ID=keys["access"], DS_S3_SECRET_ACCESS_KEY=keys["secret"],
                      MINIO_ROOT_USER=keys["access"], MINIO_ROOT_PASSWORD=keys["secret"],
                      MINIO_BROWSER="off", MINIO_UPDATE="off")
    os.execv(args[0], args)


def proxy(upstream, port, config, log, secret_file):
    import boto3
    from botocore.config import Config
    keys = credentials(secret_file)
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{upstream}", region_name="us-east-1",
                      aws_access_key_id=keys["access"], aws_secret_access_key=keys["secret"],
                      config=Config(retries={"max_attempts": 0}, connect_timeout=2, read_timeout=10))
    guard, queued, completions = threading.Lock(), {}, {}
    config, log = Path(config), Path(log)

    def emit(event):
        with guard, log.open("a") as target:
            target.write(json.dumps(dict(version=1, node=port % 10, time_ns=time.monotonic_ns(), **event))+"\n")

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def reply(self, status, data=b"", headers=None):
            self.send_response(status)
            for key, value in (headers or {}).items():
                if key.lower() not in ("connection", "transfer-encoding", "content-length"):
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(data)
            self.close_connection = True

        def fault(self, path):
            try:
                fault = json.loads(config.read_text())
            except FileNotFoundError:
                return ""
            return fault.get("mode", "") if self.command in fault.get("methods", []) and fault.get("contains", "") in path else ""

        def handle_request(self):
            path = unquote(urlsplit(self.path).path)
            length = int(self.headers.get("Content-Length", "0"))
            if length > 9 * 1024 * 1024:
                self.reply(413)
                return
            body = self.rfile.read(length)
            if path == "/_fault/release":
                with guard:
                    pending = list(queued.items())
                    queued.clear()
                    multipart = list(completions.items())
                    completions.clear()
                for name, value in pending:
                    bucket, key = name.lstrip("/").split("/", 1)
                    s3.put_object(Bucket=bucket, Key=key, Body=value)
                    emit(dict(op="late-put", path=name, bytes=len(value), sha256=hashlib.sha256(value).hexdigest()))
                for (name, upload), parts in multipart:
                    bucket, key = name.lstrip("/").split("/", 1)
                    s3.complete_multipart_upload(Bucket=bucket,Key=key,UploadId=upload,MultipartUpload={"Parts":parts})
                    emit(dict(op="late-inventory",path=name,parts=len(parts)))
                self.reply(200, json.dumps({"released": len(pending),"inventories":len(multipart)}).encode())
                return
            fault = self.fault(path)
            if fault == "queue-inventory" and "uploadId" in parse_qs(urlsplit(self.path).query):
                # Hold real MinIO multipart completion AND its failed abort.
                # Parts are already in MinIO, not recreated by the proxy.
                assert "/i/" in path and self.command in ("POST","DELETE")
                if self.command == "POST":
                    upload = parse_qs(urlsplit(self.path).query)["uploadId"][0]
                    parts = [{"PartNumber":int(p.findtext("{*}PartNumber")),"ETag":p.findtext("{*}ETag")}
                             for p in ET.fromstring(body)]
                    assert parts
                    with guard:
                        assert len(completions) < 32 or (path,upload) in completions
                        completions[path,upload] = parts
                emit(dict(op="queued-inventory" if self.command == "POST" else "unknown-abort",path=path))
                self.reply(503)
                return
            if fault == "queue-put":
                assert self.command == "PUT" and "/p/" in path
                with guard:
                    if len(queued) < 32:
                        queued[path] = body
                emit(dict(op="queued-put", path=path, bytes=len(body), sha256=hashlib.sha256(body).hexdigest()))
                self.reply(503)
                return
            if fault == "hold":
                emit(dict(op="held", method=self.command, path=path))
                deadline = time.monotonic()+30
                while self.fault(path) == "hold" and time.monotonic() < deadline:
                    time.sleep(0.01)
            if fault == "unavailable":
                emit(dict(op="unavailable", method=self.command, path=path))
                self.reply(503)
                return
            start = time.monotonic_ns()
            connection = http.client.HTTPConnection("127.0.0.1", upstream, timeout=15)
            try:
                # Preserve Host for SigV4; TCP routing is independent of it.
                connection.request(self.command, self.path, body, dict(self.headers))
                response = connection.getresponse()
                headers = dict(response.getheaders())
                data = response.read(10 * 1024 * 1024 + 1)
                assert len(data) <= 10 * 1024 * 1024, "fixture proxy response bound"
                event = dict(op="s3", method=self.command, path=path, status=response.status,
                             start_ns=start, bytes=len(data), request_bytes=len(body),
                             range=self.headers.get("Range"),
                             sha256=hashlib.sha256(data).hexdigest(), fault=fault)
                if self.command == "GET" and path.count("/") == 1:
                    event["list_after"] = parse_qs(urlsplit(self.path).query).get("start-after",[""])[0]
                # Decode complete small inventory responses for the independent
                # checker. These are key hashes, never payloads or credentials.
                if self.command == "GET" and data[:8] == b"ERIC0001" and (len(data)-16) % 40 == 0:
                    event["inventory"] = dict(cut=int.from_bytes(data[8:16],"big"), records=[
                        dict(birth=int.from_bytes(data[i:i+8],"big"), key_sha256=data[i+8:i+40].hex())
                        for i in range(16,len(data),40)])
                emit(event)
                if fault == "unknown-delete":
                    assert self.command == "DELETE"
                    self.reply(503)  # Actual MinIO deletion completed, reply lost.
                else:
                    if fault == "corrupt" and data and response.status in (200, 206):
                        data = bytes([data[0] ^ 1]) + data[1:]
                    # HEAD has no payload but must retain the object's real size.
                    if self.command == "HEAD":
                        self.send_response(response.status)
                        for k, v in headers.items():
                            if k.lower() not in ("connection", "transfer-encoding"):
                                self.send_header(k, v)
                        self.send_header("Connection", "close")
                        self.end_headers()
                        self.close_connection = True
                    else:
                        self.reply(response.status, data, headers)
            except (OSError, http.client.HTTPException) as error:
                emit(dict(op="transport-unknown", method=self.command, path=path, error=type(error).__name__))
                self.close_connection = True
            finally:
                connection.close()

        do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_request

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    if sys.argv[1] == "exec":
        execute(sys.argv[2], sys.argv[3:])
    elif sys.argv[1] == "proxy":
        proxy(int(sys.argv[2]), int(sys.argv[3]), *sys.argv[4:])
    else:
        raise SystemExit("expected exec or proxy")
