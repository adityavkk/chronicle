"""Disposable real HTTP receiver. Records delivery before deciding its response.

This is deliberately independent of the Rust server and stores no signing key.
Control calls arrange lost responses, delayed completion, retries and redirects.
"""
import base64
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import threading
import time


def serve(output, port):
    output = Path(output)
    lock = threading.Lock()
    policies, replies, records = {}, {}, []

    def record(event):
        with lock:
            records.append(event)
            with open(output / "deliveries.jsonl", "a") as file:
                file.write(json.dumps(event)+"\n")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def send(self, status, value):
            body = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Type", "application/json")
            if status == 307:
                self.send_header("Location", f"http://127.0.0.1:{port}/forbidden")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/_records":
                with lock:
                    result = records.copy()
                self.send(200, result)
            elif self.path == "/health":
                self.send(200, {})
            else:
                self.do_POST()

        def do_PUT(self):
            value = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            with lock:
                if self.path.startswith("/_reply/"):
                    replies[int(self.path.rsplit("/", 1)[1])] = value["mode"]
                else:
                    policies[value["path"]] = value["mode"]
            self.send(200, {})

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            with lock:
                identity = sum(r["op"] == "reservation" for r in records)
                mode = policies.get(self.path, "async")
                # Reserve the identity before another request can enter.
                records.append(dict(op="reservation", id=identity))
            record(dict(op="webhook", id=identity, path=self.path, start=time.monotonic(),
                        wall=time.time(), body=base64.b64encode(body).decode(),
                        headers=dict(self.headers), method=self.command))
            deadline = time.monotonic() + 90
            while mode == "hold" and time.monotonic() < deadline:
                with lock:
                    mode = replies.get(identity, "hold")
                time.sleep(0.01)
            event = dict(op="webhook_reply", id=identity, start=time.monotonic(), mode=mode)
            try:
                if mode in ("drop", "hold"):
                    self.close_connection = True
                else:
                    self.send(500 if mode == "fail" else 307 if mode == "redirect" else 200,
                              {"done": mode == "done"})
            except (BrokenPipeError, ConnectionResetError) as error:
                event["write_error"] = str(error)
            record(event)

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    serve(sys.argv[1], int(sys.argv[2]))
