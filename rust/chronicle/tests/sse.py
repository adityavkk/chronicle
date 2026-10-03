#!/usr/bin/env python3
"""Dependency-free HTTP SSE contract observations (schema 3, not Porcupine input).

This deliberately uses HTTPConnection: urllib's helpers buffer the response and
therefore cannot distinguish an event, an idle connection, and an aborted body.
"""
import argparse
import base64
import http.client
import itertools
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from history import emit
from gated_history import group_for


def offset(n):
    return f"0000000000000000_{n:016d}"


def encode_wire(body, is_json=False):
    """Independent model of wire.rs encode_wire, sufficient for test fixtures."""
    if not is_json:
        return body
    value = json.loads(body)
    values = value if isinstance(value, list) else [value]
    return b"".join(json.dumps(v, ensure_ascii=False, separators=(",", ":")).encode() + b"," for v in values)


class SSEError(ValueError):
    pass


class SSEParser:
    """Incremental UTF-8 SSE parser with strict end-of-body validation."""
    def __init__(self):
        self.buffer = b""
        self.data = []
        self.event = None

    def feed(self, chunk):
        self.buffer += chunk
        out = []
        while True:
            lf = self.buffer.find(b"\n")
            cr = self.buffer.find(b"\r")
            ends = [x for x in (lf, cr) if x >= 0]
            if not ends:
                break
            end = min(ends)
            # A CR at the chunk edge may be the first half of CRLF.
            if self.buffer[end] == 13 and end + 1 == len(self.buffer):
                break
            width = 2 if self.buffer[end:end + 2] == b"\r\n" else 1
            line, self.buffer = self.buffer[:end], self.buffer[end + width:]
            out.extend(self._line(line))
        return out

    def _line(self, raw):
        try:
            line = raw.decode("utf-8")
        except UnicodeDecodeError as error:
            raise SSEError("invalid UTF-8 in SSE field") from error
        if not line:
            if self.event is None and not self.data:
                return []
            if not self.data:
                raise SSEError("event has no data field")
            result = {"event": self.event or "message", "data": "\n".join(self.data)}
            self.event, self.data = None, []
            return [result]
        if line.startswith(":"):
            return []
        field, sep, value = line.partition(":")
        if sep and value.startswith(" "):
            value = value[1:]
        if field == "data":
            self.data.append(value if sep else "")
        elif field == "event":
            if not sep or not value or "\x00" in value:
                raise SSEError("malformed event field")
            self.event = value
        # id, retry, and extension fields are intentionally ignored.
        return []

    def finish(self):
        if self.buffer or self.event is not None or self.data:
            raise SSEError("truncated SSE event")


class Harness:
    def __init__(self, args, fp):
        self.args, self.fp = args, fp
        self.lock, self.ids = threading.Lock(), itertools.count()
        u = urllib.parse.urlsplit(args.url)
        if u.scheme not in ("http", "https") or not u.hostname:
            raise ValueError("--url must be one HTTP(S) ingress")
        self.url = u

    def path(self, suffix, query=None):
        name = urllib.parse.quote(self.args.path + "-" + suffix, safe="/")
        p = (self.url.path.rstrip("/") + "/v1/stream/live/" + name)
        return p + (("?" + urllib.parse.urlencode(query)) if query else "")

    def routing(self, suffix):
        group = group_for("live", self.args.path + "-" + suffix)
        with urllib.request.urlopen(self.args.url.rstrip("/") + "/admin/status", timeout=5) as response:
            status = json.load(response)[str(group)]
        emit(self.fp, self.lock, {"schema": 3, "type": "info", "f": "routing", "value": {
            "path": suffix, "shard": group, "status": status}})
        if self.args.require_forwarded:
            assert status["id"] != status["current_leader"], status

    def request(self, suffix, method, body=b"", headers=None, query=None):
        oid = str(next(self.ids))
        common = {"schema": 3, "id": oid, "process": "request", "f": "http"}
        headers = {"content-type": "application/octet-stream", **(headers or {}),
                   "x-request-id": f"sse-{self.args.path}-{oid}"}
        emit(self.fp, self.lock, {**common, "type": "invoke", "value": {
            "tenant": "live", "path": self.args.path + "-" + suffix, "method": method,
            "headers": headers, "query": query, "body_base64": base64.b64encode(body).decode()}})
        req = urllib.request.Request(urllib.parse.urlunsplit((self.url.scheme, self.url.netloc,
            self.path(suffix, query), "", "")), data=body if method in ("PUT", "POST") else None,
            method=method, headers=headers)
        started = time.monotonic()
        try:
            try:
                response = urllib.request.urlopen(req, timeout=10)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                status, response_headers, data = response.status, dict(response.headers), response.read()
        except (OSError, http.client.HTTPException) as error:
            emit(self.fp, self.lock, {**common, "type": "unknown", "value": {"error": repr(error)}})
            raise
        emit(self.fp, self.lock, {**common, "type": "ok" if status < 500 else "unknown", "value": {
            "status": status, "headers": response_headers, "body_base64": base64.b64encode(data).decode(),
            "elapsed_s": time.monotonic() - started}})
        return status, response_headers, data

    def stream(self, suffix, query, checks, timeout=20):
        oid = str(next(self.ids)); common = {"schema": 3, "id": oid, "process": "sse", "f": "http-sse"}
        emit(self.fp, self.lock, {**common, "type": "invoke", "value": {"tenant": "live",
            "path": self.args.path + "-" + suffix, "method": "GET", "query": query}})
        conn = (http.client.HTTPSConnection if self.url.scheme == "https" else http.client.HTTPConnection)(
            self.url.hostname, self.url.port, timeout=timeout)
        started, events, status, headers, error = time.monotonic(), [], None, {}, None
        stopped, callback_error = False, None
        try:
            conn.request("GET", self.path(suffix, query), headers={"accept": "text/event-stream",
                "x-request-id": f"sse-{self.args.path}-{oid}"})
            response = conn.getresponse(); status = response.status
            headers = {k.lower(): v for k, v in response.getheaders()}
            emit(self.fp, self.lock, {**common, "type": "info", "value": {"status": status, "headers": headers}})
            parser = SSEParser()
            if status == 200:
                stopped = False
                while True:
                    # read() tries to fill its requested size and can hide an
                    # otherwise complete event on a still-open connection.
                    chunk = response.read1(4096)
                    if not chunk: break
                    for event in parser.feed(chunk):
                        events.append(event)
                        emit(self.fp, self.lock, {**common, "type": "info", "f": "sse-event", "value": event})
                        try:
                            stopped = checks(events, time.monotonic() - started)
                        except Exception as exc:
                            callback_error = exc
                            raise
                        if stopped:
                            break
                    if stopped: break
                if not stopped: parser.finish()
            else:
                response.read()
        except Exception as exc:
            error = repr(exc)
        finally:
            conn.close()
        terminal = "ok" if not stopped and error is None and status is not None and status < 500 else "unknown"
        emit(self.fp, self.lock, {**common, "type": terminal, "value": {"status": status, "headers": headers,
            "events": events, "error": error, "error_source": "callback" if callback_error else "stream",
            "cancelled": stopped, "elapsed_s": time.monotonic() - started}})
        if callback_error is not None:
            raise callback_error
        return status, headers, events, error


def control(event, at, closed=False):
    assert event["event"] == "control", event
    value = json.loads(event["data"])
    assert value["streamNextOffset"] == offset(at), value
    assert value.get("upToDate") is True, value
    assert value.get("streamClosed", False) is closed, value
    assert ("streamCursor" in value) != closed, value
    if not closed: assert isinstance(value["streamCursor"], str), value


def run(args):
    # x mode is part of the safety contract: counterexamples are never replaced.
    with open(args.output, "x", encoding="utf-8") as fp:
        h = Harness(args, fp)
        live = {"live": "sse", "offset": "-1"}
        fixtures = [
            ("text", "text/plain", b" a\r\n b\xff", " a\n\n b�", None),
            ("json", "application/json", b'["a,b",true]', '["a,b",true]', None),
            ("binary", "application/octet-stream", b"\x00\xffabc", base64.b64encode(b"\x00\xffabc").decode(), "base64"),
        ]
        for name, ctype, body, expected, encoding in fixtures:
            wire = encode_wire(body, ctype == "application/json")
            assert h.request(name, "PUT", body, {"content-type": ctype})[0] == 201
            assert h.request(name, "POST", headers={"stream-closed": "true"})[0] == 200
            status, headers, events, error = h.stream(name, live, lambda *_: False)
            assert status == 200 and error is None and headers.get("content-type", "").startswith("text/event-stream")
            assert headers.get("stream-incarnation") == "1" and headers.get("stream-consistency") == "strict", headers
            assert headers.get("cache-control") == "no-store", headers
            assert headers.get("stream-sse-data-encoding") == encoding
            assert len(events) == 2, events
            assert events[0] == {"event": "data", "data": expected}; control(events[1], len(wire), True)

        # Caught-up control, append, then close. Reading continues until clean EOF.
        assert h.request("open", "PUT", headers={"content-type": "text/plain"})[0] == 201
        def writer(events, _elapsed):
            if len(events) == 1:
                control(events[0], 0)
                assert h.request("open", "POST", b"next", {"content-type": "text/plain"})[0] == 200
            elif len(events) == 3:
                control(events[2], 4)
                assert h.request("open", "POST", headers={"stream-closed": "true"})[0] == 200
            return False
        _, _, events, error = h.stream("open", {"live": "sse", "offset": "now"}, writer)
        assert error is None and len(events) == 4, (events, error)
        assert events[1] == {"event": "data", "data": "next"}; control(events[3], 4, True)

        # Beyond-tail numeric offsets clamp; now skips backlog.
        for suffix, start in (("future", offset(99)), ("now", "now")):
            assert h.request(suffix, "PUT", b"old", {"content-type": "text/plain"})[0] == 201
            initial_at = []
            def stop(events, elapsed):
                if events: initial_at.append(elapsed)
                return bool(events)
            _, _, events, error = h.stream(suffix, {"live": "sse", "offset": start}, stop)
            assert error is None and initial_at[0] < 2, initial_at
            control(events[0], 3)
        assert h.request("reject", "PUT")[0] == 201
        assert h.stream("reject", {"live": "sse"}, lambda *_: False)[0] == 400
        assert h.stream("reject", {**live, "consistency": "stale"}, lambda *_: False)[0] == 400

        # This catches an ingress's ordinary 10 s timeout; tolerate timer jitter.
        assert h.request("heartbeat", "PUT")[0] == 201
        h.routing("heartbeat")
        heartbeat_at = []
        def got_heartbeat(events, elapsed):
            if len(events) >= 2: heartbeat_at.append(elapsed)
            return bool(heartbeat_at)
        _, _, events, error = h.stream("heartbeat", {"live": "sse", "offset": "now"},
            got_heartbeat, timeout=25)
        assert error is None and len(events) >= 2
        assert 12 <= heartbeat_at[0] <= 18, heartbeat_at
        control(events[0], 0); control(events[1], 0)

        # The server owns this finite response lifetime and must finish cleanly.
        assert h.request("lifetime", "PUT")[0] == 201
        h.routing("lifetime")
        started = time.monotonic()
        _, _, lifetime_events, error = h.stream("lifetime", {"live": "sse", "offset": "now"},
            lambda *_: False, timeout=70)
        elapsed = time.monotonic() - started
        assert error is None and 55 <= elapsed <= 65, (elapsed, error)
        assert len(lifetime_events) >= 4
        for event in lifetime_events:
            control(event, 0)

        # Recreate while subscribed must terminate without leaking replacement bytes.
        assert h.request("recreate", "PUT", b"old", {"content-type": "text/plain"})[0] == 201
        def recreate(events, _elapsed):
            if len(events) == 1:
                control(events[0], 3)
                assert h.request("recreate", "DELETE")[0] == 204
                assert h.request("recreate", "PUT", b"NEW", {"content-type": "text/plain", "stream-incarnation": "2"})[0] == 201
            return False
        _, _, events, error = h.stream("recreate", {"live": "sse", "offset": "now"}, recreate, timeout=5)
        assert error is not None and "IncompleteRead" in error, error
        assert len(events) == 1, events
        control(events[0], 3)
        assert all("NEW" not in e["data"] for e in events)
    print(json.dumps({"result": "passed", "history": args.output,
        "scope": "SSE HTTP contract observations; not linearizability or fault evidence"}))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", required=True, help="one local ingress")
    p.add_argument("--path", required=True, help="new unused path prefix")
    p.add_argument("--output", required=True)
    p.add_argument("--require-forwarded", action="store_true", help="require a nonleader ingress for heartbeat/lifetime")
    run(p.parse_args())


if __name__ == "__main__":
    main()
