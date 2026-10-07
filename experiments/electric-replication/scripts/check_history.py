"""Independent append-only history checker; does not import the server/lab.

Find a common byte prefix. Build a happens-before graph for successful strict
reads, observed appends (including unknown outcomes), and deduplicated records.
Require an acyclic linearization consistent with real-time and returned prefixes.
Prefix/session reads are checked for committed-prefix content and session minimum,
not treated as linearizable. Lifecycle/membership assertions belong to the driver.
This checks the tested append-only model, not arbitrary DS histories.
"""
import json
import sys
from collections import defaultdict, deque


def offset(event):
    # This fixture creates empty binary streams and appends UTF-8 value + LF.
    # Offsets are exact byte positions, not record counts or Python characters.
    raw = event["headers"]["stream-next-offset"]
    generation, position = raw.split("_")
    assert generation == "0000000000000000" and len(position) == 16 and position.isdecimal()
    return int(position)


def check(history):
    streams = defaultdict(list)
    for event in history:
        if event.get("op") in ("append", "read"):
            streams[event["stream"]].append(event)
    checked = 0
    for stream, events in streams.items():
        reads = [e for e in events if e["op"] == "read" and e["status"] == 200]
        assert reads, f"{stream}: no successful read evidence"
        canonical = max((e["records"] for e in reads), key=len)
        assert len(canonical) == len(set(canonical)), f"{stream}: duplicate effect"
        positions = {record: i for i, record in enumerate(canonical)}
        ends = {}
        byte_prefixes = {0}
        tail = 0
        for record in canonical:
            tail += len(record.encode("utf-8")) + 1
            ends[record] = tail
            byte_prefixes.add(tail)
        writes = defaultdict(list)
        for e in events:
            if e["op"] == "append":
                writes[e["value"]].append(e)
                if 200 <= e["status"] < 300:
                    assert e["value"] in positions, f"lost acknowledged record: {e}"
                    returned = offset(e)
                    if e["status"] == 200:  # New producer append, not a retry.
                        assert returned == ends[e["value"]], "append reply belongs to a different payload"
                    else:  # Duplicate retry reports the then-current durable tail.
                        assert e["status"] == 204
                        assert returned in byte_prefixes and returned >= ends[e["value"]], "invalid duplicate tail"
        vertices = {}
        for value, i in positions.items():
            attempts = writes[value]
            assert attempts, f"phantom record: {value}"
            # Dedup linearizes at the first effective attempt. Unknown attempts
            # may take effect up to the final observation, but not before invoke.
            successes = [e for e in attempts if 200 <= e["status"] < 300]
            vertices[("w", i)] = (min(e["start"] for e in attempts),
                                   min((e["end"] for e in successes), default=max(e["end"] for e in reads)))
        edges = defaultdict(set)
        for i in range(len(canonical)-1):
            edges[("w", i)].add(("w", i+1))
        for r, read in enumerate(reads):
            content = read["records"]
            assert content == canonical[:len(content)], f"incompatible prefix: {read}"
            assert offset(read) == (ends[content[-1]] if content else 0), "read offset differs from returned bytes"
            assert set(read.get("required", [])) <= set(content), f"session rollback: {read}"
            for record in content:
                assert any(e["start"] <= read["end"] for e in writes[record]), "read before invocation"
            if read["mode"] != "linearizable":
                continue
            key = ("r", r)
            vertices[key] = read["start"], read["end"]
            if content:
                edges[("w", len(content)-1)].add(key)
            if len(content) < len(canonical):
                edges[key].add(("w", len(content)))
        for a, (_, finish) in vertices.items():
            for b, (start, _) in vertices.items():
                if a != b and finish < start:
                    edges[a].add(b)
        indegree = dict.fromkeys(vertices, 0)
        for destinations in edges.values():
            for b in destinations:
                indegree[b] += 1
        ready = deque(k for k, degree in indegree.items() if degree == 0)
        visited = 0
        while ready:
            a = ready.popleft()
            visited += 1
            for b in edges[a]:
                indegree[b] -= 1
                if indegree[b] == 0:
                    ready.append(b)
        assert visited == len(vertices), f"{stream}: no real-time-consistent linearization"
        checked += len(events)
    assert checked > 0, "empty history is not evidence"
    return dict(verdict="PASS", operations=checked, streams=len(streams),
                scope="append-only prefix/dedup, exact per-append/read byte offsets, strict real-time graph, explicit session minimum")


if __name__ == "__main__":
    history = [json.loads(line) for line in open(sys.argv[1])]
    print(json.dumps(check(history), indent=2))
