import http.client
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import sse


class ParserTest(unittest.TestCase):
    def parse(self, chunks):
        parser, out = sse.SSEParser(), []
        for chunk in chunks: out.extend(parser.feed(chunk))
        parser.finish()
        return out

    def test_every_byte_boundary_and_all_line_endings(self):
        wire = b": hello\r\nevent: data\rdata:a\ndata: b\r\n\r\n"
        expected = [{"event": "data", "data": "a\nb"}]
        self.assertEqual(self.parse([wire]), expected)
        for i in range(1, len(wire)):
            self.assertEqual(self.parse([wire[:i], wire[i:]]), expected, i)

    def test_comments_unknown_fields_and_default_event(self):
        self.assertEqual(self.parse([b":x\nunknown: ignored\nid: 4\ndata:x\n\n"]),
                         [{"event": "message", "data": "x"}])

    def test_data_lines_join_exactly(self):
        self.assertEqual(self.parse([b"event:data\ndata:\ndata: x \ndata\n\n"]),
                         [{"event": "data", "data": "\nx \n"}])

    def test_rejects_malformed_and_truncated_events(self):
        for wire in (b"event: data\n\n", b"event:\ndata:x\n\n", b"event: data\ndata:x", b"data:\xff\n\n"):
            with self.subTest(wire=wire), self.assertRaises(sse.SSEError):
                self.parse([wire])

    def test_empty_lines_and_complete_comments_are_not_truncated(self):
        self.assertEqual(self.parse([b"\n: comment\n\n"]), [])


class ModelTest(unittest.TestCase):
    def test_mutation_failure_cannot_count_as_sse_abort(self):
        class Args:
            url = "http://127.0.0.1:1"; path = "test"
        history = io.StringIO()
        with patch("sse.http.client.HTTPConnection") as connection:
            response = connection.return_value.getresponse.return_value
            response.status = 200
            response.getheaders.return_value = [("content-type", "text/event-stream")]
            response.read1.return_value = b'event: control\ndata: {}\n\n'
            def failing_mutation(*_args):
                raise http.client.IncompleteRead(b"mutation response")
            with self.assertRaises(http.client.IncompleteRead):
                sse.Harness(Args, history).stream("recreate", {}, failing_mutation)
        terminal = json.loads(history.getvalue().splitlines()[-1])
        self.assertEqual(terminal["type"], "unknown")
        self.assertEqual(terminal["value"]["error_source"], "callback")

    def test_wire_offsets_exclude_json_brackets(self):
        wire = sse.encode_wire(b'["a,b",true]', True)
        self.assertEqual(wire, b'"a,b",true,')
        self.assertEqual(len(wire), 11)

    def test_output_refuses_overwrite_before_network(self):
        class Args:
            output = ""; url = "http://127.0.0.1:1"; path = "unused"
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "history.jsonl"; path.write_text("keep")
            Args.output = str(path)
            with self.assertRaises(FileExistsError): sse.run(Args)
            self.assertEqual(path.read_text(), "keep")


if __name__ == "__main__":
    unittest.main()
