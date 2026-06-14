package payload

import (
	"encoding/json"
	"testing"
)

func TestBuildJSONExactSizeAndRoundTrip(t *testing.T) {
	for _, size := range []int{MinSize, 100, 120, 1024, 64 << 10} {
		b := BuildJSON(42, 1700000000123456789, size)
		if len(b) != size {
			t.Errorf("size %d: got %d bytes", size, len(b))
		}
		if !json.Valid(b) {
			t.Fatalf("size %d: invalid JSON: %s", size, b[:min(len(b), 80)])
		}
		m, ok := Parse(b)
		if !ok || m.Seq != 42 || m.SentNano != 1700000000123456789 {
			t.Errorf("size %d: round trip = %+v, %v", size, m, ok)
		}
	}
}

func TestBuildJSONBatchFlattens(t *testing.T) {
	b := BuildJSONBatch(10, 999, 100, 3)
	raw, err := SplitJSONArray(b)
	if err != nil {
		t.Fatalf("SplitJSONArray: %v", err)
	}
	if len(raw) != 3 {
		t.Fatalf("len = %d, want 3", len(raw))
	}
	for i, r := range raw {
		m, ok := Parse(r)
		if !ok || m.Seq != 10+uint64(i) || m.SentNano != 999 {
			t.Errorf("element %d = %+v, %v", i, m, ok)
		}
		if len(r) != 100 {
			t.Errorf("element %d size = %d, want 100", i, len(r))
		}
	}
}

func TestBuildBytesFraming(t *testing.T) {
	b := BuildBytesBatch(5, 777, 80, 4)
	frames := SplitBytesFrames(b)
	if len(frames) != 4 {
		t.Fatalf("frames = %d, want 4", len(frames))
	}
	for i, f := range frames {
		m, ok := Parse(f)
		if !ok || m.Seq != 5+uint64(i) || m.SentNano != 777 {
			t.Errorf("frame %d = %+v, %v", i, m, ok)
		}
		// frame body is size-1 after the newline is consumed by the split
		if len(f) != 79 {
			t.Errorf("frame %d size = %d, want 79", i, len(f))
		}
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	if _, ok := Parse([]byte("not json")); ok {
		t.Error("Parse accepted garbage")
	}
}
