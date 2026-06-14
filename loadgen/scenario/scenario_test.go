package scenario

import (
	"strings"
	"testing"
	"time"
)

const valid = `
name: smoke
target:
  base_url: http://localhost:4437
duration: 10s
warmup: 2s
streams:
  count: 4
  prefix: bench/smoke
writers:
  per_stream: 1
  rate: 20/s
  message_bytes: 120
tailers:
  sse_per_stream: 2
  long_poll_per_stream: 1
`

func TestParseValid(t *testing.T) {
	sc, err := Parse([]byte(valid))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if sc.Streams.ContentType != "application/json" {
		t.Errorf("default content type = %q", sc.Streams.ContentType)
	}
	if sc.Writers.Rate.From != 20 || sc.Writers.Rate.IsRamp() {
		t.Errorf("rate = %+v", sc.Writers.Rate)
	}
	if got := sc.TotalWriters(); got != 4 {
		t.Errorf("TotalWriters = %d, want 4", got)
	}
	if got := sc.TotalTailers(); got != 12 {
		t.Errorf("TotalTailers = %d, want 12", got)
	}
	if sc.Limits.RequestTimeout.Duration != 10*time.Second {
		t.Errorf("default request timeout = %v", sc.Limits.RequestTimeout)
	}
	if got := sc.StreamName(7); got != "bench/smoke-0007" {
		t.Errorf("StreamName = %q", got)
	}
}

func TestParseRate(t *testing.T) {
	cases := []struct {
		in      string
		from    float64
		to      float64
		wantErr bool
	}{
		{"30/s", 30, 30, false},
		{"0.5/s", 0.5, 0.5, false},
		{"120/m", 2, 2, false},
		{"5/s..50/s", 5, 50, false},
		{"5", 0, 0, true},
		{"x/s", 0, 0, true},
		{"5/h", 0, 0, true},
	}
	for _, c := range cases {
		r, err := ParseRate(c.in)
		if c.wantErr != (err != nil) {
			t.Errorf("ParseRate(%q) error = %v, wantErr %v", c.in, err, c.wantErr)
			continue
		}
		if err == nil && (r.From != c.from || r.To != c.to) {
			t.Errorf("ParseRate(%q) = %+v, want %g..%g", c.in, r, c.from, c.to)
		}
	}
}

func TestValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"no name", "duration: 5s\nwriters: {per_stream: 1, rate: 1/s}", "name is required"},
		{"no duration", "name: x\nwriters: {per_stream: 1, rate: 1/s}", "duration must be positive"},
		{"no workload", "name: x\nduration: 5s", "no workload"},
		{"writer no rate", "name: x\nduration: 5s\nwriters: {per_stream: 1}", "writers.rate is required"},
		{"bad producer", "name: x\nduration: 5s\nwriters: {per_stream: 1, rate: 1/s, producer: kafka}", "writers.producer"},
		{"bad from", "name: x\nduration: 5s\ntailers: {sse_per_stream: 1, from: middle}", "tailers.from"},
		{"tiny messages", "name: x\nduration: 5s\nwriters: {per_stream: 1, rate: 1/s, message_bytes: 8}", "message_bytes"},
		{"catchup empty", "name: x\nduration: 5s\ncatchup: {rate: 1/s}", "streams would be empty"},
		{"unknown field", "name: x\nduration: 5s\nwriterz: {}", "writerz"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(c.yaml))
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not mention %q", err, c.want)
			}
		})
	}
}
