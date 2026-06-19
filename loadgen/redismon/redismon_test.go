package redismon

import (
	"strings"
	"testing"
	"time"
)

// A golden `gcloud monitoring time-series list --format=json` response for the
// CPU metric: two points across one series, out of time order to prove the parser
// sorts.
const goldenCPUJSON = `[
  {
    "metric": {"type": "redis.googleapis.com/stats/cpu_utilization"},
    "resource": {"labels": {"instance_id": "chronicle-loadtest-redis"}},
    "points": [
      {"interval": {"endTime": "2026-06-14T12:01:00Z"}, "value": {"doubleValue": 0.42}},
      {"interval": {"endTime": "2026-06-14T12:00:00Z"}, "value": {"doubleValue": 0.18}}
    ]
  }
]`

func TestParseCPUSeries(t *testing.T) {
	s, err := ParseCPUSeries([]byte(goldenCPUJSON))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(s.Points))
	}
	// Sorted ascending by time.
	if s.Points[0].Time != "2026-06-14T12:00:00Z" || s.Points[1].Time != "2026-06-14T12:01:00Z" {
		t.Fatalf("points not time-sorted: %+v", s.Points)
	}
	if got := s.Max(); got != 0.42 {
		t.Fatalf("Max = %v, want 0.42", got)
	}
	if got := s.Mean(); got != 0.30 {
		t.Fatalf("Mean = %v, want 0.30", got)
	}
}

func TestParseCPUSeries_Empty(t *testing.T) {
	s, err := ParseCPUSeries([]byte(`[]`))
	if err != nil {
		t.Fatal(err)
	}
	if s.Max() != 0 || s.Mean() != 0 {
		t.Fatalf("empty series should have zero Max/Mean, got %+v", s)
	}
}

func TestMonitoringArgs(t *testing.T) {
	r := Reader{Project: "proj-x", Instance: "chronicle-loadtest-redis"}
	start := time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)
	end := time.Date(2026, 6, 14, 12, 5, 0, 0, time.UTC)
	args := strings.Join(r.MonitoringArgs(start, end), " ")

	for _, want := range []string{
		"monitoring time-series list",
		"--project proj-x",
		`metric.type="redis.googleapis.com/stats/cpu_utilization"`,
		`resource.labels.instance_id="chronicle-loadtest-redis"`,
		"--interval-start-time 2026-06-14T12:00:00Z",
		"--interval-end-time 2026-06-14T12:05:00Z",
		"--format json",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("MonitoringArgs missing %q\n got: %s", want, args)
		}
	}
}

func TestReadCPU_UsesInjectedRunner(t *testing.T) {
	var gotName string
	r := Reader{Project: "p", Instance: "i", Run: func(name string, _ ...string) ([]byte, error) {
		gotName = name
		return []byte(goldenCPUJSON), nil
	}}
	s, err := r.ReadCPU(time.Date(2026, 6, 14, 12, 5, 0, 0, time.UTC), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if gotName != "gcloud" {
		t.Fatalf("expected to invoke gcloud, got %q", gotName)
	}
	if s.Max() != 0.42 {
		t.Fatalf("Max = %v, want 0.42", s.Max())
	}
}
