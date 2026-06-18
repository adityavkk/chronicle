package rediscpu

import "testing"

func TestParseMonitoringJSON(t *testing.T) {
	data := []byte(`[
  {
    "metric": {"type": "redis.googleapis.com/stats/cpu_utilization"},
    "points": [
      {"value": {"doubleValue": 1.5}},
      {"value": {"doubleValue": "2.5"}}
    ]
  },
  {
    "metric": {"type": "redis.googleapis.com/stats/cpu_utilization"},
    "points": [
      {"value": {"int64Value": "3"}}
    ]
  }
]`)
	got, err := ParseMonitoringJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Samples != 3 || got.MeanSeconds != 7.0/3.0 || got.MaxSeconds != 3 {
		t.Fatalf("summary = %+v, want 3 samples mean 2.333 max 3", got)
	}
}

func TestParseMonitoringJSONNoSamples(t *testing.T) {
	got, err := ParseMonitoringJSON([]byte(`[]`))
	if err != nil {
		t.Fatal(err)
	}
	if got.Samples != 0 || got.MeanSeconds != 0 || got.MaxSeconds != 0 {
		t.Fatalf("empty summary = %+v", got)
	}
}
