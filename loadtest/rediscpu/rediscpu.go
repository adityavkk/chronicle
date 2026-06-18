package rediscpu

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// Summary is a compact report over Cloud Monitoring time-series points for
// redis.googleapis.com/stats/cpu_utilization.
type Summary struct {
	Metric      string  `json:"metric"`
	Samples     int     `json:"samples"`
	MeanSeconds float64 `json:"mean_seconds"`
	MaxSeconds  float64 `json:"max_seconds"`
}

type timeSeries struct {
	Metric struct {
		Type string `json:"type"`
	} `json:"metric"`
	Points []point `json:"points"`
}

type point struct {
	Value typedValue `json:"value"`
}

type typedValue struct {
	DoubleValue any `json:"doubleValue"`
	Int64Value  any `json:"int64Value"`
}

// ParseMonitoringJSON parses `gcloud monitoring time-series list --format=json`
// output. Google encodes numeric values as JSON numbers or strings depending on
// the typed-value field, so parse both forms.
func ParseMonitoringJSON(data []byte) (Summary, error) {
	var series []timeSeries
	if err := json.Unmarshal(data, &series); err != nil {
		return Summary{}, err
	}
	var values []float64
	metric := "redis.googleapis.com/stats/cpu_utilization"
	for _, ts := range series {
		if ts.Metric.Type != "" {
			metric = ts.Metric.Type
		}
		for _, p := range ts.Points {
			v, ok, err := pointValue(p.Value)
			if err != nil {
				return Summary{}, err
			}
			if ok {
				values = append(values, v)
			}
		}
	}
	sort.Float64s(values)
	out := Summary{Metric: metric, Samples: len(values)}
	if len(values) == 0 {
		return out, nil
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	out.MeanSeconds = sum / float64(len(values))
	out.MaxSeconds = values[len(values)-1]
	return out, nil
}

func pointValue(v typedValue) (float64, bool, error) {
	if v.DoubleValue != nil {
		f, err := numeric(v.DoubleValue)
		return f, true, err
	}
	if v.Int64Value != nil {
		f, err := numeric(v.Int64Value)
		return f, true, err
	}
	return 0, false, nil
}

func numeric(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return 0, err
		}
		return f, nil
	default:
		return 0, fmt.Errorf("unsupported numeric value %T", v)
	}
}
