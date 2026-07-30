package gate

import (
	"strings"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/run"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/scenario"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/stats"
)

func mixedResult(appends int64, p99 float64) *run.Result {
	start := time.Unix(100, 0)
	return &run.Result{
		Scenario:     scenario.Scenario{Duration: scenario.D{Duration: 10 * time.Second}},
		MeasureStart: start,
		// Catch-up requests can drain after the open-loop offer closes. The
		// retained write rate must still use the configured offer duration.
		MeasureEnd: start.Add(30 * time.Second),
		Counters:   map[string]int64{"appends_ok": appends},
		Metrics: map[string]stats.Quantiles{
			string(stats.Append): {Count: appends, P99: p99},
		},
	}
}

func TestCheckMixedCatchupPassesAtThresholds(t *testing.T) {
	got, err := CheckMixedCatchup(
		mixedResult(380, 2000),
		MixedCatchupSLO{MinAppendRate: 38, MaxAppendP99MS: 2000},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.AppendRate != 38 || got.AppendP99MS != 2000 {
		t.Fatalf("measured = %+v", got)
	}
}

func TestCheckMixedCatchupRejectsCapacityLoss(t *testing.T) {
	_, err := CheckMixedCatchup(
		mixedResult(379, 100),
		MixedCatchupSLO{MinAppendRate: 38, MaxAppendP99MS: 2000},
	)
	if err == nil || !strings.Contains(err.Error(), "append rate 37.90/s is below 38.00/s") {
		t.Fatalf("error = %v", err)
	}
}

func TestCheckMixedCatchupRejectsLatency(t *testing.T) {
	_, err := CheckMixedCatchup(
		mixedResult(400, 2000.01),
		MixedCatchupSLO{MinAppendRate: 38, MaxAppendP99MS: 2000},
	)
	if err == nil || !strings.Contains(err.Error(), "append p99 2000.01 ms exceeds 2000.00 ms") {
		t.Fatalf("error = %v", err)
	}
}

func TestCheckMixedCatchupRejectsMissingMeasurement(t *testing.T) {
	result := mixedResult(400, 100)
	delete(result.Metrics, string(stats.Append))
	if _, err := CheckMixedCatchup(
		result,
		MixedCatchupSLO{MinAppendRate: 38, MaxAppendP99MS: 2000},
	); err == nil || !strings.Contains(err.Error(), "append latency metric is missing") {
		t.Fatalf("error = %v", err)
	}
}

func TestCheckMixedCatchupRejectsCounterHistogramMismatch(t *testing.T) {
	result := mixedResult(400, 100)
	result.Metrics[string(stats.Append)] = stats.Quantiles{Count: 399, P99: 100}
	if _, err := CheckMixedCatchup(
		result,
		MixedCatchupSLO{MinAppendRate: 38, MaxAppendP99MS: 2000},
	); err == nil || !strings.Contains(err.Error(), "append latency count 399 does not match appends_ok 400") {
		t.Fatalf("error = %v", err)
	}
}
