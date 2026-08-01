package gate

import (
	"strings"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/run"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/scenario"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/stats"
)

func completeCatchupResult(completions int64) *run.Result {
	const responseBytes = int64(16_781_313)
	return &run.Result{
		Scenario: scenario.Scenario{
			Streams: scenario.Streams{
				ContentType: "application/json",
				Prefill: scenario.Prefill{
					Messages:     4096,
					MessageBytes: 4096,
				},
			},
		},
		Counters: map[string]int64{
			"catchup_ok":           completions,
			"catchup_integrity_ok": completions,
			"catchup_bytes":        completions * responseBytes,
		},
		Metrics: map[string]stats.Quantiles{
			string(stats.CatchupTTFB):  {Count: completions},
			string(stats.CatchupTotal): {Count: completions},
		},
		MetricSamples: []run.MetricSample{
			{Name: "redis", Metric: "total_commands_processed"},
			{Name: "chronicle", Metric: "chronicle_read_fetched_bytes_total"},
			{Name: "chronicle", Metric: "chronicle_read_returned_bytes_total"},
			{Name: "chronicle", Metric: "process_resident_memory_bytes"},
		},
	}
}

func TestCheckCatchupPassesCompleteInstrumentedCell(t *testing.T) {
	measured, err := CheckCatchup(completeCatchupResult(512), CatchupSLO{MinCompletions: 512})
	if err != nil {
		t.Fatal(err)
	}
	if measured.Completions != 512 || measured.ExpectedResponseBytes != 16_781_313 {
		t.Fatalf("measured = %+v", measured)
	}
}

func TestCheckCatchupRejectsZeroCompletionFalsePass(t *testing.T) {
	result := completeCatchupResult(0)
	_, err := CheckCatchup(result, CatchupSLO{MinCompletions: 512})
	if err == nil || !strings.Contains(err.Error(), "catch-up completions 0 are below required 512") {
		t.Fatalf("error = %v", err)
	}
	t.Logf("zero-completion rejection: %v", err)
}

func TestCheckCatchupRejectsPartialBodyDespiteSuccessfulCount(t *testing.T) {
	result := completeCatchupResult(8)
	result.Counters["catchup_bytes"]--
	_, err := CheckCatchup(result, CatchupSLO{MinCompletions: 8})
	if err == nil || !strings.Contains(err.Error(), "do not match 8 complete responses") {
		t.Fatalf("error = %v", err)
	}
	t.Logf("partial-body rejection: %v", err)
}

func TestCheckCatchupRejectsUnverifiedBodyIntegrity(t *testing.T) {
	result := completeCatchupResult(8)
	result.Counters["catchup_integrity_ok"] = 7
	_, err := CheckCatchup(result, CatchupSLO{MinCompletions: 8})
	if err == nil || !strings.Contains(err.Error(), "integrity completions 7") {
		t.Fatalf("error = %v", err)
	}
}

func TestCheckCatchupRejectsMissingTelemetry(t *testing.T) {
	result := completeCatchupResult(8)
	result.MetricSamples = nil
	_, err := CheckCatchup(result, CatchupSLO{MinCompletions: 8})
	if err == nil ||
		!strings.Contains(err.Error(), "required Redis metric samples are missing") ||
		!strings.Contains(err.Error(), "chronicle_read_fetched_bytes_total is missing") {
		t.Fatalf("error = %v", err)
	}
}
