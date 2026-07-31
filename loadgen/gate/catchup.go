package gate

import (
	"errors"
	"fmt"
	"strings"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/run"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/stats"
)

// CatchupSLO is the minimum evidence required from one reader-curve cell.
type CatchupSLO struct {
	MinCompletions int64
}

// CatchupResult contains the values checked by the reader-cell gate.
type CatchupResult struct {
	Completions           int64
	IntegrityCompletions  int64
	BodyBytes             int64
	ExpectedResponseBytes int64
}

// CheckCatchup rejects empty, partial, internally inconsistent, or
// uninstrumented reader cells.
func CheckCatchup(result *run.Result, slo CatchupSLO) (CatchupResult, error) {
	if result == nil {
		return CatchupResult{}, errors.New("result is required")
	}
	if slo.MinCompletions <= 0 {
		return CatchupResult{}, errors.New("minimum completions must be positive")
	}
	expectedBytes, err := expectedCatchupResponseBytes(result)
	if err != nil {
		return CatchupResult{}, err
	}
	completions := result.Counters["catchup_ok"]
	integrityCompletions := result.Counters["catchup_integrity_ok"]
	bodyBytes := result.Counters["catchup_bytes"]
	measured := CatchupResult{
		Completions:           completions,
		IntegrityCompletions:  integrityCompletions,
		BodyBytes:             bodyBytes,
		ExpectedResponseBytes: expectedBytes,
	}

	var failures []error
	if completions < slo.MinCompletions {
		failures = append(failures, fmt.Errorf(
			"catch-up completions %d are below required %d",
			completions,
			slo.MinCompletions,
		))
	}
	total, totalOK := result.Metrics[string(stats.CatchupTotal)]
	if !totalOK || total.Count == 0 {
		failures = append(failures, errors.New("catch-up total latency metric is missing"))
	} else if total.Count != completions {
		failures = append(failures, fmt.Errorf(
			"catch-up total latency count %d does not match catchup_ok %d",
			total.Count,
			completions,
		))
	}
	ttfb, ttfbOK := result.Metrics[string(stats.CatchupTTFB)]
	if !ttfbOK || ttfb.Count == 0 {
		failures = append(failures, errors.New("catch-up TTFB metric is missing"))
	} else if ttfb.Count != completions {
		failures = append(failures, fmt.Errorf(
			"catch-up TTFB count %d does not match catchup_ok %d",
			ttfb.Count,
			completions,
		))
	}
	wantTotalBytes := completions * expectedBytes
	if bodyBytes != wantTotalBytes {
		failures = append(failures, fmt.Errorf(
			"catch-up body bytes %d do not match %d complete responses at %d bytes each (want %d)",
			bodyBytes,
			completions,
			expectedBytes,
			wantTotalBytes,
		))
	}
	if result.Scenario.Writers.PerStream == 0 || result.Scenario.Writers.Rate.IsZero() {
		if integrityCompletions != completions {
			failures = append(failures, fmt.Errorf(
				"catch-up body integrity completions %d do not match successful completions %d",
				integrityCompletions,
				completions,
			))
		}
	}
	if !hasMetricSample(result, "redis", "total_commands_processed") {
		failures = append(failures, errors.New("required Redis metric samples are missing"))
	}
	for _, metric := range []string{
		"chronicle_read_fetched_bytes_total",
		"chronicle_read_returned_bytes_total",
		"process_resident_memory_bytes",
	} {
		if !hasMetricSample(result, "chronicle", metric) {
			failures = append(failures, fmt.Errorf("required Chronicle metric sample %s is missing", metric))
		}
	}
	return measured, errors.Join(failures...)
}

func expectedCatchupResponseBytes(result *run.Result) (int64, error) {
	prefill := result.Scenario.Streams.Prefill
	if prefill.Messages <= 0 || prefill.MessageBytes <= 0 {
		return 0, errors.New("positive prefill dimensions are required")
	}
	payloadBytes := int64(prefill.Messages) * int64(prefill.MessageBytes)
	contentType := result.Scenario.Streams.ContentType
	if mediaType, _, ok := strings.Cut(contentType, ";"); ok {
		contentType = mediaType
	}
	if strings.EqualFold(strings.TrimSpace(contentType), "application/json") {
		return payloadBytes + int64(prefill.Messages-1) + 2, nil
	}
	return payloadBytes, nil
}

func hasMetricSample(result *run.Result, source, metric string) bool {
	for _, sample := range result.MetricSamples {
		if sample.Name == source && sample.Metric == metric {
			return true
		}
	}
	return false
}
