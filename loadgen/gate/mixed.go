// Package gate evaluates load-test results against explicit acceptance limits.
package gate

import (
	"errors"
	"fmt"
	"strings"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/run"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/stats"
)

// MixedCatchupSLO is the retained write-capacity gate for catch-up load.
type MixedCatchupSLO struct {
	MinAppendRate  float64
	MaxAppendP99MS float64
}

// MixedCatchupResult contains the measured values used by the gate.
type MixedCatchupResult struct {
	AppendRate       float64
	AppendP99MS      float64
	AppendsOK        int64
	CatchupOK        int64
	CatchupBodyBytes int64
	OfferSeconds     float64
}

// CheckMixedCatchup evaluates successful append throughput and append p99.
func CheckMixedCatchup(result *run.Result, slo MixedCatchupSLO) (MixedCatchupResult, error) {
	if result == nil {
		return MixedCatchupResult{}, errors.New("result is required")
	}
	if slo.MinAppendRate <= 0 {
		return MixedCatchupResult{}, errors.New("minimum append rate must be positive")
	}
	if slo.MaxAppendP99MS <= 0 {
		return MixedCatchupResult{}, errors.New("maximum append p99 must be positive")
	}

	offerSeconds := result.Scenario.Duration.Duration.Seconds()
	if offerSeconds <= 0 {
		return MixedCatchupResult{}, errors.New("scenario duration must be positive")
	}
	appendLatency, ok := result.Metrics[string(stats.Append)]
	if !ok || appendLatency.Count == 0 {
		return MixedCatchupResult{}, errors.New("append latency metric is missing")
	}

	appendsOK := result.Counters["appends_ok"]
	measured := MixedCatchupResult{
		AppendRate:       float64(appendsOK) / offerSeconds,
		AppendP99MS:      appendLatency.P99,
		AppendsOK:        appendsOK,
		CatchupOK:        result.Counters["catchup_ok"],
		CatchupBodyBytes: result.Counters["catchup_bytes"],
		OfferSeconds:     offerSeconds,
	}

	var failures []error
	if appendLatency.Count != appendsOK {
		failures = append(failures, fmt.Errorf(
			"append latency count %d does not match appends_ok %d",
			appendLatency.Count,
			appendsOK,
		))
	}
	catchupTotal, catchupMetricOK := result.Metrics[string(stats.CatchupTotal)]
	if !catchupMetricOK || catchupTotal.Count == 0 {
		failures = append(failures, errors.New("mixed catch-up total latency metric is missing"))
	} else if catchupTotal.Count != measured.CatchupOK {
		failures = append(failures, fmt.Errorf(
			"mixed catch-up latency count %d does not match catchup_ok %d",
			catchupTotal.Count,
			measured.CatchupOK,
		))
	}
	if measured.CatchupOK == 0 {
		failures = append(failures, errors.New("mixed catch-up completed no reads"))
	}
	if measured.CatchupBodyBytes <= 0 {
		failures = append(failures, errors.New("mixed catch-up completed no response bytes"))
	}
	if bodyReads := result.Counters["catchup_body_read_ok"]; bodyReads != measured.CatchupOK {
		failures = append(failures, fmt.Errorf(
			"mixed completed-body reads %d do not match catchup_ok %d",
			bodyReads,
			measured.CatchupOK,
		))
	}
	for name, count := range result.Counters {
		if count > 0 && (strings.HasPrefix(name, "err:append:") || strings.HasPrefix(name, "err:catchup:")) {
			failures = append(failures, fmt.Errorf("mixed workload recorded %s=%d", name, count))
		}
	}
	if measured.AppendRate < slo.MinAppendRate {
		failures = append(failures, fmt.Errorf(
			"append rate %.2f/s is below %.2f/s",
			measured.AppendRate,
			slo.MinAppendRate,
		))
	}
	if measured.AppendP99MS > slo.MaxAppendP99MS {
		failures = append(failures, fmt.Errorf(
			"append p99 %.2f ms exceeds %.2f ms",
			measured.AppendP99MS,
			slo.MaxAppendP99MS,
		))
	}
	return measured, errors.Join(failures...)
}
