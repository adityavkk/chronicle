package telemetry

import (
	"fmt"
	"strings"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
)

// newSampler builds the process sampler. A span with a parent follows the
// parent's sampled flag, remote (the caller's traceparent) or local, so a
// caller's trace is never broken here and an unsampled caller costs nothing.
// A root span is Chronicle's own decision (rootSampler).
func newSampler(ratio float64, always ...string) sdktrace.Sampler {
	root := rootSampler{ratio: sdktrace.TraceIDRatioBased(ratio), always: make(map[string]bool, len(always))}
	for _, operation := range always {
		root.always[operation] = true
	}
	return sdktrace.ParentBased(root)
}

// rootSampler decides spans that start a trace. An operation in always is
// kept; any other Chronicle operation is kept by the trace-id ratio; a root
// that is not a Chronicle operation is dropped: it is an instrumentation span
// from background work (slot ownership, queue polling, the recovery sweep),
// which would otherwise flood the destination with one-span traces.
type rootSampler struct {
	ratio  sdktrace.Sampler
	always map[string]bool
}

func (s rootSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	operation, ok := correlation.OperationOf(p.Name)
	if !ok {
		return sdktrace.SamplingResult{Decision: sdktrace.Drop, Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState()}
	}
	if s.always[operation] {
		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample, Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState()}
	}
	return s.ratio.ShouldSample(p)
}

func (s rootSampler) Description() string {
	always := make([]string, 0, len(s.always))
	for operation := range s.always {
		always = append(always, operation)
	}
	return fmt.Sprintf("ChronicleRoots{%s,always=%s}", s.ratio.Description(), strings.Join(always, ","))
}
