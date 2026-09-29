package telemetry

import (
	"context"
	"log/slog"
	"sync/atomic"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// observedExporter wraps the OTLP exporter so a failing destination is one
// warning when it starts refusing batches, one line when it recovers, and a
// rising counter in between, never a warning per batch: the batcher flushes
// every second, so a destination that is down for an hour would otherwise
// log the same warning 3600 times. The counter is what an operator alerts on
// (chronicle_tracing_export_failures_total).
type observedExporter struct {
	sdktrace.SpanExporter
	logger   *slog.Logger
	failures atomic.Uint64
	failing  atomic.Bool
}

func observeExports(exporter sdktrace.SpanExporter, logger *slog.Logger) *observedExporter {
	return &observedExporter{SpanExporter: exporter, logger: logger}
}

// ExportSpans exports the batch and records the outcome. The error is
// returned unchanged, so the SDK still treats the batch as dropped.
func (e *observedExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	err := e.SpanExporter.ExportSpans(ctx, spans)
	if err != nil {
		e.failures.Add(1)
		if !e.failing.Swap(true) {
			e.logger.Warn("tracing export failing; spans are dropped until the destination recovers",
				"event", "tracing_export_failed", "error", err)
		}
		return err
	}
	if e.failing.Swap(false) {
		e.logger.Info("tracing export recovered", "event", "tracing_export_recovered")
	}
	return nil
}

// Failures is the number of batches the destination refused.
func (e *observedExporter) Failures() uint64 { return e.failures.Load() }
