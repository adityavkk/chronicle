package telemetry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
)

// scriptedExporter answers each ExportSpans call with the next scripted error.
type scriptedExporter struct {
	errs  []error
	calls int
}

func (s *scriptedExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	err := s.errs[s.calls]
	s.calls++
	return err
}

func (s *scriptedExporter) Shutdown(context.Context) error { return nil }

func TestObservedExporterCountsFailuresAndLogsTransitionsOnly(t *testing.T) {
	logger, logs := jsonLogger()
	refused := errors.New("destination refused the batch")
	inner := &scriptedExporter{errs: []error{nil, refused, refused, refused, nil, nil, refused}}
	observed := observeExports(inner, logger)
	for i, want := range inner.errs {
		if got := observed.ExportSpans(context.Background(), nil); !errors.Is(got, want) {
			t.Fatalf("export %d = %v, want the exporter's own %v", i, got, want)
		}
	}
	if got := observed.Failures(); got != 4 {
		t.Fatalf("Failures = %d, want every refused batch counted (4)", got)
	}
	records := logRecords(t, logs)
	want := []struct{ level, event string }{
		{"WARN", "tracing_export_failed"},
		{"INFO", "tracing_export_recovered"},
		{"WARN", "tracing_export_failed"},
	}
	if len(records) != len(want) {
		t.Fatalf("log = %s, want exactly one line per transition (%d)", logs.String(), len(want))
	}
	for i, rec := range records {
		if rec["level"] != want[i].level || rec["event"] != want[i].event {
			t.Fatalf("log line %d = %v, want %s %s", i, rec, want[i].level, want[i].event)
		}
	}
}

func TestStartCountsRefusedExports(t *testing.T) {
	// 400 is final for the exporter (no retry), so the refusal is immediate.
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer collector.Close()
	logger, logs := jsonLogger()
	tracing, err := Start(context.Background(), lookupFrom(map[string]string{EnvEndpoint: collector.URL + "/v1/traces"}), logger)
	if err != nil {
		t.Fatal(err)
	}
	if !tracing.Enabled() || tracing.ExportFailures() != 0 {
		t.Fatalf("enabled=%v failures=%d before any export", tracing.Enabled(), tracing.ExportFailures())
	}
	_, span := tracing.Tracer().Start(context.Background(), correlation.SpanName(correlation.OperationRead))
	span.End()
	_ = tracing.Shutdown(context.Background()) // flushes the batch; the refusal is the error under test
	if got := tracing.ExportFailures(); got != 1 {
		t.Fatalf("ExportFailures = %d after one refused batch, want 1", got)
	}
	var warned int
	for _, rec := range logRecords(t, logs) {
		if rec["event"] == "tracing_export_failed" && rec["level"] == "WARN" {
			warned++
		}
	}
	if warned != 1 {
		t.Fatalf("log = %s, want one WARN tracing_export_failed", logs.String())
	}
}
