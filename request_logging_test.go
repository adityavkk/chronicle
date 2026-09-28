package chronicle

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
)

// logLines decodes one JSON log record per line.
func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

func TestRequestLoggingMiddlewareCarriesSafeRequestID(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil)) // Info level: the start line is filtered
	var downstreamID string
	handler := RequestLoggingMiddleware(logger, "", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downstreamID = correlation.RequestID(r.Context())
		w.WriteHeader(http.StatusAccepted)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/stream/tenant/s1/main?token=secret-query", nil)
	req.Header.Set(correlation.DefaultHeader, "gateway-request-123")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if downstreamID != "gateway-request-123" {
		t.Fatalf("downstream request id = %q, want the caller value", downstreamID)
	}
	if got := rec.Header().Get(correlation.DefaultHeader); got != downstreamID {
		t.Fatalf("echoed header = %q, want %q", got, downstreamID)
	}
	if strings.Contains(logs.String(), "secret-query") || strings.Contains(logs.String(), "token=") {
		t.Fatalf("query string leaked into logs: %s", logs.String())
	}
	lines := logLines(t, &logs)
	if len(lines) != 1 {
		t.Fatalf("log lines at Info = %d, want exactly one completion event: %s", len(lines), logs.String())
	}
	done := lines[0]
	if done["event"] != "http_request_completed" || done["request_id"] != downstreamID ||
		done["http_status"] != float64(http.StatusAccepted) || done["outcome"] != "completed" ||
		done["method"] != http.MethodPost || done["path"] != "/v1/stream/tenant/s1/main" {
		t.Fatalf("completion event = %#v", done)
	}
	if _, ok := done["duration_ms"]; !ok {
		t.Fatalf("completion event lacks duration_ms: %#v", done)
	}
}

func TestRequestLoggingMiddlewareStartLineIsDebug(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	handler := RequestLoggingMiddleware(logger, "", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/stream/example", nil))

	lines := logLines(t, &logs)
	if len(lines) != 2 {
		t.Fatalf("log lines at Debug = %d, want start + completion: %s", len(lines), logs.String())
	}
	if lines[0]["event"] != "http_request_started" || lines[0]["level"] != "DEBUG" {
		t.Fatalf("first line = %#v, want a DEBUG http_request_started", lines[0])
	}
	if lines[0]["request_id"] != lines[1]["request_id"] {
		t.Fatalf("start and completion carry different ids: %v vs %v", lines[0]["request_id"], lines[1]["request_id"])
	}
}

func TestRequestLoggingMiddlewareReplacesUnsafeRequestID(t *testing.T) {
	handler := RequestLoggingMiddleware(
		slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)), "",
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }),
	)
	req := httptest.NewRequest(http.MethodGet, "/v1/stream/example", nil)
	req.Header.Set(correlation.DefaultHeader, "unsafe request id")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	requestID := rec.Header().Get(correlation.DefaultHeader)
	if requestID == "unsafe request id" || !correlation.Valid(requestID) {
		t.Fatalf("unsafe request id was not replaced: %q", requestID)
	}
}

func TestRequestLoggingMiddlewareHonorsConfiguredHeader(t *testing.T) {
	const header = "My-Platform-Request-ID"
	var downstreamID, downstreamHeader string
	handler := RequestLoggingMiddleware(
		slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)), header,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			downstreamID = correlation.RequestID(r.Context())
			downstreamHeader = r.Header.Get(header)
			w.WriteHeader(http.StatusNoContent)
		}),
	)
	req := httptest.NewRequest(http.MethodGet, "/v1/stream/example", nil)
	req.Header.Set(header, "platform-7")
	req.Header.Set(correlation.DefaultHeader, "ignored-default")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if downstreamID != "platform-7" || downstreamHeader != "platform-7" {
		t.Fatalf("configured header not read: ctx=%q header=%q", downstreamID, downstreamHeader)
	}
	if got := rec.Header().Get(header); got != "platform-7" {
		t.Fatalf("configured header not echoed: %q", got)
	}
	if got := rec.Header().Get(correlation.DefaultHeader); got != "" {
		t.Fatalf("default header must not be echoed when another name is configured, got %q", got)
	}
}

func TestRequestLoggingMiddlewarePreservesFlusher(t *testing.T) {
	handler := RequestLoggingMiddleware(
		slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)), "",
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			flusher, ok := w.(http.Flusher)
			if !ok {
				t.Fatal("logging response writer does not preserve http.Flusher")
			}
			flusher.Flush()
		}),
	)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/stream/example", nil))
	if rec.Code != http.StatusOK || !rec.Flushed {
		t.Fatalf("streaming response status/flushed = %d/%v", rec.Code, rec.Flushed)
	}
}

func TestRequestLoggingMiddlewareOutcomes(t *testing.T) {
	t.Run("server error is an ERROR failed event", func(t *testing.T) {
		var logs bytes.Buffer
		handler := RequestLoggingMiddleware(slog.New(slog.NewJSONHandler(&logs, nil)), "",
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "boom", http.StatusInternalServerError)
			}))
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/stream/example", nil))
		lines := logLines(t, &logs)
		if len(lines) != 1 || lines[0]["level"] != "ERROR" || lines[0]["outcome"] != "failed" ||
			lines[0]["http_status"] != float64(http.StatusInternalServerError) {
			t.Fatalf("5xx completion = %#v", lines)
		}
	})
	t.Run("panic is logged then re-raised", func(t *testing.T) {
		var logs bytes.Buffer
		handler := RequestLoggingMiddleware(slog.New(slog.NewJSONHandler(&logs, nil)), "",
			http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }))
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("middleware swallowed the panic")
				}
			}()
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/stream/example", nil))
		}()
		lines := logLines(t, &logs)
		if len(lines) != 1 || lines[0]["level"] != "ERROR" || lines[0]["outcome"] != "panicked" {
			t.Fatalf("panic completion = %#v", lines)
		}
	})
}

func TestRequestLoggingMiddlewareLogsTheTraceID(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	handler := RequestLoggingMiddleware(logger, "", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	traceID, _ := trace.TraceIDFromHex("11111111111111111111111111111111")
	spanID, _ := trace.SpanIDFromHex("2222222222222222")
	span := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled})
	req := httptest.NewRequest(http.MethodGet, "/v1/stream/example", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req.WithContext(trace.ContextWithSpanContext(req.Context(), span)))

	lines := logLines(t, &logs)
	if len(lines) != 2 {
		t.Fatalf("log lines = %d, want start + completion: %s", len(lines), logs.String())
	}
	for _, line := range lines {
		if line["trace_id"] != "11111111111111111111111111111111" {
			t.Fatalf("%s lacks the trace id: %#v", line["event"], line)
		}
	}

	logs.Reset()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/stream/example", nil))
	for _, line := range logLines(t, &logs) {
		if _, ok := line["trace_id"]; ok {
			t.Fatalf("an untraced request logged a trace_id: %#v", line)
		}
	}
}
