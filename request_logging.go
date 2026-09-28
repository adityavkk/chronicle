package chronicle

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/trace"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
)

// requestStatusWriter records the status a handler committed so the completion
// event can report it. It keeps Chronicle's streaming contract intact: several
// SSE paths assert http.Flusher directly, so Unwrap alone is not enough.
type requestStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *requestStatusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *requestStatusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

// Flush commits a 200 if nothing was written yet, then flushes the underlying
// writer through the response controller.
func (w *requestStatusWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (w *requestStatusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// RequestLoggingMiddleware gives every request one correlation id and one
// completion event.
//
// The id is read from header (correlation.DefaultHeader when header is empty),
// normalized by correlation.Normalize, stored on the request context, written
// back onto the request header so downstream code sees the normalized value,
// and echoed on the response. The completion event (http_request_completed)
// carries request_id, method, path, http_status, outcome and duration_ms at
// Info, or at Error when the handler failed or panicked; a start line is
// emitted at Debug only. Both carry trace_id when the request is traced. URLs
// are logged as their path: never the query string, a body or a header.
func RequestLoggingMiddleware(logger *slog.Logger, header string, next http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	if header == "" {
		header = correlation.DefaultHeader
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := correlation.Normalize(r.Header.Get(header))
		ctx := correlation.WithRequestID(r.Context(), requestID)
		r = r.WithContext(ctx)
		r.Header.Set(header, requestID)
		w.Header().Set(header, requestID)

		startedAt := time.Now()
		logger.LogAttrs(ctx, slog.LevelDebug, "http request started",
			slog.String("event", "http_request_started"),
			slog.String("request_id", requestID),
			traceAttr(ctx),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path))

		tracked := &requestStatusWriter{ResponseWriter: w}
		complete := func(level slog.Level, outcome string) {
			status := tracked.status
			if status == 0 {
				status = http.StatusOK
			}
			logger.LogAttrs(ctx, level, "http request completed",
				slog.String("event", "http_request_completed"),
				slog.String("request_id", requestID),
				traceAttr(ctx),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("http_status", status),
				slog.String("outcome", outcome),
				slog.Int64("duration_ms", time.Since(startedAt).Milliseconds()))
		}
		defer func() {
			if recovered := recover(); recovered != nil {
				complete(slog.LevelError, "panicked")
				panic(recovered)
			}
			switch {
			case errors.Is(ctx.Err(), context.Canceled):
				complete(slog.LevelInfo, "cancelled")
			case tracked.status >= http.StatusInternalServerError:
				complete(slog.LevelError, "failed")
			default:
				complete(slog.LevelInfo, "completed")
			}
		}()

		next.ServeHTTP(tracked, r)
	})
}

// traceAttr is the trace_id attribute of a traced request, or the empty Attr,
// which slog handlers omit, for an untraced one.
func traceAttr(ctx context.Context) slog.Attr {
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return slog.String("trace_id", sc.TraceID().String())
	}
	return slog.Attr{}
}
