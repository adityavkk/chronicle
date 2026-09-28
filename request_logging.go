package chronicle

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/trace"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
	"gecgithub01.walmart.com/auk000v/chronicle/internal/responsewriter"
)

// RequestLoggingMiddleware gives every request one correlation id and one
// completion event.
//
// The id is read from header (correlation.DefaultHeader when header is empty),
// normalized once as it is stored on the request context (correlation.WithRequestID),
// written back onto the request header so downstream code sees the normalized
// value, and echoed on the response. The completion event (http_request_completed)
// carries request_id, method, path, http_status, outcome and duration_ms at
// Info; at Error when the handler failed or panicked; at Warn when it aborted
// a committed stream (http.ErrAbortHandler), or Info if that client had
// already gone. A start line is emitted at Debug only. Both carry trace_id when the request is traced. URLs
// are logged as their path: never the query string, a body or a header.
func RequestLoggingMiddleware(logger *slog.Logger, header string, next http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	if header == "" {
		header = correlation.DefaultHeader
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := correlation.WithRequestID(r.Context(), r.Header.Get(header))
		requestID := correlation.RequestID(ctx)
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

		tracked := responsewriter.Track(w)
		complete := func(level slog.Level, outcome string) {
			logger.LogAttrs(ctx, level, "http request completed",
				slog.String("event", "http_request_completed"),
				slog.String("request_id", requestID),
				traceAttr(ctx),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("http_status", tracked.Status()),
				slog.String("outcome", outcome),
				slog.Int64("duration_ms", time.Since(startedAt).Milliseconds()))
		}
		defer func() {
			recovered := recover()
			switch {
			case recovered == http.ErrAbortHandler:
				// The SSE paths end a committed stream they cannot finish this
				// way, and net/http stays silent about it: a client that went
				// away is routine, a write that timed out on a live one is
				// worth a Warn, neither is an Error.
				level := slog.LevelWarn
				if ctx.Err() != nil {
					level = slog.LevelInfo
				}
				complete(level, "aborted")
			case recovered != nil:
				complete(slog.LevelError, "panicked")
			case errors.Is(ctx.Err(), context.Canceled):
				complete(slog.LevelInfo, "cancelled")
			case tracked.Status() >= http.StatusInternalServerError:
				complete(slog.LevelError, "failed")
			default:
				complete(slog.LevelInfo, "completed")
			}
			if recovered != nil {
				panic(recovered)
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
