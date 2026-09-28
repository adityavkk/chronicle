package telemetry

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
	"gecgithub01.walmart.com/auk000v/chronicle/internal/responsewriter"
)

// Handler traces every request to the main listener as one server span named
// for its operation, a child of the caller's traceparent when one is sent.
// The span carries the method, the status and the response size; the URL
// (stream paths name a deployment's entities) is never an attribute.
// Handlers below see the span on the request context, so the request logger
// can print its trace id and an append can hand it to the webhook it causes.
// When tracing is off, next is returned unwrapped.
func (t *Tracing) Handler(streamRoot string, next http.Handler) http.Handler {
	if t.provider == nil {
		return next
	}
	tracer := t.provider.Tracer(scopeName)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, correlation.SpanName(operationFor(streamRoot, r)),
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.request.method", r.Method)))
		defer span.End()

		tracked := responsewriter.Track(w)
		next.ServeHTTP(tracked, r.WithContext(ctx))

		span.SetAttributes(
			attribute.Int("http.response.status_code", tracked.Status()),
			attribute.Int64("http.response.body.size", tracked.Written()),
		)
		if tracked.Status() >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, "server error")
		}
		if r.Context().Err() != nil {
			span.SetAttributes(attribute.Bool("chronicle.cancelled", true))
		}
	})
}

// operationFor classifies a request by method and by whether it targets the
// reserved __ds subscription routes under streamRoot.
func operationFor(streamRoot string, r *http.Request) string {
	rest, underRoot := strings.CutPrefix(r.URL.Path, streamRoot)
	if !underRoot {
		return correlation.OperationOther
	}
	if rest == "__ds" || strings.HasPrefix(rest, "__ds/") {
		return correlation.OperationSubscription
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return correlation.OperationRead
	case http.MethodPost:
		return correlation.OperationAppend
	case http.MethodPut:
		return correlation.OperationCreate
	case http.MethodDelete:
		return correlation.OperationDelete
	default:
		return correlation.OperationOther
	}
}
