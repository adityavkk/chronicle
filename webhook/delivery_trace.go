package webhook

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
)

// deliveryTrace is the client span of one webhook delivery attempt and the
// context it runs under. Without a Tracer it is inert: no span is started and
// no trace header is written, so a deployment with tracing off leaves the
// wire exactly as before.
type deliveryTrace struct {
	ctx     context.Context
	span    trace.Span // nil when tracing is off
	traceID string     // "" when tracing is off
}

// startDelivery opens the span for one attempt at delivering wakeID, a child
// of the append that armed it when this process still remembers that append
// and the root of a new trace otherwise. Its attributes are the safe
// identifiers a receiver can join on; the target URL is not among them.
func (m *Manager) startDelivery(id string, generation int64, wakeID string) deliveryTrace {
	if m.tracer == nil {
		return deliveryTrace{ctx: context.Background()}
	}
	parent := trace.ContextWithSpanContext(context.Background(), m.traceForWake(wakeID))
	ctx, span := m.tracer.Start(parent, correlation.SpanName(correlation.OperationDelivery),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("chronicle.subscription_id", id),
			attribute.String("chronicle.wake_id", wakeID),
			attribute.Int64("chronicle.generation", generation),
		))
	return deliveryTrace{ctx: ctx, span: span, traceID: span.SpanContext().TraceID().String()}
}

// inject writes the attempt's W3C traceparent onto the outgoing headers.
// Trace state is never sent: the origin dropped it (appendOriginFrom).
func (d deliveryTrace) inject(h http.Header) {
	if d.span != nil {
		propagation.TraceContext{}.Inject(d.ctx, propagation.HeaderCarrier(h))
	}
}

// end closes the span with the response status (0 when none was received)
// and the outcome that failed the attempt ("" when it succeeded).
func (d deliveryTrace) end(status int, failure string) {
	if d.span == nil {
		return
	}
	if status != 0 {
		d.span.SetAttributes(attribute.Int("http.response.status_code", status))
	}
	if failure != "" {
		d.span.SetStatus(codes.Error, failure)
	}
	d.span.End()
}
