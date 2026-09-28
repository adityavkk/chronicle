package webhook

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
)

const (
	callerTraceID = "11111111111111111111111111111111"
	callerSpanID  = "2222222222222222"
)

// testTracer is a tracer over an in-memory exporter that samples every span.
func testTracer(t *testing.T) (trace.Tracer, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return provider.Tracer("webhook-test"), exporter
}

// tracedAppendContext is the context of an append request that arrived with
// a sampled traceparent and a tracestate a receiver must never be sent.
func tracedAppendContext(t *testing.T, requestID string) context.Context {
	t.Helper()
	traceID, _ := trace.TraceIDFromHex(callerTraceID)
	spanID, _ := trace.SpanIDFromHex(callerSpanID)
	state, err := trace.ParseTraceState("vendor=opaque")
	if err != nil {
		t.Fatal(err)
	}
	caller := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, TraceState: state, Remote: true})
	return trace.ContextWithRemoteSpanContext(correlation.WithRequestID(context.Background(), requestID), caller)
}

func spanAttribute(span tracetest.SpanStub, key string) string {
	for _, attr := range span.Attributes {
		if string(attr.Key) == key {
			return attr.Value.Emit()
		}
	}
	return ""
}

func TestAppendTraceReachesTheWebhookDelivery(t *testing.T) {
	tracer, exporter := testTracer(t)
	logger, logs := jsonLogger()
	post := newRecordingTransport("traceparent")
	_, base := armWebhookFixture(t, ManagerOptions{HTTPClient: &http.Client{Transport: post}, Tracer: tracer, Logger: logger}, tracedAppendContext(t, "gateway-request-123"))
	post.waitSettled(t)
	sub, _, _ := base.Get("s1")

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want the one delivery span", len(spans))
	}
	span := spans[0]
	if span.Name != "chronicle.delivery" || span.SpanKind != trace.SpanKindClient ||
		span.SpanContext.TraceID().String() != callerTraceID || span.Parent.SpanID().String() != callerSpanID {
		t.Fatalf("span %s %v (trace %s, parent %s) is not a delivery child of the append's trace", span.Name, span.SpanKind, span.SpanContext.TraceID(), span.Parent.SpanID())
	}
	if spanAttribute(span, "chronicle.subscription_id") != "s1" || spanAttribute(span, "chronicle.wake_id") != sub.WakeID ||
		spanAttribute(span, "http.response.status_code") != "200" {
		t.Fatalf("delivery span attributes = %v", span.Attributes)
	}
	headers := post.postHeaders()
	if len(headers) != 1 {
		t.Fatalf("POSTs = %d, want 1", len(headers))
	}
	if want := "00-" + callerTraceID + "-" + span.SpanContext.SpanID().String() + "-01"; headers[0].Get("traceparent") != want {
		t.Fatalf("traceparent = %q, want %q (the caller's trace, the delivery span, sampled)", headers[0].Get("traceparent"), want)
	}
	if headers[0].Get("tracestate") != "" {
		t.Fatalf("tracestate %q was forwarded to the receiver", headers[0].Get("tracestate"))
	}
	if headers[0].Get(correlation.DefaultHeader) != "gateway-request-123" {
		t.Fatalf("the request id header was lost alongside the trace: %v", headers[0])
	}
	events := logEvents(t, logs, "webhook_delivery_completed")
	if len(events) != 1 || events[0]["trace_id"] != callerTraceID || events[0]["request_id"] != "gateway-request-123" {
		t.Fatalf("delivery record = %v, want trace_id %s beside the request id", events, callerTraceID)
	}
}

func TestWebhookRetryReusesTheTrace(t *testing.T) {
	tracer, exporter := testTracer(t)
	post := newRecordingTransport("traceparent", http.StatusServiceUnavailable)
	mgr, base := armWebhookFixture(t, ManagerOptions{HTTPClient: &http.Client{Transport: post}, Tracer: tracer}, tracedAppendContext(t, "gateway-request-123"))
	post.waitSettled(t)
	sub, _, _ := base.Get("s1")
	if sub.RetryCount != 1 {
		t.Fatalf("first failure did not schedule a retry: %+v", sub)
	}
	mgr.deliverWebhookUnscoped("s1", sub.Generation, sub.WakeID)

	values := post.headerValues()
	if len(values) != 2 || !strings.HasPrefix(values[0], "00-"+callerTraceID+"-") || !strings.HasPrefix(values[1], "00-"+callerTraceID+"-") || values[0] == values[1] {
		t.Fatalf("traceparents across retry = %v, want the same trace with a new span each attempt", values)
	}
	spans := exporter.GetSpans()
	if len(spans) != 2 || spans[0].Parent.SpanID().String() != callerSpanID || spans[1].Parent.SpanID().String() != callerSpanID {
		t.Fatalf("spans = %+v, want two delivery spans under the append", spans)
	}
	if spanAttribute(spans[0], "http.response.status_code") != "503" || spans[0].Status.Code == spans[1].Status.Code {
		t.Fatalf("the failed attempt must record its status: %v / %v", spans[0].Status, spans[1].Status)
	}
}

func TestNoTraceparentWithoutATracer(t *testing.T) {
	post := newRecordingTransport("traceparent")
	_, _ = armWebhookFixture(t, ManagerOptions{HTTPClient: &http.Client{Transport: post}}, tracedAppendContext(t, "gateway-request-123"))
	post.waitSettled(t)
	if got := post.headerValues(); len(got) != 1 || got[0] != "" {
		t.Fatalf("traceparent = %v on a Manager without a Tracer, want none", got)
	}
}

func TestAppendOriginKeepsTheTraceButNotItsState(t *testing.T) {
	origin := appendOriginFrom(tracedAppendContext(t, "gateway-request-123"))
	if origin.requestID != "gateway-request-123" || !origin.trace.IsValid() || !origin.trace.IsSampled() || origin.trace.TraceID().String() != callerTraceID {
		t.Fatalf("origin = %+v", origin)
	}
	if origin.trace.TraceState().Len() != 0 {
		t.Fatalf("trace state %q kept in the bounded hint", origin.trace.TraceState())
	}
	if untraced := appendOriginFrom(context.Background()); untraced.known() {
		t.Fatalf("an append without id or trace has an origin: %+v", untraced)
	}
}

func TestWakeOriginIsRememberedUntilForgotten(t *testing.T) {
	mgr, _, _ := newTestManager(t)
	origin := appendOriginFrom(tracedAppendContext(t, "gateway-request-123"))
	if got := mgr.rememberWakeOrigin("w1", origin, DefaultLeaseTTLMs); got != "gateway-request-123" {
		t.Fatalf("rememberWakeOrigin = %q", got)
	}
	if !mgr.traceForWake("w1").Equal(origin.trace) {
		t.Fatal("the wake lost the append's trace")
	}
	// A traced append without a valid request id is still worth remembering.
	if got := mgr.rememberWakeOrigin("w2", appendOrigin{trace: origin.trace}, DefaultLeaseTTLMs); got != "wake-w2" || !mgr.traceForWake("w2").IsValid() {
		t.Fatalf("trace-only origin: id %q, trace valid %v", got, mgr.traceForWake("w2").IsValid())
	}
	mgr.forgetWakeRequestID("w1")
	if mgr.traceForWake("w1").IsValid() {
		t.Fatal("a forgotten wake still has a trace")
	}
}

func TestNewManagerKeepsTheDefaultDeliveryTimeout(t *testing.T) {
	mgr, store, streams := newTestManager(t)
	if mgr.client.Timeout != webhookDeliveryTimeout {
		t.Fatalf("default client timeout = %v, want %v", mgr.client.Timeout, webhookDeliveryTimeout)
	}
	supplied := &http.Client{Timeout: 3 * time.Second}
	adapted, err := NewManager(store, streams, ManagerOptions{StreamRootURL: "http://x/v1/stream/", HTTPClient: supplied})
	if err != nil {
		t.Fatal(err)
	}
	if adapted.client != supplied {
		t.Fatal("a supplied client must be used as-is")
	}
}
