package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
)

// remoteParent is a context carrying an extracted traceparent whose sampled
// flag is set or clear.
func remoteParent(t *testing.T, sampled bool) context.Context {
	t.Helper()
	traceID, _ := trace.TraceIDFromHex("11111111111111111111111111111111")
	spanID, _ := trace.SpanIDFromHex("2222222222222222")
	var flags trace.TraceFlags
	if sampled {
		flags = trace.FlagsSampled
	}
	return trace.ContextWithRemoteSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: flags, Remote: true,
	}))
}

// sampledProvider returns a provider over an in-memory exporter that samples
// roots with newSampler(ratio, always...).
func sampledProvider(t *testing.T, ratio float64, always ...string) (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(newSampler(ratio, always...)), sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return provider, exporter
}

func TestRootSamplerByRatioAndOperation(t *testing.T) {
	redis := trace.WithAttributes(attribute.String("db.system", "redis"))
	cases := []struct {
		name   string
		ratio  float64
		always []string
		parent context.Context
		span   string
		kind   trace.SpanKind
		attrs  []trace.SpanStartOption
		want   bool
	}{
		{"an always-listed operation is sampled at ratio zero", 0, []string{correlation.OperationAppend}, context.Background(), "chronicle.append", trace.SpanKindServer, nil, true},
		{"an unlisted operation follows the ratio: zero drops it", 0, []string{correlation.OperationAppend}, context.Background(), "chronicle.read", trace.SpanKindServer, nil, false},
		{"an unlisted operation follows the ratio: one keeps it", 1, nil, context.Background(), "chronicle.read", trace.SpanKindServer, nil, true},
		{"a delivery root follows the ratio", 1, nil, context.Background(), "chronicle.delivery", trace.SpanKindClient, nil, true},
		{"an instrumentation root is dropped even at ratio one", 1, nil, context.Background(), "redis.evalsha", trace.SpanKindClient, []trace.SpanStartOption{redis}, false},
		{"a sampled remote parent is honoured at ratio zero", 0, nil, remoteParent(t, true), "chronicle.read", trace.SpanKindServer, nil, true},
		{"an unsampled remote parent is honoured over the always list", 0, []string{correlation.OperationAppend}, remoteParent(t, false), "chronicle.append", trace.SpanKindServer, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			provider, exporter := sampledProvider(t, tc.ratio, tc.always...)
			opts := append([]trace.SpanStartOption{trace.WithSpanKind(tc.kind)}, tc.attrs...)
			_, span := provider.Tracer("sampling-test").Start(tc.parent, tc.span, opts...)
			span.End()
			if got := len(exporter.GetSpans()) == 1; got != tc.want {
				t.Fatalf("exported = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestChildrenFollowTheirParent(t *testing.T) {
	provider, exporter := sampledProvider(t, 0, correlation.OperationAppend)
	tracer := provider.Tracer("sampling-test")
	redis := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attribute.String("db.system", "redis"))}

	ctx, request := tracer.Start(context.Background(), "chronicle.append", trace.WithSpanKind(trace.SpanKindServer))
	_, command := tracer.Start(ctx, "redis.evalsha", redis...)
	command.End()
	request.End()
	spans := exporter.GetSpans()
	if len(spans) != 2 || spans[0].Parent.SpanID() != spans[1].SpanContext.SpanID() {
		t.Fatalf("a sampled append must export itself and its Redis child; got %d spans", len(spans))
	}

	exporter.Reset()
	ctx, request = tracer.Start(context.Background(), "chronicle.read", trace.WithSpanKind(trace.SpanKindServer))
	_, command = tracer.Start(ctx, "redis.evalsha", redis...)
	command.End()
	request.End()
	if got := exporter.GetSpans(); len(got) != 0 {
		t.Fatalf("a dropped read exported %d spans, want none", len(got))
	}
}
