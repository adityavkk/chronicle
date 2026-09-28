package telemetry

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
)

const neverLogged = "credential-value-never-logged"

func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func jsonLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

func logRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
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

func TestStartIsOffWithoutAnEndpoint(t *testing.T) {
	logger, logs := jsonLogger()
	// Nothing else is read while the endpoint is unset, a malformed ratio included.
	tracing, err := Start(context.Background(), lookupFrom(map[string]string{EnvSampleRatio: "often"}), logger)
	if err != nil {
		t.Fatal(err)
	}
	if tracing.Enabled() {
		t.Fatal("tracing enabled without an endpoint")
	}
	if reason, failed := tracing.FailedOpen(); failed {
		t.Fatalf("failed open without an endpoint: %s", reason)
	}
	if tracing.Tracer() != nil {
		t.Fatal("a disabled Tracing must hand out no tracer")
	}
	next := http.NewServeMux()
	if got := tracing.Handler("/v1/stream/", next); got != http.Handler(next) {
		t.Fatal("a disabled Tracing must leave the handler unwrapped")
	}
	if err := tracing.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if logs.Len() != 0 {
		t.Fatalf("disabled tracing logged: %s", logs.String())
	}
}

// assertFailedOpen checks the fail-open contract: no error, tracing disabled,
// the reason reported for the metric, exactly one warning that names it, and
// no credential material anywhere in the log.
func assertFailedOpen(t *testing.T, tracing *Tracing, logs *bytes.Buffer, reason string) {
	t.Helper()
	if tracing.Enabled() || tracing.Tracer() != nil {
		t.Fatal("tracing must be disabled after failing open")
	}
	if got, failed := tracing.FailedOpen(); !failed || got != reason {
		t.Fatalf("FailedOpen = (%q, %v), want (%q, true)", got, failed, reason)
	}
	records := logRecords(t, logs)
	if len(records) != 1 || records[0]["level"] != "WARN" || records[0]["event"] != "tracing_disabled" || records[0]["reason"] != reason {
		t.Fatalf("log = %s, want one WARN tracing_disabled with reason %s", logs.String(), reason)
	}
	if strings.Contains(logs.String(), neverLogged) {
		t.Fatalf("credential material reached the log: %s", logs.String())
	}
}

func TestStartFailsOpenWhenACredentialFileIsUnavailable(t *testing.T) {
	present := writeFile(t, "user", neverLogged)
	cases := []struct{ name, username, password string }{
		{"username file missing", filepath.Join(t.TempDir(), "absent"), present},
		{"password file empty", present, writeFile(t, "empty", "")},
		{"password file blank", present, writeFile(t, "blank", " \n")},
		{"username file is a directory", t.TempDir(), present},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, logs := jsonLogger()
			tracing, err := Start(context.Background(), lookupFrom(map[string]string{
				EnvEndpoint: "https://collector.example/v1/traces", EnvUsernameFile: tc.username, EnvPasswordFile: tc.password,
			}), logger)
			if err != nil {
				t.Fatalf("startup must continue without tracing, got %v", err)
			}
			assertFailedOpen(t, tracing, logs, ReasonCredentialsUnavailable)
		})
	}
}

func TestStartFailsOpenWhenTheCAFileIsUnavailable(t *testing.T) {
	cases := []struct{ name, ca string }{
		{"CA file missing", filepath.Join(t.TempDir(), "absent")},
		{"CA file is not PEM", writeFile(t, "ca.pem", neverLogged)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, logs := jsonLogger()
			tracing, err := Start(context.Background(), lookupFrom(map[string]string{
				EnvEndpoint: "https://collector.example/v1/traces", EnvCAFile: tc.ca,
			}), logger)
			if err != nil {
				t.Fatalf("startup must continue without tracing, got %v", err)
			}
			assertFailedOpen(t, tracing, logs, ReasonCAUnavailable)
		})
	}
}

func TestStartRefusesAMalformedConfiguration(t *testing.T) {
	const endpoint = "https://collector.example/v1/traces"
	user := writeFile(t, "user", "u")
	cases := map[string]map[string]string{
		"plaintext to a remote host":            {EnvEndpoint: "http://collector.example/v1/traces"},
		"credentials in the URL":                {EnvEndpoint: "https://u:p@collector.example/v1/traces"},
		"query in the URL":                      {EnvEndpoint: endpoint + "?tenant=1"},
		"fragment in the URL":                   {EnvEndpoint: endpoint + "#x"},
		"no host":                               {EnvEndpoint: "https:///v1/traces"},
		"unknown scheme":                        {EnvEndpoint: "grpc://collector.example:4317"},
		"unparsable URL":                        {EnvEndpoint: "https://collector.example/%zz"},
		"ratio above one":                       {EnvEndpoint: endpoint, EnvSampleRatio: "1.5"},
		"ratio below zero":                      {EnvEndpoint: endpoint, EnvSampleRatio: "-0.1"},
		"ratio not a number":                    {EnvEndpoint: endpoint, EnvSampleRatio: "often"},
		"unknown operation":                     {EnvEndpoint: endpoint, EnvSampleAlways: "append,teleport"},
		"username file without a password file": {EnvEndpoint: endpoint, EnvUsernameFile: user},
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			tracing, err := Start(context.Background(), lookupFrom(values), slog.Default())
			if err == nil || tracing != nil {
				t.Fatalf("Start(%v) = (%v, %v), want a startup error", values, tracing, err)
			}
		})
	}
}

func TestStartExportsWithCredentialsFromFiles(t *testing.T) {
	var mu sync.Mutex
	var authorizations, contentTypes []string
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authorizations = append(authorizations, r.Header.Get("Authorization"))
		contentTypes = append(contentTypes, r.Header.Get("Content-Type"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer collector.Close()

	logger, logs := jsonLogger()
	tracing, err := Start(context.Background(), lookupFrom(map[string]string{
		EnvEndpoint:     collector.URL + "/v1/traces", // plain HTTP is allowed to loopback only
		EnvUsernameFile: writeFile(t, "user", "public-key\n"),
		EnvPasswordFile: writeFile(t, "pass", neverLogged),
	}), logger)
	if err != nil {
		t.Fatal(err)
	}
	if !tracing.Enabled() {
		reason, _ := tracing.FailedOpen()
		t.Fatalf("tracing disabled: %s: %s", reason, logs.String())
	}
	_, span := tracing.Tracer().Start(context.Background(), correlation.SpanName(correlation.OperationAppend))
	span.End()
	if err := tracing.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("public-key:"+neverLogged))
	if len(authorizations) == 0 {
		t.Fatal("no export reached the collector")
	}
	for i := range authorizations {
		if authorizations[i] != want || contentTypes[i] != "application/x-protobuf" {
			t.Fatalf("export %d: Authorization %q Content-Type %q", i, authorizations[i], contentTypes[i])
		}
	}
	if strings.Contains(logs.String(), neverLogged) || strings.Contains(logs.String(), "public-key") {
		t.Fatalf("credential material reached the log: %s", logs.String())
	}
	records := logRecords(t, logs)
	if len(records) != 1 || records[0]["event"] != "tracing_enabled" {
		t.Fatalf("log = %s, want one tracing_enabled record", logs.String())
	}
}

// newTestTracing builds a Tracing over an in-memory exporter that samples
// every root.
func newTestTracing(t *testing.T) (*Tracing, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(newSampler(1)))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return newTracing(provider, slog.Default()), exporter
}

func TestHandlerTracesTheRequestWithoutItsPathOrQuery(t *testing.T) {
	tracing, exporter := newTestTracing(t)
	var seen trace.SpanContext
	handler := tracing.Handler("/v1/stream/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = trace.SpanContextFromContext(r.Context())
		w.Write([]byte("first"))
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush: %v", err)
		}
		w.Write([]byte("second"))
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/stream/tenant/secret-stream?token=never-export", nil)
	req.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Body.String() != "firstsecond" || !rec.Flushed {
		t.Fatal("streaming changed: the response must flush through the traced writer")
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	got := spans[0]
	if got.Name != "chronicle.read" || got.SpanKind != trace.SpanKindServer {
		t.Fatalf("span = %s %v, want a chronicle.read server span", got.Name, got.SpanKind)
	}
	if got.SpanContext.TraceID().String() != "11111111111111111111111111111111" || got.Parent.SpanID().String() != "2222222222222222" {
		t.Fatalf("span %v with parent %v is not a child of the caller's traceparent", got.SpanContext, got.Parent)
	}
	if !seen.Equal(got.SpanContext) {
		t.Fatal("the handler did not run under the span")
	}
	want := map[string]string{"http.request.method": "GET", "http.response.status_code": "200", "http.response.body.size": "11"}
	if len(got.Attributes) != len(want) {
		t.Fatalf("attributes = %v, want exactly %v", got.Attributes, want)
	}
	for _, attr := range got.Attributes {
		if want[string(attr.Key)] != attr.Value.Emit() {
			t.Fatalf("attribute %s = %s, want %q", attr.Key, attr.Value.Emit(), want[string(attr.Key)])
		}
	}
	exported, err := json.Marshal(spans)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-stream", "never-export", "token", "/v1/stream"} {
		if bytes.Contains(exported, []byte(secret)) {
			t.Fatalf("%q was exported: %s", secret, exported)
		}
	}
}

func TestHandlerMarksServerErrors(t *testing.T) {
	tracing, exporter := newTestTracing(t)
	handler := tracing.Handler("/v1/stream/", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/stream/a", nil))
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "chronicle.append" || spans[0].Status.Code != codes.Error {
		t.Fatalf("spans = %+v, want one chronicle.append with an error status", spans)
	}
}

func TestOperationFor(t *testing.T) {
	cases := []struct{ method, path, want string }{
		{http.MethodGet, "/v1/stream/tenant/a", correlation.OperationRead},
		{http.MethodHead, "/v1/stream/tenant/a", correlation.OperationRead},
		{http.MethodPost, "/v1/stream/tenant/a", correlation.OperationAppend},
		{http.MethodPut, "/v1/stream/tenant/a", correlation.OperationCreate},
		{http.MethodDelete, "/v1/stream/tenant/a", correlation.OperationDelete},
		{http.MethodPost, "/v1/stream/__ds/subscriptions", correlation.OperationSubscription},
		{http.MethodPost, "/v1/stream/__ds/subscriptions/s1/callback", correlation.OperationSubscription},
		{http.MethodGet, "/v1/stream/__ds/jwks.json", correlation.OperationSubscription},
		{http.MethodOptions, "/v1/stream/tenant/a", correlation.OperationOther},
		{http.MethodPatch, "/v1/stream/tenant/a", correlation.OperationOther},
		{http.MethodGet, "/dsui-config.json", correlation.OperationOther},
		{http.MethodGet, "/v1/streams/a", correlation.OperationOther},
	}
	for _, tc := range cases {
		if got := operationFor("/v1/stream/", httptest.NewRequest(tc.method, tc.path, nil)); got != tc.want {
			t.Errorf("operationFor(%s %s) = %s, want %s", tc.method, tc.path, got, tc.want)
		}
	}
}
