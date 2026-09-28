package correlation

import (
	"context"
	"strings"
	"testing"
)

func TestValid(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  bool
	}{
		{"uuid", "9b2f4c1e-3d6a-4b7f-8e21-0f5d6a7b8c9d", true},
		{"single alphanumeric", "7", true},
		{"every allowed punctuation after the first byte", "a._:-Z9", true},
		{"exactly max length", strings.Repeat("a", MaxLength), true},
		{"empty", "", false},
		{"one over max length", strings.Repeat("a", MaxLength+1), false},
		{"leading hyphen", "-abc", false},
		{"leading dot", ".abc", false},
		{"leading colon", ":abc", false},
		{"internal space", "req 123", false},
		{"tab", "req\t123", false},
		{"header injection", "req\r\nX-Other: 1", false},
		{"slash", "req/123", false},
		{"multi-byte", "réq", false},
		{"quote", `req"123`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Valid(tc.value); got != tc.want {
				t.Fatalf("Valid(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	t.Run("preserves a safe value", func(t *testing.T) {
		if got := Normalize("gateway-request-123"); got != "gateway-request-123" {
			t.Fatalf("Normalize = %q, want the caller value", got)
		}
	})
	t.Run("trims surrounding whitespace before judging", func(t *testing.T) {
		if got := Normalize("  gateway-request-123 \t"); got != "gateway-request-123" {
			t.Fatalf("Normalize = %q, want the trimmed caller value", got)
		}
	})
	for _, unsafe := range []string{"", "unsafe request id", "-leading", strings.Repeat("x", MaxLength+1), "a\nb"} {
		t.Run("replaces "+strings.ReplaceAll(unsafe, "\n", `\n`), func(t *testing.T) {
			got := Normalize(unsafe)
			if got == unsafe || !Valid(got) {
				t.Fatalf("Normalize(%q) = %q, want a fresh valid id", unsafe, got)
			}
		})
	}
	t.Run("fresh ids are distinct", func(t *testing.T) {
		if a, b := Normalize(""), Normalize(""); a == b {
			t.Fatalf("two fresh ids collided: %q", a)
		}
	})
}

func TestContextRoundTrip(t *testing.T) {
	if got := RequestID(context.Background()); got != "" {
		t.Fatalf("RequestID on a bare context = %q, want empty", got)
	}
	ctx := WithRequestID(context.Background(), "gateway-request-123")
	if got := RequestID(ctx); got != "gateway-request-123" {
		t.Fatalf("RequestID = %q, want the stored value", got)
	}
	ctx = WithRequestID(context.Background(), "unsafe value")
	if got := RequestID(ctx); got == "unsafe value" || !Valid(got) {
		t.Fatalf("a context must never carry an unsafe id, got %q", got)
	}
}

func TestWakeRequestID(t *testing.T) {
	if got := WakeRequestID("w_abc123"); got != "wake-w_abc123" {
		t.Fatalf("WakeRequestID = %q, want the stable wake- prefix", got)
	}
	// A wake id the grammar cannot carry still yields a valid, fresh id rather
	// than an invalid one leaking into logs and headers.
	if got := WakeRequestID("bad wake/id"); !Valid(got) || strings.HasPrefix(got, "wake-") {
		t.Fatalf("WakeRequestID on an unsafe wake id = %q, want a fresh valid id", got)
	}
}

func TestCheckHeaderName(t *testing.T) {
	cases := []struct {
		name    string
		wantErr bool
	}{
		{DefaultHeader, false},
		{"My-Platform-Request-ID", false},
		{"x-request-id", false},
		{"Stream", false}, // the protocol families are prefixes with the dash
		{"", true},
		{"X Request ID", true},
		{"X-Request-ID:", true},
		{"X-Request-ID\r\n", true},
		{"X-Réquest", true},
		// Reserved: Chronicle reads or writes these for another purpose.
		{"Authorization", true},
		{"authorization", true},
		{"Cookie", true},
		{"Content-Type", true},
		{"Content-Length", true},
		{"Host", true},
		{"traceparent", true},
		{"TraceState", true},
		{"X-Forwarded-Client-Cert", true},
		{"electric-claim-token", true},
		{"Stream-Seq", true},
		{"producer-id", true},
		{"Write-Token", true},
		{"Webhook-Signature", true},
	}
	for _, tc := range cases {
		err := CheckHeaderName(tc.name)
		if (err != nil) != tc.wantErr {
			t.Errorf("CheckHeaderName(%q) = %v, want error %v", tc.name, err, tc.wantErr)
		}
	}
}
