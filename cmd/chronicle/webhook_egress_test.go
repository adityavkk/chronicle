package main

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// The test-only adapter below registers through the same hook a deployment
// adapter file uses, so the fold is exercised end to end from init().

// testWebhookEgressEnv activates the test adapter: the value is the one origin
// its policy admits, and "invalid" makes the load fail.
const testWebhookEgressEnv = "CHRONICLE_TEST_WEBHOOK_EGRESS_ORIGIN"

type testTargetPolicy struct{ origin string }

func (p testTargetPolicy) AllowTarget(target *url.URL) bool {
	return target.Scheme+"://"+target.Host == p.origin
}

func (testTargetPolicy) PrepareRequest(*http.Request) error { return nil }

func init() {
	webhookEgressLoaders = append(webhookEgressLoaders, webhookEgressLoader{name: "test", load: loadTestWebhookEgress})
}

func loadTestWebhookEgress(lookup func(string) (string, bool)) (webhookEgress, error) {
	origin, ok := lookup(testWebhookEgressEnv)
	if !ok {
		return webhookEgress{}, nil
	}
	if origin == "invalid" {
		return webhookEgress{}, errors.New("test webhook egress: invalid origin")
	}
	return webhookEgress{policy: testTargetPolicy{origin: origin}, client: &http.Client{}}, nil
}

func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// TestLoadWebhookEgressFoldsRegisteredAdapter is the registration contract: an
// adapter appended to webhookEgressLoaders from init() is folded by
// loadWebhookEgress — inactive while its configuration is unset, active (policy,
// client, name) once set, a startup error when invalid, and refused alongside
// -webhook-allow-private, which stays available on its own.
func TestLoadWebhookEgressFoldsRegisteredAdapter(t *testing.T) {
	configured := map[string]string{testWebhookEgressEnv: "http://receiver:8080"}
	tests := []struct {
		name         string
		values       map[string]string
		allowPrivate bool
		wantActive   bool
		wantErr      string
	}{
		{name: "unset is inactive"},
		{name: "configured is active", values: configured, wantActive: true},
		{name: "invalid configuration refuses startup", values: map[string]string{testWebhookEgressEnv: "invalid"}, wantErr: "invalid origin"},
		{name: "broad private mode is refused", values: configured, allowPrivate: true, wantErr: "-webhook-allow-private"},
		{name: "broad private mode alone stays available", allowPrivate: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			egress, err := loadWebhookEgress(test.allowPrivate, lookupFrom(test.values))
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("err = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if active := egress.policy != nil; active != test.wantActive {
				t.Fatalf("active = %v, want %v (%#v)", active, test.wantActive, egress)
			}
			if test.wantActive && (egress.client == nil || egress.name != "test") {
				t.Fatalf("active egress = %#v, want the test adapter's client and name", egress)
			}
		})
	}
}

// TestFoldWebhookEgressRefusesTwoActiveAdapters is the at-most-one invariant:
// among several registered adapters the fold returns the single configured one
// whatever its position, refuses two configured ones by name, and propagates a
// loader error.
func TestFoldWebhookEgressRefusesTwoActiveAdapters(t *testing.T) {
	active := func(name string) webhookEgressLoader {
		return webhookEgressLoader{name: name, load: func(func(string) (string, bool)) (webhookEgress, error) {
			return webhookEgress{policy: testTargetPolicy{}}, nil
		}}
	}
	inactive := webhookEgressLoader{name: "inactive", load: func(func(string) (string, bool)) (webhookEgress, error) {
		return webhookEgress{}, nil
	}}
	failing := webhookEgressLoader{name: "failing", load: func(func(string) (string, bool)) (webhookEgress, error) {
		return webhookEgress{}, errors.New("failing adapter: bad configuration")
	}}
	tests := []struct {
		name     string
		loaders  []webhookEgressLoader
		wantName string
		wantErr  string
	}{
		{name: "none registered"},
		{name: "one inactive", loaders: []webhookEgressLoader{inactive}},
		{name: "active after inactive", loaders: []webhookEgressLoader{inactive, active("a")}, wantName: "a"},
		{name: "active before inactive", loaders: []webhookEgressLoader{active("a"), inactive}, wantName: "a"},
		{name: "two active", loaders: []webhookEgressLoader{active("a"), inactive, active("b")}, wantErr: "a and b webhook egress adapters are mutually exclusive"},
		{name: "loader error", loaders: []webhookEgressLoader{active("a"), failing}, wantErr: "bad configuration"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			egress, err := foldWebhookEgress(test.loaders, false, lookupFrom(nil))
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("err = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if egress.name != test.wantName || (egress.policy != nil) != (test.wantName != "") {
				t.Fatalf("egress = %#v, want adapter %q", egress, test.wantName)
			}
		})
	}
}
