package main

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestParsePrometheusMetric(t *testing.T) {
	body := `
# HELP chronicle_sse_subscription_events_total test
chronicle_sse_subscription_events_total{event="opened"} 3
chronicle_sse_subscription_events_total{event="reconnect"} 2
chronicle_sse_subscription_events_total{event="reconnect"} 4
chronicle_sse_subscription_events_total_created{event="reconnect"} 99
`
	if got := parsePrometheusMetric(
		body,
		"chronicle_sse_subscription_events_total",
		`event="reconnect"`,
	); got != 6 {
		t.Fatalf("reconnect total = %v, want 6", got)
	}
}

func TestInClusterResumingReaderShellPausesThenRecordsVerifiedReconnect(t *testing.T) {
	shell := inClusterResumingReaderShell("events/sse-123")
	for _, want := range []string{
		"GET /v1/stream/events/sse-123?offset=$offset&live=sse",
		"nc chronicle 4437",
		"sleep 2; awk",
		`status == 200 && start > 0`,
		`print session " " offset " ready"`,
		slowReaderStateFile,
		">/tmp/chronicle-sse-slow.log",
	} {
		if !strings.Contains(shell, want) {
			t.Fatalf("slow-reader shell omitted %q:\n%s", want, shell)
		}
	}
	if out, err := exec.Command("sh", "-n", "-c", shell).CombinedOutput(); err != nil {
		t.Fatalf("resuming-reader shell syntax: %v: %s", err, out)
	}
	if strings.Contains(shell, `echo "$session $offset"`) {
		t.Fatalf("shell records session state before response validation:\n%s", shell)
	}
}

func TestSlowReaderControlAWKRequiresHTTP200AndControl(t *testing.T) {
	const offset = "0000000000000000_0000000000000042"
	cases := []struct {
		name      string
		response  string
		wantState bool
	}{
		{
			name:      "error status with control",
			response:  "HTTP/1.1 500 Internal Server Error\r\n\r\ndata: {\"streamNextOffset\":\"" + offset + "\"}\n",
			wantState: false,
		},
		{
			name:      "success status without control",
			response:  "HTTP/1.1 200 OK\r\n\r\nevent: data\ndata: [{\"n\":1}]\n",
			wantState: false,
		},
		{
			name:      "success status and control",
			response:  "HTTP/1.1 200 OK\r\n\r\nevent: control\ndata: {\"streamNextOffset\":\"" + offset + "\"}\n",
			wantState: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := t.TempDir() + "/state"
			cmd := exec.Command("awk", slowReaderControlAWK(2, state))
			cmd.Stdin = strings.NewReader(tc.response)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("awk failed: %v: %s", err, out)
			}
			raw, err := os.ReadFile(state)
			if !tc.wantState {
				if err == nil {
					t.Fatalf("unverified response wrote state %q", raw)
				}
				if !os.IsNotExist(err) {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got, want := strings.TrimSpace(string(raw)), "2 "+offset+" ready"; got != want {
				t.Fatalf("state = %q, want %q", got, want)
			}
		})
	}
}

func TestParseSlowReaderStateFailsClosed(t *testing.T) {
	const offset = "0000000000000000_0000000000000042"
	if session, gotOffset, err := parseSlowReaderState("2 " + offset + " ready"); err != nil ||
		session != 2 || gotOffset != offset {
		t.Fatalf("valid state = %d %q, %v", session, gotOffset, err)
	}
	for _, raw := range []string{
		"2 " + offset,
		"2 -1 ready",
		"2 not-an-offset ready",
		"pending " + offset + " ready",
		"2 " + offset + " connecting",
	} {
		if _, _, err := parseSlowReaderState(raw); err == nil {
			t.Fatalf("invalid state %q was accepted", raw)
		}
	}
}

func TestInClusterStuckReaderShellUsesDirectUnreadPipe(t *testing.T) {
	shell := inClusterStuckReaderShell("events/sse-123")
	for _, want := range []string{
		"GET /v1/stream/events/sse-123?offset=-1&live=sse",
		"nc chronicle 4437",
		"| sleep 120",
		">/tmp/chronicle-sse-slow.log",
	} {
		if !strings.Contains(shell, want) {
			t.Fatalf("stuck-reader shell omitted %q:\n%s", want, shell)
		}
	}
	if out, err := exec.Command("sh", "-n", "-c", shell).CombinedOutput(); err != nil {
		t.Fatalf("stuck-reader shell syntax: %v: %s", err, out)
	}
}

func TestHighestMetricPodSelectsReadyClientServingReplica(t *testing.T) {
	pods := []kubePod{
		{name: "empty", ready: true},
		{name: "busy", ready: true},
		{name: "unready", ready: false},
	}
	selected := highestMetricPod(pods, map[string]float64{
		"empty":   0,
		"busy":    7,
		"unready": 99,
	})
	if selected == nil || selected.name != "busy" {
		t.Fatalf("selected = %#v, want busy", selected)
	}
}

func TestSinglePodMetricIncreaseRequiresOneStablePod(t *testing.T) {
	before := map[string]podMetricSample{
		"a": {pod: kubePod{name: "a", uid: "uid-a", ready: true}, value: 4},
		"b": {pod: kubePod{name: "b", uid: "uid-b", ready: true}, value: 5},
	}
	after := map[string]podMetricSample{
		"a": {pod: kubePod{name: "a", uid: "uid-a", ready: true}, value: 5},
		"b": {pod: kubePod{name: "b", uid: "uid-b", ready: true}, value: 5},
	}
	pod, value, err := singlePodMetricIncrease(before, after)
	if err != nil || pod.name != "a" || value != 5 {
		t.Fatalf("single increase = %#v, %v, %v", pod, value, err)
	}

	noIncrease := map[string]podMetricSample{
		"a": {pod: kubePod{name: "a", uid: "uid-a", ready: true}, value: 4},
		"b": {pod: kubePod{name: "b", uid: "uid-b", ready: true}, value: 5},
	}
	if _, _, err := singlePodMetricIncrease(before, noIncrease); !errors.Is(err, errNoPodMetricIncrease) {
		t.Fatalf("no increase error = %v", err)
	}

	ambiguous := map[string]podMetricSample{
		"a": {pod: kubePod{name: "a", uid: "uid-a", ready: true}, value: 5},
		"b": {pod: kubePod{name: "b", uid: "uid-b", ready: true}, value: 6},
	}
	if _, _, err := singlePodMetricIncrease(before, ambiguous); err == nil {
		t.Fatal("two increased pods were accepted")
	}

	jumped := map[string]podMetricSample{
		"a": {pod: kubePod{name: "a", uid: "uid-a", ready: true}, value: 6},
		"b": {pod: kubePod{name: "b", uid: "uid-b", ready: true}, value: 5},
	}
	if _, _, err := singlePodMetricIncrease(before, jumped); err == nil {
		t.Fatal("metric increase larger than one client was accepted")
	}

	replaced := map[string]podMetricSample{
		"a": {pod: kubePod{name: "a", uid: "replacement", ready: true}, value: 5},
		"b": {pod: kubePod{name: "b", uid: "uid-b", ready: true}, value: 5},
	}
	if _, _, err := singlePodMetricIncrease(before, replaced); err == nil {
		t.Fatal("replacement pod was accepted as the serving pod")
	}
}

func TestTimeoutAndClientDropObservedRequiresBothTransitions(t *testing.T) {
	if !timeoutAndClientDropObserved(3, 4, 8, 7) {
		t.Fatal("counter increase plus client drop was rejected")
	}
	if timeoutAndClientDropObserved(3, 3, 8, 7) {
		t.Fatal("client drop without timeout increase was accepted")
	}
	if timeoutAndClientDropObserved(3, 4, 8, 8) {
		t.Fatal("timeout increase without client drop was accepted")
	}
}
