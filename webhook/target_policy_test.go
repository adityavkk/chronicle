package webhook

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The egress seam tests drive the Manager's real create and delivery paths
// against an httptest receiver reached only through an injected client, and
// record every policy call and receiver hit in one ordered log.

const (
	admittedHost = "hook.internal.test"
	deniedHost   = "other.internal.test"
)

// egressLog records the seam's observable events — policy calls and receiver
// hits — in the order they happen.
type egressLog struct {
	mu     sync.Mutex
	events []string
}

func (l *egressLog) add(event string) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *egressLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events)
}

func (l *egressLog) received() int {
	n := 0
	for _, event := range l.snapshot() {
		if strings.HasPrefix(event, "receive:") {
			n++
		}
	}
	return n
}

// recordingTargetPolicy admits exactly admittedHost and stamps a routing
// header on every request it prepares; prepareErr, when set, rejects the
// delivery instead.
type recordingTargetPolicy struct {
	log        *egressLog
	prepareErr error
}

func (p *recordingTargetPolicy) AllowTarget(target *url.URL) bool {
	p.log.add("allow:" + target.Host)
	return target.Host == admittedHost
}

func (p *recordingTargetPolicy) PrepareRequest(req *http.Request) error {
	p.log.add("prepare:" + req.URL.Host)
	if p.prepareErr != nil {
		return p.prepareErr
	}
	req.Header.Set("X-Test-Route", "admitted")
	return nil
}

// newEgressReceiver is the webhook receiver behind the injected client. Each
// hit is logged with the host, the policy's routing header, and whether the
// envelope signature arrived; the first failures hits answer 500 so a retry is
// exercised, the rest 204.
func newEgressReceiver(t *testing.T, log *egressLog, failures int32) *httptest.Server {
	t.Helper()
	var remaining atomic.Int32
	remaining.Store(failures)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.add(fmt.Sprintf("receive:%s route=%s signed=%v",
			r.Host, r.Header.Get("X-Test-Route"), r.Header.Get("Webhook-Signature") != ""))
		if remaining.Add(-1) >= 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newEgressTestManager builds a Manager whose injected client dials receiver
// for every host — the policy's private name is reachable only through it —
// and whose resolver maps every name to a private address, so the SSRF rules
// alone reject the receiver's URL and only the policy can admit it.
func newEgressTestManager(t *testing.T, policy TargetPolicy, receiver *httptest.Server) (*Manager, *RedisStore) {
	t.Helper()
	store, _ := newTestStore(t)
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, receiver.Listener.Addr().String())
		},
	}
	t.Cleanup(transport.CloseIdleConnections)
	fs := &fakeStreams{tails: map[string]string{"events/a": "0000000000000001_0000000000000000"}}
	mgr, err := NewManager(store, fs, ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
		HTTPClient:    &http.Client{Transport: transport},
		TargetPolicy:  policy,
		Resolver:      func(string) ([]net.IP, error) { return []net.IP{net.ParseIP("10.0.0.7")}, nil },
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mgr, store
}

func webhookBody(host string) string {
	return fmt.Sprintf(`{"type":"webhook","streams":["events/a"],"webhook":{"url":"http://%s/wake"}}`, host)
}

// createAndArm creates the webhook subscription through the router (the
// AllowTarget gate) and arms one wake for the Manager to deliver.
func createAndArm(t *testing.T, rt *Routes, store *RedisStore, id string) ArmResult {
	t.Helper()
	if rec := doCreate(t, rt, id, "", webhookBody(admittedHost)); rec.Code != http.StatusCreated {
		t.Fatalf("create = %d, body %q", rec.Code, rec.Body.String())
	}
	arm, err := store.ArmWakeUnscoped(id, time.Now(), 1000, true, "wake-1")
	if err != nil || !arm.Armed {
		t.Fatalf("arm = %+v, %v", arm, err)
	}
	return arm
}

func retryCountOf(t *testing.T, store *RedisStore, id string) int {
	t.Helper()
	sub, ok, err := store.Get(id)
	if err != nil || !ok {
		t.Fatalf("get %s: ok=%v err=%v", id, ok, err)
	}
	return sub.RetryCount
}

// TestDeliverWebhookConsultsTargetPolicyBeforeTransport is the seam's ordering
// invariant: AllowTarget admits the route once at create, ahead of the SSRF
// rules; PrepareRequest runs on every attempt — the retry included — after the
// envelope is signed and before the POST; and the POST leaves through the
// injected client, which is the only way to reach the receiver.
func TestDeliverWebhookConsultsTargetPolicyBeforeTransport(t *testing.T) {
	log := &egressLog{}
	receiver := newEgressReceiver(t, log, 1)
	mgr, store := newEgressTestManager(t, &recordingTargetPolicy{log: log}, receiver)
	arm := createAndArm(t, NewRoutes(mgr), store, "s1")

	mgr.deliverWebhook("s1", arm.Generation, arm.WakeID, nil)
	if n := retryCountOf(t, store, "s1"); n != 1 {
		t.Fatalf("retry_count after the 500 = %d, want 1", n)
	}
	mgr.deliverWebhook("s1", arm.Generation, arm.WakeID, nil)
	if n := retryCountOf(t, store, "s1"); n != 0 {
		t.Fatalf("retry_count after the retry's 204 = %d, want 0", n)
	}

	delivered := "receive:" + admittedHost + " route=admitted signed=true"
	want := []string{
		"allow:" + admittedHost,
		"prepare:" + admittedHost, delivered,
		"prepare:" + admittedHost, delivered,
	}
	if got := log.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("events = %q\nwant     %q", got, want)
	}
}

// TestCreateRejectsTargetPolicyDeniedURLLikeSSRF is the admission invariant:
// the policy admits exactly its route ahead of the SSRF rules, while any other
// private target — and the same route without a policy — is rejected with the
// SSRF outcome, 400 WEBHOOK_URL_REJECTED, reaching neither the store nor the
// transport.
func TestCreateRejectsTargetPolicyDeniedURLLikeSSRF(t *testing.T) {
	tests := []struct {
		name     string
		policy   bool
		host     string
		wantCode int
	}{
		{name: "policy admits its route", policy: true, host: admittedHost, wantCode: http.StatusCreated},
		{name: "policy denies another private route", policy: true, host: deniedHost, wantCode: http.StatusBadRequest},
		{name: "no policy keeps the SSRF rules", policy: false, host: admittedHost, wantCode: http.StatusBadRequest},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			log := &egressLog{}
			receiver := newEgressReceiver(t, log, 0)
			var policy TargetPolicy
			if test.policy {
				policy = &recordingTargetPolicy{log: log}
			}
			mgr, store := newEgressTestManager(t, policy, receiver)
			id := fmt.Sprintf("s%d", i)
			rec := doCreate(t, NewRoutes(mgr), id, "", webhookBody(test.host))
			if rec.Code != test.wantCode {
				t.Fatalf("create = %d, body %q; want %d", rec.Code, rec.Body.String(), test.wantCode)
			}
			if test.wantCode == http.StatusBadRequest {
				if code := errCodeOf(t, rec); code != ErrCodeWebhookURLRejected {
					t.Fatalf("code = %s, want %s", code, ErrCodeWebhookURLRejected)
				}
				if _, ok, _ := store.Get(id); ok {
					t.Fatal("rejected create reached the store")
				}
			}
			if n := log.received(); n != 0 {
				t.Fatalf("receiver was dialed %d times during create", n)
			}
		})
	}
}

// TestDeliverWebhookFailsAttemptOnPrepareRequestError is the fail-closed
// invariant: a PrepareRequest error fails the attempt before the transport (the
// receiver sees nothing, the normal retry is scheduled), and the retry is
// prepared afresh, so a policy that admits the next attempt lets it through.
func TestDeliverWebhookFailsAttemptOnPrepareRequestError(t *testing.T) {
	log := &egressLog{}
	receiver := newEgressReceiver(t, log, 0)
	policy := &recordingTargetPolicy{log: log, prepareErr: errors.New("route not ready")}
	mgr, store := newEgressTestManager(t, policy, receiver)
	metrics := &fakeMetrics{}
	mgr.metrics = metrics
	arm := createAndArm(t, NewRoutes(mgr), store, "s1")

	mgr.deliverWebhook("s1", arm.Generation, arm.WakeID, nil)
	if n := log.received(); n != 0 {
		t.Fatalf("receiver was dialed %d times after the policy rejected the delivery", n)
	}
	if got := metrics.delivered(); !maps.Equal(got, map[string]int{"rejected": 1}) {
		t.Fatalf("WakeDelivery outcomes after the rejection = %v, want {rejected: 1}", got)
	}
	sub, _, _ := store.Get("s1")
	if sub.RetryCount != 1 || sub.NextAttemptNs == 0 {
		t.Fatalf("retry_count=%d next_attempt_ns=%d after the rejection, want a scheduled retry", sub.RetryCount, sub.NextAttemptNs)
	}

	policy.prepareErr = nil
	mgr.deliverWebhook("s1", arm.Generation, arm.WakeID, nil)
	if n := retryCountOf(t, store, "s1"); n != 0 {
		t.Fatalf("retry_count after the admitted retry = %d, want 0", n)
	}
	if got := metrics.delivered(); !maps.Equal(got, map[string]int{"rejected": 1, "ok": 1}) {
		t.Fatalf("WakeDelivery outcomes after the admitted retry = %v, want {rejected: 1, ok: 1}", got)
	}
	want := []string{
		"allow:" + admittedHost,
		"prepare:" + admittedHost,
		"prepare:" + admittedHost,
		"receive:" + admittedHost + " route=admitted signed=true",
	}
	if got := log.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("events = %q\nwant     %q", got, want)
	}
}
