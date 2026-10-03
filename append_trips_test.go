package chronicle

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/internal/redistest"
	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// warmEveryMaster calls warm with a stream name whose slot lives on each
// cluster master in turn. A stream's keys hash by the {path} tag, so
// MasterForKey on that tag names the master the stream's appends run on.
func warmEveryMaster(t *testing.T, cc *goredis.ClusterClient, path func(string) string, warm func(string)) {
	t.Helper()
	var (
		mu      sync.Mutex // ForEachMaster visits the masters concurrently
		masters = map[string]bool{}
	)
	err := cc.ForEachMaster(context.Background(), func(_ context.Context, node *goredis.Client) error {
		mu.Lock()
		defer mu.Unlock()
		masters[node.Options().Addr] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	warmed := map[string]bool{}
	for n := 0; len(warmed) < len(masters); n++ {
		if n == 16384 { // one name per slot would have hit every master
			t.Fatalf("warmed %d of %d masters: a master owns no slot", len(warmed), len(masters))
		}
		name := "warm-" + strconv.Itoa(n)
		master, err := cc.MasterForKey(context.Background(), "{"+path(name)+"}")
		if err != nil {
			t.Fatal(err)
		}
		if addr := master.Options().Addr; !warmed[addr] {
			warmed[addr] = true
			warm(name)
		}
	}
}

// TestHandleAppendRoundTrips pins the serial Redis round trips a POST spends
// on real Redis: the metadata pipeline, then the atomic append script, with
// the live write-fence pre-check between them for a claim-scoped write. Every
// round trip is a full network latency in a remote region, so the sequence is
// the latency budget of the hottest request. The cases run on the standalone
// test Redis and, when REDIS_CLUSTER_ADDRS is set, on that cluster too (the
// pre-check lives in another slot, so there it is another node).
func TestHandleAppendRoundTrips(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Redis integration test in -short mode")
	}
	t.Run("standalone", func(t *testing.T) {
		rawURL := os.Getenv("CHRONICLE_ITEST_REDIS_URL")
		if rawURL == "" {
			rawURL = "redis://localhost:6379/13"
		}
		options, err := goredis.ParseURL(rawURL)
		if err != nil {
			t.Fatalf("parse Redis URL: %v", err)
		}
		runHandleAppendRoundTrips(t, func() goredis.UniversalClient { return goredis.NewClient(options) })
	})
	t.Run("cluster", func(t *testing.T) {
		raw := os.Getenv("REDIS_CLUSTER_ADDRS")
		if raw == "" {
			t.Skip("REDIS_CLUSTER_ADDRS is required for Redis Cluster integration")
		}
		addrs := strings.Split(raw, ",")
		runHandleAppendRoundTrips(t, func() goredis.UniversalClient {
			return goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs})
		})
	})
}

func runHandleAppendRoundTrips(t *testing.T, newClient func() goredis.UniversalClient) {
	client := newClient()
	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		t.Skipf("redis unreachable: %v", err)
	}
	trips := &redistest.TripLog{}
	client.AddHook(trips)
	// side moves tails behind the measured request's back; it is not logged.
	side := newClient()
	t.Cleanup(func() {
		_ = client.Close()
		_ = side.Close()
	})

	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	data := redisstore.New(client, redisstore.Options{Logger: quiet})
	sideData := redisstore.New(side, redisstore.Options{Logger: quiet})

	// Open-class handler without authentication, and the write-fenced stack
	// (enforce mode, write-token authorizer) sharing the one logged client,
	// as newRedisFenceStack builds it.
	h := testHandler(time.Second, time.Second)
	h.Store = data
	subStore := webhook.NewRedisStore(client)
	creds, err := auth.ParseServiceBearerConfig("agents-server:" + tb4SvcBearer)
	if err != nil {
		t.Fatal(err)
	}
	serviceAccess := &ServiceAuth{
		Credentials:            creds,
		TrustedSPIFFEIDs:       []string{tb4AgentsID},
		AllowXFCCWithoutMarker: true,
		Policies:               gatewayPolicies(t, "agents-server", tb4AgentsID),
	}
	mgr, err := webhook.NewManager(subStore, redisFenceStreamAdapter{streamAdapter{st: data, rs: data}}, webhook.ManagerOptions{
		StreamRootURL: "http://x/v1/stream/",
		ServiceAccess: serviceAccess,
		Logger:        quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	fh := testHandler(time.Second, time.Second)
	fh.Store = data
	fh.AuthMode = auth.ModeEnforce
	fh.AppendAuth = mgr.WriteAuthorizer()
	fh.ServiceAuth = serviceAccess
	rt := webhook.NewRoutes(mgr)

	// Run-unique names: no flush is needed, on a cluster or otherwise.
	run := strconv.FormatInt(time.Now().UnixNano(), 10)
	path := func(name string) string { return "/trips/" + run + "/" + name }
	fenced := "/events/" + run
	subID := "s-" + run
	mustCreate(t, h, path("plain"), "text/plain", nil)
	mustCreate(t, h, path("prod"), "text/plain", nil)
	mustCreate(t, h, path("json"), "application/json", nil)
	mustCreate(t, h, path("moved"), "text/plain", nil)
	mustCreate(t, h, path("mismatch"), "application/json", nil)
	if _, _, err := data.Create(fenced, store.CreateOptions{ContentType: "application/json", WriteFence: true}); err != nil {
		t.Fatal(err)
	}
	// Claim with a lease that outlives the test, so the fenced POST is a
	// live claim-scoped write and not a lapsed one.
	cfg := webhook.Config{Type: webhook.DispatchPullWake, Pattern: "events/*", WakeStream: "wake/pool", LeaseTTLMs: 60_000}
	if _, err := subStore.CreateOrConfirm(subID, cfg, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := subStore.Link(subID, strings.TrimPrefix(fenced, "/"), webhook.LinkGlob, "0000000000000000_0000000000000000"); err != nil {
		t.Fatal(err)
	}
	claimRec := httptest.NewRecorder()
	claimReq := httptest.NewRequest(http.MethodPost, "/__ds/subscriptions/"+subID+"/claim", strings.NewReader(`{"worker":"worker-A"}`))
	if !rt.HandleRequest(claimRec, claimReq) || claimRec.Code != http.StatusOK {
		t.Fatalf("claim = %d %q", claimRec.Code, claimRec.Body.String())
	}
	var claim webhook.ClaimResponse
	if err := json.Unmarshal(claimRec.Body.Bytes(), &claim); err != nil || claim.WriteToken == "" {
		t.Fatalf("claim response %q: %v", claimRec.Body.String(), err)
	}
	generation := strconv.FormatInt(claim.Generation, 10)
	fencedHeaders := func(seq string) map[string]string {
		return map[string]string{
			"Content-Type": "application/json", WriteTokenHeader: claim.WriteToken,
			"Producer-Id": "entity-" + run, "Producer-Epoch": generation, "Producer-Seq": seq,
		}
	}

	// Warm the script cache so no EVALSHA falls back to EVAL mid-count. Script
	// caches are per node, so on a cluster one append must land on every master.
	warm := func(name string) {
		mustCreate(t, h, path(name), "text/plain", nil)
		mustAppend(t, h, path(name), "text/plain", []byte("w"))
	}
	if cc, ok := client.(*goredis.ClusterClient); ok {
		warmEveryMaster(t, cc, path, warm)
	} else {
		warm("warm")
	}
	if rec := do(fh, http.MethodPost, fenced, fencedHeaders("0"), []byte(`{"warm":1}`)); rec.Code != http.StatusOK {
		t.Fatalf("warm fenced append = %d %q", rec.Code, rec.Body.String())
	}

	const (
		pipe     = "pipe(hgetall+hgetall)"
		script   = "evalsha:append"
		precheck = "evalsha:control"
	)
	plain := map[string]string{"Content-Type": "text/plain"}
	producer := func(seq string) map[string]string {
		return map[string]string{"Content-Type": "text/plain", "Producer-Id": "p", "Producer-Epoch": "0", "Producer-Seq": seq}
	}
	cases := []struct {
		name         string
		handler      *Handler
		path         string
		headers      map[string]string
		body         []byte
		beforeScript func()
		wantStatus   int
		wantTrips    []string
	}{
		{
			name: "plain", handler: h, path: path("plain"), headers: plain, body: []byte("hello"),
			wantStatus: http.StatusNoContent, wantTrips: []string{pipe, script},
		},
		{
			name: "producer new seq", handler: h, path: path("prod"), headers: producer("0"), body: []byte("hello"),
			wantStatus: http.StatusOK, wantTrips: []string{pipe, script},
		},
		{
			name: "producer duplicate", handler: h, path: path("prod"), headers: producer("0"), body: []byte("hello"),
			wantStatus: http.StatusNoContent, wantTrips: []string{pipe, script},
		},
		{
			name: "json", handler: h, path: path("json"), headers: map[string]string{"Content-Type": "application/json"}, body: []byte(`{"a":1}`),
			wantStatus: http.StatusNoContent, wantTrips: []string{pipe, script},
		},
		{
			name: "write-fenced with a live claim token", handler: fh, path: fenced, headers: fencedHeaders("1"), body: []byte(`{"turn":1}`),
			wantStatus: http.StatusOK, wantTrips: []string{pipe, precheck, script},
		},
		{
			// RETRY carries the live tail, so the second attempt is the script
			// again: one round trip per retry.
			name: "tail moved before the script", handler: h, path: path("moved"), headers: plain, body: []byte("hello"),
			beforeScript: func() {
				if _, err := sideData.Append(path("moved"), []byte("concurrent"), store.AppendOptions{ContentType: "text/plain"}); err != nil {
					t.Errorf("side append: %v", err)
				}
			},
			wantStatus: http.StatusNoContent, wantTrips: []string{pipe, script, script},
		},
		{
			name: "first append after create", handler: h, path: path("first"), headers: plain, body: []byte("hello"),
			wantStatus: http.StatusNoContent, wantTrips: []string{pipe, script},
		},
		{
			name: "content-type mismatch", handler: h, path: path("mismatch"), headers: plain, body: []byte("hello"),
			wantStatus: http.StatusConflict, wantTrips: []string{pipe},
		},
		{
			name: "missing stream", handler: h, path: path("missing"), headers: plain, body: []byte("hello"),
			wantStatus: http.StatusNotFound, wantTrips: []string{pipe},
		},
	}
	mustCreate(t, h, path("first"), "text/plain", nil)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trips.Take()
			trips.BeforeAppendScript(tc.beforeScript)
			rec := do(tc.handler, http.MethodPost, tc.path, tc.headers, tc.body)
			got := trips.Take()
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d body %q, want %d", rec.Code, rec.Body.String(), tc.wantStatus)
			}
			if !reflect.DeepEqual(got, tc.wantTrips) {
				t.Errorf("round trips %v, want %v", got, tc.wantTrips)
			}
			if rec.Code == http.StatusNoContent || rec.Code == http.StatusOK {
				// The echoed next offset is the live tail: the hint changed
				// where the frames were built, never what was committed.
				meta, err := data.Get(tc.path)
				if err != nil {
					t.Fatal(err)
				}
				if got := rec.Header().Get(protocol.HeaderStreamNextOffset); got != meta.CurrentOffset.String() {
					t.Errorf("%s = %q, live tail %s", protocol.HeaderStreamNextOffset, got, meta.CurrentOffset)
				}
			}
		})
	}
}
