package chronicle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
)

func newSnapshotTestHandler(t *testing.T, st store.Store) *Handler {
	t.Helper()
	creds, err := auth.ParseServiceBearerConfig("worker:worker-test-token,reader:reader-test-token,writer:writer-test-token")
	if err != nil {
		t.Fatal(err)
	}
	policies, err := auth.NewServicePolicies([]auth.ServicePolicyConfig{
		{Identity: "worker", Actions: []auth.Action{auth.ActionRead, auth.ActionCreate, auth.ActionAppend, auth.ActionDelete, auth.ActionSnapshotPublish}, Namespaces: []string{"agents"}},
		{Identity: "reader", Actions: []auth.Action{auth.ActionRead}, Namespaces: []string{"agents"}},
		{Identity: "writer", Actions: []auth.Action{auth.ActionRead, auth.ActionCreate, auth.ActionAppend}, Namespaces: []string{"agents"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &Handler{
		Store: st, EnableSnapshots: true, AuthMode: auth.ModeEnforce,
		ServiceAuth:     &ServiceAuth{Credentials: creds, Policies: policies},
		LongPollTimeout: 10 * time.Millisecond, SSEReconnectInterval: 20 * time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// Recreate immediately after the first atomic page, not before the request.
// This catches implementations that guard only a HEAD/preflight or fail to
// preserve the captured incarnation across wait, pagination, and live attach.
type snapshotRecreateStore struct {
	*store.MemoryStore
	afterFirst func()
}

func (s *snapshotRecreateStore) ReadPage(ctx context.Context, path string, offset store.Offset, opts store.ReadPageOptions) (store.ReadPage, error) {
	page, err := s.MemoryStore.ReadPage(ctx, path, offset, opts)
	if s.afterFirst != nil {
		fn := s.afterFirst
		s.afterFirst = nil
		fn()
	}
	return page, err
}

func TestSnapshotGuardFencesInFlightRecreation(t *testing.T) {
	for _, mode := range []string{"catchup", "long-poll", "sse"} {
		t.Run(mode, func(t *testing.T) {
			s := &snapshotRecreateStore{MemoryStore: store.NewMemoryStore()}
			defer s.Close()
			meta, _, err := s.Create("/agents/test", store.CreateOptions{ContentType: "application/json", InitialData: []byte(`[1,22]`)})
			if err != nil {
				t.Fatal(err)
			}
			inc := meta.Incarnation
			s.afterFirst = func() {
				if err := s.Delete("/agents/test"); err != nil {
					t.Fatal(err)
				}
				if _, _, err := s.Create("/agents/test", store.CreateOptions{ContentType: "application/json", InitialData: []byte(`[999]`), Closed: true}); err != nil {
					t.Fatal(err)
				}
			}
			h := newSnapshotTestHandler(t, s)
			h.ReadPageBytes = 1
			query := "?offset=-1"
			switch mode {
			case "sse":
				query += "&live=sse"
			case "long-poll":
				query = "?offset=now&live=long-poll"
			}
			r := httptest.NewRequest("GET", "/agents/test"+query, nil)
			r.Header.Set("Authorization", "Bearer reader-test-token")
			r.Header.Set(protocol.HeaderIfStreamIncarnation, inc)
			w := httptest.NewRecorder()
			switch mode {
			case "long-poll":
				h.ServeHTTP(w, r)
				requireSnapshotStatus(t, w, 412)
			case "sse":
				// The hub may observe recreation before the header flush (412)
				// or after it (committed-response abort). Neither may serve the
				// replacement or quietly complete a successful response.
				var aborted any
				func() {
					defer func() { aborted = recover() }()
					h.ServeHTTP(w, r)
				}()
				if aborted == nil {
					requireSnapshotStatus(t, w, 412)
				} else {
					abortErr, ok := aborted.(error)
					if !ok || !errors.Is(abortErr, http.ErrAbortHandler) {
						t.Fatalf("unexpected panic: %v", aborted)
					}
					requireSnapshotStatus(t, w, 200)
				}
			default:
				serveExpectAbort(t, h, w, r)
			}
			if strings.Contains(w.Body.String(), "999") {
				t.Fatalf("guarded read emitted replacement source data: %s", w.Body.String())
			}
		})
	}
}

func snapshotPublicationHeaders(incarnation, offset, etag string, body []byte) map[string]string {
	digest := sha256.Sum256(body)
	headers := map[string]string{
		"Authorization": "Bearer worker-test-token", "Content-Type": "application/json",
		protocol.HeaderStreamSnapshot: "v1", protocol.HeaderIfStreamIncarnation: incarnation,
		protocol.HeaderStreamSnapshotOffset: offset,
		protocol.HeaderContentDigest:        "sha-256=:" + base64.StdEncoding.EncodeToString(digest[:]) + ":",
	}
	if etag == "" {
		headers["If-None-Match"] = "*"
	} else {
		headers["If-Match"] = etag
	}
	return headers
}

func requireSnapshotStatus(t *testing.T, response *httptest.ResponseRecorder, want int) {
	t.Helper()
	if response.Code != want {
		t.Fatalf("HTTP status = %d, want %d; body=%s", response.Code, want, response.Body.String())
	}
}

// Exercise the entire public contract against both real backends. The fold is
// intentionally order-sensitive: replaying the prefix twice or skipping a tail
// page cannot accidentally produce the independently calculated answer 116.
func TestSnapshotHTTPEndToEnd(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			var st store.Store = store.NewMemoryStore()
			if backend == "redis" {
				if testing.Short() {
					t.Skip("Redis integration")
				}
				raw := os.Getenv("CHRONICLE_SNAPSHOT_REDIS_URL")
				if raw == "" {
					raw = "redis://localhost:6379/11"
				}
				opts, err := goredis.ParseURL(raw)
				if err != nil {
					t.Fatal("invalid snapshot test Redis URL")
				}
				client := goredis.NewClient(opts)
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				err = client.Ping(ctx).Err()
				cancel()
				if err != nil {
					_ = client.Close()
					t.Skip("snapshot test Redis unavailable")
				}
				st = redisstore.New(client, redisstore.Options{})
			}
			t.Cleanup(func() { _ = st.Close() })
			h := newSnapshotTestHandler(t, st)
			hooks := &hookRecorder{}
			h.SubHooks = hooks
			path := fmt.Sprintf("/agents/snapshot-%d", time.Now().UnixNano())
			t.Cleanup(func() { _ = st.Delete(path) })
			headers := map[string]string{"Authorization": "Bearer worker-test-token", "Content-Type": "application/json"}
			requireSnapshotStatus(t, do(h, "PUT", path, headers, []byte(`[2,5]`)), 201)
			head := do(h, "HEAD", path, headers, nil)
			requireSnapshotStatus(t, head, 200)
			inc := head.Header().Get(protocol.HeaderStreamIncarnation)
			if inc == "" || head.Header().Get(protocol.HeaderStreamSnapshot) != "v1" {
				t.Fatal("missing discovery/identity")
			}
			requireSnapshotStatus(t, do(h, "GET", path+"?snapshot=p-v1", headers, nil), 404)
			headers[protocol.HeaderIfStreamIncarnation] = inc
			read := do(h, "GET", path+"?offset=-1", headers, nil)
			requireSnapshotStatus(t, read, 200)
			if read.Body.String() != `[2,5]` {
				t.Fatalf("initial replay: %s", read.Body.String())
			}
			cut := read.Header().Get(protocol.HeaderStreamNextOffset)
			body := []byte(`{"value":11,"count":2}`)
			putHeaders := snapshotPublicationHeaders(inc, cut, "", body)
			appendHooksBefore := hooks.appendCount()
			published := do(h, "PUT", path+"?snapshot=p-v1", putHeaders, body)
			requireSnapshotStatus(t, published, 201)
			etag := published.Header().Get("ETag")
			if etag == "" || published.Body.Len() != 0 || published.Header().Get(protocol.HeaderContentDigest) != "" || hooks.appendCount() != appendHooksBefore {
				t.Fatal("snapshot publication leaked digest/body or triggered append hooks")
			}
			delete(headers, protocol.HeaderIfStreamIncarnation)
			requireSnapshotStatus(t, do(h, "POST", path, headers, []byte(`[7,-4]`)), 204)
			// Use a fresh handler to rule out process-local snapshot state.
			h = newSnapshotTestHandler(t, st)
			saved := do(h, "GET", path+"?snapshot=p-v1", headers, nil)
			requireSnapshotStatus(t, saved, 200)
			if !bytes.Equal(saved.Body.Bytes(), body) || saved.Header().Get("ETag") != etag ||
				saved.Header().Get(protocol.HeaderStreamSnapshotOffset) != cut ||
				saved.Header().Get(protocol.HeaderStreamNextOffset) != "" || saved.Header().Get(protocol.HeaderStreamUpToDate) != "" ||
				saved.Header().Get(protocol.HeaderContentDigest) != putHeaders[protocol.HeaderContentDigest] {
				t.Fatalf("bad saved image: headers=%v body=%s", saved.Header(), saved.Body.String())
			}
			var state struct{ Value, Count int }
			if err := json.Unmarshal(saved.Body.Bytes(), &state); err != nil {
				t.Fatal(err)
			}
			headers[protocol.HeaderIfStreamIncarnation] = inc
			next := cut
			for i := 0; i < 2; i++ {
				page := do(h, "GET", path+"?offset="+next+"&limit=1", headers, nil)
				requireSnapshotStatus(t, page, 200)
				if page.Header().Get("Cache-Control") != "private, no-store" || page.Header().Get(protocol.HeaderStreamIncarnation) != inc {
					t.Fatal("guarded page lost identity or private cache posture")
				}
				var events []int
				if err := json.Unmarshal(page.Body.Bytes(), &events); err != nil || len(events) != 1 {
					t.Fatalf("bad tail page: %s (%v)", page.Body.String(), err)
				}
				state.Value = 3*state.Value + events[0]
				state.Count++
				next = page.Header().Get(protocol.HeaderStreamNextOffset)
				if (page.Header().Get(protocol.HeaderStreamUpToDate) == "true") != (i == 1) {
					t.Fatal("incorrect catch-up boundary")
				}
			}
			if state.Value != 116 || state.Count != 4 {
				t.Fatalf("restored fold = %+v, want {116 4}", state)
			}
			newBody := []byte(`{"value":116,"count":4}`)
			putHeaders = snapshotPublicationHeaders(inc, next, etag, newBody)
			replaced := do(h, "PUT", path+"?snapshot=p-v1", putHeaders, newBody)
			requireSnapshotStatus(t, replaced, 200)
			requireSnapshotStatus(t, do(h, "PUT", path+"?snapshot=p-v1", putHeaders, newBody), 412)
			putHeaders["If-Match"] = replaced.Header().Get("ETag")
			requireSnapshotStatus(t, do(h, "PUT", path+"?snapshot=p-v1", putHeaders, newBody), 200)
			retireHeaders := map[string]string{
				"Authorization": "Bearer worker-test-token", "If-Match": etag,
				protocol.HeaderStreamSnapshot: "v1", protocol.HeaderIfStreamIncarnation: inc,
			}
			requireSnapshotStatus(t, do(h, "DELETE", path+"?snapshot=p-v1", retireHeaders, nil), 412)
			retireHeaders["If-Match"] = replaced.Header().Get("ETag")
			requireSnapshotStatus(t, do(h, "DELETE", path+"?snapshot=p-v1", retireHeaders, nil), 204)
			requireSnapshotStatus(t, do(h, "GET", path+"?snapshot=p-v1", headers, nil), 404)
			delete(putHeaders, "If-Match")
			putHeaders["If-None-Match"] = "*"
			requireSnapshotStatus(t, do(h, "PUT", path+"?snapshot=p-v1", putHeaders, newBody), 201)
			delete(headers, protocol.HeaderIfStreamIncarnation)
			requireSnapshotStatus(t, do(h, "DELETE", path, headers, nil), 204)
			requireSnapshotStatus(t, do(h, "GET", path+"?snapshot=p-v1", headers, nil), 404)
			requireSnapshotStatus(t, do(h, "PUT", path, headers, []byte(`[99]`)), 201)
			requireSnapshotStatus(t, do(h, "GET", path+"?snapshot=p-v1", headers, nil), 404)
			headers[protocol.HeaderIfStreamIncarnation] = inc
			requireSnapshotStatus(t, do(h, "GET", path+"?offset="+next, headers, nil), 412)
			requireSnapshotStatus(t, do(h, "PUT", path+"?snapshot=p-v1", putHeaders, newBody), 412)
		})
	}
}

func TestSnapshotHTTPValidationAndAuthorization(t *testing.T) {
	s := store.NewMemoryStore()
	defer s.Close()
	h := newSnapshotTestHandler(t, s)
	meta, _, err := s.Create("/agents/test", store.CreateOptions{ContentType: "text/plain", InitialData: []byte("abcdef")})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"value":1}`)
	for _, tc := range []struct {
		name, query string
		status      int
		change      func(http.Header)
	}{
		{"no publisher", "snapshot=p", 401, func(h http.Header) { h.Del("Authorization") }},
		{"read grant", "snapshot=p", 403, func(h http.Header) { h.Set("Authorization", "Bearer reader-test-token") }},
		{"append create grants", "snapshot=p", 403, func(h http.Header) { h.Set("Authorization", "Bearer writer-test-token") }},
		{"missing precondition", "snapshot=p", 428, func(h http.Header) { h.Del("If-None-Match") }},
		{"both conditions", "snapshot=p", 400, func(h http.Header) { h.Set("If-Match", `"old"`) }},
		{"weak condition", "snapshot=p", 400, func(h http.Header) { h.Del("If-None-Match"); h.Set("If-Match", `W/"old"`) }},
		{"condition list", "snapshot=p", 400, func(h http.Header) { h.Del("If-None-Match"); h.Set("If-Match", `"a", "b"`) }},
		{"duplicate header", "snapshot=p", 400, func(h http.Header) { h.Add(protocol.HeaderStreamSnapshotOffset, meta.CurrentOffset.String()) }},
		{"wrong digest", "snapshot=p", 400, func(h http.Header) { h.Set(protocol.HeaderContentDigest, "sha-256=:AAAA:") }},
		{"missing digest", "snapshot=p", 400, func(h http.Header) { h.Del(protocol.HeaderContentDigest) }},
		{"encoding", "snapshot=p", 415, func(h http.Header) { h.Set("Content-Encoding", "gzip") }},
		{"invalid media type", "snapshot=p", 415, func(h http.Header) { h.Set("Content-Type", "bad type") }},
		{"sentinel", "snapshot=p", 400, func(h http.Header) { h.Set(protocol.HeaderStreamSnapshotOffset, "-1") }},
		{"noncanonical", "snapshot=p", 400, func(h http.Header) { h.Set(protocol.HeaderStreamSnapshotOffset, "0_6") }},
		{"interior offset", "snapshot=p", 409, func(h http.Header) {
			h.Set(protocol.HeaderStreamSnapshotOffset, (store.Offset{ByteOffset: 3}).String())
		}},
		{"future offset", "snapshot=p", 409, func(h http.Header) {
			h.Set(protocol.HeaderStreamSnapshotOffset, (store.Offset{ByteOffset: 7}).String())
		}},
		{"incarnation", "snapshot=p", 412, func(h http.Header) { h.Set(protocol.HeaderIfStreamIncarnation, "old-source") }},
		{"missing incarnation", "snapshot=p", 400, func(h http.Header) { h.Del(protocol.HeaderIfStreamIncarnation) }},
		{"marker", "snapshot=p", 400, func(h http.Header) { h.Set(protocol.HeaderStreamSnapshot, "v2") }},
		{"empty projection", "snapshot=", 400, nil},
		{"control projection", "snapshot=p%0A", 400, nil},
		{"duplicate projection", "snapshot=p&snapshot=q", 400, nil},
		{"offset combination", "snapshot=p&offset=-1", 400, nil},
		{"unknown parameter", "snapshot=p&unknown=1", 400, nil},
		{"invalid query escape", "snapshot=%XX", 400, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("PUT", "/agents/test?"+tc.query, bytes.NewReader(body))
			for k, v := range snapshotPublicationHeaders(meta.Incarnation, meta.CurrentOffset.String(), "", body) {
				r.Header.Set(k, v)
			}
			if tc.change != nil {
				tc.change(r.Header)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			requireSnapshotStatus(t, w, tc.status)
			if _, err := s.GetSnapshot(t.Context(), "/agents/test", "p"); !errors.Is(err, store.ErrSnapshotNotFound) {
				t.Fatalf("rejected request mutated snapshot: %v", err)
			}
		})
	}
	oversized := bytes.Repeat([]byte("x"), store.MaxSnapshotBytes+1)
	requireSnapshotStatus(t, do(h, "PUT", "/agents/test?snapshot=p", snapshotPublicationHeaders(meta.Incarnation, meta.CurrentOffset.String(), "", oversized), oversized), 413)
	h.AuthMode = auth.ModeInsecure
	requireSnapshotStatus(t, do(h, "PUT", "/agents/test?snapshot=p", nil, body), 401)
	// Even disabled/unsupported replicas must not create a stream for this PUT.
	h.EnableSnapshots = false
	requireSnapshotStatus(t, do(h, "PUT", "/agents/absent?snapshot=p", nil, body), 501)
	if s.Has("/agents/absent") {
		t.Fatal("disabled snapshot PUT created a stream")
	}
	requireSnapshotStatus(t, do(h, "GET", "/agents/test", map[string]string{protocol.HeaderIfStreamIncarnation: meta.Incarnation}, nil), 501)
}

func TestSnapshotReadGuardsCoverEveryResponseMode(t *testing.T) {
	s := store.NewMemoryStore()
	defer s.Close()
	h := newSnapshotTestHandler(t, s)
	meta, _, err := s.Create("/agents/test", store.CreateOptions{ContentType: "application/json", InitialData: []byte(`[1,2]`)})
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"?offset=-1", "?offset=-1&limit=1", "?offset=now", "?offset=now&live=long-poll", "?offset=-1&live=sse"} {
		t.Run(query, func(t *testing.T) {
			headers := map[string]string{"Authorization": "Bearer reader-test-token", protocol.HeaderIfStreamIncarnation: "wrong"}
			requireSnapshotStatus(t, do(h, "GET", "/agents/test"+query, headers, nil), 412)
			headers[protocol.HeaderIfStreamIncarnation] = meta.Incarnation
			w := do(h, "GET", "/agents/test"+query, headers, nil)
			want := 200
			if strings.Contains(query, "long-poll") {
				want = 204
			}
			requireSnapshotStatus(t, w, want)
			if w.Header().Get("Cache-Control") != "private, no-store" || w.Header().Get(protocol.HeaderStreamIncarnation) != meta.Incarnation || w.Header().Get(protocol.HeaderStreamSnapshot) != "v1" {
				t.Fatalf("bad guarded headers: %v", w.Header())
			}
		})
	}
	if _, err := s.CloseStream("/agents/test"); err != nil {
		t.Fatal(err)
	}
	headers := map[string]string{"Authorization": "Bearer reader-test-token", protocol.HeaderIfStreamIncarnation: meta.Incarnation}
	w := do(h, "GET", "/agents/test?offset="+meta.CurrentOffset.String()+"&live=long-poll", headers, nil)
	requireSnapshotStatus(t, w, 204)
	if w.Header().Get(protocol.HeaderStreamClosed) != "true" || w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("closed-tail response lost guard/cache headers")
	}
	headers[protocol.HeaderIfStreamIncarnation] = "old"
	requireSnapshotStatus(t, do(h, "HEAD", "/agents/test", headers, nil), 412)
	preflight := do(h, "OPTIONS", "/agents/test?snapshot=p", nil, nil)
	for _, header := range []string{"If-Match", "Content-Digest", "If-Stream-Incarnation", "Stream-Snapshot-Offset"} {
		if !strings.Contains(preflight.Header().Get("Access-Control-Allow-Headers"), header) {
			t.Fatalf("CORS missing %s", header)
		}
	}
}

func TestSnapshotRetirementAuthorizationAndQuotas(t *testing.T) {
	s := store.NewMemoryStore()
	t.Cleanup(func() { _ = s.Close() })
	h := newSnapshotTestHandler(t, s)
	meta, _, err := s.Create("/agents/test", store.CreateOptions{InitialData: []byte("abc")})
	if err != nil {
		t.Fatal(err)
	}
	var etag string
	for i := 0; i < store.MaxSnapshotVersions; i++ {
		w := do(h, "PUT", fmt.Sprintf("/agents/test?snapshot=p%d", i), snapshotPublicationHeaders(meta.Incarnation, meta.CurrentOffset.String(), "", nil), nil)
		requireSnapshotStatus(t, w, 201)
		if i == 0 {
			etag = w.Header().Get("ETag")
		}
		for header, want := range map[string]string{
			protocol.HeaderStreamSnapshotMaxBytes: "1048576", protocol.HeaderStreamSnapshotMaxTotalBytes: "4194304", protocol.HeaderStreamSnapshotMaxVersions: "8",
		} {
			if w.Header().Get(header) != want {
				t.Fatalf("%s = %q, want %q", header, w.Header().Get(header), want)
			}
		}
	}
	requireSnapshotStatus(t, do(h, "PUT", "/agents/test?snapshot=overflow", snapshotPublicationHeaders(meta.Incarnation, meta.CurrentOffset.String(), "", nil), nil), 507)
	for _, tc := range []struct {
		name   string
		status int
		mutate func(http.Header)
	}{
		{"no credential", 401, func(h http.Header) { h.Del("Authorization") }},
		{"reader", 403, func(h http.Header) { h.Set("Authorization", "Bearer reader-test-token") }},
		{"writer", 403, func(h http.Header) { h.Set("Authorization", "Bearer writer-test-token") }},
		{"missing condition", 428, func(h http.Header) { h.Del("If-Match") }},
		{"wildcard", 400, func(h http.Header) { h.Set("If-Match", "*") }},
		{"both conditions", 400, func(h http.Header) { h.Set("If-None-Match", "*") }},
		{"weak condition", 400, func(h http.Header) { h.Set("If-Match", "W/"+etag) }},
		{"duplicate condition", 400, func(h http.Header) { h.Add("If-Match", etag) }},
		{"old incarnation", 412, func(h http.Header) { h.Set(protocol.HeaderIfStreamIncarnation, "old") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("DELETE", "/agents/test?snapshot=p0", nil)
			r.Header.Set("Authorization", "Bearer worker-test-token")
			r.Header.Set(protocol.HeaderStreamSnapshot, "v1")
			r.Header.Set(protocol.HeaderIfStreamIncarnation, meta.Incarnation)
			r.Header.Set("If-Match", etag)
			tc.mutate(r.Header)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			requireSnapshotStatus(t, w, tc.status)
			if image, err := s.GetSnapshot(t.Context(), "/agents/test", "p0"); err != nil || image.ETag != etag {
				t.Fatalf("rejected retirement changed image: %v", err)
			}
		})
	}
	retire := map[string]string{"Authorization": "Bearer worker-test-token", protocol.HeaderStreamSnapshot: "v1", protocol.HeaderIfStreamIncarnation: meta.Incarnation, "If-Match": etag}
	requireSnapshotStatus(t, do(h, "DELETE", "/agents/test?snapshot=p0", retire, nil), 204)
	requireSnapshotStatus(t, do(h, "DELETE", "/agents/test?snapshot=p0", retire, nil), 412)
	requireSnapshotStatus(t, do(h, "PUT", "/agents/test?snapshot=overflow", snapshotPublicationHeaders(meta.Incarnation, meta.CurrentOffset.String(), "", nil), nil), 201)
	if after, err := s.Get("/agents/test"); err != nil || after.CurrentOffset != meta.CurrentOffset || after.Incarnation != meta.Incarnation {
		t.Fatalf("retirement changed source: %+v %v", after, err)
	}
}
