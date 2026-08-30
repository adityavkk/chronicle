package chronicle

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	"gecgithub01.walmart.com/auk000v/chronicle/store/segments"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// The write-fencing acceptance suite of the handler and authz seam (#183,
// design §E and §H.1 rows 12–20): the write-token carriers and their
// fail-closed presentation, the class a request takes on a fenced stream, the
// disclosure a rejection carries, the create/HEAD echo, and the single
// rejection counter. The in-slot rung itself is pinned in store/redis; here a
// scripted store says what the slot decided and the tests pin what the
// handler does with it.

// fencedStore is the root test double of a write-fence-capable store:
// MemoryStore for everything base, plus the opt-in flag, a HEAD seal summary,
// and a scripted in-slot verdict for the next write, so class routing and
// disclosure are pinned without Redis. It records the fence each write reached
// the slot with (nil = open class) — the class decision made observable.
type fencedStore struct {
	*store.MemoryStore
	mu      sync.Mutex
	fenced  map[string]bool
	seals   map[string]store.WriteFenceSeal
	verdict *store.AppendResult // FenceReason/FenceGeneration/FenceHolder of the next write
	fences  []*auth.AppendFence
}

var _ store.WriteFenceStore = (*fencedStore)(nil)

func newFencedStore() *fencedStore {
	return &fencedStore{
		MemoryStore: store.NewMemoryStore(),
		fenced:      map[string]bool{},
		seals:       map[string]store.WriteFenceSeal{},
	}
}

// reject scripts the in-slot verdict of the next write.
func (s *fencedStore) reject(reason store.FenceReason, generation int64, holder string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.verdict = &store.AppendResult{FenceReason: reason, FenceGeneration: generation, FenceHolder: holder}
}

// slot records the fence a write reached the slot with and takes the scripted
// verdict, if any.
func (s *fencedStore) slot(fence *auth.AppendFence) *store.AppendResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fences = append(s.fences, fence)
	v := s.verdict
	s.verdict = nil
	return v
}

// lastFence is the fence of the most recent write; ok is false when none ran.
func (s *fencedStore) lastFence() (fence *auth.AppendFence, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.fences) == 0 {
		return nil, false
	}
	return s.fences[len(s.fences)-1], true
}

func (s *fencedStore) Create(path string, opts store.CreateOptions) (*store.StreamMetadata, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Has(path) && s.fenced[path] != opts.WriteFence {
		return nil, false, store.ErrConfigMismatch
	}
	writeFence := opts.WriteFence
	opts.WriteFence = false
	meta, created, err := s.MemoryStore.Create(path, opts)
	if err != nil {
		return nil, false, err
	}
	s.fenced[path] = writeFence
	view := *meta
	view.WriteFence = writeFence
	return &view, created, nil
}

func (s *fencedStore) Get(path string) (*store.StreamMetadata, error) {
	meta, err := s.MemoryStore.Get(path)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	meta.WriteFence = s.fenced[path]
	return meta, nil
}

func (s *fencedStore) ReadPage(ctx context.Context, path string, offset store.Offset, opts store.ReadPageOptions) (store.ReadPage, error) {
	page, err := s.MemoryStore.ReadPage(ctx, path, offset, opts)
	if err != nil {
		return page, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	page.Snapshot.WriteFence = s.fenced[path]
	if seal, ok := s.seals[path]; ok {
		off := seal.Offset
		page.Snapshot.SealedGeneration, page.Snapshot.SealedOffset = seal.Generation, &off
	}
	return page, nil
}

func (s *fencedStore) Append(path string, data []byte, opts store.AppendOptions) (store.AppendResult, error) {
	if v := s.slot(opts.Fence); v != nil {
		return *v, store.ErrAppendFenced
	}
	opts.Fence = nil
	return s.MemoryStore.Append(path, data, opts)
}

func (s *fencedStore) CloseStreamWithProducer(path string, opts store.CloseProducerOptions) (*store.CloseProducerResult, error) {
	if v := s.slot(opts.Fence); v != nil {
		return &store.CloseProducerResult{FenceReason: v.FenceReason, FenceGeneration: v.FenceGeneration, FenceHolder: v.FenceHolder}, store.ErrAppendFenced
	}
	opts.Fence = nil
	return s.MemoryStore.CloseStreamWithProducer(path, opts)
}

func (s *fencedStore) CloseStreamFenced(path string, fence auth.AppendFence) (*store.CloseResult, error) {
	if v := s.slot(&fence); v != nil {
		return &store.CloseResult{FenceReason: v.FenceReason, FenceGeneration: v.FenceGeneration, FenceHolder: v.FenceHolder}, store.ErrAppendFenced
	}
	return s.CloseStream(path)
}

func (s *fencedStore) GrantAppendFence(string, auth.AppendFence) (bool, error) { return true, nil }
func (s *fencedStore) RevokeAppendFence(string, auth.AppendFence) error        { return nil }
func (s *fencedStore) SealAppendFence(string, auth.AppendFence) (store.SealResult, error) {
	return store.SealResult{Outcome: store.SealSealed}, nil
}

// fenceAuthorizer is the root test double of the two-phase append authorizer:
// the real credential check under key, and a phase-2 fence built from the
// token's own claim identity — what the Manager's authorizer returns once its
// control-plane pre-check passes. fenced scripts that pre-check refusing.
type fenceAuthorizer struct {
	webhook.WriteTokenAuthorizer
	key    []byte
	fenced bool
}

func newFenceAuthorizer(key []byte) fenceAuthorizer {
	return fenceAuthorizer{WriteTokenAuthorizer: webhook.NewWriteTokenAuthorizer(key), key: key}
}

func (a fenceAuthorizer) AuthorizeAppendFence(token string, path auth.StreamPath, now time.Time) (auth.Decision, *auth.AppendFence) {
	if d := a.AuthorizeAppendCredential(token, path, now); !d.Allowed() {
		return d, nil
	}
	if a.fenced {
		return auth.Deny(auth.ReasonFenced, "write token claim is fenced"), nil
	}
	v := webhook.ValidateWriteToken(a.key, token, path, now)
	return auth.Allow(), &auth.AppendFence{
		SubscriptionID:          v.SubID,
		SubscriptionIncarnation: v.Incarnation,
		Shard:                   v.Shard,
		Generation:              v.Generation,
		WakeID:                  v.WakeID,
		Holder:                  v.Holder,
	}
}

// fenceCounter is the FenceMetrics double: one bucket per reason.
type fenceCounter struct {
	mu      sync.Mutex
	reasons map[string]int
}

func (c *fenceCounter) AppendFenceRejection(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reasons == nil {
		c.reasons = map[string]int{}
	}
	c.reasons[reason]++
}

// only asserts the counter holds exactly one increment, under reason; an empty
// reason asserts nothing was counted (a base denial, not a fence rejection).
func (c *fenceCounter) only(t *testing.T, reason string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, n := range c.reasons {
		total += n
	}
	switch {
	case reason == "" && total != 0:
		t.Errorf("fence rejections = %v, want none", c.reasons)
	case reason != "" && (total != 1 || c.reasons[reason] != 1):
		t.Errorf("fence rejections = %v, want exactly one under %q", c.reasons, reason)
	}
}

// fencelessStore hides the wrapped store's write-fence capability while
// keeping the page-snapshot surface the segments wrapper requires, so a
// handler (or segments primary) over it sees a store with no fence support.
type fencelessStore struct {
	store.Store
	store.PageReader
}

func newFencelessStore(ms *store.MemoryStore) fencelessStore {
	return fencelessStore{Store: ms, PageReader: ms}
}

// fenceFixture is a handler over a fencedStore with every credential family
// wired: the token arm under key, the service bearer tb4SvcBearer (a trusted
// gateway), a wake-token entity arm under wakeKey, a rejection counter, and a
// captured log.
type fenceFixture struct {
	h       *Handler
	store   *fencedStore
	key     []byte
	wakeKey webhook.SigningKey
	counter *fenceCounter
	logs    *bytes.Buffer
}

func newFenceFixture(t *testing.T, mode auth.Mode) *fenceFixture {
	t.Helper()
	wakeKey, err := webhook.GenerateSigningKey(rand.Reader, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	creds, err := auth.ParseServiceBearerConfig("agents-server:" + tb4SvcBearer)
	if err != nil {
		t.Fatal(err)
	}
	f := &fenceFixture{
		store:   newFencedStore(),
		key:     testAuthKey(t),
		wakeKey: wakeKey,
		counter: &fenceCounter{},
		logs:    &bytes.Buffer{},
	}
	f.h = testHandler(time.Second, time.Second)
	f.h.Store = f.store
	f.h.Logger = slog.New(slog.NewTextHandler(f.logs, nil))
	f.h.AuthMode = mode
	f.h.AppendAuth = newFenceAuthorizer(f.key)
	f.h.EntityAuth = webhook.NewWakeTokenAuthorizer("", webhook.StaticKidResolver(wakeKey))
	f.h.ServiceAuth = &ServiceAuth{Credentials: creds, Policies: gatewayPolicies(t, "agents-server")}
	f.h.FenceMetrics = f.counter
	return f
}

// create seeds a stream through the store, fenced or not.
func (f *fenceFixture) create(t *testing.T, path string, writeFence bool) {
	t.Helper()
	if _, _, err := f.store.Create(path, store.CreateOptions{ContentType: "application/json", WriteFence: writeFence}); err != nil {
		t.Fatal(err)
	}
}

// claimToken mints a live-fenced write token at generation for holder, scoped
// to paths (wire spelling, no leading slash).
func (f *fenceFixture) claimToken(t *testing.T, generation int64, holder string, paths ...string) string {
	t.Helper()
	scope := make([]auth.StreamPath, len(paths))
	for i, p := range paths {
		scope[i] = mustStreamPath(t, p)
	}
	tok, err := webhook.GenerateClaimWriteToken(f.key, "sub-1", "inc-1", generation, "w_1", holder, 0, scope, time.Now(), time.Hour, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func (f *fenceFixture) wakeToken(t *testing.T, entity string) string {
	t.Helper()
	return mintWakeFor(t, f.wakeKey, entity, "", time.Minute)
}

// header pairs, as [name, value]; a name repeated is a duplicated header.
type hdr [2]string

// producerAt is the producer triple of one fenced-class write.
func producerAt(epoch, seq string) []hdr {
	return []hdr{{"Producer-Id", "entity-agents/e1"}, {"Producer-Epoch", epoch}, {"Producer-Seq", seq}}
}

// post runs one POST with headers added in order (duplicates preserved).
func (f *fenceFixture) post(path string, body []byte, headers ...hdr) *httptest.ResponseRecorder {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req := httptest.NewRequest(http.MethodPost, path, rdr)
	for _, h := range headers {
		req.Header.Add(h[0], h[1])
	}
	rec := httptest.NewRecorder()
	f.h.ServeHTTP(rec, req)
	return rec
}

func jsonAppend(headers ...hdr) []hdr {
	return append([]hdr{{"Content-Type", "application/json"}}, headers...)
}

func decodeFenceEnvelope(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantReason string) webhook.ErrorDetail {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d body %q, want %d", rec.Code, rec.Body.String(), wantStatus)
	}
	eb := decodeEnvelope(t, rec)
	if eb.Error.Reason != wantReason {
		t.Errorf("reason = %q, want %q (body %s)", eb.Error.Reason, wantReason, rec.Body.String())
	}
	return eb.Error
}

// assertNoProducerDisclosure pins B.4: the epoch echo and the terminal pair
// are emitted only on a 409.
func assertNoProducerDisclosure(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	for _, name := range []string{protocol.HeaderProducerEpoch, protocol.HeaderProducerExpectedSeq, protocol.HeaderProducerReceivedSeq, protocol.HeaderStreamNextOffset} {
		if v := rec.Header().Get(name); v != "" {
			t.Errorf("%s = %q on a %d, want absent", name, v, rec.Code)
		}
	}
}

// TestAppendClassDerivation pins A.0 Q1's class rule on a fenced stream: the
// class is fenced iff the request presents a write-token carrier (Write-Token,
// the electric-claim-token alias, or a Bearer that phase 1 did not consume) or
// asserts Write-Fence: true; a routed principal with a token must verify on
// both; a malformed carrier is presented-but-invalid and never falls through
// (addendum §1); everything else is the open class under the principal rule —
// where an anonymous request in enforce mode is still phase 1's pre-lookup
// 401 (no reason, no count: existence never leaks, §12.2).
func TestAppendClassDerivation(t *testing.T) {
	const path = "/agents/e1/session"
	cases := []struct {
		name       string
		headers    func(f *fenceFixture, tok string) []hdr
		wantStatus int
		wantReason string // "" on a base denial: no disclosure, nothing counted
		wantFenced bool   // the class that reached the store, on success
	}{
		{"Write-Token carrier", func(_ *fenceFixture, tok string) []hdr {
			return []hdr{{WriteTokenHeader, tok}}
		}, http.StatusOK, "", true},
		{"electric-claim-token alias", func(_ *fenceFixture, tok string) []hdr {
			return []hdr{{ClaimTokenHeader, tok}}
		}, http.StatusOK, "", true},
		{"bearer not consumed by another family", func(_ *fenceFixture, tok string) []hdr {
			return []hdr{{"Authorization", "Bearer " + tok}}
		}, http.StatusOK, "", true},
		{"service principal with token", func(_ *fenceFixture, tok string) []hdr {
			return []hdr{{"Authorization", "Bearer " + tb4SvcBearer}, {WriteTokenHeader, tok}}
		}, http.StatusOK, "", true},
		{"wake token with token", func(f *fenceFixture, tok string) []hdr {
			return []hdr{{"Authorization", "Bearer " + f.wakeToken(t, "agents/e1")}, {WriteTokenHeader, tok}}
		}, http.StatusOK, "", true},
		{"declared with token", func(_ *fenceFixture, tok string) []hdr {
			return []hdr{{protocol.HeaderWriteFence, "true"}, {WriteTokenHeader, tok}}
		}, http.StatusOK, "", true},
		{"opaque bearer", func(*fenceFixture, string) []hdr {
			return []hdr{{"Authorization", "Bearer 3f0c1a2e-opaque"}}
		}, http.StatusUnauthorized, reasonCredential, false},
		{"declared without token", func(*fenceFixture, string) []hdr {
			return []hdr{{protocol.HeaderWriteFence, "true"}}
		}, http.StatusUnauthorized, reasonCredential, false},
		{"declared service principal without token", func(*fenceFixture, string) []hdr {
			return []hdr{{"Authorization", "Bearer " + tb4SvcBearer}, {protocol.HeaderWriteFence, "true"}}
		}, http.StatusUnauthorized, reasonCredential, false},
		{"service principal with garbage token", func(*fenceFixture, string) []hdr {
			return []hdr{{"Authorization", "Bearer " + tb4SvcBearer}, {WriteTokenHeader, "garbage"}}
		}, http.StatusUnauthorized, reasonCredential, false},
		{"wake token with garbage token", func(f *fenceFixture, _ string) []hdr {
			return []hdr{{"Authorization", "Bearer " + f.wakeToken(t, "agents/e1")}, {WriteTokenHeader, "garbage"}}
		}, http.StatusUnauthorized, reasonCredential, false},
		{"wrong-path token", func(f *fenceFixture, _ string) []hdr {
			return []hdr{{WriteTokenHeader, f.claimToken(t, 8, "worker-A", "agents/other/session")}}
		}, http.StatusForbidden, reasonCredential, false},
		{"duplicated Write-Token", func(_ *fenceFixture, tok string) []hdr {
			return []hdr{{WriteTokenHeader, tok}, {WriteTokenHeader, tok}}
		}, http.StatusUnauthorized, reasonCredential, false},
		{"empty Write-Token does not fall through to the bearer", func(_ *fenceFixture, tok string) []hdr {
			return []hdr{{WriteTokenHeader, ""}, {"Authorization", "Bearer " + tok}}
		}, http.StatusUnauthorized, reasonCredential, false},
		{"empty alias does not fall through to the bearer", func(_ *fenceFixture, tok string) []hdr {
			return []hdr{{ClaimTokenHeader, ""}, {"Authorization", "Bearer " + tok}}
		}, http.StatusUnauthorized, reasonCredential, false},
		{"service principal alone", func(*fenceFixture, string) []hdr {
			return []hdr{{"Authorization", "Bearer " + tb4SvcBearer}}
		}, http.StatusOK, "", false},
		{"no credential", func(*fenceFixture, string) []hdr {
			return nil
		}, http.StatusUnauthorized, "", false},
		{"wake token alone", func(f *fenceFixture, _ string) []hdr {
			return []hdr{{"Authorization", "Bearer " + f.wakeToken(t, "agents/e1")}}
		}, http.StatusForbidden, reasonWakeToken, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFenceFixture(t, auth.ModeEnforce)
			f.create(t, path, true)
			tok := f.claimToken(t, 8, "worker-A", "agents/e1/session")
			before := tailOf(t, f.h, path)

			rec := f.post(path, []byte(`{"n":1}`), jsonAppend(append(producerAt("8", "0"), c.headers(f, tok)...)...)...)
			if rec.Code != c.wantStatus {
				t.Fatalf("status = %d body %q, want %d", rec.Code, rec.Body.String(), c.wantStatus)
			}
			if rec.Code >= http.StatusBadRequest {
				decodeFenceEnvelope(t, rec, c.wantStatus, c.wantReason)
				f.counter.only(t, c.wantReason)
				if after := tailOf(t, f.h, path); !after.Equal(before) {
					t.Fatalf("denied write mutated the stream: %s -> %s", before, after)
				}
				if _, ok := f.store.lastFence(); ok {
					t.Fatal("denied write reached the store")
				}
				return
			}
			fence, ok := f.store.lastFence()
			if !ok {
				t.Fatal("accepted write never reached the store")
			}
			if (fence != nil) != c.wantFenced {
				t.Errorf("class reached the store fenced=%v, want %v", fence != nil, c.wantFenced)
			}
		})
	}
}

// TestAppendDeclaredWithoutTokenIs401InEveryMode pins B.1: a POST asserting
// Write-Fence: true without a write-token carrier is refused before the stream
// lookup — on a stream that exists or not, fenced or not, in insecure and
// enforce mode, with or without an authorizer — with no producer disclosure.
func TestAppendDeclaredWithoutTokenIs401InEveryMode(t *testing.T) {
	handlers := []struct {
		name string
		mk   func(t *testing.T) *Handler
	}{
		{"zero-value handler", func(*testing.T) *Handler { return testHandler(time.Second, time.Second) }},
		{"insecure with authorizer", func(t *testing.T) *Handler {
			h := testHandler(time.Second, time.Second)
			h.AppendAuth = webhook.NewWriteTokenAuthorizer(testAuthKey(t))
			return h
		}},
		{"enforce with authorizer", func(t *testing.T) *Handler {
			h, _ := enforcedHandler(t, testAuthKey(t))
			return h
		}},
	}
	for _, hc := range handlers {
		for _, path := range []string{"/events/exists", "/events/missing"} {
			for _, producer := range []bool{false, true} {
				name := hc.name + " " + path
				if producer {
					name += " with producer headers"
				}
				t.Run(name, func(t *testing.T) {
					h := hc.mk(t)
					createDirect(t, h, "/events/exists", "application/json")
					before := tailOf(t, h, "/events/exists")
					headers := map[string]string{"Content-Type": "application/json", protocol.HeaderWriteFence: "true"}
					if producer {
						for _, p := range producerAt("8", "0") {
							headers[p[0]] = p[1]
						}
					}
					rec := do(h, http.MethodPost, path, headers, []byte(`{"n":1}`))
					detail := decodeFenceEnvelope(t, rec, http.StatusUnauthorized, reasonCredential)
					if detail.Message != "fenced write requires a write token" {
						t.Errorf("message = %q", detail.Message)
					}
					if rec.Header().Get("WWW-Authenticate") == "" {
						t.Error("401 must carry WWW-Authenticate")
					}
					assertNoProducerDisclosure(t, rec)
					if after := tailOf(t, h, "/events/exists"); !after.Equal(before) {
						t.Fatalf("declared write without a token mutated the stream: %s -> %s", before, after)
					}
				})
			}
		}
	}
}

// TestAppendShardTokenRefused pins A.0 Q9: a write token minted for a shard
// other than 0 is refused 401 in phase 1 on every stream in every mode — no
// marker is ever granted for it — under reason "shard".
func TestAppendShardTokenRefused(t *testing.T) {
	for _, c := range []struct {
		name   string
		mode   auth.Mode
		fenced bool
	}{
		{"enforce unfenced", auth.ModeEnforce, false},
		{"insecure unfenced", auth.ModeInsecure, false},
		{"insecure fenced", auth.ModeInsecure, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFenceFixture(t, c.mode)
			f.create(t, "/events/a", c.fenced)
			before := tailOf(t, f.h, "/events/a")
			tok, err := webhook.GenerateClaimWriteToken(f.key, "sub-1", "inc-1", 8, "w_1", "worker-A", 1,
				[]auth.StreamPath{mustStreamPath(t, "events/a")}, time.Now(), time.Hour, rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			rec := f.post("/events/a", []byte(`{"n":1}`), jsonAppend(append(producerAt("8", "0"), hdr{WriteTokenHeader, tok})...)...)
			detail := decodeFenceEnvelope(t, rec, http.StatusUnauthorized, reasonShard)
			if detail.Message != webhook.DetailWriteTokenShard {
				t.Errorf("message = %q", detail.Message)
			}
			f.counter.only(t, reasonShard)
			if after := tailOf(t, f.h, "/events/a"); !after.Equal(before) {
				t.Fatalf("shard token mutated the stream: %s -> %s", before, after)
			}
		})
	}
}

// TestHandleAppendFencedDisclosure pins B.2/B.4 per reason: the 409 FENCED
// envelope carries reason, generation, and current_holder as the slot knew
// them; with producer headers it carries Producer-Epoch iff a generation is
// known and always the terminal pair Expected == Received == the request's
// seq; it never carries Stream-Next-Offset; and no producer disclosure at all
// leaves on a 401, 403, or 400. Each rejection counts exactly once.
func TestHandleAppendFencedDisclosure(t *testing.T) {
	const path = "/agents/e1/session"
	type want struct {
		status            int
		generation        int64
		reason, message   string
		holder, epochEcho string
		pair              bool
	}
	cases := []struct {
		name    string
		prepare func(f *fenceFixture)
		request func(f *fenceFixture, tok string) (body []byte, headers []hdr)
		want    want
	}{
		{
			"sealed", func(f *fenceFixture) { f.store.reject(store.FenceSealed, 8, "") },
			func(_ *fenceFixture, tok string) ([]byte, []hdr) {
				return []byte(`{"late":true}`), jsonAppend(append(producerAt("8", "5"), hdr{WriteTokenHeader, tok})...)
			},
			want{409, 8, "sealed", "claim generation is sealed", "", "8", true},
		},
		{
			"marker", func(f *fenceFixture) { f.store.reject(store.FenceMarker, 9, "worker-B") },
			func(_ *fenceFixture, tok string) ([]byte, []hdr) {
				return []byte(`{"stale":true}`), jsonAppend(append(producerAt("8", "4"), hdr{WriteTokenHeader, tok})...)
			},
			want{409, 9, "marker", "write token claim is fenced", "worker-B", "9", true},
		},
		{
			"marker with no generation known", func(f *fenceFixture) { f.store.reject(store.FenceMarker, 0, "") },
			func(_ *fenceFixture, tok string) ([]byte, []hdr) {
				return []byte(`{"stale":true}`), jsonAppend(append(producerAt("8", "4"), hdr{WriteTokenHeader, tok})...)
			},
			want{409, 0, "marker", "write token claim is fenced", "", "", true},
		},
		{
			"no disclosure from the backend", func(f *fenceFixture) { f.store.reject(store.FenceNone, 0, "") },
			func(_ *fenceFixture, tok string) ([]byte, []hdr) {
				return []byte(`{"n":1}`), jsonAppend(append(producerAt("8", "4"), hdr{WriteTokenHeader, tok})...)
			},
			// A backend that reports ErrAppendFenced with zero disclosure fields
			// still yields a well-formed 409: the generic marker refusal, no
			// generation, no holder, no epoch echo, the terminal pair intact.
			want{409, 0, "marker", "write token claim is fenced", "", "", true},
		},
		{
			"epoch", func(f *fenceFixture) { f.store.reject(store.FenceEpoch, 8, "worker-A") },
			func(_ *fenceFixture, tok string) ([]byte, []hdr) {
				return []byte(`{"n":1}`), jsonAppend(append(producerAt("9", "0"), hdr{WriteTokenHeader, tok})...)
			},
			want{409, 8, "epoch", "producer epoch must equal the claim generation", "worker-A", "8", true},
		},
		{
			"bound", func(f *fenceFixture) { f.store.reject(store.FenceBound, 8, "") },
			func(*fenceFixture, string) ([]byte, []hdr) {
				return []byte(`{"n":1}`), jsonAppend(append(producerAt("9", "0"), hdr{"Authorization", "Bearer " + tb4SvcBearer})...)
			},
			want{409, 8, "bound", "producer is bound to the write fence", "", "8", true},
		},
		{
			"close-only marker", func(f *fenceFixture) { f.store.reject(store.FenceMarker, 9, "worker-B") },
			func(_ *fenceFixture, tok string) ([]byte, []hdr) {
				return nil, append(producerAt("8", "5"), hdr{protocol.HeaderStreamClosed, "true"}, hdr{WriteTokenHeader, tok})
			},
			want{409, 9, "marker", "write token claim is fenced", "worker-B", "9", true},
		},
		{
			"precheck", func(f *fenceFixture) {
				f.h.AppendAuth = fenceAuthorizer{WriteTokenAuthorizer: webhook.NewWriteTokenAuthorizer(f.key), key: f.key, fenced: true}
			},
			func(_ *fenceFixture, tok string) ([]byte, []hdr) {
				return []byte(`{"n":1}`), jsonAppend(append(producerAt("8", "3"), hdr{WriteTokenHeader, tok})...)
			},
			want{409, 0, "precheck", "write token claim is fenced", "", "", true},
		},
		{
			"store", func(f *fenceFixture) { f.h.AppendAuth = webhook.NewWriteTokenAuthorizer(f.key) },
			func(_ *fenceFixture, tok string) ([]byte, []hdr) {
				return []byte(`{"n":1}`), jsonAppend(append(producerAt("8", "3"), hdr{WriteTokenHeader, tok})...)
			},
			want{409, 0, "store", "atomic append fence unavailable", "", "", true},
		},
		{
			"sealed without producer headers is the 400 first", func(f *fenceFixture) { f.store.reject(store.FenceSealed, 8, "") },
			func(_ *fenceFixture, tok string) ([]byte, []hdr) {
				return []byte(`{"n":1}`), jsonAppend(hdr{WriteTokenHeader, tok})
			},
			want{400, 0, "producer_required", "fenced write requires Producer-Id, Producer-Epoch, and Producer-Seq", "", "", false},
		},
		{
			"declared without token", func(*fenceFixture) {},
			func(*fenceFixture, string) ([]byte, []hdr) {
				return []byte(`{"n":1}`), jsonAppend(append(producerAt("8", "0"), hdr{protocol.HeaderWriteFence, "true"})...)
			},
			want{401, 0, "credential", "fenced write requires a write token", "", "", false},
		},
		{
			"wake token", func(*fenceFixture) {},
			func(f *fenceFixture, _ string) ([]byte, []hdr) {
				return []byte(`{"n":1}`), jsonAppend(append(producerAt("8", "0"), hdr{"Authorization", "Bearer " + f.wakeToken(t, "agents/e1")})...)
			},
			want{403, 0, "wake_token", "wake token cannot write to a fenced stream", "", "", false},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFenceFixture(t, auth.ModeEnforce)
			f.create(t, path, true)
			tok := f.claimToken(t, 8, "worker-A", "agents/e1/session")
			c.prepare(f)
			before := tailOf(t, f.h, path)

			body, headers := c.request(f, tok)
			rec := f.post(path, body, headers...)
			if rec.Code != c.want.status {
				t.Fatalf("status = %d body %q, want %d", rec.Code, rec.Body.String(), c.want.status)
			}
			f.counter.only(t, c.want.reason)
			if after := tailOf(t, f.h, path); !after.Equal(before) {
				t.Fatalf("rejected write mutated the stream: %s -> %s", before, after)
			}
			if meta, _ := f.h.Store.Get(path); meta.Closed {
				t.Fatal("rejected close closed the stream")
			}
			if c.want.status == 400 {
				if got := strings.TrimSpace(rec.Body.String()); got != c.want.message {
					t.Errorf("400 body = %q, want %q", got, c.want.message)
				}
				assertNoProducerDisclosure(t, rec)
				return
			}
			detail := decodeFenceEnvelope(t, rec, c.want.status, c.want.reason)
			if detail.Message != c.want.message {
				t.Errorf("message = %q, want %q", detail.Message, c.want.message)
			}
			if detail.Generation != c.want.generation || detail.CurrentHolder != c.want.holder {
				t.Errorf("disclosure = (gen %d, holder %q), want (gen %d, holder %q)", detail.Generation, detail.CurrentHolder, c.want.generation, c.want.holder)
			}
			if !c.want.pair {
				assertNoProducerDisclosure(t, rec)
				return
			}
			if got := rec.Header().Get(protocol.HeaderProducerEpoch); got != c.want.epochEcho {
				t.Errorf("Producer-Epoch = %q, want %q", got, c.want.epochEcho)
			}
			seq := ""
			for _, h := range headers {
				if h[0] == protocol.HeaderProducerSeq {
					seq = h[1]
				}
			}
			if exp, rcv := rec.Header().Get(protocol.HeaderProducerExpectedSeq), rec.Header().Get(protocol.HeaderProducerReceivedSeq); exp != seq || rcv != seq {
				t.Errorf("terminal pair = (%q, %q), want both %q", exp, rcv, seq)
			}
			if v := rec.Header().Get(protocol.HeaderStreamNextOffset); v != "" {
				t.Errorf("Stream-Next-Offset = %q on a fenced 409, want absent", v)
			}
		})
	}
}

// TestHandleAppendOpenClassOnFencedStream pins the open-class principal rule
// (A.0 Q3, insecure-mode row): an anonymous open write needs a principal per
// AuthMode — in enforce it is phase 1's pre-lookup 401 (no disclosure), in
// insecure it lands with one telemetry line — a wake token never writes a
// fenced stream in any mode, a service principal writes under its policy, and
// a service principal riding with a token must have both verify.
func TestHandleAppendOpenClassOnFencedStream(t *testing.T) {
	const path = "/agents/e1/session"
	for _, mode := range []auth.Mode{auth.ModeEnforce, auth.ModeInsecure} {
		anonymous := http.StatusNoContent
		if mode == auth.ModeEnforce {
			anonymous = http.StatusUnauthorized
		}
		cases := []struct {
			name       string
			headers    func(f *fenceFixture) []hdr
			wantStatus int
			wantReason string
		}{
			{"anonymous", func(*fenceFixture) []hdr { return nil }, anonymous, ""},
			{"wake token", func(f *fenceFixture) []hdr {
				return []hdr{{"Authorization", "Bearer " + f.wakeToken(t, "agents/e1")}}
			}, http.StatusForbidden, reasonWakeToken},
			{"service principal", func(*fenceFixture) []hdr {
				return []hdr{{"Authorization", "Bearer " + tb4SvcBearer}}
			}, http.StatusNoContent, ""},
			{"service principal with garbage token", func(*fenceFixture) []hdr {
				return []hdr{{"Authorization", "Bearer " + tb4SvcBearer}, {WriteTokenHeader, "garbage"}}
			}, http.StatusUnauthorized, reasonCredential},
		}
		for _, c := range cases {
			t.Run(mode.String()+" "+c.name, func(t *testing.T) {
				f := newFenceFixture(t, mode)
				f.create(t, path, true)
				before := tailOf(t, f.h, path)
				rec := f.post(path, []byte(`{"n":1}`), jsonAppend(c.headers(f)...)...)
				if rec.Code != c.wantStatus {
					t.Fatalf("status = %d body %q, want %d", rec.Code, rec.Body.String(), c.wantStatus)
				}
				if rec.Code >= http.StatusBadRequest {
					decodeFenceEnvelope(t, rec, c.wantStatus, c.wantReason)
					f.counter.only(t, c.wantReason)
					if after := tailOf(t, f.h, path); !after.Equal(before) {
						t.Fatalf("denied write mutated the stream: %s -> %s", before, after)
					}
					return
				}
				if fence, ok := f.store.lastFence(); !ok || fence != nil {
					t.Fatalf("open write reached the store with fence %v, want nil", fence)
				}
				if c.name == "anonymous" && !strings.Contains(f.logs.String(), "authz telemetry: fenced stream open class would be denied") {
					t.Errorf("insecure-mode anonymous open write logged no telemetry: %s", f.logs.String())
				}
			})
		}
	}
}

// TestFencedStreamBindsInInsecureMode pins the insecure-mode split (A.0): on a
// fenced stream the fence semantics bind in every mode — an invalid token is a
// 401 — while the same token on a stream that never opted in keeps today's
// telemetry posture, and an anonymous open write on the fenced stream lands.
func TestFencedStreamBindsInInsecureMode(t *testing.T) {
	f := newFenceFixture(t, auth.ModeInsecure)
	f.create(t, "/agents/e1/session", true)
	f.create(t, "/events/plain", false)
	producer := producerAt("8", "0")

	rec := f.post("/agents/e1/session", []byte(`{"n":1}`), jsonAppend(append(producer, hdr{WriteTokenHeader, "not-a-token"})...)...)
	decodeFenceEnvelope(t, rec, http.StatusUnauthorized, reasonCredential)
	f.counter.only(t, reasonCredential)

	rec = f.post("/events/plain", []byte(`{"n":1}`), jsonAppend(append(producer, hdr{WriteTokenHeader, "not-a-token"})...)...)
	if rec.Code != http.StatusOK {
		t.Fatalf("invalid token on an unfenced stream in insecure mode = %d body %q, want 200 (telemetry)", rec.Code, rec.Body.String())
	}

	rec = f.post("/agents/e1/session", []byte(`{"n":1}`), jsonAppend()...)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("anonymous open write on a fenced stream in insecure mode = %d body %q, want 204", rec.Code, rec.Body.String())
	}
}

// TestHandleAppendBoundProducerWithoutToken pins rule 5 of the rung as the
// handler discloses it: a service principal naming a producer id an accepted
// fenced write bound (the zombie that dropped its token) is refused 409 bound
// with the bound generation as Producer-Epoch and the terminal pair, while
// the same principal naming an unbound producer id lands on the open class.
func TestHandleAppendBoundProducerWithoutToken(t *testing.T) {
	const path = "/agents/e1/session"
	f := newFenceFixture(t, auth.ModeEnforce)
	f.create(t, path, true)
	service := hdr{"Authorization", "Bearer " + tb4SvcBearer}

	rec := f.post(path, []byte(`{"cmd":"inbox"}`), jsonAppend(hdr{"Producer-Id", "wake-reg-7"}, hdr{"Producer-Epoch", "0"}, hdr{"Producer-Seq", "0"}, service)...)
	if rec.Code != http.StatusOK {
		t.Fatalf("unbound open-class producer append = %d body %q, want 200", rec.Code, rec.Body.String())
	}
	before := tailOf(t, f.h, path)

	f.store.reject(store.FenceBound, 8, "")
	rec = f.post(path, []byte(`{"zombie":true}`), jsonAppend(append(producerAt("9", "0"), service)...)...)
	detail := decodeFenceEnvelope(t, rec, http.StatusConflict, "bound")
	if detail.Generation != 8 || detail.CurrentHolder != "" {
		t.Errorf("disclosure = %+v, want generation 8 and no holder", detail)
	}
	if got := rec.Header().Get(protocol.HeaderProducerEpoch); got != "8" {
		t.Errorf("Producer-Epoch = %q, want 8", got)
	}
	if exp, rcv := rec.Header().Get(protocol.HeaderProducerExpectedSeq), rec.Header().Get(protocol.HeaderProducerReceivedSeq); exp != "0" || rcv != "0" {
		t.Errorf("terminal pair = (%q, %q), want (0, 0)", exp, rcv)
	}
	if fence, ok := f.store.lastFence(); !ok || fence != nil {
		t.Fatalf("bound write reached the store with fence %v, want the open class (nil)", fence)
	}
	f.counter.only(t, "bound")
	if after := tailOf(t, f.h, path); !after.Equal(before) {
		t.Fatalf("bound write mutated the stream: %s -> %s", before, after)
	}
}

// TestHandleCreateWriteFence pins B.1/B.2 at create: Write-Fence: true is part
// of the idempotent-create comparison and echoed on 201/200, any other value
// is ignored, a store without the fence capability answers 501 and creates
// nothing, and a fenced create without an append authorizer is warn-logged.
func TestHandleCreateWriteFence(t *testing.T) {
	t.Run("echo and config match", func(t *testing.T) {
		f := newFenceFixture(t, auth.ModeInsecure)
		put := func(path string, headers ...hdr) *httptest.ResponseRecorder {
			req := httptest.NewRequest(http.MethodPut, path, nil)
			req.Header.Set("Content-Type", "application/json")
			for _, h := range headers {
				req.Header.Add(h[0], h[1])
			}
			rec := httptest.NewRecorder()
			f.h.ServeHTTP(rec, req)
			return rec
		}
		fenced := hdr{protocol.HeaderWriteFence, "true"}
		if rec := put("/agents/e1/session", fenced); rec.Code != http.StatusCreated || rec.Header().Get(protocol.HeaderWriteFence) != "true" {
			t.Fatalf("fenced create = %d Write-Fence %q, want 201 true", rec.Code, rec.Header().Get(protocol.HeaderWriteFence))
		}
		if rec := put("/agents/e1/session", fenced); rec.Code != http.StatusOK || rec.Header().Get(protocol.HeaderWriteFence) != "true" {
			t.Fatalf("fenced re-create = %d Write-Fence %q, want 200 true", rec.Code, rec.Header().Get(protocol.HeaderWriteFence))
		}
		if rec := put("/agents/e1/session"); rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "different configuration") {
			t.Fatalf("re-create without Write-Fence = %d %q, want 409 config mismatch", rec.Code, rec.Body.String())
		}
		if rec := put("/events/plain"); rec.Code != http.StatusCreated || rec.Header().Get(protocol.HeaderWriteFence) != "" {
			t.Fatalf("plain create = %d Write-Fence %q, want 201 and no echo", rec.Code, rec.Header().Get(protocol.HeaderWriteFence))
		}
		if rec := put("/events/plain", fenced); rec.Code != http.StatusConflict {
			t.Fatalf("fenced re-create of a plain stream = %d, want 409", rec.Code)
		}
		if rec := put("/events/other", hdr{protocol.HeaderWriteFence, "yes"}); rec.Code != http.StatusCreated || rec.Header().Get(protocol.HeaderWriteFence) != "" {
			t.Fatalf("create with Write-Fence: yes = %d Write-Fence %q, want 201 unfenced", rec.Code, rec.Header().Get(protocol.HeaderWriteFence))
		}
		if strings.Contains(f.logs.String(), "write-fenced stream created with no append authorizer configured") {
			t.Error("fenced create with an authorizer warned about a missing one")
		}
	})

	t.Run("501 without the capability", func(t *testing.T) {
		// MemoryStore itself implements store.WriteFenceStore (the WP2 parity
		// oracle), so the capability-less primary is an interface-only shim
		// that hides everything but store.Store.
		backend, err := segments.NewFileBackend(segments.ModeLocalFiles, t.TempDir(), 1<<20, nil)
		if err != nil {
			t.Fatal(err)
		}
		segmented, err := segments.New(newFencelessStore(store.NewMemoryStore()), segments.Options{Backend: backend, InitialState: segments.StateServing}, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = segmented.Close() })
		for _, c := range []struct {
			name string
			st   store.Store
		}{
			{"store without the capability", newFencelessStore(store.NewMemoryStore())},
			{"segments over a primary without it", segmented},
		} {
			t.Run(c.name, func(t *testing.T) {
				h := testHandler(time.Second, time.Second)
				h.Store = c.st
				rec := do(h, http.MethodPut, "/agents/e1/session", map[string]string{"Content-Type": "application/json", protocol.HeaderWriteFence: "true"}, nil)
				if rec.Code != http.StatusNotImplemented || strings.TrimSpace(rec.Body.String()) != store.ErrWriteFenceUnsupported.Error() {
					t.Fatalf("fenced create = %d %q, want 501 %q", rec.Code, rec.Body.String(), store.ErrWriteFenceUnsupported.Error())
				}
				if h.Store.Has("/agents/e1/session") {
					t.Fatal("refused fenced create left a stream behind")
				}
			})
		}
	})

	t.Run("warns without an append authorizer", func(t *testing.T) {
		f := newFenceFixture(t, auth.ModeInsecure)
		f.h.AppendAuth = nil
		rec := do(f.h, http.MethodPut, "/agents/e1/session", map[string]string{"Content-Type": "application/json", protocol.HeaderWriteFence: "true"}, nil)
		if rec.Code != http.StatusCreated {
			t.Fatalf("fenced create = %d %q, want 201", rec.Code, rec.Body.String())
		}
		if !strings.Contains(f.logs.String(), "level=WARN") || !strings.Contains(f.logs.String(), "write-fenced stream created with no append authorizer configured") {
			t.Errorf("fenced create without an authorizer did not warn: %s", f.logs.String())
		}
	})
}

// TestHandleHeadWriteFence pins B.2 on HEAD: Write-Fence: true on a fenced
// stream, the sealed generation and offset once a seal exists, and none of
// the three on a stream that never opted in.
func TestHandleHeadWriteFence(t *testing.T) {
	f := newFenceFixture(t, auth.ModeInsecure)
	f.create(t, "/agents/e1/session", true)
	f.create(t, "/agents/e2/session", true)
	f.create(t, "/events/plain", false)
	sealed := store.Offset{ByteOffset: 210}
	f.store.seals["/agents/e2/session"] = store.WriteFenceSeal{Present: true, Generation: 7, WakeID: "w_7", Offset: sealed}

	for _, c := range []struct {
		path                      string
		fence, generation, offset string
	}{
		{"/agents/e1/session", "true", "", ""},
		{"/agents/e2/session", "true", "7", sealed.String()},
		{"/events/plain", "", "", ""},
	} {
		rec := do(f.h, http.MethodHead, c.path, nil, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("HEAD %s = %d, want 200", c.path, rec.Code)
		}
		got := [3]string{
			rec.Header().Get(protocol.HeaderWriteFence),
			rec.Header().Get(protocol.HeaderWriteFenceSealedGeneration),
			rec.Header().Get(protocol.HeaderWriteFenceSealedOffset),
		}
		if got != [3]string{c.fence, c.generation, c.offset} {
			t.Errorf("HEAD %s write-fence headers = %q, want %q", c.path, got, [3]string{c.fence, c.generation, c.offset})
		}
	}
}

// TestAppendFenceRejectionCountedOnce pins R4.2: every fence rejection — the
// pre-store ones and the in-slot ones, the JSON envelope and the plaintext
// 400 — increments the counter exactly once, under its reason.
func TestAppendFenceRejectionCountedOnce(t *testing.T) {
	const path = "/agents/e1/session"
	cases := []struct {
		reason  string
		prepare func(f *fenceFixture)
		headers func(f *fenceFixture, tok string) []hdr
	}{
		{reasonCredential, func(*fenceFixture) {}, func(*fenceFixture, string) []hdr {
			return append(producerAt("8", "0"), hdr{WriteTokenHeader, "garbage"})
		}},
		{reasonShard, func(*fenceFixture) {}, func(f *fenceFixture, _ string) []hdr {
			tok, err := webhook.GenerateClaimWriteToken(f.key, "sub-1", "inc-1", 8, "w_1", "worker-A", 1,
				[]auth.StreamPath{mustStreamPath(t, "agents/e1/session")}, time.Now(), time.Hour, rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			return append(producerAt("8", "0"), hdr{WriteTokenHeader, tok})
		}},
		{"producer_required", func(*fenceFixture) {}, func(_ *fenceFixture, tok string) []hdr {
			return []hdr{{WriteTokenHeader, tok}}
		}},
		{reasonWakeToken, func(*fenceFixture) {}, func(f *fenceFixture, _ string) []hdr {
			return []hdr{{"Authorization", "Bearer " + f.wakeToken(t, "agents/e1")}}
		}},
		{reasonPrecheck, func(f *fenceFixture) {
			f.h.AppendAuth = fenceAuthorizer{WriteTokenAuthorizer: webhook.NewWriteTokenAuthorizer(f.key), key: f.key, fenced: true}
		}, func(_ *fenceFixture, tok string) []hdr {
			return append(producerAt("8", "0"), hdr{WriteTokenHeader, tok})
		}},
		{reasonStore, func(f *fenceFixture) { f.h.AppendAuth = webhook.NewWriteTokenAuthorizer(f.key) }, func(_ *fenceFixture, tok string) []hdr {
			return append(producerAt("8", "0"), hdr{WriteTokenHeader, tok})
		}},
		{"marker", func(f *fenceFixture) { f.store.reject(store.FenceMarker, 9, "worker-B") }, func(_ *fenceFixture, tok string) []hdr {
			return append(producerAt("8", "0"), hdr{WriteTokenHeader, tok})
		}},
	}
	for _, c := range cases {
		t.Run(c.reason, func(t *testing.T) {
			f := newFenceFixture(t, auth.ModeEnforce)
			f.create(t, path, true)
			tok := f.claimToken(t, 8, "worker-A", "agents/e1/session")
			c.prepare(f)
			rec := f.post(path, []byte(`{"n":1}`), jsonAppend(c.headers(f, tok)...)...)
			if rec.Code < 400 {
				t.Fatalf("status = %d, want a rejection", rec.Code)
			}
			f.counter.only(t, c.reason)
		})
	}
}

// TestAppendFencedClassPartialProducerHeadersCounted pins the partial-triple
// shape of the missing-producer-headers rejection on the fenced class (#183
// polish): the wire response is the base all-or-nothing 400 byte for byte —
// it runs before the complete-absence 400 — and the rejection counts under
// producer_required exactly once. The same partial triple on the open class
// keeps the base 400 uncounted.
func TestAppendFencedClassPartialProducerHeadersCounted(t *testing.T) {
	const path = "/agents/e1/session"
	const allOrNothing = "all producer headers (Producer-Id, Producer-Epoch, Producer-Seq) must be provided together"
	partial := []hdr{{"Producer-Id", "entity-agents/e1"}, {"Producer-Seq", "0"}}

	f := newFenceFixture(t, auth.ModeEnforce)
	f.create(t, path, true)
	tok := f.claimToken(t, 8, "worker-A", "agents/e1/session")
	rec := f.post(path, []byte(`{"n":1}`), jsonAppend(append(partial, hdr{WriteTokenHeader, tok})...)...)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body %q, want 400", rec.Code, rec.Body.String())
	}
	if got := strings.TrimSpace(rec.Body.String()); got != allOrNothing {
		t.Errorf("400 body = %q, want %q", got, allOrNothing)
	}
	f.counter.only(t, "producer_required")

	open := newFenceFixture(t, auth.ModeEnforce)
	open.create(t, path, true)
	rec = open.post(path, []byte(`{"n":1}`), jsonAppend(append(partial, hdr{"Authorization", "Bearer " + tb4SvcBearer})...)...)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("open-class status = %d body %q, want 400", rec.Code, rec.Body.String())
	}
	open.counter.only(t, "")
}

// TestCORSListsWriteFenceHeaders pins the addendum rule that every new
// request header is allowed and every new response header exposed, so a
// browser client can send and read the extension.
func TestCORSListsWriteFenceHeaders(t *testing.T) {
	rec := do(testHandler(time.Second, time.Second), http.MethodOptions, "/agents/e1/session", nil, nil)
	allow := rec.Header().Get("Access-Control-Allow-Headers")
	for _, name := range []string{protocol.HeaderWriteFence, protocol.HeaderWriteToken} {
		if !strings.Contains(allow, name) {
			t.Errorf("Access-Control-Allow-Headers = %q, missing %s", allow, name)
		}
	}
	expose := rec.Header().Get("Access-Control-Expose-Headers")
	for _, name := range []string{protocol.HeaderWriteFence, protocol.HeaderWriteFenceSealedGeneration, protocol.HeaderWriteFenceSealedOffset} {
		if !strings.Contains(expose, name) {
			t.Errorf("Access-Control-Expose-Headers = %q, missing %s", expose, name)
		}
	}
}

// TestErrorDetailReasonIsAdditive pins B.3: the reason field is omitted from
// every envelope that does not set it, so control-plane bodies are
// byte-identical to before.
func TestErrorDetailReasonIsAdditive(t *testing.T) {
	raw, err := json.Marshal(webhook.ErrorBody{Error: webhook.ErrorDetail{Code: webhook.ErrCodeFenced}})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"error":{"code":"FENCED"}}` {
		t.Fatalf("envelope without a reason = %s", raw)
	}
}
