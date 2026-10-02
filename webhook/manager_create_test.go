package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/correlation"
)

// createHookStore is the create hook's view of the Store: List, the pattern
// read, Link. Every other method is the nil embedded Store, so an unexpected
// call (a per-id Get, a GetMany) panics the test — the regression this file
// exists to catch is the hook falling back to one round trip per subscription.
type createHookStore struct {
	Store
	ids      []string
	listErr  error
	read     PatternRead
	readErr  error
	reads    int
	readIDs  []string
	linkErr  map[string]error
	linkSubs []string     // Link calls that succeeded, in order
	links    []StreamLink // what each successful Link wrote
}

func (s *createHookStore) List() ([]string, error) { return slices.Clone(s.ids), s.listErr }

func (s *createHookStore) PatternSubscriptions(ids []string) (PatternRead, error) {
	s.reads++
	s.readIDs = slices.Clone(ids)
	return s.read, s.readErr
}

func (s *createHookStore) Link(id, path string, kind LinkType, offset string) error {
	if err := s.linkErr[id]; err != nil {
		return err
	}
	s.linkSubs = append(s.linkSubs, id)
	s.links = append(s.links, StreamLink{Path: path, LinkType: kind, AckedOffset: offset})
	return nil
}

// hookManager builds the bare Manager the hook needs (NewManager would load
// signing keys through the nil Store) and captures its log as JSON lines.
func hookManager(s Store) (*Manager, *bytes.Buffer) {
	var logs bytes.Buffer
	m := &Manager{
		store:   s,
		streams: &fakeStreams{},
		log:     slog.New(slog.NewJSONHandler(&logs, nil)),
		metrics: NopMetrics{},
		now:     time.Now,
	}
	return m, &logs
}

// completionLine decodes the hook's stream_create_subscriptions_completed line.
func completionLine(t *testing.T, logs *bytes.Buffer) map[string]any {
	t.Helper()
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if rec["event"] == "stream_create_subscriptions_completed" {
			return rec
		}
	}
	t.Fatalf("no completion line in %s", logs.String())
	return nil
}

// TestOnStreamCreatedReadsPatternsOnceAndLinksMatches is the batching
// regression: 83 listed subscriptions cost exactly one pattern read of exactly
// List()'s ids, no per-id Get, and the 80 matching ones are linked as glob
// links at the beginning offset; the completion line carries the request id,
// the counts and the phase durations.
func TestOnStreamCreatedReadsPatternsOnceAndLinksMatches(t *testing.T) {
	s := &createHookStore{}
	var want []string
	for i := range 80 {
		id := strconv.Itoa(i)
		s.ids = append(s.ids, id)
		s.read.Subs = append(s.read.Subs, PatternSubscription{ID: id, Pattern: "events/*"})
		want = append(want, id)
	}
	s.ids = append(s.ids, "other", "explicit", "deleted")
	s.read.Subs = append(s.read.Subs, PatternSubscription{ID: "other", Pattern: "other/*"})
	s.read.Missing = 1 // "deleted": listed, but its hash is gone
	m, logs := hookManager(s)

	m.OnStreamCreatedWithContext(correlation.WithRequestID(context.Background(), "req-create-1"), "events/new")

	if s.reads != 1 || !slices.Equal(s.readIDs, s.ids) {
		t.Fatalf("pattern reads = %d with ids %v; want one read of exactly List()'s ids", s.reads, s.readIDs)
	}
	if !slices.Equal(s.linkSubs, want) {
		t.Fatalf("linked %v, want the 80 matching subscriptions in List order", s.linkSubs)
	}
	begin := (&fakeStreams{}).BeginningOffset()
	for _, l := range s.links {
		if l.Path != "events/new" || l.LinkType != LinkGlob || l.AckedOffset != begin {
			t.Fatalf("link %+v: want a glob link to events/new at the beginning offset", l)
		}
	}
	rec := completionLine(t, logs)
	for k, v := range map[string]any{
		"outcome": "success", "request_id": "req-create-1", "stream_path": "events/new",
		"subscriptions": 83.0, "pattern_subscriptions": 81.0, "missing": 1.0, "read_failures": 0.0,
		"matched": 80.0, "linked": 80.0, "link_failures": 0.0,
	} {
		if rec[k] != v {
			t.Fatalf("completion line %s = %v, want %v (%s)", k, rec[k], v, logs.String())
		}
	}
	for _, k := range []string{"list_ms", "read_ms", "link_ms", "duration_ms"} {
		if _, ok := rec[k].(float64); !ok {
			t.Fatalf("completion line lacks %s: %s", k, logs.String())
		}
	}
}

// TestOnStreamCreatedOutcomes pins the best-effort contract: a failed list
// stops the hook, a failed read links nothing, a partial read links what was
// read, one failed Link does not stop the others, and a listed id without a
// hash (deleted concurrently) is counted, not treated as a failure.
func TestOnStreamCreatedOutcomes(t *testing.T) {
	two := []PatternSubscription{{ID: "a", Pattern: "events/*"}, {ID: "b", Pattern: "events/*"}}
	cases := []struct {
		name        string
		store       *createHookStore
		wantOutcome string
		wantLinked  []string
		wantReads   int
	}{
		{"list failure", &createHookStore{ids: []string{"a"}, listErr: errors.New("down")}, "list_failed", nil, 0},
		{"every read failed", &createHookStore{ids: []string{"a", "b"}, read: PatternRead{Failed: 2}, readErr: errors.New("timeout")}, "read_failed", nil, 1},
		{"partial read links what was read", &createHookStore{ids: []string{"a", "b", "c"}, read: PatternRead{Subs: two, Failed: 1}, readErr: errors.New("wrongtype")}, "partial_failure", []string{"a", "b"}, 1},
		{"one link failure does not stop the rest", &createHookStore{ids: []string{"a", "b"}, read: PatternRead{Subs: two}, linkErr: map[string]error{"a": errors.New("busy")}}, "partial_failure", []string{"b"}, 1},
		{"missing hashes are not failures", &createHookStore{ids: []string{"a", "gone"}, read: PatternRead{Subs: two[:1], Missing: 1}}, "success", []string{"a"}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, logs := hookManager(tc.store)
			m.OnStreamCreated("events/new")
			if tc.store.reads != tc.wantReads {
				t.Fatalf("reads = %d, want %d", tc.store.reads, tc.wantReads)
			}
			if !slices.Equal(tc.store.linkSubs, tc.wantLinked) {
				t.Fatalf("linked %v, want %v", tc.store.linkSubs, tc.wantLinked)
			}
			if rec := completionLine(t, logs); rec["outcome"] != tc.wantOutcome {
				t.Fatalf("outcome = %v, want %s (%s)", rec["outcome"], tc.wantOutcome, logs.String())
			}
		})
	}
}
