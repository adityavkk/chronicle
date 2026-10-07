package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

type aggregate struct {
	Value int64
	Count int
}

type testReporter interface {
	Helper()
	Fatal(...any)
}

func projection() Projection[aggregate] {
	return Projection[aggregate]{
		Version: "ordered-fold/v1",
		Initial: func() aggregate { return aggregate{} },
		Apply: func(s aggregate, m store.Message) (aggregate, error) {
			var n int64
			if err := json.Unmarshal(m.Data, &n); err != nil {
				return s, err
			}
			return aggregate{Value: 3*s.Value + n, Count: s.Count + 1}, nil
		},
	}
}

func appendJSON(t testReporter, s store.Store, path, body string) store.Offset {
	t.Helper()
	r, err := s.Append(path, []byte(body), store.AppendOptions{ContentType: "application/json"})
	if err != nil {
		t.Fatal(err)
	}
	return r.Offset
}

func createJSON(t testReporter, s store.Store, path string) {
	t.Helper()
	if _, _, err := s.Create(path, store.CreateOptions{ContentType: "application/json"}); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureSplitProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		events := rapid.SliceOfN(rapid.Int64Range(-100, 100), 1, 35).Draw(t, "events")
		split := rapid.IntRange(0, len(events)).Draw(t, "checkpoint-boundary")
		s := store.NewMemoryStore()
		defer s.Close()
		createJSON(t, s, "/a")
		for _, event := range events[:split] {
			appendJSON(t, s, "/a", fmt.Sprint(event))
		}
		_, image, _, err := Capture(context.Background(), s, "/a", projection(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events[split:] {
			appendJSON(t, s, "/a", fmt.Sprint(event))
		}
		got, _, stats, err := Capture(context.Background(), s, "/a", projection(), &image)
		// Independent polynomial evaluation, not a second call to Apply.
		var want int64
		weight := int64(1)
		for i := len(events) - 1; i >= 0; i-- {
			want += weight * events[i]
			weight *= 3
		}
		if err != nil || got != (aggregate{Value: want, Count: len(events)}) || !stats.Restored || stats.Frames != len(events)-split {
			t.Fatalf("got %+v, stats %+v, err %v; want value %d, count %d, suffix %d", got, stats, err, want, len(events), len(events)-split)
		}
	})
}

func TestInvalidImagesFallBack(t *testing.T) {
	s := store.NewMemoryStore()
	defer s.Close()
	createJSON(t, s, "/a")
	appendJSON(t, s, "/a", "[2,7]")
	_, image, _, err := Capture(t.Context(), s, "/a", projection(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Image){
		"digest":       func(i *Image) { i.State = json.RawMessage(`{"Value":999}`) },
		"offset":       func(i *Image) { i.Offset = store.ZeroOffset.String() },
		"schema":       func(i *Image) { i.Projection = "v2" },
		"incarnation":  func(i *Image) { i.Incarnation = "other" },
		"source":       func(i *Image) { i.Path = "/other" },
		"format":       func(i *Image) { i.Format = 2 },
		"future":       func(i *Image) { i.Offset = store.Offset{ByteOffset: 999}.String(); i.SHA256 = i.digest() },
		"decode-state": func(i *Image) { i.State = json.RawMessage(`"not an aggregate"`); i.SHA256 = i.digest() },
	} {
		t.Run(name, func(t *testing.T) {
			bad := image
			mutate(&bad)
			got, _, stats, err := Capture(t.Context(), s, "/a", projection(), &bad)
			if err != nil || got != (aggregate{13, 2}) || stats.Restored || stats.Frames != 2 {
				t.Fatalf("got %+v, %+v, %v", got, stats, err)
			}
		})
	}
}

type hookedReader struct {
	store.PageReader
	beforePage func()
}

func (r hookedReader) ReadPage(ctx context.Context, path string, off store.Offset, opts store.ReadPageOptions) (store.ReadPage, error) {
	if opts.Snapshot != nil {
		r.beforePage()
	}
	opts.MaxFrames = 1 // force paging so tail/incarnation fencing is exercised
	return r.PageReader.ReadPage(ctx, path, off, opts)
}

func TestAppendDuringCaptureAndRecreate(t *testing.T) {
	s := store.NewMemoryStore()
	defer s.Close()
	createJSON(t, s, "/a")
	oldTail := appendJSON(t, s, "/a", "[2,7]")
	first := true
	r := hookedReader{s, func() {
		if first {
			first = false
			appendJSON(t, s, "/a", "-4")
		}
	}}
	got, image, _, err := Capture(t.Context(), r, "/a", projection(), nil)
	if err != nil || got != (aggregate{13, 2}) || image.Offset != oldTail.String() {
		t.Fatalf("capture crossed fixed tail: %+v, %+v, %v", got, image, err)
	}
	got, current, stats, err := Capture(t.Context(), s, "/a", projection(), &image)
	if err != nil || got != (aggregate{35, 3}) || stats.Frames != 1 {
		t.Fatalf("suffix skipped or duplicated: %+v, %+v, %v", got, stats, err)
	}
	r.beforePage = func() {
		if err := s.Delete("/a"); err != nil {
			t.Fatal(err)
		}
		createJSON(t, s, "/a")
		appendJSON(t, s, "/a", "11")
	}
	// Even an empty suffix must validate the source after checkpoint restore.
	_, failed, _, err := Capture(t.Context(), r, "/a", projection(), &current)
	if !errors.Is(err, store.ErrReadSnapshotChanged) || failed.Format != 0 {
		t.Fatalf("recreated source accepted: %+v, %v", failed, err)
	}
	got, _, stats, err = Capture(t.Context(), s, "/a", projection(), &current)
	if err != nil || got != (aggregate{11, 1}) || stats.Restored {
		t.Fatalf("old incarnation reused: %+v, %+v, %v", got, stats, err)
	}
}

func TestFailureDoesNotProduceImage(t *testing.T) {
	s := store.NewMemoryStore()
	defer s.Close()
	createJSON(t, s, "/a")
	appendJSON(t, s, "/a", `[2,"bad",7]`)
	_, image, stats, err := Capture(t.Context(), s, "/a", projection(), nil)
	if err == nil || image.Format != 0 || stats.Frames != 1 {
		t.Fatalf("partial fold was publishable: %+v, %+v, %v", image, stats, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, _, err := Capture(ctx, s, "/a", projection(), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	large := Projection[string]{Version: "large", Initial: func() string { return strings.Repeat("x", MaxImageBytes) }, Apply: func(s string, _ store.Message) (string, error) { return s, nil }}
	if _, image, _, err := Capture(t.Context(), s, "/a", large, nil); err == nil || image.Format != 0 {
		t.Fatalf("oversized image accepted: %v", err)
	}
}

func BenchmarkCapture(b *testing.B) {
	s := store.NewMemoryStore()
	defer s.Close()
	createJSON(b, s, "/bench")
	for range 10000 {
		appendJSON(b, s, "/bench", "1")
	}
	_, image, _, err := Capture(b.Context(), s, "/bench", projection(), nil)
	if err != nil {
		b.Fatal(err)
	}
	for range 10 {
		appendJSON(b, s, "/bench", "-2")
	}
	for name, prior := range map[string]*Image{"full": nil, "snapshot-tail": &image} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _, stats, err := Capture(b.Context(), s, "/bench", projection(), prior)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(stats.Frames), "frames/op")
			}
		})
	}
}
