package store

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func drainReadPages(t *testing.T, reader PageReader, path string, offset Offset, target, maxFrames int, afterFirst func(ReadSnapshot)) ([]Message, ReadPage) {
	t.Helper()

	var (
		all      []Message
		snapshot *ReadSnapshot
		next     = offset
		last     ReadPage
	)
	for pageNum := 0; ; pageNum++ {
		page, err := reader.ReadPage(context.Background(), path, next, ReadPageOptions{
			TargetBytes: target,
			MaxFrames:   maxFrames,
			Snapshot:    snapshot,
		})
		if err != nil {
			t.Fatalf("ReadPage(%d): %v", pageNum, err)
		}
		if snapshot == nil {
			captured := page.Snapshot
			snapshot = &captured
			if afterFirst != nil {
				afterFirst(captured)
			}
		} else if page.Snapshot != *snapshot {
			t.Fatalf("snapshot changed on page %d: got %+v want %+v", pageNum, page.Snapshot, *snapshot)
		}

		if len(page.Messages) > maxFrames {
			t.Fatalf("page %d returned %d frames, max %d", pageNum, len(page.Messages), maxFrames)
		}
		if len(page.Messages) > 1 && page.Stats.ReturnedBytes > target {
			t.Fatalf("page %d returned %d bytes, target %d", pageNum, page.Stats.ReturnedBytes, target)
		}
		all = append(all, page.Messages...)
		last = page
		if page.UpToDate {
			return all, last
		}
		if page.NextOffset.Equal(next) {
			t.Fatalf("page %d made no progress at %s", pageNum, next)
		}
		next = page.NextOffset
		if pageNum > 10000 {
			t.Fatal("too many pages")
		}
	}
}

func TestMemoryReadPageBoundaries(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		initial     []byte
		target      int
		maxFrames   int
		wantPages   int
	}{
		{name: "empty binary", contentType: "application/octet-stream", target: 4, maxFrames: 8, wantPages: 1},
		{name: "one binary frame", contentType: "application/octet-stream", initial: []byte("abcd"), target: 4, maxFrames: 8, wantPages: 1},
		{name: "oversized binary frame", contentType: "application/octet-stream", initial: []byte("abcde"), target: 4, maxFrames: 8, wantPages: 1},
		{name: "exact JSON boundary", contentType: "application/json", initial: []byte(`[1,22,333]`), target: 3, maxFrames: 8, wantPages: 2},
		{name: "one byte over JSON boundary", contentType: "application/json", initial: []byte(`[1,22,333]`), target: 2, maxFrames: 8, wantPages: 3},
		{name: "frame cap", contentType: "application/json", initial: []byte(`[1,2,3,4,5]`), target: 1024, maxFrames: 2, wantPages: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewMemoryStore()
			if _, _, err := s.Create("/page", CreateOptions{
				ContentType: tc.contentType,
				InitialData: tc.initial,
			}); err != nil {
				t.Fatal(err)
			}

			var pages int
			var all []Message
			var snapshot *ReadSnapshot
			next := ZeroOffset
			for {
				page, err := s.ReadPage(context.Background(), "/page", next, ReadPageOptions{
					TargetBytes: tc.target,
					MaxFrames:   tc.maxFrames,
					Snapshot:    snapshot,
				})
				if err != nil {
					t.Fatal(err)
				}
				pages++
				if snapshot == nil {
					captured := page.Snapshot
					snapshot = &captured
				}
				all = append(all, page.Messages...)
				if page.UpToDate {
					break
				}
				next = page.NextOffset
			}

			if pages != tc.wantPages {
				t.Fatalf("pages = %d, want %d", pages, tc.wantPages)
			}
			got := bytes.Join(messageData(all), nil)
			logical, _, err := s.Read("/page", ZeroOffset)
			if err != nil {
				t.Fatal(err)
			}
			want := bytes.Join(messageData(logical), nil)
			if !bytes.Equal(got, want) {
				t.Fatalf("concatenated bytes = %q, want %q", got, want)
			}
		})
	}
}

func TestMemoryReadPageTargets(t *testing.T) {
	for _, target := range []int{256 << 10, 1 << 20, 4 << 20} {
		t.Run(string(rune(target)), func(t *testing.T) {
			s := NewMemoryStore()
			frame := bytes.Repeat([]byte("x"), 64<<10)
			body := []byte("[")
			for i := 0; i < 80; i++ {
				if i > 0 {
					body = append(body, ',')
				}
				body = append(body, '"')
				body = append(body, frame...)
				body = append(body, '"')
			}
			body = append(body, ']')
			if _, _, err := s.Create("/targets", CreateOptions{ContentType: "application/json", InitialData: body}); err != nil {
				t.Fatal(err)
			}

			all, last := drainReadPages(t, s, "/targets", ZeroOffset, target, DefaultReadPageFrames, nil)
			logical, _, err := s.Read("/targets", ZeroOffset)
			if err != nil {
				t.Fatal(err)
			}
			assertMessagesEqual(t, all, logical)
			if !last.UpToDate {
				t.Fatal("last page is not up to date")
			}
		})
	}
}

func TestMemoryReadPageSnapshotExcludesConcurrentAppend(t *testing.T) {
	s := NewMemoryStore()
	if _, _, err := s.Create("/snapshot", CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2,3]`),
	}); err != nil {
		t.Fatal(err)
	}
	before, _, err := s.Read("/snapshot", ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}

	all, last := drainReadPages(t, s, "/snapshot", ZeroOffset, 1, 1, func(snapshot ReadSnapshot) {
		if _, err := s.Append("/snapshot", []byte(`[4,5]`), AppendOptions{ContentType: "application/json"}); err != nil {
			t.Fatal(err)
		}
		if snapshot.Tail.Equal(s.streams["/snapshot"].metadata.CurrentOffset) {
			t.Fatal("append did not advance beyond snapshot")
		}
	})
	assertMessagesEqual(t, all, before)
	if !last.NextOffset.Equal(last.Snapshot.Tail) {
		t.Fatalf("last offset %s, snapshot tail %s", last.NextOffset, last.Snapshot.Tail)
	}

	after, _ := drainReadPages(t, s, "/snapshot", last.NextOffset, 1, 1, nil)
	if len(after) != 2 || string(after[0].Data) != "4" || string(after[1].Data) != "5" {
		t.Fatalf("post-snapshot page = %+v", after)
	}
}

func TestMemoryReadPageForkCrossesBoundary(t *testing.T) {
	s := NewMemoryStore()
	if _, _, err := s.Create("/source", CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,22,333]`),
	}); err != nil {
		t.Fatal(err)
	}
	source, _, err := s.Read("/source", ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	forkOffset := source[1].Offset
	if _, _, err := s.Create("/fork", CreateOptions{
		ContentType: "application/json",
		ForkedFrom:  "/source",
		ForkOffset:  &forkOffset,
		InitialData: []byte(`[4444,55555]`),
	}); err != nil {
		t.Fatal(err)
	}

	all, _ := drainReadPages(t, s, "/fork", ZeroOffset, 3, 2, nil)
	if got, want := string(bytes.Join(messageData(all), []byte(","))), "1,22,4444,55555"; got != want {
		t.Fatalf("fork pages = %q, want %q", got, want)
	}
	for i := 1; i < len(all); i++ {
		if !all[i-1].Offset.LessThan(all[i].Offset) {
			t.Fatalf("offsets not increasing at %d: %s then %s", i, all[i-1].Offset, all[i].Offset)
		}
	}
}

func TestMemoryReadPageClosedFinalAppend(t *testing.T) {
	s := NewMemoryStore()
	if _, _, err := s.Create("/closed", CreateOptions{ContentType: "application/json"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append("/closed", []byte(`[1,2]`), AppendOptions{ContentType: "application/json", Close: true}); err != nil {
		t.Fatal(err)
	}
	all, last := drainReadPages(t, s, "/closed", ZeroOffset, 1, 1, nil)
	if len(all) != 2 || !last.Snapshot.Closed || !last.UpToDate {
		t.Fatalf("closed page result: messages=%d snapshot=%+v upToDate=%v", len(all), last.Snapshot, last.UpToDate)
	}
}

func TestMemoryReadPageOffsetsAndCancellation(t *testing.T) {
	s := NewMemoryStore()
	if _, _, err := s.Create("/offsets", CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2,3]`),
	}); err != nil {
		t.Fatal(err)
	}
	messages, _, err := s.Read("/offsets", ZeroOffset)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name   string
		offset Offset
		want   int
	}{
		{name: "start", offset: ZeroOffset, want: 3},
		{name: "middle", offset: messages[0].Offset, want: 2},
		{name: "tail", offset: messages[2].Offset, want: 0},
		{name: "beyond tail", offset: messages[2].Offset.Add(10), want: 0},
		{name: "stale read sequence", offset: Offset{ReadSeq: 1}, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := drainReadPages(t, s, "/offsets", tc.offset, 1, 1, nil)
			if len(got) != tc.want {
				t.Fatalf("message count = %d, want %d", len(got), tc.want)
			}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.ReadPage(ctx, "/offsets", ZeroOffset, ReadPageOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ReadPage error = %v", err)
	}
}

func TestMemoryReadPageRenewsSlidingTTLOncePerSnapshot(t *testing.T) {
	clock := NewFakeClock(time.Unix(100, 0))
	s := NewMemoryStore(WithClock(clock))
	ttl := int64(10)
	if _, _, err := s.Create("/ttl", CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2]`),
		TTLSeconds:  &ttl,
	}); err != nil {
		t.Fatal(err)
	}

	first, err := s.ReadPage(context.Background(), "/ttl", ZeroOffset, ReadPageOptions{TargetBytes: 1, MaxFrames: 1})
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(9 * time.Second)
	second, err := s.ReadPage(context.Background(), "/ttl", first.NextOffset, ReadPageOptions{
		TargetBytes: 1,
		MaxFrames:   1,
		Snapshot:    &first.Snapshot,
	})
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(9 * time.Second)
	if _, err := s.ReadPage(
		context.Background(),
		"/ttl",
		second.NextOffset,
		ReadPageOptions{Snapshot: &first.Snapshot},
	); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("continuation extended sliding TTL: %v", err)
	}
}

func TestMemoryReadPageAbsoluteExpiryAndNonExpiring(t *testing.T) {
	now := time.Unix(1_000, 0)
	clock := NewFakeClock(now)
	s := NewMemoryStore(WithClock(clock))
	expiresAt := now.Add(10 * time.Second)
	if _, _, err := s.Create("/absolute", CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: []byte("frame"),
		ExpiresAt:   &expiresAt,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Create("/forever", CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: []byte("frame"),
	}); err != nil {
		t.Fatal(err)
	}

	// A page read is access, but absolute expiry never slides.
	if _, err := s.ReadPage(context.Background(), "/absolute", ZeroOffset, ReadPageOptions{}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(11 * time.Second)
	if _, err := s.ReadPage(context.Background(), "/absolute", ZeroOffset, ReadPageOptions{}); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("absolute-expiry page error = %v, want ErrStreamNotFound", err)
	}
	if _, err := s.ReadPage(context.Background(), "/forever", ZeroOffset, ReadPageOptions{}); err != nil {
		t.Fatalf("non-expiring page failed after time advance: %v", err)
	}
}

func messageData(messages []Message) [][]byte {
	out := make([][]byte, len(messages))
	for i := range messages {
		out[i] = messages[i].Data
	}
	return out
}

func assertMessagesEqual(t *testing.T, got, want []Message) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("message count = %d, want %d", len(got), len(want))
	}
	for i := range got {
		if !got[i].Offset.Equal(want[i].Offset) || !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("message %d = {%q %s}, want {%q %s}", i, got[i].Data, got[i].Offset, want[i].Data, want[i].Offset)
		}
	}
}
