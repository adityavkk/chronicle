package redis

import (
	"context"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func TestIntegrationWaitWakesOnAppend(t *testing.T) {
	s := newTestStore(t)
	path := testPath("wait-wake")
	mustCreate(t, s, path, store.CreateOptions{})

	go func() {
		time.Sleep(300 * time.Millisecond)
		_, _ = s.Append(path, []byte("ping"), store.AppendOptions{})
	}()

	start := time.Now()
	msgs, timedOut, closed, err := s.WaitForMessages(context.Background(), path, store.ZeroOffset, 5*time.Second)
	elapsed := time.Since(start)
	if err != nil || timedOut || closed {
		t.Fatalf("wait: timedOut=%v closed=%v err=%v", timedOut, closed, err)
	}
	if len(msgs) != 1 || string(msgs[0].Data) != "ping" {
		t.Fatalf("wait messages: %v", msgs)
	}
	if elapsed >= time.Second {
		t.Errorf("wakeup took %v, want <1s (pub/sub path, not the defensive poll)", elapsed)
	}
}

func TestIntegrationWaitImmediateData(t *testing.T) {
	s := newTestStore(t)
	path := testPath("wait-immediate")
	mustCreate(t, s, path, store.CreateOptions{})
	mustAppend(t, s, path, []byte("already"), store.AppendOptions{})

	msgs, timedOut, closed, err := s.WaitForMessages(context.Background(), path, store.ZeroOffset, time.Second)
	if err != nil || timedOut || closed || len(msgs) != 1 {
		t.Fatalf("immediate: msgs=%v timedOut=%v closed=%v err=%v", msgs, timedOut, closed, err)
	}
}

func TestIntegrationWaitTimeout(t *testing.T) {
	s := newTestStore(t)
	path := testPath("wait-timeout")
	mustCreate(t, s, path, store.CreateOptions{})
	tail := mustAppend(t, s, path, []byte("x"), store.AppendOptions{}).Offset

	msgs, timedOut, closed, err := s.WaitForMessages(context.Background(), path, tail, 300*time.Millisecond)
	if err != nil || !timedOut || closed || len(msgs) != 0 {
		t.Fatalf("timeout: msgs=%v timedOut=%v closed=%v err=%v", msgs, timedOut, closed, err)
	}
}

func TestIntegrationWaitClosedDuringWait(t *testing.T) {
	s := newTestStore(t)
	path := testPath("wait-close")
	mustCreate(t, s, path, store.CreateOptions{})
	tail := mustAppend(t, s, path, []byte("x"), store.AppendOptions{}).Offset

	go func() {
		time.Sleep(300 * time.Millisecond)
		_, _ = s.CloseStream(path)
	}()

	start := time.Now()
	msgs, timedOut, closed, err := s.WaitForMessages(context.Background(), path, tail, 5*time.Second)
	if err != nil || timedOut || !closed || len(msgs) != 0 {
		t.Fatalf("closed during wait: msgs=%v timedOut=%v closed=%v err=%v", msgs, timedOut, closed, err)
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Errorf("close wakeup took %v", elapsed)
	}
}

func TestIntegrationWaitClosedAtTailFastPath(t *testing.T) {
	s := newTestStore(t)
	path := testPath("wait-closed-fast")
	mustCreate(t, s, path, store.CreateOptions{})
	tail := mustAppend(t, s, path, []byte("x"), store.AppendOptions{Close: true}).Offset

	start := time.Now()
	msgs, timedOut, closed, err := s.WaitForMessages(context.Background(), path, tail, 5*time.Second)
	if err != nil || timedOut || !closed || len(msgs) != 0 {
		t.Fatalf("closed fast path: msgs=%v timedOut=%v closed=%v err=%v", msgs, timedOut, closed, err)
	}
	if elapsed := time.Since(start); elapsed >= 200*time.Millisecond {
		t.Errorf("fast path took %v, should return immediately", elapsed)
	}

	// Closed but data pending: messages, not streamClosed.
	msgs, timedOut, closed, err = s.WaitForMessages(context.Background(), path, store.ZeroOffset, time.Second)
	if err != nil || timedOut || closed || len(msgs) != 1 {
		t.Fatalf("closed with pending data: msgs=%v timedOut=%v closed=%v err=%v", msgs, timedOut, closed, err)
	}
}

func TestIntegrationWaitContextCancel(t *testing.T) {
	s := newTestStore(t)
	path := testPath("wait-cancel")
	mustCreate(t, s, path, store.CreateOptions{})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	_, timedOut, closed, err := s.WaitForMessages(ctx, path, store.ZeroOffset, 5*time.Second)
	if err != context.Canceled || timedOut || closed {
		t.Fatalf("cancel: timedOut=%v closed=%v err=%v", timedOut, closed, err)
	}
}

func TestIntegrationWaitMissingStream(t *testing.T) {
	s := newTestStore(t)
	_, _, _, err := s.WaitForMessages(context.Background(), testPath("wait-missing"), store.ZeroOffset, time.Second)
	if err != store.ErrStreamNotFound {
		t.Fatalf("missing stream: %v", err)
	}
}

// TestIntegrationWaitForkInheritedRangeNeverWaits: an offset in a fork's
// inherited range can only be served by the source, and source appends do
// not notify fork waiters — the wait must return empty immediately rather
// than hang. Simulated by wiping the source's data plane so the inherited
// read comes back empty.
func TestIntegrationWaitForkInheritedRangeNeverWaits(t *testing.T) {
	s := newTestStore(t)
	src := testPath("wait-fork-src")
	mustCreate(t, s, src, store.CreateOptions{})
	mustAppend(t, s, src, []byte("hello"), store.AppendOptions{})

	fork := testPath("wait-fork")
	mustCreate(t, s, fork, store.CreateOptions{ForkedFrom: src})

	// Vanish the source's frames (simulates a reaped source).
	if err := testClient.Del(context.Background(), msgKey(src)).Err(); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	msgs, timedOut, closed, err := s.WaitForMessages(context.Background(), fork, store.ZeroOffset, 5*time.Second)
	if err != nil || timedOut || closed || len(msgs) != 0 {
		t.Fatalf("inherited-range wait: msgs=%v timedOut=%v closed=%v err=%v", msgs, timedOut, closed, err)
	}
	if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
		t.Errorf("inherited-range wait took %v, must not block", elapsed)
	}
}
