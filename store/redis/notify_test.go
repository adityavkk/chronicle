package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func TestNotificationEventPayloads(t *testing.T) {
	tests := []struct {
		item any
		want store.NotificationEvent
	}{
		{item: &goredis.Message{Payload: "a"}, want: store.NotificationAppend},
		{item: &goredis.Message{Payload: "c"}, want: store.NotificationClose},
		{item: &goredis.Message{Payload: "d"}, want: store.NotificationDelete},
		{item: &goredis.Subscription{Kind: "subscribe"}, want: store.NotificationReconnect},
	}
	for _, test := range tests {
		if got := notificationEvent(test.item); got != test.want {
			t.Errorf("notificationEvent(%T) = %v, want %v", test.item, got, test.want)
		}
	}
}

func TestCoalesceNotificationPreservesTerminalAndReconnectSignals(t *testing.T) {
	if got := coalesceNotification(store.NotificationAppend, store.NotificationClose); got != store.NotificationClose {
		t.Fatalf("append + close = %v", got)
	}
	if got := coalesceNotification(store.NotificationClose, store.NotificationReconnect); got != store.NotificationReconnect {
		t.Fatalf("close + reconnect = %v", got)
	}
	if got := coalesceNotification(store.NotificationReconnect, store.NotificationDelete); got != store.NotificationDelete {
		t.Fatalf("reconnect + delete = %v", got)
	}
}

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

func TestIntegrationPersistentNotificationWakesAndCancels(t *testing.T) {
	s := newTestStore(t)
	path := testPath("persistent-notification")
	mustCreate(t, s, path, store.CreateOptions{})

	sub, err := s.SubscribeNotifications(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close() //nolint:errcheck

	go func() {
		time.Sleep(100 * time.Millisecond)
		_, _ = s.Append(path, []byte("wake"), store.AppendOptions{})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if event, err := sub.Wait(ctx); err != nil {
		t.Fatalf("wait for append notification: %v", err)
	} else if event != store.NotificationAppend {
		t.Fatalf("notification = %v, want append", event)
	}
	msgs, _, err := s.Read(path, store.ZeroOffset)
	if err != nil || len(msgs) != 1 || string(msgs[0].Data) != "wake" {
		t.Fatalf("read after persistent wake: msgs=%v err=%v", msgs, err)
	}

	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := sub.Wait(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait = %v, want context.Canceled", err)
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
	if !errors.Is(err, context.Canceled) || timedOut || closed {
		t.Fatalf("cancel: timedOut=%v closed=%v err=%v", timedOut, closed, err)
	}
}

func TestIntegrationWaitMissingStream(t *testing.T) {
	s := newTestStore(t)
	_, _, _, err := s.WaitForMessages(context.Background(), testPath("wait-missing"), store.ZeroOffset, time.Second)
	if !errors.Is(err, store.ErrStreamNotFound) {
		t.Fatalf("missing stream: %v", err)
	}
}

// TestIntegrationWaitForkInheritedRangeNeverWaits: an offset in a fork's
// inherited range can only be served by the source. If that acknowledged data
// vanishes, the read must fail loudly rather than wait or skip bytes.
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
	if !errors.Is(err, store.ErrReadDataMissing) || timedOut || closed || len(msgs) != 0 {
		t.Fatalf("inherited-range missing data: msgs=%v timedOut=%v closed=%v err=%v", msgs, timedOut, closed, err)
	}
	if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
		t.Errorf("inherited-range wait took %v, must not block", elapsed)
	}
}
