package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// defensivePollInterval is the fallback re-read cadence while blocked on
// pub/sub: Redis pub/sub is fire-and-forget, so a dropped wakeup (connection
// churn) is recovered within one tick instead of hanging until timeout.
const defensivePollInterval = time.Second

type notificationSubscription struct {
	pubsub *goredis.PubSub
	wake   <-chan any
}

// SubscribeNotifications opens and confirms one Redis subscription for path.
// The caller reuses it across durable reads and owns Close.
func (s *Store) SubscribeNotifications(ctx context.Context, path string) (store.NotificationSubscription, error) {
	pubsub := s.client.Subscribe(ctx, notifyChannel(path))
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, err
	}
	return &notificationSubscription{
		pubsub: pubsub,
		wake:   pubsub.ChannelWithSubscriptions(),
	}, nil
}

func (s *notificationSubscription) Wait(ctx context.Context) (store.NotificationEvent, error) {
	select {
	case item, ok := <-s.wake:
		if !ok {
			return store.NotificationAppend, store.ErrNotificationSubscriptionClosed
		}
		event := notificationEvent(item)
		for {
			select {
			case item, ok := <-s.wake:
				if !ok {
					return event, nil
				}
				event = coalesceNotification(event, notificationEvent(item))
			default:
				return event, nil
			}
		}
	case <-ctx.Done():
		return store.NotificationAppend, ctx.Err()
	}
}

func (s *notificationSubscription) Close() error {
	return s.pubsub.Close()
}

func notificationEvent(item any) store.NotificationEvent {
	switch value := item.(type) {
	case *goredis.Subscription:
		return store.NotificationReconnect
	case *goredis.Message:
		switch value.Payload {
		case "c":
			return store.NotificationClose
		case "d":
			return store.NotificationDelete
		default:
			return store.NotificationAppend
		}
	default:
		return store.NotificationAppend
	}
}

func coalesceNotification(
	current store.NotificationEvent,
	next store.NotificationEvent,
) store.NotificationEvent {
	if current == store.NotificationDelete || next == store.NotificationDelete {
		return store.NotificationDelete
	}
	if current == store.NotificationReconnect || next == store.NotificationReconnect {
		return store.NotificationReconnect
	}
	if current == store.NotificationClose || next == store.NotificationClose {
		return store.NotificationClose
	}
	return store.NotificationAppend
}

// WaitForMessages blocks until messages past offset exist, the stream
// closes, the timeout expires, or ctx is cancelled.
//
// Wake protocol (docs/PLAN.md §4.5): fast-path read first, SUBSCRIBE to the
// stream's notify channel, then re-read BEFORE waiting (an append between
// the first read and the subscribe must not be missed), then loop on
// notification / defensive poll / timeout.
func (s *Store) WaitForMessages(ctx context.Context, path string, offset store.Offset, timeout time.Duration) ([]store.Message, bool, bool, error) {
	// Fast path: stream closed and caller at tail.
	meta, err := s.fetchMeta(ctx, path)
	if err != nil {
		return nil, false, false, err
	}
	if meta != nil && meta.Closed && offset.Equal(meta.CurrentOffset) {
		return nil, false, true, nil
	}

	// Fast path: one bounded page is enough to decide whether messages are
	// already available.
	page, err := s.ReadPage(ctx, path, offset, store.ReadPageOptions{})
	if err != nil {
		return nil, false, false, err
	}
	if len(page.Messages) > 0 {
		return page.Messages, false, false, nil
	}

	// Fork guard: an offset in the inherited range (< ForkOffset) can only
	// be served by the source, and source appends never notify fork
	// waiters — waiting would hang. Return empty immediately.
	if meta != nil && meta.ForkedFrom != "" && offset.LessThan(meta.ForkOffset) {
		return nil, false, false, nil
	}

	pubsub := s.client.Subscribe(ctx, notifyChannel(path))
	defer func() { _ = pubsub.Close() }()
	if _, err := pubsub.Receive(ctx); err != nil { // confirm subscription
		return nil, false, false, err
	}
	wake := pubsub.Channel()

	// Re-read before waiting: closes the missed-wakeup race window between
	// the fast-path read and the subscribe.
	if msgs, closed, done, err := s.recheck(ctx, path, offset); done {
		return msgs, false, closed, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(defensivePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wake:
			if msgs, closed, done, err := s.recheck(ctx, path, offset); done {
				return msgs, false, closed, err
			}
		case <-ticker.C:
			if msgs, closed, done, err := s.recheck(ctx, path, offset); done {
				return msgs, false, closed, err
			}
		case <-timer.C:
			// Timed out: report whether the stream is now closed (snapshot,
			// mirroring MemoryStore's timeout path).
			m, err := s.fetchMeta(ctx, path)
			if err != nil {
				return nil, false, false, err
			}
			return nil, true, m != nil && m.Closed, nil
		case <-ctx.Done():
			return nil, false, false, ctx.Err()
		}
	}
}

// recheck re-reads the stream. done=true means the wait is over: messages
// arrived, the stream closed at the caller's tail, or reading failed (e.g.
// the stream was deleted mid-wait). A spurious wakeup with nothing new
// keeps waiting (done=false).
func (s *Store) recheck(ctx context.Context, path string, offset store.Offset) (msgs []store.Message, closed, done bool, err error) {
	page, err := s.ReadPage(ctx, path, offset, store.ReadPageOptions{})
	if err != nil {
		return nil, false, true, err
	}
	if len(page.Messages) > 0 {
		return page.Messages, false, true, nil
	}
	meta, err := s.fetchMeta(ctx, path)
	if err != nil {
		return nil, false, true, err
	}
	if meta != nil && meta.Closed && offset.Equal(meta.CurrentOffset) {
		return nil, true, true, nil
	}
	return nil, false, false, nil
}
