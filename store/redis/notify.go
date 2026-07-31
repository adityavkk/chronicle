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

// WaitForPage implements store.PageWaiter. The caller's first page already
// performed the logical access touch. Every race-closing recheck is therefore
// a fresh, no-touch snapshot fenced to the same stream incarnation.
func (s *Store) WaitForPage(
	ctx context.Context,
	path string,
	offset store.Offset,
	initial store.ReadSnapshot,
	timeout time.Duration,
	opts store.ReadPageOptions,
) (store.ReadWaitResult, error) {
	pubsub := s.client.Subscribe(ctx, notifyChannel(path))
	defer func() { _ = pubsub.Close() }()
	if _, err := pubsub.Receive(ctx); err != nil { // confirm subscription
		return store.ReadWaitResult{}, err
	}
	wake := pubsub.Channel()

	recheck := func() (store.ReadPage, bool, error) {
		recheckOpts := opts
		recheckOpts.Snapshot = nil
		recheckOpts.NoTouch = true
		page, err := s.ReadPage(ctx, path, offset, recheckOpts)
		if err != nil {
			return store.ReadPage{}, false, err
		}
		if !store.SameReadStream(initial, page.Snapshot) {
			return store.ReadPage{}, false, store.ErrReadSnapshotChanged
		}
		done := len(page.Messages) > 0 ||
			(page.Snapshot.Closed && offset.Equal(page.Snapshot.Tail))
		return page, done, nil
	}

	// Re-read after subscription confirmation so an append in the attach
	// window cannot be missed.
	if page, done, err := recheck(); err != nil {
		return store.ReadWaitResult{}, err
	} else if done {
		return store.ReadWaitResult{Page: page}, nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(defensivePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wake:
			page, done, err := recheck()
			if err != nil {
				return store.ReadWaitResult{}, err
			}
			if done {
				return store.ReadWaitResult{Page: page}, nil
			}
		case <-ticker.C:
			page, done, err := recheck()
			if err != nil {
				return store.ReadWaitResult{}, err
			}
			if done {
				return store.ReadWaitResult{Page: page}, nil
			}
		case <-timer.C:
			page, done, err := recheck()
			if err != nil {
				return store.ReadWaitResult{}, err
			}
			return store.ReadWaitResult{Page: page, TimedOut: !done}, nil
		case <-ctx.Done():
			return store.ReadWaitResult{}, ctx.Err()
		}
	}
}

// WaitForMessages retains the Store compatibility contract by performing the
// initial logical read, then delegating race closure to the typed page waiter.
func (s *Store) WaitForMessages(ctx context.Context, path string, offset store.Offset, timeout time.Duration) ([]store.Message, bool, bool, error) {
	page, err := s.ReadPage(ctx, path, offset, store.ReadPageOptions{})
	if err != nil {
		return nil, false, false, err
	}
	if len(page.Messages) > 0 {
		return page.Messages, false, false, nil
	}
	if page.Snapshot.Closed && offset.Equal(page.Snapshot.Tail) {
		return nil, false, true, nil
	}
	if page.Snapshot.ForkedFrom != "" && offset.LessThan(page.Snapshot.ForkOffset) {
		return nil, false, false, nil
	}

	result, err := s.WaitForPage(
		ctx,
		path,
		offset,
		page.Snapshot,
		timeout,
		store.ReadPageOptions{},
	)
	if err != nil {
		return nil, false, false, err
	}
	closed := result.Page.Snapshot.Closed &&
		offset.Equal(result.Page.Snapshot.Tail) &&
		len(result.Page.Messages) == 0
	return result.Page.Messages, result.TimedOut, closed, nil
}
