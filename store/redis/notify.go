package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// defensivePollInterval is the fallback re-read cadence while blocked on
// pub/sub: Redis pub/sub is fire-and-forget, so a dropped wakeup (connection
// churn) is recovered within one tick instead of hanging until timeout.
const defensivePollInterval = time.Second

// SubscribeNotifications registers path with the store-owned Pub/Sub
// multiplexer and returns only after the actor has observed Redis's
// acknowledgement for its current connection generation.
func (s *Store) SubscribeNotifications(ctx context.Context, path string) (store.NotificationSubscription, error) {
	if s.notifications == nil {
		return nil, store.ErrNotificationSubscriptionClosed
	}
	return s.notifications.Register(ctx, path)
}

// SetNotificationMetrics attaches the process metrics recorder to the
// store-owned multiplexer. It is safe after registrations have started.
func (s *Store) SetNotificationMetrics(metrics store.NotificationMetrics) {
	if s.notifications != nil {
		s.notifications.SetMetrics(metrics)
	}
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

// WaitForPage implements store.PageWaiter through the store-owned notification
// multiplexer. The caller's first page already performed the logical access
// touch. Every race-closing recheck is therefore a fresh, no-touch snapshot
// fenced to the same stream incarnation.
func (s *Store) WaitForPage(
	ctx context.Context,
	path string,
	offset store.Offset,
	initial store.ReadSnapshot,
	timeout time.Duration,
	opts store.ReadPageOptions,
) (store.ReadWaitResult, error) {
	subscription, err := s.SubscribeNotifications(ctx, path)
	if err != nil {
		return store.ReadWaitResult{}, err
	}
	defer func() { _ = subscription.Close() }()

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

	deadline := time.Now().Add(timeout)

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			page, done, recheckErr := recheck()
			return store.ReadWaitResult{Page: page, TimedOut: !done}, recheckErr
		}

		waitCtx, cancel := context.WithTimeout(ctx, min(remaining, defensivePollInterval))
		_, waitErr := subscription.Wait(waitCtx)
		cancel()
		if ctx.Err() != nil {
			return store.ReadWaitResult{}, ctx.Err()
		}
		if waitErr != nil && !errors.Is(waitErr, context.DeadlineExceeded) {
			return store.ReadWaitResult{}, waitErr
		}

		page, done, recheckErr := recheck()
		if recheckErr != nil {
			return store.ReadWaitResult{}, recheckErr
		}
		if done {
			return store.ReadWaitResult{Page: page}, nil
		}
		if time.Now().Compare(deadline) >= 0 {
			return store.ReadWaitResult{Page: page, TimedOut: true}, nil
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
