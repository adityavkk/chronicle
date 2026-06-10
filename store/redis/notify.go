package redis

import (
	"context"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// defensivePollInterval is the fallback re-read cadence while blocked on
// pub/sub: Redis pub/sub is fire-and-forget, so a dropped wakeup (connection
// churn) is recovered within one tick instead of hanging until timeout.
const defensivePollInterval = time.Second

// WaitForMessages blocks until messages past offset exist, the stream
// closes, the timeout expires, or ctx is cancelled.
//
// Wake protocol (docs/PLAN.md §4.5): fast-path read first, SUBSCRIBE to the
// stream's notify channel, then re-read BEFORE waiting (an append between
// the first read and the subscribe must not be missed), then loop on
// notification / defensive poll / timeout.
func (s *RedisStore) WaitForMessages(ctx context.Context, path string, offset store.Offset, timeout time.Duration) ([]store.Message, bool, bool, error) {
	// Fast path: stream closed and caller at tail.
	meta, err := s.fetchMeta(ctx, path)
	if err != nil {
		return nil, false, false, err
	}
	if meta != nil && meta.Closed && offset.Equal(meta.CurrentOffset) {
		return nil, false, true, nil
	}

	// Fast path: messages already available.
	msgs, _, err := s.Read(path, offset)
	if err != nil {
		return nil, false, false, err
	}
	if len(msgs) > 0 {
		return msgs, false, false, nil
	}

	// Fork guard: an offset in the inherited range (< ForkOffset) can only
	// be served by the source, and source appends never notify fork
	// waiters — waiting would hang. Return empty immediately.
	if meta != nil && meta.ForkedFrom != "" && offset.LessThan(meta.ForkOffset) {
		return nil, false, false, nil
	}

	pubsub := s.client.Subscribe(ctx, notifyChannel(path))
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil { // confirm subscription
		return nil, false, false, err
	}
	wake := pubsub.Channel()

	// Re-read before waiting: closes the missed-wakeup race window between
	// the fast-path read and the subscribe.
	if msgs, closed, done, err := s.recheck(path, offset); done {
		return msgs, false, closed, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(defensivePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wake:
			if msgs, closed, done, err := s.recheck(path, offset); done {
				return msgs, false, closed, err
			}
		case <-ticker.C:
			if msgs, closed, done, err := s.recheck(path, offset); done {
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
func (s *RedisStore) recheck(path string, offset store.Offset) (msgs []store.Message, closed, done bool, err error) {
	msgs, _, err = s.Read(path, offset)
	if err != nil {
		return nil, false, true, err
	}
	if len(msgs) > 0 {
		return msgs, false, true, nil
	}
	meta, err := s.fetchMeta(context.Background(), path)
	if err != nil {
		return nil, false, true, err
	}
	if meta != nil && meta.Closed && offset.Equal(meta.CurrentOffset) {
		return nil, true, true, nil
	}
	return nil, false, false, nil
}
