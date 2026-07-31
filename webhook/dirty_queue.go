package webhook

import "time"

// dirtyQueue is the process-local latency hint between a committed append and
// subscription fan-out. Durable cursors and the recovery sweep remain the
// correctness state; this queue is deliberately fixed-size and disposable.
//
// The queue itself is a pure state machine. Manager supplies synchronization
// and performs every external action after releasing its mutex.
type dirtyQueue struct {
	capacity int
	entries  map[string]dirtyEntry
	ready    []string
	head     int
	readyN   int

	oldest time.Time

	overflow overflowState
}

type dirtyEntryState uint8

const (
	dirtyQueued dirtyEntryState = iota
	dirtyProcessing
)

type dirtyEntry struct {
	state      dirtyEntryState
	since      time.Time
	dirtyAgain bool
}

// dirtyEnqueueResult makes overload and coalescing visible to both control flow
// and metrics. The vocabulary is closed and contains no stream identifiers.
type dirtyEnqueueResult uint8

const (
	dirtyEnqueued dirtyEnqueueResult = iota
	dirtyCoalescedQueued
	dirtyCoalescedProcessing
	dirtyOverflowed
	dirtyOverflowCoalesced
	dirtyStopped
)

func (r dirtyEnqueueResult) String() string {
	switch r {
	case dirtyEnqueued:
		return "enqueued"
	case dirtyCoalescedQueued:
		return "coalesced-queued"
	case dirtyCoalescedProcessing:
		return "coalesced-processing"
	case dirtyOverflowed:
		return "overflow"
	case dirtyOverflowCoalesced:
		return "overflow-coalesced"
	case dirtyStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

type dirtyCompletion uint8

const (
	dirtySucceeded dirtyCompletion = iota
	dirtyRetry
)

type dirtyWork struct {
	path  string
	since time.Time
}

type dirtyQueueStats struct {
	depth     int
	capacity  int
	oldestAge time.Duration
}

type overflowPhase uint8

const (
	overflowIdle overflowPhase = iota
	overflowPending
	overflowReconciling
)

// overflowState uses the same pending/processing/dirty-again shape as a stream
// entry. An overflow that occurs during a recovery sweep must request a second
// sweep because the first may already have read the affected tail.
type overflowState struct {
	phase      overflowPhase
	since      time.Time
	dirtyAgain bool
	againSince time.Time
}

func newDirtyQueue(capacity int) dirtyQueue {
	if capacity <= 0 {
		panic("webhook: dirty queue capacity must be positive")
	}
	return dirtyQueue{
		capacity: capacity,
		entries:  make(map[string]dirtyEntry, capacity),
		ready:    make([]string, capacity),
	}
}

// enqueue adds one stream hint. requestRecovery is true only for the first
// overflow in an epoch; repeated overflow remains represented by the state
// machine without producing an unbounded signal storm.
func (q *dirtyQueue) enqueue(path string, now time.Time) (result dirtyEnqueueResult, requestRecovery bool) {
	if entry, ok := q.entries[path]; ok {
		switch entry.state {
		case dirtyQueued:
			return dirtyCoalescedQueued, false
		case dirtyProcessing:
			entry.dirtyAgain = true
			q.entries[path] = entry
			return dirtyCoalescedProcessing, false
		default:
			panic("webhook: invalid dirty entry state")
		}
	}

	if len(q.entries) == q.capacity {
		return q.noteOverflow(now)
	}

	q.entries[path] = dirtyEntry{state: dirtyQueued, since: now}
	q.push(path)
	if q.oldest.IsZero() || now.Before(q.oldest) {
		q.oldest = now
	}
	return dirtyEnqueued, false
}

func (q *dirtyQueue) noteOverflow(now time.Time) (dirtyEnqueueResult, bool) {
	switch q.overflow.phase {
	case overflowIdle:
		q.overflow = overflowState{phase: overflowPending, since: now}
		return dirtyOverflowed, true
	case overflowPending:
		return dirtyOverflowCoalesced, false
	case overflowReconciling:
		if !q.overflow.dirtyAgain {
			q.overflow.dirtyAgain = true
			q.overflow.againSince = now
		}
		return dirtyOverflowCoalesced, false
	default:
		panic("webhook: invalid dirty overflow state")
	}
}

func (q *dirtyQueue) take(limit int) []dirtyWork {
	if limit <= 0 {
		panic("webhook: dirty batch limit must be positive")
	}
	if limit > q.readyN {
		limit = q.readyN
	}
	work := make([]dirtyWork, 0, limit)
	for range limit {
		path := q.pop()
		entry := q.entries[path]
		if entry.state != dirtyQueued {
			panic("webhook: dirty ring contains non-queued entry")
		}
		entry.state = dirtyProcessing
		q.entries[path] = entry
		work = append(work, dirtyWork{path: path, since: entry.since})
	}
	return work
}

// complete removes successful work unless another append arrived while it was
// processing. Retry and dirty-again both return the entry to the queue tail,
// which prevents a hot stream from starving older streams.
func (q *dirtyQueue) complete(path string, completion dirtyCompletion) {
	entry, ok := q.entries[path]
	if !ok || entry.state != dirtyProcessing {
		panic("webhook: completing dirty work that is not processing")
	}
	if completion == dirtyRetry || entry.dirtyAgain {
		entry.state = dirtyQueued
		entry.dirtyAgain = false
		q.entries[path] = entry
		q.push(path)
		return
	}
	delete(q.entries, path)
	if entry.since.Equal(q.oldest) {
		q.recomputeOldest()
	}
}

// beginReconcile associates a recovery sweep with the current overflow epoch.
// It is safe to call for every sweep; false means no overflow needs coverage.
func (q *dirtyQueue) beginReconcile() (time.Time, bool) {
	if q.overflow.phase != overflowPending {
		return time.Time{}, false
	}
	q.overflow.phase = overflowReconciling
	return q.overflow.since, true
}

// completeReconcile closes the overflow epoch only after a successful sweep.
// If overflow happened while the sweep ran, requestAgain is true because that
// append may be newer than the sweep's tail snapshot.
func (q *dirtyQueue) completeReconcile(success bool) bool {
	if q.overflow.phase != overflowReconciling {
		return false
	}
	if !success {
		q.overflow.phase = overflowPending
		// The next sweep starts after every overflow observed by the failed pass,
		// so it covers both the original epoch and dirtyAgain. A later overflow
		// during that retry will set dirtyAgain anew.
		q.overflow.dirtyAgain = false
		q.overflow.againSince = time.Time{}
		return true
	}
	if q.overflow.dirtyAgain {
		q.overflow = overflowState{phase: overflowPending, since: q.overflow.againSince}
		return true
	}
	q.overflow = overflowState{}
	return false
}

func (q *dirtyQueue) hasReady() bool { return q.readyN > 0 }

func (q *dirtyQueue) hasPendingOverflow() bool {
	return q.overflow.phase == overflowPending
}

func (q *dirtyQueue) stats(now time.Time) dirtyQueueStats {
	oldest := q.oldest
	if q.overflow.phase != overflowIdle && (oldest.IsZero() || q.overflow.since.Before(oldest)) {
		oldest = q.overflow.since
	}
	age := time.Duration(0)
	if !oldest.IsZero() && now.After(oldest) {
		age = now.Sub(oldest)
	}
	return dirtyQueueStats{depth: len(q.entries), capacity: q.capacity, oldestAge: age}
}

func (q *dirtyQueue) push(path string) {
	if q.readyN == q.capacity {
		panic("webhook: dirty ready ring overflow")
	}
	idx := (q.head + q.readyN) % q.capacity
	q.ready[idx] = path
	q.readyN++
}

func (q *dirtyQueue) pop() string {
	if q.readyN == 0 {
		panic("webhook: pop from empty dirty queue")
	}
	path := q.ready[q.head]
	q.ready[q.head] = ""
	q.head = (q.head + 1) % q.capacity
	q.readyN--
	return path
}

func (q *dirtyQueue) recomputeOldest() {
	q.oldest = time.Time{}
	for _, entry := range q.entries {
		if q.oldest.IsZero() || entry.since.Before(q.oldest) {
			q.oldest = entry.since
		}
	}
}
