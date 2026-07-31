package webhook

import (
	"testing"
	"time"
)

func TestDirtyQueueTransitions(t *testing.T) {
	t0 := time.Unix(100, 0)
	q := newDirtyQueue(2)

	if got, recover := q.enqueue("a", t0); got != dirtyEnqueued || recover {
		t.Fatalf("enqueue a = (%v, %v)", got, recover)
	}
	if got, _ := q.enqueue("a", t0.Add(time.Second)); got != dirtyCoalescedQueued {
		t.Fatalf("queued duplicate = %v", got)
	}

	work := q.take(1)
	if len(work) != 1 || work[0].path != "a" || !work[0].since.Equal(t0) {
		t.Fatalf("take = %+v", work)
	}
	if got, _ := q.enqueue("a", t0.Add(2*time.Second)); got != dirtyCoalescedProcessing {
		t.Fatalf("processing duplicate = %v", got)
	}
	q.complete("a", dirtySucceeded)

	work = q.take(1)
	if len(work) != 1 || work[0].path != "a" {
		t.Fatalf("dirty-again work = %+v", work)
	}
	q.complete("a", dirtySucceeded)
	if stats := q.stats(t0.Add(3 * time.Second)); stats.depth != 0 || stats.oldestAge != 0 {
		t.Fatalf("empty stats = %+v", stats)
	}
}

func TestDirtyQueueBoundAndOverflowEpoch(t *testing.T) {
	t0 := time.Unix(100, 0)
	q := newDirtyQueue(1)
	_, _ = q.enqueue("a", t0)

	if got, recover := q.enqueue("b", t0.Add(time.Second)); got != dirtyOverflowed || !recover {
		t.Fatalf("first overflow = (%v, %v)", got, recover)
	}
	if got, recover := q.enqueue("c", t0.Add(2*time.Second)); got != dirtyOverflowCoalesced || recover {
		t.Fatalf("coalesced overflow = (%v, %v)", got, recover)
	}
	if stats := q.stats(t0.Add(3 * time.Second)); stats.depth != 1 || stats.capacity != 1 || stats.oldestAge != 3*time.Second {
		t.Fatalf("bounded stats = %+v", stats)
	}

	since, ok := q.beginReconcile()
	if !ok || !since.Equal(t0.Add(time.Second)) {
		t.Fatalf("begin reconcile = (%v, %v)", since, ok)
	}
	// This append committed after the recovery snapshot may have begun. It must
	// leave a second overflow epoch behind.
	if got, recover := q.enqueue("d", t0.Add(4*time.Second)); got != dirtyOverflowCoalesced || recover {
		t.Fatalf("overflow during reconcile = (%v, %v)", got, recover)
	}
	if again := q.completeReconcile(true); !again || !q.hasPendingOverflow() {
		t.Fatal("overflow during reconcile did not request another epoch")
	}
	if since, ok = q.beginReconcile(); !ok || !since.Equal(t0.Add(4*time.Second)) {
		t.Fatalf("second epoch = (%v, %v)", since, ok)
	}
	if again := q.completeReconcile(true); again || q.hasPendingOverflow() {
		t.Fatal("successful second reconcile did not clear overflow")
	}
}

func TestDirtyQueueFailedReconcileRetries(t *testing.T) {
	q := newDirtyQueue(1)
	t0 := time.Unix(100, 0)
	_, _ = q.enqueue("a", t0)
	_, _ = q.enqueue("b", t0.Add(time.Second))
	if _, ok := q.beginReconcile(); !ok {
		t.Fatal("overflow reconcile was not pending")
	}
	_, _ = q.enqueue("c", t0.Add(2*time.Second))
	if again := q.completeReconcile(false); !again || !q.hasPendingOverflow() {
		t.Fatal("failed reconcile must remain pending")
	}
	if _, ok := q.beginReconcile(); !ok {
		t.Fatal("failed overflow reconcile was not retried")
	}
	if again := q.completeReconcile(true); again || q.hasPendingOverflow() {
		t.Fatal("successful retry did not cover overflow seen by the failed pass")
	}
}

func TestDirtyQueueRetryAndFairness(t *testing.T) {
	q := newDirtyQueue(3)
	t0 := time.Unix(100, 0)
	for i, path := range []string{"hot", "b", "c"} {
		_, _ = q.enqueue(path, t0.Add(time.Duration(i)*time.Second))
	}

	first := q.take(1)
	if first[0].path != "hot" {
		t.Fatalf("first = %q", first[0].path)
	}
	q.complete("hot", dirtyRetry)

	next := q.take(3)
	want := []string{"b", "c", "hot"}
	for i := range want {
		if next[i].path != want[i] {
			t.Fatalf("retry order = %v, want %v", next, want)
		}
		q.complete(next[i].path, dirtySucceeded)
	}
}

func TestDirtyQueueCapacityMustBePositive(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("newDirtyQueue(0) did not panic")
		}
	}()
	_ = newDirtyQueue(0)
}
