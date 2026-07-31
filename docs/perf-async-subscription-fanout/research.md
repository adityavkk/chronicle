# Async subscription fan-out research

## Baseline

This work starts from `4c78ea9daef78278a73f1637dd41e3fbfca0d2cf`.
The worktree was clean before the first edit. The baseline `make test-unit` run
passed with the race detector.

The append path has four synchronous stages after the durable stream write:

1. `handler.handleAppend` calls `Store.Append`.
2. A new append calls `Manager.OnStreamAppend` before the response is written.
3. `OnStreamAppend` reads every subscriber ID for the stream.
4. It loads each subscription, reads linked stream tails, evaluates pending
   work, and arms or delivers a wake.

A producer duplicate returns before the hook. A failed append also returns
before the hook. A create with initial data fires the create hook once and the
append hook once.

The existing `chronicle_fanout_seconds` metric ends after subscriber lookup. It
does not measure subscription hydration, linked-tail reads, pending-work
evaluation, or pull-wake event appends. It therefore understates the current
response-path cost.

## Existing correctness boundary

The recovery sweep derives owed work from durable subscription cursors and
durable stream tails. It is already responsible for repairing lost process
notifications, process death, Redis reconnects, missing schedule entries, and
ownership gaps. Wake generation and wake ID fences make repeated evaluation
safe. The per-subscription due set preserves armed work. None of these rules
depend on `OnStreamAppend` completing successfully.

An append and a subscription notification cannot be one Redis transaction in
cluster mode because durable streams and subscription shards use different hash
slots. Adding a second durable dirty database would not remove that boundary.
It would add cleanup and replay state while the cursor sweep would still be
required.

## Chosen design

Use a process-local queue as a low-latency hint and retain the recovery sweep as
the durable backstop.

The queue has a fixed capacity. It stores at most one entry per stream. An entry
is either queued or processing. A repeated append marks queued work as
coalesced, or marks processing work dirty again so it returns to the back of the
queue. This preserves fairness when one stream is hot.

The append goroutine performs one bounded operation after commit: it takes the
queue mutex, changes this small typed state, records metrics, and attempts one
nonblocking signal. It performs no Redis call and starts no goroutine.

When the queue is full, the enqueue transition atomically marks one overflow
epoch. The caller requests the existing eager recovery loop with a nonblocking,
depth-one signal. Further overflow coalesces until a successful full recovery
sweep covers the epoch. A failed recovery leaves the epoch pending for retry.
The periodic recovery floor remains unchanged.

The async worker handles a bounded batch at a time. For each dirty stream it
does one subscriber lookup, one pipelined subscription hydration, and one
batched read for all distinct linked tails. It then evaluates idle
subscriptions and uses the existing arm and delivery functions. No manager
mutex is held during store, stream, or webhook work.

The manager owns the worker context and lifecycle. Start and stop are
idempotent. Stop cancels all manager loops and waits for them. A stopped process
may abandon queue entries because they are only latency hints; the next boot
reconcile and the unchanged recovery floor derive the same owed wakes from
durable state.

## Prior art

The queue state follows the useful part of Kubernetes client-go's workqueue
model. Its package contract calls the queue fair and "stingy": an item is not
processed concurrently, repeated adds before processing collapse, and an add
during processing causes one later pass. The source implements that contract
with separate dirty and processing sets. Chronicle uses the same transition
shape, including returning dirtied in-flight work to the queue tail. It does not
copy client-go's unbounded storage or global metrics provider. See the
[client-go workqueue contract](https://pkg.go.dev/k8s.io/client-go/util/workqueue)
and [queue implementation](https://github.com/kubernetes/client-go/blob/master/util/workqueue/queue.go).

Linux inotify provides the overload precedent. It caps queued events, emits an
explicit `IN_Q_OVERFLOW` indication when later events are dropped, and tells
robust consumers to rebuild affected cache state because event details were
lost. Chronicle applies that rule to durable cursors: a queue overflow creates
one observable recovery epoch, and a full cursor sweep rebuilds the owed-wake
view. An overflow that occurs while that sweep is running creates a second
epoch because it may be newer than the sweep snapshot. See
[inotify(7)](https://man7.org/linux/man-pages/man7/inotify.7.html).

Redis documents keyspace Pub/Sub notifications as fire-and-forget and states
that events published while a client is disconnected are lost. This rules out
treating an ephemeral notification as correctness state, even if the local
queue were replaced with Pub/Sub. See
[Redis keyspace notifications](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/).

Kubernetes describes controllers as control loops that move current state
toward durable desired state. Chronicle's recovery sweep has the same useful
property: it evaluates current subscription state against durable stream tails.
The append queue only asks that control loop to run sooner. See
[Kubernetes controllers](https://kubernetes.io/docs/concepts/architecture/controller/).

## Invariants

- Only a successful, non-duplicate append enqueues dirty work.
- Enqueue is bounded by a fixed queue capacity and constant-size signal channel.
- The queue and its membership map have the same fixed bound.
- At most one entry exists for a stream, including while it is processing.
- Work dirtied during processing returns to the queue tail.
- A failed dirty batch is retried and also requests eager recovery.
- Overflow is observable and requests recovery. It is never described as a
  durable marker.
- Wake arming retains every generation, wake ID, lease, claim, acknowledgement,
  retry, write-token, and owner-epoch fence.
- Recovery coverage and frequency are not reduced.

## Downside

The queue improves normal wake latency but does not make notification durable.
A crash can discard queued hints, and overload can move a wake onto the more
expensive full recovery path. That is acceptable only because recovery derives
owed work from durable cursors and tails after restart and on the unchanged
periodic floor.
