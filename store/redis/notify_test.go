package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

type notificationTestMetrics struct {
	physical   atomic.Int64
	acks       atomic.Int64
	reconnects atomic.Int64
	staleAcks  atomic.Int64
	topology   atomic.Value
}

func (m *notificationTestMetrics) NotificationPhysicalConnection(topology string, delta int) {
	m.topology.Store(topology)
	m.physical.Add(int64(delta))
}

func (m *notificationTestMetrics) NotificationEvent(event string) {
	switch event {
	case "acknowledged":
		m.acks.Add(1)
	case "reconnect":
		m.reconnects.Add(1)
	case "stale_acknowledgement":
		m.staleAcks.Add(1)
	}
}

func newPendingNotificationRegistration(actor *notificationActor, channel string) *multiplexedNotificationSubscription {
	return &multiplexedNotificationSubscription{
		actor:   actor,
		channel: channel,
		ready:   make(chan error, 1),
		signal:  make(chan struct{}, 1),
		closed:  make(chan struct{}),
	}
}

// newNotificationTestStore gives connection-lifecycle assertions exclusive
// ownership of both the Store and a uniquely named Redis client. The general
// integration fixture deliberately reuses one Store across the package, which
// is inappropriate for tests that swap metrics observers and assert exact
// physical connection counts across repeated test invocations.
func newNotificationTestStore(t *testing.T) (*Store, *goredis.Client, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Redis integration test in short mode")
	}
	rawURL := os.Getenv("REDIS_URL")
	if rawURL == "" {
		rawURL = "redis://localhost:6379/15"
	}
	opts, err := goredis.ParseURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	clientName := fmt.Sprintf(
		"chronicle-notification-test-%d-%d",
		testRunStamp,
		pathCounter.Add(1),
	)
	opts.ClientName = clientName
	client := goredis.NewClient(opts)
	if err := client.Ping(t.Context()).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("redis not reachable at %s: %v", rawURL, err)
	}
	s := New(client, Options{})
	t.Cleanup(func() { _ = s.Close() })
	return s, client, clientName
}

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

func TestNotificationMultiplexerRejectsStaleAcknowledgementGeneration(t *testing.T) {
	metrics := &notificationTestMetrics{}
	mux := &notificationMultiplexer{topology: "standalone", observer: metrics}
	actor := &notificationActor{
		mux:        mux,
		generation: 7,
		desired:    make(map[string]*notificationChannel),
	}
	const channel = "ds:notify:{/stale}"
	registration := newPendingNotificationRegistration(actor, channel)
	actor.desired[channel] = &notificationChannel{
		registrations: map[*multiplexedNotificationSubscription]struct{}{registration: {}},
	}

	actor.acknowledge(notificationAcknowledgement{generation: 6, channel: channel})
	select {
	case err := <-registration.ready:
		t.Fatalf("stale acknowledgement marked registration ready: %v", err)
	default:
	}
	if got := metrics.staleAcks.Load(); got != 1 {
		t.Fatalf("stale acknowledgement metric = %d, want 1", got)
	}

	actor.acknowledge(notificationAcknowledgement{generation: 7, channel: channel})
	select {
	case err := <-registration.ready:
		if err != nil {
			t.Fatalf("current acknowledgement: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("current acknowledgement did not mark registration ready")
	}
}

func TestNotificationMultiplexerReconnectInvalidatesGenerationAndWakesAll(t *testing.T) {
	metrics := &notificationTestMetrics{}
	mux := &notificationMultiplexer{topology: "standalone", observer: metrics}
	actor := &notificationActor{
		mux:         mux,
		generation:  3,
		serverCount: 2,
		desired:     make(map[string]*notificationChannel),
	}
	channels := []string{"ds:notify:{/one}", "ds:notify:{/two}"}
	registrations := make([]*multiplexedNotificationSubscription, 0, len(channels))
	for _, channel := range channels {
		registration := newPendingNotificationRegistration(actor, channel)
		registration.markReady(nil)
		registrations = append(registrations, registration)
		actor.desired[channel] = &notificationChannel{
			registrations: map[*multiplexedNotificationSubscription]struct{}{registration: {}},
			ackGeneration: 3,
		}
	}

	actor.handleSubscription(&goredis.Subscription{
		Kind:    "subscribe",
		Channel: channels[0],
		Count:   1,
	})
	if actor.generation != 4 {
		t.Fatalf("connection generation = %d, want 4", actor.generation)
	}
	if got := metrics.reconnects.Load(); got != 1 {
		t.Fatalf("reconnect metric = %d, want 1", got)
	}
	for i, registration := range registrations {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		event, err := registration.Wait(ctx)
		cancel()
		if err != nil || event != store.NotificationReconnect {
			t.Fatalf("registration %d reconnect = %v, %v", i, event, err)
		}
	}
	if actor.desired[channels[0]].ackGeneration != 4 ||
		actor.desired[channels[1]].ackGeneration != 0 {
		t.Fatalf("ack generations after first reconnect ack = %d, %d", actor.desired[channels[0]].ackGeneration, actor.desired[channels[1]].ackGeneration)
	}
}

func TestNotificationMultiplexerBlockedRegistrationDoesNotBlockAnother(t *testing.T) {
	mux := &notificationMultiplexer{topology: "standalone"}
	actor := &notificationActor{mux: mux, desired: make(map[string]*notificationChannel)}
	first := newPendingNotificationRegistration(actor, "first")
	second := newPendingNotificationRegistration(actor, "second")
	actor.desired[first.channel] = &notificationChannel{
		registrations: map[*multiplexedNotificationSubscription]struct{}{first: {}},
	}
	actor.desired[second.channel] = &notificationChannel{
		registrations: map[*multiplexedNotificationSubscription]struct{}{second: {}},
	}
	first.notify(store.NotificationAppend) // leave the first hub's signal full

	actor.handle(&goredis.Message{Channel: first.channel, Payload: "c"})
	actor.handle(&goredis.Message{Channel: second.channel, Payload: "a"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := second.Wait(ctx)
	if err != nil || event != store.NotificationAppend {
		t.Fatalf("unblocked registration = %v, %v", event, err)
	}
	firstEvent, err := first.Wait(ctx)
	if err != nil || firstEvent != store.NotificationClose {
		t.Fatalf("blocked registration coalesced event = %v, %v", firstEvent, err)
	}
}

func TestIntegrationNotificationMultiplexerUsesOnePhysicalConnectionForManyChannels(t *testing.T) {
	s, _, _ := newNotificationTestStore(t)
	metrics := &notificationTestMetrics{}
	s.SetNotificationMetrics(metrics)
	t.Cleanup(func() { s.SetNotificationMetrics(nil) })

	const registrations = 100
	subs := make([]store.NotificationSubscription, 0, registrations)
	for i := range registrations {
		sub, err := s.SubscribeNotifications(
			context.Background(),
			testPath("multiplexed"),
		)
		if err != nil {
			t.Fatalf("register channel %d: %v", i, err)
		}
		subs = append(subs, sub)
	}
	if got := metrics.physical.Load(); got != 1 {
		t.Fatalf("physical notification connections = %d, want 1", got)
	}
	for _, sub := range subs {
		if err := sub.Close(); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for metrics.physical.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := metrics.physical.Load(); got != 0 {
		t.Fatalf("physical notification connections after cleanup = %d, want 0", got)
	}
}

func TestIntegrationWaitForPageSharesNotificationMultiplexerConnection(t *testing.T) {
	s, client, clientName := newNotificationTestStore(t)
	metrics := &notificationTestMetrics{}
	s.SetNotificationMetrics(metrics)
	t.Cleanup(func() { s.SetNotificationMetrics(nil) })

	persistentPath := testPath("persistent-registration")
	waitPath := testPath("page-wait-registration")
	mustCreate(t, s, persistentPath, store.CreateOptions{})
	mustCreate(t, s, waitPath, store.CreateOptions{})
	persistent, err := s.SubscribeNotifications(t.Context(), persistentPath)
	if err != nil {
		t.Fatal(err)
	}
	defer persistent.Close() //nolint:errcheck // test cleanup

	initial, err := s.ReadPage(t.Context(), waitPath, store.ZeroOffset, store.ReadPageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	type waitResult struct {
		result store.ReadWaitResult
		err    error
	}
	waited := make(chan waitResult, 1)
	go func() {
		result, waitErr := s.WaitForPage(
			context.Background(),
			waitPath,
			initial.NextOffset,
			initial.Snapshot,
			5*time.Second,
			store.ReadPageOptions{},
		)
		waited <- waitResult{result: result, err: waitErr}
	}()

	channels := []string{notifyChannel(persistentPath), notifyChannel(waitPath)}
	deadline := time.Now().Add(2 * time.Second)
	for {
		counts, countErr := client.PubSubNumSub(t.Context(), channels...).Result()
		if countErr != nil {
			t.Fatal(countErr)
		}
		if counts[channels[0]] == 1 && counts[channels[1]] == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("logical notification channels not ready: %v", counts)
		}
		time.Sleep(time.Millisecond)
	}
	if got := countRedisPubSubClients(t, client, clientName); got != 1 {
		t.Fatalf("owned physical Pub/Sub clients = %d, want one shared connection", got)
	}

	if _, err := s.Append(waitPath, []byte("waited"), store.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-waited:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.result.TimedOut || len(got.result.Page.Messages) != 1 ||
			string(got.result.Page.Messages[0].Data) != "waited" {
			t.Fatalf("WaitForPage result = %+v", got.result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForPage did not return its durable wake page")
	}
}

func countRedisPubSubClients(t *testing.T, client *goredis.Client, clientName string) int {
	t.Helper()
	list, err := client.ClientList(t.Context()).Result()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(list), "\n") {
		owned := false
		subscribed := false
		for _, field := range strings.Fields(line) {
			if field == "name="+clientName {
				owned = true
			}
			if field != "sub=0" && strings.HasPrefix(field, "sub=") {
				subscribed = true
			}
		}
		if owned && subscribed {
			count++
		}
	}
	return count
}

func TestIntegrationClusterNotificationMultiplexerUsesOneGlobalConnection(t *testing.T) {
	rawAddresses := os.Getenv("REDIS_CLUSTER_ADDRS")
	if rawAddresses == "" {
		t.Skip("REDIS_CLUSTER_ADDRS is required for Redis Cluster integration")
	}
	addresses := strings.Split(rawAddresses, ",")
	client := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addresses})
	ctx := t.Context()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("cluster ping: %v", err)
	}
	s := New(client, Options{})
	t.Cleanup(func() { _ = s.Close() })
	metrics := &notificationTestMetrics{}
	s.SetNotificationMetrics(metrics)

	const registrations = 100
	subs := make([]store.NotificationSubscription, 0, registrations)
	paths := make([]string, 0, registrations)
	for i := range registrations {
		path := testPath("cluster-multiplexed")
		sub, err := s.SubscribeNotifications(ctx, path)
		if err != nil {
			t.Fatalf("register cluster channel %d: %v", i, err)
		}
		paths = append(paths, path)
		subs = append(subs, sub)
	}
	if got := metrics.physical.Load(); got != 1 {
		t.Fatalf("cluster physical notification connections = %d, want 1", got)
	}
	if topology, _ := metrics.topology.Load().(string); topology != "cluster_global" {
		t.Fatalf("notification topology = %q, want cluster_global", topology)
	}

	for _, index := range []int{0, registrations - 1} {
		if err := client.Publish(ctx, notifyChannel(paths[index]), "a").Err(); err != nil {
			t.Fatalf("publish cluster notification %d: %v", index, err)
		}
		waitCtx, cancel := context.WithTimeout(ctx, time.Second)
		event, err := subs[index].Wait(waitCtx)
		cancel()
		if err != nil || event != store.NotificationAppend {
			t.Fatalf("cluster notification %d = %v, %v", index, event, err)
		}
	}
	for _, sub := range subs {
		if err := sub.Close(); err != nil {
			t.Fatal(err)
		}
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
	s, _, _ := newNotificationTestStore(t)
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
