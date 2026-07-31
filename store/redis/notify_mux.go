package redis

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

const notificationActorQueue = 1024

// notificationMultiplexer owns a bounded set of physical Pub/Sub actors for a
// Store. A channel is assigned to exactly one actor, while each actor can serve
// any number of logical stream registrations.
type notificationMultiplexer struct {
	ctx      context.Context
	cancel   context.CancelFunc
	topology string
	actors   []*notificationActor
	closed   sync.Once

	physical atomic.Int64
	metrics  sync.RWMutex
	observer store.NotificationMetrics
}

func newNotificationMultiplexer(
	client goredis.UniversalClient,
	groups int,
) *notificationMultiplexer {
	ctx, cancel := context.WithCancel(context.Background())
	topology := "standalone"
	if _, ok := client.(*goredis.ClusterClient); ok {
		topology = "cluster_global"
	}
	mux := &notificationMultiplexer{
		ctx:      ctx,
		cancel:   cancel,
		topology: topology,
		actors:   make([]*notificationActor, groups),
	}
	for i := range mux.actors {
		actor := &notificationActor{
			mux:      mux,
			client:   client,
			ctx:      ctx,
			commands: make(chan notificationCommand, notificationActorQueue),
			desired:  make(map[string]*notificationChannel),
			done:     make(chan struct{}),
		}
		mux.actors[i] = actor
		go actor.run()
	}
	return mux
}

func (m *notificationMultiplexer) Register(
	ctx context.Context,
	path string,
) (*multiplexedNotificationSubscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	channel := notifyChannel(path)
	actor := m.actors[notificationGroup(channel, len(m.actors))]
	registration := &multiplexedNotificationSubscription{
		actor:   actor,
		channel: channel,
		ready:   make(chan error, 1),
		signal:  make(chan struct{}, 1),
		closed:  make(chan struct{}),
	}
	if !actor.send(ctx, notificationCommand{add: registration}) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, store.ErrNotificationSubscriptionClosed
	}
	select {
	case err := <-registration.ready:
		if err != nil {
			registration.Close() //nolint:errcheck // cleanup after failed registration
			return nil, err
		}
		return registration, nil
	case <-ctx.Done():
		registration.Close() //nolint:errcheck // cancellation owns the pending registration
		return nil, ctx.Err()
	case <-m.ctx.Done():
		return nil, store.ErrNotificationSubscriptionClosed
	}
}

func notificationGroup(channel string, groups int) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(channel))
	return int(hash.Sum32() % uint32(groups)) // #nosec G115 -- modulo is bounded by groups
}

func (m *notificationMultiplexer) SetMetrics(observer store.NotificationMetrics) {
	m.metrics.Lock()
	previous := m.observer
	connections := int(m.physical.Load())
	if previous != nil && connections != 0 {
		previous.NotificationPhysicalConnection(m.topology, -connections)
	}
	m.observer = observer
	if observer != nil && connections != 0 {
		observer.NotificationPhysicalConnection(m.topology, connections)
	}
	m.metrics.Unlock()
}

func (m *notificationMultiplexer) physicalConnection(delta int) {
	m.physical.Add(int64(delta))
	m.metrics.RLock()
	observer := m.observer
	if observer != nil {
		observer.NotificationPhysicalConnection(m.topology, delta)
	}
	m.metrics.RUnlock()
}

func (m *notificationMultiplexer) event(event string) {
	m.metrics.RLock()
	observer := m.observer
	if observer != nil {
		observer.NotificationEvent(event)
	}
	m.metrics.RUnlock()
}

func (m *notificationMultiplexer) Close() {
	m.closed.Do(func() {
		m.cancel()
		for _, actor := range m.actors {
			<-actor.done
		}
	})
}

type notificationCommand struct {
	add    *multiplexedNotificationSubscription
	remove *multiplexedNotificationSubscription
}

type notificationActor struct {
	mux      *notificationMultiplexer
	client   goredis.UniversalClient
	ctx      context.Context
	commands chan notificationCommand
	desired  map[string]*notificationChannel
	done     chan struct{}

	pubsub       *goredis.PubSub
	events       <-chan any
	generation   uint64
	serverCount  int
	physicalOpen bool
}

type notificationChannel struct {
	registrations map[*multiplexedNotificationSubscription]struct{}
	ackGeneration uint64
}

func (a *notificationActor) send(ctx context.Context, command notificationCommand) bool {
	select {
	case a.commands <- command:
		return true
	case <-ctx.Done():
		return false
	case <-a.ctx.Done():
		return false
	}
}

func (a *notificationActor) run() {
	defer close(a.done)
	defer a.shutdown()
	for {
		select {
		case <-a.ctx.Done():
			return
		case command := <-a.commands:
			switch {
			case command.add != nil:
				a.add(command.add)
			case command.remove != nil:
				a.remove(command.remove)
			}
		case item, ok := <-a.events:
			if !ok {
				a.closePhysical()
				a.reopenDesired()
				continue
			}
			a.handle(item)
		}
	}
}

func (a *notificationActor) add(registration *multiplexedNotificationSubscription) {
	entry := a.desired[registration.channel]
	newChannel := entry == nil
	if newChannel {
		entry = &notificationChannel{
			registrations: make(map[*multiplexedNotificationSubscription]struct{}),
		}
		a.desired[registration.channel] = entry
	}
	entry.registrations[registration] = struct{}{}
	if entry.ackGeneration == a.generation && a.physicalOpen {
		registration.markReady(nil)
		return
	}

	if !a.physicalOpen {
		a.openPhysical([]string{registration.channel}, false)
		return
	}
	if !newChannel {
		return
	}
	if err := a.pubsub.Subscribe(a.ctx, registration.channel); err != nil {
		delete(entry.registrations, registration)
		delete(a.desired, registration.channel)
		a.mux.event("registration_failure")
		registration.markReady(err)
		registration.markClosed()
	}
}

func (a *notificationActor) remove(registration *multiplexedNotificationSubscription) {
	entry := a.desired[registration.channel]
	if entry == nil {
		registration.markClosed()
		return
	}
	delete(entry.registrations, registration)
	if len(entry.registrations) > 0 {
		registration.markClosed()
		return
	}
	delete(a.desired, registration.channel)
	if len(a.desired) == 0 {
		a.closePhysical()
		registration.markClosed()
		return
	}
	if a.pubsub != nil {
		if err := a.pubsub.Unsubscribe(a.ctx, registration.channel); err != nil &&
			!errors.Is(err, context.Canceled) {
			a.mux.event("unregistration_failure")
		}
	}
	registration.markClosed()
}

func (a *notificationActor) openPhysical(channels []string, reconnect bool) {
	if len(channels) == 0 || a.ctx.Err() != nil {
		return
	}
	a.generation++
	a.serverCount = 0
	for _, entry := range a.desired {
		entry.ackGeneration = 0
		if reconnect {
			for registration := range entry.registrations {
				registration.notify(store.NotificationReconnect)
			}
		}
	}
	if reconnect {
		a.mux.event("reconnect")
	}
	a.pubsub = a.client.Subscribe(a.ctx, channels...)
	a.events = a.pubsub.ChannelWithSubscriptions(
		goredis.WithChannelSize(notificationActorQueue),
		goredis.WithChannelHealthCheckInterval(time.Second),
		goredis.WithChannelSendTimeout(time.Second),
	)
	a.physicalOpen = true
	a.mux.physicalConnection(1)
	a.mux.event("physical_opened")
}

func (a *notificationActor) reopenDesired() {
	if len(a.desired) == 0 || a.ctx.Err() != nil {
		return
	}
	channels := make([]string, 0, len(a.desired))
	for channel := range a.desired {
		channels = append(channels, channel)
	}
	a.openPhysical(channels, true)
}

func (a *notificationActor) closePhysical() {
	if a.pubsub != nil {
		_ = a.pubsub.Close()
	}
	a.pubsub = nil
	a.events = nil
	a.serverCount = 0
	if a.physicalOpen {
		a.physicalOpen = false
		a.mux.physicalConnection(-1)
		a.mux.event("physical_closed")
	}
}

func (a *notificationActor) handle(item any) {
	switch value := item.(type) {
	case *goredis.Subscription:
		a.handleSubscription(value)
	case *goredis.Message:
		entry := a.desired[value.Channel]
		if entry == nil {
			return
		}
		event := notificationEvent(value)
		for registration := range entry.registrations {
			registration.notify(event)
		}
	}
}

func (a *notificationActor) handleSubscription(subscription *goredis.Subscription) {
	switch subscription.Kind {
	case "subscribe":
		// Redis reports the current subscription count in every acknowledgement.
		// A live connection's new channel increases that count. A count that
		// restarts at or below the previous value is the first acknowledgement
		// from a go-redis reconnect and therefore begins a new generation before
		// this acknowledgement can make any registration ready.
		if a.serverCount > 0 && subscription.Count <= a.serverCount {
			a.beginReconnectGeneration()
		}
		a.serverCount = subscription.Count
		a.acknowledge(notificationAcknowledgement{
			generation: a.generation,
			channel:    subscription.Channel,
		})
	case "unsubscribe":
		a.serverCount = subscription.Count
	}
}

type notificationAcknowledgement struct {
	generation uint64
	channel    string
}

func (a *notificationActor) acknowledge(ack notificationAcknowledgement) {
	if ack.generation != a.generation {
		a.mux.event("stale_acknowledgement")
		return
	}
	entry := a.desired[ack.channel]
	if entry == nil {
		return
	}
	if entry.ackGeneration == ack.generation {
		return
	}
	entry.ackGeneration = ack.generation
	a.mux.event("acknowledged")
	for registration := range entry.registrations {
		registration.markReady(nil)
	}
}

func (a *notificationActor) beginReconnectGeneration() {
	a.generation++
	a.serverCount = 0
	for _, entry := range a.desired {
		entry.ackGeneration = 0
		for registration := range entry.registrations {
			registration.notify(store.NotificationReconnect)
		}
	}
	a.mux.event("reconnect")
}

func (a *notificationActor) shutdown() {
	for _, entry := range a.desired {
		for registration := range entry.registrations {
			registration.markReady(store.ErrNotificationSubscriptionClosed)
			registration.markClosed()
		}
	}
	a.desired = nil
	a.closePhysical()
}

type multiplexedNotificationSubscription struct {
	actor   *notificationActor
	channel string
	ready   chan error
	signal  chan struct{}
	closed  chan struct{}

	readyOnce      sync.Once
	unregisterOnce sync.Once
	closedOnce     sync.Once
	pending        sync.Mutex
	hasEvent       bool
	event          store.NotificationEvent
}

func (s *multiplexedNotificationSubscription) Wait(
	ctx context.Context,
) (store.NotificationEvent, error) {
	select {
	case <-ctx.Done():
		return store.NotificationAppend, ctx.Err()
	case <-s.closed:
		return store.NotificationAppend, store.ErrNotificationSubscriptionClosed
	case <-s.signal:
		s.pending.Lock()
		event := s.event
		s.hasEvent = false
		s.pending.Unlock()
		return event, nil
	}
}

func (s *multiplexedNotificationSubscription) Close() error {
	s.unregisterOnce.Do(func() {
		if !s.actor.send(context.Background(), notificationCommand{remove: s}) {
			s.markClosed()
		}
	})
	// Do not leave the physical connection teardown racing the caller's next
	// registration or Store shutdown.
	<-s.closed
	return nil
}

func (s *multiplexedNotificationSubscription) notify(event store.NotificationEvent) {
	s.pending.Lock()
	if s.hasEvent {
		s.event = coalesceNotification(s.event, event)
	} else {
		s.event = event
		s.hasEvent = true
	}
	s.pending.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
}

func (s *multiplexedNotificationSubscription) markReady(err error) {
	s.readyOnce.Do(func() { s.ready <- err })
}

func (s *multiplexedNotificationSubscription) markClosed() {
	s.closedOnce.Do(func() { close(s.closed) })
}
