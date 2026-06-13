package chronicle

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// SubscriptionRouter handles reserved __ds requests, returning true when it has
// claimed the request. *webhook.Routes satisfies it.
type SubscriptionRouter interface {
	HandleRequest(w http.ResponseWriter, r *http.Request) bool
}

// SubscriptionHooks receives stream lifecycle events so the subscription layer
// can wake subscribers. *webhook.Manager satisfies it.
type SubscriptionHooks interface {
	OnStreamCreated(path string)
	OnStreamAppend(path string)
	OnStreamDeleted(path string)
}

// SubscriptionService is the runnable subscription manager: the lifecycle hooks
// plus its background loops (lease worker, retry worker, recovery sweep).
// *webhook.Manager satisfies it.
type SubscriptionService interface {
	SubscriptionHooks
	Start()
	Stop()
	RunSweep()
}

// streamAdapter adapts the durable stream store to webhook.Streams: the seam the
// subscription Manager uses to read tails and append pull-wake events.
type streamAdapter struct {
	st store.Store
}

func (a streamAdapter) TailOffset(path string) (string, bool) {
	off, err := a.st.GetCurrentOffset(path)
	if err != nil {
		return "", false
	}
	return off.String(), true
}

func (a streamAdapter) BeginningOffset() string { return store.ZeroOffset.String() }

func (a streamAdapter) AppendWakeEvent(wakeStream string, data []byte) error {
	_, err := a.st.Append(wakeStream, data, store.AppendOptions{ContentType: "application/json"})
	return err
}

// redisLister adapts the Redis stream store to webhook.StreamLister for pattern
// backfill.
type redisLister struct {
	rs *redisstore.Store
}

func (l redisLister) ListStreamPaths() ([]string, error) {
	return l.rs.ListStreamPaths(context.Background())
}

// NewSubscriptions builds the Redis-backed __ds subscription stack: the HTTP
// router and the Manager whose background loops (lease, retry, recovery sweep)
// the caller starts with Manager.Start(). streamRootURL is the public URL the
// protocol is served under (scheme+host+root, trailing slash), used to build
// callback and JWKS URLs. rs may be nil to disable pattern backfill of existing
// streams (new streams are still linked as they are created).
func NewSubscriptions(client redis.UniversalClient, streamStore store.Store, rs *redisstore.Store, streamRootURL string, logger *slog.Logger) (SubscriptionRouter, SubscriptionService, error) {
	opts := webhook.ManagerOptions{
		StreamRootURL: streamRootURL,
		Logger:        logger,
	}
	if rs != nil {
		opts.Lister = redisLister{rs: rs}
	}
	mgr, err := webhook.NewManager(webhook.NewRedisStore(client), streamAdapter{st: streamStore}, opts)
	if err != nil {
		return nil, nil, err
	}
	return webhook.NewRoutes(mgr), mgr, nil
}
