package chronicle

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
	redisstore "gecgithub01.walmart.com/auk000v/chronicle/store/redis"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// SubscriptionTuning configures the subscription background loops. Zero values
// fall back to the Manager's defaults (30s floor, 30s reconcile, no sweep cap).
type SubscriptionTuning struct {
	SweepInterval         time.Duration
	ReconcileInterval     time.Duration
	SweepBatch            int
	ReplicaID             string
	MemberLeaseTTL        time.Duration
	HeartbeatInterval     time.Duration
	SlotLeaseTTL          time.Duration
	SlotReconcileInterval time.Duration
	ConsistencyTier       webhook.ConsistencyTier
	// Metrics, if set, receives sweep/delivery/worker observations from the
	// Manager. Nil leaves the Manager on its no-op recorder.
	Metrics webhook.Metrics
}

// storePath maps a stream-root-relative subscription path ("events/abc") to the
// store's leading-slash key ("/events/abc"). The inverse of subStreamPath.
func storePath(p string) string { return "/" + strings.TrimPrefix(p, "/") }

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
// plus its background loops (lease worker, retry worker, recovery floor).
// *webhook.Manager satisfies it.
type SubscriptionService interface {
	SubscriptionHooks
	Start()
	Stop()
	RunSweep()
	RunRedisReconnect()
}

// streamAdapter adapts the durable stream store to webhook.Streams: the seam the
// subscription Manager uses to read tails and append pull-wake events. rs is
// optional: when set it enables a pipelined batch tail read for the recovery
// sweep; when nil, TailOffsets falls back to per-path reads.
type streamAdapter struct {
	st store.Store
	rs *redisstore.Store
}

func (a streamAdapter) TailOffset(path string) (string, bool) {
	off, err := a.st.GetCurrentOffset(storePath(path))
	if err != nil {
		return "", false
	}
	return off.String(), true
}

// TailOffsets reads many stream tails at once: one pipelined batch when the Redis
// store is available, else a per-path fallback. Paths absent from the result do
// not exist. The sweep reads every linked tail per tick, so the batch keeps that
// from being a round trip per link.
func (a streamAdapter) TailOffsets(paths []string) map[string]string {
	if a.rs != nil {
		keyed := make([]string, len(paths))
		for i, p := range paths {
			keyed[i] = storePath(p)
		}
		if offs, err := a.rs.GetCurrentOffsets(context.Background(), keyed); err == nil {
			out := make(map[string]string, len(offs))
			for sp, off := range offs {
				out[strings.TrimPrefix(sp, "/")] = off.String()
			}
			return out
		}
		// fall through to per-path on error
	}
	out := make(map[string]string, len(paths))
	for _, p := range paths {
		if tail, ok := a.TailOffset(p); ok {
			out[p] = tail
		}
	}
	return out
}

func (a streamAdapter) BeginningOffset() string { return store.ZeroOffset.String() }

func (a streamAdapter) AppendWakeEvent(wakeStream string, data []byte) error {
	_, err := a.st.Append(storePath(wakeStream), data, store.AppendOptions{ContentType: "application/json"})
	return err
}

// redisLister adapts the Redis stream store to webhook.StreamLister for pattern
// backfill and recovery reconciliation.
type redisLister struct {
	rs *redisstore.Store
}

func (l redisLister) ListStreams() ([]webhook.StreamMeta, error) {
	metas, err := l.rs.ListStreamMeta(context.Background())
	if err != nil {
		return nil, err
	}
	out := make([]webhook.StreamMeta, len(metas))
	for i, m := range metas {
		// Store keys carry a leading slash; the subscription layer is slash-free.
		out[i] = webhook.StreamMeta{
			Path:        strings.TrimPrefix(m.Path, "/"),
			Tail:        m.Tail,
			CreatedAtNs: m.CreatedAtNs,
		}
	}
	return out, nil
}

// NewSubscriptions builds the Redis-backed __ds subscription stack: the HTTP
// router and the Manager whose background loops (lease, retry, recovery floor)
// the caller starts with Manager.Start(). streamRootURL is the public URL the
// protocol is served under (scheme+host+root, trailing slash), used to build
// callback and JWKS URLs. rs may be nil to disable pattern backfill of existing
// streams (new streams are still linked as they are created).
func NewSubscriptions(client redis.UniversalClient, streamStore store.Store, rs *redisstore.Store, streamRootURL string, allowPrivateWebhooks bool, tuning SubscriptionTuning, logger *slog.Logger) (SubscriptionRouter, SubscriptionService, error) {
	opts := webhook.ManagerOptions{
		StreamRootURL:              streamRootURL,
		Logger:                     logger,
		AllowPrivateWebhookTargets: allowPrivateWebhooks,
		SweepInterval:              tuning.SweepInterval,
		ReconcileInterval:          tuning.ReconcileInterval,
		SweepBatch:                 tuning.SweepBatch,
		ReplicaID:                  tuning.ReplicaID,
		MemberLeaseTTL:             tuning.MemberLeaseTTL,
		HeartbeatInterval:          tuning.HeartbeatInterval,
		SlotLeaseTTL:               tuning.SlotLeaseTTL,
		SlotReconcileInterval:      tuning.SlotReconcileInterval,
		ConsistencyTier:            tuning.ConsistencyTier,
		Metrics:                    tuning.Metrics,
	}
	if rs != nil {
		opts.Lister = redisLister{rs: rs}
	}
	mgr, err := webhook.NewManager(webhook.NewRedisStore(client), streamAdapter{st: streamStore, rs: rs}, opts)
	if err != nil {
		return nil, nil, err
	}
	return webhook.NewRoutes(mgr), mgr, nil
}
