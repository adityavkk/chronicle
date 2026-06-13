package webhook

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore implements Store on Redis 8, persisting the subscription control
// plane under the {__ds} hash tag. It shares the go-redis client with the stream
// store (chronicle uses one Redis), and does not own it: Close is a no-op.
type RedisStore struct {
	client redis.UniversalClient
}

var _ Store = (*RedisStore)(nil)

// NewRedisStore wraps a go-redis client as a subscription Store.
func NewRedisStore(client redis.UniversalClient) *RedisStore {
	return &RedisStore{client: client}
}

func (s *RedisStore) ctx() context.Context { return context.Background() }

func nsArg(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

// evalStrings runs a script and decodes its reply as a slice of strings, the
// fixed reply shape of every subscription script.
func (s *RedisStore) evalStrings(script *redis.Script, keys []string, args ...any) ([]string, error) {
	raw, err := script.Run(s.ctx(), s.client, keys, args...).Result()
	if err != nil {
		return nil, err
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("webhook: unexpected script reply %T", raw)
	}
	out := make([]string, len(arr))
	for i, e := range arr {
		switch v := e.(type) {
		case string:
			out[i] = v
		case int64:
			out[i] = strconv.FormatInt(v, 10)
		case nil:
			out[i] = ""
		default:
			return nil, fmt.Errorf("webhook: unexpected reply element %d: %T", i, e)
		}
	}
	return out, nil
}

// CreateOrConfirm seeds the create_sub script with the config fields and the
// explicit links at their current tails.
func (s *RedisStore) CreateOrConfirm(id string, cfg Config, links []StreamLink, now time.Time) (CreateStatus, error) {
	cfg = NormalizeConfig(cfg)
	args := []any{
		id, ConfigHash(cfg), nsArg(now),
		string(cfg.Type), cfg.Pattern, cfg.WebhookURL, cfg.WakeStream,
		strconv.FormatInt(cfg.LeaseTTLMs, 10), cfg.Description,
		strconv.Itoa(len(links)),
	}
	for _, l := range links {
		args = append(args, l.Path, string(l.LinkType), l.AckedOffset)
	}
	reply, err := s.evalStrings(createSubScript, []string{subKey(id), subsKey, linksKey(id)}, args...)
	if err != nil {
		return 0, err
	}
	switch reply[0] {
	case "CREATED":
		for _, l := range links {
			if err := s.indexStream(l.Path, id); err != nil {
				return 0, err
			}
		}
		return CreateCreated, nil
	case "MATCHED":
		return CreateMatched, nil
	case "CONFLICT":
		return CreateConflict, nil
	default:
		return 0, fmt.Errorf("create_sub: unexpected status %q", reply[0])
	}
}

// Get hydrates a subscription and its links.
func (s *RedisStore) Get(id string) (Subscription, bool, error) {
	pipe := s.client.Pipeline()
	subCmd := pipe.HGetAll(s.ctx(), subKey(id))
	linkCmd := pipe.HGetAll(s.ctx(), linksKey(id))
	if _, err := pipe.Exec(s.ctx()); err != nil {
		return Subscription{}, false, err
	}
	fields := subCmd.Val()
	if len(fields) == 0 {
		return Subscription{}, false, nil
	}
	return subscriptionFromHash(id, fields, linkCmd.Val()), true, nil
}

// Delete removes the subscription and de-indexes its streams. Links are read
// first so the fan-out entries can be cleaned up.
func (s *RedisStore) Delete(id string) error {
	links, err := s.client.HKeys(s.ctx(), linksKey(id)).Result()
	if err != nil {
		return err
	}
	if _, err := s.evalStrings(deleteSubScript,
		[]string{subKey(id), subsKey, linksKey(id), leaseZKey, retryZKey}, id); err != nil {
		return err
	}
	for _, path := range links {
		if err := s.deindexStream(path, id); err != nil {
			return err
		}
	}
	return nil
}

// List returns all subscription ids.
func (s *RedisStore) List() ([]string, error) {
	return s.client.SMembers(s.ctx(), subsKey).Result()
}

// Link links a stream and maintains the fan-out index.
func (s *RedisStore) Link(id, path string, linkType LinkType, offset string) error {
	if _, err := s.evalStrings(linkStreamScript, []string{linksKey(id)}, path, string(linkType), offset); err != nil {
		return err
	}
	return s.indexStream(path, id)
}

// Unlink removes an explicit link; de-indexes only when the link is gone.
func (s *RedisStore) Unlink(id, path string, stillGlob bool) error {
	flag := "0"
	if stillGlob {
		flag = "1"
	}
	reply, err := s.evalStrings(unlinkStreamScript, []string{linksKey(id)}, path, flag)
	if err != nil {
		return err
	}
	if reply[0] == "REMOVED" {
		return s.deindexStream(path, id)
	}
	return nil
}

func (s *RedisStore) StreamSubscribers(path string) ([]string, error) {
	return s.client.SMembers(s.ctx(), streamSubsKey(path)).Result()
}

func (s *RedisStore) indexStream(path, id string) error {
	return s.client.SAdd(s.ctx(), streamSubsKey(path), id).Err()
}

func (s *RedisStore) deindexStream(path, id string) error {
	return s.client.SRem(s.ctx(), streamSubsKey(path), id).Err()
}

// ArmWake issues a wake if idle.
func (s *RedisStore) ArmWake(id string, now time.Time, leaseTTLMs int64, armLease bool, wakeID string) (ArmResult, error) {
	arm := "0"
	if armLease {
		arm = "1"
	}
	reply, err := s.evalStrings(armWakeScript, []string{subKey(id), leaseZKey},
		id, nsArg(now), strconv.FormatInt(leaseTTLMs, 10), arm, wakeID)
	if err != nil {
		return ArmResult{}, err
	}
	switch reply[0] {
	case "ARMED":
		gen, _ := strconv.ParseInt(reply[1], 10, 64)
		return ArmResult{Armed: true, Generation: gen, WakeID: reply[2]}, nil
	case "BUSY":
		gen, _ := strconv.ParseInt(reply[1], 10, 64)
		return ArmResult{Busy: true, Generation: gen, WakeID: reply[2]}, nil
	case "NOSUB":
		return ArmResult{NoSub: true}, nil
	default:
		return ArmResult{}, fmt.Errorf("arm_wake: unexpected status %q", reply[0])
	}
}

// Claim runs the pull-wake CAS claim.
func (s *RedisStore) Claim(id, worker, wakeID string, now time.Time, leaseTTLMs int64) (ClaimResult, error) {
	reply, err := s.evalStrings(claimScript, []string{subKey(id), leaseZKey},
		id, worker, nsArg(now), strconv.FormatInt(leaseTTLMs, 10), wakeID)
	if err != nil {
		return ClaimResult{}, err
	}
	switch reply[0] {
	case "CLAIMED":
		gen, _ := strconv.ParseInt(reply[1], 10, 64)
		return ClaimResult{Claimed: true, Generation: gen, WakeID: reply[2], Holder: reply[3]}, nil
	case "BUSY":
		gen, _ := strconv.ParseInt(reply[1], 10, 64)
		return ClaimResult{Busy: true, Generation: gen, Holder: reply[3]}, nil
	case "NOSUB":
		return ClaimResult{NoSub: true}, nil
	default:
		return ClaimResult{}, fmt.Errorf("claim: unexpected status %q", reply[0])
	}
}

// Ack fences, applies acks, and releases or heartbeats.
func (s *RedisStore) Ack(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64, done bool, acks []Ack, now time.Time, leaseTTLMs int64) (string, error) {
	doneArg := "0"
	if done {
		doneArg = "1"
	}
	args := []any{
		id, strconv.FormatInt(reqGeneration, 10), reqWakeID, strconv.FormatInt(tokenGeneration, 10),
		doneArg, nsArg(now), strconv.FormatInt(leaseTTLMs, 10), strconv.Itoa(len(acks)),
	}
	for _, a := range acks {
		args = append(args, a.Stream, a.Offset)
	}
	reply, err := s.evalStrings(ackScript, []string{subKey(id), linksKey(id), leaseZKey, retryZKey}, args...)
	if err != nil {
		return "", err
	}
	return reply[0], nil
}

// Release fences then releases the lease.
func (s *RedisStore) Release(id string, reqGeneration int64, reqWakeID string, tokenGeneration int64) (string, error) {
	reply, err := s.evalStrings(releaseScript, []string{subKey(id), leaseZKey, retryZKey},
		id, strconv.FormatInt(reqGeneration, 10), reqWakeID, strconv.FormatInt(tokenGeneration, 10))
	if err != nil {
		return "", err
	}
	return reply[0], nil
}

// ExpireLease clears an expired lease.
func (s *RedisStore) ExpireLease(id string, now time.Time) (string, error) {
	reply, err := s.evalStrings(expireLeaseScript, []string{subKey(id), leaseZKey}, id, nsArg(now))
	if err != nil {
		return "", err
	}
	return reply[0], nil
}

func (s *RedisStore) DueLeases(now time.Time, limit int, visibility time.Duration) ([]string, error) {
	return s.due(leaseZKey, now, limit, visibility)
}

func (s *RedisStore) DueRetries(now time.Time, limit int, visibility time.Duration) ([]string, error) {
	return s.due(retryZKey, now, limit, visibility)
}

func (s *RedisStore) due(zkey string, now time.Time, limit int, visibility time.Duration) ([]string, error) {
	return s.evalStrings(claimDueScript, []string{zkey},
		nsArg(now), strconv.Itoa(limit), strconv.FormatInt(int64(visibility), 10))
}

func (s *RedisStore) ScheduleRetry(id string, now, nextAttempt time.Time) (int, error) {
	reply, err := s.evalStrings(scheduleRetryScript, []string{subKey(id), retryZKey},
		id, nsArg(now), nsArg(nextAttempt))
	if err != nil {
		return 0, err
	}
	if reply[0] == "NOSUB" {
		return 0, nil
	}
	n, _ := strconv.Atoi(reply[1])
	return n, nil
}

func (s *RedisStore) RecordSuccess(id string) error {
	_, err := s.evalStrings(recordSuccessScript, []string{subKey(id), retryZKey}, id)
	return err
}

// LoadSigningKey adopts the persisted active key or installs a freshly-generated
// candidate, atomically (get_or_create_key). The kid is therefore stable across
// restarts (PROTOCOL §6.5).
func (s *RedisStore) LoadSigningKey(now time.Time) (SigningKey, error) {
	cand, err := GenerateSigningKey(rand.Reader, now)
	if err != nil {
		return SigningKey{}, err
	}
	reply, err := s.evalStrings(getOrCreateKeyScript, []string{jwksKey, activeKidKey},
		cand.Kid, marshalKeyMaterial(cand))
	if err != nil {
		return SigningKey{}, err
	}
	return unmarshalKeyMaterial(reply[0], reply[1])
}

// SigningKeys returns all persisted keys (active first) for the JWKS.
func (s *RedisStore) SigningKeys() ([]SigningKey, error) {
	all, err := s.client.HGetAll(s.ctx(), jwksKey).Result()
	if err != nil {
		return nil, err
	}
	activeKid, _ := s.client.Get(s.ctx(), activeKidKey).Result()
	keys := make([]SigningKey, 0, len(all))
	for kid, material := range all {
		k, err := unmarshalKeyMaterial(kid, material)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	// Active key first so the JWKS lists it as the preferred verification key.
	for i, k := range keys {
		if k.Kid == activeKid && i != 0 {
			keys[0], keys[i] = keys[i], keys[0]
			break
		}
	}
	return keys, nil
}

// LoadTokenKey adopts or installs the persisted HMAC token key, so callback and
// claim tokens issued before a restart still validate (PROTOCOL §12.9).
func (s *RedisStore) LoadTokenKey() ([]byte, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	cand := base64.RawURLEncoding.EncodeToString(raw)
	ok, err := s.client.SetNX(s.ctx(), tokenKeyKey, cand, 0).Result()
	if err != nil {
		return nil, err
	}
	if ok {
		return raw, nil
	}
	stored, err := s.client.Get(s.ctx(), tokenKeyKey).Result()
	if err != nil {
		return nil, err
	}
	return base64.RawURLEncoding.DecodeString(stored)
}

// marshalKeyMaterial encodes a signing key as "<priv_b64url>:<created_unix>:<status>".
// The public half is recovered from the private key.
func marshalKeyMaterial(k SigningKey) string {
	return strings.Join([]string{
		base64.RawURLEncoding.EncodeToString(k.Private),
		strconv.FormatInt(k.CreatedAt.Unix(), 10),
		k.Status,
	}, ":")
}

func unmarshalKeyMaterial(kid, material string) (SigningKey, error) {
	parts := strings.SplitN(material, ":", 3)
	if len(parts) != 3 {
		return SigningKey{}, fmt.Errorf("webhook: malformed key material for %q", kid)
	}
	priv, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return SigningKey{}, err
	}
	if len(priv) != ed25519.PrivateKeySize {
		return SigningKey{}, fmt.Errorf("webhook: bad ed25519 private key length %d", len(priv))
	}
	created, _ := strconv.ParseInt(parts[1], 10, 64)
	pk := ed25519.PrivateKey(priv)
	return SigningKey{
		Kid:       kid,
		Private:   pk,
		Public:    pk.Public().(ed25519.PublicKey),
		CreatedAt: time.Unix(created, 0),
		Status:    parts[2],
	}, nil
}

// subscriptionFromHash decodes the sub HASH and links HASH into a Subscription.
func subscriptionFromHash(id string, f map[string]string, linkFields map[string]string) Subscription {
	atoi := func(k string) int64 { n, _ := strconv.ParseInt(f[k], 10, 64); return n }
	createdNs := atoi("created_ns")
	sub := Subscription{
		ID: id,
		Config: Config{
			Type:        DispatchType(f["type"]),
			Pattern:     f["pattern"],
			WebhookURL:  f["webhook_url"],
			WakeStream:  f["wake_stream"],
			LeaseTTLMs:  atoi("lease_ttl_ms"),
			Description: f["description"],
		},
		CfgHash:       f["cfg_hash"],
		CreatedAt:     time.Unix(0, createdNs),
		Status:        Status(f["status"]),
		Phase:         Phase(f["phase"]),
		Generation:    atoi("generation"),
		WakeID:        f["wake_id"],
		Holder:        f["holder"] == "1",
		HolderWorker:  f["holder_worker"],
		LeaseUntilNs:  atoi("lease_until_ns"),
		RetryCount:    int(atoi("retry_count")),
		FirstFailNs:   atoi("first_fail_ns"),
		NextAttemptNs: atoi("next_attempt_ns"),
	}
	sub.Links = linksFromHash(linkFields)
	// Rebuild the normalized explicit stream list so the config round-trips for
	// idempotency checks after a reload.
	for _, l := range sub.Links {
		if l.LinkType == LinkExplicit {
			sub.Config.Streams = append(sub.Config.Streams, l.Path)
		}
	}
	sub.Config.Streams = normalizeStreams(sub.Config.Streams)
	return sub
}

func linksFromHash(linkFields map[string]string) []StreamLink {
	links := make([]StreamLink, 0, len(linkFields))
	for path, v := range linkFields {
		lt, off, ok := strings.Cut(v, ":")
		if !ok {
			continue
		}
		links = append(links, StreamLink{Path: path, LinkType: LinkType(lt), AckedOffset: off})
	}
	return links
}
