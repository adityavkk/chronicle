package segments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var publishManifestScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
local expected = ARGV[1]
if expected == "" then
  if current then return {"CONFLICT", current} end
else
  if not current or current ~= expected then
    return {"CONFLICT", current or ""}
  end
end
redis.call("SET", KEYS[1], ARGV[2])
redis.call("PERSIST", KEYS[1])
return {"OK", ARGV[2]}
`)

// RedisBackend stores immutable record blobs and fixed-width sparse indexes in
// Redis STRINGs. They use the stream's existing hash tag, so manifest CAS and
// all candidate keys remain cluster-slot local. The existing frame ZSET stays
// authoritative and untouched.
type RedisBackend struct {
	client       redis.UniversalClient
	faults       FaultInjector
	reads        atomic.Uint64
	writes       atomic.Uint64
	readBytes    atomic.Uint64
	writtenBytes atomic.Uint64
}

// NewRedisBackend constructs the Redis immutable-chunk candidate.
func NewRedisBackend(client redis.UniversalClient, faults FaultInjector) *RedisBackend {
	return &RedisBackend{client: client, faults: faults}
}

// Mode returns ModeRedisChunks.
func (b *RedisBackend) Mode() Mode { return ModeRedisChunks }

func segmentEscapePath(path string) string {
	return strings.NewReplacer("%", "%25", "{", "%7B", "}", "%7D").Replace(path)
}

func segmentTag(path string) string { return "{" + segmentEscapePath(path) + "}" }

func segmentPrefix(path string) string { return "ds:" + segmentTag(path) + ":segments" }

func redisCurrentKey(path string) string { return segmentPrefix(path) + ":current" }

func redisManifestKey(path, token string) string {
	return segmentPrefix(path) + ":manifest:" + token
}

func redisManifestIndexKey(path string) string { return segmentPrefix(path) + ":manifest-index" }

func redisInventoryKey(path string) string { return segmentPrefix(path) + ":inventory" }

func redisManifestIndexMember(generation uint64, token string) string {
	return fmt.Sprintf("%020d|%s", generation, token)
}

func redisBlobKey(path string, generation uint64, checksum, suffix string) string {
	return fmt.Sprintf("%s:blob:%020d:%s:%s", segmentPrefix(path), generation, checksum, suffix)
}

// Load returns the manifest referenced by the current Redis pointer.
func (b *RedisBackend) Load(ctx context.Context, path string) (*Manifest, string, error) {
	token, err := b.client.Get(ctx, redisCurrentKey(path)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, "", ErrNoManifest
	}
	if err != nil {
		return nil, "", err
	}
	raw, err := b.client.Get(ctx, redisManifestKey(path, token)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, "", fmt.Errorf("%w: current Redis manifest missing", ErrCorrupt)
	}
	if err != nil {
		return nil, "", err
	}
	if manifestDigest(raw) != token {
		return nil, "", fmt.Errorf("%w: Redis manifest digest", ErrChecksum)
	}
	var manifest Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, "", fmt.Errorf("%w: Redis manifest json: %w", ErrCorrupt, err)
	}
	return &manifest, token, nil
}

// Put stores one checksum-addressed segment and sparse index in Redis.
func (b *RedisBackend) Put(ctx context.Context, path string, generation uint64, encoded EncodedSegment) (SegmentRef, error) {
	if err := hit(b.faults, FaultChecksum); err != nil {
		return SegmentRef{}, err
	}
	if segmentChecksum(encoded.Data, encoded.Index) != encoded.Checksum {
		return SegmentRef{}, ErrChecksum
	}
	dataKey := redisBlobKey(path, generation, encoded.Checksum, "data")
	indexKey := redisBlobKey(path, generation, encoded.Checksum, "index")
	if err := b.setImmutable(ctx, dataKey, encoded.Data); err != nil {
		return SegmentRef{}, err
	}
	if err := b.setImmutable(ctx, indexKey, encoded.Index); err != nil {
		return SegmentRef{}, err
	}
	pipe := b.client.Pipeline()
	pipe.SAdd(ctx, redisInventoryKey(path), dataKey, indexKey)
	pipe.Persist(ctx, redisInventoryKey(path))
	if _, err := pipe.Exec(ctx); err != nil {
		return SegmentRef{}, err
	}
	b.writes.Add(1)
	b.writtenBytes.Add(uint64(len(encoded.Data) + len(encoded.Index)))
	return SegmentRef{
		ID:             encoded.Checksum,
		DataKey:        dataKey,
		IndexKey:       indexKey,
		StartExclusive: encoded.StartExclusive.String(),
		EndInclusive:   encoded.EndInclusive.String(),
		Records:        encoded.Records,
		DataBytes:      int64(len(encoded.Data)),
		IndexBytes:     int64(len(encoded.Index)),
		IndexStride:    encoded.IndexStride,
		BlockBytes:     encoded.BlockBytes,
		BlockChecksums: append([]string(nil), encoded.BlockChecksums...),
		IndexChecksum:  encoded.IndexChecksum,
		IndexEntries:   append([]IndexEntry(nil), encoded.IndexEntries...),
		Checksum:       encoded.Checksum,
	}, nil
}

func (b *RedisBackend) setImmutable(ctx context.Context, key string, value []byte) error {
	created, err := b.client.SetNX(ctx, key, value, 0).Result()
	if err != nil {
		return err
	}
	if created {
		return nil
	}
	existing, err := b.client.Get(ctx, key).Bytes()
	if err != nil {
		return err
	}
	if string(existing) != string(value) {
		return fmt.Errorf("%w: immutable Redis object collision at %s", ErrChecksum, key)
	}
	return nil
}

// Publish advances the current manifest with an atomic compare-and-swap.
func (b *RedisBackend) Publish(ctx context.Context, path, expectedToken string, manifest *Manifest) (string, error) {
	if err := hit(b.faults, FaultManifest); err != nil {
		return "", err
	}
	raw, token, err := manifestBytes(manifest)
	if err != nil {
		return "", err
	}
	key := redisManifestKey(path, token)
	if err := b.setImmutable(ctx, key, raw); err != nil {
		return "", err
	}
	pipe := b.client.Pipeline()
	pipe.ZAdd(ctx, redisManifestIndexKey(path), redis.Z{
		Score:  0,
		Member: redisManifestIndexMember(manifest.Generation, token),
	})
	pipe.Persist(ctx, redisManifestIndexKey(path))
	if _, err := pipe.Exec(ctx); err != nil {
		return "", err
	}
	rawReply, err := publishManifestScript.Run(ctx, b.client, []string{redisCurrentKey(path)}, expectedToken, token).Result()
	if err != nil {
		return "", err
	}
	reply, ok := rawReply.([]any)
	if !ok || len(reply) < 1 {
		return "", fmt.Errorf("%w: malformed manifest CAS reply", ErrCorrupt)
	}
	status, _ := reply[0].(string)
	if status == "CONFLICT" {
		return "", ErrConflict
	}
	if status != "OK" {
		return "", fmt.Errorf("%w: unknown manifest CAS status %q", ErrCorrupt, status)
	}
	return token, nil
}

// Read returns and verifies one immutable Redis data and index pair.
func (b *RedisBackend) Read(ctx context.Context, ref SegmentRef) ([]byte, []byte, error) {
	if err := hit(b.faults, FaultChecksum); err != nil {
		return nil, nil, err
	}
	values, err := b.client.MGet(ctx, ref.DataKey, ref.IndexKey).Result()
	if err != nil {
		return nil, nil, err
	}
	if len(values) != 2 || values[0] == nil || values[1] == nil {
		return nil, nil, fmt.Errorf("%w: immutable Redis object missing", ErrCorrupt)
	}
	data, okData := values[0].(string)
	index, okIndex := values[1].(string)
	if !okData || !okIndex {
		return nil, nil, fmt.Errorf("%w: unexpected Redis object type", ErrCorrupt)
	}
	if segmentChecksum([]byte(data), []byte(index)) != ref.Checksum {
		return nil, nil, ErrChecksum
	}
	b.reads.Add(1)
	b.readBytes.Add(uint64(len(data) + len(index)))
	return []byte(data), []byte(index), nil
}

// ReadDataRange fetches and authenticates one block with GETRANGE. Segment
// serving never needs to materialize the complete Redis string.
func (b *RedisBackend) ReadDataRange(
	ctx context.Context,
	ref SegmentRef,
	start, length int64,
) ([]byte, error) {
	if err := hit(b.faults, FaultChecksum); err != nil {
		return nil, err
	}
	block, err := validateDataRange(ref, start, length)
	if err != nil {
		return nil, err
	}
	raw, err := b.client.GetRange(ctx, ref.DataKey, start, start+length-1).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("%w: immutable Redis object missing", ErrCorrupt)
	}
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) != length {
		return nil, fmt.Errorf("%w: short immutable Redis range", ErrCorrupt)
	}
	if byteChecksum(raw) != ref.BlockChecksums[block] {
		return nil, ErrChecksum
	}
	b.reads.Add(1)
	b.readBytes.Add(uint64(len(raw)))
	return raw, nil
}

// Tombstone removes the current pointer for a deleted stream incarnation.
func (b *RedisBackend) Tombstone(ctx context.Context, path string) error {
	return b.client.Del(ctx, redisCurrentKey(path)).Err()
}

// GC classifies reclaimable generations and objects but deliberately deletes
// neither. Redis Put and Publish are separate operations, so safe reclamation
// requires a same-slot staging barrier that this prototype does not yet have.
func (b *RedisBackend) GC(ctx context.Context, path string, retention GCRetention) (GCResult, error) {
	if err := hit(b.faults, FaultGC); err != nil {
		return GCResult{}, err
	}
	if retention.KeepGenerations < 1 {
		retention.KeepGenerations = 2
	}
	if retention.Now.IsZero() {
		retention.Now = time.Now()
	}
	members, err := b.client.ZRevRangeByLex(ctx, redisManifestIndexKey(path), &redis.ZRangeBy{
		Max: "+",
		Min: "-",
	}).Result()
	if err != nil {
		return GCResult{}, err
	}
	current, _ := b.client.Get(ctx, redisCurrentKey(path)).Result()
	type generation struct {
		token string
		m     Manifest
	}
	gens := make([]generation, 0, len(members))
	for _, member := range members {
		if len(member) < 22 || member[20] != '|' {
			continue
		}
		token := member[21:]
		raw, rerr := b.client.Get(ctx, redisManifestKey(path, token)).Bytes()
		if rerr != nil {
			continue
		}
		var m Manifest
		if json.Unmarshal(raw, &m) == nil {
			gens = append(gens, generation{token: token, m: m})
		}
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i].m.Generation > gens[j].m.Generation })
	// Re-read CURRENT after traversing manifests. A concurrent publish between
	// the first pointer read and traversal must be retained, never collected.
	if latest, rerr := b.client.Get(ctx, redisCurrentKey(path)).Result(); rerr == nil {
		current = latest
	}
	referencedObjects := map[string]bool{}
	protected := make(map[string]bool, len(retention.ProtectedTokens))
	for _, token := range retention.ProtectedTokens {
		protected[token] = true
	}
	var result GCResult
	for i, gen := range gens {
		published := time.Unix(0, gen.m.PublishedAtUnixN)
		young := retention.MinAge > 0 && retention.Now.Sub(published) < retention.MinAge
		keep := i < retention.KeepGenerations || gen.token == current || protected[gen.token] || young
		if keep {
			result.ManifestsKept++
		} else {
			result.ManifestsDeferred++
		}
		for _, ref := range gen.m.Segments {
			referencedObjects[ref.DataKey] = true
			referencedObjects[ref.IndexKey] = true
		}
	}
	objects, err := b.client.SMembers(ctx, redisInventoryKey(path)).Result()
	if err != nil {
		return result, err
	}
	for _, object := range objects {
		if referencedObjects[object] {
			result.SegmentsKept++
			continue
		}
		result.SegmentsDeferred++
	}
	return result, nil
}

// Close releases backend resources.
func (b *RedisBackend) Close() error { return nil }

// BackendStats returns operational counters for metrics.
func (b *RedisBackend) BackendStats() BackendStats {
	return BackendStats{
		BytesRead:    b.readBytes.Load(),
		BytesWritten: b.writtenBytes.Load(),
		RedisReads:   b.reads.Load(),
		RedisWrites:  b.writes.Load(),
	}
}

// RedisKeyLayout is repair-tool evidence that every candidate key for a path
// shares one Redis Cluster hash tag.
func RedisKeyLayout(path string) []string {
	prefix := segmentPrefix(path)
	return []string{
		redisCurrentKey(path),
		redisManifestIndexKey(path),
		redisInventoryKey(path),
		prefix + ":manifest:<sha256>",
		prefix + ":blob:<generation>:<sha256>:data",
		prefix + ":blob:<generation>:<sha256>:index",
	}
}
