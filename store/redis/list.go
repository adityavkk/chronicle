package redis

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// unescapePath is the inverse of escapePath. The Replacer tries each pair in
// order at every position, so "%257B" (an escaped literal "%7B") decodes via the
// "%25"→"%" rule first, leaving "7B" — never mis-decoding to "{".
func unescapePath(esc string) string {
	r := strings.NewReplacer("%7B", "{", "%7D", "}", "%25", "%")
	return r.Replace(esc)
}

// StreamMeta is the subset of a stream's metadata the subscription layer needs
// to backfill and reconcile glob links: the path, the current tail offset, and
// the creation time (to choose between linking at the beginning or the tail).
type StreamMeta struct {
	Path        string
	Tail        string
	CreatedAtNs int64
}

// ListStreamMeta scans for all live stream paths (excluding the reserved {__ds}
// control plane and soft-deleted streams) and returns each with its tail and
// creation time. It is used by the subscription layer's pattern backfill and
// recovery reconciliation (PROTOCOL §6.2). Not part of store.Store.
//
// On a single Redis this scans the whole keyspace and pipelines one HMGET per
// stream; on a cluster SCAN visits one node, so a cluster deployment would fan
// this out per node — acceptable because it is the outage backstop, not the
// low-latency path.
func (s *Store) ListStreamMeta(ctx context.Context) ([]StreamMeta, error) {
	const openTag = keyPrefix + "{"
	const closeMeta = "}" + metaSuffix
	type cand struct{ path, key string }
	var cands []cand
	iter := s.client.Scan(ctx, 0, keyPrefix+"{*}"+metaSuffix, 512).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		if !strings.HasPrefix(key, openTag) || !strings.HasSuffix(key, closeMeta) {
			continue
		}
		inner := key[len(openTag) : len(key)-len(closeMeta)]
		if inner == "__ds" {
			continue
		}
		cands = append(cands, cand{unescapePath(inner), key})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	if len(cands) == 0 {
		return nil, nil
	}
	pipe := s.client.Pipeline()
	cmds := make([]*redis.SliceCmd, len(cands))
	for i, c := range cands {
		cmds[i] = pipe.HMGet(ctx, c.key, fTail, fCreatedAt, fSoftDel)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make([]StreamMeta, 0, len(cands))
	for i, c := range cands {
		vals := cmds[i].Val()
		tail, _ := vals[0].(string)
		if tail == "" {
			continue // no existence marker — vanished between scan and read
		}
		if sd, _ := vals[2].(string); sd == "1" {
			continue // skip soft-deleted streams
		}
		var createdNs int64
		if cs, _ := vals[1].(string); cs != "" {
			createdNs, _ = strconv.ParseInt(cs, 10, 64)
		}
		out = append(out, StreamMeta{Path: c.path, Tail: tail, CreatedAtNs: createdNs})
	}
	return out, nil
}
