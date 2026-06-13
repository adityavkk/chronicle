package redis

import (
	"context"
	"strings"
)

// unescapePath is the inverse of escapePath. The Replacer tries each pair in
// order at every position, so "%257B" (an escaped literal "%7B") decodes via the
// "%25"→"%" rule first, leaving "7B" — never mis-decoding to "{".
func unescapePath(esc string) string {
	r := strings.NewReplacer("%7B", "{", "%7D", "}", "%25", "%")
	return r.Replace(esc)
}

// ListStreamPaths scans for all live stream paths by matching meta keys. It
// returns stream-root-relative paths, excluding the reserved {__ds} subscription
// control-plane keyspace. Used by the subscription layer's pattern backfill
// (PROTOCOL §6.2). Not part of store.Store.
//
// On a single Redis this scans the whole keyspace; on a cluster SCAN visits one
// node, so a cluster deployment would fan this out per node — acceptable because
// backfill is best-effort and the recovery sweep re-links anything missed.
func (s *Store) ListStreamPaths(ctx context.Context) ([]string, error) {
	const openTag = keyPrefix + "{"
	const closeMeta = "}" + metaSuffix
	var paths []string
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
		paths = append(paths, unescapePath(inner))
	}
	return paths, iter.Err()
}
