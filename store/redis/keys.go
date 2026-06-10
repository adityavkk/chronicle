// Package redis implements the chronicle store.Store contract on Redis 8.
//
// Data model (docs/PLAN.md §4): one ZSET of lex-ordered frames per stream,
// a meta HASH, a producer HASH, and a pub/sub notify channel. All keys for a
// stream share the {<path>} hash tag so multi-key Lua scripts are
// cluster-safe.
package redis

import (
	"fmt"
	"strings"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// Key schema. The stream path is embedded raw inside the {...} hash tag
// (Redis keys are binary-safe); '{' and '}' in user paths are escaped so a
// hostile path can't break out of the tag.
const (
	keyPrefix     = "ds:"
	notifyPrefix  = "ds:notify:"
	metaSuffix    = ":meta"
	msgSuffix     = ":msg"
	prodSuffix    = ":prod"
	forksSuffix   = ":forks"
	frameSep      = "|"
	frameSepByte  = byte('|')
	offsetStrLen  = 33 // len("%016d_%016d")
	framePrefixLn = offsetStrLen + 1
)

// escapePath escapes hash-tag delimiters in user paths. Redis hash tags are
// delimited by the first '{' and the next '}'; escaping both characters keeps
// arbitrary user paths inside a single tag. The escaping is injective
// (% is escaped too) so distinct paths map to distinct keys.
func escapePath(path string) string {
	r := strings.NewReplacer("%", "%25", "{", "%7B", "}", "%7D")
	return r.Replace(path)
}

func tag(path string) string {
	return "{" + escapePath(path) + "}"
}

// metaKey returns the HASH key holding StreamMetadata fields.
func metaKey(path string) string { return keyPrefix + tag(path) + metaSuffix }

// msgKey returns the ZSET key holding "<offset>|<data>" frames at score 0.
func msgKey(path string) string { return keyPrefix + tag(path) + msgSuffix }

// prodKey returns the HASH key holding producerId -> "epoch:lastSeq:lastUpdated".
func prodKey(path string) string { return keyPrefix + tag(path) + prodSuffix }

// forksKey returns the SET key registering fork paths of this stream.
func forksKey(path string) string { return keyPrefix + tag(path) + forksSuffix }

// notifyChannel returns the pub/sub channel for stream wakeups
// ("a" append, "c" close, "d" delete).
func notifyChannel(path string) string { return notifyPrefix + tag(path) }

// encodeFrame builds a ZSET member: the message's end offset in its
// fixed-width lexicographic string form, a '|' separator, then the raw
// payload. Because offsets are fixed-width and zero-padded, bytewise lex
// order of members equals stream order, and the offset prefix makes members
// unique by construction.
func encodeFrame(offset store.Offset, data []byte) string {
	var b strings.Builder
	b.Grow(framePrefixLn + len(data))
	b.WriteString(offset.String())
	b.WriteByte(frameSepByte)
	b.Write(data)
	return b.String()
}

// decodeFrame splits a ZSET member back into (end offset, payload).
func decodeFrame(member string) (store.Offset, []byte, error) {
	if len(member) < framePrefixLn || member[offsetStrLen] != frameSepByte {
		return store.Offset{}, nil, fmt.Errorf("malformed frame member %q", truncateForErr(member))
	}
	off, err := store.ParseOffset(member[:offsetStrLen])
	if err != nil {
		return store.Offset{}, nil, fmt.Errorf("malformed frame offset: %w", err)
	}
	return off, []byte(member[framePrefixLn:]), nil
}

// lexLowerBound returns the exclusive ZRANGEBYLEX lower bound selecting
// exactly the frames whose end offset is strictly greater than offset.
//
// A frame whose end offset equals the requested offset must be EXCLUDED
// (the caller already has those bytes); any greater offset prefix must be
// INCLUDED. The offset prefix is fixed-width and followed by the constant
// separator '|' (0x7C), so "(<offset>\xff" works: for members sharing the
// requested prefix the next byte is '|' < 0xff (excluded), and any member
// with a lexicographically greater prefix sorts above "<offset>\xff" in the
// first differing prefix byte (included). Payload bytes (including 0x00 and
// 0xff) never participate in the comparison before the prefix decides.
func lexLowerBound(offset store.Offset) string {
	return "(" + offset.String() + "\xff"
}

// lexUpperBoundInclusive returns the inclusive ZRANGEBYLEX upper bound
// selecting frames whose end offset is <= offset (used for fork reads capped
// at the fork point). "[<offset>\xff" includes every member whose prefix
// equals offset (next byte '|' < 0xff) and excludes all greater prefixes.
func lexUpperBoundInclusive(offset store.Offset) string {
	return "[" + offset.String() + "\xff"
}

func truncateForErr(s string) string {
	const max = 64
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}
