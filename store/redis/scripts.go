package redis

import (
	"embed"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/*.lua
var scriptFS embed.FS

// loadScript builds a redis.Script from the shared common.lua prelude plus
// the named script body. Script.Run handles NOSCRIPT reloads transparently,
// which keeps EVALSHA working against managed Redis's volatile script cache.
func loadScript(name string) *redis.Script {
	prelude, err := scriptFS.ReadFile("scripts/common.lua")
	if err != nil {
		panic(fmt.Sprintf("chronicle redis: embedded common.lua missing: %v", err))
	}
	body, err := scriptFS.ReadFile("scripts/" + name)
	if err != nil {
		panic(fmt.Sprintf("chronicle redis: embedded script %s missing: %v", name, err))
	}
	return redis.NewScript(string(prelude) + "\n" + string(body))
}

var (
	appendScript  = loadScript("append.lua")
	createScript  = loadScript("create.lua")
	closeScript   = loadScript("close.lua")
	readScript    = loadScript("read.lua")
	deleteScript  = loadScript("delete.lua")
	incrRefScript = loadScript("incr_ref.lua")
	decrRefScript = loadScript("decr_ref.lua")
)

// Status sentinels returned in reply[0] by the scripts.
const (
	stOK          = "OK"
	stValOnly     = "VALONLY"
	stRetry       = "RETRY"
	stNotFound    = "NOTFOUND"
	stSoftDel     = "SOFTDEL"
	stClosed      = "CLOSED"
	stCTMismatch  = "CTMISMATCH"
	stSeqConflict = "SEQCONFLICT"
	stStaleEpoch  = "STALE_EPOCH"
	stEpochSeq    = "EPOCH_SEQ"
	stSeqGap      = "SEQ_GAP"
	stExists      = "EXISTS"
	stMismatch    = "MISMATCH"
	stMatched     = "MATCHED"
	stCreated     = "CREATED"
	stSoftDeleted = "SOFTDELETED"
	stDeleted     = "DELETED"
	stGone        = "GONE"
	stUnderflow   = "UNDERFLOW"
	stCascade     = "CASCADE"
)

// scriptReply is the decoded fixed-shape array reply of the append/close
// scripts: {status, tail, producerResult, currentEpoch, expectedSeq,
// receivedSeq, lastSeq, closed, alreadyClosed}.
type scriptReply struct {
	Status        string
	Tail          string
	ProducerRes   int64
	CurrentEpoch  int64
	ExpectedSeq   int64
	ReceivedSeq   int64
	LastSeq       int64
	Closed        bool
	AlreadyClosed bool
}

func decodeScriptReply(v any) (*scriptReply, error) {
	arr, ok := v.([]any)
	if !ok || len(arr) < 9 {
		return nil, fmt.Errorf("unexpected script reply %T %v", v, v)
	}
	s := make([]string, 9)
	for i := range s {
		str, ok := arr[i].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected script reply element %d: %T", i, arr[i])
		}
		s[i] = str
	}
	r := &scriptReply{Status: s[0], Tail: s[1], Closed: s[7] == "1", AlreadyClosed: s[8] == "1"}
	for _, f := range []struct {
		dst *int64
		src string
	}{
		{&r.ProducerRes, s[2]},
		{&r.CurrentEpoch, s[3]},
		{&r.ExpectedSeq, s[4]},
		{&r.ReceivedSeq, s[5]},
		{&r.LastSeq, s[6]},
	} {
		n, err := strconv.ParseInt(f.src, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("unexpected numeric reply %q: %w", f.src, err)
		}
		*f.dst = n
	}
	return r, nil
}

// decodeStatusReply decodes a variable-shape {status, extras...} reply.
func decodeStatusReply(v any) (status string, rest []any, err error) {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return "", nil, fmt.Errorf("unexpected script reply %T %v", v, v)
	}
	status, ok = arr[0].(string)
	if !ok {
		return "", nil, fmt.Errorf("unexpected script reply status %T", arr[0])
	}
	return status, arr[1:], nil
}

// flatToMap converts a Lua-returned HGETALL flat array to a string map.
func flatToMap(v any) (map[string]string, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected flat reply %T", v)
	}
	if len(arr)%2 != 0 {
		return nil, fmt.Errorf("odd flat reply length %d", len(arr))
	}
	m := make(map[string]string, len(arr)/2)
	for i := 0; i < len(arr); i += 2 {
		k, ok1 := arr[i].(string)
		val, ok2 := arr[i+1].(string)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("unexpected flat reply elements %T/%T", arr[i], arr[i+1])
		}
		m[k] = val
	}
	return m, nil
}

// toStrings converts a Lua-returned array of bulk strings.
func toStrings(v any) ([]string, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected array reply %T", v)
	}
	out := make([]string, len(arr))
	for i, e := range arr {
		s, ok := e.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected array element %T", e)
		}
		out[i] = s
	}
	return out, nil
}
