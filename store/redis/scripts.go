package redis

import (
	"embed"
	"fmt"

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

var appendScript = loadScript("append.lua")

// Status sentinels returned in reply[0] by the mutation scripts.
const (
	stOK          = "OK"
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
	stMatCHED     = "MATCHED"
	stCreated     = "CREATED"
	stAlready     = "ALREADY"
	stDeleted     = "DELETED"
)

// scriptReply is the decoded fixed-shape array reply of append/close scripts:
// {status, tail, producerResult, currentEpoch, expectedSeq, receivedSeq,
// lastSeq, closed}.
type scriptReply struct {
	Status       string
	Tail         string
	ProducerRes  int64
	CurrentEpoch int64
	ExpectedSeq  int64
	ReceivedSeq  int64
	LastSeq      int64
	Closed       bool
}

func decodeScriptReply(v any) (*scriptReply, error) {
	arr, ok := v.([]any)
	if !ok || len(arr) < 8 {
		return nil, fmt.Errorf("unexpected script reply %T %v", v, v)
	}
	s := make([]string, 8)
	for i := 0; i < 8; i++ {
		str, ok := arr[i].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected script reply element %d: %T", i, arr[i])
		}
		s[i] = str
	}
	r := &scriptReply{Status: s[0], Tail: s[1], Closed: s[7] == "1"}
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
		if _, err := fmt.Sscan(f.src, f.dst); err != nil {
			return nil, fmt.Errorf("unexpected numeric reply %q: %w", f.src, err)
		}
	}
	return r, nil
}
