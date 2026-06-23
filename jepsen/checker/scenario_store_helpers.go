package main

import (
	"bytes"
	"fmt"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// scenario_store_helpers.go holds the small imperative-shell helpers the
// store-linz driver (scenario_store.go) needs but that are not part of the
// pure model: how to obtain a live Redis client, how to stamp/recover the
// (clientId, opSeq) frame identity in the payload bytes, and how to classify
// an append error as commit-indeterminate. They are deliberately kept out of
// model_store.go so the model stays an I/O-free, dependency-free oracle.

// storeRedisClient builds a go-redis client for the store-linz driver from
// -redis-url / $REDIS_URL, the same source the contention/shard drivers use
// (contentionStore in scenario_shard.go). The store-linz scenario talks to
// Redis directly (no cluster), so a plain client over the configured URL is
// all it needs; redisstore.New takes ownership and closes it on Close().
func storeRedisClient(c config) (redis.UniversalClient, error) {
	url := c.redisURL
	if url == "" {
		url = os.Getenv("REDIS_URL")
	}
	if url == "" {
		return nil, fmt.Errorf("need -redis-url or $REDIS_URL (e.g. redis://localhost:6379/14)")
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url %q: %w", url, err)
	}
	return redis.NewClient(opt), nil
}

// closeIn / getOffIn / readIn construct the parameter-free (or offset-only)
// storeInput values the live driver records for CLOSE / GETOFFSET / READ. They
// are production constructors (the model_store_test.go unit tests reuse them),
// so they live here rather than in the _test.go file the driver cannot import.
func closeIn(path string) storeInput  { return storeInput{path: path, op: opClose} }
func getOffIn(path string) storeInput { return storeInput{path: path, op: opGetOffset} }
func readIn(path string, from uint64) storeInput {
	return storeInput{path: path, op: opRead, readFrom: from}
}

// frameField separates the two header integers; framePad is the payload filler.
const (
	frameField = byte(':')
	framePad   = byte('.')
)

// encodeFrame stamps a frame's (clientId, opSeq) producer coordinate into the
// payload bytes — the Elle-recoverability idea — so a reader can recover the
// exact frame identity the writer committed (decodeClient / decodeSeq), making
// the model's READ step compare frame IDENTITY, not just byte length.
//
// Layout: "<clientID>:<opSeq>:" followed by pad bytes. pad is the requested
// number of trailing filler bytes (callers pass a small random count to vary
// frame sizes); the total length is len(header)+pad, and nbytes in the frame
// tag is taken from len(the returned slice), so the model's tail arithmetic
// (Offset.Add by exactly len(data)) lines up with what the store records.
func encodeFrame(clientID, seq, pad int) []byte {
	if pad < 0 {
		pad = 0
	}
	var b bytes.Buffer
	b.WriteString(strconv.Itoa(clientID))
	b.WriteByte(frameField)
	b.WriteString(strconv.Itoa(seq))
	b.WriteByte(frameField)
	for i := 0; i < pad; i++ {
		b.WriteByte(framePad)
	}
	return b.Bytes()
}

// decodeClient recovers the clientID a frame's payload was stamped with. It
// returns -1 if the bytes are not a frame this driver produced (so a phantom /
// foreign frame is never silently mapped onto a real producer coordinate).
func decodeClient(data []byte) int {
	v, _, ok := decodeFrame(data)
	if !ok {
		return -1
	}
	return v
}

// decodeSeq recovers the opSeq a frame's payload was stamped with, or -1 if the
// bytes are not a frame this driver produced.
func decodeSeq(data []byte) int {
	_, v, ok := decodeFrame(data)
	if !ok {
		return -1
	}
	return v
}

// decodeFrame parses the "<clientID>:<opSeq>:" header. It returns ok=false on
// any malformed payload so a corrupted/foreign frame surfaces as a phantom
// (-1,-1) tag the model rejects rather than as a plausible-looking identity.
func decodeFrame(data []byte) (clientID, seq int, ok bool) {
	i := bytes.IndexByte(data, frameField)
	if i < 0 {
		return 0, 0, false
	}
	clientID, err := strconv.Atoi(string(data[:i]))
	if err != nil {
		return 0, 0, false
	}
	rest := data[i+1:]
	j := bytes.IndexByte(rest, frameField)
	if j < 0 {
		return 0, 0, false
	}
	seq, err = strconv.Atoi(string(rest[:j]))
	if err != nil {
		return 0, 0, false
	}
	return clientID, seq, true
}

// isContentionExhausted reports whether an append error is the bounded
// optimistic re-frame loop giving up (store/redis/store.go: "too much
// contention" after maxAppendRetries stRetry bounces). The store wraps this as
// a plain fmt.Errorf with no sentinel, so it is matched on the stable message
// substring. A contention-exhausted append is commit-INDETERMINATE: the last
// EVAL may or may not have landed before the loop bailed, exactly like a
// timeout, so the model linearizes it committed-or-not (INV-LIN-02).
func isContentionExhausted(err error) bool {
	if err == nil {
		return false
	}
	return bytes.Contains([]byte(err.Error()), []byte("too much contention"))
}
