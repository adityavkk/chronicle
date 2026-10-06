// Package redistest supports tests that pin the Redis round trips a code path
// spends. Every serial round trip is a full network latency in a remote
// region, so the exact sequence is the latency budget of a request.
package redistest

import (
	"context"
	"strings"
	"sync"

	goredis "github.com/redis/go-redis/v9"
)

// TripLog is a go-redis Hook that records one entry per Redis round trip: a
// single command by name, a pipeline Exec as "pipe(name+name)", and a script
// call as "evalsha:append" when its first key is a stream's meta hash (so it
// runs in that stream's slot) or "evalsha:control" otherwise (the write-fence
// pre-check in the control plane). A script reload after NOSCRIPT is one
// extra round trip (EVALSHA, then EVAL), logged as "eval:..." so a cold
// script cache is diagnosed as such rather than as a regression. It also
// counts append.lua RETRY replies and can run a callback once, just before
// the next append-slot script (to move the tail under an append) or the next
// pipeline Exec (to stall one round trip).
type TripLog struct {
	mu             sync.Mutex
	trips          []string
	retries        int
	beforeAppend   func()
	beforePipeline func()
}

// DialHook implements goredis.Hook; dials are not round trips of the request.
func (l *TripLog) DialHook(next goredis.DialHook) goredis.DialHook { return next }

// ProcessHook implements goredis.Hook.
func (l *TripLog) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		name, slot := cmd.Name(), ""
		if name == "evalsha" || name == "eval" {
			slot = scriptSlot(cmd)
			name += ":" + slot
		}
		var before func()
		l.mu.Lock()
		l.trips = append(l.trips, name)
		if slot == "append" {
			before, l.beforeAppend = l.beforeAppend, nil
		}
		l.mu.Unlock()
		if before != nil {
			before()
		}
		err := next(ctx, cmd)
		if slot == "append" && err == nil && isRetryReply(cmd) {
			l.mu.Lock()
			l.retries++
			l.mu.Unlock()
		}
		return err
	}
}

// ProcessPipelineHook implements goredis.Hook: one pipeline Exec is one trip.
func (l *TripLog) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		names := make([]string, len(cmds))
		for i, cmd := range cmds {
			names[i] = cmd.Name()
		}
		var before func()
		l.mu.Lock()
		l.trips = append(l.trips, "pipe("+strings.Join(names, "+")+")")
		before, l.beforePipeline = l.beforePipeline, nil
		l.mu.Unlock()
		if before != nil {
			before()
		}
		return next(ctx, cmds)
	}
}

// Take returns the trips recorded since the last Take and clears them.
func (l *TripLog) Take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	trips := l.trips
	l.trips = nil
	return trips
}

// Retries is the number of append.lua RETRY replies seen so far.
func (l *TripLog) Retries() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.retries
}

// BeforeAppendScript schedules fn to run once, just before the next
// append-slot script call (never before a control-plane script).
func (l *TripLog) BeforeAppendScript(fn func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.beforeAppend = fn
}

// BeforePipeline schedules fn to run once, just before the next pipeline Exec
// is sent: a stall injected on exactly one round trip.
func (l *TripLog) BeforePipeline(fn func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.beforePipeline = fn
}

// scriptSlot classifies a script call by KEYS[1]: a stream's meta hash means
// the script runs in that stream's slot (append.lua and its siblings).
func scriptSlot(cmd goredis.Cmder) string {
	if args := cmd.Args(); len(args) > 3 {
		if key, _ := args[3].(string); strings.HasPrefix(key, "ds:{") && strings.HasSuffix(key, ":meta") {
			return "append"
		}
	}
	return "control"
}

// isRetryReply reports whether a script reply is append.lua's RETRY.
func isRetryReply(cmd goredis.Cmder) bool {
	c, ok := cmd.(*goredis.Cmd)
	if !ok {
		return false
	}
	reply, ok := c.Val().([]any)
	return ok && len(reply) > 0 && reply[0] == "RETRY"
}
