// Cursor generation ported from the Durable Streams reference Caddy plugin
// (packages/caddy-plugin/handler.go @ 82f9963), lifted into pure functions:
// the clock is an argument, so cursor behavior is fully unit-testable.
//
// Cursors prevent CDN cache loops on live reads (PROTOCOL.md §10.1): time is
// divided into fixed intervals counted from an epoch, and servers must return
// a cursor strictly greater than any cursor the client echoes back.
package protocol

import (
	"strconv"
	"time"
)

// CursorEpoch is the protocol cursor epoch: October 9, 2024 00:00:00 UTC.
var CursorEpoch = time.Date(2024, 10, 9, 0, 0, 0, 0, time.UTC)

// CursorIntervalSeconds is the default cursor interval duration.
const CursorIntervalSeconds = 20

// Jitter range in seconds (per protocol spec).
const (
	minJitterSeconds = 1
	maxJitterSeconds = 3600
)

// GenerateCursor returns the interval number for now as a decimal string.
func GenerateCursor(now time.Time) string {
	epochMs := CursorEpoch.UnixMilli()
	nowMs := now.UnixMilli()
	intervalMs := CursorIntervalSeconds * 1000

	intervalNumber := (nowMs - epochMs) / int64(intervalMs)
	return strconv.FormatInt(intervalNumber, 10)
}

// GenerateResponseCursor returns the cursor to set on a live-mode response,
// guaranteeing monotonic progression past any cursor the client echoed.
func GenerateResponseCursor(clientCursor string, now time.Time) string {
	currentCursor := GenerateCursor(now)
	currentInterval, _ := strconv.ParseInt(currentCursor, 10, 64)

	// No client cursor - return current interval
	if clientCursor == "" {
		return currentCursor
	}

	clientInterval, err := strconv.ParseInt(clientCursor, 10, 64)
	if err != nil || clientInterval < currentInterval {
		// Invalid or behind current time - return current interval
		return currentCursor
	}

	// Client cursor is at or ahead - advance past it by a fixed jitter step
	// (the reference implementation uses the middle of the jitter range).
	jitterSeconds := minJitterSeconds + (maxJitterSeconds-minJitterSeconds)/2
	jitterIntervals := int64(1)
	if jitterSeconds/CursorIntervalSeconds > 1 {
		jitterIntervals = int64(jitterSeconds / CursorIntervalSeconds)
	}

	return strconv.FormatInt(clientInterval+jitterIntervals, 10)
}
