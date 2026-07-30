package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// runPagedCatchup is the issue-5 fault gate for bounded HTTP catch-up. It reads
// a fork across many storage pages while interrupting requests, restarting all
// Chronicle origins, restarting Redis, and appending+closing concurrently. The
// final history is checked against raw Redis ZSET members, not an HTTP read.
func runPagedCatchup(c config) error {
	nem := &nemesis{
		ctx:      fmt.Sprintf("k3d-%s", c.cluster),
		ns:       c.namespace,
		scenario: c.scenario,
	}
	runID := time.Now().UnixNano()
	parent := fmt.Sprintf("paged/%d/parent", runID)
	child := fmt.Sprintf("paged/%d/child", runID)
	defer deleteStream(c.base, child)
	defer deleteStream(c.base, parent)

	seed := c.msgs
	if seed < 32 {
		seed = 32
	}
	var expected []pagedFrame
	parentFrames, parentTail, err := pagedCreateAndAppend(c.base, parent, "parent", 0, seed)
	if err != nil {
		return fmt.Errorf("seed parent: %w", err)
	}
	expected = append(expected, parentFrames...)
	if err := pagedCreateFork(c.base, child, parent, parentTail); err != nil {
		return fmt.Errorf("create fork: %w", err)
	}
	childFrames, _, err := pagedAppendRange(c.base, child, "child", 0, seed)
	if err != nil {
		return fmt.Errorf("seed child: %w", err)
	}
	expected = append(expected, childFrames...)

	fmt.Printf("== paged-catchup: fork=%s inherited=%d own=%d page target is deployment-configured ==\n",
		child, len(parentFrames), len(childFrames))

	cursor := "-1"
	var attempts []pagedAttempt
	addAttempt := func(attempt pagedAttempt) {
		attempts = append(attempts, attempt)
		if len(attempt.Frames) > 0 {
			cursor = attempt.Frames[len(attempt.Frames)-1].Offset
		}
	}

	// Explicit client cancellation after complete envelope entries exercises
	// resume from the last whole frame. With the Jepsen deployment's 1-byte page
	// target, every frame is one page, so this cancellation is between pages.
	attempt, err := fetchPagedAttempt(c.base, child, cursor, "cancel-between-pages", 3,
		func(cancel context.CancelFunc, _ io.Closer) { cancel() })
	if err != nil {
		return err
	}
	addAttempt(attempt)

	// Kill every origin after a few complete frames. The active HTTP connection
	// may end immediately or finish from already-buffered chunks; both outcomes
	// are legal, and the checker constrains any returned frames to the snapshot.
	attempt, err = fetchPagedAttempt(c.base, child, cursor, "chronicle-restart", 3,
		func(_ context.CancelFunc, _ io.Closer) { nem.killAllOrigins() })
	if err != nil {
		return err
	}
	addAttempt(attempt)
	if err := waitReady(c.base, 90*time.Second); err != nil {
		return fmt.Errorf("chronicle did not recover during paged catch-up: %w", err)
	}

	more, _, err := pagedAppendRange(c.base, child, "after-origin", 0, 16)
	if err != nil {
		return fmt.Errorf("append after Chronicle restart: %w", err)
	}
	expected = append(expected, more...)

	attempt, err = fetchPagedAttempt(c.base, child, cursor, "redis-restart", 3,
		func(_ context.CancelFunc, _ io.Closer) { nem.killRedis() })
	if err != nil {
		return err
	}
	addAttempt(attempt)
	if err := waitReady(c.base, 90*time.Second); err != nil {
		return fmt.Errorf("Redis/Chronicle did not recover during paged catch-up: %w", err)
	}

	more, _, err = pagedAppendRange(c.base, child, "after-redis", 0, 16)
	if err != nil {
		return fmt.Errorf("append after Redis restart: %w", err)
	}
	expected = append(expected, more...)

	// When a toxiproxy substrate is supplied, interrupt Chronicle's Redis link.
	// The default k3d gate still injects a real client-side TCP interruption by
	// closing the response body while the server is between flushed pages.
	var healed <-chan struct{}
	if c.toxiproxy != "" {
		done := make(chan struct{})
		healed = done
		proxy := newToxiproxy(c.toxiproxy, c.redisProxy)
		attempt, err = fetchPagedAttempt(c.base, child, cursor, "redis-network-partition", 3,
			func(_ context.CancelFunc, _ io.Closer) {
				_ = proxy.partition()
				go func() {
					defer close(done)
					sleep(750 * time.Millisecond)
					_ = proxy.heal()
				}()
			})
	} else {
		attempt, err = fetchPagedAttempt(c.base, child, cursor, "client-network-interruption", 3,
			func(_ context.CancelFunc, body io.Closer) { _ = body.Close() })
	}
	if err != nil {
		return err
	}
	addAttempt(attempt)
	if healed != nil {
		<-healed
	}

	more, _, err = pagedAppendRange(c.base, child, "before-close", 0, 16)
	if err != nil {
		return fmt.Errorf("append before concurrent close: %w", err)
	}
	expected = append(expected, more...)

	// Capture the response snapshot first, then append and close while its pages
	// are in flight. Those new frames must not leak past the advertised snapshot.
	type appendResult struct {
		frames []pagedFrame
		err    error
	}
	appendDone := make(chan appendResult, 1)
	attempt, err = fetchPagedAttempt(c.base, child, cursor, "concurrent-append-close", 3,
		func(_ context.CancelFunc, _ io.Closer) {
			go func() {
				frames, _, appendErr := pagedAppendRange(c.base, child, "concurrent", 0, 8)
				if appendErr == nil {
					appendErr = pagedClose(c.base, child)
				}
				appendDone <- appendResult{frames: frames, err: appendErr}
			}()
		})
	if err != nil {
		return err
	}
	addAttempt(attempt)
	appended := <-appendDone
	if appended.err != nil {
		return fmt.Errorf("concurrent append+close: %w", appended.err)
	}
	expected = append(expected, appended.frames...)

	// The final retry must drain the new closed snapshot and expose EOF.
	attempt, err = fetchPagedAttempt(c.base, child, cursor, "final-closed-drain", 0, nil)
	if err != nil {
		return err
	}
	addAttempt(attempt)

	oracle, err := pagedRedisForkOracle(nem, parent, child)
	if err != nil {
		return fmt.Errorf("read direct Redis oracle: %w", err)
	}
	if err := comparePagedOracle(expected, oracle); err != nil {
		return fmt.Errorf("redis oracle disagrees with acknowledged appends: %w", err)
	}
	if err := CheckPagedCatchup(oracle, attempts); err != nil {
		return err
	}

	fmt.Println("---- result ----")
	fmt.Printf("attempts:           %d\n", len(attempts))
	fmt.Printf("Redis-oracle frames:%d (fork inherited + own)\n", len(oracle))
	fmt.Printf("nemesis actions:    %d (%s)\n", len(nem.log), join(nem.log))
	fmt.Println("PASS: every captured snapshot was a contiguous Redis-oracle prefix; retries covered the fork exactly once through final close")
	return nil
}

type pagedEnvelope struct {
	Offset string          `json:"offset"`
	Data   json.RawMessage `json:"data"`
}

func fetchPagedAttempt(
	base, stream, start, name string,
	triggerAfter int,
	action func(context.CancelFunc, io.Closer),
) (pagedAttempt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	requestURL := fmt.Sprintf("%s/v1/stream/%s?offset=%s&envelope=1", base, stream, url.QueryEscape(start))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return pagedAttempt{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return pagedAttempt{}, fmt.Errorf("%s start: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return pagedAttempt{}, fmt.Errorf("%s status %d: %s", name, resp.StatusCode, body)
	}

	attempt := pagedAttempt{
		Name:         name,
		StartOffset:  start,
		SnapshotTail: resp.Header.Get("Stream-Next-Offset"),
		Closed:       strings.EqualFold(resp.Header.Get("Stream-Closed"), "true"),
	}
	decoder := json.NewDecoder(resp.Body)
	token, err := decoder.Token()
	if err != nil {
		return attempt, fmt.Errorf("%s envelope start: %w", name, err)
	}
	if token != json.Delim('[') {
		return attempt, fmt.Errorf("%s envelope starts with %v, want [", name, token)
	}

	triggered := false
	for decoder.More() {
		var frame pagedEnvelope
		if err := decoder.Decode(&frame); err != nil {
			if action != nil && triggered {
				return attempt, nil
			}
			return attempt, fmt.Errorf("%s decode: %w", name, err)
		}
		attempt.Frames = append(attempt.Frames, pagedFrame{Offset: frame.Offset, Data: bytes.Clone(frame.Data)})
		if action != nil && !triggered && len(attempt.Frames) >= triggerAfter {
			triggered = true
			action(cancel, resp.Body)
		}
	}
	if _, err := decoder.Token(); err != nil {
		if action != nil && triggered {
			return attempt, nil
		}
		return attempt, fmt.Errorf("%s envelope end: %w", name, err)
	}
	attempt.Complete = true
	if action != nil && !triggered {
		return attempt, fmt.Errorf("%s did not reach its %d-frame fault trigger", name, triggerAfter)
	}
	return attempt, nil
}

func pagedCreateAndAppend(base, stream, scope string, from, count int) ([]pagedFrame, string, error) {
	if count < 1 {
		return nil, "", fmt.Errorf("create count must be positive")
	}
	first, err := json.Marshal(map[string]any{"scope": scope, "n": from})
	if err != nil {
		return nil, "", err
	}
	requestURL := fmt.Sprintf("%s/v1/stream/%s", base, stream)
	var tail string
	err = retry(160, 500*time.Millisecond, func() error {
		req, _ := http.NewRequest(http.MethodPut, requestURL, bytes.NewReader(first))
		req.Header.Set("Content-Type", "application/json")
		resp, doErr := http.DefaultClient.Do(req)
		if doErr != nil {
			return doErr
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			return fmt.Errorf("create status %d", resp.StatusCode)
		}
		tail = resp.Header.Get("Stream-Next-Offset")
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	frames := make([]pagedFrame, 1, count)
	frames[0] = pagedFrame{Offset: tail, Data: first}
	if count == 1 {
		return frames, tail, nil
	}
	rest, tail, err := pagedAppendRange(base, stream, scope, from+1, count-1)
	return append(frames, rest...), tail, err
}

func pagedAppendRange(base, stream, scope string, from, count int) ([]pagedFrame, string, error) {
	requestURL := fmt.Sprintf("%s/v1/stream/%s", base, stream)
	frames := make([]pagedFrame, 0, count)
	tail := ""
	for n := from; n < from+count; n++ {
		payload, _ := json.Marshal(map[string]any{"scope": scope, "n": n})
		err := retry(160, 500*time.Millisecond, func() error {
			req, _ := http.NewRequest(http.MethodPost, requestURL, bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			resp, doErr := http.DefaultClient.Do(req)
			if doErr != nil {
				return doErr
			}
			defer resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				return fmt.Errorf("append status %d", resp.StatusCode)
			}
			tail = resp.Header.Get("Stream-Next-Offset")
			return nil
		})
		if err != nil {
			return frames, tail, err
		}
		frames = append(frames, pagedFrame{Offset: tail, Data: payload})
	}
	return frames, tail, nil
}

func pagedCreateFork(base, child, parent, parentTail string) error {
	requestURL := fmt.Sprintf("%s/v1/stream/%s", base, child)
	return retry(160, 500*time.Millisecond, func() error {
		req, _ := http.NewRequest(http.MethodPut, requestURL, nil)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Stream-Forked-From", "/v1/stream/"+parent)
		req.Header.Set("Stream-Fork-Offset", parentTail)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			return fmt.Errorf("fork status %d: %s", resp.StatusCode, body)
		}
		return nil
	})
}

func pagedClose(base, stream string) error {
	requestURL := fmt.Sprintf("%s/v1/stream/%s", base, stream)
	return retry(160, 500*time.Millisecond, func() error {
		req, _ := http.NewRequest(http.MethodPost, requestURL, nil)
		req.Header.Set("Stream-Closed", "true")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("close status %d", resp.StatusCode)
		}
		return nil
	})
}

func deleteStream(base, stream string) {
	req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/v1/stream/%s", base, stream), nil)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
}

func pagedRedisForkOracle(nem *nemesis, parent, child string) ([]pagedFrame, error) {
	parentFrames, err := pagedRedisFrames(nem, "/"+parent)
	if err != nil {
		return nil, err
	}
	childFrames, err := pagedRedisFrames(nem, "/"+child)
	if err != nil {
		return nil, err
	}
	return append(parentFrames, childFrames...), nil
}

func pagedRedisFrames(nem *nemesis, path string) ([]pagedFrame, error) {
	key := "ds:{" + pagedEscapePath(path) + "}:msg"
	out, err := nem.redisCLI("--raw", "ZRANGE", key, "0", "-1")
	if err != nil {
		return nil, err
	}
	raw := strings.TrimSuffix(string(out), "\n")
	if raw == "" {
		return nil, nil
	}
	lines := strings.Split(raw, "\n")
	frames := make([]pagedFrame, 0, len(lines))
	for _, member := range lines {
		if len(member) < 34 || member[33] != '|' {
			return nil, fmt.Errorf("malformed Redis frame %q", member)
		}
		frames = append(frames, pagedFrame{
			Offset: member[:33],
			Data:   []byte(member[34:]),
		})
	}
	return frames, nil
}

func pagedEscapePath(path string) string {
	return strings.NewReplacer("%", "%25", "{", "%7B", "}", "%7D").Replace(path)
}

func comparePagedOracle(want, got []pagedFrame) error {
	if len(got) != len(want) {
		return fmt.Errorf("frame count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Offset != want[i].Offset || !bytes.Equal(got[i].Data, want[i].Data) {
			return fmt.Errorf("frame %d = (%q,%q), want (%q,%q)",
				i, got[i].Offset, got[i].Data, want[i].Offset, want[i].Data)
		}
	}
	return nil
}
