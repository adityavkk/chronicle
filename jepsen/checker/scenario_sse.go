package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

const sseResumePaddingBytes = 128 << 10

var sseResumePadding = strings.Repeat("x", sseResumePaddingBytes)

// sse-resume checks the data-plane live-read property under Chronicle and Redis
// restarts. Every client must observe a forward-only sequence that covers every
// durable message exactly once and ends at the closed stream's final tail.
// Data is committed to the observation only with its following control event;
// an interrupted, uncheckpointed data event is discarded before reconnect.
func runSSEResume(c config, nem *nemesis) error {
	if c.msgs < 12 {
		return fmt.Errorf("sse-resume requires -msgs >= 12, got %d", c.msgs)
	}
	if err := waitReady(c.base, 60*time.Second); err != nil {
		return fmt.Errorf("chronicle not ready: %w", err)
	}

	stream := fmt.Sprintf("events/sse-%d", time.Now().UnixNano())
	tail, err := createStream(c.base, stream)
	if err != nil {
		return fmt.Errorf("create SSE stream: %w", err)
	}
	expectedOffsets := make([]string, c.msgs)
	expectedOffsets[0] = tail

	clientCount := max(c.workers, 8)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	results := make([]sseObservation, clientCount)
	errs := make([]error, clientCount)
	attached := make([]chan struct{}, clientCount)
	sessionCounts := make([]atomic.Int64, clientCount)
	seen := make(chan sseSeen, clientCount*c.msgs*2)
	var clients sync.WaitGroup
	for i := range clientCount {
		attached[i] = make(chan struct{})
		clients.Add(1)
		go func(index int) {
			defer clients.Done()
			results[index], errs[index] = readSSEUntilClosed(
				ctx,
				c.base,
				stream,
				index,
				attached[index],
				&sessionCounts[index],
				seen,
			)
		}(i)
	}

	// An HTTP 200 is sent only after the hub's subscription is confirmed. Begin
	// appending after all readers have that header but while their first durable
	// catch-up may still be in progress. This targets the catch-up/live handoff.
	for i, ready := range attached {
		select {
		case <-ready:
		case <-time.After(30 * time.Second):
			cancel()
			clients.Wait()
			return fmt.Errorf("client %d did not attach before the fault phase", i)
		}
	}

	lostHintSeq := max(c.msgs/4, 2)
	reconnectSeq := lostHintSeq + 1
	singleRestartSeq := max(c.msgs/3, reconnectSeq+1)
	duplicateHintSeq := max(c.msgs/2, singleRestartSeq+2)
	redisRestartSeq := max((2*c.msgs)/3, duplicateHintSeq+2)
	reconnectBefore, err := sumPodMetric(
		nem,
		"chronicle_sse_subscription_events_total",
		`event="reconnect"`,
	)
	if err != nil {
		cancel()
		clients.Wait()
		return fmt.Errorf("read reconnect metric before faults: %w", err)
	}
	lagStream := fmt.Sprintf("events/sse-lag-resume-%d", time.Now().UnixNano())
	if _, err := createStream(c.base, lagStream); err != nil {
		cancel()
		clients.Wait()
		return fmt.Errorf("create isolated lag-resume stream: %w", err)
	}
	activeClientsBefore, err := samplePodMetric(nem, "chronicle_sse_clients", "")
	if err != nil {
		cancel()
		clients.Wait()
		return fmt.Errorf("sample active clients before direct slow reader: %w", err)
	}
	if err := startInClusterResumingReader(nem, lagStream); err != nil {
		cancel()
		clients.Wait()
		return err
	}
	lagPod, _, err := waitForSinglePodMetricIncrease(
		nem,
		"chronicle_sse_clients",
		"",
		activeClientsBefore,
		15*time.Second,
	)
	if err != nil {
		cancel()
		clients.Wait()
		return fmt.Errorf("identify Chronicle pod serving direct slow reader: %w", err)
	}
	laggedBefore, err := readPodMetric(
		nem,
		lagPod,
		"chronicle_sse_lagged_disconnects_total",
		"",
	)
	if err != nil {
		cancel()
		clients.Wait()
		return fmt.Errorf("read lag metric from slow-reader pod %s: %w", lagPod.name, err)
	}
	laggedAfter, err := exerciseSSELagResume(
		nem,
		c.base,
		lagStream,
		lagPod,
		laggedBefore,
	)
	if err != nil {
		cancel()
		clients.Wait()
		return err
	}
	var reconnectAfter, writeTimeoutBefore, writeTimeoutAfter float64

	for n := 1; n < c.msgs; n++ {
		appendNormally := true
		switch n {
		case lostHintSeq:
			next, appendErr := appendSSEWithoutNotification(
				nem,
				stream,
				n,
				expectedOffsets[n-1],
			)
			if appendErr != nil {
				cancel()
				clients.Wait()
				return fmt.Errorf("append message %d without notification: %w", n, appendErr)
			}
			expectedOffsets[n] = next
			appendNormally = false
			nem.record("drop-single-pubsub-hint")
		case reconnectSeq:
			out, killErr := nem.redisCLI("CLIENT", "KILL", "TYPE", "pubsub")
			if killErr != nil {
				cancel()
				clients.Wait()
				return fmt.Errorf("kill Pub/Sub connections: %w: %s", killErr, out)
			}
			killed, parseErr := parseRedisInteger(out)
			if parseErr != nil {
				cancel()
				clients.Wait()
				return fmt.Errorf("parse killed Pub/Sub client count: %w", parseErr)
			}
			if killed < 1 {
				cancel()
				clients.Wait()
				return fmt.Errorf("kill Pub/Sub connections affected %d clients: %q", killed, out)
			}
			reconnectAfter, err = waitMetricIncrease(
				nem,
				"chronicle_sse_subscription_events_total",
				`event="reconnect"`,
				reconnectBefore,
				30*time.Second,
			)
			if err != nil {
				cancel()
				clients.Wait()
				return fmt.Errorf("redis Pub/Sub reconnect was not observed: %w", err)
			}
			nem.record("kill-pubsub-verified")
		case singleRestartSeq:
			before := snapshotSessions(sessionCounts)
			if err := replaceChroniclePodServingClients(
				nem,
				"kill-origin-verified",
				90*time.Second,
			); err != nil {
				cancel()
				clients.Wait()
				return err
			}
			if err := waitAnySessionAdvance(sessionCounts, before, 20*time.Second); err != nil {
				cancel()
				clients.Wait()
				return fmt.Errorf("no SSE reader crossed the single-origin restart: %w", err)
			}
		case redisRestartSeq:
			redisReconnectBefore, metricErr := sumPodMetric(
				nem,
				"chronicle_sse_subscription_events_total",
				`event="reconnect"`,
			)
			if metricErr != nil {
				cancel()
				clients.Wait()
				return fmt.Errorf("read reconnect metric before Redis replacement: %w", metricErr)
			}
			if err := replaceOnePod(nem, "app=redis", "kill-redis-verified", 90*time.Second); err != nil {
				cancel()
				clients.Wait()
				return err
			}
			reconnectAfter, err = waitMetricIncrease(
				nem,
				"chronicle_sse_subscription_events_total",
				`event="reconnect"`,
				redisReconnectBefore,
				30*time.Second,
			)
			if err != nil {
				cancel()
				clients.Wait()
				return fmt.Errorf("chronicle did not reconnect after Redis replacement: %w", err)
			}
		}
		if appendNormally {
			if next, appendErr := appendSSEOne(c.base, stream, n); appendErr != nil {
				cancel()
				clients.Wait()
				return fmt.Errorf("append message %d: %w", n, appendErr)
			} else if next != "" {
				expectedOffsets[n] = next
			}
		}

		if n == lostHintSeq {
			if err := waitForSeenMessage(seen, n, clientCount, 6*time.Second); err != nil {
				cancel()
				clients.Wait()
				return fmt.Errorf("lost notification was not recovered within the poll bound: %w", err)
			}
			writeTimeoutBefore, writeTimeoutAfter, err = exerciseSSEWriteTimeout(
				nem,
				c.base,
			)
			if err != nil {
				cancel()
				clients.Wait()
				return err
			}
		}
		if n == duplicateHintSeq {
			channel := redisNotifyChannel("/" + stream)
			out, publishErr := nem.redisCLI("PUBLISH", channel, "a")
			if publishErr != nil {
				cancel()
				clients.Wait()
				return fmt.Errorf("publish duplicate notification: %w: %s", publishErr, out)
			}
			subscribers, parseErr := parseRedisInteger(out)
			if parseErr != nil {
				cancel()
				clients.Wait()
				return fmt.Errorf("parse duplicate notification subscriber count: %w", parseErr)
			}
			if subscribers < 1 {
				cancel()
				clients.Wait()
				return fmt.Errorf("duplicate notification reached %d subscribers: %q", subscribers, out)
			}
			nem.record("duplicate-pubsub-hint-verified")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Force every still-connected reader through an origin loss before the
	// final close. The resumed connection must use its last durable control
	// offset and still observe EOF after the close.
	sessionsBeforeAll := snapshotSessions(sessionCounts)
	if err := replaceAllPods(nem, "app=chronicle", "kill-all-origins-verified", 90*time.Second); err != nil {
		cancel()
		clients.Wait()
		return err
	}
	if err := waitReady(c.base, 90*time.Second); err != nil {
		cancel()
		clients.Wait()
		return fmt.Errorf("chronicle did not recover before close: %w", err)
	}
	if err := waitEverySessionAdvance(sessionCounts, sessionsBeforeAll, 30*time.Second); err != nil {
		cancel()
		clients.Wait()
		return fmt.Errorf("not every SSE reader crossed the all-origin restart: %w", err)
	}
	closeIssued := time.Now()
	tail, err = closeSSEStream(c.base, stream)
	if err != nil {
		cancel()
		clients.Wait()
		return err
	}
	if tail != expectedOffsets[len(expectedOffsets)-1] {
		cancel()
		clients.Wait()
		return fmt.Errorf("close tail %s, want last append tail %s", tail, expectedOffsets[len(expectedOffsets)-1])
	}

	clients.Wait()
	var failures []error
	var delivered int
	for i := range clientCount {
		if errs[i] != nil {
			failures = append(failures, fmt.Errorf("client %d: %w", i, errs[i]))
			continue
		}
		if results[i].closedAt.Before(closeIssued) {
			failures = append(failures, fmt.Errorf("client %d observed streamClosed before close was issued", i))
			continue
		}
		if err := checkSSEObservation(results[i], expectedOffsets, tail); err != nil {
			failures = append(failures, fmt.Errorf("client %d: %w", i, err))
			continue
		}
		delivered += len(results[i].messages)
	}
	durable, durableTail, durableClosed, err := readClosedSSESequence(c.base, stream)
	if err != nil {
		failures = append(failures, fmt.Errorf("closed-stream HTTP read: %w", err))
	} else {
		if durableTail != tail || !durableClosed {
			failures = append(failures, fmt.Errorf(
				"durable stream state tail=%s closed=%t, want tail=%s closed=true",
				durableTail,
				durableClosed,
				tail,
			))
		}
		for i, message := range durable {
			if i >= c.msgs || message != i {
				failures = append(failures, fmt.Errorf("durable message %d = %d, want %d", i, message, i))
				break
			}
		}
		if len(durable) != c.msgs {
			failures = append(failures, fmt.Errorf("durable messages = %d, want %d", len(durable), c.msgs))
		}
	}
	redisState, err := readRedisSSEState(nem, stream)
	if err != nil {
		failures = append(failures, fmt.Errorf("direct Redis durable-state read: %w", err))
	} else if err := validateRedisSSEState(
		redisState,
		c.msgs,
		expectedOffsets,
		tail,
	); err != nil {
		failures = append(failures, fmt.Errorf("direct Redis durable-state oracle: %w", err))
	}

	fmt.Println("---- result ----")
	fmt.Printf("scenario:          %s\n", c.scenario)
	fmt.Printf("clients complete:  %d/%d\n", clientCount-len(failures), clientCount)
	fmt.Printf("messages expected: %d per client\n", c.msgs)
	fmt.Printf("messages observed: %d total\n", delivered)
	fmt.Printf("subscription reconnects: %.0f -> %.0f\n", reconnectBefore, reconnectAfter)
	fmt.Printf("lagged disconnects:      %.0f -> %.0f\n", laggedBefore, laggedAfter)
	fmt.Printf("write timeouts:          %.0f -> %.0f\n", writeTimeoutBefore, writeTimeoutAfter)
	fmt.Printf("final tail:        %s\n", short(tail))
	fmt.Printf("nemesis actions:   %d (%s)\n", len(nem.log), join(nem.log))
	if len(failures) > 0 {
		for _, failure := range failures {
			fmt.Printf("  FAIL %v\n", failure)
		}
		return fmt.Errorf("SSE resume property failed for %d/%d clients", len(failures), clientCount)
	}
	fmt.Println("PASS: every SSE client observed each durable message exactly once and resumed to the closed tail")
	return nil
}

type sseObservation struct {
	messages      []int
	controls      []string
	frames        []sseObservedFrame
	raw           []sseRawMessage
	sessionStarts []string
	closed        bool
	closedAt      time.Time
}

type sseObservedFrame struct {
	session  int
	messages []int
	control  string
	closed   bool
	at       time.Time
}

type sseRawMessage struct {
	session int
	message int
}

type sseSeen struct {
	client  int
	message int
}

type sseControl struct {
	StreamNextOffset string `json:"streamNextOffset"`
	StreamClosed     bool   `json:"streamClosed"`
}

type sseEvent struct {
	eventType string
	data      []byte
}

type sseParser struct {
	eventType string
	data      [][]byte
}

func (p *sseParser) line(line []byte) (sseEvent, bool) {
	if len(line) == 0 {
		if p.eventType == "" && len(p.data) == 0 {
			return sseEvent{}, false
		}
		event := sseEvent{
			eventType: p.eventType,
			data:      bytes.Join(p.data, []byte{'\n'}),
		}
		p.eventType = ""
		p.data = nil
		return event, true
	}
	if line[0] == ':' {
		return sseEvent{}, false
	}
	field, value, _ := bytes.Cut(line, []byte{':'})
	value = bytes.TrimPrefix(value, []byte{' '})
	switch string(field) {
	case "event":
		p.eventType = string(value)
	case "data":
		p.data = append(p.data, append([]byte(nil), value...))
	}
	return sseEvent{}, false
}

func readSSEUntilClosed(
	ctx context.Context,
	base string,
	stream string,
	clientIndex int,
	attached chan struct{},
	sessionCount *atomic.Int64,
	seen chan<- sseSeen,
) (sseObservation, error) {
	observation := sseObservation{}
	offset := "-1"
	var lastErr error
	var attachOnce sync.Once
	client := http.DefaultClient

	for ctx.Err() == nil {
		endpoint := fmt.Sprintf(
			"%s/v1/stream/%s?offset=%s&live=sse",
			base,
			stream,
			url.QueryEscape(offset),
		)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return observation, err
		}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			if !sleepContextChecker(ctx, 100*time.Millisecond) {
				break
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			lastErr = fmt.Errorf("SSE status %d", resp.StatusCode)
			if !sleepContextChecker(ctx, 100*time.Millisecond) {
				break
			}
			continue
		}

		session := int(sessionCount.Add(1))
		observation.sessionStarts = append(observation.sessionStarts, offset)
		attachOnce.Do(func() { close(attached) })

		var parser sseParser
		var pending []int
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 64<<10), 2<<20)
		for scanner.Scan() {
			event, complete := parser.line(scanner.Bytes())
			if !complete {
				continue
			}
			switch event.eventType {
			case "data":
				var batch []struct {
					N int `json:"n"`
				}
				if err := json.Unmarshal(event.data, &batch); err != nil {
					resp.Body.Close()
					return observation, fmt.Errorf("decode data event: %w", err)
				}
				for _, message := range batch {
					pending = append(pending, message.N)
					observation.raw = append(observation.raw, sseRawMessage{
						session: session,
						message: message.N,
					})
				}
			case "control":
				var control sseControl
				if err := json.Unmarshal(event.data, &control); err != nil {
					resp.Body.Close()
					return observation, fmt.Errorf("decode control event: %w", err)
				}
				if control.StreamNextOffset == "" {
					resp.Body.Close()
					return observation, errors.New("control event omitted streamNextOffset")
				}
				now := time.Now()
				committed := append([]int(nil), pending...)
				observation.messages = append(observation.messages, pending...)
				pending = nil
				observation.controls = append(observation.controls, control.StreamNextOffset)
				observation.frames = append(observation.frames, sseObservedFrame{
					session:  session,
					messages: committed,
					control:  control.StreamNextOffset,
					closed:   control.StreamClosed,
					at:       now,
				})
				for _, message := range committed {
					select {
					case seen <- sseSeen{client: clientIndex, message: message}:
					default:
					}
				}
				offset = control.StreamNextOffset
				if control.StreamClosed {
					observation.closed = true
					observation.closedAt = now
					resp.Body.Close()
					return observation, nil
				}
			}
		}
		if err := scanner.Err(); err != nil {
			lastErr = err
		}
		resp.Body.Close()
		if !sleepContextChecker(ctx, 50*time.Millisecond) {
			break
		}
	}
	if lastErr == nil {
		lastErr = ctx.Err()
	}
	return observation, fmt.Errorf("SSE did not reach closed tail: %w", lastErr)
}

func checkSSEObservation(
	observation sseObservation,
	expectedOffsets []string,
	finalTail string,
) error {
	wantMessages := len(expectedOffsets)
	if !observation.closed {
		return errors.New("streamClosed control was not observed")
	}
	if len(observation.controls) == 0 {
		return errors.New("no control events were observed")
	}
	if observation.controls[len(observation.controls)-1] != finalTail {
		return fmt.Errorf(
			"final control offset %s, want %s",
			observation.controls[len(observation.controls)-1],
			finalTail,
		)
	}

	var previous store.Offset
	for i, raw := range observation.controls {
		offset, err := store.ParseOffset(raw)
		if err != nil {
			return fmt.Errorf("control %d has invalid offset %q: %w", i, raw, err)
		}
		if i > 0 && offset.LessThan(previous) {
			return fmt.Errorf("control offset regressed at %d: %s before %s", i, previous, offset)
		}
		previous = offset
	}

	nextNew := 0
	for i, message := range observation.messages {
		if message < 0 || message >= wantMessages {
			return fmt.Errorf("message %d has unexpected sequence %d", i, message)
		}
		if message != nextNew {
			return fmt.Errorf(
				"message sequence violation at %d: got %d, want %d",
				i,
				message,
				nextNew,
			)
		}
		nextNew++
	}
	if nextNew != wantMessages {
		return fmt.Errorf("message sequence %d was never observed", nextNew)
	}

	offsetIndex := map[string]int{store.ZeroOffset.String(): 0, "-1": 0}
	for message, offset := range expectedOffsets {
		if offset == "" {
			return fmt.Errorf("expected offset for message %d is empty", message)
		}
		offsetIndex[offset] = message + 1
	}
	sessionNext := make(map[int]int, len(observation.sessionStarts))
	sessionControl := make(map[int]string, len(observation.sessionStarts))
	for i, start := range observation.sessionStarts {
		next, ok := offsetIndex[start]
		if !ok {
			return fmt.Errorf("session %d started from unknown offset %s", i+1, start)
		}
		sessionNext[i+1] = next
		if start == "-1" {
			start = store.ZeroOffset.String()
		}
		sessionControl[i+1] = start
	}

	closedFrames := 0
	for i, frame := range observation.frames {
		next, ok := sessionNext[frame.session]
		if !ok {
			return fmt.Errorf("frame %d refers to unknown session %d", i, frame.session)
		}
		expectedControl := sessionControl[frame.session]
		for j, message := range frame.messages {
			if message != next {
				return fmt.Errorf(
					"session %d frame %d message %d = %d, want %d",
					frame.session,
					i,
					j,
					message,
					next,
				)
			}
			expectedControl = expectedOffsets[message]
			next++
		}
		if frame.control != expectedControl {
			return fmt.Errorf(
				"session %d frame %d control %s checkpointed %d message(s), want %s",
				frame.session,
				i,
				frame.control,
				len(frame.messages),
				expectedControl,
			)
		}
		sessionNext[frame.session] = next
		sessionControl[frame.session] = frame.control
		if frame.closed {
			closedFrames++
			if i != len(observation.frames)-1 {
				return fmt.Errorf("streamClosed frame %d was not the final frame", i)
			}
		}
	}
	if closedFrames != 1 {
		return fmt.Errorf("streamClosed frames = %d, want 1", closedFrames)
	}

	rawNext := make(map[int]int, len(observation.sessionStarts))
	for i, start := range observation.sessionStarts {
		rawNext[i+1] = offsetIndex[start]
	}
	for i, raw := range observation.raw {
		next, ok := rawNext[raw.session]
		if !ok {
			return fmt.Errorf("raw message %d refers to unknown session %d", i, raw.session)
		}
		if raw.message < 0 || raw.message >= len(expectedOffsets) {
			return fmt.Errorf(
				"raw session %d message %d has unexpected sequence %d",
				raw.session,
				i,
				raw.message,
			)
		}
		if raw.message != next {
			return fmt.Errorf(
				"raw session %d message %d = %d, want %d from its resume offset",
				raw.session,
				i,
				raw.message,
				next,
			)
		}
		rawNext[raw.session] = next + 1
	}
	return nil
}

// appendSSEOne uses the producer fence so an HTTP retry after an ambiguous
// origin failure cannot append the same numbered message twice. This keeps the
// history suitable for an exact-once observation check without weakening the
// server's at-least-once transport behavior.
func appendSSEOne(base, stream string, seq int) (string, error) {
	var tail string
	endpoint := fmt.Sprintf("%s/v1/stream/%s", base, stream)
	payload, err := ssePayload(seq)
	if err != nil {
		return "", err
	}
	err = retry(160, 500*time.Millisecond, func() error {
		req, _ := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(protocol.HeaderProducerId, "jepsen-sse-resume")
		req.Header.Set(protocol.HeaderProducerEpoch, "0")
		req.Header.Set(protocol.HeaderProducerSeq, strconv.Itoa(seq-1))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("append message %d status %d: %s", seq, resp.StatusCode, body)
		}
		tail = resp.Header.Get(protocol.HeaderStreamNextOffset)
		if tail == "" {
			return fmt.Errorf("append message %d omitted Stream-Next-Offset", seq)
		}
		return nil
	})
	return tail, err
}

func ssePayload(seq int) ([]byte, error) {
	return json.Marshal(struct {
		N   int    `json:"n"`
		Pad string `json:"pad"`
	}{N: seq, Pad: sseResumePadding})
}

// appendSSEWithoutNotification writes exactly the frame and producer state
// that appendSSEOne would commit, but deliberately omits PUBLISH. This is a
// narrow Jepsen nemesis for one lost fire-and-forget hint; the normal durable
// read at the end independently verifies the resulting stream.
func appendSSEWithoutNotification(
	nem *nemesis,
	stream string,
	seq int,
	previousTail string,
) (string, error) {
	previous, err := store.ParseOffset(previousTail)
	if err != nil {
		return "", err
	}
	payload, err := ssePayload(seq)
	if err != nil {
		return "", err
	}
	next := previous.Add(uint64(len(payload))).String()
	path := "/" + strings.TrimPrefix(stream, "/")
	tag := "{" + escapeRedisPath(path) + "}"
	const script = `
local current = redis.call('HGET', KEYS[1], 'tail')
if current ~= ARGV[1] then return {'TAIL', current or ''} end
local payload = '{"n":' .. ARGV[4] .. ',"pad":"' ..
  string.rep('x', tonumber(ARGV[5])) .. '"}'
if #payload ~= tonumber(ARGV[6]) then return {'LENGTH', tostring(#payload)} end
redis.call('ZADD', KEYS[2], '0', ARGV[2] .. '|' .. payload)
redis.call('HSET', KEYS[1], 'tail', ARGV[2])
redis.call('HSET', KEYS[3], 'jepsen-sse-resume', '0:' .. ARGV[3] .. ':0')
return ARGV[2]
`
	out, err := nem.redisCLI(
		"EVAL",
		script,
		"3",
		"ds:"+tag+":meta",
		"ds:"+tag+":msg",
		"ds:"+tag+":prod",
		previousTail,
		next,
		strconv.Itoa(seq-1),
		strconv.Itoa(seq),
		strconv.Itoa(sseResumePaddingBytes),
		strconv.Itoa(len(payload)),
	)
	if err != nil {
		return "", fmt.Errorf("redis append-without-publish: %w: %s", err, out)
	}
	if got := strings.TrimSpace(string(out)); got != next {
		return "", fmt.Errorf("redis append-without-publish returned %q, want %q", got, next)
	}
	return next, nil
}

func escapeRedisPath(path string) string {
	return strings.NewReplacer("%", "%25", "{", "%7B", "}", "%7D").Replace(path)
}

func redisNotifyChannel(path string) string {
	return "ds:notify:{" + escapeRedisPath(path) + "}"
}

func parseRedisInteger(out []byte) (int, error) {
	value := strings.TrimSpace(string(out))
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse Redis integer %q: %w", value, err)
	}
	return n, nil
}

func exerciseSSELagResume(
	nem *nemesis,
	base string,
	stream string,
	pod kubePod,
	laggedBefore float64,
) (float64, error) {
	// Each padded message exceeds the configured replay byte limit. A burst
	// while the direct reader is paused must evict its next event and disconnect
	// that reader from the pod identified by the active-client metric delta.
	const lagProbeMessages = 64
	for seq := 1; seq <= lagProbeMessages; seq++ {
		if _, err := appendSSEOne(base, stream, seq); err != nil {
			return 0, fmt.Errorf("append isolated lag probe %d: %w", seq, err)
		}
	}
	laggedAfter, err := waitPodMetricIncrease(
		nem,
		pod,
		"chronicle_sse_lagged_disconnects_total",
		"",
		laggedBefore,
		30*time.Second,
	)
	if err != nil {
		return laggedAfter, fmt.Errorf(
			"slow reader did not trigger replay lag on serving pod %s: %w",
			pod.name,
			err,
		)
	}
	if err := waitSlowReaderResumed(nem, 30*time.Second); err != nil {
		return laggedAfter, err
	}
	if _, err := closeSSEStream(base, stream); err != nil {
		return laggedAfter, fmt.Errorf("close isolated lag-resume stream: %w", err)
	}
	nem.record("slow-reader-lag-pod-verified")
	return laggedAfter, nil
}

func exerciseSSEWriteTimeout(
	nem *nemesis,
	base string,
) (float64, float64, error) {
	stream := fmt.Sprintf("events/sse-write-timeout-%d", time.Now().UnixNano())
	endpoint := fmt.Sprintf("%s/v1/stream/%s", base, stream)
	req, _ := http.NewRequest(http.MethodPut, endpoint, nil)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("create write-timeout stream: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return 0, 0, fmt.Errorf("create write-timeout stream status %d", resp.StatusCode)
	}

	activeBefore, err := samplePodMetric(nem, "chronicle_sse_clients", "")
	if err != nil {
		return 0, 0, fmt.Errorf("sample active clients before write-timeout reader: %w", err)
	}
	if err := startInClusterStuckReader(nem, stream); err != nil {
		return 0, 0, err
	}
	pod, _, err := waitForSinglePodMetricIncrease(
		nem,
		"chronicle_sse_clients",
		"",
		activeBefore,
		15*time.Second,
	)
	if err != nil {
		return 0, 0, fmt.Errorf("identify Chronicle pod serving stuck reader: %w", err)
	}

	// Sample both values only after the reader is attached and immediately
	// before the append. Earlier timeouts cannot satisfy this phase.
	clientsBefore, err := readPodMetric(nem, pod, "chronicle_sse_clients", "")
	if err != nil {
		return 0, 0, fmt.Errorf("read active clients from stuck-reader pod %s: %w", pod.name, err)
	}
	writeTimeoutBefore, err := readPodMetric(
		nem,
		pod,
		"chronicle_sse_write_timeouts_total",
		"",
	)
	if err != nil {
		return 0, 0, fmt.Errorf("read write timeouts from stuck-reader pod %s: %w", pod.name, err)
	}

	const oversizedEventBytes = 16 << 20
	appendReq, _ := http.NewRequest(
		http.MethodPost,
		endpoint,
		bytes.NewReader(bytes.Repeat([]byte{'z'}, oversizedEventBytes)),
	)
	appendReq.Header.Set("Content-Type", "application/octet-stream")
	appendResp, err := http.DefaultClient.Do(appendReq)
	if err != nil {
		return writeTimeoutBefore, 0, fmt.Errorf("append oversized write-timeout event: %w", err)
	}
	appendResp.Body.Close()
	if appendResp.StatusCode/100 != 2 {
		return writeTimeoutBefore, 0, fmt.Errorf("append oversized write-timeout event status %d", appendResp.StatusCode)
	}

	after, _, err := waitPodTimeoutAndClientDrop(
		nem,
		pod,
		writeTimeoutBefore,
		clientsBefore,
		30*time.Second,
	)
	if err != nil {
		return writeTimeoutBefore, after, fmt.Errorf(
			"stuck reader did not trigger the write-timeout gate on pod %s: %w",
			pod.name,
			err,
		)
	}
	nem.record("write-timeout-verified")

	deleteReq, _ := http.NewRequest(http.MethodDelete, endpoint, nil)
	if deleteResp, deleteErr := http.DefaultClient.Do(deleteReq); deleteErr == nil {
		deleteResp.Body.Close()
	}
	return writeTimeoutBefore, after, nil
}

func snapshotSessions(counts []atomic.Int64) []int64 {
	snapshot := make([]int64, len(counts))
	for i := range counts {
		snapshot[i] = counts[i].Load()
	}
	return snapshot
}

func waitAnySessionAdvance(
	counts []atomic.Int64,
	before []int64,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i := range counts {
			if counts[i].Load() > before[i] {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("no client opened a replacement SSE session")
}

func waitEverySessionAdvance(
	counts []atomic.Int64,
	before []int64,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for i := range counts {
			if counts[i].Load() <= before[i] {
				all = false
				break
			}
		}
		if all {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	var missing []string
	for i := range counts {
		if counts[i].Load() <= before[i] {
			missing = append(missing, strconv.Itoa(i))
		}
	}
	return fmt.Errorf("clients without a replacement SSE session: %s", strings.Join(missing, ","))
}

func waitForSeenMessage(
	seen <-chan sseSeen,
	message int,
	wantClients int,
	timeout time.Duration,
) error {
	clients := make(map[int]struct{}, wantClients)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for len(clients) < wantClients {
		select {
		case observation := <-seen:
			if observation.message == message {
				clients[observation.client] = struct{}{}
			}
		case <-timer.C:
			return fmt.Errorf(
				"message %d reached %d/%d clients",
				message,
				len(clients),
				wantClients,
			)
		}
	}
	return nil
}

type redisSSEState struct {
	meta      map[string]string
	frames    []string
	producers map[string]string
}

func readRedisSSEState(nem *nemesis, stream string) (redisSSEState, error) {
	path := "/" + strings.TrimPrefix(stream, "/")
	tag := "{" + escapeRedisPath(path) + "}"
	metaOut, err := nem.redisCLI("--raw", "HGETALL", "ds:"+tag+":meta")
	if err != nil {
		return redisSSEState{}, fmt.Errorf("read Redis meta hash: %w: %s", err, metaOut)
	}
	frameOut, err := nem.redisCLI("--raw", "ZRANGE", "ds:"+tag+":msg", "0", "-1")
	if err != nil {
		return redisSSEState{}, fmt.Errorf("read Redis message set: %w: %s", err, frameOut)
	}
	producerOut, err := nem.redisCLI("--raw", "HGETALL", "ds:"+tag+":prod")
	if err != nil {
		return redisSSEState{}, fmt.Errorf("read Redis producer hash: %w: %s", err, producerOut)
	}
	meta, err := parseRedisRawHash(metaOut)
	if err != nil {
		return redisSSEState{}, fmt.Errorf("parse Redis meta hash: %w", err)
	}
	producers, err := parseRedisRawHash(producerOut)
	if err != nil {
		return redisSSEState{}, fmt.Errorf("parse Redis producer hash: %w", err)
	}
	return redisSSEState{
		meta:      meta,
		frames:    splitRedisRawLines(frameOut),
		producers: producers,
	}, nil
}

func splitRedisRawLines(raw []byte) []string {
	text := strings.TrimSuffix(string(raw), "\n")
	if text == "" {
		return nil
	}
	lines := strings.Split(text, "\n")
	for i := range lines {
		lines[i] = strings.TrimSuffix(lines[i], "\r")
	}
	return lines
}

func parseRedisRawHash(raw []byte) (map[string]string, error) {
	lines := splitRedisRawLines(raw)
	if len(lines)%2 != 0 {
		return nil, fmt.Errorf("odd HGETALL line count %d", len(lines))
	}
	fields := make(map[string]string, len(lines)/2)
	for i := 0; i < len(lines); i += 2 {
		if lines[i] == "" {
			return nil, fmt.Errorf("empty HGETALL field at line %d", i+1)
		}
		if _, exists := fields[lines[i]]; exists {
			return nil, fmt.Errorf("duplicate HGETALL field %q", lines[i])
		}
		fields[lines[i]] = lines[i+1]
	}
	return fields, nil
}

func validateRedisSSEState(
	state redisSSEState,
	messageCount int,
	httpOffsets []string,
	httpTail string,
) error {
	if messageCount < 2 {
		return fmt.Errorf("message count must be at least 2, got %d", messageCount)
	}
	if len(httpOffsets) != messageCount {
		return fmt.Errorf("HTTP offsets = %d, want %d", len(httpOffsets), messageCount)
	}
	if len(state.frames) != messageCount {
		return fmt.Errorf("redis frames = %d, want %d", len(state.frames), messageCount)
	}

	current := store.ZeroOffset
	for seq := 0; seq < messageCount; seq++ {
		var payload []byte
		var err error
		if seq == 0 {
			payload = []byte(`{"n":0}`)
		} else {
			payload, err = ssePayload(seq)
			if err != nil {
				return fmt.Errorf("build expected Redis payload %d: %w", seq, err)
			}
		}
		current = current.Add(uint64(len(payload)))
		offset := current.String()
		if httpOffsets[seq] != offset {
			return fmt.Errorf(
				"HTTP append offset %d = %s, independently computed %s",
				seq,
				httpOffsets[seq],
				offset,
			)
		}
		wantFrame := offset + "|" + string(payload)
		if state.frames[seq] != wantFrame {
			return fmt.Errorf(
				"redis frame %d did not exactly match offset %s and its payload",
				seq,
				offset,
			)
		}
	}

	computedTail := current.String()
	if httpTail != computedTail {
		return fmt.Errorf("HTTP close tail %s, independently computed %s", httpTail, computedTail)
	}
	if state.meta["tail"] != computedTail {
		return fmt.Errorf("redis meta tail %q, want %s", state.meta["tail"], computedTail)
	}
	if state.meta["closed"] != "1" {
		return fmt.Errorf("redis meta closed = %q, want 1", state.meta["closed"])
	}

	if len(state.producers) != 1 {
		return fmt.Errorf("redis producer entries = %d, want 1", len(state.producers))
	}
	rawProducer, ok := state.producers["jepsen-sse-resume"]
	if !ok {
		return fmt.Errorf("redis producer state omitted jepsen-sse-resume")
	}
	producerParts := strings.Split(rawProducer, ":")
	if len(producerParts) != 3 {
		return fmt.Errorf("redis producer state %q is malformed", rawProducer)
	}
	if producerParts[0] != "0" {
		return fmt.Errorf("redis producer epoch = %q, want 0", producerParts[0])
	}
	wantSequence := strconv.Itoa(messageCount - 2)
	if producerParts[1] != wantSequence {
		return fmt.Errorf(
			"redis producer sequence = %q, want %s",
			producerParts[1],
			wantSequence,
		)
	}
	updatedAt, err := strconv.ParseInt(producerParts[2], 10, 64)
	if err != nil || updatedAt <= 0 {
		return fmt.Errorf("redis producer update time %q is invalid", producerParts[2])
	}
	return nil
}

func readClosedSSESequence(base, stream string) ([]int, string, bool, error) {
	endpoint := fmt.Sprintf("%s/v1/stream/%s?offset=-1", base, stream)
	resp, err := http.Get(endpoint)
	if err != nil {
		return nil, "", false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, "", false, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	var messages []struct {
		N int `json:"n"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		return nil, "", false, err
	}
	sequence := make([]int, len(messages))
	for i, message := range messages {
		sequence[i] = message.N
	}
	return sequence,
		resp.Header.Get(protocol.HeaderStreamNextOffset),
		resp.Header.Get(protocol.HeaderStreamClosed) == "true",
		nil
}

func closeSSEStream(base, stream string) (string, error) {
	var tail string
	endpoint := fmt.Sprintf("%s/v1/stream/%s", base, stream)
	err := retry(160, 500*time.Millisecond, func() error {
		req, _ := http.NewRequest(http.MethodPost, endpoint, nil)
		req.Header.Set(protocol.HeaderStreamClosed, "true")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("close stream status %d", resp.StatusCode)
		}
		tail = resp.Header.Get(protocol.HeaderStreamNextOffset)
		if tail == "" {
			return errors.New("close response omitted Stream-Next-Offset")
		}
		return nil
	})
	return tail, err
}

func sleepContextChecker(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
