package main

import (
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func TestRequireCommittedSSEAbort(t *testing.T) {
	if err := requireCommittedSSEAbort(io.ErrUnexpectedEOF); err != nil {
		t.Fatal(err)
	}
	if err := requireCommittedSSEAbort(errors.Join(errors.New("read body"), io.ErrUnexpectedEOF)); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{nil, errors.New("ordinary read failure")} {
		if got := requireCommittedSSEAbort(err); got == nil {
			t.Fatalf("termination %v accepted without a committed-response abort", err)
		}
	}
}

func TestSSEParserHandlesMultilineDataCommentsAndCRLF(t *testing.T) {
	var parser sseParser
	lines := [][]byte{
		[]byte(": keep-alive"),
		[]byte("event: data\r"),
		[]byte("data: [\r"),
		[]byte(`data: {"n":0}` + "\r"),
		[]byte("data: ]\r"),
		[]byte("\r"),
	}
	var events []sseEvent
	for _, line := range lines {
		normalized := strings.TrimSuffix(string(line), "\r")
		if event, complete := parser.line([]byte(normalized)); complete {
			events = append(events, event)
		}
	}
	if len(events) != 1 || events[0].eventType != "data" {
		t.Fatalf("events = %#v", events)
	}
	var messages []struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal(events[0].data, &messages); err != nil {
		t.Fatalf("decode event: %v\n%s", err, events[0].data)
	}
	if len(messages) != 1 || messages[0].N != 0 {
		t.Fatalf("messages = %#v", messages)
	}
}

func TestCheckSSEObservationAcceptsExactSequence(t *testing.T) {
	observation, offsets := validSSEObservation()
	if err := checkSSEObservation(observation, offsets, offsets[3]); err != nil {
		t.Fatal(err)
	}
}

func TestCheckSSEObservationRejectsDuplicate(t *testing.T) {
	observation, offsets := validSSEObservation()
	observation.messages = []int{0, 1, 1, 2, 3}
	if err := checkSSEObservation(observation, offsets, offsets[3]); err == nil ||
		!strings.Contains(err.Error(), "got 1, want 2") {
		t.Fatalf("duplicate check = %v", err)
	}
}

func TestCheckSSEObservationRejectsGap(t *testing.T) {
	observation, offsets := validSSEObservation()
	observation.messages = []int{0, 1, 3}
	if err := checkSSEObservation(observation, offsets, offsets[3]); err == nil ||
		!strings.Contains(err.Error(), "got 3, want 2") {
		t.Fatalf("gap check = %v", err)
	}
}

func TestCheckSSEObservationRejectsMessageGapBeforeReplay(t *testing.T) {
	observation, offsets := validSSEObservation()
	observation.messages = []int{0, 2, 1, 3}
	if err := checkSSEObservation(observation, offsets, offsets[3]); err == nil ||
		!strings.Contains(err.Error(), "got 2, want 1") {
		t.Fatalf("order check = %v", err)
	}
}

func TestCheckSSEObservationRejectsControlRegression(t *testing.T) {
	observation, offsets := validSSEObservation()
	observation.controls = []string{
		store.Offset{ByteOffset: 3}.String(),
		store.Offset{ByteOffset: 2}.String(),
		offsets[3],
	}
	if err := checkSSEObservation(observation, offsets, offsets[3]); err == nil ||
		!strings.Contains(err.Error(), "regressed") {
		t.Fatalf("control check = %v", err)
	}
}

func TestCheckSSEObservationRejectsControlBeyondDeliveredData(t *testing.T) {
	observation, offsets := validSSEObservation()
	observation.frames[0].control = offsets[2]
	if err := checkSSEObservation(observation, offsets, offsets[3]); err == nil ||
		!strings.Contains(err.Error(), "checkpointed") {
		t.Fatalf("control/data check = %v", err)
	}
}

func TestCheckSSEObservationAcceptsRawReplayAcrossSessionBoundary(t *testing.T) {
	observation, offsets := validSSEObservation()
	observation.sessionStarts = []string{"-1", offsets[1]}
	observation.raw = []sseRawMessage{
		{session: 1, message: 0},
		{session: 1, message: 1},
		{session: 1, message: 2}, // delivered without a following control
		{session: 2, message: 2}, // legal replay from the last control
		{session: 2, message: 3},
	}
	observation.frames = []sseObservedFrame{
		{session: 1, messages: []int{0, 1}, control: offsets[1]},
		{session: 2, messages: []int{2, 3}, control: offsets[3], closed: true},
	}
	if err := checkSSEObservation(observation, offsets, offsets[3]); err != nil {
		t.Fatal(err)
	}
}

func TestCheckSSEObservationRejectsRawReplayBeforeResumeOffset(t *testing.T) {
	observation, offsets := validSSEObservation()
	observation.sessionStarts = []string{"-1", offsets[1]}
	observation.raw = append(observation.raw, sseRawMessage{session: 2, message: 0})
	if err := checkSSEObservation(observation, offsets, offsets[3]); err == nil ||
		!strings.Contains(err.Error(), "resume offset") {
		t.Fatalf("raw replay check = %v", err)
	}
}

func TestCheckSSEObservationRejectsRawMessageBeyondDurableSequence(t *testing.T) {
	observation, offsets := validSSEObservation()
	observation.raw = append(observation.raw, sseRawMessage{session: 1, message: len(offsets)})
	if err := checkSSEObservation(observation, offsets, offsets[3]); err == nil ||
		!strings.Contains(err.Error(), "unexpected sequence") {
		t.Fatalf("phantom raw message check = %v", err)
	}
}

func TestRedisNotifyChannelIncludesMountedLeadingSlash(t *testing.T) {
	if got, want := redisNotifyChannel("/events/sse-{123}%"), "ds:notify:{/events/sse-%7B123%7D%25}"; got != want {
		t.Fatalf("notify channel = %q, want %q", got, want)
	}
}

func TestParseRedisInteger(t *testing.T) {
	if got, err := parseRedisInteger([]byte("2\n")); err != nil || got != 2 {
		t.Fatalf("parse Redis integer = %d, %v", got, err)
	}
	if _, err := parseRedisInteger([]byte("not-an-integer")); err == nil {
		t.Fatal("invalid Redis integer was accepted")
	}
}

func TestParseRedisRawHashFailsClosed(t *testing.T) {
	fields, err := parseRedisRawHash([]byte("tail\n0000_0007\nclosed\n1\n"))
	if err != nil {
		t.Fatal(err)
	}
	if fields["tail"] != "0000_0007" || fields["closed"] != "1" {
		t.Fatalf("fields = %#v", fields)
	}
	if _, err := parseRedisRawHash([]byte("tail\n0000_0007\nclosed\n")); err == nil {
		t.Fatal("odd HGETALL output was accepted")
	}
	if _, err := parseRedisRawHash([]byte("tail\none\ntail\ntwo\n")); err == nil {
		t.Fatal("duplicate HGETALL field was accepted")
	}
}

func TestValidateRedisSSEStateAcceptsExactState(t *testing.T) {
	state, offsets, tail := validRedisSSEState(t, 3)
	if err := validateRedisSSEState(state, 3, offsets, tail); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRedisSSEStateFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*redisSSEState, []string, *string)
		match  string
	}{
		{
			name: "frame payload",
			mutate: func(state *redisSSEState, _ []string, _ *string) {
				state.frames[1] += "corrupt"
			},
			match: "frame 1",
		},
		{
			name: "frame offset",
			mutate: func(state *redisSSEState, _ []string, _ *string) {
				_, payload, _ := strings.Cut(state.frames[1], "|")
				state.frames[1] = store.Offset{ByteOffset: 999}.String() + "|" + payload
			},
			match: "frame 1",
		},
		{
			name: "http offset",
			mutate: func(_ *redisSSEState, offsets []string, _ *string) {
				offsets[1] = store.Offset{ByteOffset: 999}.String()
			},
			match: "HTTP append offset 1",
		},
		{
			name: "meta tail",
			mutate: func(state *redisSSEState, _ []string, _ *string) {
				state.meta["tail"] = store.ZeroOffset.String()
			},
			match: "meta tail",
		},
		{
			name: "closed flag",
			mutate: func(state *redisSSEState, _ []string, _ *string) {
				delete(state.meta, "closed")
			},
			match: "closed",
		},
		{
			name: "producer epoch",
			mutate: func(state *redisSSEState, _ []string, _ *string) {
				state.producers["jepsen-sse-resume"] = "1:1:123"
			},
			match: "epoch",
		},
		{
			name: "producer sequence",
			mutate: func(state *redisSSEState, _ []string, _ *string) {
				state.producers["jepsen-sse-resume"] = "0:99:123"
			},
			match: "sequence",
		},
		{
			name: "extra producer",
			mutate: func(state *redisSSEState, _ []string, _ *string) {
				state.producers["unexpected"] = "0:0:123"
			},
			match: "producer entries",
		},
		{
			name: "close tail",
			mutate: func(_ *redisSSEState, _ []string, tail *string) {
				*tail = store.ZeroOffset.String()
			},
			match: "HTTP close tail",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, offsets, tail := validRedisSSEState(t, 3)
			tc.mutate(&state, offsets, &tail)
			err := validateRedisSSEState(state, 3, offsets, tail)
			if err == nil || !strings.Contains(err.Error(), tc.match) {
				t.Fatalf("validation error = %v, want %q", err, tc.match)
			}
		})
	}
}

func validRedisSSEState(
	t *testing.T,
	messageCount int,
) (redisSSEState, []string, string) {
	t.Helper()
	frames := make([]string, messageCount)
	offsets := make([]string, messageCount)
	current := store.ZeroOffset
	for seq := range messageCount {
		var payload []byte
		var err error
		if seq == 0 {
			payload = []byte(`{"n":0}`)
		} else {
			payload, err = ssePayload(seq)
			if err != nil {
				t.Fatal(err)
			}
		}
		current = current.Add(uint64(len(payload)))
		offsets[seq] = current.String()
		frames[seq] = offsets[seq] + "|" + string(payload)
	}
	state := redisSSEState{
		meta: map[string]string{
			"tail":   current.String(),
			"closed": "1",
		},
		frames: frames,
		producers: map[string]string{
			"jepsen-sse-resume": "0:" + strconv.Itoa(messageCount-2) + ":123",
		},
	}
	return state, offsets, current.String()
}

func validSSEObservation() (sseObservation, []string) {
	offsets := []string{
		store.Offset{ByteOffset: 1}.String(),
		store.Offset{ByteOffset: 2}.String(),
		store.Offset{ByteOffset: 3}.String(),
		store.Offset{ByteOffset: 4}.String(),
	}
	return sseObservation{
		messages: []int{0, 1, 2, 3},
		controls: []string{offsets[1], offsets[3]},
		frames: []sseObservedFrame{
			{session: 1, messages: []int{0, 1}, control: offsets[1]},
			{session: 1, messages: []int{2, 3}, control: offsets[3], closed: true},
		},
		raw: []sseRawMessage{
			{session: 1, message: 0},
			{session: 1, message: 1},
			{session: 1, message: 2},
			{session: 1, message: 3},
		},
		sessionStarts: []string{"-1"},
		closed:        true,
	}, offsets
}
