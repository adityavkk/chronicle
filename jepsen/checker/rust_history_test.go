package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func rustTestState(data string) rustStream {
	return rustStream{exists: true, incarnation: 1, data: []byte(data), producers: map[string]rustProducerState{}}
}

func rustTestInput(kind, data string) rustInput {
	return rustInput{tenant: "t", path: "p", kind: kind, data: []byte(data), incarnation: 1, config: rustConfig{ContentType: "x"}}
}
func rustTestOutput(end, inc uint64) rustOutput { return rustOutput{end: end, incarnation: inc} }

func TestRustModelProducerRules(t *testing.T) {
	s := rustTestState("")
	first := rustTestInput("append", "a")
	first.producer = &rustProducer{"p", 2, 0}
	n := rustStep(s, first, rustTestOutput(1, 1))
	if len(n) != 1 {
		t.Fatal("epoch seq zero rejected")
	}
	s = n[0].(rustStream)
	retry := first
	retry.data = []byte("different")
	out := rustTestOutput(1, 1)
	out.duplicate = true
	if len(rustStep(s, retry, out)) != 1 {
		t.Fatal("retry did not return original frontier")
	}
	gap := rustTestInput("append", "g")
	gap.producer = &rustProducer{"p", 2, 2}
	if len(rustStep(s, gap, rustOutput{error: "SequenceGap"})) != 1 {
		t.Fatal("gap rejection illegal")
	}
	pred := rustTestInput("append", "b")
	pred.producer = &rustProducer{"p", 2, 1}
	if len(rustStep(s, pred, rustTestOutput(2, 1))) != 1 {
		t.Fatal("gap consumed tuple")
	}
}

func TestRustModelLifecycleAndExactRead(t *testing.T) {
	s := rustTestState("a")
	n := rustStep(s, rustTestInput("delete", ""), rustTestOutput(0, 1))
	if len(n) != 1 {
		t.Fatal("delete")
	}
	s = n[0].(rustStream)
	two := uint64(2)
	create := rustTestInput("create", "b")
	create.expected = &two
	n = rustStep(s, create, rustTestOutput(1, 2))
	if len(n) != 1 {
		t.Fatal("recreate")
	}
	s = n[0].(rustStream)
	stale := rustTestInput("append", "x")
	stale.incarnation = 1
	if len(rustStep(s, stale, rustOutput{error: "StaleIncarnation"})) != 1 {
		t.Fatal("stale retry")
	}
	if len(rustStep(s, rustTestInput("read", ""), rustOutput{end: 1, incarnation: 2, data: []byte("x")})) != 0 {
		t.Fatal("corrupt same-length read accepted")
	}
}

func TestRustUnknownMayCompleteAfterInterveningRead(t *testing.T) {
	h := `{"schema":2,"type":"invoke","f":"create","process":"s","id":"c","time_ns":1,"value":{"tenant":"t","path":"p","content_type":"x","expected_incarnation":1}}
{"schema":2,"type":"ok","f":"create","process":"s","id":"c","time_ns":2,"value":{"end":0,"incarnation":1}}
{"schema":2,"type":"invoke","f":"append","process":"w","id":"a","time_ns":3,"value":{"tenant":"t","path":"p","data":"a","incarnation":1,"producer":"p"}}
{"schema":2,"type":"unknown","f":"append","process":"w","id":"a","time_ns":4,"value":{}}
{"schema":2,"type":"invoke","f":"read","process":"r","id":"r1","time_ns":5,"value":{"tenant":"t","path":"p"}}
{"schema":2,"type":"ok","f":"read","process":"r","id":"r1","time_ns":6,"value":{"end":0,"incarnation":1}}
{"schema":2,"type":"invoke","f":"read","process":"r","id":"r2","time_ns":7,"value":{"tenant":"t","path":"p"}}
{"schema":2,"type":"ok","f":"read","process":"r","id":"r2","time_ns":8,"value":{"data":"a","end":1,"incarnation":1}}
`
	r, err := checkRustHistory(strings.NewReader(h), time.Second)
	if err != nil || r != porcupine.Ok {
		t.Fatalf("result=%v err=%v", r, err)
	}
}

func TestRustHistoryFailsClosed(t *testing.T) {
	if _, err := parseRustHistory(strings.NewReader("not-json\n")); err == nil {
		t.Fatal("malformed accepted")
	}
	bad := `{"schema":2,"type":"invoke","f":"create","process":"s","id":"c","time_ns":1,"value":{"tenant":"t","path":"p","bogus":1}}` + "\n"
	if _, err := parseRustHistory(strings.NewReader(bad)); err == nil {
		t.Fatal("unknown v2 field accepted")
	}
	valid := "{\"schema\":2,\"type\":\"invoke\",\"f\":\"create\",\"process\":\"s\",\"id\":\"c\",\"time_ns\":1,\"value\":{\"tenant\":\"t\",\"path\":\"p\",\"content_type\":\"x\",\"expected_incarnation\":1}}\n" +
		"{\"schema\":2,\"type\":\"ok\",\"f\":\"create\",\"process\":\"s\",\"id\":\"c\",\"time_ns\":2,\"value\":{\"end\":0,\"incarnation\":1}}\n"
	result, err := checkRustHistory(strings.NewReader(valid), 0)
	if err != nil {
		t.Fatal(err)
	}
	if result == porcupine.Ok {
		t.Fatal("timeout failed open")
	}
}

func TestRustHistoryRejectsContradictoryV2Completions(t *testing.T) {
	tests := []struct {
		name       string
		kind       string
		completion string
	}{
		{name: "fail with successful frontier", kind: "create", completion: `{"error":"Missing","end":0,"incarnation":1}`},
		{name: "fail without error", kind: "create", completion: `{}`},
		{name: "ok with error", kind: "create", completion: `{"error":"Missing","end":0,"incarnation":1}`},
		{name: "mutation success with read data", kind: "create", completion: `{"data":"x","end":1,"incarnation":1}`},
		{name: "read success with duplicate", kind: "read", completion: `{"end":0,"incarnation":1,"duplicate":true}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			invoke := `{"schema":2,"type":"invoke","f":"` + tc.kind + `","process":"p","id":"1","time_ns":1,"value":{"tenant":"t","path":"p"}}`
			typ := "ok"
			if strings.HasPrefix(tc.name, "fail") {
				typ = "fail"
			}
			completion := `{"schema":2,"type":"` + typ + `","f":"` + tc.kind + `","process":"p","id":"1","time_ns":2,"value":` + tc.completion + `}`
			if _, err := parseRustHistory(strings.NewReader(invoke + "\n" + completion + "\n")); err == nil {
				t.Fatal("contradictory completion accepted")
			}
		})
	}
}

func TestRustHistoryTransportUnknownRemainsPendingToHistoryEnd(t *testing.T) {
	h := `{"schema":2,"type":"invoke","f":"append","process":"p","id":"1","time_ns":1,"value":{"tenant":"t","path":"p","data":"a","incarnation":1}}
{"schema":2,"type":"unknown","f":"append","process":"p","id":"1","time_ns":2,"value":{}}
{"schema":2,"type":"info","f":"nemesis","process":"n","id":"n","time_ns":9,"value":{}}
`
	ops, err := parseRustHistory(strings.NewReader(h))
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 || !ops[0].Output.(rustOutput).unknown || ops[0].Return != 10 {
		t.Fatalf("unknown completion = %#v", ops)
	}
}

func TestRustHistoryAcrossIncarnationsAndPaths(t *testing.T) {
	// A successful append need not appear after delete/recreate, nor in another
	// stream. Also, a retry's ignored payload must not be required in a read.
	var h strings.Builder
	clock := 0
	add := func(kind string, input, output map[string]interface{}) {
		clock++
		id := fmt.Sprint(clock)
		input["tenant"] = "t"
		if _, ok := input["path"]; !ok {
			input["path"] = "p"
		}
		for i, value := range []map[string]interface{}{input, output} {
			typ := "invoke"
			if i == 1 {
				typ = "ok"
			}
			line, err := json.Marshal(map[string]interface{}{"schema": 2, "type": typ, "f": kind, "process": "client", "id": id, "time_ns": clock*2 + i, "value": value})
			if err != nil {
				t.Fatal(err)
			}
			h.Write(line)
			h.WriteByte('\n')
		}
	}
	add("create", map[string]interface{}{"content_type": "x"}, map[string]interface{}{"end": 0, "incarnation": 1})
	add("append", map[string]interface{}{"data": "a", "incarnation": 1, "producer": "p", "seq": 0}, map[string]interface{}{"end": 1, "incarnation": 1})
	add("append", map[string]interface{}{"data": "ignored", "incarnation": 1, "producer": "p", "seq": 0}, map[string]interface{}{"end": 1, "incarnation": 1, "duplicate": true})
	add("read", map[string]interface{}{}, map[string]interface{}{"data": "a", "end": 1, "incarnation": 1})
	add("delete", map[string]interface{}{"incarnation": 1}, map[string]interface{}{"end": 0, "incarnation": 1})
	add("create", map[string]interface{}{"content_type": "x", "expected_incarnation": 2}, map[string]interface{}{"end": 0, "incarnation": 2})
	add("read", map[string]interface{}{}, map[string]interface{}{"end": 0, "incarnation": 2})
	add("create", map[string]interface{}{"path": "other", "content_type": "x"}, map[string]interface{}{"end": 0, "incarnation": 1})
	add("read", map[string]interface{}{"path": "other"}, map[string]interface{}{"end": 0, "incarnation": 1})
	result, err := checkRustHistory(strings.NewReader(h.String()), time.Second)
	if err != nil || result != porcupine.Ok {
		t.Fatalf("result=%v err=%v", result, err)
	}
}
