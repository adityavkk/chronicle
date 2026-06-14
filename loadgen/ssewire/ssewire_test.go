package ssewire

import (
	"encoding/json"
	"testing"
)

func feed(t *testing.T, p *Parser, lines []string) []Event {
	t.Helper()
	var events []Event
	for _, l := range lines {
		if ev, ok := p.Line([]byte(l)); ok {
			events = append(events, ev)
		}
	}
	return events
}

// The exact framing from PROTOCOL.md §"SSE mode".
func TestProtocolExample(t *testing.T) {
	var p Parser
	events := feed(t, &p, []string{
		"event: data",
		"data: [",
		`data: {"k":"v"},`,
		`data: {"k":"w"}`,
		"data: ]",
		"",
		"event: control",
		`data: {"streamNextOffset":"123456_789","streamCursor":"abc"}`,
		"",
	})
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	if events[0].Type != "data" {
		t.Errorf("type = %q", events[0].Type)
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(events[0].Data, &arr); err != nil {
		t.Fatalf("data event not a JSON array after join: %v\n%s", err, events[0].Data)
	}
	if len(arr) != 2 {
		t.Errorf("messages = %d, want 2", len(arr))
	}
	var ctl Control
	if err := json.Unmarshal(events[1].Data, &ctl); err != nil {
		t.Fatalf("control: %v", err)
	}
	if ctl.StreamNextOffset != "123456_789" || ctl.StreamCursor != "abc" || ctl.StreamClosed {
		t.Errorf("control = %+v", ctl)
	}
}

func TestNoSpaceAfterColon(t *testing.T) {
	var p Parser
	events := feed(t, &p, []string{"event:control", `data:{"streamClosed":true}`, ""})
	if len(events) != 1 || events[0].Type != "control" {
		t.Fatalf("events = %+v", events)
	}
	var ctl Control
	if err := json.Unmarshal(events[0].Data, &ctl); err != nil || !ctl.StreamClosed {
		t.Errorf("control = %+v, err %v", ctl, err)
	}
}

func TestCommentsAndStraySeparatorsIgnored(t *testing.T) {
	var p Parser
	events := feed(t, &p, []string{"", ": keep-alive", "", "data: x", ""})
	if len(events) != 1 || string(events[0].Data) != "x" {
		t.Fatalf("events = %+v", events)
	}
}

func TestOnlySingleLeadingSpaceStripped(t *testing.T) {
	var p Parser
	events := feed(t, &p, []string{"data:  two spaces", ""})
	if len(events) != 1 || string(events[0].Data) != " two spaces" {
		t.Fatalf("data = %q", events[0].Data)
	}
}
