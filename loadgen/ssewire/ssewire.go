// Package ssewire is an incremental Server-Sent Events parser: a pure
// line-at-a-time state machine. The imperative shell feeds it lines from
// the network; it emits completed events. Only the fields the Durable
// Streams protocol uses (event, data) are interpreted.
package ssewire

import "bytes"

// Event is one complete SSE event.
type Event struct {
	Type string // "data", "control", … ("" defaults to "message" per spec)
	Data []byte // data lines joined with \n, per the SSE spec
}

// Control is the decoded payload of a Durable Streams `control` event.
type Control struct {
	StreamNextOffset string `json:"streamNextOffset"`
	StreamCursor     string `json:"streamCursor"`
	StreamClosed     bool   `json:"streamClosed"`
	UpToDate         bool   `json:"upToDate"`
}

// Parser accumulates lines into events. The zero value is ready to use.
type Parser struct {
	eventType string
	data      [][]byte
}

// Line consumes one line (without its terminator) and returns a
// completed event when the line is the blank separator.
func (p *Parser) Line(line []byte) (Event, bool) {
	if len(line) == 0 {
		if p.eventType == "" && len(p.data) == 0 {
			return Event{}, false // stray separator / keep-alive
		}
		ev := Event{Type: p.eventType, Data: bytes.Join(p.data, []byte{'\n'})}
		p.eventType = ""
		p.data = nil
		return ev, true
	}
	if line[0] == ':' {
		return Event{}, false // comment / keep-alive
	}
	field, value, _ := bytes.Cut(line, []byte{':'})
	// Per SSE spec, a single leading space in the value is stripped.
	value = bytes.TrimPrefix(value, []byte{' '})
	switch string(field) {
	case "event":
		p.eventType = string(value)
	case "data":
		p.data = append(p.data, append([]byte(nil), value...))
	}
	return Event{}, false
}
