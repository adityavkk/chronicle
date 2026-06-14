// Package payload builds and parses benchmark messages. Every message
// embeds the writer's sequence number and a wall-clock send timestamp so
// any reader, anywhere, can compute write-to-receipt delivery latency
// without sharing state with the writer. Pure — timestamps are passed in.
package payload

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Message is the parsed form of a benchmark payload.
type Message struct {
	Seq      uint64 `json:"i"`
	SentNano int64  `json:"t"`
	Pad      string `json:"p"`
}

// jsonOverhead is the size of the JSON envelope around the padding for
// maximal field widths: {"i":18446744073709551615,"t":-9223372036854775808,"p":""}.
const jsonOverhead = len(`{"i":18446744073709551615,"t":-9223372036854775808,"p":""}`)

// MinSize is the smallest message size BuildJSON/BuildBytes can honor.
const MinSize = jsonOverhead

// BuildJSON renders one JSON-mode message of exactly size bytes (assuming
// size >= MinSize; smaller asks yield the unpadded minimum for the values).
func BuildJSON(seq uint64, sentNano int64, size int) []byte {
	head := fmt.Sprintf(`{"i":%d,"t":%d,"p":"`, seq, sentNano)
	padLen := size - len(head) - len(`"}`)
	if padLen < 0 {
		padLen = 0
	}
	buf := make([]byte, 0, len(head)+padLen+2)
	buf = append(buf, head...)
	buf = appendPad(buf, padLen)
	buf = append(buf, '"', '}')
	return buf
}

// BuildJSONBatch renders a JSON array of count messages, each of size
// bytes, with consecutive sequence numbers starting at seq. Durable
// Streams servers flatten one-level arrays into individual messages.
func BuildJSONBatch(seq uint64, sentNano int64, size, count int) []byte {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for k := 0; k < count; k++ {
		if k > 0 {
			buf.WriteByte(',')
		}
		buf.Write(BuildJSON(seq+uint64(k), sentNano, size))
	}
	buf.WriteByte(']')
	return buf.Bytes()
}

// BuildBytes renders one newline-delimited binary-mode frame of exactly
// size bytes (including the trailing newline).
func BuildBytes(seq uint64, sentNano int64, size int) []byte {
	head := fmt.Sprintf(`{"i":%d,"t":%d,"p":"`, seq, sentNano)
	padLen := size - len(head) - len("\"}\n")
	if padLen < 0 {
		padLen = 0
	}
	buf := make([]byte, 0, len(head)+padLen+3)
	buf = append(buf, head...)
	buf = appendPad(buf, padLen)
	buf = append(buf, '"', '}', '\n')
	return buf
}

// BuildBytesBatch concatenates count newline-delimited frames.
func BuildBytesBatch(seq uint64, sentNano int64, size, count int) []byte {
	var buf bytes.Buffer
	for k := 0; k < count; k++ {
		buf.Write(BuildBytes(seq+uint64(k), sentNano, size))
	}
	return buf.Bytes()
}

// Parse decodes a single message payload (either mode; the frame body is
// JSON in both).
func Parse(raw []byte) (Message, bool) {
	var m Message
	if err := json.Unmarshal(bytes.TrimSpace(raw), &m); err != nil {
		return Message{}, false
	}
	return m, true
}

// SplitJSONArray decodes a JSON-mode response body (an array of
// messages) into raw elements.
func SplitJSONArray(body []byte) ([]json.RawMessage, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// SplitBytesFrames splits a binary-mode body into newline-delimited
// frames, dropping a trailing empty fragment.
func SplitBytesFrames(body []byte) [][]byte {
	frames := bytes.Split(body, []byte{'\n'})
	if n := len(frames); n > 0 && len(frames[n-1]) == 0 {
		frames = frames[:n-1]
	}
	return frames
}

const padChunk = "xy"

func appendPad(buf []byte, n int) []byte {
	for n >= len(padChunk) {
		buf = append(buf, padChunk...)
		n -= len(padChunk)
	}
	return append(buf, padChunk[:n]...)
}
