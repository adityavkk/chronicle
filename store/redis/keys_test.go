package redis

import (
	"bytes"
	"sort"
	"strings"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func off(b uint64) store.Offset { return store.Offset{ReadSeq: 0, ByteOffset: b} }

func TestKeySchema(t *testing.T) {
	if got := metaKey("/v1/s"); got != "ds:{/v1/s}:meta" {
		t.Errorf("metaKey = %q", got)
	}
	if got := msgKey("/v1/s"); got != "ds:{/v1/s}:msg" {
		t.Errorf("msgKey = %q", got)
	}
	if got := prodKey("/v1/s"); got != "ds:{/v1/s}:prod" {
		t.Errorf("prodKey = %q", got)
	}
	if got := forksKey("/v1/s"); got != "ds:{/v1/s}:forks" {
		t.Errorf("forksKey = %q", got)
	}
	if got := notifyChannel("/v1/s"); got != "ds:notify:{/v1/s}" {
		t.Errorf("notifyChannel = %q", got)
	}
}

func TestEscapePathHashTagSafety(t *testing.T) {
	// Braces in user paths must not terminate the hash tag.
	k := metaKey("/a{b}c")
	if strings.Count(k, "{") != 1 || strings.Count(k, "}") != 1 {
		t.Errorf("hash tag broken: %q", k)
	}
	// Escaping must be injective.
	if metaKey("/a%7Bb") == metaKey("/a{b") {
		t.Error("escapePath is not injective")
	}
}

func TestEncodeDecodeFrameRoundTrip(t *testing.T) {
	cases := [][]byte{
		[]byte("hello"),
		{},
		{0x00},
		{0xff},
		{0x00, 0xff, '|', 0x00, '_', 0xff},
		bytes.Repeat([]byte{0xff, 0x00}, 1000),
	}
	for _, data := range cases {
		o := off(12345)
		m := encodeFrame(o, data)
		gotOff, gotData, err := decodeFrame(m)
		if err != nil {
			t.Fatalf("decodeFrame(%q): %v", m, err)
		}
		if !gotOff.Equal(o) {
			t.Errorf("offset round-trip: got %v want %v", gotOff, o)
		}
		if !bytes.Equal(gotData, data) {
			t.Errorf("data round-trip mismatch for %q", data)
		}
	}
}

func TestDecodeFrameMalformed(t *testing.T) {
	for _, m := range []string{"", "short", strings.Repeat("0", 33), strings.Repeat("0", 16) + "_" + strings.Repeat("0", 16) + "X"} {
		if _, _, err := decodeFrame(m); err == nil {
			t.Errorf("decodeFrame(%q): want error", m)
		}
	}
	// 33 chars of correct shape + separator is the minimum valid member (empty payload).
	valid := off(0).String() + "|"
	if _, _, err := decodeFrame(valid); err != nil {
		t.Errorf("decodeFrame(%q): %v", valid, err)
	}
}

// TestLexLowerBound verifies the exclusive lower bound against Redis's lex
// comparison rule (plain bytewise memcmp, shorter string smaller on prefix
// match) for payloads containing 0x00 and 0xff.
func TestLexLowerBound(t *testing.T) {
	bound := lexLowerBound(off(10))
	wantExcluded := []string{
		encodeFrame(off(10), []byte{}),               // same offset, empty payload
		encodeFrame(off(10), []byte{0x00}),           // same offset, low byte
		encodeFrame(off(10), []byte{0xff, 0xff}),     // same offset, high bytes
		encodeFrame(off(10), []byte("zzzz")),         // same offset, text
		encodeFrame(off(9), []byte{0xff}),            // lower offset
		encodeFrame(off(0), bytes.Repeat([]byte{0xff}, 40)), // much lower offset, 0xff payload
	}
	wantIncluded := []string{
		encodeFrame(off(11), []byte{}),     // next offset, empty payload
		encodeFrame(off(11), []byte{0x00}), // next offset, low byte
		encodeFrame(off(9999999), []byte("x")),
		encodeFrame(store.Offset{ReadSeq: 1, ByteOffset: 0}, []byte("x")), // higher readSeq
	}
	boundStr := bound[1:] // strip the '(' marker; exclusive: member must be > boundStr
	for _, m := range wantExcluded {
		if m > boundStr {
			t.Errorf("member %q should be EXCLUDED by bound %q", m, boundStr)
		}
	}
	for _, m := range wantIncluded {
		if !(m > boundStr) {
			t.Errorf("member %q should be INCLUDED by bound %q", m, boundStr)
		}
	}
}

func TestLexUpperBoundInclusive(t *testing.T) {
	bound := lexUpperBoundInclusive(off(10))
	boundStr := bound[1:] // strip '[' marker; inclusive: member must be <= boundStr
	wantIncluded := []string{
		encodeFrame(off(10), []byte{}),
		encodeFrame(off(10), bytes.Repeat([]byte{0xff}, 8)),
		encodeFrame(off(9), []byte("x")),
	}
	wantExcluded := []string{
		encodeFrame(off(11), []byte{}),
		encodeFrame(off(11), []byte{0x00}),
	}
	for _, m := range wantIncluded {
		if !(m <= boundStr) {
			t.Errorf("member %q should be INCLUDED by upper bound %q", m, boundStr)
		}
	}
	for _, m := range wantExcluded {
		if m <= boundStr {
			t.Errorf("member %q should be EXCLUDED by upper bound %q", m, boundStr)
		}
	}
}

// TestFrameLexOrderEqualsStreamOrder pins the core model invariant: bytewise
// lex order of encoded members equals offset order regardless of payload.
func TestFrameLexOrderEqualsStreamOrder(t *testing.T) {
	frames := []string{
		encodeFrame(off(5), bytes.Repeat([]byte{0xff}, 10)),
		encodeFrame(off(100), []byte{0x00}),
		encodeFrame(off(7), []byte("|||")),
		encodeFrame(off(50), []byte{}),
	}
	sorted := append([]string(nil), frames...)
	sort.Strings(sorted)
	wantOrder := []uint64{5, 7, 50, 100}
	for i, m := range sorted {
		o, _, err := decodeFrame(m)
		if err != nil {
			t.Fatal(err)
		}
		if o.ByteOffset != wantOrder[i] {
			t.Errorf("position %d: got offset %d want %d", i, o.ByteOffset, wantOrder[i])
		}
	}
}
