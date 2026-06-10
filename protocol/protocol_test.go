package protocol

import (
	"testing"
	"time"
)

func TestParseTTL(t *testing.T) {
	valid := map[string]int64{"0": 0, "1": 1, "3600": 3600, "9": 9}
	for in, want := range valid {
		got, err := ParseTTL(in)
		if err != nil || got != want {
			t.Errorf("ParseTTL(%q) = %d, %v; want %d, nil", in, got, err, want)
		}
	}

	// The conformance suite probes the grammar's strictness directly.
	invalid := []string{"", "+3600", "03600", "3600.0", "3.6e3", "-1", " 1", "1 ", "0x10"}
	for _, in := range invalid {
		if _, err := ParseTTL(in); err == nil {
			t.Errorf("ParseTTL(%q) succeeded, want error", in)
		}
	}
}

func TestParseSubOffset(t *testing.T) {
	if v, err := ParseSubOffset("0"); err != nil || v != 0 {
		t.Errorf("ParseSubOffset(0) = %d, %v", v, err)
	}
	if v, err := ParseSubOffset("42"); err != nil || v != 42 {
		t.Errorf("ParseSubOffset(42) = %d, %v", v, err)
	}
	for _, in := range []string{"", "-1", "007", "+1", "1.5"} {
		if _, err := ParseSubOffset(in); err == nil {
			t.Errorf("ParseSubOffset(%q) succeeded, want error", in)
		}
	}
}

func TestIsValidIntegerString(t *testing.T) {
	for _, in := range []string{"0", "10", "007"} { // leading zeros allowed here, unlike TTL
		if !IsValidIntegerString(in) {
			t.Errorf("IsValidIntegerString(%q) = false, want true", in)
		}
	}
	for _, in := range []string{"", "-1", "+1", "1.0", "1e3", "abc"} {
		if IsValidIntegerString(in) {
			t.Errorf("IsValidIntegerString(%q) = true, want false", in)
		}
	}
}

func TestCursorMonotonicProgression(t *testing.T) {
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	current := GenerateCursor(now)

	// No client cursor: current interval.
	if got := GenerateResponseCursor("", now); got != current {
		t.Errorf("no client cursor: got %s, want %s", got, current)
	}

	// Client cursor behind current time: current interval.
	if got := GenerateResponseCursor("1", now); got != current {
		t.Errorf("stale client cursor: got %s, want %s", got, current)
	}

	// Client cursor at/ahead of current interval: strictly greater.
	for _, echo := range []string{current, "99999999999"} {
		got := GenerateResponseCursor(echo, now)
		if got <= echo && len(got) == len(echo) {
			t.Errorf("echoed cursor %s not advanced: got %s", echo, got)
		}
	}

	// Invalid client cursor: current interval, no error.
	if got := GenerateResponseCursor("not-a-number", now); got != current {
		t.Errorf("invalid client cursor: got %s, want %s", got, current)
	}
}
