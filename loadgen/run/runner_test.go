package run

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/dsclient"
)

func TestMeasurementIncludesUsesIntendedScheduleBoundaries(t *testing.T) {
	start := time.Unix(100, 0)
	r := &runner{
		measureStart: start,
		measureEnd:   start.Add(30 * time.Second),
	}
	tests := []struct {
		name     string
		intended time.Time
		want     bool
	}{
		{name: "warmup-before-start", intended: start.Add(-time.Nanosecond), want: false},
		{name: "inclusive-start", intended: start, want: true},
		{name: "inside", intended: start.Add(29 * time.Second), want: true},
		{name: "exclusive-end", intended: start.Add(30 * time.Second), want: false},
		{name: "after-end", intended: start.Add(31 * time.Second), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := r.measurementIncludes(tc.intended); got != tc.want {
				t.Fatalf("measurementIncludes(%s) = %v, want %v", tc.intended, got, tc.want)
			}
		})
	}
}

func TestValidateCatchupResponseRequiresSnapshotTailHeaders(t *testing.T) {
	tests := []struct {
		name string
		resp dsclient.Response
	}{
		{name: "missing next offset", resp: dsclient.Response{UpToDate: true, BodySHA256: "digest"}},
		{name: "missing up to date", resp: dsclient.Response{NextOffset: "42", BodySHA256: "digest"}},
		{name: "missing completed body digest", resp: dsclient.Response{NextOffset: "42", UpToDate: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateCatchupResponse(tc.resp, ""); err == nil {
				t.Fatal("invalid catch-up response accepted")
			}
		})
	}
	if err := validateCatchupResponse(dsclient.Response{NextOffset: "42", UpToDate: true, BodySHA256: "digest"}, ""); err != nil {
		t.Fatalf("complete catch-up response rejected: %v", err)
	}
	if err := validateCatchupResponse(
		dsclient.Response{NextOffset: "42", UpToDate: true, BodySHA256: "bad"},
		"expected",
	); err == nil {
		t.Fatal("mismatched catch-up digest accepted")
	}
}

func TestCatchupDigestMatchesExactBinaryAndJSONResponses(t *testing.T) {
	for _, test := range []struct {
		name     string
		jsonMode bool
		batches  [][]byte
		response []byte
	}{
		{name: "binary", batches: [][]byte{[]byte("ab"), []byte("cd")}, response: []byte("abcd")},
		{name: "json", jsonMode: true, batches: [][]byte{[]byte(`[1,2]`), []byte(`[3]`)}, response: []byte(`[1,2,3]`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			digest := newCatchupDigest(test.jsonMode)
			for _, batch := range test.batches {
				if err := digest.addBatch(batch); err != nil {
					t.Fatal(err)
				}
			}
			want := sha256.Sum256(test.response)
			if got := digest.finish(); got != hex.EncodeToString(want[:]) {
				t.Fatalf("digest = %s, want %x", got, want)
			}
		})
	}
}
