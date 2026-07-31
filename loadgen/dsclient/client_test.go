package dsclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadCatchupCountsWithoutRetainingBody(t *testing.T) {
	t.Parallel()

	const body = "a complete catch-up response"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("offset"); got != "-1" {
			t.Errorf("offset = %q, want -1", got)
		}
		w.Header().Set(HeaderNextOffset, "42")
		w.Header().Set(HeaderUpToDate, "true")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	client := New(server.URL, "/")
	response, err := client.ReadCatchup(context.Background(), "stream", "-1")
	if err != nil {
		t.Fatalf("ReadCatchup: %v", err)
	}
	if response.Status != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Status, http.StatusOK)
	}
	if response.Body != nil {
		t.Fatalf("body retained %d byte(s)", len(response.Body))
	}
	if response.BodyBytes != int64(len(body)) {
		t.Fatalf("body bytes = %d, want %d", response.BodyBytes, len(body))
	}
	wantDigest := sha256.Sum256([]byte(body))
	if response.BodySHA256 != hex.EncodeToString(wantDigest[:]) {
		t.Fatalf("body SHA-256 = %q, want %x", response.BodySHA256, wantDigest)
	}
	if response.NextOffset != "42" || !response.UpToDate {
		t.Fatalf("protocol headers = offset %q up-to-date %v", response.NextOffset, response.UpToDate)
	}
}

func TestReadCatchupDoesNotTreatFalseUpToDateHeaderAsTrue(t *testing.T) {
	t.Parallel()

	client := &Client{
		hc: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header: http.Header{
					HeaderNextOffset: []string{"42"},
					HeaderUpToDate:   []string{"false"},
				},
				Body: io.NopCloser(strings.NewReader("body")),
			}, nil
		})},
		baseURL: "http://example.test",
		root:    "/",
	}
	response, err := client.ReadCatchup(context.Background(), "stream", "-1")
	if err != nil {
		t.Fatal(err)
	}
	if response.UpToDate {
		t.Fatal("Stream-Up-To-Date:false was accepted as true")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
