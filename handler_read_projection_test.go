package chronicle

import (
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

type getCountingMemoryStore struct {
	*store.MemoryStore
	gets atomic.Int64
}

func (s *getCountingMemoryStore) Get(path string) (*store.StreamMetadata, error) {
	s.gets.Add(1)
	return s.MemoryStore.Get(path)
}

func TestHTTPReadWorkDoesNotLoadFullMetadata(t *testing.T) {
	for _, tc := range []struct {
		name       string
		target     string
		initial    []byte
		longPoll   time.Duration
		reconnect  time.Duration
		wantStatus int
	}{
		{
			name:       "plain read",
			target:     "/stream?offset=-1",
			initial:    []byte("x"),
			wantStatus: http.StatusOK,
		},
		{
			name:       "long-poll timeout",
			target:     "/stream?offset=0000000000000000_0000000000000001&live=long-poll",
			initial:    []byte("x"),
			longPoll:   5 * time.Millisecond,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "SSE attach and refresh",
			target:     "/stream?offset=-1&live=sse",
			reconnect:  5 * time.Millisecond,
			wantStatus: http.StatusOK,
		},
		{
			name:       "HEAD projection",
			target:     "/stream",
			wantStatus: http.StatusOK,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subject := &getCountingMemoryStore{MemoryStore: store.NewMemoryStore()}
			handler := &Handler{
				Store:                subject,
				LongPollTimeout:      tc.longPoll,
				SSEReconnectInterval: tc.reconnect,
				Logger:               slog.Default(),
			}
			if handler.LongPollTimeout == 0 {
				handler.LongPollTimeout = time.Second
			}
			if handler.SSEReconnectInterval == 0 {
				handler.SSEReconnectInterval = time.Second
			}
			mustCreate(t, handler, "/stream", "text/plain", tc.initial)
			subject.gets.Store(0)

			method := http.MethodGet
			if tc.name == "HEAD projection" {
				method = http.MethodHead
			}
			response := do(handler, method, tc.target, nil, nil)
			if response.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%q", response.Code, tc.wantStatus, response.Body.String())
			}
			if got := subject.gets.Load(); got != 0 {
				t.Fatalf("full metadata Get calls = %d, want 0", got)
			}
		})
	}
}
