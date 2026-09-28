package chronicle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

type cancellationReadStore struct {
	store.Store
	cancel context.CancelFunc
	err    error
}

func (s cancellationReadStore) ReadPage(context.Context, string, store.Offset, store.ReadPageOptions) (store.ReadPage, error) {
	if s.cancel != nil {
		s.cancel()
	}
	return store.ReadPage{}, s.err
}

func TestRequestCancellationLogging(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cancel      bool
		err         error
		wantFailure bool
	}{
		{"client cancellation", true, context.Canceled, false},
		{"wrapped client cancellation", true, fmt.Errorf("read: %w", context.Canceled), false},
		{"backend cancellation with live request", false, context.Canceled, true},
		{"backend timeout", false, context.DeadlineExceeded, true},
		{"backend failure after client cancellation", true, errors.New("backend unavailable"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var logs bytes.Buffer
			h := testHandler(0, 0)
			backend := cancellationReadStore{Store: h.Store, err: tc.err}
			if tc.cancel {
				backend.cancel = cancel
			}
			h.Store = backend
			h.Logger = slog.New(slog.NewTextHandler(&logs, nil))
			request := httptest.NewRequest(http.MethodHead, "/events/test", nil).WithContext(ctx)
			response := httptest.NewRecorder()
			h.ServeHTTP(response, request)
			if tc.wantFailure {
				if response.Code != http.StatusInternalServerError || !strings.Contains(logs.String(), "level=ERROR") {
					t.Fatalf("backend failure hidden: status=%d logs=%s", response.Code, logs.String())
				}
			} else if response.Flushed || response.Body.Len() != 0 || response.Code != http.StatusOK || strings.Contains(logs.String(), "level=ERROR") {
				t.Fatalf("cancelled request produced a failure response/log: status=%d body=%q logs=%s", response.Code, response.Body.String(), logs.String())
			}
		})
	}
}
