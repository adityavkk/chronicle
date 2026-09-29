package responsewriter

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTrackedRecordsWhatTheHandlerWrote(t *testing.T) {
	cases := []struct {
		name    string
		handle  func(w http.ResponseWriter)
		status  int
		written int64
	}{
		{"nothing written", func(http.ResponseWriter) {}, http.StatusOK, 0},
		{"body without a header", func(w http.ResponseWriter) { _, _ = w.Write([]byte("hello")) }, http.StatusOK, 5},
		{"explicit status", func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("ok"))
		}, http.StatusAccepted, 2},
		{"a second header is ignored", func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusNotFound)
			w.WriteHeader(http.StatusInternalServerError)
		}, http.StatusNotFound, 0},
		{"a flush commits 200", func(w http.ResponseWriter) { w.(http.Flusher).Flush() }, http.StatusOK, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			w := Track(rec)
			tc.handle(w)
			if w.Status() != tc.status || w.Written() != tc.written || rec.Code != tc.status {
				t.Fatalf("status %d written %d recorder %d, want %d %d %d", w.Status(), w.Written(), rec.Code, tc.status, tc.written, tc.status)
			}
		})
	}
}

type flushFailingRecorder struct {
	*httptest.ResponseRecorder
	err error
}

func (r *flushFailingRecorder) FlushError() error { return r.err }

func TestTrackedPreservesTheConnectionFlushError(t *testing.T) {
	deadline := errors.New("write deadline exceeded")
	w := Track(&flushFailingRecorder{httptest.NewRecorder(), deadline})
	if err := http.NewResponseController(w).Flush(); !errors.Is(err, deadline) {
		t.Fatalf("controller flush = %v, want %v", err, deadline)
	}
	rec := httptest.NewRecorder()
	ok := Track(rec)
	if err := http.NewResponseController(ok).Flush(); err != nil || !rec.Flushed || rec.Code != http.StatusOK {
		t.Fatalf("controller flush on a healthy writer = %v, flushed %v, code %d", err, rec.Flushed, rec.Code)
	}
}
