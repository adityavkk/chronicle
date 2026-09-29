// Package responsewriter wraps an http.ResponseWriter so middleware can see
// what the handler below it wrote, the status it committed and the bytes it
// sent, without breaking Chronicle's streaming contract: the wrapper
// implements http.Flusher (several SSE paths assert it directly), preserves
// the connection-level flush error for http.ResponseController (the SSE write
// deadline is reported through it), and unwraps so the controller reaches the
// connection's deadlines.
package responsewriter

import "net/http"

// Tracked is an http.ResponseWriter that records what the handler wrote.
type Tracked struct {
	http.ResponseWriter
	status  int
	written int64
}

// Track wraps w.
func Track(w http.ResponseWriter) *Tracked { return &Tracked{ResponseWriter: w} }

// Status is the status the handler committed, or http.StatusOK when it
// returned without committing one, which is what net/http then sends.
func (w *Tracked) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

// Written is the number of body bytes the handler wrote.
func (w *Tracked) Written() int64 { return w.written }

// WriteHeader commits status once; later calls are ignored, as net/http
// ignores them.
func (w *Tracked) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *Tracked) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(body)
	w.written += int64(n)
	return n, err
}

// Flush satisfies http.Flusher for the paths that assert it.
func (w *Tracked) Flush() { _ = w.FlushError() }

// FlushError commits a 200 if nothing was written yet, then flushes the
// writer below and returns its connection-level error, which
// http.ResponseController prefers over Flush.
func (w *Tracked) FlushError() error {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

// Unwrap lets http.ResponseController reach the writer below.
func (w *Tracked) Unwrap() http.ResponseWriter { return w.ResponseWriter }
