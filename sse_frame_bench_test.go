package chronicle

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

type benchmarkControl struct {
	StreamClosed     bool   `json:"streamClosed,omitempty"`
	StreamCursor     string `json:"streamCursor,omitempty"`
	StreamNextOffset string `json:"streamNextOffset"`
	UpToDate         bool   `json:"upToDate,omitempty"`
}

type benchmarkFrameWriter struct {
	control  bytes.Buffer
	combined []byte
}

func (f *benchmarkFrameWriter) typedReusable(
	w http.ResponseWriter,
	data []byte,
	next store.Offset,
) error {
	return withSSEWriteDeadline(w, time.Minute, func(controller *http.ResponseController) error {
		if _, err := w.Write(data); err != nil {
			return err
		}
		f.control.Reset()
		f.control.WriteString("event: control\ndata:")
		if err := json.NewEncoder(&f.control).Encode(benchmarkControl{
			StreamCursor:     "benchmark-cursor",
			StreamNextOffset: next.String(),
			UpToDate:         true,
		}); err != nil {
			return err
		}
		f.control.WriteByte('\n')
		if _, err := w.Write(f.control.Bytes()); err != nil {
			return err
		}
		return controller.Flush()
	})
}

func (f *benchmarkFrameWriter) typedCombined(
	w http.ResponseWriter,
	data []byte,
	next store.Offset,
) error {
	return withSSEWriteDeadline(w, time.Minute, func(controller *http.ResponseController) error {
		f.control.Reset()
		f.control.WriteString("event: control\ndata:")
		if err := json.NewEncoder(&f.control).Encode(benchmarkControl{
			StreamCursor:     "benchmark-cursor",
			StreamNextOffset: next.String(),
			UpToDate:         true,
		}); err != nil {
			return err
		}
		f.control.WriteByte('\n')
		f.combined = append(f.combined[:0], data...)
		f.combined = append(f.combined, f.control.Bytes()...)
		if _, err := w.Write(f.combined); err != nil {
			return err
		}
		return controller.Flush()
	})
}

// BenchmarkSSEClientFramePaths measures complete writes through a real
// net/http server. The production path is changed only if a candidate improves
// CPU time or allocated bytes by at least 10 percent without regressing the
// other by more than 5 percent.
func BenchmarkSSEClientFramePaths(b *testing.B) {
	data := bytes.Repeat([]byte("event: data\ndata:0123456789abcdef\n\n"), 4)
	next := store.Offset{ByteOffset: 64}
	tests := []struct {
		name string
		new  func(http.ResponseWriter) func() error
	}{
		{
			name: "current_map_two_writes",
			new: func(w http.ResponseWriter) func() error {
				return func() error {
					return writeLegacySSEUpdate(w, data, next, "benchmark-cursor", true, false, time.Minute)
				}
			},
		},
		{
			name: "typed_reusable_two_writes",
			new: func(w http.ResponseWriter) func() error {
				writer := &benchmarkFrameWriter{}
				return func() error { return writer.typedReusable(w, data, next) }
			},
		},
		{
			name: "typed_reusable_combined_write",
			new: func(w http.ResponseWriter) func() error {
				writer := &benchmarkFrameWriter{}
				return func() error { return writer.typedCombined(w, data, next) }
			},
		},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			benchmarkSSEFramePath(b, test.new)
		})
	}
}

// writeLegacySSEUpdate preserves the pre-issue-13 production implementation so
// the benchmark keeps an honest, immutable baseline after the selected frame
// writer replaces it.
func writeLegacySSEUpdate(
	w http.ResponseWriter,
	data []byte,
	next store.Offset,
	cursor string,
	upToDate bool,
	closed bool,
	writeTimeout time.Duration,
) error {
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil &&
		!errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			return err
		}
	}

	control := map[string]any{"streamNextOffset": next.String()}
	if closed {
		control["streamClosed"] = true
	} else {
		control["streamCursor"] = cursor
		if upToDate {
			control["upToDate"] = true
		}
	}
	controlJSON, err := json.Marshal(control)
	if err != nil {
		return err
	}
	var event bytes.Buffer
	event.Grow(len(controlJSON) + 32)
	event.WriteString("event: control\n")
	event.WriteString("data:")
	event.Write(controlJSON)
	event.WriteString("\n\n")
	if _, err := w.Write(event.Bytes()); err != nil {
		return err
	}
	if err := controller.Flush(); err != nil {
		return err
	}
	if err := controller.SetWriteDeadline(time.Time{}); err != nil &&
		!errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

func benchmarkSSEFramePath(
	b *testing.B,
	newWrite func(http.ResponseWriter) func() error,
) {
	b.Helper()
	start := make(chan struct{})
	connected := make(chan struct{})
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if err := http.NewResponseController(w).Flush(); err != nil {
			serverErr <- err
			return
		}
		close(connected)
		<-start
		write := newWrite(w)
		for i := 0; i < b.N; i++ {
			if err := write(); err != nil {
				serverErr <- fmt.Errorf("write frame %d: %w", i, err)
				return
			}
		}
		serverErr <- nil
	}))
	defer server.Close()

	clientErr := make(chan error, 1)
	go func() {
		response, err := http.Get(server.URL) //nolint:gosec,noctx // local benchmark server
		if err != nil {
			clientErr <- err
			return
		}
		_, copyErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if copyErr != nil {
			clientErr <- copyErr
			return
		}
		clientErr <- closeErr
	}()
	<-connected
	b.ReportAllocs()
	b.SetBytes(152)
	b.ResetTimer()
	close(start)
	err := <-serverErr
	clientErrValue := <-clientErr
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
	if clientErrValue != nil {
		b.Fatal(clientErrValue)
	}
}
