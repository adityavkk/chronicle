package chronicle

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

const (
	catchupBenchmarkBytes = 16 << 20
	catchupBenchmarkFrame = 4 << 10
)

// BenchmarkCatchupStreaming16MiB measures the complete handler and MemoryStore
// page path while the response destination discards each Write. The result
// therefore exposes handler and page allocations without adding a recorder's
// second full response body.
func BenchmarkCatchupStreaming16MiB(b *testing.B) {
	stream := newCatchupBenchmarkStore(b)
	for _, pageBytes := range []int{256 << 10, 1 << 20, 4 << 20} {
		b.Run("page_bytes="+strconv.Itoa(pageBytes), func(b *testing.B) {
			handler := &Handler{
				Store:         stream,
				ReadPageBytes: pageBytes,
				Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			maxWrite := 0
			b.SetBytes(catchupBenchmarkBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				writer := &discardResponseWriter{header: make(http.Header)}
				request := httptest.NewRequest(http.MethodGet, "/benchmark?offset=-1", nil)
				handler.ServeHTTP(writer, request)
				if writer.status != http.StatusOK {
					b.Fatalf("status = %d, want 200", writer.status)
				}
				if writer.bytes != catchupBenchmarkBytes {
					b.Fatalf("response bytes = %d, want %d", writer.bytes, catchupBenchmarkBytes)
				}
				if writer.maxWrite > maxWrite {
					maxWrite = writer.maxWrite
				}
			}
			b.ReportMetric(float64(maxWrite), "max-write-B")
		})
	}
}

func newCatchupBenchmarkStore(tb testing.TB) *store.MemoryStore {
	tb.Helper()
	stream := store.NewMemoryStore()
	frame := make([]byte, catchupBenchmarkFrame)
	if _, _, err := stream.Create("/benchmark", store.CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: frame,
	}); err != nil {
		tb.Fatal(err)
	}
	for written := catchupBenchmarkFrame; written < catchupBenchmarkBytes; written += catchupBenchmarkFrame {
		if _, err := stream.Append("/benchmark", frame, store.AppendOptions{
			ContentType: "application/octet-stream",
		}); err != nil {
			tb.Fatal(err)
		}
	}
	return stream
}

type discardResponseWriter struct {
	header   http.Header
	status   int
	bytes    int64
	maxWrite int
}

func (w *discardResponseWriter) Header() http.Header { return w.header }

func (w *discardResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *discardResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.bytes += int64(len(p))
	if len(p) > w.maxWrite {
		w.maxWrite = len(p)
	}
	return io.Discard.Write(p)
}

func (*discardResponseWriter) Flush() {}
