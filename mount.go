package chronicle

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
)

// Mount serves the stream handler under root (e.g. "/v1/stream/"), the
// boundary between wire paths and store paths:
//
//   - requests outside root get a 404;
//   - the reserved "__ds" first segment (the protocol's subscription APIs) is
//     passed through to the handler, which routes it to the webhook layer when
//     subscriptions are enabled (else it 404s as a reserved path);
//   - the root prefix is stripped before dispatch, so stream paths reaching
//     the store are root-relative with a leading slash ("/v1/stream/foo" →
//     "/foo"); the Stream-Forked-From request header is translated the same
//     way so fork sources resolve against store keys;
//   - the Location response header, which the handler builds from the
//     stripped path, is rewritten back to the full wire path.
//
// root must start and end with "/".
func Mount(root string, stream http.Handler) (http.Handler, error) {
	if !strings.HasPrefix(root, "/") || !strings.HasSuffix(root, "/") {
		return nil, fmt.Errorf("stream root %q must start and end with %q", root, "/")
	}
	prefix := strings.TrimSuffix(root, "/") // "" when root is "/"

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Browser security headers on routing errors too; the stream handler
		// re-sets them for protocol responses.
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")

		if !strings.HasPrefix(r.URL.Path, root) {
			http.NotFound(w, r)
			return
		}
		rel := strings.TrimPrefix(r.URL.Path, prefix) // keeps the leading "/"

		r2 := r.Clone(r.Context())
		r2.URL.Path = rel
		if ff := r2.Header.Get(protocol.HeaderStreamForkedFrom); strings.HasPrefix(ff, root) {
			r2.Header.Set(protocol.HeaderStreamForkedFrom, strings.TrimPrefix(ff, prefix))
		}

		if prefix == "" {
			stream.ServeHTTP(w, r2)
			return
		}
		stream.ServeHTTP(&locationRewriter{ResponseWriter: w, prefix: prefix}, r2)
	}), nil
}

// locationRewriter rewrites the Location response header from the
// root-relative path the stream handler saw back to the full wire path.
type locationRewriter struct {
	http.ResponseWriter
	prefix      string
	wroteHeader bool
}

func (w *locationRewriter) WriteHeader(status int) {
	if !w.wroteHeader {
		w.wroteHeader = true
		if loc := w.Header().Get("Location"); loc != "" {
			if u, err := url.Parse(loc); err == nil {
				u.Path = w.prefix + u.Path
				w.Header().Set("Location", u.String())
			}
		}
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *locationRewriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// Flush forwards http.Flusher so SSE streaming works through the wrapper.
func (w *locationRewriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
