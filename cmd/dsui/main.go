// Command dsui serves a DBeaver-style web UI for Durable Streams servers.
//
// The front-end (in ../../ui) is built with Vite into ./embedded and embedded
// into this binary with //go:embed, so `dsui` is a single self-contained
// executable: run it and a browser UI appears. The UI connects to any Durable
// Streams server by url:port; --server only prefills the default connection.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		return v == "true" || v == "1"
	}
	return def
}

func main() {
	// Flags default from env vars (DSUI_*) so the binary works under WCNP's
	// env-driven helm chart as well as from the command line.
	listen := flag.String("listen", envOr("DSUI_LISTEN", ":4438"), "address for the dsui web server")
	server := flag.String("server", envOr("DSUI_SERVER", ""), "default Durable Streams server URL to prefill (e.g. http://localhost:4437)")
	captureBase := flag.String("capture-base", envOr("DSUI_CAPTURE_BASE", ""), "base URL the chronicle server uses to reach this binary's webhook-capture endpoint (default derived from --listen, e.g. http://localhost:4438)")
	open := flag.Bool("open", envBool("DSUI_OPEN", true), "open the UI in a browser on start")
	flag.Parse()

	webRoot, err := fs.Sub(embeddedFS, "embedded")
	if err != nil {
		log.Fatalf("dsui: embedded assets: %v", err)
	}
	fileServer := http.FileServer(http.FS(webRoot))

	mux := http.NewServeMux()

	// The built-in webhook-capture endpoint (/__hooks/{id}). A webhook
	// subscription's webhook_url points at <captureBase>/__hooks/<id>; chronicle
	// POSTs signed wakes there, this binary buffers them and relays to the browser
	// over SSE (the browser cannot host an inbound endpoint itself).
	captureStore := newCaptureStore()
	registerCaptureRoutes(mux, captureStore)

	// The base URL the chronicle server uses to reach this capture endpoint. The
	// browser builds a webhook_url as <captureBase>/__hooks/<id> from this, so it
	// must be reachable by chronicle (localhost when both run on the same host).
	resolvedCaptureBase := *captureBase
	if resolvedCaptureBase == "" {
		resolvedCaptureBase = "http://localhost" + portOnly(*listen)
	}

	// Runtime config the front-end fetches on load.
	mux.HandleFunc("/dsui-config.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"defaultServer": *server,
			"captureBase":   resolvedCaptureBase,
		})
	})

	// Serve embedded assets with a single-page-app fallback to index.html.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			serveIndex(w, webRoot)
			return
		}
		if _, statErr := fs.Stat(webRoot, p); statErr != nil {
			serveIndex(w, webRoot)
			return
		}
		fileServer.ServeHTTP(w, r)
	})

	url := "http://localhost" + portOnly(*listen)
	log.Printf("dsui: serving UI on %s (default server %q, capture base %q)", url, *server, resolvedCaptureBase)
	if *open {
		go openBrowser(url)
	}
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

func serveIndex(w http.ResponseWriter, webRoot fs.FS) {
	b, err := fs.ReadFile(webRoot, "index.html")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err != nil {
		_, _ = w.Write([]byte("<h1>dsui</h1><p>The UI assets are not built yet. " +
			"Build the front-end (cd ui && npm install && npm run build) and rebuild this binary.</p>"))
		return
	}
	_, _ = w.Write(b)
}

// portOnly turns ":4438" or "host:4438" into ":4438" for building a localhost URL.
func portOnly(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[i:]
	}
	return ":" + addr
}

func openBrowser(url string) {
	var name string
	args := make([]string, 0, 1)
	switch runtime.GOOS {
	case "darwin":
		name = "open"
	case "windows":
		name, args = "cmd", []string{"/c", "start"}
	default:
		name = "xdg-open"
	}
	args = append(args, url)
	// Fire-and-forget: the spawned browser outlives the call, so there is no
	// meaningful deadline — context.Background() satisfies the no-context lint
	// without pretending to a cancellation we never act on.
	_ = exec.CommandContext(context.Background(), name, args...).Start()
}
