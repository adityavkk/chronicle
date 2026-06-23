// Command dsui serves a DBeaver-style web UI for Durable Streams servers.
//
// The front-end (in ../../ui) is built with Vite into ./embedded and embedded
// into this binary with //go:embed, so `dsui` is a single self-contained
// executable: run it and a browser UI appears. The UI connects to any Durable
// Streams server by url:port; --server only prefills the default connection.
package main

import (
	"encoding/json"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

func main() {
	listen := flag.String("listen", ":4438", "address for the dsui web server")
	server := flag.String("server", "", "default Durable Streams server URL to prefill (e.g. http://localhost:4437)")
	open := flag.Bool("open", true, "open the UI in a browser on start")
	flag.Parse()

	webRoot, err := fs.Sub(embeddedFS, "embedded")
	if err != nil {
		log.Fatalf("dsui: embedded assets: %v", err)
	}
	fileServer := http.FileServer(http.FS(webRoot))

	mux := http.NewServeMux()

	// Runtime config the front-end fetches on load.
	mux.HandleFunc("/dsui-config.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"defaultServer": *server})
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
	log.Printf("dsui: serving UI on %s (default server %q)", url, *server)
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
	var args []string
	switch runtime.GOOS {
	case "darwin":
		name = "open"
	case "windows":
		name, args = "cmd", []string{"/c", "start"}
	default:
		name = "xdg-open"
	}
	args = append(args, url)
	_ = exec.Command(name, args...).Start()
}
