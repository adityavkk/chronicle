package main

import "embed"

// embeddedFS holds the built dsui front-end (Vite output), so chronicle is a
// single binary serving both the Durable Streams API (under the stream root) and
// the web console (everything else). The `all:` prefix keeps dotfiles so an empty
// checkout still compiles via embedded/.gitkeep; the Docker build populates the
// real index.html + assets before `go build` (see Dockerfile.chronicle). When the
// UI was not built in, index.html is absent and chronicle serves API-only.
//
//go:embed all:embedded
var embeddedFS embed.FS
