package main

import "embed"

// embeddedFS holds the built front-end (Vite output). The `all:` prefix keeps
// dotfiles so an empty checkout still compiles via embedded/.gitkeep; after the
// UI is built, this carries the real index.html + assets.
//
//go:embed all:embedded
var embeddedFS embed.FS
