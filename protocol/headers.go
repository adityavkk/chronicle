// Package protocol holds the pure, I/O-free core of chronicle: protocol
// header names, request parsing, producer validation, cursor math, JSON-mode
// handling, and SSE framing.
//
// Header constants are ported verbatim from the Durable Streams reference
// Caddy plugin (packages/caddy-plugin/handler.go @ 82f9963).
package protocol

// Protocol header names.
const (
	HeaderStreamNextOffset      = "Stream-Next-Offset"
	HeaderStreamCursor          = "Stream-Cursor"
	HeaderStreamUpToDate        = "Stream-Up-To-Date"
	HeaderStreamSeq             = "Stream-Seq"
	HeaderStreamTTL             = "Stream-TTL"
	HeaderStreamExpiresAt       = "Stream-Expires-At"
	HeaderStreamClosed          = "Stream-Closed"
	HeaderStreamSSEDataEncoding = "Stream-SSE-Data-Encoding"

	// Idempotent producer headers
	HeaderProducerId          = "Producer-Id"
	HeaderProducerEpoch       = "Producer-Epoch"
	HeaderProducerSeq         = "Producer-Seq"
	HeaderProducerExpectedSeq = "Producer-Expected-Seq"
	HeaderProducerReceivedSeq = "Producer-Received-Seq"
)

// Fork headers (request headers only — not set on responses).
const (
	HeaderStreamForkedFrom    = "Stream-Forked-From"
	HeaderStreamForkOffset    = "Stream-Fork-Offset"
	HeaderStreamForkSubOffset = "Stream-Fork-Sub-Offset"
)
