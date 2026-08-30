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
	HeaderStreamEnvelope        = "Stream-Envelope"

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

// Write-fencing extension headers (#183, PROTOCOL §11.1). Write-Fence declares
// a fenced stream on PUT and asserts the fenced class on POST; Write-Token
// carries the claim-scoped write token; the sealed pair is HEAD's summary of
// the most recent seal.
const (
	HeaderWriteFence                 = "Write-Fence"
	HeaderWriteToken                 = "Write-Token"
	HeaderWriteFenceSealedGeneration = "Write-Fence-Sealed-Generation"
	HeaderWriteFenceSealedOffset     = "Write-Fence-Sealed-Offset"
)
