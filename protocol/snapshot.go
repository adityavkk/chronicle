package protocol

// Projection snapshot extension headers. Offsets and incarnations are opaque
// to clients; the extension never changes ordinary stream append semantics.
const (
	HeaderStreamSnapshot              = "Stream-Snapshot"
	HeaderStreamSnapshotOffset        = "Stream-Snapshot-Offset"
	HeaderStreamSnapshotMaxBytes      = "Stream-Snapshot-Max-Bytes"
	HeaderStreamSnapshotMaxTotalBytes = "Stream-Snapshot-Max-Total-Bytes"
	HeaderStreamSnapshotMaxVersions   = "Stream-Snapshot-Max-Versions"
	HeaderStreamIncarnation           = "Stream-Incarnation"
	HeaderIfStreamIncarnation         = "If-Stream-Incarnation"
	HeaderContentDigest               = "Content-Digest"
)
