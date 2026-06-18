package redis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// Meta HASH field names. Timestamps are stored as UnixNano decimal strings:
// full fidelity for Go round-trips (ConfigMatches uses time.Equal), and
// precise enough in Lua (doubles lose ~hundreds of ns at this magnitude,
// irrelevant for ms-granularity expiry math).
const (
	fCT          = "ct"          // content type
	fTail        = "tail"        // current offset, "%016d_%016d" (presence marker)
	fLastSeq     = "lastSeq"     // last Stream-Seq value (absent if never set)
	fClosed      = "closed"      // "1" when closed
	fTTL         = "ttl"         // TTL seconds (absent if nil)
	fExpAt       = "expAtNs"     // absolute expiry, UnixNano (absent if nil)
	fCreatedAt   = "createdAtNs" // UnixNano
	fAccessedAt  = "accessedAtNs"
	fForkedFrom  = "forkedFrom" // source path (absent if not a fork)
	fForkOff     = "forkOff"    // resolved fork offset string
	fForkOffReq  = "forkOffReq" // user-supplied fork offset string (absent if nil)
	fForkSubOff  = "forkSubOff" // raw user-supplied sub-offset (absent if 0)
	fRefCount    = "refCount"   // number of forks referencing this stream
	fSoftDel     = "softDel"    // "1" when soft-deleted
	fClosedById  = "cbId"       // closedBy tuple; presence keyed on cbEpoch
	fClosedByEp  = "cbEpoch"
	fClosedBySeq = "cbSeq"
)

// metaToFields flattens metadata into HSET field/value pairs. Optional
// fields are omitted (callers write into fresh keys, so no HDEL needed).
func metaToFields(m *store.StreamMetadata) map[string]string {
	f := map[string]string{
		fCT:         m.ContentType,
		fTail:       m.CurrentOffset.String(),
		fCreatedAt:  strconv.FormatInt(m.CreatedAt.UnixNano(), 10),
		fAccessedAt: strconv.FormatInt(m.LastAccessedAt.UnixNano(), 10),
	}
	if m.LastSeq != "" {
		f[fLastSeq] = m.LastSeq
	}
	if m.Closed {
		f[fClosed] = "1"
	}
	if m.TTLSeconds != nil {
		f[fTTL] = strconv.FormatInt(*m.TTLSeconds, 10)
	}
	if m.ExpiresAt != nil {
		f[fExpAt] = strconv.FormatInt(m.ExpiresAt.UnixNano(), 10)
	}
	if m.ForkedFrom != "" {
		f[fForkedFrom] = m.ForkedFrom
		f[fForkOff] = m.ForkOffset.String()
		if m.ForkOffsetRequested != nil {
			f[fForkOffReq] = m.ForkOffsetRequested.String()
		}
		if m.ForkSubOffset != 0 {
			f[fForkSubOff] = strconv.FormatUint(m.ForkSubOffset, 10)
		}
	}
	if m.RefCount != 0 {
		f[fRefCount] = strconv.FormatInt(int64(m.RefCount), 10)
	}
	if m.SoftDeleted {
		f[fSoftDel] = "1"
	}
	if m.ClosedBy != nil {
		f[fClosedById] = m.ClosedBy.ProducerId
		f[fClosedByEp] = strconv.FormatInt(m.ClosedBy.Epoch, 10)
		f[fClosedBySeq] = strconv.FormatInt(m.ClosedBy.Seq, 10)
	}
	return f
}

// metaFromFields rebuilds metadata from an HGETALL result. Returns
// (nil, nil) when the hash is missing or lacks the tail marker field
// (stream does not exist). Producers are attached separately from the
// prod HASH.
func metaFromFields(path string, fields map[string]string) (*store.StreamMetadata, error) {
	tail, ok := fields[fTail]
	if !ok {
		return nil, nil
	}
	cur, err := store.ParseOffset(tail)
	if err != nil {
		return nil, fmt.Errorf("meta %s: bad tail: %w", path, err)
	}
	m := &store.StreamMetadata{
		Path:          path,
		ContentType:   fields[fCT],
		CurrentOffset: cur,
		LastSeq:       fields[fLastSeq],
		Closed:        fields[fClosed] == "1",
		SoftDeleted:   fields[fSoftDel] == "1",
		ForkedFrom:    fields[fForkedFrom],
		Producers:     make(map[string]*store.ProducerState),
	}
	if m.CreatedAt, err = parseNanoTime(fields, fCreatedAt); err != nil {
		return nil, fmt.Errorf("meta %s: %w", path, err)
	}
	if m.LastAccessedAt, err = parseNanoTime(fields, fAccessedAt); err != nil {
		return nil, fmt.Errorf("meta %s: %w", path, err)
	}
	if v, ok := fields[fTTL]; ok {
		ttl, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("meta %s: bad ttl %q", path, v)
		}
		m.TTLSeconds = &ttl
	}
	if v, ok := fields[fExpAt]; ok {
		ns, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("meta %s: bad expAtNs %q", path, v)
		}
		t := time.Unix(0, ns)
		m.ExpiresAt = &t
	}
	if m.ForkedFrom != "" {
		if m.ForkOffset, err = store.ParseOffset(fields[fForkOff]); err != nil {
			return nil, fmt.Errorf("meta %s: bad forkOff: %w", path, err)
		}
		if v, ok := fields[fForkOffReq]; ok {
			o, err := store.ParseOffset(v)
			if err != nil {
				return nil, fmt.Errorf("meta %s: bad forkOffReq: %w", path, err)
			}
			m.ForkOffsetRequested = &o
		}
		if v, ok := fields[fForkSubOff]; ok {
			if m.ForkSubOffset, err = strconv.ParseUint(v, 10, 64); err != nil {
				return nil, fmt.Errorf("meta %s: bad forkSubOff %q", path, v)
			}
		}
	}
	if v, ok := fields[fRefCount]; ok {
		rc, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("meta %s: bad refCount %q", path, v)
		}
		m.RefCount = int32(rc)
	}
	if v, ok := fields[fClosedByEp]; ok {
		ep, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("meta %s: bad cbEpoch %q", path, v)
		}
		seq, err := strconv.ParseInt(fields[fClosedBySeq], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("meta %s: bad cbSeq %q", path, fields[fClosedBySeq])
		}
		m.ClosedBy = &store.ClosedByProducer{
			ProducerId: fields[fClosedById],
			Epoch:      ep,
			Seq:        seq,
		}
	}
	return m, nil
}

func parseNanoTime(fields map[string]string, field string) (time.Time, error) {
	v, ok := fields[field]
	if !ok {
		return time.Time{}, fmt.Errorf("missing %s", field)
	}
	ns, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("bad %s %q", field, v)
	}
	return time.Unix(0, ns), nil
}

// encodeProducerState renders a prod HASH value: "epoch:lastSeq:lastUpdated".
// The numeric parts cannot contain ':', so the encoding is unambiguous.
func encodeProducerState(s *store.ProducerState) string {
	return strconv.FormatInt(s.Epoch, 10) + ":" +
		strconv.FormatInt(s.LastSeq, 10) + ":" +
		strconv.FormatInt(s.LastUpdated, 10)
}

func decodeProducerState(v string) (*store.ProducerState, error) {
	parts := strings.SplitN(v, ":", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("malformed producer state %q", v)
	}
	epoch, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("malformed producer epoch %q", v)
	}
	lastSeq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("malformed producer lastSeq %q", v)
	}
	lastUpdated, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("malformed producer lastUpdated %q", v)
	}
	return &store.ProducerState{Epoch: epoch, LastSeq: lastSeq, LastUpdated: lastUpdated}, nil
}

// producersFromHash decodes the full prod HASH into the metadata map shape.
func producersFromHash(h map[string]string) (map[string]*store.ProducerState, error) {
	out := make(map[string]*store.ProducerState, len(h))
	for id, v := range h {
		s, err := decodeProducerState(v)
		if err != nil {
			return nil, err
		}
		out[id] = s
	}
	return out, nil
}
