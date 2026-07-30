package segments

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

const (
	recordHeaderBytes = 20 // read-seq u64, byte-offset u64, payload length u32
	indexEntryBytes   = 32 // end offset (2*u64), ordinal u64, data position u64
)

var segmentMagic = [8]byte{'C', 'H', 'S', 'E', 'G', '0', '0', '1'}

// EncodedSegment is the immutable wire representation. Data is a sequence of
// length-delimited records; Index is a fixed-width sparse boundary table. The
// checksum covers both blobs and their lengths.
type EncodedSegment struct {
	StartExclusive store.Offset
	EndInclusive   store.Offset
	Records        uint64
	IndexStride    uint32
	Data           []byte
	Index          []byte
	BlockBytes     int64
	BlockChecksums []string
	IndexChecksum  string
	IndexEntries   []IndexEntry
	Checksum       string
}

// Encode builds a segment without changing message bytes or logical offsets.
func Encode(start store.Offset, messages []store.Message, stride int) (EncodedSegment, error) {
	if len(messages) == 0 {
		return EncodedSegment{}, fmt.Errorf("%w: cannot encode an empty segment", ErrCorrupt)
	}
	if stride <= 0 {
		return EncodedSegment{}, fmt.Errorf("%w: index stride must be positive", ErrCorrupt)
	}
	if len(messages) > SegmentMaxFrames {
		return EncodedSegment{}, fmt.Errorf("%w: segment contains %d records, maximum is %d", ErrCorrupt, len(messages), SegmentMaxFrames)
	}
	data := make([]byte, 0)
	index := make([]byte, 0, ((len(messages)+stride-1)/stride)*indexEntryBytes)
	entries := make([]IndexEntry, 0, (len(messages)+stride-1)/stride)
	previous := start
	for i, msg := range messages {
		if !previous.LessThan(msg.Offset) || uint64(len(msg.Data)) > uint64(^uint32(0)) {
			return EncodedSegment{}, fmt.Errorf("%w: non-increasing offset or oversized record %d", ErrCorrupt, i)
		}
		if i%stride == 0 {
			entries = append(entries, IndexEntry{
				Offset:       msg.Offset.String(),
				Ordinal:      uint64(i),
				DataPosition: uint64(len(data)),
			})
			var entry [indexEntryBytes]byte
			binary.BigEndian.PutUint64(entry[0:8], msg.Offset.ReadSeq)
			binary.BigEndian.PutUint64(entry[8:16], msg.Offset.ByteOffset)
			binary.BigEndian.PutUint64(entry[16:24], uint64(i))
			binary.BigEndian.PutUint64(entry[24:32], uint64(len(data)))
			index = append(index, entry[:]...)
		}
		var header [recordHeaderBytes]byte
		binary.BigEndian.PutUint64(header[0:8], msg.Offset.ReadSeq)
		binary.BigEndian.PutUint64(header[8:16], msg.Offset.ByteOffset)
		binary.BigEndian.PutUint32(header[16:20], uint32(len(msg.Data)))
		data = append(data, header[:]...)
		data = append(data, msg.Data...)
		previous = msg.Offset
	}
	return EncodedSegment{
		StartExclusive: start,
		EndInclusive:   previous,
		Records:        uint64(len(messages)),
		IndexStride:    uint32(stride),
		Data:           data,
		Index:          index,
		BlockBytes:     SegmentBlockBytes,
		BlockChecksums: blockChecksums(data, SegmentBlockBytes),
		IndexChecksum:  byteChecksum(index),
		IndexEntries:   entries,
		Checksum:       segmentChecksum(data, index),
	}, nil
}

func byteChecksum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func blockChecksums(data []byte, blockBytes int64) []string {
	if blockBytes <= 0 {
		return nil
	}
	out := make([]string, 0, (int64(len(data))+blockBytes-1)/blockBytes)
	for start := int64(0); start < int64(len(data)); start += blockBytes {
		end := min(start+blockBytes, int64(len(data)))
		out = append(out, byteChecksum(data[start:end]))
	}
	return out
}

func segmentChecksum(data, index []byte) string {
	h := sha256.New()
	h.Write(segmentMagic[:])
	var sizes [16]byte
	binary.BigEndian.PutUint64(sizes[0:8], uint64(len(data)))
	binary.BigEndian.PutUint64(sizes[8:16], uint64(len(index)))
	h.Write(sizes[:])
	h.Write(data)
	h.Write(index)
	return hex.EncodeToString(h.Sum(nil))
}

// DecodeAfter verifies the entire immutable object, validates its sparse
// index, and returns records whose end offset is strictly greater than from.
func DecodeAfter(ref SegmentRef, data, index []byte, from store.Offset) ([]store.Message, error) {
	if err := validateSegmentRef(ref); err != nil {
		return nil, err
	}
	if int64(len(data)) != ref.DataBytes || int64(len(index)) != ref.IndexBytes {
		return nil, fmt.Errorf("%w: immutable object length mismatch", ErrCorrupt)
	}
	if segmentChecksum(data, index) != ref.Checksum {
		return nil, ErrChecksum
	}
	if byteChecksum(index) != ref.IndexChecksum {
		return nil, ErrChecksum
	}
	for block, checksum := range ref.BlockChecksums {
		start := int64(block) * ref.BlockBytes
		end := min(start+ref.BlockBytes, int64(len(data)))
		if byteChecksum(data[start:end]) != checksum {
			return nil, ErrChecksum
		}
	}
	entries, err := decodeIndex(index)
	if err != nil {
		return nil, err
	}
	if len(entries) != len(ref.IndexEntries) {
		return nil, fmt.Errorf("%w: manifest and index cardinality differ", ErrCorrupt)
	}
	for i := range entries {
		if entries[i] != ref.IndexEntries[i] {
			return nil, fmt.Errorf("%w: manifest and index entry %d differ", ErrCorrupt, i)
		}
	}

	start, err := store.ParseOffset(ref.StartExclusive)
	if err != nil {
		return nil, fmt.Errorf("%w: bad ref start", ErrCorrupt)
	}
	end, err := store.ParseOffset(ref.EndInclusive)
	if err != nil {
		return nil, fmt.Errorf("%w: bad ref end", ErrCorrupt)
	}
	out := make([]store.Message, 0)
	previous := start
	var pos uint64
	for ordinal := uint64(0); ordinal < ref.Records; ordinal++ {
		recordPos := pos
		off, payload, next, parseErr := decodeRecord(data, pos)
		if parseErr != nil {
			return nil, parseErr
		}
		if !previous.LessThan(off) {
			return nil, fmt.Errorf("%w: non-increasing record offsets", ErrCorrupt)
		}
		if ordinal%uint64(ref.IndexStride) == 0 {
			entry := ref.IndexEntries[ordinal/uint64(ref.IndexStride)]
			if entry.Ordinal != ordinal || entry.DataPosition != recordPos || entry.Offset != off.String() {
				return nil, fmt.Errorf("%w: sparse entry does not describe record %d", ErrCorrupt, ordinal)
			}
		}
		if from.LessThan(off) {
			out = append(out, store.Message{Data: append([]byte(nil), payload...), Offset: off})
		}
		previous = off
		pos = next
	}
	if pos != uint64(len(data)) {
		return nil, fmt.Errorf("%w: record count or trailing data mismatch", ErrCorrupt)
	}
	if !previous.Equal(end) {
		return nil, fmt.Errorf("%w: final offset mismatch", ErrCorrupt)
	}
	return out, nil
}

func decodeIndex(index []byte) ([]IndexEntry, error) {
	if len(index) == 0 || len(index)%indexEntryBytes != 0 {
		return nil, fmt.Errorf("%w: invalid sparse index length", ErrCorrupt)
	}
	entries := make([]IndexEntry, 0, len(index)/indexEntryBytes)
	for p := 0; p < len(index); p += indexEntryBytes {
		offset := store.Offset{
			ReadSeq:    binary.BigEndian.Uint64(index[p : p+8]),
			ByteOffset: binary.BigEndian.Uint64(index[p+8 : p+16]),
		}
		entries = append(entries, IndexEntry{
			Offset:       offset.String(),
			Ordinal:      binary.BigEndian.Uint64(index[p+16 : p+24]),
			DataPosition: binary.BigEndian.Uint64(index[p+24 : p+32]),
		})
	}
	return entries, nil
}

func decodeRecord(data []byte, pos uint64) (store.Offset, []byte, uint64, error) {
	if pos > uint64(len(data)) || uint64(len(data))-pos < recordHeaderBytes {
		return store.Offset{}, nil, pos, fmt.Errorf("%w: truncated record header", ErrCorrupt)
	}
	header := data[pos : pos+recordHeaderBytes]
	off := store.Offset{
		ReadSeq:    binary.BigEndian.Uint64(header[0:8]),
		ByteOffset: binary.BigEndian.Uint64(header[8:16]),
	}
	size := uint64(binary.BigEndian.Uint32(header[16:20]))
	payloadStart := pos + recordHeaderBytes
	if size > uint64(len(data))-payloadStart {
		return store.Offset{}, nil, pos, fmt.Errorf("%w: truncated record payload", ErrCorrupt)
	}
	return off, data[payloadStart : payloadStart+size], payloadStart + size, nil
}

type rangeDecoder struct {
	ctx     context.Context
	backend Backend
	ref     SegmentRef
	blocks  map[int64][]byte
	fetched int
}

func (r *rangeDecoder) read(start, length int64) ([]byte, error) {
	if start < 0 || length < 0 || start > r.ref.DataBytes || length > r.ref.DataBytes-start {
		return nil, fmt.Errorf("%w: record range exceeds segment", ErrCorrupt)
	}
	out := make([]byte, length)
	written := int64(0)
	for written < length {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		position := start + written
		block := position / r.ref.BlockBytes
		blockStart := block * r.ref.BlockBytes
		data := r.blocks[block]
		if data == nil {
			blockLength := min(r.ref.BlockBytes, r.ref.DataBytes-blockStart)
			var err error
			data, err = r.backend.ReadDataRange(r.ctx, r.ref, blockStart, blockLength)
			if err != nil {
				return nil, err
			}
			r.blocks[block] = data
			r.fetched += len(data)
		}
		inBlock := position - blockStart
		n := min(int64(len(data))-inBlock, length-written)
		if n <= 0 {
			return nil, fmt.Errorf("%w: empty authenticated data block", ErrCorrupt)
		}
		copy(out[written:written+n], data[inBlock:inBlock+n])
		written += n
	}
	return out, nil
}

func (r *rangeDecoder) recordHeader(pos uint64) (store.Offset, int64, int64, uint64, error) {
	header, err := r.read(int64(pos), recordHeaderBytes)
	if err != nil {
		return store.Offset{}, 0, 0, pos, err
	}
	off := store.Offset{
		ReadSeq:    binary.BigEndian.Uint64(header[0:8]),
		ByteOffset: binary.BigEndian.Uint64(header[8:16]),
	}
	size := int64(binary.BigEndian.Uint32(header[16:20]))
	payloadStart := int64(pos) + recordHeaderBytes
	if payloadStart > r.ref.DataBytes || size > r.ref.DataBytes-payloadStart {
		return store.Offset{}, 0, 0, pos, fmt.Errorf("%w: truncated record payload", ErrCorrupt)
	}
	return off, size, payloadStart, uint64(payloadStart + size), nil
}

// DecodePageAfter uses the manifest-bound sparse index and independently
// authenticated data blocks to return one bounded page without loading a whole
// segment. A first record larger than targetBytes is returned alone.
func DecodePageAfter(
	ctx context.Context,
	backend Backend,
	ref SegmentRef,
	from store.Offset,
	targetBytes int,
	maxFrames int,
) ([]store.Message, int, int, error) {
	if err := validateSegmentRef(ref); err != nil {
		return nil, 0, 0, err
	}
	if targetBytes <= 0 {
		targetBytes = store.DefaultReadPageBytes
	}
	if maxFrames <= 0 {
		maxFrames = store.DefaultReadPageFrames
	}
	if maxFrames > SegmentMaxFrames {
		maxFrames = SegmentMaxFrames
	}

	parsedEntries := make([]store.Offset, len(ref.IndexEntries))
	for i, entry := range ref.IndexEntries {
		offset, err := store.ParseOffset(entry.Offset)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("%w: sparse offset %d", ErrCorrupt, i)
		}
		parsedEntries[i] = offset
	}
	entryIndex := sort.Search(len(parsedEntries), func(i int) bool {
		return from.LessThan(parsedEntries[i])
	})
	if entryIndex > 0 {
		entryIndex--
	}
	entry := ref.IndexEntries[entryIndex]
	decoder := rangeDecoder{
		ctx:     ctx,
		backend: backend,
		ref:     ref,
		blocks:  make(map[int64][]byte),
	}
	pos := entry.DataPosition
	ordinal := entry.Ordinal
	var previous store.Offset
	out := make([]store.Message, 0, min(maxFrames, 16))
	discarded := 0
	returned := 0
	first := true
	for ordinal < ref.Records {
		recordPos := pos
		off, size, payloadStart, next, err := decoder.recordHeader(pos)
		if err != nil {
			return nil, decoder.fetched, discarded, err
		}
		if first {
			if off.String() != entry.Offset || recordPos != entry.DataPosition {
				return nil, decoder.fetched, discarded, fmt.Errorf("%w: sparse entry does not match ranged record", ErrCorrupt)
			}
			first = false
		} else if !previous.LessThan(off) {
			return nil, decoder.fetched, discarded, fmt.Errorf("%w: non-increasing ranged record offsets", ErrCorrupt)
		}
		previous = off
		pos = next
		ordinal++
		if !from.LessThan(off) {
			discarded += int(size)
			continue
		}
		if len(out) > 0 && (len(out) >= maxFrames || returned+int(size) > targetBytes) {
			break
		}
		payload, err := decoder.read(payloadStart, size)
		if err != nil {
			return nil, decoder.fetched, discarded, err
		}
		out = append(out, store.Message{Data: payload, Offset: off})
		returned += len(payload)
		if len(out) >= maxFrames || returned >= targetBytes {
			break
		}
	}
	return out, decoder.fetched, discarded, nil
}
