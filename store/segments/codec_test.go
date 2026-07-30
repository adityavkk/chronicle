package segments

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func refForEncoded(encoded EncodedSegment) SegmentRef {
	return SegmentRef{
		ID:             encoded.Checksum,
		DataKey:        "data",
		IndexKey:       "index",
		StartExclusive: encoded.StartExclusive.String(),
		EndInclusive:   encoded.EndInclusive.String(),
		Records:        encoded.Records,
		DataBytes:      int64(len(encoded.Data)),
		IndexBytes:     int64(len(encoded.Index)),
		IndexStride:    encoded.IndexStride,
		BlockBytes:     encoded.BlockBytes,
		BlockChecksums: append([]string(nil), encoded.BlockChecksums...),
		IndexChecksum:  encoded.IndexChecksum,
		IndexEntries:   append([]IndexEntry(nil), encoded.IndexEntries...),
		Checksum:       encoded.Checksum,
	}
}

func TestCodecPreservesBytesOffsetsAndSparseBoundaries(t *testing.T) {
	messages := []store.Message{
		{Data: []byte{0, 0xff, '|'}, Offset: store.Offset{ByteOffset: 3}},
		{Data: []byte(`{"n":1}`), Offset: store.Offset{ByteOffset: 10}},
		{Data: []byte("tail"), Offset: store.Offset{ByteOffset: 14}},
	}
	encoded, err := Encode(store.ZeroOffset, messages, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(encoded.Index), 2*indexEntryBytes; got != want {
		t.Fatalf("index bytes = %d, want %d", got, want)
	}
	ref := refForEncoded(encoded)
	got, err := DecodeAfter(ref, encoded.Data, encoded.Index, store.Offset{ByteOffset: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !bytes.Equal(got[0].Data, messages[1].Data) ||
		!got[0].Offset.Equal(messages[1].Offset) || !bytes.Equal(got[1].Data, messages[2].Data) {
		t.Fatalf("round trip mismatch: %#v", got)
	}

	corrupt := append([]byte(nil), encoded.Data...)
	corrupt[len(corrupt)-1] ^= 0xff
	if _, err := DecodeAfter(ref, corrupt, encoded.Index, store.ZeroOffset); !errors.Is(err, ErrChecksum) {
		t.Fatalf("corruption error = %v, want ErrChecksum", err)
	}
}

type rangeRecordingBackend struct {
	Backend
	wholeReads int
	ranges     [][2]int64
}

func (b *rangeRecordingBackend) Read(
	context.Context,
	SegmentRef,
) ([]byte, []byte, error) {
	b.wholeReads++
	return nil, nil, errors.New("unexpected whole-object read")
}

func (b *rangeRecordingBackend) ReadDataRange(
	ctx context.Context,
	ref SegmentRef,
	start, length int64,
) ([]byte, error) {
	b.ranges = append(b.ranges, [2]int64{start, length})
	return b.Backend.ReadDataRange(ctx, ref, start, length)
}

func TestDecodePageUsesBoundedAuthenticatedRanges(t *testing.T) {
	messages := make([]store.Message, SegmentMaxFrames)
	var tail uint64
	for i := range messages {
		payload := bytes.Repeat([]byte{byte(i)}, 512)
		tail += uint64(len(payload))
		messages[i] = store.Message{
			Data:   payload,
			Offset: store.Offset{ByteOffset: tail},
		}
	}
	encoded, err := Encode(store.ZeroOffset, messages, 32)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := NewFileBackend(ModeLocalFiles, t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := backend.Put(context.Background(), "/ranged", 1, encoded)
	if err != nil {
		t.Fatal(err)
	}
	recording := &rangeRecordingBackend{Backend: backend}
	from := messages[700].Offset
	got, fetched, _, err := DecodePageAfter(
		context.Background(),
		recording,
		ref,
		from,
		1024,
		8,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !got[0].Offset.Equal(messages[701].Offset) ||
		!got[1].Offset.Equal(messages[702].Offset) {
		t.Fatalf("ranged page = %#v", got)
	}
	if recording.wholeReads != 0 {
		t.Fatalf("whole-object reads = %d, want 0", recording.wholeReads)
	}
	if len(recording.ranges) == 0 || fetched >= len(encoded.Data) {
		t.Fatalf("ranges=%v fetched=%d full=%d", recording.ranges, fetched, len(encoded.Data))
	}
	for _, gotRange := range recording.ranges {
		if gotRange[0]%SegmentBlockBytes != 0 || gotRange[1] > SegmentBlockBytes {
			t.Fatalf("unbounded or unaligned range: %v", gotRange)
		}
	}
}

func TestDecodeRejectsManifestIndexThatDoesNotNameARecord(t *testing.T) {
	encoded, err := Encode(store.ZeroOffset, []store.Message{
		{Data: []byte("one"), Offset: store.Offset{ByteOffset: 3}},
		{Data: []byte("two"), Offset: store.Offset{ByteOffset: 6}},
		{Data: []byte("three"), Offset: store.Offset{ByteOffset: 11}},
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	ref := refForEncoded(encoded)
	ref.IndexEntries[1].DataPosition++
	if _, err := DecodeAfter(ref, encoded.Data, encoded.Index, store.ZeroOffset); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("full decode error = %v, want ErrCorrupt", err)
	}

	backend := &encodedRangeBackend{data: encoded.Data}
	if _, _, _, err := DecodePageAfter(
		context.Background(),
		backend,
		ref,
		store.Offset{ByteOffset: 6},
		1024,
		8,
	); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("range decode error = %v, want ErrCorrupt", err)
	}
}

type encodedRangeBackend struct {
	data []byte
}

func (b *encodedRangeBackend) Mode() Mode { return ModeLocalFiles }
func (b *encodedRangeBackend) Load(context.Context, string) (*Manifest, string, error) {
	return nil, "", ErrNoManifest
}

func (b *encodedRangeBackend) Put(context.Context, string, uint64, EncodedSegment) (SegmentRef, error) {
	return SegmentRef{}, errors.New("not implemented")
}

func (b *encodedRangeBackend) Publish(context.Context, string, string, *Manifest) (string, error) {
	return "", errors.New("not implemented")
}

func (b *encodedRangeBackend) Read(context.Context, SegmentRef) ([]byte, []byte, error) {
	return nil, nil, errors.New("not implemented")
}

func (b *encodedRangeBackend) ReadDataRange(
	_ context.Context,
	ref SegmentRef,
	start, length int64,
) ([]byte, error) {
	block, err := validateDataRange(ref, start, length)
	if err != nil {
		return nil, err
	}
	data := append([]byte(nil), b.data[start:start+length]...)
	if byteChecksum(data) != ref.BlockChecksums[block] {
		return nil, ErrChecksum
	}
	return data, nil
}
func (b *encodedRangeBackend) Tombstone(context.Context, string) error { return nil }
func (b *encodedRangeBackend) GC(context.Context, string, GCRetention) (GCResult, error) {
	return GCResult{}, nil
}
func (b *encodedRangeBackend) Close() error { return nil }

func TestRangeReadRejectsCorruptAuthenticatedBlock(t *testing.T) {
	encoded, err := Encode(store.ZeroOffset, []store.Message{{
		Data:   bytes.Repeat([]byte("x"), SegmentBlockBytes+100),
		Offset: store.Offset{ByteOffset: SegmentBlockBytes + 100},
	}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	ref := refForEncoded(encoded)
	backend := &encodedRangeBackend{data: append([]byte(nil), encoded.Data...)}
	backend.data[SegmentBlockBytes+10] ^= 0xff
	_, _, _, err = DecodePageAfter(
		context.Background(),
		backend,
		ref,
		store.ZeroOffset,
		1,
		1,
	)
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("error = %v, want ErrChecksum", err)
	}
}

func TestEncodeRejectsMoreThanOneBoundedPageOfFrames(t *testing.T) {
	messages := make([]store.Message, SegmentMaxFrames+1)
	for i := range messages {
		messages[i] = store.Message{
			Data:   []byte("x"),
			Offset: store.Offset{ByteOffset: uint64(i + 1)},
		}
	}
	if _, err := Encode(store.ZeroOffset, messages, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}
}

func TestCodecRejectsNonIncreasingOffsets(t *testing.T) {
	_, err := Encode(store.ZeroOffset, []store.Message{
		{Data: []byte("a"), Offset: store.Offset{ByteOffset: 1}},
		{Data: []byte("b"), Offset: store.Offset{ByteOffset: 1}},
	}, 1)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}
}
