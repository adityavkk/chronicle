package store

import (
	"testing"
	"time"
)

func TestShouldRenewReadAccess(t *testing.T) {
	zero := int64(0)
	one := int64(1)
	expiresAt := time.Unix(10, 0)

	for _, tc := range []struct {
		name string
		meta *StreamMetadata
		want bool
	}{
		{name: "missing metadata", meta: nil, want: false},
		{name: "non-expiring", meta: &StreamMetadata{}, want: false},
		{name: "absolute expiry", meta: &StreamMetadata{ExpiresAt: &expiresAt}, want: false},
		{name: "minimum sliding TTL", meta: &StreamMetadata{TTLSeconds: &zero}, want: true},
		{name: "positive sliding TTL", meta: &StreamMetadata{TTLSeconds: &one}, want: true},
		{
			name: "combined rules",
			meta: &StreamMetadata{TTLSeconds: &one, ExpiresAt: &expiresAt},
			want: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldRenewReadAccess(tc.meta); got != tc.want {
				t.Fatalf("ShouldRenewReadAccess() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReadSnapshotProjectionOwnsExpiryValues(t *testing.T) {
	ttl := int64(7)
	expiresAt := time.Unix(20, 0)
	requested := Offset{ReadSeq: 3, ByteOffset: 4}
	meta := &StreamMetadata{
		CurrentOffset:       Offset{ReadSeq: 5, ByteOffset: 6},
		ContentType:         "text/plain",
		Closed:              true,
		Incarnation:         "inc",
		TTLSeconds:          &ttl,
		ExpiresAt:           &expiresAt,
		ForkOffsetRequested: &requested,
		Producers: map[string]*ProducerState{
			"writer": {Epoch: 1, LastSeq: 2},
		},
	}

	snapshot := ReadSnapshotFromMetadata(meta)
	*meta.TTLSeconds = 8
	*meta.ExpiresAt = time.Unix(30, 0)
	*meta.ForkOffsetRequested = Offset{ReadSeq: 9}

	if *snapshot.TTLSeconds != 7 {
		t.Fatalf("snapshot TTL = %d, want 7", *snapshot.TTLSeconds)
	}
	if !snapshot.ExpiresAt.Equal(time.Unix(20, 0)) {
		t.Fatalf("snapshot expiry = %s, want %s", snapshot.ExpiresAt, time.Unix(20, 0))
	}
	wantRequested := Offset{ReadSeq: 3, ByteOffset: 4}
	if !snapshot.ForkOffsetRequested.Equal(wantRequested) {
		t.Fatalf("snapshot requested fork offset = %s, want %s", snapshot.ForkOffsetRequested, wantRequested)
	}
}
