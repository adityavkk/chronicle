package redis

import (
	"reflect"
	"testing"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func TestMetaRoundTripFull(t *testing.T) {
	ttl := int64(300)
	expAt := time.Unix(1765432100, 123456789)
	reqOff := off(7)
	in := &store.StreamMetadata{
		Path:                "/v1/full",
		ContentType:         "application/json",
		CurrentOffset:       off(42),
		LastSeq:             "seq-009",
		TTLSeconds:          &ttl,
		ExpiresAt:           &expAt,
		CreatedAt:           time.Unix(1765000000, 111),
		LastAccessedAt:      time.Unix(1765000050, 222),
		Producers:           map[string]*store.ProducerState{},
		Closed:              true,
		ClosedBy:            &store.ClosedByProducer{ProducerId: "p1", Epoch: 3, Seq: 9},
		ForkedFrom:          "/v1/src",
		ForkOffset:          off(10),
		ForkOffsetRequested: &reqOff,
		ForkSubOffset:       2,
		RefCount:            3,
		SoftDeleted:         true,
	}
	out, err := metaFromFields(in.Path, metaToFields(in))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip mismatch:\n in: %+v\nout: %+v", in, out)
	}
	if !out.ExpiresAt.Equal(expAt) {
		t.Error("ExpiresAt lost nanosecond fidelity")
	}
}

func TestMetaRoundTripMinimal(t *testing.T) {
	in := &store.StreamMetadata{
		Path:           "/v1/min",
		ContentType:    "application/octet-stream",
		CurrentOffset:  store.ZeroOffset,
		CreatedAt:      time.Unix(1765000000, 0),
		LastAccessedAt: time.Unix(1765000000, 0),
		Producers:      map[string]*store.ProducerState{},
	}
	fields := metaToFields(in)
	for _, absent := range []string{fLastSeq, fClosed, fTTL, fExpAt, fForkedFrom, fForkOff, fForkSubOff, fRefCount, fSoftDel, fClosedByEp} {
		if _, ok := fields[absent]; ok {
			t.Errorf("optional field %q should be absent", absent)
		}
	}
	out, err := metaFromFields(in.Path, fields)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip mismatch:\n in: %+v\nout: %+v", in, out)
	}
	if out.TTLSeconds != nil || out.ExpiresAt != nil || out.ClosedBy != nil || out.ForkOffsetRequested != nil {
		t.Error("nil-able fields must stay nil")
	}
}

func TestMetaFromFieldsMissing(t *testing.T) {
	m, err := metaFromFields("/x", map[string]string{})
	if err != nil || m != nil {
		t.Errorf("empty hash: got (%v, %v), want (nil, nil)", m, err)
	}
}

func TestProducerStateRoundTrip(t *testing.T) {
	cases := []*store.ProducerState{
		{Epoch: 0, LastSeq: 0, LastUpdated: 0},
		{Epoch: 5, LastSeq: 123, LastUpdated: 1765000000},
		{Epoch: -1, LastSeq: -2, LastUpdated: -3}, // defensive: negative ints survive
	}
	for _, in := range cases {
		out, err := decodeProducerState(encodeProducerState(in))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(in, out) {
			t.Errorf("producer round trip: in %+v out %+v", in, out)
		}
	}
	for _, bad := range []string{"", "1:2", "a:b:c", "1:2:x"} {
		if _, err := decodeProducerState(bad); err == nil {
			t.Errorf("decodeProducerState(%q): want error", bad)
		}
	}
}

func TestProducersFromHash(t *testing.T) {
	got, err := producersFromHash(map[string]string{
		"p1": "1:5:1765000000",
		"p2": "2:0:1765000001",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]*store.ProducerState{
		"p1": {Epoch: 1, LastSeq: 5, LastUpdated: 1765000000},
		"p2": {Epoch: 2, LastSeq: 0, LastUpdated: 1765000001},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}

// TestMetaExpiryFidelity: a stored meta with TTL must still satisfy
// IsExpired/ConfigMatches identically after a round trip.
func TestMetaExpiryFidelity(t *testing.T) {
	ttl := int64(1)
	in := &store.StreamMetadata{
		Path:           "/v1/exp",
		ContentType:    "text/plain",
		CurrentOffset:  store.ZeroOffset,
		TTLSeconds:     &ttl,
		CreatedAt:      time.Now().Add(-10 * time.Second),
		LastAccessedAt: time.Now().Add(-10 * time.Second),
		Producers:      map[string]*store.ProducerState{},
	}
	out, err := metaFromFields(in.Path, metaToFields(in))
	if err != nil {
		t.Fatal(err)
	}
	if !out.IsExpired() {
		t.Error("expired stream must stay expired after round trip")
	}
	if !out.ConfigMatches(store.CreateOptions{ContentType: "text/plain", TTLSeconds: &ttl}) {
		t.Error("ConfigMatches must hold after round trip")
	}
}
