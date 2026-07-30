package main

import (
	"errors"
	"math/rand"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

type storeReadProbe struct {
	store.Store
	meta    *store.StreamMetadata
	metaErr error
}

func (s *storeReadProbe) GetCurrentOffset(string) (store.Offset, error) {
	return store.ZeroOffset, errors.New("tail unavailable")
}

func (s *storeReadProbe) Read(string, store.Offset) ([]store.Message, bool, error) {
	return []store.Message{{
		Data:   encodeFrame(1, 2, 8),
		Offset: store.Offset{ByteOffset: 8},
	}}, true, nil
}

func (s *storeReadProbe) Get(string) (*store.StreamMetadata, error) {
	return s.meta, s.metaErr
}

func TestStoreDoReadSkipsUnknownClosedState(t *testing.T) {
	rec := newRecorder()
	storeDoRead(
		&storeReadProbe{metaErr: errors.New("partitioned")},
		"/stream",
		1,
		rand.New(rand.NewSource(1)),
		rec,
	)
	if got := len(rec.history()); got != 0 {
		t.Fatalf("recorded %d read operations with unknown metadata, want 0", got)
	}
}

func TestStoreDoReadRecordsConfirmedClosedState(t *testing.T) {
	rec := newRecorder()
	storeDoRead(
		&storeReadProbe{meta: &store.StreamMetadata{
			CurrentOffset: store.Offset{ByteOffset: 8},
			Closed:        true,
		}},
		"/stream",
		1,
		rand.New(rand.NewSource(1)),
		rec,
	)
	history := rec.history()
	if len(history) != 1 {
		t.Fatalf("recorded %d read operations, want 1", len(history))
	}
	out := history[0].Output.(storeOutput)
	if !out.readClosed {
		t.Fatal("confirmed closed read was recorded as open")
	}
}
