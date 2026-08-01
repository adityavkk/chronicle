package redis

import (
	"context"
	"errors"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

var (
	errPageReaderSessionClosed     = errors.New("page reader session is closed")
	errPageReaderSessionPathChange = errors.New("page reader session path changed")
)

type pageReaderSession struct {
	store  *Store
	path   string
	plan   *redisResponseReadPlan
	closed bool
}

// NewPageReaderSession returns a reader whose only retained state is one
// response's immutable fork plan. It opens no connections or goroutines.
func (s *Store) NewPageReaderSession(path string) store.PageReaderSession {
	return &pageReaderSession{store: s, path: path}
}

func (s *pageReaderSession) ReadPage(
	ctx context.Context,
	path string,
	offset store.Offset,
	opts store.ReadPageOptions,
) (store.ReadPage, error) {
	if s.closed {
		return store.ReadPage{}, errPageReaderSessionClosed
	}
	if path != s.path {
		return store.ReadPage{}, errPageReaderSessionPathChange
	}
	return s.store.readPage(ctx, path, offset, opts, &s.plan)
}

func (s *pageReaderSession) Close() {
	s.closed = true
	s.plan = nil
}
