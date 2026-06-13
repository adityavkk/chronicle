// Behavioral assertions ported from the Durable Streams reference Caddy
// plugin (packages/caddy-plugin/store @ 82f9963): the MemoryStore cases in
// expiry_test.go plus the store-contract cases from file_store_test.go
// (which exercise the same Store interface), run here against MemoryStore.
package store

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryStore_CreateAndGet(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	// Create a stream
	opts := CreateOptions{
		ContentType: "application/json",
	}
	meta, created, err := s.Create("/test/stream", opts)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if !created {
		t.Error("expected created=true for new stream")
	}
	if meta.Path != "/test/stream" {
		t.Errorf("path mismatch: %q", meta.Path)
	}
	if meta.ContentType != "application/json" {
		t.Errorf("content type mismatch: %q", meta.ContentType)
	}

	// Get it back
	gotMeta, err := s.Get("/test/stream")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if gotMeta.Path != meta.Path {
		t.Errorf("path mismatch on get")
	}

	// Has should return true
	if !s.Has("/test/stream") {
		t.Error("Has returned false for existing stream")
	}

	// Get nonexistent
	_, err = s.Get("/nonexistent")
	if !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("expected ErrStreamNotFound, got %v", err)
	}
}

func TestMemoryStore_CreateIdempotent(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	opts := CreateOptions{ContentType: "text/plain"}

	// First create
	_, created1, err := s.Create("/test", opts)
	if err != nil {
		t.Fatalf("first Create failed: %v", err)
	}
	if !created1 {
		t.Error("first create should return created=true")
	}

	// Second create with same config
	_, created2, err := s.Create("/test", opts)
	if err != nil {
		t.Fatalf("second Create failed: %v", err)
	}
	if created2 {
		t.Error("idempotent create should return created=false")
	}

	// Create with different config
	opts.ContentType = "application/json"
	_, _, err = s.Create("/test", opts)
	if !errors.Is(err, ErrConfigMismatch) {
		t.Errorf("expected ErrConfigMismatch, got %v", err)
	}
}

func TestMemoryStore_AppendAndRead(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	// Create stream
	_, _, err := s.Create("/test", CreateOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Append
	data := []byte("hello world")
	result, err := s.Append("/test", data, AppendOptions{})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if result.Offset.ByteOffset == 0 {
		t.Error("offset should be non-zero after append")
	}

	// Read from start
	messages, upToDate, err := s.Read("/test", ZeroOffset)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(messages))
	}
	if !bytes.Equal(messages[0].Data, data) {
		t.Errorf("data mismatch")
	}
	if !upToDate {
		t.Error("should be up to date")
	}

	// Read from tail (should be empty)
	messages, upToDate, err = s.Read("/test", result.Offset)
	if err != nil {
		t.Fatalf("Read from tail failed: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("expected 0 messages at tail, got %d", len(messages))
	}
	if !upToDate {
		t.Error("should be up to date at tail")
	}
}

func TestMemoryStore_AppendJSON(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	// Create JSON stream
	_, _, err := s.Create("/json", CreateOptions{ContentType: "application/json"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Append array (should be flattened)
	_, err = s.Append("/json", []byte(`[{"id":1},{"id":2}]`), AppendOptions{})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Read back
	messages, _, err := s.Read("/json", ZeroOffset)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(messages) != 2 {
		t.Errorf("expected 2 messages (flattened array), got %d", len(messages))
	}

	// Format response
	resp, err := s.FormatResponse("/json", messages)
	if err != nil {
		t.Fatalf("FormatResponse failed: %v", err)
	}
	if string(resp) != `[{"id":1},{"id":2}]` {
		t.Errorf("formatted response mismatch: %s", resp)
	}
}

func TestMemoryStore_Delete(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	// Create and then delete
	_, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"})

	if err := s.Delete("/test"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if s.Has("/test") {
		t.Error("stream still exists after delete")
	}

	// Delete nonexistent
	err := s.Delete("/nonexistent")
	if !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("expected ErrStreamNotFound, got %v", err)
	}
}

func TestMemoryStore_SequenceConflict(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	_, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"})

	// First append with seq
	_, err := s.Append("/test", []byte("a"), AppendOptions{Seq: "seq1"})
	if err != nil {
		t.Fatalf("first append failed: %v", err)
	}

	// Second append with same seq should fail
	_, err = s.Append("/test", []byte("b"), AppendOptions{Seq: "seq1"})
	if !errors.Is(err, ErrSequenceConflict) {
		t.Errorf("expected ErrSequenceConflict, got %v", err)
	}

	// Append with higher seq should work
	_, err = s.Append("/test", []byte("c"), AppendOptions{Seq: "seq2"})
	if err != nil {
		t.Fatalf("third append failed: %v", err)
	}
}

func TestMemoryStore_ContentTypeMismatch(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	_, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"})

	// Append with wrong content type
	_, err := s.Append("/test", []byte("data"), AppendOptions{ContentType: "application/json"})
	if !errors.Is(err, ErrContentTypeMismatch) {
		t.Errorf("expected ErrContentTypeMismatch, got %v", err)
	}
}

func TestMemoryStore_LongPoll(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	_, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"})

	// Start long-poll
	done := make(chan struct{})
	var messages []Message
	var timedOut bool
	go func() {
		messages, timedOut, _, _ = s.WaitForMessages(context.Background(), "/test", ZeroOffset, 5*time.Second)
		close(done)
	}()

	// Wait a bit then append
	time.Sleep(100 * time.Millisecond)
	if _, err := s.Append("/test", []byte("wakeup"), AppendOptions{}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Wait for long-poll to complete
	select {
	case <-done:
		if timedOut {
			t.Error("long-poll should not have timed out")
		}
		if len(messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(messages))
		}
	case <-time.After(2 * time.Second):
		t.Error("long-poll did not complete in time")
	}
}

func TestMemoryStore_LongPollTimeout(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	_, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"})
	if _, err := s.Append("/test", []byte("initial"), AppendOptions{}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	offset, _ := s.GetCurrentOffset("/test")

	// Long-poll at tail with short timeout
	messages, timedOut, _, err := s.WaitForMessages(context.Background(), "/test", offset, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitForMessages failed: %v", err)
	}
	if !timedOut {
		t.Error("expected timeout")
	}
	if len(messages) != 0 {
		t.Errorf("expected 0 messages on timeout, got %d", len(messages))
	}
}

func TestMemoryStore_LongPollStreamClosed(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	_, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"})

	// Waiter at tail; closing the stream must wake it with streamClosed=true.
	done := make(chan struct{})
	var streamClosed bool
	go func() {
		_, _, streamClosed, _ = s.WaitForMessages(context.Background(), "/test", ZeroOffset, 5*time.Second)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	if _, err := s.CloseStream("/test"); err != nil {
		t.Fatalf("CloseStream failed: %v", err)
	}

	select {
	case <-done:
		if !streamClosed {
			t.Error("expected streamClosed=true after close during wait")
		}
	case <-time.After(2 * time.Second):
		t.Error("long-poll did not complete in time")
	}
}

func TestMemoryStore_InitialData(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	// Create with initial data
	meta, _, err := s.Create("/test", CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte("initial content"),
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if meta.CurrentOffset.ByteOffset == 0 {
		t.Error("offset should be non-zero with initial data")
	}

	// Read back
	messages, _, err := s.Read("/test", ZeroOffset)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(messages))
	}
	if !bytes.Equal(messages[0].Data, []byte("initial content")) {
		t.Error("initial data mismatch")
	}
}

func TestMemoryStore_StreamClosure(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	// Create stream
	_, _, err := s.Create("/test", CreateOptions{
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Append some data
	_, err = s.Append("/test", []byte("data"), AppendOptions{})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Close the stream
	closeResult, err := s.CloseStream("/test")
	if err != nil {
		t.Fatalf("CloseStream failed: %v", err)
	}
	if closeResult.AlreadyClosed {
		t.Error("stream should not be already closed")
	}

	// Verify stream is closed
	meta, err := s.Get("/test")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !meta.Closed {
		t.Error("stream should be closed")
	}

	// Try to append to closed stream - should fail
	_, err = s.Append("/test", []byte("more data"), AppendOptions{})
	if !errors.Is(err, ErrStreamClosed) {
		t.Errorf("expected ErrStreamClosed, got: %v", err)
	}

	// Close again (idempotent)
	closeResult, err = s.CloseStream("/test")
	if err != nil {
		t.Fatalf("second CloseStream failed: %v", err)
	}
	if !closeResult.AlreadyClosed {
		t.Error("stream should be already closed")
	}
}

func TestMemoryStore_CreateClosed(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	// Create stream in closed state
	meta, _, err := s.Create("/closed", CreateOptions{
		ContentType: "text/plain",
		Closed:      true,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if !meta.Closed {
		t.Error("stream should be created closed")
	}

	// Append should fail
	_, err = s.Append("/closed", []byte("data"), AppendOptions{})
	if !errors.Is(err, ErrStreamClosed) {
		t.Errorf("expected ErrStreamClosed, got: %v", err)
	}
}

func TestMemoryStore_AppendAndClose(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	// Create stream
	_, _, err := s.Create("/test", CreateOptions{
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Append with close
	result, err := s.Append("/test", []byte("final"), AppendOptions{
		Close: true,
	})
	if err != nil {
		t.Fatalf("Append with close failed: %v", err)
	}
	if !result.StreamClosed {
		t.Error("StreamClosed should be true")
	}

	// Verify stream is closed
	meta, err := s.Get("/test")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !meta.Closed {
		t.Error("stream should be closed after append with close")
	}

	// Read back data
	messages, _, err := s.Read("/test", ZeroOffset)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(messages))
	}
	if !bytes.Equal(messages[0].Data, []byte("final")) {
		t.Error("data mismatch")
	}
}

func TestMemoryStore_ProducerFlow(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	_, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"})

	epoch := int64(1)
	producerOpts := func(seq int64) AppendOptions {
		seqCopy := seq
		return AppendOptions{ProducerId: "p1", ProducerEpoch: &epoch, ProducerSeq: &seqCopy}
	}

	// First append must be seq=0
	result, err := s.Append("/test", []byte("a"), producerOpts(0))
	if err != nil {
		t.Fatalf("seq=0 append failed: %v", err)
	}
	if result.ProducerResult != ProducerResultAccepted {
		t.Errorf("expected accepted, got %v", result.ProducerResult)
	}

	// Next seq accepted
	result, err = s.Append("/test", []byte("b"), producerOpts(1))
	if err != nil {
		t.Fatalf("seq=1 append failed: %v", err)
	}
	if result.LastSeq != 1 {
		t.Errorf("expected LastSeq=1, got %d", result.LastSeq)
	}

	// Retransmit of seq=0 is a duplicate; no data is appended and LastSeq
	// reports the highest accepted seq.
	result, err = s.Append("/test", []byte("a"), producerOpts(0))
	if err != nil {
		t.Fatalf("duplicate append failed: %v", err)
	}
	if result.ProducerResult != ProducerResultDuplicate {
		t.Errorf("expected duplicate, got %v", result.ProducerResult)
	}
	if result.LastSeq != 1 {
		t.Errorf("expected LastSeq=1 on duplicate, got %d", result.LastSeq)
	}

	// Sequence gap rejected with expected/received
	result, err = s.Append("/test", []byte("d"), producerOpts(5))
	if !errors.Is(err, ErrProducerSeqGap) {
		t.Fatalf("expected ErrProducerSeqGap, got %v", err)
	}
	if result.ExpectedSeq != 2 || result.ReceivedSeq != 5 {
		t.Errorf("expected gap 2/5, got %d/%d", result.ExpectedSeq, result.ReceivedSeq)
	}

	// Stale epoch rejected with current epoch
	staleEpoch := int64(0)
	staleSeq := int64(0)
	result, err = s.Append("/test", []byte("e"), AppendOptions{ProducerId: "p1", ProducerEpoch: &staleEpoch, ProducerSeq: &staleSeq})
	if !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("expected ErrStaleEpoch, got %v", err)
	}
	if result.CurrentEpoch != epoch {
		t.Errorf("expected CurrentEpoch=%d, got %d", epoch, result.CurrentEpoch)
	}

	// Only two messages should have landed
	messages, _, err := s.Read("/test", ZeroOffset)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(messages) != 2 {
		t.Errorf("expected 2 messages, got %d", len(messages))
	}

	// Partial producer headers rejected
	_, err = s.Append("/test", []byte("f"), AppendOptions{ProducerId: "p1"})
	if !errors.Is(err, ErrPartialProducer) {
		t.Errorf("expected ErrPartialProducer, got %v", err)
	}
}

func TestMemoryStore_CloseStreamWithProducer(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	_, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"})

	opts := CloseProducerOptions{ProducerId: "p1", ProducerEpoch: 1, ProducerSeq: 0}
	result, err := s.CloseStreamWithProducer("/test", opts)
	if err != nil {
		t.Fatalf("CloseStreamWithProducer failed: %v", err)
	}
	if !result.StreamClosed || result.AlreadyClosed {
		t.Errorf("expected closed now, got StreamClosed=%v AlreadyClosed=%v", result.StreamClosed, result.AlreadyClosed)
	}

	// Same producer tuple again: idempotent duplicate
	result, err = s.CloseStreamWithProducer("/test", opts)
	if err != nil {
		t.Fatalf("duplicate close failed: %v", err)
	}
	if result.ProducerResult != ProducerResultDuplicate || !result.AlreadyClosed {
		t.Errorf("expected duplicate of closing request, got %+v", result)
	}

	// Different producer tuple: stream is closed
	_, err = s.CloseStreamWithProducer("/test", CloseProducerOptions{ProducerId: "p2", ProducerEpoch: 1, ProducerSeq: 0})
	if !errors.Is(err, ErrStreamClosed) {
		t.Errorf("expected ErrStreamClosed, got %v", err)
	}
}

func TestMemoryStore_ForkLifecycle(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	// Source with data
	_, _, err := s.Create("/src", CreateOptions{ContentType: "text/plain", InitialData: []byte("shared")})
	if err != nil {
		t.Fatalf("Create source failed: %v", err)
	}

	// Fork at head (default)
	forkMeta, created, err := s.Create("/fork", CreateOptions{ContentType: "text/plain", ForkedFrom: "/src"})
	if err != nil {
		t.Fatalf("Create fork failed: %v", err)
	}
	if !created {
		t.Error("fork should be newly created")
	}
	if forkMeta.ForkedFrom != "/src" {
		t.Errorf("fork metadata: ForkedFrom = %q", forkMeta.ForkedFrom)
	}

	// Fork inherits source data before the fork point
	messages, _, err := s.Read("/fork", ZeroOffset)
	if err != nil {
		t.Fatalf("Read fork failed: %v", err)
	}
	if len(messages) != 1 || !bytes.Equal(messages[0].Data, []byte("shared")) {
		t.Errorf("fork should inherit source data, got %d messages", len(messages))
	}

	// Source appends after fork creation are NOT visible to the fork
	if _, err := s.Append("/src", []byte(" after"), AppendOptions{}); err != nil {
		t.Fatalf("Append to source failed: %v", err)
	}
	messages, _, _ = s.Read("/fork", ZeroOffset)
	if len(messages) != 1 {
		t.Errorf("fork should not see post-fork source appends, got %d messages", len(messages))
	}

	// Deleting the source with a live fork soft-deletes it
	if err := s.Delete("/src"); err != nil {
		t.Fatalf("Delete source failed: %v", err)
	}
	if _, err := s.Get("/src"); !errors.Is(err, ErrStreamSoftDeleted) {
		t.Errorf("expected ErrStreamSoftDeleted on source Get, got %v", err)
	}

	// Fork still reads through the soft-deleted source
	messages, _, err = s.Read("/fork", ZeroOffset)
	if err != nil || len(messages) != 1 {
		t.Errorf("fork read through soft-deleted source failed: %v, %d messages", err, len(messages))
	}

	// Deleting the last fork cascades to the soft-deleted source
	if err := s.Delete("/fork"); err != nil {
		t.Fatalf("Delete fork failed: %v", err)
	}
	if s.Has("/src") {
		t.Error("source should be fully deleted after last fork removed")
	}
	if _, err := s.Get("/src"); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("expected ErrStreamNotFound after cascade, got %v", err)
	}
}

// --- Expiry tests (ported from upstream expiry_test.go) ---

func TestStreamMetadata_IsExpired_ExpiresAt(t *testing.T) {
	// Stream with ExpiresAt in the past
	pastTime := time.Now().Add(-1 * time.Hour)
	meta := &StreamMetadata{
		Path:      "/test",
		ExpiresAt: &pastTime,
		CreatedAt: time.Now().Add(-2 * time.Hour),
	}
	if !meta.IsExpired() {
		t.Error("stream with past ExpiresAt should be expired")
	}

	// Stream with ExpiresAt in the future
	futureTime := time.Now().Add(1 * time.Hour)
	meta.ExpiresAt = &futureTime
	if meta.IsExpired() {
		t.Error("stream with future ExpiresAt should not be expired")
	}
}

func TestStreamMetadata_IsExpired_TTL(t *testing.T) {
	// Stream with TTL that has passed
	ttl := int64(1) // 1 second
	past := time.Now().Add(-2 * time.Second)
	meta := &StreamMetadata{
		Path:           "/test",
		TTLSeconds:     &ttl,
		CreatedAt:      past,
		LastAccessedAt: past, // Last accessed 2 seconds ago — TTL has expired
	}
	if !meta.IsExpired() {
		t.Error("stream with expired TTL should be expired")
	}

	// Stream with TTL that hasn't passed
	now := time.Now()
	meta.CreatedAt = now      // Just created
	meta.LastAccessedAt = now // Just accessed
	if meta.IsExpired() {
		t.Error("stream with non-expired TTL should not be expired")
	}
}

func TestStreamMetadata_IsExpired_NoExpiry(t *testing.T) {
	// Stream without any expiry
	meta := &StreamMetadata{
		Path:      "/test",
		CreatedAt: time.Now().Add(-24 * time.Hour),
	}
	if meta.IsExpired() {
		t.Error("stream without expiry settings should never expire")
	}
}

func TestMemoryStore_ExpiryOnGet(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	// Create a stream with very short TTL
	ttl := int64(1) // 1 second
	_, _, err := s.Create("/expiring", CreateOptions{
		ContentType: "text/plain",
		TTLSeconds:  &ttl,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Should be accessible immediately
	_, err = s.Get("/expiring")
	if err != nil {
		t.Fatalf("Get failed immediately after create: %v", err)
	}

	// Wait for TTL to expire
	time.Sleep(1100 * time.Millisecond)

	// Should now return not found
	_, err = s.Get("/expiring")
	if !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("expected ErrStreamNotFound after expiry, got %v", err)
	}
}

func TestMemoryStore_ExpiryOnHas(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	ttl := int64(1)
	_, _, _ = s.Create("/expiring", CreateOptions{
		ContentType: "text/plain",
		TTLSeconds:  &ttl,
	})

	if !s.Has("/expiring") {
		t.Error("Has should return true before expiry")
	}

	time.Sleep(1100 * time.Millisecond)

	if s.Has("/expiring") {
		t.Error("Has should return false after expiry")
	}
}

func TestMemoryStore_ExpiryOnAppend(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	ttl := int64(1)
	_, _, _ = s.Create("/expiring", CreateOptions{
		ContentType: "text/plain",
		TTLSeconds:  &ttl,
	})

	// Should be able to append immediately
	_, err := s.Append("/expiring", []byte("data"), AppendOptions{})
	if err != nil {
		t.Fatalf("Append failed before expiry: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)

	// Should fail after expiry
	_, err = s.Append("/expiring", []byte("more data"), AppendOptions{})
	if !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("expected ErrStreamNotFound on append after expiry, got %v", err)
	}
}

func TestMemoryStore_ExpiryOnRead(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	ttl := int64(1)
	_, _, _ = s.Create("/expiring", CreateOptions{
		ContentType: "text/plain",
		TTLSeconds:  &ttl,
	})
	if _, err := s.Append("/expiring", []byte("data"), AppendOptions{}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Should be able to read immediately
	_, _, err := s.Read("/expiring", ZeroOffset)
	if err != nil {
		t.Fatalf("Read failed before expiry: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)

	// Should fail after expiry
	_, _, err = s.Read("/expiring", ZeroOffset)
	if !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("expected ErrStreamNotFound on read after expiry, got %v", err)
	}
}

func TestMemoryStore_ExpiresAtExpiry(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	// Create a stream that expires 1 second from now
	expiresAt := time.Now().Add(1 * time.Second)
	_, _, err := s.Create("/expiring", CreateOptions{
		ContentType: "text/plain",
		ExpiresAt:   &expiresAt,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Should be accessible immediately
	if !s.Has("/expiring") {
		t.Error("stream should exist before expiry")
	}

	time.Sleep(1100 * time.Millisecond)

	// Should be expired
	if s.Has("/expiring") {
		t.Error("stream should not exist after expiry")
	}
}
