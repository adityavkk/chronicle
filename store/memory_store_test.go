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

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
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

func TestMemoryStore_IncarnationChangesOnRecreateWithFrozenClock(t *testing.T) {
	clock := NewFakeClock(time.Unix(100, 0))
	s := NewMemoryStore(WithClock(clock))
	path := "/same-clock-incarnation"
	first, created, err := s.Create(path, CreateOptions{ContentType: "application/octet-stream"})
	if err != nil || !created {
		t.Fatalf("first Create: created=%v err=%v", created, err)
	}
	if first.Incarnation == "" {
		t.Fatal("first create has empty incarnation")
	}
	if err := s.Delete(path); err != nil {
		t.Fatal(err)
	}
	second, created, err := s.Create(path, CreateOptions{ContentType: "application/octet-stream"})
	if err != nil || !created {
		t.Fatalf("second Create: created=%v err=%v", created, err)
	}
	if second.Incarnation == "" || second.Incarnation == first.Incarnation {
		t.Fatalf("recreate incarnation = %q, first = %q", second.Incarnation, first.Incarnation)
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

func TestMemoryStore_NotificationRegistrationPrecedesRead(t *testing.T) {
	s := NewMemoryStore()
	if _, _, err := s.Create("/registered", CreateOptions{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	subscription, err := s.SubscribeNotifications(ctx, "/registered")
	if err != nil {
		t.Fatal(err)
	}

	initial, err := s.ReadPage(ctx, "/registered", NowOffset, ReadPageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append("/registered", []byte("wake"), AppendOptions{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	if _, err := subscription.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	page, err := s.ReadPage(ctx, "/registered", initial.Snapshot.Tail, ReadPageOptions{NoTouch: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 1 || string(page.Messages[0].Data) != "wake" {
		t.Fatalf("registered read = %+v", page)
	}
	if err := subscription.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := subscription.Wait(ctx); !errors.Is(err, ErrNotificationSubscriptionClosed) {
		t.Fatalf("Wait after Close = %v", err)
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

func TestStreamMetadataMaxTTLDoesNotOverflow(t *testing.T) {
	ttl := int64(1<<63 - 1)
	now := time.Unix(1_765_000_000, 123)
	meta := &StreamMetadata{
		TTLSeconds:     &ttl,
		LastAccessedAt: now,
	}
	if meta.IsExpiredAt(now.Add(100 * 365 * 24 * time.Hour)) {
		t.Fatal("max TTL overflowed into an expired deadline")
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

// ---- write fence (#183) ----

// fenceFixture is one write-fence scenario's store at a frozen clock, with
// the helpers store/redis/append_fence_test.go uses against live Redis, so
// both backends are held to the same steps.
type fenceFixture struct {
	t     *testing.T
	s     *MemoryStore
	clock *FakeClock
	path  string
}

// fence is a complete fence of subscription "sub" / incarnation "inc-a" at gen
// whose lease runs one minute past the frozen now.
func (f *fenceFixture) fence(gen int64, wake, holder string) auth.AppendFence {
	return auth.AppendFence{
		SubscriptionID:          "sub",
		SubscriptionIncarnation: "inc-a",
		Generation:              gen,
		WakeID:                  wake,
		Holder:                  holder,
		LeaseUntilNs:            f.clock.Now().Add(time.Minute).UnixNano(),
	}
}

func (f *fenceFixture) create(opts CreateOptions) {
	f.t.Helper()
	if _, _, err := f.s.Create(f.path, opts); err != nil {
		f.t.Fatalf("create: %v", err)
	}
}

func (f *fenceFixture) grant(fence auth.AppendFence) {
	f.t.Helper()
	if installed, err := f.s.GrantAppendFence(f.path, fence); err != nil || !installed {
		f.t.Fatalf("grant generation %d = installed:%t err:%v, want true/nil", fence.Generation, installed, err)
	}
}

func (f *fenceFixture) refuseGrant(label string, fence auth.AppendFence) {
	f.t.Helper()
	if _, err := f.s.GrantAppendFence(f.path, fence); !errors.Is(err, ErrAppendFenced) {
		f.t.Fatalf("%s = %v, want ErrAppendFenced", label, err)
	}
}

// fencedAppendAs is a fenced-class append naming producer at epoch == gen.
func (f *fenceFixture) fencedAppendAs(fence auth.AppendFence, producer string, seq int64) (AppendResult, error) {
	epoch := fence.Generation
	return f.s.Append(f.path, []byte("x"), AppendOptions{
		ContentType: "text/plain", Fence: &fence, ProducerId: producer, ProducerEpoch: &epoch, ProducerSeq: &seq,
	})
}

func (f *fenceFixture) fencedAppend(fence auth.AppendFence, seq int64) Offset {
	f.t.Helper()
	res, err := f.fencedAppendAs(fence, "p", seq)
	if err != nil {
		f.t.Fatalf("fenced append generation %d seq %d: %v", fence.Generation, seq, err)
	}
	return res.Offset
}

// openAppendAs is an open-class append naming producer at epoch.
func (f *fenceFixture) openAppendAs(producer string, epoch, seq int64) (AppendResult, error) {
	return f.s.Append(f.path, []byte("open"), AppendOptions{
		ContentType: "text/plain", ProducerId: producer, ProducerEpoch: &epoch, ProducerSeq: &seq,
	})
}

func (f *fenceFixture) seal(fence auth.AppendFence) SealResult {
	f.t.Helper()
	res, err := f.s.SealAppendFence(f.path, fence)
	if err != nil {
		f.t.Fatalf("seal generation %d: %v", fence.Generation, err)
	}
	return res
}

func (f *fenceFixture) assertSeal(label string, got SealResult, outcome SealOutcome, gen int64, off Offset) {
	f.t.Helper()
	if got.Outcome != outcome || got.Generation != gen || !got.FinalOffset.Equal(off) {
		f.t.Fatalf("%s = %+v, want %s/%d/%v", label, got, outcome, gen, off)
	}
}

// assertFenced asserts a refusal carrying the expected disclosure and no tail.
func (f *fenceFixture) assertFenced(label string, res AppendResult, err error, reason FenceReason, gen int64, holder string) {
	f.t.Helper()
	if !errors.Is(err, ErrAppendFenced) {
		f.t.Fatalf("%s = %v, want ErrAppendFenced", label, err)
	}
	if res.FenceReason != reason || res.FenceGeneration != gen || res.FenceHolder != holder || !res.Offset.Equal(Offset{}) {
		f.t.Fatalf("%s disclosure = (%q, %d, %q, tail %v), want (%q, %d, %q, no tail)",
			label, res.FenceReason, res.FenceGeneration, res.FenceHolder, res.Offset, reason, gen, holder)
	}
}

// assertSummary asserts the HEAD seal summary Get reports; gen 0 means none.
func (f *fenceFixture) assertSummary(label string, gen int64, off Offset) {
	f.t.Helper()
	meta, err := f.s.Get(f.path)
	if err != nil {
		f.t.Fatalf("%s: get: %v", label, err)
	}
	if meta.SealedGeneration != gen || (meta.SealedOffset == nil) != (gen == 0) ||
		(gen != 0 && !meta.SealedOffset.Equal(off)) {
		f.t.Fatalf("%s: seal summary = gen:%d off:%v, want %d/%v", label, meta.SealedGeneration, meta.SealedOffset, gen, off)
	}
}

// sealOf reads one authority's recorded seal, as the Redis tests HGET wfseal.
func (f *fenceFixture) sealOf(fence auth.AppendFence) WriteFenceSeal {
	return f.s.streams[f.path].seals[FenceAuthority(fence)]
}

// markerOf reads one authority's marker entry, as the Redis tests HGET the key.
func (f *fenceFixture) markerOf(fence auth.AppendFence) memoryMarker {
	return f.s.markers[f.path][FenceAuthority(fence)]
}

// reap drops one authority's marker as the Redis key TTL would.
func (f *fenceFixture) reap(fence auth.AppendFence) {
	delete(f.s.markers[f.path], FenceAuthority(fence))
}

// TestMemoryStoreWriteFenceParity pins the MemoryStore as the in-process
// oracle of the write-fence extension (#183, C.7): every outcome and check
// order of grant_append_fence.lua, revoke_append_fence.lua,
// seal_append_fence.lua and the fence rung of append.lua / close.lua, with
// EvaluateWriteFence as the one source of truth. Each row is a scenario
// store/redis/append_fence_test.go pins against live Redis, so the two
// backends are held to the same table; the reaper (the Redis marker key TTL)
// is simulated by dropping the marker.
func TestMemoryStoreWriteFenceParity(t *testing.T) {
	ttl := int64(60)
	plain := CreateOptions{ContentType: "text/plain"}
	fenced := CreateOptions{ContentType: "text/plain", WriteFence: true}
	fencedTTL := CreateOptions{ContentType: "text/plain", WriteFence: true, TTLSeconds: &ttl}

	// sealedAfterReap pins K.4 / INV-FENCE-06 for one way of writing the seal:
	// a delayed grant of a sealed generation is refused even after its marker
	// tombstone has been reaped, and so is its write.
	sealedAfterReap := func(seal func(f *fenceFixture, f1, f2 auth.AppendFence)) func(f *fenceFixture) {
		return func(f *fenceFixture) {
			f1, f2 := f.fence(1, "w_1", "worker-a"), f.fence(2, "w_2", "worker-b")
			f.grant(f1)
			f.fencedAppend(f1, 0)
			seal(f, f1, f2)
			f.reap(f1)
			f.refuseGrant("delayed grant of the sealed generation after reap", f1)
			res, err := f.fencedAppendAs(f1, "p", 1)
			f.assertFenced("sealed generation after reap", res, err, FenceSealed, 1, "")
			f.grant(f2)
			f.fencedAppend(f2, 0)
		}
	}

	tests := []struct {
		name string
		opts CreateOptions
		run  func(f *fenceFixture)
	}{
		{"incomplete and lapsed claims are refused before the ladder", plain, func(f *fenceFixture) {
			var none auth.AppendFence
			if _, err := f.s.Append(f.path, []byte("x"), AppendOptions{Fence: &none}); !errors.Is(err, ErrAppendFenced) {
				f.t.Fatalf("append with an incomplete fence = %v, want ErrAppendFenced", err)
			}
			if _, err := f.s.CloseStreamFenced(f.path, none); !errors.Is(err, ErrAppendFenced) {
				f.t.Fatalf("fenced close with an incomplete fence = %v, want ErrAppendFenced", err)
			}
			if _, err := f.s.CloseStreamWithProducer(f.path, CloseProducerOptions{ProducerId: "p", Fence: &none}); !errors.Is(err, ErrAppendFenced) {
				f.t.Fatalf("producer close with an incomplete fence = %v, want ErrAppendFenced", err)
			}
			f.refuseGrant("grant of an incomplete fence", none)
			if err := f.s.RevokeAppendFence(f.path, none); !errors.Is(err, ErrAppendFenced) {
				f.t.Fatalf("revoke of an incomplete fence = %v, want ErrAppendFenced", err)
			}
			if _, err := f.s.SealAppendFence(f.path, none); !errors.Is(err, ErrAppendFenced) {
				f.t.Fatalf("seal of an incomplete fence = %v, want ErrAppendFenced", err)
			}
			atNow := f.fence(1, "w_1", "worker-a")
			atNow.LeaseUntilNs = f.clock.Now().UnixNano()
			f.refuseGrant("grant with a lease at now", atNow)
			if f.s.Has(f.path) != true || len(f.s.markers[f.path]) != 0 {
				f.t.Fatal("a refused grant wrote a marker")
			}
		}},
		{"absent stream is not granted but its marker is tombstoned by a seal", plain, func(f *fenceFixture) {
			f1 := f.fence(1, "w_1", "worker-a")
			if installed, err := f.s.GrantAppendFence("/absent", f1); err != nil || installed {
				f.t.Fatalf("grant on an absent stream = installed:%t err:%v, want false/nil", installed, err)
			}
			if res, err := f.s.SealAppendFence("/absent", f1); err != nil || res.Outcome != SealNotFound {
				f.t.Fatalf("seal on an absent stream = %+v, %v; want notfound", res, err)
			}
			if m := f.s.markers["/absent"][FenceAuthority(f1)]; m.State != WriteFenceMarkerRevoked || m.Generation != 1 {
				f.t.Fatalf("seal on an absent stream left marker %+v, want a generation-1 tombstone", m.WriteFenceMarker)
			}
		}},
		{"legacy stream (no incarnation) is not granted, claim proceeds", plain, func(f *fenceFixture) {
			// The parity row of grant_append_fence.lua's legacy guard (and
			// TestAppendFenceGrantLegacyStreamNotInstalled on Redis): a stream
			// that predates the incarnation field cannot carry a marker —
			// (false, nil), never an error, so a claim linked to it still
			// succeeds uninstalled. MemoryStore always assigns incarnations,
			// so the shape is forged for the mirror.
			f.s.streams[f.path].metadata.Incarnation = ""
			f1 := f.fence(1, "w_1", "worker-a")
			if installed, err := f.s.GrantAppendFence(f.path, f1); err != nil || installed {
				f.t.Fatalf("grant on a legacy stream = installed:%t err:%v, want false/nil", installed, err)
			}
			if len(f.s.markers[f.path]) != 0 {
				f.t.Fatal("grant on a legacy stream wrote a marker")
			}
		}},
		{"claim lifecycle on an unfenced stream", plain, func(f *fenceFixture) {
			appendWith := func(fence auth.AppendFence) (AppendResult, error) {
				return f.s.Append(f.path, []byte("x"), AppendOptions{ContentType: "text/plain", Fence: &fence})
			}
			f1 := f.fence(1, "w_a", "worker-a")
			res, err := appendWith(f1)
			f.assertFenced("append before grant", res, err, FenceMarker, 0, "")
			f.grant(f1)
			if _, err := appendWith(f1); err != nil {
				f.t.Fatalf("append generation 1: %v", err)
			}
			if err := f.s.RevokeAppendFence(f.path, f1); err != nil {
				f.t.Fatalf("revoke generation 1: %v", err)
			}
			res, err = appendWith(f1)
			f.assertFenced("append after revoke", res, err, FenceMarker, 1, "")
			f.refuseGrant("same-generation regrant against the tombstone", f1)

			f2 := f.fence(2, "w_b", "worker-b")
			f.grant(f2)
			if err := f.s.RevokeAppendFence(f.path, f1); err != nil {
				f.t.Fatalf("delayed generation 1 revoke: %v", err)
			}
			if _, err := appendWith(f2); err != nil {
				f.t.Fatalf("append generation 2 after the stale revoke: %v", err)
			}
			res, err = appendWith(f1)
			f.assertFenced("append with the stale generation", res, err, FenceMarker, 2, "worker-b")

			f.clock.Advance(time.Minute) // now == lease: lapsed (strict >), like the stream expiry boundary
			res, err = appendWith(f2)
			f.assertFenced("append after the lease lapsed", res, err, FenceMarker, 2, "")
			f.grant(f.fence(2, "w_b", "worker-b")) // the heartbeat renewal from the advanced clock
			if _, err := appendWith(f2); err != nil {
				f.t.Fatalf("append after renewal: %v", err)
			}
			if _, err := f.s.CloseStreamFenced(f.path, f2); err != nil {
				f.t.Fatalf("fenced close: %v", err)
			}
		}},
		{"seal refuses the sealed generation and discloses the successor", fenced, func(f *fenceFixture) {
			f1 := f.fence(1, "w_1", "worker-a")
			f.grant(f1)
			tail1 := f.fencedAppend(f1, 0)
			f.assertSeal("seal", f.seal(f1), SealSealed, 1, tail1)
			res, err := f.fencedAppendAs(f1, "p", 1)
			f.assertFenced("append after seal", res, err, FenceSealed, 1, "")
			if tail, _ := f.s.GetCurrentOffset(f.path); !tail.Equal(tail1) {
				f.t.Fatalf("tail moved after a sealed write: %v != %v", tail, tail1)
			}
			if meta, err := f.s.Get(f.path); err != nil || !meta.WriteFence {
				f.t.Fatalf("metadata = %+v, %v; want write-fenced", meta, err)
			}
			f.assertSummary("after the seal", 1, tail1)
			f.refuseGrant("regrant of the sealed generation", f1)

			f2 := f.fence(2, "w_2", "worker-b")
			f.grant(f2)
			f.fencedAppend(f2, 0)
			res, err = f.fencedAppendAs(f1, "p", 1)
			f.assertFenced("sealed generation behind a successor", res, err, FenceSealed, 2, "worker-b")
		}},
		{"seal redelivery is idempotent before and after the tombstone reap", fenced, func(f *fenceFixture) {
			f1 := f.fence(1, "w_1", "worker-a")
			f.grant(f1)
			tail1 := f.fencedAppend(f1, 0)
			f.assertSeal("first seal", f.seal(f1), SealSealed, 1, tail1)
			f.assertSeal("redelivered seal, tombstone present", f.seal(f1), SealAlready, 1, tail1)
			f.reap(f1)
			f.assertSeal("redelivered seal, tombstone reaped", f.seal(f1), SealAlready, 1, tail1)
		}},
		{"stale seal mutates nothing", fenced, func(f *fenceFixture) {
			f2 := f.fence(2, "w_2", "worker-b")
			f.grant(f2)
			otherWake := f2
			otherWake.WakeID = "w_other"
			for i, tt := range []struct {
				name  string
				fence auth.AppendFence
			}{
				{"older generation", f.fence(1, "w_1", "worker-a")},
				{"same generation, different wake", otherWake},
			} {
				f.assertSeal(tt.name, f.seal(tt.fence), SealStale, 0, Offset{})
				if f.sealOf(tt.fence).Present {
					f.t.Fatalf("%s: a stale seal wrote a seal", tt.name)
				}
				f.fencedAppend(f2, int64(i)) // the live marker keeps accepting
			}
		}},
		{"unfenced stream seals tombstone only", plain, func(f *fenceFixture) {
			f1 := f.fence(1, "w_1", "worker-a")
			f.grant(f1)
			f.fencedAppend(f1, 0)
			f.assertSeal("seal on an unfenced stream", f.seal(f1), SealUnfenced, 0, Offset{})
			res, err := f.fencedAppendAs(f1, "p", 1)
			f.assertFenced("append after tombstone", res, err, FenceMarker, 1, "")
			if f.sealOf(f1).Present {
				f.t.Fatal("unfenced stream carries a seal")
			}
			f.assertSummary("unfenced", 0, Offset{})
			f.refuseGrant("regrant against the tombstone", f1)
			f.reap(f1)
			f.grant(f1) // the tombstone-only residual on unfenced streams
		}},
		{"supersession seals the predecessor at its last fenced offset", fenced, func(f *fenceFixture) {
			f1 := f.fence(1, "w_1", "worker-a")
			f.grant(f1)
			fencedTail := f.fencedAppend(f1, 0)
			open, err := f.s.Append(f.path, []byte("inbox"), AppendOptions{ContentType: "text/plain"})
			if err != nil || open.Offset.Equal(fencedTail) {
				f.t.Fatalf("open-class append = %+v, %v; want the tail moved", open, err)
			}
			f2 := f.fence(2, "w_2", "worker-b")
			f.grant(f2)
			if got, want := f.sealOf(f1), (WriteFenceSeal{Present: true, Generation: 1, WakeID: "w_1", Offset: fencedTail}); got != want {
				f.t.Fatalf("supersession seal = %+v, want %+v", got, want)
			}
			f.assertSummary("after supersession", 1, fencedTail)
			res, err := f.fencedAppendAs(f1, "p", 1)
			f.assertFenced("predecessor after supersession", res, err, FenceSealed, 2, "worker-b")
			f.fencedAppend(f2, 0)
		}},
		{"sealed by done is refused after the tombstone reap", fenced, sealedAfterReap(func(f *fenceFixture, f1, _ auth.AppendFence) {
			if res := f.seal(f1); res.Outcome != SealSealed {
				f.t.Fatalf("seal = %+v, want sealed", res)
			}
		})},
		{"sealed by supersession is refused after the tombstone reap", fenced, sealedAfterReap(func(f *fenceFixture, _, f2 auth.AppendFence) {
			f.grant(f2)
		})},
		{"renewal never shortens the lease", fenced, func(f *fenceFixture) {
			base := f.clock.Now()
			leaseAt := func(d time.Duration) auth.AppendFence {
				fence := f.fence(1, "w_1", "worker-a")
				fence.LeaseUntilNs = base.Add(d).UnixNano()
				return fence
			}
			assertMarker := func(label string, want auth.AppendFence) {
				f.t.Helper()
				m := f.markerOf(want)
				if m.LeaseUntilNs != want.LeaseUntilNs {
					f.t.Fatalf("%s: marker lease = %d, want %d", label, m.LeaseUntilNs, want.LeaseUntilNs)
				}
				// The retention runs from the retained lease, on the wall clock.
				wantExpiry := time.Now().Add(time.Duration(want.LeaseUntilNs - base.UnixNano())).Add(appendFenceRetention)
				if d := m.expiresAt.Sub(wantExpiry); d < -time.Second || d > time.Second {
					f.t.Fatalf("%s: marker expires at %v, want %v (the retained lease plus retention)", label, m.expiresAt, wantExpiry)
				}
			}
			long := leaseAt(time.Minute)
			f.grant(long)
			f.grant(leaseAt(30 * time.Second)) // delayed, older re-grant
			assertMarker("after the shorter re-grant", long)
			longer := leaseAt(90 * time.Second)
			f.grant(longer)
			assertMarker("after the longer re-grant", longer)
		}},
		{"seals are per authority", fenced, func(f *fenceFixture) {
			old := f.fence(5, "w_5", "worker-a")
			recreated := f.fence(1, "w_1", "worker-a")
			recreated.SubscriptionIncarnation = "inc-b"

			f.grant(old)
			oldTail := f.fencedAppend(old, 0)
			f.assertSeal("seal old incarnation", f.seal(old), SealSealed, 5, oldTail)

			f.grant(recreated) // generation 1 < 5, but its own namespace
			// Producer "p" keeps epoch 5 from the old incarnation: its stale-epoch
			// refusal at epoch 1 is the documented K.9 limitation, so a fresh id.
			res, err := f.fencedAppendAs(recreated, "p-recreated", 0)
			if err != nil {
				f.t.Fatalf("recreated incarnation append: %v", err)
			}
			newTail := res.Offset
			f.refuseGrant("old incarnation regrant", old)
			f.assertSummary("after the old seal", 5, oldTail)

			f.assertSeal("seal recreated incarnation", f.seal(recreated), SealSealed, 1, newTail)
			if got, want := f.sealOf(old), (WriteFenceSeal{Present: true, Generation: 5, WakeID: "w_5", Offset: oldTail}); got != want {
				f.t.Fatalf("old incarnation seal disturbed: %+v, want %+v", got, want)
			}
			if got, want := f.sealOf(recreated), (WriteFenceSeal{Present: true, Generation: 1, WakeID: "w_1", Offset: newTail}); got != want {
				f.t.Fatalf("recreated incarnation seal = %+v, want %+v", got, want)
			}
			f.assertSummary("after the newer seal", 1, newTail)
			res, err = f.fencedAppendAs(old, "p", 1)
			f.assertFenced("old incarnation after both seals", res, err, FenceSealed, 5, "")
			if m := f.markerOf(recreated); m.State != WriteFenceMarkerRevoked || m.expiresAt.After(time.Now().Add(appendFenceRetention)) {
				f.t.Fatalf("sealed marker = %+v expiring %v, want a tombstone within %v", m.WriteFenceMarker, m.expiresAt, appendFenceRetention)
			}
		}},
		{"recreated stream matches no marker", plain, func(f *fenceFixture) {
			f1 := f.fence(1, "w_1", "worker-a")
			f.grant(f1)
			if err := f.s.Delete(f.path); err != nil {
				f.t.Fatalf("delete: %v", err)
			}
			f.create(plain)
			res, err := f.s.Append(f.path, []byte("x"), AppendOptions{ContentType: "text/plain", Fence: &f1})
			f.assertFenced("append to the recreated stream", res, err, FenceMarker, 1, "worker-a")
		}},
		{"fork does not inherit the fence", fenced, func(f *fenceFixture) {
			f1 := f.fence(1, "w_1", "worker-a")
			f.grant(f1)
			f.fencedAppend(f1, 0)
			res, err := f.openAppendAs("p", 2, 0)
			f.assertFenced("bound producer on the source", res, err, FenceBound, 1, "")
			for _, tt := range []struct {
				name string
				opts CreateOptions
				want bool
			}{
				{"inherits nothing", CreateOptions{ForkedFrom: f.path}, false},
				{"declares its own", CreateOptions{ForkedFrom: f.path, WriteFence: true}, true},
			} {
				fork := f.path + "/" + tt.name
				meta, created, err := f.s.Create(fork, tt.opts)
				if err != nil || !created {
					f.t.Fatalf("%s: create fork = created:%t err:%v", tt.name, created, err)
				}
				if meta.WriteFence != tt.want {
					f.t.Fatalf("%s: fork WriteFence = %t, want %t", tt.name, meta.WriteFence, tt.want)
				}
				if st := f.s.streams[fork]; st.bound != nil || st.lastFencedOff != nil || st.seals != nil {
					f.t.Fatalf("%s: fork carries the source's fence state", tt.name)
				}
				epoch, seq := int64(2), int64(0)
				if _, err := f.s.Append(fork, []byte("open"), AppendOptions{ContentType: "text/plain", ProducerId: "p", ProducerEpoch: &epoch, ProducerSeq: &seq}); err != nil {
					f.t.Fatalf("%s: open-class append of the source's bound producer on the fork: %v", tt.name, err)
				}
			}
		}},
		{"rung precedes the TTL touch and the closed check", fencedTTL, func(f *fenceFixture) {
			created := f.s.streams[f.path].metadata.LastAccessedAt
			if _, err := f.s.CloseStream(f.path); err != nil {
				f.t.Fatalf("close: %v", err)
			}
			f.clock.Advance(10 * time.Second)
			f1 := f.fence(1, "w_1", "worker-a")
			res, err := f.fencedAppendAs(f1, "p", 0)
			f.assertFenced("ungranted claim on a closed stream", res, err, FenceMarker, 0, "")
			if got := f.s.streams[f.path].metadata.LastAccessedAt; !got.Equal(created) {
				f.t.Fatalf("a fence refusal touched the sliding TTL: %v != %v", got, created)
			}
			f.grant(f1)
			if _, err := f.fencedAppendAs(f1, "p", 0); !errors.Is(err, ErrStreamClosed) {
				f.t.Fatalf("accepted claim on a closed stream = %v, want ErrStreamClosed", err)
			}
			if got := f.s.streams[f.path].metadata.LastAccessedAt; !got.Equal(f.clock.Now()) {
				f.t.Fatalf("an accepted claim did not touch the sliding TTL before the closed check: %v", got)
			}
		}},
		{"fenced stream rung outcomes and the producer binding", fenced, func(f *fenceFixture) {
			f1 := f.fence(1, "w_1", "worker-a")
			f.grant(f1)
			res, err := f.s.Append(f.path, []byte("x"), AppendOptions{ContentType: "text/plain", Fence: &f1})
			f.assertFenced("fenced class without producer headers", res, err, FenceProducerRequired, 1, "worker-a")
			epoch, seq := int64(2), int64(0)
			res, err = f.s.Append(f.path, []byte("x"), AppendOptions{ContentType: "text/plain", Fence: &f1, ProducerId: "p", ProducerEpoch: &epoch, ProducerSeq: &seq})
			f.assertFenced("fenced class at the wrong epoch", res, err, FenceEpoch, 1, "worker-a")
			tail := f.fencedAppend(f1, 0)
			if st := f.s.streams[f.path]; st.bound["p"] != 1 || st.lastFencedOff == nil || !st.lastFencedOff.Equal(tail) {
				f.t.Fatalf("accepted fenced write recorded bound=%v lastFencedOff=%v, want p:1 / %v", st.bound, st.lastFencedOff, tail)
			}
			res, err = f.openAppendAs("p", 2, 0)
			f.assertFenced("open class naming the bound producer", res, err, FenceBound, 1, "")
			res, err = f.openAppendAs("p", 1, 0) // WF-17: the accepted tuple replayed without the credential
			f.assertFenced("open class replaying the bound tuple", res, err, FenceBound, 1, "")
			if _, err := f.openAppendAs("q", 1, 0); err != nil {
				f.t.Fatalf("unbound producer on the open class: %v", err)
			}
			if _, err := f.s.Append(f.path, []byte("x"), AppendOptions{ContentType: "text/plain"}); err != nil {
				f.t.Fatalf("open class without producer headers: %v", err)
			}
			if st := f.s.streams[f.path]; !st.lastFencedOff.Equal(tail) || len(st.bound) != 1 {
				f.t.Fatalf("open-class writes moved the fence state: lastFencedOff=%v bound=%v", st.lastFencedOff, st.bound)
			}
		}},
		{"fenced close binds and fixes the last fenced offset", fenced, func(f *fenceFixture) {
			f1 := f.fence(1, "w_1", "worker-a")
			f.grant(f1)
			open, err := f.s.Append(f.path, []byte("open"), AppendOptions{ContentType: "text/plain"})
			if err != nil {
				f.t.Fatalf("open-class append: %v", err)
			}
			cres, err := f.s.CloseStreamFenced(f.path, f1)
			if !errors.Is(err, ErrAppendFenced) || cres == nil || cres.FenceReason != FenceProducerRequired ||
				cres.FenceGeneration != 1 || cres.FenceHolder != "worker-a" || !cres.FinalOffset.Equal(Offset{}) {
				f.t.Fatalf("fenced close without producer headers = %+v, %v; want producer_required/1/worker-a, no tail", cres, err)
			}
			pres, err := f.s.CloseStreamWithProducer(f.path, CloseProducerOptions{ProducerId: "p", ProducerEpoch: 1, ProducerSeq: 0, Fence: &f1})
			if err != nil || !pres.StreamClosed || pres.ProducerResult != ProducerResultAccepted {
				f.t.Fatalf("fenced producer close = %+v, %v; want accepted and closed", pres, err)
			}
			if st := f.s.streams[f.path]; st.bound["p"] != 1 || st.lastFencedOff == nil || !st.lastFencedOff.Equal(open.Offset) {
				f.t.Fatalf("fenced close recorded bound=%v lastFencedOff=%v, want p:1 / %v", st.bound, st.lastFencedOff, open.Offset)
			}
			f.assertSeal("seal after the fenced close", f.seal(f1), SealSealed, 1, open.Offset)
			pres, err = f.s.CloseStreamWithProducer(f.path, CloseProducerOptions{ProducerId: "p", ProducerEpoch: 1, ProducerSeq: 0, Fence: &f1})
			if !errors.Is(err, ErrAppendFenced) || pres == nil || pres.FenceReason != FenceSealed || pres.FenceGeneration != 1 || pres.StreamClosed {
				f.t.Fatalf("sealed claim's close = %+v, %v; want sealed/1 before the closed check", pres, err)
			}
			if _, err := f.s.CloseStream(f.path); err != nil {
				f.t.Fatalf("open-class close on a fenced stream: %v", err)
			}
		}},
		{"reaped tombstone reads as absent", plain, func(f *fenceFixture) {
			f1 := f.fence(1, "w_1", "worker-a")
			f.grant(f1)
			if err := f.s.RevokeAppendFence(f.path, f1); err != nil {
				f.t.Fatalf("revoke: %v", err)
			}
			res, err := f.fencedAppendAs(f1, "p", 0)
			f.assertFenced("tombstone present", res, err, FenceMarker, 1, "")
			m := f.markerOf(f1)
			m.expiresAt = time.Now().Add(-time.Second)
			f.s.markers[f.path][FenceAuthority(f1)] = m
			res, err = f.fencedAppendAs(f1, "p", 0)
			f.assertFenced("tombstone reaped", res, err, FenceMarker, 0, "")
			if _, ok := f.s.markers[f.path][FenceAuthority(f1)]; ok {
				f.t.Fatal("a reaped marker was kept")
			}
			f.grant(f1) // on an unfenced stream the generation is grantable again
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := NewFakeClock(time.Unix(100, 0))
			f := &fenceFixture{t: t, s: NewMemoryStore(WithClock(clock)), clock: clock, path: "/fenced"}
			f.create(tt.opts)
			tt.run(f)
		})
	}
}

// TestMemoryStoreCreateWriteFenceConfigMatches pins C.1 on the MemoryStore:
// the write-fence opt-in is part of the idempotent-create comparison, so a
// re-PUT must agree with the stream's declaration in both directions, and a
// matching re-PUT reports the fence back.
func TestMemoryStoreCreateWriteFenceConfigMatches(t *testing.T) {
	tests := []struct {
		name         string
		first, again bool
		wantErr      error
	}{
		{"fenced then fenced matches", true, true, nil},
		{"plain then plain matches", false, false, nil},
		{"fenced then plain mismatches", true, false, ErrConfigMismatch},
		{"plain then fenced mismatches", false, true, ErrConfigMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemoryStore()
			if _, _, err := s.Create("/wf", CreateOptions{ContentType: "text/plain", WriteFence: tt.first}); err != nil {
				t.Fatalf("first create: %v", err)
			}
			meta, created, err := s.Create("/wf", CreateOptions{ContentType: "text/plain", WriteFence: tt.again})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("re-create = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && (created || meta.WriteFence != tt.first) {
				t.Fatalf("re-create = created:%t WriteFence:%t, want false/%t", created, meta.WriteFence, tt.first)
			}
		})
	}
}
