package redis

import (
	"context"
	"errors"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func TestPageReaderSessionPreservesSoftDeletedAncestorAndSnapshotTail(t *testing.T) {
	subject := newTestStore(t)
	source := testPath("session-soft-source")
	mustCreate(t, subject, source, store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2]`),
	})
	fork := testPath("session-soft-fork")
	mustCreate(t, subject, fork, store.CreateOptions{ForkedFrom: source})
	session := subject.NewPageReaderSession(fork)
	defer session.Close()

	first, err := session.ReadPage(
		context.Background(),
		fork,
		store.ZeroOffset,
		store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Messages) != 1 || string(first.Messages[0].Data) != "1" || first.UpToDate {
		t.Fatalf("first page = %+v", first)
	}
	if err := subject.Delete(source); err != nil {
		t.Fatalf("soft-delete source: %v", err)
	}
	if _, err := subject.Append(fork, []byte(`[3]`), store.AppendOptions{ContentType: "application/json"}); err != nil {
		t.Fatalf("append after snapshot: %v", err)
	}

	second, err := session.ReadPage(
		context.Background(),
		fork,
		first.NextOffset,
		store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1, Snapshot: &first.Snapshot},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Messages) != 1 || string(second.Messages[0].Data) != "2" || !second.UpToDate {
		t.Fatalf("second page crossed ordering or captured tail = %+v", second)
	}
}

func TestPageReaderSessionFencesPlannedAncestorIncarnation(t *testing.T) {
	subject := newTestStore(t)
	source := testPath("session-fence-source")
	mustCreate(t, subject, source, store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2]`),
	})
	fork := testPath("session-fence-fork")
	mustCreate(t, subject, fork, store.CreateOptions{ForkedFrom: source})
	session := subject.NewPageReaderSession(fork)
	defer session.Close()
	first, err := session.ReadPage(
		context.Background(),
		fork,
		store.ZeroOffset,
		store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := subject.client.HSet(context.Background(), metaKey(source), fIncarnation, "replacement").Err(); err != nil {
		t.Fatal(err)
	}
	_, err = session.ReadPage(
		context.Background(),
		fork,
		first.NextOffset,
		store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1, Snapshot: &first.Snapshot},
	)
	if !errors.Is(err, store.ErrReadSnapshotChanged) {
		t.Fatalf("ancestor recreation error = %v, want snapshot changed", err)
	}
}

func TestPageReaderSessionMissingPlannedAncestorFailsLoudly(t *testing.T) {
	subject := newTestStore(t)
	source := testPath("session-missing-source")
	mustCreate(t, subject, source, store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2]`),
	})
	fork := testPath("session-missing-fork")
	mustCreate(t, subject, fork, store.CreateOptions{ForkedFrom: source})
	session := subject.NewPageReaderSession(fork)
	defer session.Close()
	first, err := session.ReadPage(
		context.Background(),
		fork,
		store.ZeroOffset,
		store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := subject.client.Del(context.Background(), metaKey(source)).Err(); err != nil {
		t.Fatal(err)
	}
	_, err = session.ReadPage(
		context.Background(),
		fork,
		first.NextOffset,
		store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1, Snapshot: &first.Snapshot},
	)
	if !errors.Is(err, store.ErrReadDataMissing) {
		t.Fatalf("missing planned ancestor error = %v, want ErrReadDataMissing", err)
	}
}
