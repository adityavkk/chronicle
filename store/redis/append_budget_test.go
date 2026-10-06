package redis

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/internal/redistest"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// TestAppendWarnsAtHalfTheRetryBudget pins the one signal the re-frame loop
// gives before it fails: an append that has spent half of its attempts on
// RETRY replies logs a warning once, naming the path, the attempts and the
// budget, whether it goes on to commit or to exhaust the budget. The tail is
// moved under the logged store by a second store, once before every script
// attempt, through the TripLog callback re-arming itself.
func TestAppendWarnsAtHalfTheRetryBudget(t *testing.T) {
	side := newTestStore(t)
	for _, tc := range []struct {
		name    string
		moves   int
		commits bool
	}{
		{"commits after half the budget", maxAppendRetries / 2, true},
		{"exhausts the budget", maxAppendRetries, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			client := goredis.NewClient(testClient.Options())
			t.Cleanup(func() { _ = client.Close() })
			rec := &redistest.TripLog{}
			client.AddHook(rec)
			s := New(client, Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
			path := testPath("budget")
			mustCreate(t, s, path, store.CreateOptions{ContentType: "text/plain"})
			plain := store.AppendOptions{ContentType: "text/plain"}

			moves := 0
			var move func()
			move = func() {
				if moves == tc.moves {
					return
				}
				moves++
				mustAppend(t, side, path, []byte("m"), plain)
				rec.BeforeAppendScript(move)
			}
			rec.BeforeAppendScript(move)
			hinted := plain
			hinted.TailHint = offsetPtr(store.Offset{})
			_, err := s.Append(path, []byte("x"), hinted)
			if tc.commits && err != nil {
				t.Fatalf("append after %d moves: %v", tc.moves, err)
			}
			if !tc.commits && (err == nil || !strings.Contains(err.Error(), "too much contention")) {
				t.Fatalf("append after %d moves: err=%v, want too much contention", tc.moves, err)
			}
			if got := rec.Retries(); got != tc.moves {
				t.Errorf("RETRY replies: %d, want %d", got, tc.moves)
			}
			const msg = "append has spent half its contention budget on RETRY"
			if n := strings.Count(logs.String(), msg); n != 1 {
				t.Errorf("warning logged %d times, want once:\n%s", n, logs.String())
			}
			for _, want := range []string{`"level":"WARN"`, `"path":"` + path + `"`, `"attempts":64`, `"budget":128`} {
				if !strings.Contains(logs.String(), want) {
					t.Errorf("warning lacks %s:\n%s", want, logs.String())
				}
			}
			if errors.Is(err, store.ErrStreamNotFound) {
				t.Fatal("stream vanished under the test")
			}
		})
	}
}
