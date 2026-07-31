package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func TestReadPageNonExpiringCommandsAreReadOnly(t *testing.T) {
	subject := newTestStore(t)
	path := testPath("read-only-commands")
	mustCreate(t, subject, path, store.CreateOptions{
		ContentType: "text/plain",
	})
	client := concreteTestClient(t, subject)

	before := keyPTTLs(t, client, path)
	const reads = 16
	lines := monitorRedis(t, client, func() {
		var group sync.WaitGroup
		group.Add(reads)
		for range reads {
			go func() {
				defer group.Done()
				page, err := subject.ReadPage(
					context.Background(),
					path,
					store.ZeroOffset,
					store.ReadPageOptions{},
				)
				if err != nil {
					t.Errorf("ReadPage: %v", err)
					return
				}
				if len(page.Messages) != 0 || !page.UpToDate {
					t.Errorf("ReadPage result = %+v", page)
				}
			}()
		}
		group.Wait()
	})

	assertNoMonitoredCommands(t, lines, path, "hset", "hsetnx", "pexpire", "persist")
	if got := countMonitoredCommand(lines, "hgetall", metaKey(path)); got != reads {
		t.Fatalf("metadata HGETALL calls = %d, want one per read (%d)\n%s", got, reads, strings.Join(lines, "\n"))
	}
	if got := countMonitoredCommand(lines, "hgetall", prodKey(path)); got != 0 {
		t.Fatalf("producer HGETALL calls = %d, want 0\n%s", got, strings.Join(lines, "\n"))
	}
	after := keyPTTLs(t, client, path)
	if fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("read changed key expiry: before=%v after=%v", before, after)
	}
}

func TestReadPageRootOwnedCommandShape(t *testing.T) {
	subject := newTestStore(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name   string
		setup  func(*testing.T) (string, store.Offset, store.ReadPageOptions)
		frames int
	}{
		{
			name: "unforked first page",
			setup: func(t *testing.T) (string, store.Offset, store.ReadPageOptions) {
				path := testPath("root-command-first")
				mustCreate(t, subject, path, store.CreateOptions{
					ContentType: "application/octet-stream",
					InitialData: []byte("frame"),
				})
				return path, store.ZeroOffset, store.ReadPageOptions{}
			},
			frames: 1,
		},
		{
			name: "unforked continuation",
			setup: func(t *testing.T) (string, store.Offset, store.ReadPageOptions) {
				path := testPath("root-command-continuation")
				mustCreate(t, subject, path, store.CreateOptions{
					ContentType: "application/json",
					InitialData: []byte(`[1,2]`),
				})
				first, err := subject.ReadPage(ctx, path, store.ZeroOffset, store.ReadPageOptions{
					TargetBytes: 1,
					MaxFrames:   1,
				})
				if err != nil {
					t.Fatal(err)
				}
				return path, first.NextOffset, store.ReadPageOptions{
					TargetBytes: 1,
					MaxFrames:   1,
					Snapshot:    &first.Snapshot,
				}
			},
			frames: 1,
		},
		{
			name: "fork root-owned first page",
			setup: func(t *testing.T) (string, store.Offset, store.ReadPageOptions) {
				sourcePath := testPath("root-command-fork-source")
				source := mustCreate(t, subject, sourcePath, store.CreateOptions{
					ContentType: "application/octet-stream",
					InitialData: []byte("source"),
				})
				forkPath := testPath("root-command-fork")
				forkOffset := source.CurrentOffset
				mustCreate(t, subject, forkPath, store.CreateOptions{
					ContentType: "application/octet-stream",
					ForkedFrom:  sourcePath,
					ForkOffset:  &forkOffset,
					InitialData: []byte("own"),
				})
				return forkPath, forkOffset, store.ReadPageOptions{}
			},
			frames: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, offset, opts := tc.setup(t)
			// Load read.lua before the measured EVALSHA so NOSCRIPT recovery does
			// not add an EVAL to the command trace.
			if _, err := subject.ReadPage(ctx, path, store.NowOffset, store.ReadPageOptions{NoTouch: true}); err != nil {
				t.Fatal(err)
			}

			var page store.ReadPage
			lines := monitorRedis(t, concreteTestClient(t, subject), func() {
				var err error
				page, err = subject.ReadPage(ctx, path, offset, opts)
				if err != nil {
					t.Fatal(err)
				}
			})
			if len(page.Messages) != tc.frames || page.Stats.RedisScriptInvokes != 1 {
				t.Fatalf("page messages/scripts = %d/%d, want %d/1", len(page.Messages), page.Stats.RedisScriptInvokes, tc.frames)
			}
			if got := countMonitoredCommand(lines, "evalsha", metaKey(path)); got != 1 {
				t.Fatalf("EVALSHA calls = %d, want 1\n%s", got, strings.Join(lines, "\n"))
			}
			if got := countMonitoredCommand(lines, "hgetall", metaKey(path)); got != 1 {
				t.Fatalf("metadata HGETALL calls = %d, want 1\n%s", got, strings.Join(lines, "\n"))
			}
			if got := countMonitoredCommand(lines, "zrangebylex", msgKey(path)); got != 1 {
				t.Fatalf("ZRANGEBYLEX calls = %d, want 1\n%s", got, strings.Join(lines, "\n"))
			}
			if got := countMonitoredCommand(lines, "hgetall", prodKey(path)); got != 0 {
				t.Fatalf("producer HGETALL calls = %d, want 0\n%s", got, strings.Join(lines, "\n"))
			}
			assertNoMonitoredCommands(
				t,
				lines,
				path,
				"hset",
				"hsetnx",
				"pexpire",
				"persist",
				"del",
				"zadd",
			)
		})
	}
}

func TestReadPageInheritedForkKeepsTwoSegmentCommands(t *testing.T) {
	subject := newTestStore(t)
	ctx := context.Background()
	sourcePath := testPath("inherited-command-source")
	source := mustCreate(t, subject, sourcePath, store.CreateOptions{
		ContentType: "application/octet-stream",
		InitialData: []byte("source"),
	})
	forkPath := testPath("inherited-command-fork")
	forkOffset := source.CurrentOffset
	mustCreate(t, subject, forkPath, store.CreateOptions{
		ContentType: "application/octet-stream",
		ForkedFrom:  sourcePath,
		ForkOffset:  &forkOffset,
	})
	// Load read.lua before the measured EVALSHA so NOSCRIPT recovery does not
	// add an EVAL to the trace.
	if _, err := subject.ReadPage(ctx, forkPath, store.NowOffset, store.ReadPageOptions{NoTouch: true}); err != nil {
		t.Fatal(err)
	}

	var page store.ReadPage
	lines := monitorRedis(t, concreteTestClient(t, subject), func() {
		var err error
		page, err = subject.ReadPage(ctx, forkPath, store.ZeroOffset, store.ReadPageOptions{})
		if err != nil {
			t.Fatal(err)
		}
	})
	if len(page.Messages) != 1 || string(page.Messages[0].Data) != "source" ||
		page.Stats.RedisScriptInvokes != 2 {
		t.Fatalf("inherited page = %+v, want one source frame and two scripts", page)
	}
	if got := countMonitoredCommand(lines, "evalsha", metaKey(forkPath)); got != 1 {
		t.Fatalf("root EVALSHA calls = %d, want 1\n%s", got, strings.Join(lines, "\n"))
	}
	if got := countMonitoredCommand(lines, "evalsha", metaKey(sourcePath)); got != 1 {
		t.Fatalf("source EVALSHA calls = %d, want 1\n%s", got, strings.Join(lines, "\n"))
	}
	if got := countMonitoredCommand(lines, "hgetall", metaKey(forkPath)); got != 1 {
		t.Fatalf("root metadata HGETALL calls = %d, want 1\n%s", got, strings.Join(lines, "\n"))
	}
	if got := countMonitoredCommand(lines, "hgetall", metaKey(sourcePath)); got != 2 {
		t.Fatalf("source metadata HGETALL calls = %d, want traversal plus script (2)\n%s", got, strings.Join(lines, "\n"))
	}
	if got := countMonitoredCommand(lines, "zrangebylex", msgKey(forkPath)); got != 0 {
		t.Fatalf("root ZRANGEBYLEX calls = %d, want 0\n%s", got, strings.Join(lines, "\n"))
	}
	if got := countMonitoredCommand(lines, "zrangebylex", msgKey(sourcePath)); got != 1 {
		t.Fatalf("source ZRANGEBYLEX calls = %d, want 1\n%s", got, strings.Join(lines, "\n"))
	}
	if got := countMonitoredCommand(lines, "hgetall", prodKey(forkPath)); got != 0 {
		t.Fatalf("root producer HGETALL calls = %d, want 0\n%s", got, strings.Join(lines, "\n"))
	}
	if got := countMonitoredCommand(lines, "hgetall", prodKey(sourcePath)); got != 0 {
		t.Fatalf("source producer HGETALL calls = %d, want 0\n%s", got, strings.Join(lines, "\n"))
	}
}

func TestReadPageRootOwnedDoesNotAdvanceAOFOrReplication(t *testing.T) {
	subject := newTestStore(t)
	client := concreteTestClient(t, subject)
	ctx := context.Background()

	config, err := client.ConfigGet(ctx, "appendonly").Result()
	if err != nil {
		t.Fatal(err)
	}
	if config["appendonly"] != "yes" {
		t.Skip("Redis appendonly is disabled; repository Redis enables it")
	}

	for _, tc := range []struct {
		name      string
		expiresAt *time.Time
	}{
		{name: "persistent"},
		{name: "absolute expiry", expiresAt: func() *time.Time {
			value := time.Now().Add(time.Hour)
			return &value
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := testPath("read-persistence-" + tc.name)
			mustCreate(t, subject, path, store.CreateOptions{
				ContentType: "application/octet-stream",
				InitialData: []byte("frame"),
				ExpiresAt:   tc.expiresAt,
			})
			if _, err := subject.ReadPage(ctx, path, store.NowOffset, store.ReadPageOptions{NoTouch: true}); err != nil {
				t.Fatal(err)
			}
			if err := client.Do(ctx, "WAITAOF", 1, 0, 1000).Err(); err != nil {
				t.Fatal(err)
			}
			beforeAOF := redisInfoInt(t, client, "persistence", "aof_current_size")
			beforeReplication := redisInfoInt(t, client, "replication", "master_repl_offset")

			page, err := subject.ReadPage(ctx, path, store.ZeroOffset, store.ReadPageOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Messages) != 1 || page.Stats.RedisScriptInvokes != 1 {
				t.Fatalf("page = %+v", page)
			}
			if err := client.Do(ctx, "WAITAOF", 1, 0, 1000).Err(); err != nil {
				t.Fatal(err)
			}
			afterAOF := redisInfoInt(t, client, "persistence", "aof_current_size")
			afterReplication := redisInfoInt(t, client, "replication", "master_repl_offset")
			if afterAOF != beforeAOF || afterReplication != beforeReplication {
				t.Fatalf(
					"read persistence deltas: AOF=%d replication=%d, want 0/0",
					afterAOF-beforeAOF,
					afterReplication-beforeReplication,
				)
			}
			t.Logf(
				"AOF %d -> %d; master replication offset %d -> %d",
				beforeAOF,
				afterAOF,
				beforeReplication,
				afterReplication,
			)
		})
	}
}

func TestReadPageMetadataOnlyCommandShape(t *testing.T) {
	subject := newTestStore(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		initial []byte
		offset  store.Offset
	}{
		{name: "empty", offset: store.ZeroOffset},
		{name: "now", initial: []byte("frame"), offset: store.NowOffset},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := testPath("metadata-command-" + tc.name)
			mustCreate(t, subject, path, store.CreateOptions{
				ContentType: "application/octet-stream",
				InitialData: tc.initial,
			})
			if _, err := subject.ReadPage(ctx, path, store.NowOffset, store.ReadPageOptions{NoTouch: true}); err != nil {
				t.Fatal(err)
			}

			lines := monitorRedis(t, concreteTestClient(t, subject), func() {
				page, err := subject.ReadPage(ctx, path, tc.offset, store.ReadPageOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if len(page.Messages) != 0 || page.Stats.RedisScriptInvokes != 1 {
					t.Fatalf("metadata page = %+v", page)
				}
			})
			if got := countMonitoredCommand(lines, "evalsha", metaKey(path)); got != 1 {
				t.Fatalf("EVALSHA calls = %d, want 1\n%s", got, strings.Join(lines, "\n"))
			}
			if got := countMonitoredCommand(lines, "hgetall", metaKey(path)); got != 1 {
				t.Fatalf("metadata HGETALL calls = %d, want 1\n%s", got, strings.Join(lines, "\n"))
			}
			if got := countMonitoredCommand(lines, "zrangebylex", msgKey(path)); got != 0 {
				t.Fatalf("ZRANGEBYLEX calls = %d, want 0\n%s", got, strings.Join(lines, "\n"))
			}
		})
	}
}

func TestPageReaderSessionReusesOnlyResponseLocalForkPlan(t *testing.T) {
	subject := newTestStore(t)
	source := testPath("session-fork-source")
	mustCreate(t, subject, source, store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2,3]`),
	})
	fork := testPath("session-fork-root")
	mustCreate(t, subject, fork, store.CreateOptions{ForkedFrom: source})
	client := concreteTestClient(t, subject)
	session := subject.NewPageReaderSession(fork)

	var pages []store.ReadPage
	lines := monitorRedis(t, client, func() {
		var snapshot *store.ReadSnapshot
		next := store.ZeroOffset
		for range 3 {
			page, err := session.ReadPage(
				context.Background(),
				fork,
				next,
				store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1, Snapshot: snapshot},
			)
			if err != nil {
				t.Fatal(err)
			}
			pages = append(pages, page)
			if snapshot == nil {
				captured := page.Snapshot
				snapshot = &captured
			}
			next = page.NextOffset
		}
	})
	session.Close()

	if len(pages) != 3 || string(pages[0].Messages[0].Data) != "1" ||
		string(pages[2].Messages[0].Data) != "3" || !pages[2].UpToDate {
		t.Fatalf("session pages = %+v", pages)
	}
	for i, page := range pages {
		if page.Stats.RedisScriptInvokes != 2 {
			t.Fatalf("page %d script invocations = %d, want root + source", i, page.Stats.RedisScriptInvokes)
		}
	}
	if got := countMonitoredCommand(lines, "evalsha", ""); got != 6 {
		t.Fatalf("EVALSHA calls = %d, want 6\n%s", got, strings.Join(lines, "\n"))
	}
	if got := countMonitoredCommand(lines, "hgetall", metaKey(fork)); got != 3 {
		t.Fatalf("root metadata HGETALL calls = %d, want one per page\n%s", got, strings.Join(lines, "\n"))
	}
	// The source is loaded once while building the response plan, then once in
	// each source script so its expected incarnation is still validated.
	if got := countMonitoredCommand(lines, "hgetall", metaKey(source)); got != 4 {
		t.Fatalf("source metadata HGETALL calls = %d, want one plan + three validation loads\n%s", got, strings.Join(lines, "\n"))
	}
	if got := countMonitoredCommand(lines, "zrangebylex", msgKey(source)); got != 3 {
		t.Fatalf("source range calls = %d, want one per page\n%s", got, strings.Join(lines, "\n"))
	}
	if got := countMonitoredCommand(lines, "hgetall", prodKey(source)); got != 0 {
		t.Fatalf("producer metadata reads = %d, want 0\n%s", got, strings.Join(lines, "\n"))
	}
	assertNoMonitoredCommands(t, lines, fork, "hset", "hsetnx", "pexpire", "persist")
	if _, err := session.ReadPage(context.Background(), fork, store.ZeroOffset, store.ReadPageOptions{}); !errors.Is(err, errPageReaderSessionClosed) {
		t.Fatalf("ReadPage after Close error = %v, want session closed", err)
	}
}

func TestPageReaderSessionRebuildsPlanAtNilSnapshotResponseBoundary(t *testing.T) {
	subject := newTestStore(t)
	source := testPath("session-boundary-source")
	mustCreate(t, subject, source, store.CreateOptions{
		ContentType: "application/json",
		InitialData: []byte(`[1,2]`),
	})
	fork := testPath("session-boundary-fork")
	mustCreate(t, subject, fork, store.CreateOptions{ForkedFrom: source})
	session := subject.NewPageReaderSession(fork)
	defer session.Close()

	lines := monitorRedis(t, concreteTestClient(t, subject), func() {
		for range 2 {
			page, err := session.ReadPage(
				context.Background(),
				fork,
				store.ZeroOffset,
				store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Messages) != 1 || string(page.Messages[0].Data) != "1" {
				t.Fatalf("first page = %+v", page)
			}
		}
	})

	// Each nil-snapshot call starts a new response, so each must rediscover the
	// source once and then validate it again inside the source script.
	if got := countMonitoredCommand(lines, "hgetall", metaKey(source)); got != 4 {
		t.Fatalf("source metadata HGETALL calls = %d, want 4 across two responses\n%s", got, strings.Join(lines, "\n"))
	}
}

func TestReadPageLegacyIncarnationMigratesExactlyOnce(t *testing.T) {
	subject := newTestStore(t)
	path := testPath("legacy-incarnation-command")
	mustCreate(t, subject, path, store.CreateOptions{ContentType: "text/plain"})
	client := concreteTestClient(t, subject)
	if err := subject.client.HDel(context.Background(), metaKey(path), fIncarnation).Err(); err != nil {
		t.Fatal(err)
	}

	var first, second store.ReadPage
	lines := monitorRedis(t, client, func() {
		var err error
		first, err = subject.ReadPage(context.Background(), path, store.ZeroOffset, store.ReadPageOptions{})
		if err != nil {
			t.Fatal(err)
		}
		second, err = subject.ReadPage(context.Background(), path, store.ZeroOffset, store.ReadPageOptions{})
		if err != nil {
			t.Fatal(err)
		}
	})

	if first.Snapshot.Incarnation == "" ||
		first.Snapshot.Incarnation != second.Snapshot.Incarnation {
		t.Fatalf("legacy migration snapshots = %q, %q", first.Snapshot.Incarnation, second.Snapshot.Incarnation)
	}
	if got := countMonitoredCommand(lines, "hsetnx", metaKey(path)); got != 1 {
		t.Fatalf("HSETNX calls = %d, want exactly 1\n%s", got, strings.Join(lines, "\n"))
	}
	assertNoMonitoredCommands(t, lines, path, "hset", "pexpire", "persist")
}

func TestReadPageMinimumSlidingTTLTouchesOnlyFirstSnapshot(t *testing.T) {
	base := newTestStore(t)
	clock := store.NewFakeClock(time.Unix(1_000, 0))
	subject := New(base.client, Options{Clock: clock})
	path := testPath("minimum-sliding-ttl")
	ttl := int64(0)
	mustCreate(t, subject, path, store.CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte("frame"),
		TTLSeconds:  &ttl,
	})
	client := concreteTestClient(t, subject)

	var first store.ReadPage
	lines := monitorRedis(t, client, func() {
		var err error
		first, err = subject.ReadPage(
			context.Background(),
			path,
			store.ZeroOffset,
			store.ReadPageOptions{TargetBytes: 1, MaxFrames: 1},
		)
		if err != nil {
			t.Fatal(err)
		}
		_, err = subject.ReadPage(
			context.Background(),
			path,
			first.NextOffset,
			store.ReadPageOptions{Snapshot: &first.Snapshot},
		)
		if err != nil {
			t.Fatal(err)
		}
	})

	if got := countMonitoredCommand(lines, "hset", metaKey(path)); got != 1 {
		t.Fatalf("access HSET calls = %d, want 1\n%s", got, strings.Join(lines, "\n"))
	}
	if got := countMonitoredCommand(lines, "pexpire", metaKey(path)); got != 1 {
		t.Fatalf("metadata PEXPIRE calls = %d, want 1\n%s", got, strings.Join(lines, "\n"))
	}
}

func concreteTestClient(t *testing.T, subject *Store) *goredis.Client {
	t.Helper()
	client, ok := subject.client.(*goredis.Client)
	if !ok {
		t.Fatalf("test store client type = %T, want *redis.Client", subject.client)
	}
	return client
}

func monitorRedis(t *testing.T, client *goredis.Client, action func()) []string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	conn, reader := openRedisMonitor(t, ctx, client.Options())
	events := make(chan string, 4096)
	errors := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			line, err := readRedisSimpleString(reader)
			if err != nil {
				select {
				case errors <- err:
				case <-ctx.Done():
				}
				return
			}
			select {
			case events <- line:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() {
		cancel()
		_ = conn.Close()
		<-done
	}()

	start := fmt.Sprintf("chronicle-monitor-start-%d", time.Now().UnixNano())
	if err := client.Echo(ctx, start).Err(); err != nil {
		t.Fatal(err)
	}
	waitForMonitorMarker(t, events, errors, start)

	action()

	end := start + "-end"
	if err := client.Echo(ctx, end).Err(); err != nil {
		t.Fatal(err)
	}
	var lines []string
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case line := <-events:
			if strings.Contains(line, strconv.Quote(end)) {
				return lines
			}
			lines = append(lines, line)
		case err := <-errors:
			t.Fatalf("read Redis MONITOR output: %v", err)
		case <-timer.C:
			t.Fatalf("timed out waiting for Redis MONITOR end marker")
		}
	}
}

// openRedisMonitor owns a dedicated connection because MONITOR changes the
// connection protocol into an unbounded stream. Keeping it outside the client
// pool makes readiness explicit and gives shutdown one race-free owner.
func openRedisMonitor(
	t *testing.T,
	ctx context.Context,
	options *goredis.Options,
) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := options.Dialer(ctx, options.Network, options.Addr)
	if err != nil {
		t.Fatalf("dial Redis MONITOR connection: %v", err)
	}
	reader := bufio.NewReader(conn)
	ok := false
	defer func() {
		if !ok {
			_ = conn.Close()
		}
	}()

	username, password := options.Username, options.Password
	if options.CredentialsProviderContext != nil {
		username, password, err = options.CredentialsProviderContext(ctx)
		if err != nil {
			t.Fatalf("get Redis MONITOR credentials: %v", err)
		}
	} else if options.CredentialsProvider != nil {
		username, password = options.CredentialsProvider()
	}
	if username != "" {
		writeRedisCommand(t, conn, "AUTH", username, password)
		readRedisOK(t, reader, "AUTH")
	} else if password != "" {
		writeRedisCommand(t, conn, "AUTH", password)
		readRedisOK(t, reader, "AUTH")
	}
	if options.DB != 0 {
		writeRedisCommand(t, conn, "SELECT", strconv.Itoa(options.DB))
		readRedisOK(t, reader, "SELECT")
	}
	writeRedisCommand(t, conn, "MONITOR")
	readRedisOK(t, reader, "MONITOR")
	ok = true
	return conn, reader
}

func writeRedisCommand(t *testing.T, writer io.Writer, args ...string) {
	t.Helper()
	var command strings.Builder
	fmt.Fprintf(&command, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&command, "$%d\r\n%s\r\n", len(arg), arg)
	}
	if _, err := io.WriteString(writer, command.String()); err != nil {
		t.Fatalf("write Redis %s command: %v", args[0], err)
	}
}

func readRedisOK(t *testing.T, reader *bufio.Reader, command string) {
	t.Helper()
	reply, err := readRedisSimpleString(reader)
	if err != nil {
		t.Fatalf("read Redis %s response: %v", command, err)
	}
	if reply != "OK" {
		t.Fatalf("Redis %s response = %q, want OK", command, reply)
	}
}

func readRedisSimpleString(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if len(line) == 0 {
		return "", fmt.Errorf("empty Redis response")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return "", fmt.Errorf("Redis error: %s", line[1:])
	default:
		return "", fmt.Errorf("unexpected Redis response: %q", line)
	}
}

func waitForMonitorMarker(
	t *testing.T,
	events <-chan string,
	errors <-chan error,
	marker string,
) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case line := <-events:
			if strings.Contains(line, strconv.Quote(marker)) {
				return
			}
		case err := <-errors:
			t.Fatalf("read Redis MONITOR output: %v", err)
		case <-timer.C:
			t.Fatalf("timed out waiting for Redis MONITOR start marker")
		}
	}
}

func monitoredCommand(line string) string {
	_, command, ok := strings.Cut(line, "] ")
	if !ok {
		return ""
	}
	name, _, _ := strings.Cut(command, " ")
	return strings.ToLower(strings.Trim(name, `"`))
}

func countMonitoredCommand(lines []string, command, key string) int {
	count := 0
	for _, line := range lines {
		if monitoredCommand(line) != command {
			continue
		}
		if key == "" || strings.Contains(line, strconv.Quote(key)) {
			count++
		}
	}
	return count
}

func assertNoMonitoredCommands(t *testing.T, lines []string, path string, commands ...string) {
	t.Helper()
	keys := []string{metaKey(path), msgKey(path), prodKey(path), forksKey(path)}
	for _, command := range commands {
		for _, key := range keys {
			if got := countMonitoredCommand(lines, command, key); got != 0 {
				t.Fatalf("%s %s calls = %d, want 0\n%s", command, key, got, strings.Join(lines, "\n"))
			}
		}
	}
}

func redisInfoInt(t *testing.T, client *goredis.Client, section, field string) int64 {
	t.Helper()
	info, err := client.Info(context.Background(), section).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(info, "\r\n") {
		raw, ok := strings.CutPrefix(line, field+":")
		if !ok {
			continue
		}
		value, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil {
			t.Fatalf("parse Redis INFO %s %s=%q: %v", section, field, raw, parseErr)
		}
		return value
	}
	t.Fatalf("Redis INFO %s omitted %s", section, field)
	return 0
}

func keyPTTLs(t *testing.T, client *goredis.Client, path string) []time.Duration {
	t.Helper()
	keys := []string{metaKey(path), msgKey(path), prodKey(path), forksKey(path)}
	result := make([]time.Duration, len(keys))
	for i, key := range keys {
		pttl, err := client.PTTL(context.Background(), key).Result()
		if err != nil {
			t.Fatal(err)
		}
		result[i] = pttl
	}
	return result
}
