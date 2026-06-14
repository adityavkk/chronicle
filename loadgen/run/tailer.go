package run

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/payload"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/scenario"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/ssewire"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/stats"
)

// startTailers launches the live-reader population: SSE and long-poll
// tailers per stream, with connections staggered across the configured
// ramp so the SUT sees a realistic arrival of subscribers rather than a
// synchronized thundering herd.
func (r *runner) startTailers(ctx context.Context, wg *sync.WaitGroup, connected *atomic.Int64) {
	sc := r.sc
	total := sc.TotalTailers()
	if total == 0 {
		return
	}
	stagger := func(k int) time.Duration {
		return time.Duration(float64(sc.Tailers.ConnectRamp.Duration) * float64(k) / float64(total))
	}
	k := 0
	for i := 0; i < sc.Streams.Count; i++ {
		stream := sc.StreamName(i)
		for j := 0; j < sc.Tailers.SSEPerStream; j++ {
			wg.Add(1)
			go r.sseTailer(ctx, stream, stagger(k), wg, connected)
			k++
		}
		for j := 0; j < sc.Tailers.LongPollPerStream; j++ {
			wg.Add(1)
			go r.longPollTailer(ctx, stream, stagger(k), wg, connected)
			k++
		}
	}
	r.logf("starting %d tailer(s) (%d sse, %d long-poll per stream) over %s",
		total, sc.Tailers.SSEPerStream, sc.Tailers.LongPollPerStream, sc.Tailers.ConnectRamp)
}

// awaitTailers blocks until the tailer population is attached (or a
// generous deadline passes — partial attachment is a finding, not a
// crash, and is recorded as a note).
func (r *runner) awaitTailers(ctx context.Context, connected *atomic.Int64) {
	want := int64(r.sc.TotalTailers())
	if want == 0 {
		return
	}
	deadline := time.Now().Add(r.sc.Tailers.ConnectRamp.Duration + 15*time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if connected.Load() >= want {
			r.logf("all %d tailers attached", want)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	r.note("only %d/%d tailers attached before workload start", connected.Load(), want)
	r.logf("WARNING: only %d/%d tailers attached", connected.Load(), want)
}

func (r *runner) startOffset() string {
	if r.sc.Tailers.From == scenario.FromStart {
		return "-1"
	}
	return "now"
}

func (r *runner) sseTailer(ctx context.Context, stream string, delay time.Duration, wg *sync.WaitGroup, connected *atomic.Int64) {
	defer wg.Done()
	if sleepCtx(ctx, delay) != nil {
		return
	}
	rec := r.col.NewRecorder()
	offset, cursor := r.startOffset(), ""
	attached := false
	for ctx.Err() == nil {
		conn, err := r.cl.OpenSSE(ctx, stream, offset, cursor)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			rec.CountError("sse", classify(err))
			_ = sleepCtx(ctx, 250*time.Millisecond)
			continue
		}
		if !attached {
			attached = true
			connected.Add(1)
		} else {
			rec.Count("sse_reconnects", 1)
		}
		closed := r.consumeSSE(ctx, conn, &offset, &cursor, rec)
		conn.Close() //nolint:errcheck // tailer teardown; reconnect handles the rest
		if closed {
			return // stream closed: protocol-mandated EOF, do not reconnect
		}
	}
}

// consumeSSE drains one SSE connection until the server cycles it (EOF),
// an error occurs, or the stream closes. Offsets/cursors advance via
// control events exactly as the protocol prescribes for real clients.
func (r *runner) consumeSSE(ctx context.Context, conn interface {
	Next() (ssewire.Event, error)
}, offset, cursor *string, rec *stats.Recorder,
) (streamClosed bool) {
	for {
		ev, err := conn.Next()
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, io.EOF) {
				rec.CountError("sse", classify(err))
			}
			return false
		}
		switch ev.Type {
		case "data":
			r.recordDeliveries(rec, stats.DeliverySSE, "msgs_sse", "bytes_sse", ev.Data, true)
		case "control":
			var ctl ssewire.Control
			if json.Unmarshal(ev.Data, &ctl) != nil {
				rec.CountError("sse", "bad-control")
				continue
			}
			if ctl.StreamNextOffset != "" {
				*offset = ctl.StreamNextOffset
			}
			if ctl.StreamCursor != "" {
				*cursor = ctl.StreamCursor
			}
			if ctl.StreamClosed {
				return true
			}
		}
	}
}

func (r *runner) longPollTailer(ctx context.Context, stream string, delay time.Duration, wg *sync.WaitGroup, connected *atomic.Int64) {
	defer wg.Done()
	if sleepCtx(ctx, delay) != nil {
		return
	}
	rec := r.col.NewRecorder()
	offset, cursor := r.startOffset(), ""
	attached := false
	// A long-poll request legitimately holds for the server's long-poll
	// timeout (30s default on both SUTs); allow that plus slack.
	pollTimeout := 40 * time.Second
	for ctx.Err() == nil {
		if !attached {
			// Attachment means "the poll is in flight": with no data
			// flowing yet, the first response may not arrive until
			// writers start, and the startup barrier must not wait on it.
			attached = true
			connected.Add(1)
		}
		reqCtx, cancel := context.WithTimeout(ctx, pollTimeout)
		resp, err := r.cl.Read(reqCtx, stream, offset, "long-poll", cursor)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			rec.CountError("long_poll", classify(err))
			_ = sleepCtx(ctx, 250*time.Millisecond)
			continue
		}
		if resp.Cursor != "" {
			cursor = resp.Cursor
		}
		switch resp.Status {
		case 200:
			r.recordDeliveries(rec, stats.DeliveryLongPoll, "msgs_long_poll", "bytes_long_poll", resp.Body, false)
			if resp.NextOffset != "" {
				offset = resp.NextOffset
			}
			rec.Count("long_poll_200", 1)
		case 204:
			rec.Count("long_poll_204", 1)
			if resp.NextOffset != "" {
				offset = resp.NextOffset
			}
		default:
			rec.CountError("long_poll", "status="+strconv.Itoa(resp.Status))
			_ = sleepCtx(ctx, 250*time.Millisecond)
		}
		if resp.Closed {
			return
		}
	}
}

var crlf = regexp.MustCompile(`[\r\n]`)

// recordDeliveries parses a delivered body into messages and records one
// write-to-receipt latency per message: receipt wall-clock minus the send
// timestamp the writer embedded in the payload. Because each event is
// judged against its own send time, a stalled tailer accrues the full
// backlog delay — there is no coordinated omission on this path.
func (r *runner) recordDeliveries(rec *stats.Recorder, m stats.Metric, msgCol, byteCol string, body []byte, sse bool) {
	recvNano := time.Now().UnixNano()
	isJSON := r.sc.Streams.ContentType == "application/json"
	var frames [][]byte
	if isJSON {
		raw, err := payload.SplitJSONArray(body)
		if err != nil {
			rec.CountError(string(m), "bad-body")
			return
		}
		frames = make([][]byte, len(raw))
		for i, rm := range raw {
			frames[i] = rm
		}
	} else {
		if sse {
			// Binary SSE data events are base64; strip SSE line breaks first.
			decoded, err := base64.StdEncoding.DecodeString(string(crlf.ReplaceAll(body, nil)))
			if err != nil {
				rec.CountError(string(m), "bad-base64")
				return
			}
			body = decoded
		}
		frames = payload.SplitBytesFrames(body)
	}
	sec := r.sec()
	var bytes int64
	for _, f := range frames {
		bytes += int64(len(f))
		if msg, ok := payload.Parse(f); ok && msg.SentNano > 0 {
			rec.Record(m, time.Duration(recvNano-msg.SentNano))
		}
	}
	rec.Count(msgCol, int64(len(frames)))
	rec.Count(byteCol, bytes)
	r.col.Series.Add(msgCol, sec, int64(len(frames)))
	r.col.Series.Add(byteCol, sec, bytes)
}
