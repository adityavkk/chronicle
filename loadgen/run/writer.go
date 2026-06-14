package run

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/dsclient"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/payload"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/scenario"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/stats"
)

// startWriters launches one goroutine per writer. Each writer owns one
// stream (one producer per session — the realistic shape) and appends on
// an open-loop schedule: send times come from the pacer, never from
// response completions, so a slow SUT shows up as latency and drops, not
// as a quietly reduced request rate.
func (r *runner) startWriters(ctx context.Context, wg *sync.WaitGroup) {
	total := r.sc.TotalWriters()
	for w := 0; w < total; w++ {
		wg.Add(1)
		go r.writer(ctx, w, wg)
	}
	if total > 0 {
		r.logf("started %d writer(s), %s per writer (%s mode)", total, r.sc.Writers.Rate, r.sc.Writers.Producer)
	}
}

func (r *runner) writer(ctx context.Context, w int, wg *sync.WaitGroup) {
	defer wg.Done()
	sc := r.sc
	rec := r.col.NewRecorder()
	stream := sc.StreamName(w % sc.Streams.Count)
	pacer := pacerFromRate(sc.Writers.Rate, sc.Warmup.Duration+sc.Duration.Duration)
	isJSON := sc.Streams.ContentType == "application/json"
	batch := sc.Writers.Batch
	// Idempotent producers require strictly sequential Producer-Seq, so
	// that path is necessarily serialized per stream (as in Kafka): the
	// schedule still paces sends, but at most one append is in flight.
	serialize := sc.Writers.Producer == scenario.ProducerIdempotent
	producerID := fmt.Sprintf("dsload-w%04d", w)

	var inFlight sync.WaitGroup
	defer inFlight.Wait()

	for n := uint64(0); ; n++ {
		intended := r.paceStart.Add(pacer.At(n))
		if err := sleepCtx(ctx, time.Until(intended)); err != nil {
			return
		}

		select {
		case r.appendSem <- struct{}{}:
		default:
			rec.Count("appends_dropped", 1)
			r.col.Series.Add("appends_dropped", r.sec(), 1)
			continue
		}

		seq := n
		send := func() {
			defer func() { <-r.appendSem }()
			sentNano := time.Now().UnixNano()
			var body []byte
			if isJSON {
				if batch == 1 {
					body = payload.BuildJSON(seq, sentNano, sc.Writers.MessageBytes)
				} else {
					body = payload.BuildJSONBatch(seq*uint64(batch), sentNano, sc.Writers.MessageBytes, batch)
				}
			} else {
				body = payload.BuildBytesBatch(seq*uint64(batch), sentNano, sc.Writers.MessageBytes, batch)
			}
			var extra map[string]string
			if serialize {
				extra = map[string]string{
					dsclient.HeaderProdID:    producerID,
					dsclient.HeaderProdEpoch: "0",
					dsclient.HeaderProdSeq:   strconv.FormatUint(seq, 10),
				}
			}
			// Detached context: an in-flight append outlives writer
			// cancellation and finishes (or times out) on its own.
			reqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sc.Limits.RequestTimeout.Duration)
			resp, err := r.cl.Append(reqCtx, stream, sc.Streams.ContentType, body, extra)
			cancel()
			lat := time.Since(intended)
			sec := r.sec()
			r.col.Series.Add("appends_sent", sec, 1)
			switch {
			case err != nil:
				rec.CountError("append", classify(err))
				r.col.Series.Add("appends_err", sec, 1)
			case resp.Status/100 == 2:
				rec.Record(stats.Append, lat)
				rec.Count("appends_ok", 1)
				rec.Count("msgs_appended", int64(batch))
				rec.Count("bytes_appended", int64(len(body)))
				r.col.Series.Add("appends_ok", sec, 1)
			default:
				rec.CountError("append", fmt.Sprintf("status=%d", resp.Status))
				r.col.Series.Add("appends_err", sec, 1)
			}
		}

		if serialize {
			send()
		} else {
			inFlight.Add(1)
			go func() {
				defer inFlight.Done()
				send()
			}()
		}
	}
}
