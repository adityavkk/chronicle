package run

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/stats"
)

// startCatchup launches the cold-read workload: open-loop GETs from
// offset=-1 against randomly chosen streams — the "user refreshes the
// page / new viewer opens a share link" pattern.
func (r *runner) startCatchup(ctx context.Context, wg *sync.WaitGroup) {
	if r.sc.Catchup.Rate.IsZero() {
		return
	}
	wg.Add(1)
	go r.catchupLoop(ctx, wg)
	r.logf("started catch-up reads at %s across %d stream(s)", r.sc.Catchup.Rate, r.sc.Streams.Count)
}

func (r *runner) catchupLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	sc := r.sc
	rec := r.col.NewRecorder()
	pacer := pacerFromRate(sc.Catchup.Rate, sc.Warmup.Duration+sc.Duration.Duration)
	rnd := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic stream choice, not crypto
	// Full-stream reads can be large; give them more room than appends.
	timeout := 4 * sc.Limits.RequestTimeout.Duration

	var inFlight sync.WaitGroup
	defer inFlight.Wait()

	for n := uint64(0); ; n++ {
		intended := r.paceStart.Add(pacer.At(n))
		if err := sleepCtx(ctx, time.Until(intended)); err != nil {
			return
		}
		select {
		case r.catchupSem <- struct{}{}:
		default:
			rec.Count("catchup_dropped", 1)
			r.col.Series.Add("catchup_dropped", r.sec(), 1)
			continue
		}
		stream := sc.StreamName(rnd.Intn(sc.Streams.Count))
		inFlight.Add(1)
		go func() {
			defer func() { <-r.catchupSem; inFlight.Done() }()
			sendStart := time.Now()
			reqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
			resp, err := r.cl.Read(reqCtx, stream, "-1", "", "")
			cancel()
			schedDelay := sendStart.Sub(intended)
			sec := r.sec()
			r.col.Series.Add("catchup_sent", sec, 1)
			switch {
			case err != nil:
				rec.CountError("catchup", classify(err))
				r.col.Series.Add("catchup_err", sec, 1)
			case resp.Status == 200:
				rec.Record(stats.CatchupTTFB, schedDelay+resp.TTFB)
				rec.Record(stats.CatchupTotal, schedDelay+resp.Total)
				rec.Count("catchup_ok", 1)
				rec.Count("catchup_bytes", int64(len(resp.Body)))
				r.col.Series.Add("catchup_ok", sec, 1)
				r.col.Series.Add("catchup_bytes", sec, int64(len(resp.Body)))
			default:
				rec.CountError("catchup", fmt.Sprintf("status=%d", resp.Status))
				r.col.Series.Add("catchup_err", sec, 1)
			}
		}()
	}
}
