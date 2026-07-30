package run

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/dsclient"
	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/stats"
)

// startCatchup launches the cold-read workload: open-loop GETs from
// offset=-1 against randomly chosen streams — the "user refreshes the
// page / new viewer opens a share link" pattern.
func (r *runner) startCatchup(ctx context.Context, wg *sync.WaitGroup) {
	if r.sc.Catchup.Readers > 0 {
		for reader := 0; reader < r.sc.Catchup.Readers; reader++ {
			wg.Add(1)
			go r.catchupReader(ctx, wg, reader)
		}
		r.logf("started %d closed-loop catch-up reader(s) across %d stream(s)",
			r.sc.Catchup.Readers, r.sc.Streams.Count)
		return
	}
	if r.sc.Catchup.Rate.IsZero() {
		return
	}
	wg.Add(1)
	go r.catchupLoop(ctx, wg)
	r.logf("started catch-up reads at %s across %d stream(s)", r.sc.Catchup.Rate, r.sc.Streams.Count)
}

func (r *runner) catchupReader(ctx context.Context, wg *sync.WaitGroup, reader int) {
	defer wg.Done()
	rec := r.col.NewRecorder()
	timeout := 4 * r.sc.Limits.RequestTimeout.Duration
	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Rotate every closed-loop reader through the full stream set. Permanent
		// reader-to-stream assignment can leave half the configured working set
		// untouched when reader and stream counts differ.
		stream := r.sc.StreamName((reader + attempt) % r.sc.Streams.Count)
		attempt++
		sendStart := time.Now()
		eligible := r.measurementIncludes(sendStart)
		reqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		resp, err := r.cl.ReadCatchup(reqCtx, stream, "-1")
		cancel()
		sec := r.sec()
		r.col.Series.Add("catchup_sent", sec, 1)
		r.recordCatchup(rec, resp, err, 0, sec, eligible)
	}
}

func (r *runner) recordCatchup(
	rec *stats.Recorder,
	resp dsclient.Response,
	err error,
	scheduleDelay time.Duration,
	sec int,
	eligible bool,
) {
	var protocolErr error
	if err == nil && resp.Status == 200 {
		protocolErr = validateCatchupResponse(resp)
	}
	switch {
	case err != nil:
		rec.CountErrorEligible(eligible, "catchup", classify(err))
		r.col.Series.Add("catchup_err", sec, 1)
	case protocolErr != nil:
		rec.CountErrorEligible(eligible, "catchup", "protocol-headers")
		r.col.Series.Add("catchup_err", sec, 1)
	case resp.Status == 200:
		rec.RecordEligible(eligible, stats.CatchupTTFB, scheduleDelay+resp.TTFB)
		rec.RecordEligible(eligible, stats.CatchupTotal, scheduleDelay+resp.Total)
		rec.CountEligible(eligible, "catchup_ok", 1)
		rec.CountEligible(eligible, "catchup_bytes", resp.BodyBytes)
		r.col.Series.Add("catchup_ok", sec, 1)
		r.col.Series.Add("catchup_bytes", sec, resp.BodyBytes)
	default:
		rec.CountErrorEligible(eligible, "catchup", fmt.Sprintf("status=%d", resp.Status))
		r.col.Series.Add("catchup_err", sec, 1)
	}
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
		eligible := r.measurementIncludes(intended)
		select {
		case r.catchupSem <- struct{}{}:
		default:
			rec.CountEligible(eligible, "catchup_dropped", 1)
			r.col.Series.Add("catchup_dropped", r.sec(), 1)
			continue
		}
		stream := sc.StreamName(rnd.Intn(sc.Streams.Count))
		inFlight.Add(1)
		go func() {
			defer func() { <-r.catchupSem; inFlight.Done() }()
			sendStart := time.Now()
			reqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
			resp, err := r.cl.ReadCatchup(reqCtx, stream, "-1")
			cancel()
			schedDelay := sendStart.Sub(intended)
			sec := r.sec()
			r.col.Series.Add("catchup_sent", sec, 1)
			r.recordCatchup(rec, resp, err, schedDelay, sec, eligible)
		}()
	}
}

func validateCatchupResponse(resp dsclient.Response) error {
	if resp.NextOffset == "" {
		return fmt.Errorf("missing %s", dsclient.HeaderNextOffset)
	}
	if !resp.UpToDate {
		return fmt.Errorf("missing or false %s", dsclient.HeaderUpToDate)
	}
	return nil
}
