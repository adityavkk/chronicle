package run

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/payload"
)

const setupParallelism = 32

// forEachStream runs fn for every stream index with bounded parallelism
// and returns the first error.
func (r *runner) forEachStream(ctx context.Context, fn func(ctx context.Context, i int, name string) error) error {
	sem := make(chan struct{}, setupParallelism)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for i := 0; i < r.sc.Streams.Count; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer func() { <-sem; wg.Done() }()
			if err := fn(ctx, i, r.sc.StreamName(i)); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return firstErr
}

func (r *runner) createStreams(ctx context.Context) error {
	t0 := time.Now()
	err := r.forEachStream(ctx, func(ctx context.Context, _ int, name string) error {
		cctx, cancel := context.WithTimeout(ctx, r.sc.Limits.RequestTimeout.Duration)
		defer cancel()
		resp, err := r.cl.Create(cctx, name, r.sc.Streams.ContentType)
		if err != nil {
			return fmt.Errorf("create %s: %w", name, err)
		}
		if resp.Status != 200 && resp.Status != 201 {
			return fmt.Errorf("create %s: status %d", name, resp.Status)
		}
		return nil
	})
	if err != nil {
		return err
	}
	r.logf("created %d stream(s) in %s", r.sc.Streams.Count, time.Since(t0).Round(time.Millisecond))
	return nil
}

// prefill seeds streams before measurement using batched appends so big
// prefills finish quickly without resembling the measured workload.
func (r *runner) prefill(ctx context.Context) error {
	p := r.sc.Streams.Prefill
	isJSON := r.sc.Streams.ContentType == "application/json"
	t0 := time.Now()
	err := r.forEachStream(ctx, func(ctx context.Context, _ int, name string) error {
		for sent := 0; sent < p.Messages; {
			n := min(p.BatchSize, p.Messages-sent)
			var body []byte
			if isJSON {
				body = payload.BuildJSONBatch(uint64(sent), time.Now().UnixNano(), p.MessageBytes, n)
			} else {
				body = payload.BuildBytesBatch(uint64(sent), time.Now().UnixNano(), p.MessageBytes, n)
			}
			actx, cancel := context.WithTimeout(ctx, 4*r.sc.Limits.RequestTimeout.Duration)
			resp, err := r.cl.Append(actx, name, r.sc.Streams.ContentType, body, nil)
			cancel()
			if err != nil {
				return fmt.Errorf("prefill %s: %w", name, err)
			}
			if resp.Status/100 != 2 {
				return fmt.Errorf("prefill %s: status %d", name, resp.Status)
			}
			sent += n
		}
		return nil
	})
	if err != nil {
		return err
	}
	r.logf("prefilled %d stream(s) with %d × %dB messages in %s",
		r.sc.Streams.Count, p.Messages, p.MessageBytes, time.Since(t0).Round(time.Millisecond))
	return nil
}

func (r *runner) closeStreams(ctx context.Context) {
	err := r.forEachStream(ctx, func(ctx context.Context, _ int, name string) error {
		cctx, cancel := context.WithTimeout(ctx, r.sc.Limits.RequestTimeout.Duration)
		defer cancel()
		_, err := r.cl.Close(cctx, name)
		return err
	})
	if err != nil {
		r.note("closing streams: %v", err)
	}
}

func (r *runner) deleteStreams(ctx context.Context) {
	err := r.forEachStream(ctx, func(ctx context.Context, _ int, name string) error {
		dctx, cancel := context.WithTimeout(ctx, r.sc.Limits.RequestTimeout.Duration)
		defer cancel()
		_, err := r.cl.Delete(dctx, name)
		return err
	})
	if err != nil {
		r.note("deleting streams: %v", err)
	}
}
