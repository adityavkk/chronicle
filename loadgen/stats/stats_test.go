package stats

import (
	"testing"
	"time"
)

func TestRecorderGatedByCollector(t *testing.T) {
	c := NewCollector(10)
	r := c.NewRecorder()

	r.Record(Append, 5*time.Millisecond) // gate closed: discarded
	r.Count("appends_ok", 1)

	c.SetRecording(true)
	r.Record(Append, 10*time.Millisecond)
	r.Count("appends_ok", 2)
	c.SetRecording(false)
	r.Record(Append, 20*time.Millisecond)

	hists, counts := c.Merged()
	if got := hists[Append].TotalCount(); got != 1 {
		t.Errorf("recorded %d samples, want 1 (gate)", got)
	}
	if counts["appends_ok"] != 2 {
		t.Errorf("appends_ok = %d, want 2", counts["appends_ok"])
	}
}

func TestMergeAcrossRecorders(t *testing.T) {
	c := NewCollector(10)
	c.SetRecording(true)
	r1, r2 := c.NewRecorder(), c.NewRecorder()
	r1.Record(DeliverySSE, 1*time.Millisecond)
	r2.Record(DeliverySSE, 3*time.Millisecond)
	r1.CountError("sse", "transport")
	r2.CountError("sse", "transport")

	hists, counts := c.Merged()
	q := Summarize(hists[DeliverySSE])
	if q.Count != 2 {
		t.Errorf("count = %d, want 2", q.Count)
	}
	if q.Min < 0.9 || q.Max > 3.1 {
		t.Errorf("min/max = %v/%v", q.Min, q.Max)
	}
	if counts["err:sse:transport"] != 2 {
		t.Errorf("errors = %v", counts)
	}
}

func TestSummarizeQuantiles(t *testing.T) {
	c := NewCollector(10)
	c.SetRecording(true)
	r := c.NewRecorder()
	for i := 1; i <= 1000; i++ {
		r.Record(Append, time.Duration(i)*time.Millisecond)
	}
	hists, _ := c.Merged()
	q := Summarize(hists[Append])
	if q.Count != 1000 {
		t.Fatalf("count = %d", q.Count)
	}
	// HDR at 3 significant figures: within 0.1% of true values.
	approx := func(got, want float64) bool { return got > want*0.99 && got < want*1.01 }
	if !approx(q.P50, 500) || !approx(q.P99, 990) || !approx(q.Max, 1000) {
		t.Errorf("p50/p99/max = %.1f/%.1f/%.1f", q.P50, q.P99, q.Max)
	}
}

func TestRecordClampsOutOfRange(t *testing.T) {
	c := NewCollector(10)
	c.SetRecording(true)
	r := c.NewRecorder()
	r.Record(Append, 500*time.Second) // beyond histMax
	r.Record(Append, 0)               // below histMin
	hists, _ := c.Merged()
	if got := hists[Append].TotalCount(); got != 2 {
		t.Errorf("count = %d, want 2 (clamped, not dropped)", got)
	}
}

func TestSeries(t *testing.T) {
	c := NewCollector(5)
	c.Series.Add("appends_ok", 0, 3)
	c.Series.Add("appends_ok", 2, 4)
	c.Series.Add("appends_ok", -1, 9) // ignored
	c.Series.Add("appends_ok", 10_000, 9)
	snap := c.Series.Snapshot()
	col := snap["appends_ok"]
	if len(col) != 3 || col[0] != 3 || col[2] != 4 {
		t.Errorf("series = %v", col)
	}
}
