// Package redismon reads the managed-Redis (Memorystore) CPU-utilization time
// series from Cloud Monitoring, the missing signal for gate #1
// (docs/specs/horizontal-scale/research/05 experiment 1): ramp the SUT replicas
// 1->4 at a fixed K and confirm Redis CPU scales with N — the O(N*K) sweep
// redundancy the epic exists to relieve. The chronicle sweep histogram is
// per-replica (read it at replicas=1), so the multi-replica redundancy effect can
// only be seen at the shared {__ds}-slot's Redis CPU, not in the SUT's /metrics.
//
// Split pure-core / shell: ParseCPUSeries is a pure function over the gcloud JSON
// (unit-tested with a golden response, no cloud), and Reader is the thin shell
// that builds and runs the gcloud command through an injectable runner.
package redismon

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"time"
)

// cpuMetricType is the Memorystore for Redis CPU-utilization metric.
const cpuMetricType = "redis.googleapis.com/stats/cpu_utilization"

// Point is one CPU-utilization sample: an RFC3339 end time and a utilization in
// [0,1] (fraction of a vCPU).
type Point struct {
	Time  string
	Value float64
}

// Series is a flat, time-ascending CPU series for one Memorystore instance (all
// timeSeries — e.g. per shard/node — flattened together).
type Series struct {
	Points []Point
}

// Max returns the peak utilization in the series (0 when empty).
func (s Series) Max() float64 {
	max := 0.0
	for _, p := range s.Points {
		if p.Value > max {
			max = p.Value
		}
	}
	return max
}

// Mean returns the mean utilization in the series (0 when empty).
func (s Series) Mean() float64 {
	if len(s.Points) == 0 {
		return 0
	}
	sum := 0.0
	for _, p := range s.Points {
		sum += p.Value
	}
	return sum / float64(len(s.Points))
}

// gcloudTimeSeries is the subset of `gcloud monitoring time-series list
// --format=json` we read: a list of series, each a list of points.
type gcloudTimeSeries struct {
	Points []struct {
		Interval struct {
			EndTime string `json:"endTime"`
		} `json:"interval"`
		Value struct {
			DoubleValue float64 `json:"doubleValue"`
		} `json:"value"`
	} `json:"points"`
}

// ParseCPUSeries parses `gcloud monitoring time-series list --format=json` output
// (an array of timeSeries) into a flat, time-ascending CPU Series. All series'
// points are merged, so a multi-node instance reports its whole CPU picture; use
// Max for the worst node-second and Mean for the window average.
func ParseCPUSeries(data []byte) (Series, error) {
	var raw []gcloudTimeSeries
	if err := json.Unmarshal(data, &raw); err != nil {
		return Series{}, fmt.Errorf("parse monitoring json: %w", err)
	}
	var s Series
	for _, ts := range raw {
		for _, p := range ts.Points {
			s.Points = append(s.Points, Point{Time: p.Interval.EndTime, Value: p.Value.DoubleValue})
		}
	}
	sort.SliceStable(s.Points, func(i, j int) bool { return s.Points[i].Time < s.Points[j].Time })
	return s, nil
}

// Reader reads a Memorystore instance's CPU series via gcloud. Run is injectable
// (nil => exec.Command), so the command construction is unit-testable without
// gcloud.
type Reader struct {
	Project  string
	Instance string // the Memorystore instance id (resource.labels.instance_id)
	Run      func(name string, args ...string) ([]byte, error)
}

// MonitoringArgs builds the `gcloud monitoring time-series list` arguments for the
// CPU metric over [start, end]. Kept pure (times passed in) so it is testable; the
// Reader computes the window from time.Now.
func (r Reader) MonitoringArgs(start, end time.Time) []string {
	filter := fmt.Sprintf(`metric.type="%s" AND resource.labels.instance_id="%s"`, cpuMetricType, r.Instance)
	return []string{
		"monitoring", "time-series", "list",
		"--project", r.Project,
		"--filter", filter,
		"--interval-start-time", start.UTC().Format(time.RFC3339),
		"--interval-end-time", end.UTC().Format(time.RFC3339),
		"--format", "json",
	}
}

// ReadCPU reads the CPU series over the last window ending now.
func (r Reader) ReadCPU(now time.Time, window time.Duration) (Series, error) {
	args := r.MonitoringArgs(now.Add(-window), now)
	out, err := r.run("gcloud", args...)
	if err != nil {
		return Series{}, fmt.Errorf("gcloud monitoring: %w", err)
	}
	return ParseCPUSeries(out)
}

func (r Reader) run(name string, args ...string) ([]byte, error) {
	if r.Run != nil {
		return r.Run(name, args...)
	}
	return exec.Command(name, args...).Output()
}
