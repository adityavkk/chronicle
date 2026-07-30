package run

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ResourceSample is one observation of one process's resource usage.
// CPUSeconds is cumulative (user+system, all cores); reports derive
// interval CPU% from deltas, which is robust where instantaneous %CPU
// readings (decaying averages) are not.
type ResourceSample struct {
	Sec                int     `json:"sec"`
	Phase              string  `json:"phase,omitempty"`
	Name               string  `json:"name"`
	RSSBytes           int64   `json:"rss_bytes"`
	CPUSeconds         float64 `json:"cpu_seconds"`
	NetworkReadBytes   int64   `json:"network_read_bytes,omitempty"`
	NetworkWriteBytes  int64   `json:"network_write_bytes,omitempty"`
	OpenFiles          int64   `json:"open_files,omitempty"`
	Connections        int64   `json:"connections,omitempty"`
	Operations         int64   `json:"operations,omitempty"`
	ScriptMicroseconds int64   `json:"script_microseconds,omitempty"`
}

// MetricSample is one Prometheus or Redis INFO observation. Metric preserves
// any bounded labels so histogram buckets and cancellation phases remain
// distinguishable in results.json.
type MetricSample struct {
	Sec    int     `json:"sec"`
	Name   string  `json:"name"`
	Metric string  `json:"metric"`
	Value  float64 `json:"value"`
}

// sampler polls SUT processes (via ps) and Redis (via INFO over TCP)
// once per second for the duration of the run. The load generator
// samples itself too, so "was the generator the bottleneck?" is
// answerable from the results file.
type sampler struct {
	pids    map[string]int
	redis   map[string]string
	metrics map[string]string
	logf    func(string, ...any)

	mu        sync.Mutex
	out       []ResourceSample
	metricOut []MetricSample
}

func newSampler(pids map[string]int, redis, metrics map[string]string, logf func(string, ...any)) *sampler {
	all := map[string]int{"loadgen": os.Getpid()}
	for k, v := range pids {
		all[k] = v
	}
	return &sampler{pids: all, redis: redis, metrics: metrics, logf: logf}
}

func (s *sampler) start(ctx context.Context, wg *sync.WaitGroup, anchor time.Time) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				sec := int(time.Since(anchor).Seconds())
				s.samplePids(sec)
				s.sampleRedis(sec)
				s.samplePrometheus(sec)
			}
		}
	}()
}

func (s *sampler) metricSamples() []MetricSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]MetricSample(nil), s.metricOut...)
}

func (s *sampler) samples() []ResourceSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ResourceSample(nil), s.out...)
}

func (s *sampler) addMetric(sample MetricSample) {
	s.mu.Lock()
	s.metricOut = append(s.metricOut, sample)
	s.mu.Unlock()
}

func (s *sampler) add(sample ResourceSample) {
	s.mu.Lock()
	s.out = append(s.out, sample)
	s.mu.Unlock()
}

func (s *sampler) samplePids(sec int) {
	if len(s.pids) == 0 {
		return
	}
	args := make([]string, 0, 4)
	args = append(args, "-o", "pid=,rss=,cputime=")
	byPid := map[string]string{}
	var pidList []string
	for name, pid := range s.pids {
		p := strconv.Itoa(pid)
		byPid[p] = name
		pidList = append(pidList, p)
	}
	args = append(args, "-p", strings.Join(pidList, ","))
	out, err := exec.CommandContext(context.Background(), "ps", args...).Output()
	if err != nil {
		return // process gone; absence in the series is the signal
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		name, ok := byPid[fields[0]]
		if !ok {
			continue
		}
		rssKB, _ := strconv.ParseInt(fields[1], 10, 64)
		cpu, err := parseCPUTime(fields[2])
		if err != nil {
			continue
		}
		s.add(ResourceSample{
			Sec:        sec,
			Name:       name,
			RSSBytes:   rssKB * 1024,
			CPUSeconds: cpu,
			OpenFiles:  processOpenFiles(pidFromString(fields[0])),
		})
	}
}

func pidFromString(value string) int {
	pid, _ := strconv.Atoi(value)
	return pid
}

func processOpenFiles(pid int) int64 {
	if pid <= 0 {
		return 0
	}
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return 0 // unavailable on macOS; process collector remains the fallback
	}
	return int64(len(entries))
}

// parseCPUTime parses ps cputime: [[dd-]hh:]mm:ss[.cc].
func parseCPUTime(v string) (float64, error) {
	var days float64
	if d, rest, ok := strings.Cut(v, "-"); ok {
		n, err := strconv.Atoi(d)
		if err != nil {
			return 0, err
		}
		days = float64(n)
		v = rest
	}
	var total float64
	for _, p := range strings.Split(v, ":") {
		f, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return 0, err
		}
		total = total*60 + f
	}
	return days*86400 + total, nil
}

// sampleRedis fetches used_memory and cumulative CPU from INFO over a
// raw TCP connection — no client dependency, no docker exec latency.
func (s *sampler) sampleRedis(sec int) {
	for name, addr := range s.redis {
		info, err := redisInfoDetailed(addr)
		if err != nil {
			continue
		}
		s.add(ResourceSample{
			Sec:                sec,
			Name:               name,
			RSSBytes:           info.usedMemory,
			CPUSeconds:         info.cpuSeconds,
			NetworkReadBytes:   info.networkRead,
			NetworkWriteBytes:  info.networkWrite,
			Connections:        info.connections,
			Operations:         info.operations,
			ScriptMicroseconds: info.scriptMicroseconds,
		})
		for metric, value := range info.metrics {
			s.addMetric(MetricSample{Sec: sec, Name: name, Metric: metric, Value: value})
		}
	}
}

func redisInfo(addr string) (usedMemory int64, cpuSeconds float64, err error) {
	info, err := redisInfoDetailed(addr)
	return info.usedMemory, info.cpuSeconds, err
}

type redisInfoSample struct {
	usedMemory         int64
	cpuSeconds         float64
	networkRead        int64
	networkWrite       int64
	connections        int64
	operations         int64
	scriptMicroseconds int64
	metrics            map[string]float64
}

func redisInfoDetailed(addr string) (redisInfoSample, error) {
	conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return redisInfoSample{}, err
	}
	defer conn.Close() //nolint:errcheck // read-only probe connection
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write([]byte("INFO\r\n")); err != nil {
		return redisInfoSample{}, err
	}
	r := bufio.NewReader(conn)
	info, err := readRedisBulk(r)
	if err != nil {
		return redisInfoSample{}, err
	}
	result := redisInfoSample{metrics: make(map[string]float64)}
	var sys, user float64
	for _, line := range strings.Split(string(info), "\r\n") {
		key, raw, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch {
		case key == "used_memory":
			result.usedMemory, _ = strconv.ParseInt(raw, 10, 64)
		case key == "used_cpu_sys":
			sys, _ = strconv.ParseFloat(raw, 64)
		case key == "used_cpu_user":
			user, _ = strconv.ParseFloat(raw, 64)
		case key == "total_net_input_bytes":
			result.networkRead, _ = strconv.ParseInt(raw, 10, 64)
		case key == "total_net_output_bytes":
			result.networkWrite, _ = strconv.ParseInt(raw, 10, 64)
		case key == "connected_clients":
			result.connections, _ = strconv.ParseInt(raw, 10, 64)
		case key == "total_commands_processed":
			result.operations, _ = strconv.ParseInt(raw, 10, 64)
			result.metrics[key] = float64(result.operations)
		case strings.HasPrefix(key, "cmdstat_eval"):
			result.scriptMicroseconds += redisCommandStat(line, "usec")
		case redisMetricWanted(key):
			if value, parseErr := strconv.ParseFloat(raw, 64); parseErr == nil {
				result.metrics[key] = value
			}
		}
	}
	result.cpuSeconds = sys + user
	result.metrics["cmdstat_eval_usec"] = float64(result.scriptMicroseconds)
	return result, nil
}

func redisMetricWanted(name string) bool {
	switch name {
	case "total_commands_processed",
		"instantaneous_ops_per_sec",
		"blocked_clients",
		"connected_clients",
		"evicted_keys",
		"keyspace_hits",
		"keyspace_misses",
		"total_net_input_bytes",
		"total_net_output_bytes":
		return true
	default:
		return false
	}
}

func (s *sampler) samplePrometheus(sec int) {
	for name, endpoint := range s.metrics {
		samples, err := scrapePrometheus(endpoint)
		if err != nil {
			continue
		}
		for metric, value := range samples {
			s.addMetric(MetricSample{Sec: sec, Name: name, Metric: metric, Value: value})
		}
	}
}

func scrapePrometheus(endpoint string) (map[string]float64, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(endpoint)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // read-only probe
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics status %d", resp.StatusCode)
	}
	out := make(map[string]float64)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		metric, raw, ok := strings.Cut(line, " ")
		if !ok || !prometheusMetricWanted(metric) {
			continue
		}
		value, parseErr := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if parseErr == nil {
			out[metric] = value
		}
	}
	return out, scanner.Err()
}

func prometheusMetricWanted(metric string) bool {
	name := metric
	if i := strings.IndexByte(name, '{'); i >= 0 {
		name = name[:i]
	}
	if strings.HasPrefix(name, "chronicle_read_") ||
		strings.HasPrefix(name, "chronicle_sse_") ||
		strings.HasPrefix(name, "chronicle_segment_") {
		return true
	}
	switch name {
	case "process_resident_memory_bytes",
		"process_cpu_seconds_total",
		"go_memstats_alloc_bytes_total",
		"go_memstats_heap_alloc_bytes",
		"go_memstats_heap_inuse_bytes",
		"go_gc_heap_allocs_bytes_total",
		"go_gc_heap_live_bytes",
		"go_gc_cycles_total",
		"go_gc_duration_seconds_sum",
		"go_gc_duration_seconds_count":
		return true
	default:
		return false
	}
}

func readRedisBulk(r *bufio.Reader) ([]byte, error) {
	header, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(header, "$") {
		return nil, fmt.Errorf("unexpected INFO reply %q", header)
	}
	n, err := strconv.Atoi(strings.TrimSpace(header[1:]))
	if err != nil || n <= 0 {
		return nil, fmt.Errorf("bad INFO length %q", header)
	}
	buf := make([]byte, n)
	if _, err := readFull(r, buf); err != nil {
		return nil, err
	}
	// Consume the bulk-string CRLF so the next pipelined response starts at its
	// own header.
	trailer, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if trailer != "\r\n" {
		return nil, fmt.Errorf("bad INFO trailer %q", trailer)
	}
	return buf, nil
}

func redisCommandStat(line, field string) int64 {
	_, values, ok := strings.Cut(line, ":")
	if !ok {
		return 0
	}
	for _, value := range strings.Split(values, ",") {
		key, number, found := strings.Cut(value, "=")
		if found && key == field {
			parsed, _ := strconv.ParseInt(number, 10, 64)
			return parsed
		}
	}
	return 0
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
