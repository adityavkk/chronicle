package run

import (
	"bufio"
	"context"
	"fmt"
	"net"
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
	Sec        int     `json:"sec"`
	Name       string  `json:"name"`
	RSSBytes   int64   `json:"rss_bytes"`
	CPUSeconds float64 `json:"cpu_seconds"`
}

// sampler polls SUT processes (via ps) and Redis (via INFO over TCP)
// once per second for the duration of the run. The load generator
// samples itself too, so "was the generator the bottleneck?" is
// answerable from the results file.
type sampler struct {
	pids  map[string]int
	redis map[string]string
	logf  func(string, ...any)

	mu  sync.Mutex
	out []ResourceSample
}

func newSampler(pids map[string]int, redis map[string]string, logf func(string, ...any)) *sampler {
	all := map[string]int{"loadgen": os.Getpid()}
	for k, v := range pids {
		all[k] = v
	}
	return &sampler{pids: all, redis: redis, logf: logf}
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
			}
		}
	}()
}

func (s *sampler) samples() []ResourceSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ResourceSample(nil), s.out...)
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
		s.add(ResourceSample{Sec: sec, Name: name, RSSBytes: rssKB * 1024, CPUSeconds: cpu})
	}
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
		mem, cpu, err := redisInfo(addr)
		if err != nil {
			continue
		}
		s.add(ResourceSample{Sec: sec, Name: name, RSSBytes: mem, CPUSeconds: cpu})
	}
}

func redisInfo(addr string) (usedMemory int64, cpuSeconds float64, err error) {
	conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close() //nolint:errcheck // read-only probe connection
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write([]byte("INFO\r\n")); err != nil {
		return 0, 0, err
	}
	r := bufio.NewReader(conn)
	header, err := r.ReadString('\n')
	if err != nil {
		return 0, 0, err
	}
	if !strings.HasPrefix(header, "$") {
		return 0, 0, fmt.Errorf("unexpected INFO reply %q", header)
	}
	n, err := strconv.Atoi(strings.TrimSpace(header[1:]))
	if err != nil || n <= 0 {
		return 0, 0, fmt.Errorf("bad INFO length %q", header)
	}
	buf := make([]byte, n)
	if _, err := readFull(r, buf); err != nil {
		return 0, 0, err
	}
	var sys, user float64
	for _, line := range strings.Split(string(buf), "\r\n") {
		switch {
		case strings.HasPrefix(line, "used_memory:"):
			usedMemory, _ = strconv.ParseInt(strings.TrimPrefix(line, "used_memory:"), 10, 64)
		case strings.HasPrefix(line, "used_cpu_sys:"):
			sys, _ = strconv.ParseFloat(strings.TrimPrefix(line, "used_cpu_sys:"), 64)
		case strings.HasPrefix(line, "used_cpu_user:"):
			user, _ = strconv.ParseFloat(strings.TrimPrefix(line, "used_cpu_user:"), 64)
		}
	}
	return usedMemory, sys + user, nil
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
