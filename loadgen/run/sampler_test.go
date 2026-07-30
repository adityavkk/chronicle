package run

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
)

func TestRedisCommandStat(t *testing.T) {
	line := "cmdstat_evalsha:calls=17,usec=421,usec_per_call=24.76,rejected_calls=0,failed_calls=0"
	if got := redisCommandStat(line, "usec"); got != 421 {
		t.Fatalf("usec = %d, want 421", got)
	}
	if got := redisCommandStat(line, "calls"); got != 17 {
		t.Fatalf("calls = %d, want 17", got)
	}
	if got := redisCommandStat("not-a-command-stat", "usec"); got != 0 {
		t.Fatalf("invalid line = %d, want 0", got)
	}
}

func TestReadRedisBulkConsumesTrailer(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("$3\r\none\r\n$3\r\ntwo\r\n"))
	first, err := readRedisBulk(reader)
	if err != nil || string(first) != "one" {
		t.Fatalf("first bulk = %q, err=%v", first, err)
	}
	second, err := readRedisBulk(reader)
	if err != nil || string(second) != "two" {
		t.Fatalf("second bulk = %q, err=%v", second, err)
	}
}

func TestRedisInfoDetailedAndSamplerKeepRichFields(t *testing.T) {
	const payload = "# Memory\r\n" +
		"used_memory:4096\r\n" +
		"# Clients\r\n" +
		"connected_clients:7\r\n" +
		"blocked_clients:2\r\n" +
		"# Stats\r\n" +
		"total_net_input_bytes:1100\r\n" +
		"total_net_output_bytes:2200\r\n" +
		"total_commands_processed:33\r\n" +
		"instantaneous_ops_per_sec:9\r\n" +
		"evicted_keys:1\r\n" +
		"# CPU\r\n" +
		"used_cpu_sys:1.25\r\n" +
		"used_cpu_user:2.5\r\n" +
		"# Commandstats\r\n" +
		"cmdstat_eval:calls=2,usec=40,usec_per_call=20\r\n" +
		"cmdstat_evalsha:calls=3,usec=60,usec_per_call=20\r\n"

	addr, wait := serveRedisInfo(t, payload)
	info, err := redisInfoDetailed(addr)
	if err != nil {
		t.Fatal(err)
	}
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	if info.usedMemory != 4096 || info.cpuSeconds != 3.75 ||
		info.networkRead != 1100 || info.networkWrite != 2200 ||
		info.connections != 7 || info.operations != 33 ||
		info.scriptMicroseconds != 100 {
		t.Fatalf("Redis INFO fields = %+v", info)
	}
	if info.metrics["instantaneous_ops_per_sec"] != 9 ||
		info.metrics["blocked_clients"] != 2 ||
		info.metrics["cmdstat_eval_usec"] != 100 {
		t.Fatalf("Redis INFO metrics = %+v", info.metrics)
	}

	addr, wait = serveRedisInfo(t, payload)
	s := newSampler(nil, map[string]string{"redis": addr}, nil, nil)
	s.sampleRedis(4)
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	resources := s.samples()
	if len(resources) != 1 || resources[0].NetworkReadBytes != 1100 ||
		resources[0].NetworkWriteBytes != 2200 ||
		resources[0].Connections != 7 ||
		resources[0].Operations != 33 ||
		resources[0].ScriptMicroseconds != 100 {
		t.Fatalf("resource samples = %+v", resources)
	}
	metrics := s.metricSamples()
	if len(metrics) == 0 {
		t.Fatal("sampleRedis emitted no metric samples")
	}
	foundScript := false
	for _, metric := range metrics {
		if metric.Metric == "cmdstat_eval_usec" && metric.Value == 100 {
			foundScript = true
		}
	}
	if !foundScript {
		t.Fatalf("metric samples omitted script time: %+v", metrics)
	}
}

func TestPrometheusMetricWantedKeepsIntegratedSeries(t *testing.T) {
	for _, metric := range []string{
		`chronicle_read_page_seconds_bucket{le="0.1"}`,
		"chronicle_sse_clients",
		"chronicle_segment_reads_total",
		"process_resident_memory_bytes",
	} {
		if !prometheusMetricWanted(metric) {
			t.Errorf("prometheusMetricWanted(%q) = false", metric)
		}
	}
	if prometheusMetricWanted("unrelated_total") {
		t.Fatal("unrelated metric was accepted")
	}
}

func serveRedisInfo(t *testing.T, payload string) (string, func() error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close() //nolint:errcheck // test server teardown
		request, readErr := bufio.NewReader(conn).ReadString('\n')
		if readErr != nil {
			done <- readErr
			return
		}
		if request != "INFO\r\n" {
			done <- fmt.Errorf("request = %q, want INFO", request)
			return
		}
		_, writeErr := fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(payload), payload)
		done <- writeErr
	}()
	return listener.Addr().String(), func() error {
		return errors.Join(<-done, listener.Close())
	}
}
