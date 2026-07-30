package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

type kubePod struct {
	name  string
	uid   string
	ready bool
}

const slowReaderStateFile = "/tmp/chronicle-sse-slow.state"

type podMetricSample struct {
	pod   kubePod
	value float64
}

// startInClusterResumingReader opens a connection from the Redis pod directly
// to Chronicle's ClusterIP service. It pauses reads long enough to fall behind
// the replay ring, then drains the socket, records its last control offset, and
// reconnects from that offset. This proves the lag and resume path without
// k3d's host load-balancer buffering the stalled connection.
func startInClusterResumingReader(nem *nemesis, stream string) error {
	out, err := nem.kubectl(
		"exec",
		"deploy/redis",
		"--",
		"sh",
		"-c",
		inClusterResumingReaderShell(stream),
	)
	if err != nil {
		return fmt.Errorf("start direct in-cluster resuming reader: %w: %s", err, out)
	}
	nem.record("slow-reader-resume-direct")
	return nil
}

func inClusterResumingReaderShell(stream string) string {
	path := strings.TrimPrefix(stream, "/")
	firstAWK := shellSingleQuote(slowReaderControlAWK(1, slowReaderStateFile))
	secondAWK := shellSingleQuote(slowReaderControlAWK(2, slowReaderStateFile))
	return fmt.Sprintf(
		"state=%s; rm -f \"$state\"; "+
			"( request=\"GET /v1/stream/%s?offset=-1&live=sse HTTP/1.1\\r\\n"+
			"Host: chronicle\\r\\nAccept: text/event-stream\\r\\nConnection: close\\r\\n\\r\\n\"; "+
			"( printf '%%b' \"$request\"; sleep 10 ) | nc chronicle 4437 | "+
			"( sleep 2; awk %s ); "+
			"first=$(cat \"$state\") || exit 1; set -- $first; "+
			"[ \"$1\" = 1 ] && [ \"$3\" = ready ] && [ \"$2\" != -1 ] || exit 1; "+
			"offset=$2; "+
			"request=\"GET /v1/stream/%s?offset=$offset&live=sse HTTP/1.1\\r\\n"+
			"Host: chronicle\\r\\nAccept: text/event-stream\\r\\nConnection: close\\r\\n\\r\\n\"; "+
			"( printf '%%b' \"$request\"; sleep 120 ) | nc chronicle 4437 | awk %s "+
			") >/tmp/chronicle-sse-slow.log 2>&1 &",
		strconv.Quote(slowReaderStateFile),
		path,
		firstAWK,
		path,
		secondAWK,
	)
}

// slowReaderControlAWK writes a ready state only after the same response has
// supplied both an HTTP 200 status and a durable SSE control offset.
func slowReaderControlAWK(session int, stateFile string) string {
	return fmt.Sprintf(`
BEGIN {
  status = 0
  state = %s
  session = %d
  prefix = "\"streamNextOffset\":\""
}
NR == 1 {
  sub(/\r$/, "", $0)
  if ($0 ~ /^HTTP\/1\.[01] 200([[:space:]]|$)/) status = 200
}
{
  start = index($0, prefix)
  if (status == 200 && start > 0) {
    rest = substr($0, start + length(prefix))
    finish = index(rest, "\"")
    if (finish > 1) {
      offset = substr(rest, 1, finish - 1)
      print session " " offset " ready" > state
      close(state)
    }
  }
}
`, strconv.Quote(stateFile), session)
}

func shellSingleQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

// startInClusterStuckReader never drains its response. It is separate from the
// resuming reader so the write-deadline and replay-lag policies are each
// required to fire rather than allowing either one to satisfy a combined gate.
func startInClusterStuckReader(nem *nemesis, stream string) error {
	out, err := nem.kubectl(
		"exec",
		"deploy/redis",
		"--",
		"sh",
		"-c",
		inClusterStuckReaderShell(stream),
	)
	if err != nil {
		return fmt.Errorf("start direct in-cluster stuck reader: %w: %s", err, out)
	}
	nem.record("stuck-reader-direct")
	return nil
}

func inClusterStuckReaderShell(stream string) string {
	request := fmt.Sprintf(
		"GET /v1/stream/%s?offset=-1&live=sse HTTP/1.1\\r\\n"+
			"Host: chronicle\\r\\n"+
			"Accept: text/event-stream\\r\\n"+
			"Connection: close\\r\\n\\r\\n",
		stream,
	)
	return fmt.Sprintf(
		"( ( printf '%%b' %s; sleep 120 ) | nc chronicle 4437 | sleep 120 ) "+
			">/tmp/chronicle-sse-slow.log 2>&1 &",
		strconv.Quote(request),
	)
}

func waitSlowReaderResumed(nem *nemesis, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		out, err := nem.kubectl(
			"exec",
			"deploy/redis",
			"--",
			"sh",
			"-c",
			"cat "+slowReaderStateFile,
		)
		if err == nil {
			last = strings.TrimSpace(string(out))
			session, offset, parseErr := parseSlowReaderState(last)
			if parseErr == nil && session == 2 && offset != "-1" {
				nem.record("slow-reader-resumed")
				return nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("slow reader did not reconnect from a durable control offset within %s (last %q)", timeout, last)
}

func parseSlowReaderState(raw string) (int, string, error) {
	fields := strings.Fields(raw)
	if len(fields) != 3 || fields[2] != "ready" {
		return 0, "", fmt.Errorf("invalid slow-reader state %q", raw)
	}
	session, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, "", fmt.Errorf("invalid slow-reader session %q: %w", fields[0], err)
	}
	if fields[1] == "" || fields[1] == "-1" {
		return 0, "", fmt.Errorf("invalid slow-reader offset %q", fields[1])
	}
	if _, err := store.ParseOffset(fields[1]); err != nil {
		return 0, "", fmt.Errorf("invalid slow-reader offset %q: %w", fields[1], err)
	}
	return session, fields[1], nil
}

func listKubePods(nem *nemesis, selector string) ([]kubePod, error) {
	out, err := nem.kubectl("get", "pods", "-l", selector, "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("list pods %q: %w: %s", selector, err, out)
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
				UID  string `json:"uid"`
			} `json:"metadata"`
			Status struct {
				Phase             string `json:"phase"`
				ContainerStatuses []struct {
					Ready bool `json:"ready"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, fmt.Errorf("decode pods %q: %w", selector, err)
	}
	pods := make([]kubePod, 0, len(list.Items))
	for _, item := range list.Items {
		ready := item.Status.Phase == "Running" && len(item.Status.ContainerStatuses) > 0
		for _, status := range item.Status.ContainerStatuses {
			ready = ready && status.Ready
		}
		pods = append(pods, kubePod{
			name:  item.Metadata.Name,
			uid:   item.Metadata.UID,
			ready: ready,
		})
	}
	return pods, nil
}

func replaceOnePod(nem *nemesis, selector, action string, timeout time.Duration) error {
	before, err := listKubePods(nem, selector)
	if err != nil {
		return err
	}
	var target *kubePod
	for i := range before {
		if before[i].ready {
			target = &before[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("no ready pod for %q", selector)
	}
	out, err := nem.kubectl(
		"delete",
		"pod",
		target.name,
		"--grace-period=0",
		"--force",
		"--wait=false",
	)
	if err != nil {
		return fmt.Errorf("delete pod %s: %w: %s", target.name, err, out)
	}
	if err := waitForReplacementPods(nem, selector, map[string]struct{}{target.uid: {}}, len(before), timeout); err != nil {
		return err
	}
	nem.record(action)
	return nil
}

func replaceChroniclePodServingClients(
	nem *nemesis,
	action string,
	timeout time.Duration,
) error {
	before, err := listKubePods(nem, "app=chronicle")
	if err != nil {
		return err
	}
	clientCounts := make(map[string]float64, len(before))
	for _, pod := range before {
		if !pod.ready {
			continue
		}
		count, err := readPodMetric(nem, pod, "chronicle_sse_clients", "")
		if err != nil {
			return err
		}
		clientCounts[pod.name] = count
	}
	target := highestMetricPod(before, clientCounts)
	if target == nil || clientCounts[target.name] <= 0 {
		return fmt.Errorf("no ready Chronicle pod has an active SSE client")
	}
	out, err := nem.kubectl(
		"delete",
		"pod",
		target.name,
		"--grace-period=0",
		"--force",
		"--wait=false",
	)
	if err != nil {
		return fmt.Errorf("delete client-serving pod %s: %w: %s", target.name, err, out)
	}
	if err := waitForReplacementPods(
		nem,
		"app=chronicle",
		map[string]struct{}{target.uid: {}},
		len(before),
		timeout,
	); err != nil {
		return err
	}
	nem.record(action)
	return nil
}

func highestMetricPod(pods []kubePod, values map[string]float64) *kubePod {
	var selected *kubePod
	for i := range pods {
		if !pods[i].ready {
			continue
		}
		if selected == nil || values[pods[i].name] > values[selected.name] {
			selected = &pods[i]
		}
	}
	return selected
}

func replaceAllPods(nem *nemesis, selector, action string, timeout time.Duration) error {
	before, err := listKubePods(nem, selector)
	if err != nil {
		return err
	}
	if len(before) == 0 {
		return fmt.Errorf("no pods for %q", selector)
	}
	oldUIDs := make(map[string]struct{}, len(before))
	for _, pod := range before {
		oldUIDs[pod.uid] = struct{}{}
	}
	out, err := nem.kubectl(
		"delete",
		"pods",
		"-l",
		selector,
		"--grace-period=0",
		"--force",
		"--wait=false",
	)
	if err != nil {
		return fmt.Errorf("delete pods %q: %w: %s", selector, err, out)
	}
	if err := waitForReplacementPods(nem, selector, oldUIDs, len(before), timeout); err != nil {
		return err
	}
	nem.record(action)
	return nil
}

func waitForReplacementPods(
	nem *nemesis,
	selector string,
	oldUIDs map[string]struct{},
	wantReady int,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pods, err := listKubePods(nem, selector)
		if err == nil {
			ready := 0
			oldPresent := false
			for _, pod := range pods {
				if pod.ready {
					ready++
				}
				if _, ok := oldUIDs[pod.uid]; ok {
					oldPresent = true
				}
			}
			if ready >= wantReady && !oldPresent {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf(
		"pods %q did not replace %d old UID(s) with %d ready pod(s) within %s",
		selector,
		len(oldUIDs),
		wantReady,
		timeout,
	)
}

func sumPodMetric(nem *nemesis, metric, labelFragment string) (float64, error) {
	pods, err := listKubePods(nem, "app=chronicle")
	if err != nil {
		return 0, err
	}
	var total float64
	ready := 0
	for _, pod := range pods {
		if !pod.ready {
			continue
		}
		ready++
		value, err := readPodMetric(nem, pod, metric, labelFragment)
		if err != nil {
			return 0, err
		}
		total += value
	}
	if ready == 0 {
		return 0, fmt.Errorf("no ready Chronicle pods to scrape")
	}
	return total, nil
}

func samplePodMetric(
	nem *nemesis,
	metric string,
	labelFragment string,
) (map[string]podMetricSample, error) {
	pods, err := listKubePods(nem, "app=chronicle")
	if err != nil {
		return nil, err
	}
	samples := make(map[string]podMetricSample, len(pods))
	for _, pod := range pods {
		if !pod.ready {
			continue
		}
		value, err := readPodMetric(nem, pod, metric, labelFragment)
		if err != nil {
			return nil, err
		}
		samples[pod.name] = podMetricSample{pod: pod, value: value}
	}
	if len(samples) == 0 {
		return nil, fmt.Errorf("no ready Chronicle pods to scrape")
	}
	return samples, nil
}

var errNoPodMetricIncrease = errors.New("no pod metric increased")

func singlePodMetricIncrease(
	before map[string]podMetricSample,
	after map[string]podMetricSample,
) (kubePod, float64, error) {
	if len(before) != len(after) {
		return kubePod{}, 0, fmt.Errorf(
			"chronicle metric topology changed from %d to %d ready pods",
			len(before),
			len(after),
		)
	}
	var selected *podMetricSample
	for name, prior := range before {
		current, ok := after[name]
		if !ok {
			return kubePod{}, 0, fmt.Errorf("chronicle metric pod %s disappeared", name)
		}
		if current.pod.uid != prior.pod.uid {
			return kubePod{}, 0, fmt.Errorf("chronicle metric pod %s changed UID", name)
		}
		if current.value <= prior.value {
			continue
		}
		if current.value-prior.value != 1 {
			return kubePod{}, 0, fmt.Errorf(
				"metric on pod %s increased by %.0f, want exactly 1",
				name,
				current.value-prior.value,
			)
		}
		if selected != nil {
			return kubePod{}, 0, fmt.Errorf(
				"metric increased on both %s and %s",
				selected.pod.name,
				name,
			)
		}
		copy := current
		selected = &copy
	}
	if selected == nil {
		return kubePod{}, 0, errNoPodMetricIncrease
	}
	return selected.pod, selected.value, nil
}

func waitForSinglePodMetricIncrease(
	nem *nemesis,
	metric string,
	labelFragment string,
	before map[string]podMetricSample,
	timeout time.Duration,
) (kubePod, float64, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		after, err := samplePodMetric(nem, metric, labelFragment)
		if err == nil {
			pod, value, selectErr := singlePodMetricIncrease(before, after)
			if selectErr == nil {
				return pod, value, nil
			}
			if !errors.Is(selectErr, errNoPodMetricIncrease) {
				return kubePod{}, 0, selectErr
			}
			lastErr = selectErr
		} else {
			lastErr = err
		}
		time.Sleep(250 * time.Millisecond)
	}
	return kubePod{}, 0, fmt.Errorf(
		"metric %s did not identify exactly one serving pod within %s: %w",
		metric,
		timeout,
		lastErr,
	)
}

func readPodMetric(
	nem *nemesis,
	pod kubePod,
	metric string,
	labelFragment string,
) (float64, error) {
	path := fmt.Sprintf(
		"/api/v1/namespaces/%s/pods/%s:9090/proxy/metrics",
		nem.ns,
		pod.name,
	)
	out, err := nem.kubectl("get", "--raw", path)
	if err != nil {
		return 0, fmt.Errorf("scrape metrics from %s: %w: %s", pod.name, err, out)
	}
	return parsePrometheusMetric(string(out), metric, labelFragment), nil
}

func parsePrometheusMetric(body, metric, labelFragment string) float64 {
	var total float64
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, metric) {
			continue
		}
		if len(line) > len(metric) {
			next := line[len(metric)]
			if next != '{' && next != ' ' && next != '\t' {
				continue
			}
		}
		if labelFragment != "" && !strings.Contains(line, labelFragment) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err == nil {
			total += value
		}
	}
	return total
}

func waitPodMetricIncrease(
	nem *nemesis,
	pod kubePod,
	metric string,
	labelFragment string,
	before float64,
	timeout time.Duration,
) (float64, error) {
	deadline := time.Now().Add(timeout)
	var last float64
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = readPodMetric(nem, pod, metric, labelFragment)
		if lastErr == nil && last > before {
			return last, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	message := fmt.Sprintf(
		"metric %s{%s} on pod %s did not increase above %.0f within %s (last %.0f)",
		metric,
		labelFragment,
		pod.name,
		before,
		timeout,
		last,
	)
	if lastErr != nil {
		return last, fmt.Errorf("%s: %w", message, lastErr)
	}
	return last, errors.New(message)
}

func timeoutAndClientDropObserved(
	timeoutBefore, timeoutAfter, clientsBefore, clientsAfter float64,
) bool {
	return timeoutAfter > timeoutBefore && clientsAfter < clientsBefore
}

func waitPodTimeoutAndClientDrop(
	nem *nemesis,
	pod kubePod,
	timeoutBefore float64,
	clientsBefore float64,
	timeout time.Duration,
) (float64, float64, error) {
	deadline := time.Now().Add(timeout)
	var timeoutAfter, clientsAfter float64
	var lastErr error
	for time.Now().Before(deadline) {
		timeoutAfter, lastErr = readPodMetric(
			nem,
			pod,
			"chronicle_sse_write_timeouts_total",
			"",
		)
		if lastErr == nil {
			clientsAfter, lastErr = readPodMetric(nem, pod, "chronicle_sse_clients", "")
		}
		if lastErr == nil && timeoutAndClientDropObserved(
			timeoutBefore,
			timeoutAfter,
			clientsBefore,
			clientsAfter,
		) {
			return timeoutAfter, clientsAfter, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	message := fmt.Sprintf(
		"pod %s did not show both a write timeout (%.0f -> %.0f) and client drop (%.0f -> %.0f) within %s",
		pod.name,
		timeoutBefore,
		timeoutAfter,
		clientsBefore,
		clientsAfter,
		timeout,
	)
	if lastErr != nil {
		return timeoutAfter, clientsAfter, fmt.Errorf("%s: %w", message, lastErr)
	}
	return timeoutAfter, clientsAfter, errors.New(message)
}

func waitMetricIncrease(
	nem *nemesis,
	metric string,
	labelFragment string,
	before float64,
	timeout time.Duration,
) (float64, error) {
	deadline := time.Now().Add(timeout)
	var last float64
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = sumPodMetric(nem, metric, labelFragment)
		if lastErr == nil && last > before {
			return last, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	message := fmt.Sprintf(
		"metric %s{%s} did not increase above %.0f within %s (last %.0f)",
		metric,
		labelFragment,
		before,
		timeout,
		last,
	)
	if lastErr != nil {
		return last, fmt.Errorf("%s: %w", message, lastErr)
	}
	return last, errors.New(message)
}

func waitSlowClientDisconnect(
	nem *nemesis,
	laggedBefore float64,
	writeTimeoutBefore float64,
	timeout time.Duration,
) (float64, float64, error) {
	deadline := time.Now().Add(timeout)
	var lagged, writeTimeouts float64
	var lastErr error
	for time.Now().Before(deadline) {
		lagged, lastErr = sumPodMetric(nem, "chronicle_sse_lagged_disconnects_total", "")
		if lastErr == nil {
			writeTimeouts, lastErr = sumPodMetric(nem, "chronicle_sse_write_timeouts_total", "")
		}
		if lastErr == nil && (lagged > laggedBefore || writeTimeouts > writeTimeoutBefore) {
			return lagged, writeTimeouts, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	message := fmt.Sprintf(
		"neither lagged disconnects (%.0f -> %.0f) nor write timeouts (%.0f -> %.0f) increased within %s",
		laggedBefore,
		lagged,
		writeTimeoutBefore,
		writeTimeouts,
		timeout,
	)
	if lastErr != nil {
		return lagged, writeTimeouts, fmt.Errorf("%s: %w", message, lastErr)
	}
	return lagged, writeTimeouts, errors.New(message)
}
