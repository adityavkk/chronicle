package experiment

import (
	"strings"
	"testing"
)

func TestLoadAndRender(t *testing.T) {
	spec, err := Load([]byte(`
name: t10k
loadgen_image: example/loadgen:dev
sut:
  image: example/chronicle:dev
  replicas: 1
  redis_url: redis://r:6379/0
  sweep_batch: 0
workload:
  subscriptions: 10000
  links_per_sub: 5
  dispatch: pull-wake
  warmup: 30s
  measure: 120s
slo:
  sweep_p99_ms: 1500
  max_seed_errors: 0
`))
	if err != nil {
		t.Fatal(err)
	}
	if spec.Workload.Subscriptions != 10000 || spec.Workload.Warmup.String() != "30s" {
		t.Fatalf("workload not parsed: %+v", spec.Workload)
	}

	r, err := spec.Render()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"kind: Deployment", "image: example/chronicle:dev", "--metrics-listen", "replicas: 1", "path: /readyz", "redis://r:6379/0"} {
		if !strings.Contains(r.SUTManifest, want) {
			t.Errorf("sut manifest missing %q", want)
		}
	}
	for _, want := range []string{"kind: Job", "image: example/loadgen:dev", "-subscriptions=10000", "-slo-p99-ms=1500", "-warmup=30s"} {
		if !strings.Contains(r.JobManifest, want) {
			t.Errorf("job manifest missing %q", want)
		}
	}
	if !strings.Contains(r.TFVars, "sut_node_count = 1") {
		t.Errorf("tfvars missing node count:\n%s", r.TFVars)
	}
}

func TestLoadRejectsMissingImage(t *testing.T) {
	if _, err := Load([]byte("loadgen_image: x\nworkload:\n  subscriptions: 1\n")); err == nil {
		t.Fatal("expected error for missing sut.image")
	}
}

func TestLoadAndRenderCatchupSuite(t *testing.T) {
	spec, err := Load([]byte(`
name: catch
loadgen_image: example/loadgen:dev
sut:
  image: example/chronicle:dev
  redis_url: redis://10.1.2.3:6379/0
  read_page_bytes: 1048576
  max_append_bytes: 16777216
catchup:
  reader_curve: [8, 512]
  streams: 8
  messages_per_stream: 4096
  message_bytes: 4096
  batch_size: 64
  offered_rate: 200/s
  warmup: 5s
  duration: 30s
  request_timeout: 300s
  mixed_readers: 32
  mixed_writer_rate: 5/s
  mixed_min_append_rate: 38
  mixed_max_append_p99_ms: 2000
`))
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := spec.Render()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`CHRONICLE_READ_PAGE_BYTES, value: "1048576"`,
		`CHRONICLE_MAX_APPEND_BYTES, value: "16777216"`,
		`CHRONICLE_SUBSCRIPTIONS, value: "false"`,
	} {
		if !strings.Contains(rendered.SUTManifest, want) {
			t.Errorf("SUT manifest missing %q", want)
		}
	}
	for _, want := range []string{
		"kind: ConfigMap",
		"kind: Job",
		"-catchup-readers 8",
		"-catchup-readers 512",
		"dsload gate-catchup",
		"-result /results/gke-1m-r512/catchup-paged/results.json",
		"-min-completions 1",
		"-sample-redis redis=10.1.2.3:6379",
		"nodeSelector: { role: loadgen }",
		"mixed-catchup-paged",
		"dsload gate-mixed",
		"-min-append-rate 38",
		"-max-append-p99-ms 2000",
	} {
		if !strings.Contains(rendered.JobManifest, want) {
			t.Errorf("job manifest missing %q", want)
		}
	}
}

func TestCatchupPageSizeLabels(t *testing.T) {
	for _, test := range []struct {
		bytes int
		want  string
	}{
		{bytes: 256 << 10, want: "256k"},
		{bytes: 1 << 20, want: "1m"},
		{bytes: 4 << 20, want: "4m"},
		{bytes: 123, want: "123b"},
	} {
		if got := readPageLabel(test.bytes); got != test.want {
			t.Errorf("readPageLabel(%d) = %q, want %q", test.bytes, got, test.want)
		}
	}
}

func TestLoadRejectsInvalidMixedCatchupGate(t *testing.T) {
	_, err := Load([]byte(`
name: catch
loadgen_image: example/loadgen:dev
sut:
  image: example/chronicle:dev
catchup:
  mixed_min_append_rate: -1
  mixed_max_append_p99_ms: 2000
`))
	if err == nil || !strings.Contains(err.Error(), "mixed append rate and p99 gates must be positive") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadRejectsNegativeAppendCeiling(t *testing.T) {
	_, err := Load([]byte(`
loadgen_image: example/loadgen:dev
sut:
  image: example/chronicle:dev
  max_append_bytes: -1
workload:
  subscriptions: 1
`))
	if err == nil || !strings.Contains(err.Error(), "max_append_bytes must be non-negative") {
		t.Fatalf("error = %v", err)
	}
}
