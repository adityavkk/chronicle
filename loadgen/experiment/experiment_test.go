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
