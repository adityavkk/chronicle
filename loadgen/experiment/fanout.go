package experiment

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"

	"gopkg.in/yaml.v3"
)

//go:embed templates/fanout-job.yaml.tmpl
var fanoutJobTmpl string

// FanoutSpec is the gate #2 experiment spec: it drives wide-stream appends and
// measures chronicle_fanout_seconds p99 at varying subscriber counts S.
type FanoutSpec struct {
	Name         string     `yaml:"name"`
	LoadgenImage string     `yaml:"loadgen_image"`
	SUT          SUT        `yaml:"sut"`
	Fanout       FanoutLoad `yaml:"fanout"`
	SLO          FanoutSLO  `yaml:"slo"`
}

// FanoutLoad configures the fanoutscale workload.
type FanoutLoad struct {
	// Subs is S — the number of subscriptions linked to the sentinel stream.
	// Run the job once per target S value (e.g. 2, 4, 8, 256).
	Subs     int    `yaml:"subs"`
	Rate     string `yaml:"rate"`     // e.g. "10/s"
	Dispatch string `yaml:"dispatch"` // "pull-wake" or "webhook"
	Warmup   string `yaml:"warmup"`   // e.g. "30s"
	Measure  string `yaml:"measure"`  // e.g. "120s"
}

// FanoutSLO is the pass/fail gate for a fanout run.
type FanoutSLO struct {
	FanoutP99Ms float64 `yaml:"fanout_p99_ms"`
}

// LoadFanout parses, defaults, and validates a FanoutSpec.
func LoadFanout(data []byte) (FanoutSpec, error) {
	var s FanoutSpec
	if err := yaml.Unmarshal(data, &s); err != nil {
		return FanoutSpec{}, err
	}
	s.applyDefaults()
	if err := s.validate(); err != nil {
		return FanoutSpec{}, err
	}
	return s, nil
}

func (s *FanoutSpec) applyDefaults() {
	if s.Name == "" {
		s.Name = "fanout-gate2"
	}
	if s.SUT.Namespace == "" {
		s.SUT.Namespace = "chronicle-gate2"
	}
	if s.SUT.Replicas <= 0 {
		s.SUT.Replicas = 1
	}
	if s.SUT.SweepInterval == "" {
		s.SUT.SweepInterval = "2s"
	}
	if s.SUT.CPU == "" {
		s.SUT.CPU = "2"
	}
	if s.SUT.Memory == "" {
		s.SUT.Memory = "2Gi"
	}
	if s.Fanout.Rate == "" {
		s.Fanout.Rate = "10/s"
	}
	if s.Fanout.Dispatch == "" {
		s.Fanout.Dispatch = "pull-wake"
	}
	if s.Fanout.Warmup == "" {
		s.Fanout.Warmup = "30s"
	}
	if s.Fanout.Measure == "" {
		s.Fanout.Measure = "120s"
	}
	if s.Fanout.Subs <= 0 {
		s.Fanout.Subs = 4
	}
}

func (s FanoutSpec) validate() error {
	if s.SUT.Image == "" {
		return fmt.Errorf("sut.image is required")
	}
	if s.LoadgenImage == "" {
		return fmt.Errorf("loadgen_image is required")
	}
	return nil
}

// FanoutRendered holds the SUT manifest + fanoutscale Job manifest for one run.
type FanoutRendered struct {
	SUTManifest string
	JobManifest string
	TFVars      string
}

// Render produces the SUT manifest (reusing the sweep SUT template) and the
// fanoutscale Job manifest. TFVars references the CLUSTER Terraform resource.
func (s FanoutSpec) Render() (FanoutRendered, error) {
	// Reuse the sweep SUT template — the SUT (chronicle) is identical.
	sutOut, err := render(sutTmpl, s.asSpec())
	if err != nil {
		return FanoutRendered{}, fmt.Errorf("sut manifest: %w", err)
	}
	job, err := s.renderFanoutJob()
	if err != nil {
		return FanoutRendered{}, fmt.Errorf("fanout job manifest: %w", err)
	}
	tf, err := render(tfvarsTmpl, s.asSpec())
	if err != nil {
		return FanoutRendered{}, fmt.Errorf("tfvars: %w", err)
	}
	return FanoutRendered{SUTManifest: sutOut, JobManifest: job, TFVars: tf}, nil
}

// asSpec converts to a minimal Spec so the existing SUT + TFVars templates can
// be reused without duplication.
func (s FanoutSpec) asSpec() Spec {
	return Spec{
		Name:         s.Name,
		LoadgenImage: s.LoadgenImage,
		SUT:          s.SUT,
	}
}

func (s FanoutSpec) renderFanoutJob() (string, error) {
	t, err := template.New("fanout-job").Option("missingkey=error").Parse(fanoutJobTmpl)
	if err != nil {
		return "", err
	}
	var b bytes.Buffer
	if err := t.Execute(&b, s); err != nil {
		return "", err
	}
	return b.String(), nil
}
