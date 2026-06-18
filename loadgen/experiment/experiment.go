// Package experiment is the declarative system-under-test spec for the chronicle
// load-test rig: one YAML file pins the SUT (image, flags, replicas, resources,
// redis) plus the workload and the SLOs, and renders to the Kubernetes manifests
// and Terraform vars for one reproducible, diffable run.
package experiment

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"

	"gopkg.in/yaml.v3"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/sweep"
)

//go:embed templates/sut.yaml.tmpl
var sutTmpl string

//go:embed templates/job.yaml.tmpl
var jobTmpl string

//go:embed templates/tfvars.tmpl
var tfvarsTmpl string

// Spec is one load-test experiment: the SUT, the workload, and the SLO gate.
type Spec struct {
	Name         string     `yaml:"name"`
	LoadgenImage string     `yaml:"loadgen_image"`
	SUT          SUT        `yaml:"sut"`
	Workload     sweep.Spec `yaml:"workload"`
	SLO          SLO        `yaml:"slo"`
}

// SUT pins the system under test.
type SUT struct {
	Image         string `yaml:"image"`
	Replicas      int    `yaml:"replicas"`
	Namespace     string `yaml:"namespace"`
	SweepInterval string `yaml:"sweep_interval"`
	SweepBatch    int    `yaml:"sweep_batch"`
	// RedisURL is the managed Redis 8 URL. Leave empty to use the Memorystore
	// instance Terraform provisions (fill it in from the redis_url output), or
	// set it to production's managed Redis 8 so the numbers transfer.
	RedisURL string `yaml:"redis_url"`
	CPU      string `yaml:"cpu"`
	Memory   string `yaml:"memory"`
}

// SLO is the pass/fail gate the run asserts (enforced by sweepscale's exit code).
type SLO struct {
	SweepP99Ms    float64 `yaml:"sweep_p99_ms"`
	MaxSeedErrors int     `yaml:"max_seed_errors"`
}

// Load parses, defaults, and validates an experiment spec.
func Load(data []byte) (Spec, error) {
	var s Spec
	if err := yaml.Unmarshal(data, &s); err != nil {
		return Spec{}, err
	}
	s.applyDefaults()
	if err := s.validate(); err != nil {
		return Spec{}, err
	}
	return s, nil
}

func (s *Spec) applyDefaults() {
	if s.Name == "" {
		s.Name = "chronicle-loadtest-codex"
	}
	if s.SUT.Namespace == "" {
		s.SUT.Namespace = "chronicle-loadtest-codex"
	}
	if s.SUT.Replicas <= 0 {
		s.SUT.Replicas = 1
	}
	if s.SUT.SweepInterval == "" {
		s.SUT.SweepInterval = "30s"
	}
	if s.SUT.CPU == "" {
		s.SUT.CPU = "2"
	}
	if s.SUT.Memory == "" {
		s.SUT.Memory = "2Gi"
	}
	if w, err := s.Workload.Prepared(); err == nil {
		s.Workload = w
	}
}

func (s Spec) validate() error {
	if s.SUT.Image == "" {
		return fmt.Errorf("sut.image is required")
	}
	if s.LoadgenImage == "" {
		return fmt.Errorf("loadgen_image is required (the image carrying the sweepscale binary)")
	}
	if _, err := s.Workload.Prepared(); err != nil {
		return fmt.Errorf("workload: %w", err)
	}
	return nil
}

// Rendered holds the artifacts for one run.
type Rendered struct {
	SUTManifest string
	JobManifest string
	TFVars      string
}

// Render produces the SUT manifest, the load-job manifest, and the Terraform vars.
func (s Spec) Render() (Rendered, error) {
	sut, err := render(sutTmpl, s)
	if err != nil {
		return Rendered{}, fmt.Errorf("sut manifest: %w", err)
	}
	job, err := render(jobTmpl, s)
	if err != nil {
		return Rendered{}, fmt.Errorf("job manifest: %w", err)
	}
	tf, err := render(tfvarsTmpl, s)
	if err != nil {
		return Rendered{}, fmt.Errorf("tfvars: %w", err)
	}
	return Rendered{SUTManifest: sut, JobManifest: job, TFVars: tf}, nil
}

func render(tmpl string, s Spec) (string, error) {
	t, err := template.New("x").Option("missingkey=error").Parse(tmpl)
	if err != nil {
		return "", err
	}
	var b bytes.Buffer
	if err := t.Execute(&b, s); err != nil {
		return "", err
	}
	return b.String(), nil
}
