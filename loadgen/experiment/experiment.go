// Package experiment is the declarative system-under-test spec for the chronicle
// load-test rig: one YAML file pins the SUT (image, flags, replicas, resources,
// redis) plus the workload and the SLOs, and renders to the Kubernetes manifests
// and Terraform vars for one reproducible, diffable run.
package experiment

import (
	"bytes"
	_ "embed"
	"fmt"
	"net/url"
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
	Catchup      *Catchup   `yaml:"catchup,omitempty"`
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
	// RedisAddress is derived from RedisURL during Render for in-cluster INFO
	// sampling. It is not an independent spec field.
	RedisAddress string `yaml:"-"`
	CPU          string `yaml:"cpu"`
	Memory       string `yaml:"memory"`
	// ReadPageBytes is the returned payload target for a catch-up storage page.
	ReadPageBytes int `yaml:"read_page_bytes"`
	// Consistency / WaitReplicas / WaitTimeoutMs are the tunable-consistency tier
	// (issue #16) injected as CHRONICLE_CONSISTENCY_TIER / _WAIT_REPLICAS /
	// _WAIT_TIMEOUT_MS. Empty Consistency renders "" => chronicle defaults to Tier A
	// (no WAIT) — so existing specs are unchanged. The STANDARD_HA load spec sets
	// "B" + 1 replica to exercise the WAITAOF durability barrier under load.
	Consistency   string `yaml:"consistency"`
	WaitReplicas  int    `yaml:"wait_replicas"`
	WaitTimeoutMs int    `yaml:"wait_timeout_ms"`
}

// Catchup describes the issue-5 GKE suite. One job runs every ReaderCurve cell,
// followed by the mixed write-capacity cell, from the separate loadgen pool.
type Catchup struct {
	ReaderCurve         []int   `yaml:"reader_curve"`
	Streams             int     `yaml:"streams"`
	MessagesPerStream   int     `yaml:"messages_per_stream"`
	MessageBytes        int     `yaml:"message_bytes"`
	BatchSize           int     `yaml:"batch_size"`
	OfferedRate         string  `yaml:"offered_rate"`
	Warmup              string  `yaml:"warmup"`
	Duration            string  `yaml:"duration"`
	RequestTimeout      string  `yaml:"request_timeout"`
	MixedReaders        int     `yaml:"mixed_readers"`
	MixedWriterRate     string  `yaml:"mixed_writer_rate"`
	MixedMinAppendRate  float64 `yaml:"mixed_min_append_rate"`
	MixedMaxAppendP99MS float64 `yaml:"mixed_max_append_p99_ms"`
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
		s.Name = "chronicle-loadtest"
	}
	if s.SUT.Namespace == "" {
		s.SUT.Namespace = "chronicle-loadtest"
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
	if s.SUT.ReadPageBytes <= 0 {
		s.SUT.ReadPageBytes = 1 << 20
	}
	if s.Catchup == nil {
		if w, err := s.Workload.Prepared(); err == nil {
			s.Workload = w
		}
	} else {
		s.Catchup.applyDefaults()
	}
}

func (s Spec) validate() error {
	if s.SUT.Image == "" {
		return fmt.Errorf("sut.image is required")
	}
	if s.LoadgenImage == "" {
		return fmt.Errorf("loadgen_image is required (the image carrying the sweepscale binary)")
	}
	if s.Catchup == nil {
		if _, err := s.Workload.Prepared(); err != nil {
			return fmt.Errorf("workload: %w", err)
		}
	} else if err := s.Catchup.validate(); err != nil {
		return fmt.Errorf("catchup: %w", err)
	}
	return nil
}

func (c *Catchup) applyDefaults() {
	if len(c.ReaderCurve) == 0 {
		c.ReaderCurve = []int{8, 32, 128, 512}
	}
	if c.Streams <= 0 {
		c.Streams = 8
	}
	if c.MessagesPerStream <= 0 {
		c.MessagesPerStream = 4096
	}
	if c.MessageBytes <= 0 {
		c.MessageBytes = 4096
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 64
	}
	if c.OfferedRate == "" {
		c.OfferedRate = "200/s"
	}
	if c.Warmup == "" {
		c.Warmup = "5s"
	}
	if c.Duration == "" {
		c.Duration = "30s"
	}
	if c.RequestTimeout == "" {
		c.RequestTimeout = "300s"
	}
	if c.MixedReaders <= 0 {
		c.MixedReaders = 32
	}
	if c.MixedWriterRate == "" {
		c.MixedWriterRate = "5/s"
	}
	if c.MixedMinAppendRate == 0 {
		c.MixedMinAppendRate = 38
	}
	if c.MixedMaxAppendP99MS == 0 {
		c.MixedMaxAppendP99MS = 2000
	}
}

func (c Catchup) validate() error {
	for _, readers := range c.ReaderCurve {
		if readers <= 0 {
			return fmt.Errorf("reader_curve values must be positive")
		}
	}
	if c.Streams <= 0 || c.MessagesPerStream <= 0 || c.MessageBytes <= 0 || c.BatchSize <= 0 {
		return fmt.Errorf("stream and message dimensions must be positive")
	}
	if c.OfferedRate == "" || c.Warmup == "" || c.Duration == "" || c.RequestTimeout == "" {
		return fmt.Errorf("rate and duration fields are required")
	}
	if c.MixedReaders <= 0 || c.MixedWriterRate == "" {
		return fmt.Errorf("mixed reader and writer settings are required")
	}
	if c.MixedMinAppendRate <= 0 || c.MixedMaxAppendP99MS <= 0 {
		return fmt.Errorf("mixed append rate and p99 gates must be positive")
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
	if redisURL, err := url.Parse(s.SUT.RedisURL); err == nil {
		s.SUT.RedisAddress = redisURL.Host
	}
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
