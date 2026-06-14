// Package scenario defines the declarative load-scenario model: the YAML
// schema, parsing, defaulting, and validation. It is pure — no I/O, no
// clocks; callers hand it bytes and get a validated Scenario or an error.
package scenario

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Scenario is a fully-validated load scenario.
type Scenario struct {
	Name        string  `yaml:"name"`
	Description string  `yaml:"description"`
	Target      Target  `yaml:"target"`
	Duration    D       `yaml:"duration"`
	Warmup      D       `yaml:"warmup"`
	Streams     Streams `yaml:"streams"`
	Writers     Writers `yaml:"writers"`
	Tailers     Tailers `yaml:"tailers"`
	Catchup     Catchup `yaml:"catchup"`
	Limits      Limits  `yaml:"limits"`
}

// Target identifies the system under test.
type Target struct {
	BaseURL    string `yaml:"base_url"`
	StreamRoot string `yaml:"stream_root"`
}

// Streams describes the stream population the scenario operates on.
type Streams struct {
	Count       int     `yaml:"count"`
	Prefix      string  `yaml:"prefix"`
	ContentType string  `yaml:"content_type"`
	Prefill     Prefill `yaml:"prefill"`
}

// Prefill seeds each stream with messages before measurement starts.
type Prefill struct {
	Messages     int `yaml:"messages"`
	MessageBytes int `yaml:"message_bytes"`
	BatchSize    int `yaml:"batch_size"`
}

// Writers describes the append workload. Each writer owns one stream and
// appends at Rate (open-loop: send times follow the pacing schedule
// regardless of response latency).
type Writers struct {
	PerStream    int    `yaml:"per_stream"`
	Rate         Rate   `yaml:"rate"`
	MessageBytes int    `yaml:"message_bytes"`
	Batch        int    `yaml:"batch"`
	Producer     string `yaml:"producer"` // "none" | "idempotent"
	CloseStreams bool   `yaml:"close_streams"`
}

// Tailers describes the live-read population attached to every stream.
type Tailers struct {
	SSEPerStream      int    `yaml:"sse_per_stream"`
	LongPollPerStream int    `yaml:"long_poll_per_stream"`
	From              string `yaml:"from"` // "now" | "start"
	ConnectRamp       D      `yaml:"connect_ramp"`
}

// Catchup describes cold catch-up reads (offset=-1 full-stream GETs)
// issued open-loop against randomly chosen streams.
type Catchup struct {
	Rate Rate `yaml:"rate"`
}

// Limits bounds client-side resource usage so an overloaded SUT degrades
// into recorded drops/errors instead of an unbounded goroutine pile-up.
type Limits struct {
	MaxInFlightAppends int `yaml:"max_in_flight_appends"`
	MaxInFlightCatchup int `yaml:"max_in_flight_catchup"`
	RequestTimeout     D   `yaml:"request_timeout"`
}

// D is a YAML-friendly duration ("30s", "1m").
type D struct{ time.Duration }

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *D) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = dur
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (d D) MarshalYAML() (any, error) { return d.String(), nil }

// Rate is an arrival rate in events per second, optionally ramping
// linearly from From to To over the scenario duration.
// Syntax: "30/s", "0.5/s", "120/m", or a ramp "5/s..50/s".
type Rate struct {
	From float64
	To   float64
}

// IsZero reports whether the rate is entirely absent.
func (r Rate) IsZero() bool { return r.From == 0 && r.To == 0 }

// IsRamp reports whether the rate changes over the run.
func (r Rate) IsRamp() bool { return r.From != r.To }

// String renders the rate in scenario syntax.
func (r Rate) String() string {
	if r.IsRamp() {
		return fmt.Sprintf("%g/s..%g/s", r.From, r.To)
	}
	return fmt.Sprintf("%g/s", r.From)
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (r *Rate) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := ParseRate(s)
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (r Rate) MarshalYAML() (any, error) { return r.String(), nil }

// ParseRate parses scenario rate syntax.
func ParseRate(s string) (Rate, error) {
	parse := func(part string) (float64, error) {
		var v float64
		var unit string
		if _, err := fmt.Sscanf(strings.TrimSpace(part), "%f/%s", &v, &unit); err != nil {
			return 0, fmt.Errorf("invalid rate %q: want <number>/<s|m> (e.g. 30/s)", part)
		}
		switch unit {
		case "s":
			return v, nil
		case "m":
			return v / 60, nil
		default:
			return 0, fmt.Errorf("invalid rate unit %q in %q: want s or m", unit, part)
		}
	}
	if from, to, ok := strings.Cut(s, ".."); ok {
		f, err := parse(from)
		if err != nil {
			return Rate{}, err
		}
		t, err := parse(to)
		if err != nil {
			return Rate{}, err
		}
		return Rate{From: f, To: t}, nil
	}
	v, err := parse(s)
	if err != nil {
		return Rate{}, err
	}
	return Rate{From: v, To: v}, nil
}

// Producer modes.
const (
	ProducerNone       = "none"
	ProducerIdempotent = "idempotent"
)

// Tailer start positions.
const (
	FromNow   = "now"
	FromStart = "start"
)

// Parse decodes scenario YAML strictly (unknown fields are errors),
// applies defaults, and validates.
func Parse(data []byte) (Scenario, error) {
	var s Scenario
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&s); err != nil {
		return Scenario{}, fmt.Errorf("scenario: %w", err)
	}
	s.applyDefaults()
	if err := s.Validate(); err != nil {
		return Scenario{}, err
	}
	return s, nil
}

func (s *Scenario) applyDefaults() {
	if s.Target.BaseURL == "" {
		s.Target.BaseURL = "http://localhost:4437"
	}
	if s.Target.StreamRoot == "" {
		s.Target.StreamRoot = "/v1/stream/"
	}
	if s.Streams.Count == 0 {
		s.Streams.Count = 1
	}
	if s.Streams.Prefix == "" {
		s.Streams.Prefix = "bench/" + s.Name
	}
	if s.Streams.ContentType == "" {
		s.Streams.ContentType = "application/json"
	}
	if s.Streams.Prefill.Messages > 0 {
		if s.Streams.Prefill.MessageBytes == 0 {
			s.Streams.Prefill.MessageBytes = 128
		}
		if s.Streams.Prefill.BatchSize == 0 {
			s.Streams.Prefill.BatchSize = 100
		}
	}
	if s.Writers.PerStream > 0 {
		if s.Writers.MessageBytes == 0 {
			s.Writers.MessageBytes = 128
		}
		if s.Writers.Batch == 0 {
			s.Writers.Batch = 1
		}
		if s.Writers.Producer == "" {
			s.Writers.Producer = ProducerNone
		}
	}
	if s.Tailers.From == "" {
		s.Tailers.From = FromNow
	}
	if s.Tailers.ConnectRamp.Duration == 0 {
		s.Tailers.ConnectRamp.Duration = 2 * time.Second
	}
	if s.Limits.MaxInFlightAppends == 0 {
		s.Limits.MaxInFlightAppends = 1024
	}
	if s.Limits.MaxInFlightCatchup == 0 {
		s.Limits.MaxInFlightCatchup = 256
	}
	if s.Limits.RequestTimeout.Duration == 0 {
		s.Limits.RequestTimeout.Duration = 10 * time.Second
	}
}

// Validate checks cross-field invariants and enum values.
func (s *Scenario) Validate() error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}

	check(s.Name != "", "name is required")
	check(strings.HasPrefix(s.Target.BaseURL, "http://") || strings.HasPrefix(s.Target.BaseURL, "https://"),
		"target.base_url %q must be an http(s) URL", s.Target.BaseURL)
	check(strings.HasPrefix(s.Target.StreamRoot, "/") && strings.HasSuffix(s.Target.StreamRoot, "/"),
		"target.stream_root %q must start and end with /", s.Target.StreamRoot)
	check(s.Duration.Duration > 0, "duration must be positive")
	check(s.Warmup.Duration >= 0, "warmup must be non-negative")
	check(s.Streams.Count > 0, "streams.count must be positive")
	check(s.Streams.Prefill.Messages >= 0, "streams.prefill.messages must be non-negative")

	check(s.Writers.PerStream >= 0, "writers.per_stream must be non-negative")
	if s.Writers.PerStream > 0 {
		check(!s.Writers.Rate.IsZero(), "writers.rate is required when writers.per_stream > 0")
		check(s.Writers.Rate.From >= 0 && s.Writers.Rate.To >= 0, "writers.rate must be non-negative")
		check(s.Writers.Rate.From > 0 || s.Writers.Rate.To > 0, "writers.rate must not be 0/s..0/s")
		check(s.Writers.MessageBytes >= 32, "writers.message_bytes must be >= 32 (payload header needs the room), got %d", s.Writers.MessageBytes)
		check(s.Writers.Batch >= 1, "writers.batch must be >= 1")
		check(s.Writers.Producer == ProducerNone || s.Writers.Producer == ProducerIdempotent,
			"writers.producer must be %q or %q, got %q", ProducerNone, ProducerIdempotent, s.Writers.Producer)
	}

	check(s.Tailers.SSEPerStream >= 0, "tailers.sse_per_stream must be non-negative")
	check(s.Tailers.LongPollPerStream >= 0, "tailers.long_poll_per_stream must be non-negative")
	check(s.Tailers.From == FromNow || s.Tailers.From == FromStart,
		"tailers.from must be %q or %q, got %q", FromNow, FromStart, s.Tailers.From)

	check(s.Catchup.Rate.From >= 0 && s.Catchup.Rate.To >= 0, "catchup.rate must be non-negative")
	if !s.Catchup.Rate.IsZero() {
		check(s.Streams.Prefill.Messages > 0 || s.Writers.PerStream > 0,
			"catchup.rate set but streams would be empty: add writers or prefill")
	}

	hasWork := s.Writers.PerStream > 0 || !s.Catchup.Rate.IsZero() ||
		s.Tailers.SSEPerStream > 0 || s.Tailers.LongPollPerStream > 0
	check(hasWork, "scenario defines no workload: add writers, tailers, or catchup")

	check(s.Streams.Prefill.MessageBytes == 0 || s.Streams.Prefill.MessageBytes >= 32,
		"streams.prefill.message_bytes must be >= 32, got %d", s.Streams.Prefill.MessageBytes)

	return errors.Join(errs...)
}

// TotalWriters is the writer goroutine population.
func (s *Scenario) TotalWriters() int { return s.Streams.Count * s.Writers.PerStream }

// TotalTailers is the live-reader connection population.
func (s *Scenario) TotalTailers() int {
	return s.Streams.Count * (s.Tailers.SSEPerStream + s.Tailers.LongPollPerStream)
}

// StreamName returns the path-relative name of stream i.
func (s *Scenario) StreamName(i int) string {
	return fmt.Sprintf("%s-%04d", s.Streams.Prefix, i)
}
