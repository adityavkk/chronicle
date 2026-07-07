// Package durable declares crash-boundary externalizations: a durable marker,
// an external side effect, its idempotence key, and the scanner that recovers it.
package durable

import (
	"context"
	"errors"
	"fmt"
)

// Marker names durable state that proves an external action may need recovery.
type Marker string

// Action names the side effect driven from a durable marker.
type Action string

// IdempotenceKey names the fence/key that makes a duplicate action safe.
type IdempotenceKey string

// ScanRuntime is the scanner's narrow capability set. A real scanner must query
// its durable marker and re-drive its external action through this interface.
type ScanRuntime interface {
	QueryMarker(Marker) error
	RedriveAction(Action) error
}

// ScannerFunc is executable recovery code for one durable marker.
type ScannerFunc func(context.Context, ScanRuntime) error

// Scanner declares executable recovery code. Its fields are private so callers
// cannot construct a string-only scanner that bypasses NewScanner.
type Scanner struct {
	name   string
	marker Marker
	action Action
	run    ScannerFunc
}

// NewScanner returns a scanner with executable scan/re-drive code.
func NewScanner(name string, marker Marker, action Action, run ScannerFunc) Scanner {
	if name == "" || marker == "" || action == "" || run == nil {
		panic("durable externalization scanner: name, marker, action, and run func are required")
	}
	return Scanner{name: name, marker: marker, action: action, run: run}
}

// Name returns the scanner's human-readable name.
func (s Scanner) Name() string { return s.name }

// MarkerQueried returns the marker this scanner is bound to.
func (s Scanner) MarkerQueried() Marker { return s.marker }

// ActionRedriven returns the action this scanner re-drives.
func (s Scanner) ActionRedriven() Action { return s.action }

// Run executes the scanner against a real recovery runtime.
func (s Scanner) Run(ctx context.Context, rt ScanRuntime) error {
	if s.run == nil {
		return errors.New("durable externalization scanner: missing run func")
	}
	return s.run(ctx, rt)
}

// NonVacuous reports whether this scanner has executable code that queries its
// marker and re-drives its action. It runs the code against an in-memory probe;
// a string-only scanner or a scanner that skips either step fails.
func (s Scanner) NonVacuous() bool { return s.Verify(context.Background()) == nil }

// Verify proves the scanner queries its marker and re-drives its action.
func (s Scanner) Verify(ctx context.Context) error {
	if s.name == "" || s.marker == "" || s.action == "" || s.run == nil {
		return errors.New("durable externalization scanner: incomplete")
	}
	probe := newScanProbe()
	if err := s.run(ctx, probe); err != nil {
		return err
	}
	if !probe.queried[s.marker] {
		return fmt.Errorf("durable externalization scanner %q did not query marker %q", s.name, s.marker)
	}
	if !probe.redriven[s.action] {
		return fmt.Errorf("durable externalization scanner %q did not re-drive action %q", s.name, s.action)
	}
	return nil
}

type scanProbe struct {
	queried  map[Marker]bool
	redriven map[Action]bool
}

func newScanProbe() *scanProbe {
	return &scanProbe{queried: map[Marker]bool{}, redriven: map[Action]bool{}}
}

func (p *scanProbe) QueryMarker(m Marker) error {
	p.queried[m] = true
	return nil
}

func (p *scanProbe) RedriveAction(a Action) error {
	p.redriven[a] = true
	return nil
}

// Externalization is the typed outbox contract. Fields are unexported so callers
// must use NewExternalization, which rejects missing marker/action/key/scanner.
type Externalization struct {
	marker  Marker
	action  Action
	key     IdempotenceKey
	scanner Scanner
}

// NewExternalization declares a complete durable externalization. It panics on
// missing pieces so illegal declarations fail at package initialization.
func NewExternalization(marker Marker, action Action, key IdempotenceKey, scanner Scanner) Externalization {
	if marker == "" || action == "" || key == "" {
		panic("durable externalization: marker, action, and idempotence key are required")
	}
	if err := scanner.Verify(context.Background()); err != nil {
		panic(err)
	}
	if scanner.MarkerQueried() != marker {
		panic("durable externalization: scanner does not query marker")
	}
	if scanner.ActionRedriven() != action {
		panic("durable externalization: scanner does not re-drive action")
	}
	return Externalization{marker: marker, action: action, key: key, scanner: scanner}
}

// Marker returns the durable state marker.
func (e Externalization) Marker() Marker { return e.marker }

// Action returns the external side effect.
func (e Externalization) Action() Action { return e.action }

// IdempotenceKey returns the duplicate-suppression key.
func (e Externalization) IdempotenceKey() IdempotenceKey { return e.key }

// Scanner returns the recovery scanner declaration.
func (e Externalization) Scanner() Scanner { return e.scanner }

// Complete reports whether every required piece is present and the scanner is
// tied to this marker/action pair.
func (e Externalization) Complete() bool {
	return e.marker != "" && e.action != "" && e.key != "" && e.scanner.NonVacuous() &&
		e.scanner.MarkerQueried() == e.marker && e.scanner.ActionRedriven() == e.action
}

// RunExternalAction gates a real side effect through this externalization.
func (e Externalization) RunExternalAction(run func() error) error {
	if !e.Complete() {
		return errors.New("durable externalization: incomplete")
	}
	if run == nil {
		return errors.New("durable externalization: nil external action")
	}
	return run()
}
