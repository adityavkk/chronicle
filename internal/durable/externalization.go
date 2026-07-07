// Package durable declares crash-boundary externalizations: a durable marker,
// an external side effect, its idempotence key, and the scanner that recovers it.
package durable

// Marker names durable state that proves an external action may need recovery.
type Marker string

// Action names the side effect driven from a durable marker.
type Action string

// IdempotenceKey names the fence/key that makes a duplicate action safe.
type IdempotenceKey string

// Scanner declares the recovery code path that finds a marker and re-drives its
// action. MarkerQueried and ActionRedriven are deliberately typed: a scanner that
// does not query the marker it is registered for, or does not re-drive the action,
// is vacuous.
type Scanner struct {
	Name           string
	MarkerQueried  Marker
	ActionRedriven Action
}

// NewScanner returns a non-vacuous scanner declaration.
func NewScanner(name string, marker Marker, action Action) Scanner {
	if name == "" || marker == "" || action == "" {
		panic("durable externalization scanner: name, marker, and action are required")
	}
	return Scanner{Name: name, MarkerQueried: marker, ActionRedriven: action}
}

// NonVacuous reports whether this scanner actually queries a marker and re-drives
// an action.
func (s Scanner) NonVacuous() bool {
	return s.Name != "" && s.MarkerQueried != "" && s.ActionRedriven != ""
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
	if !scanner.NonVacuous() {
		panic("durable externalization: scanner is required")
	}
	if scanner.MarkerQueried != marker {
		panic("durable externalization: scanner does not query marker")
	}
	if scanner.ActionRedriven != action {
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
		e.scanner.MarkerQueried == e.marker && e.scanner.ActionRedriven == e.action
}
