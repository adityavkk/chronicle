package segments

import "errors"

// FaultPoint names a deterministic crash/failure seam. Tests inject an error
// at each seam and assert that no incomplete generation becomes visible.
type FaultPoint string

const (
	// FaultCreate fails temporary object creation.
	FaultCreate FaultPoint = "create"
	// FaultWrite fails an immutable object write.
	FaultWrite FaultPoint = "write"
	// FaultSync fails an object durability barrier.
	FaultSync FaultPoint = "sync"
	// FaultRename fails an atomic file visibility step.
	FaultRename FaultPoint = "rename"
	// FaultUpload fails an object-store upload.
	FaultUpload FaultPoint = "upload"
	// FaultChecksum fails checksum validation.
	FaultChecksum FaultPoint = "checksum"
	// FaultManifest fails manifest publication.
	FaultManifest FaultPoint = "manifest"
	// FaultCache fails a cache fill.
	FaultCache FaultPoint = "cache"
	// FaultMigration fails a migration transition.
	FaultMigration FaultPoint = "migration"
	// FaultCutover fails the one-way cutover transition.
	FaultCutover FaultPoint = "cutover"
	// FaultRollback fails a serving-to-shadow rollback.
	FaultRollback FaultPoint = "rollback"
	// FaultGC fails a garbage-collection pass.
	FaultGC FaultPoint = "gc"
)

// FaultInjector is nil in production and deterministic in fault tests.
type FaultInjector interface {
	Hit(FaultPoint) error
}

// FaultFunc adapts a function to FaultInjector.
type FaultFunc func(FaultPoint) error

// Hit calls the adapted fault function.
func (f FaultFunc) Hit(p FaultPoint) error { return f(p) }

func hit(f FaultInjector, p FaultPoint) error {
	if f == nil {
		return nil
	}
	return f.Hit(p)
}

// FailOnce returns an injector which fails once at point.
func FailOnce(point FaultPoint) FaultInjector {
	fired := false
	return FaultFunc(func(got FaultPoint) error {
		if got == point && !fired {
			fired = true
			return errors.New("injected segment fault: " + string(point))
		}
		return nil
	})
}
