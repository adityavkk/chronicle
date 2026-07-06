package webhook

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// This file is the custody seam of issues #123/#126: where the subscription
// layer's key material comes from. The default source is the Redis store
// (generate-and-persist, first writer wins) — right for development, but on a
// shared data-plane Redis it means Redis read access == authority to forge
// any webhook signature or token (#123 blocker 2). A mounted secrets file
// (the Akeyless /etc/secrets pattern) breaks that: key material never touches
// Redis, and custody follows the platform's secret mount instead of the data
// plane. The rotation state machine (rotation.go) runs on this same seam.

// KeySource supplies the subscription layer's key material: both Ed25519
// families (#123: the wake-token key is purpose-separate from the
// webhook-envelope key by construction) and the HMAC token key. *RedisStore
// satisfies it (generate-and-persist); FileKeySource reads a mounted secrets
// file and never writes anywhere. Optional capabilities are separate
// interfaces consulted by type assertion: denylistSource, keyRefresher,
// retiredKeyReaper, keyDenylister, keyRotator (rotation.go).
type KeySource interface {
	// LoadSigningKey returns the active webhook-envelope signing key.
	LoadSigningKey(now time.Time) (SigningKey, error)
	// SigningKeys returns every published webhook-envelope key, active first.
	SigningKeys() ([]SigningKey, error)
	// LoadWakeKey returns the active wake-token signing key (#123).
	LoadWakeKey(now time.Time) (SigningKey, error)
	// WakeSigningKeys returns every published wake-token key, active first.
	WakeSigningKeys() ([]SigningKey, error)
	// LoadTokenKey returns the HMAC token key.
	LoadTokenKey() ([]byte, error)
}

var _ KeySource = (*RedisStore)(nil)

// minTokenKeyBytes is the floor for the HMAC-SHA256 token key (RFC 2104: a
// key shorter than the hash output weakens the MAC).
const minTokenKeyBytes = 32

// Signing-key statuses. Only the active key signs; retiring keys stay in the
// JWKS and the verification sets until their retire_after (rotation.go).
const (
	keyStatusActive   = "active"
	keyStatusRetiring = "retiring"
)

// keysFileDoc is the exact schema of a CHRONICLE_KEYS_FILE. kid is never
// stored — it is derived (RFC 7638) from the key itself, so the file cannot
// disagree with the JWKS about a key's identity.
type keysFileDoc struct {
	// TokenKey is the base64url (unpadded) HMAC token key, >= 32 bytes.
	TokenKey string `json:"token_key"`
	// SigningKeys lists the Ed25519 keys across both purposes ("webhook" and
	// "wake"); exactly one status=active key is required per purpose.
	SigningKeys []keysFileEntry `json:"signing_keys"`
	// Denylist is the emergency kid revocation list (#123): a listed kid
	// never verifies and never serves, even mid-overlap. Optional.
	Denylist []string `json:"denylist"`
}

type keysFileEntry struct {
	// Seed is the base64url (unpadded) 32-byte Ed25519 seed (RFC 8032).
	Seed string `json:"seed"`
	// CreatedAt is RFC3339; it feeds the SigningKey metadata.
	CreatedAt string `json:"created_at"`
	// Status is "active" or "retiring".
	Status string `json:"status"`
	// Purpose is "webhook" (the envelope/caller-token family) or "wake"
	// (the wake_token family, #123). An unknown purpose is rejected outright
	// so a file written for a newer chronicle never half-loads here.
	Purpose string `json:"purpose"`
	// RetireAfter (RFC3339, optional) is the rotation overlap deadline on a
	// retiring key: past it the key leaves the JWKS and the verification
	// sets. Rejected on active keys. Absent means the retiring key serves
	// until a deadline is stamped.
	RetireAfter string `json:"retire_after"`
}

// FileKeySource is the KeySource for a mounted secrets file. It is immutable
// after load; live pickup of a replaced file is the FileKeyWatcher's job.
// Values returned by its methods are copies, so a caller can never mutate
// the loaded material.
type FileKeySource struct {
	families map[KeyPurpose]keyFamilyFile
	tokenKey []byte
	denylist []string
	warnings []string
}

type keyFamilyFile struct {
	active SigningKey
	all    []SigningKey // active first
}

// checkKeysFilePerms enforces fail-closed custody on the mounted secrets
// file's mode (issue #126 hardening, #131): the file holds the Ed25519 signing
// keys and the HMAC token key, so anyone who can read it can forge every
// webhook signature and token — filesystem custody is only as strong as the
// mount's permissions.
//
// World access (other r/w/x) and any group-write bit are refused outright:
// never defensible. Group-READ is also refused by default — on a shared group
// it is read-to-forge — UNLESS allowGroupRead is set, the one documented
// exception: a non-root container reading a root-owned Kubernetes secret
// through a dedicated fsGroup, where the file is root:<fsGroup> and 0400 would
// be unreadable by the process (0440 is the minimum that works). Even under
// the exception it is a warning, never silent. Set the mount to 0400/0600
// otherwise (Kubernetes: secret volume defaultMode 0400).
func checkKeysFilePerms(path string, perm os.FileMode, allowGroupRead bool) ([]string, error) {
	if perm&0o007 != 0 {
		return nil, fmt.Errorf("keys file %s is world-accessible (mode %04o); it must not be readable, writable, or executable by other — set the mount to 0400 or 0600", path, perm)
	}
	if perm&0o020 != 0 {
		return nil, fmt.Errorf("keys file %s is group-writable (mode %04o); it must not be writable by group — set the mount to 0400 or 0600", path, perm)
	}
	if perm&0o040 != 0 {
		if !allowGroupRead {
			return nil, fmt.Errorf("keys file %s is group-readable (mode %04o); it holds signing and token key material, so on a shared group this is read-to-forge — set the mount to 0400 or 0600, or set CHRONICLE_KEYS_FILE_ALLOW_GROUP_READ=true only if a non-root container reads it via a dedicated fsGroup", path, perm)
		}
		return []string{fmt.Sprintf("keys file %s is group-readable (mode %04o); permitted by CHRONICLE_KEYS_FILE_ALLOW_GROUP_READ — ensure the group is a dedicated single-reader fsGroup, never a shared login group", path, perm)}, nil
	}
	return nil, nil
}

// LoadFileKeySource reads and strictly parses a keys file. Everything is
// fail-closed: a missing file, unknown field, short seed or token key, an
// unknown status or purpose, a retire_after on an active key, or a purpose
// without exactly one active key — each refuses to load rather than
// guessing. Error messages name fields and reasons but never echo key
// material. allowGroupRead opts into the group-readable exception (see
// checkKeysFilePerms); the strict default (false) refuses any group/world
// readability.
func LoadFileKeySource(path string, allowGroupRead bool) (*FileKeySource, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("keys file: %w", err)
	}
	warnings, err := checkKeysFilePerms(path, info.Mode().Perm(), allowGroupRead)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path) // #nosec G304 -- the operator-configured secrets mount path
	if err != nil {
		return nil, fmt.Errorf("keys file: %w", err)
	}

	var doc keysFileDoc
	if err := strictJSONUnmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("keys file %s: %w", path, err)
	}

	tokenKey, err := base64.RawURLEncoding.DecodeString(doc.TokenKey)
	if err != nil {
		return nil, fmt.Errorf("keys file %s: token_key: invalid base64url", path)
	}
	if len(tokenKey) < minTokenKeyBytes {
		return nil, fmt.Errorf("keys file %s: token_key: %d bytes, need at least %d", path, len(tokenKey), minTokenKeyBytes)
	}

	if len(doc.SigningKeys) == 0 {
		return nil, fmt.Errorf("keys file %s: signing_keys is empty", path)
	}
	actives := map[KeyPurpose]*SigningKey{}
	retiring := map[KeyPurpose][]SigningKey{}
	for i, e := range doc.SigningKeys {
		purpose, key, err := parseKeysFileEntry(e)
		if err != nil {
			return nil, fmt.Errorf("keys file %s: signing_keys[%d]: %w", path, i, err)
		}
		if key.Status == keyStatusActive {
			if actives[purpose] != nil {
				return nil, fmt.Errorf("keys file %s: more than one active %q signing key", path, purpose)
			}
			k := key
			actives[purpose] = &k
		} else {
			retiring[purpose] = append(retiring[purpose], key)
		}
	}
	// Both families are required: the Manager mints webhook envelopes AND
	// wake_tokens, and file custody must never fall back to Redis for one of
	// them (that would silently split custody across trust domains).
	families := make(map[KeyPurpose]keyFamilyFile, 2)
	for _, purpose := range []KeyPurpose{KeyPurposeWebhook, KeyPurposeWake} {
		active := actives[purpose]
		if active == nil {
			return nil, fmt.Errorf("keys file %s: no active %q signing key (both the webhook and wake families are required)", path, purpose)
		}
		families[purpose] = keyFamilyFile{
			active: *active,
			all:    append([]SigningKey{*active}, retiring[purpose]...),
		}
	}
	if families[KeyPurposeWebhook].active.Kid == families[KeyPurposeWake].active.Kid {
		return nil, fmt.Errorf("keys file %s: the webhook and wake active keys are the same key (RFC 8725 §2.8 purpose separation)", path)
	}

	return &FileKeySource{
		families: families,
		tokenKey: tokenKey,
		denylist: append([]string(nil), doc.Denylist...),
		warnings: warnings,
	}, nil
}

func parseKeysFileEntry(e keysFileEntry) (KeyPurpose, SigningKey, error) {
	purpose := KeyPurpose(e.Purpose)
	if purpose != KeyPurposeWebhook && purpose != KeyPurposeWake {
		return "", SigningKey{}, fmt.Errorf("unknown purpose %q (want %q or %q)", e.Purpose, KeyPurposeWebhook, KeyPurposeWake)
	}
	if e.Status != keyStatusActive && e.Status != keyStatusRetiring {
		return "", SigningKey{}, fmt.Errorf("unknown status %q (want %q or %q)", e.Status, keyStatusActive, keyStatusRetiring)
	}
	seed, err := base64.RawURLEncoding.DecodeString(e.Seed)
	if err != nil {
		return "", SigningKey{}, errors.New("seed: invalid base64url")
	}
	if len(seed) != ed25519.SeedSize {
		return "", SigningKey{}, fmt.Errorf("seed: %d bytes, want %d", len(seed), ed25519.SeedSize)
	}
	createdAt, err := time.Parse(time.RFC3339, e.CreatedAt)
	if err != nil {
		return "", SigningKey{}, errors.New("created_at: not RFC3339")
	}
	var retireAfter time.Time
	if e.RetireAfter != "" {
		if e.Status != keyStatusRetiring {
			return "", SigningKey{}, errors.New("retire_after: only valid on a retiring key")
		}
		retireAfter, err = time.Parse(time.RFC3339, e.RetireAfter)
		if err != nil {
			return "", SigningKey{}, errors.New("retire_after: not RFC3339")
		}
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return "", SigningKey{}, errors.New("seed: cannot derive public key")
	}
	return purpose, SigningKey{
		Kid:         deriveKid(pub),
		Public:      pub,
		Private:     priv,
		CreatedAt:   createdAt,
		Status:      e.Status,
		RetireAfter: retireAfter,
	}, nil
}

// cloneSigningKey deep-copies the key's Public/Private byte slices, so a
// returned SigningKey never aliases the FileKeySource's loaded material —
// copying the struct alone would share the backing arrays (issue #126
// hardening: the accessors below promise defensive copies, and this makes
// that true, so a caller that mutates a returned key cannot corrupt the
// source or a later reader).
func cloneSigningKey(k SigningKey) SigningKey {
	k.Public = append(ed25519.PublicKey(nil), k.Public...)
	k.Private = append(ed25519.PrivateKey(nil), k.Private...)
	return k
}

func cloneSigningKeys(ks []SigningKey) []SigningKey {
	out := make([]SigningKey, len(ks))
	for i, k := range ks {
		out[i] = cloneSigningKey(k)
	}
	return out
}

// LoadSigningKey returns the file's active webhook-envelope key. now is
// unused: the file, not the clock, decides which key is active.
func (s *FileKeySource) LoadSigningKey(time.Time) (SigningKey, error) {
	return cloneSigningKey(s.families[KeyPurposeWebhook].active), nil
}

// SigningKeys returns the webhook-envelope family, active first.
func (s *FileKeySource) SigningKeys() ([]SigningKey, error) {
	return cloneSigningKeys(s.families[KeyPurposeWebhook].all), nil
}

// LoadWakeKey returns the file's active wake-token key (#123).
func (s *FileKeySource) LoadWakeKey(time.Time) (SigningKey, error) {
	return cloneSigningKey(s.families[KeyPurposeWake].active), nil
}

// WakeSigningKeys returns the wake-token family, active first.
func (s *FileKeySource) WakeSigningKeys() ([]SigningKey, error) {
	return cloneSigningKeys(s.families[KeyPurposeWake].all), nil
}

// LoadTokenKey returns a copy of the file's HMAC token key.
func (s *FileKeySource) LoadTokenKey() ([]byte, error) {
	out := make([]byte, len(s.tokenKey))
	copy(out, s.tokenKey)
	return out, nil
}

// DenylistedKids returns the file's emergency kid revocations.
func (s *FileKeySource) DenylistedKids() ([]string, error) {
	return append([]string(nil), s.denylist...), nil
}

// Warnings are non-fatal custody findings from the load (today: overly broad
// file permissions), for the caller to log. Never key material.
func (s *FileKeySource) Warnings() []string {
	return append([]string(nil), s.warnings...)
}

// Kids returns the loaded key ids per purpose (active first) for startup
// logging — the one piece of key identity that is safe to log.
func (s *FileKeySource) Kids() []string {
	var out []string
	for _, purpose := range []KeyPurpose{KeyPurposeWebhook, KeyPurposeWake} {
		for _, k := range s.families[purpose].all {
			out = append(out, string(purpose)+":"+k.Kid+"("+k.Status+")")
		}
	}
	return out
}

// FileKeyWatcher wraps a FileKeySource with live pickup (#123 rotation with
// file custody): the Manager's key-reload loop calls Refresh each interval,
// which re-stats the file and reparses it when the mtime or size changed —
// so replacing the mounted secret rotates keys without a restart. A file
// that becomes invalid AFTER a good load logs and keeps the last good state
// (availability posture: a half-written mount update must not wipe a serving
// key set); a bad file at boot still refuses startup in NewFileKeyWatcher.
type FileKeyWatcher struct {
	path           string
	allowGroupRead bool
	log            *slog.Logger

	mu    sync.RWMutex
	cur   *FileKeySource
	mtime time.Time
	size  int64
}

// NewFileKeyWatcher loads the file (fail-closed: a bad file at boot is an
// error) and arms the watcher. allowGroupRead threads the #131 group-read
// exception through to every (re)load, including Refresh.
func NewFileKeyWatcher(path string, allowGroupRead bool, logger *slog.Logger) (*FileKeyWatcher, error) {
	src, err := LoadFileKeySource(path, allowGroupRead)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	w := &FileKeyWatcher{path: path, allowGroupRead: allowGroupRead, log: logger, cur: src}
	if info, err := os.Stat(path); err == nil {
		w.mtime, w.size = info.ModTime(), info.Size()
	}
	return w, nil
}

// Refresh re-stats the file and reloads it when it changed. Invoked by the
// Manager's key-reload loop (keyRefresher).
func (w *FileKeyWatcher) Refresh(time.Time) {
	info, err := os.Stat(w.path)
	if err != nil {
		w.log.Warn("keys file unreadable; keeping last good key state", "path", w.path, "error", err)
		return
	}
	w.mu.RLock()
	unchanged := info.ModTime().Equal(w.mtime) && info.Size() == w.size
	w.mu.RUnlock()
	if unchanged {
		return
	}
	src, err := LoadFileKeySource(w.path, w.allowGroupRead)
	if err != nil {
		w.log.Error("replaced keys file is invalid; keeping last good key state", "path", w.path, "error", err)
		return
	}
	w.mu.Lock()
	w.cur, w.mtime, w.size = src, info.ModTime(), info.Size()
	w.mu.Unlock()
	for _, warn := range src.Warnings() {
		w.log.Warn(warn)
	}
	w.log.Info("keys file reloaded", "path", w.path, "kids", src.Kids())
}

func (w *FileKeyWatcher) current() *FileKeySource {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cur
}

// LoadSigningKey returns the current file snapshot's active webhook key.
func (w *FileKeyWatcher) LoadSigningKey(now time.Time) (SigningKey, error) {
	return w.current().LoadSigningKey(now)
}

// SigningKeys returns the current file snapshot's webhook-envelope family.
func (w *FileKeyWatcher) SigningKeys() ([]SigningKey, error) { return w.current().SigningKeys() }

// LoadWakeKey returns the current file snapshot's active wake-token key.
func (w *FileKeyWatcher) LoadWakeKey(now time.Time) (SigningKey, error) {
	return w.current().LoadWakeKey(now)
}

// WakeSigningKeys returns the current file snapshot's wake-token family.
func (w *FileKeyWatcher) WakeSigningKeys() ([]SigningKey, error) {
	return w.current().WakeSigningKeys()
}

// LoadTokenKey returns the current file snapshot's HMAC token key.
func (w *FileKeyWatcher) LoadTokenKey() ([]byte, error) { return w.current().LoadTokenKey() }

// DenylistedKids returns the current file snapshot's denylist.
func (w *FileKeyWatcher) DenylistedKids() ([]string, error) { return w.current().DenylistedKids() }

// Warnings surfaces the current snapshot's custody findings.
func (w *FileKeyWatcher) Warnings() []string { return w.current().Warnings() }

// Kids surfaces the current snapshot's loggable key identities.
func (w *FileKeyWatcher) Kids() []string { return w.current().Kids() }
