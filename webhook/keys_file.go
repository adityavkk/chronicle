package webhook

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// This file is the custody seam of issues #123/#126: where the subscription
// layer's key material comes from. The default source is the Redis store
// (generate-and-persist, first writer wins) — right for development, but on a
// shared data-plane Redis it means Redis read access == authority to forge
// any webhook signature or token (#123 blocker 2). A mounted secrets file
// (the Akeyless /etc/secrets pattern) breaks that: key material never touches
// Redis, and custody follows the platform's secret mount instead of the data
// plane. The Wave-2 rotation state machine builds on this same seam.

// KeySource supplies the subscription layer's key material: the active
// Ed25519 webhook signing key, the full signing key set the JWKS endpoint
// serves (active first), and the HMAC token key. *RedisStore satisfies it
// (generate-and-persist); FileKeySource reads a mounted secrets file and
// never writes anywhere.
type KeySource interface {
	// LoadSigningKey returns the active webhook signing key.
	LoadSigningKey(now time.Time) (SigningKey, error)
	// SigningKeys returns every published signing key, active first.
	SigningKeys() ([]SigningKey, error)
	// LoadTokenKey returns the HMAC token key.
	LoadTokenKey() ([]byte, error)
}

var _ KeySource = (*RedisStore)(nil)

// minTokenKeyBytes is the floor for the HMAC-SHA256 token key (RFC 2104: a
// key shorter than the hash output weakens the MAC).
const minTokenKeyBytes = 32

// Signing-key statuses a keys file may carry. Only the active key signs;
// retiring keys stay in the JWKS so outstanding tokens verify through the
// rotation overlap window (Wave 2 flips them).
const (
	keyStatusActive   = "active"
	keyStatusRetiring = "retiring"
)

// keyPurposeWebhook is the only purpose this loader accepts today: the
// webhook-envelope signing key family. The rotation/custody work extends the
// enum (e.g. "wake" for the #123 wake_token key, which is deliberately a
// separate key from the webhook envelope's); an unknown purpose is rejected
// outright so a file written for a newer chronicle never half-loads here.
const keyPurposeWebhook = "webhook"

// keysFileDoc is the exact schema of a CHRONICLE_KEYS_FILE. kid is never
// stored — it is derived (RFC 7638) from the key itself, so the file cannot
// disagree with the JWKS about a key's identity.
type keysFileDoc struct {
	// TokenKey is the base64url (unpadded) HMAC token key, >= 32 bytes.
	TokenKey string `json:"token_key"`
	// SigningKeys lists the Ed25519 keys; exactly one must be
	// status=active with purpose=webhook.
	SigningKeys []keysFileEntry `json:"signing_keys"`
}

type keysFileEntry struct {
	// Seed is the base64url (unpadded) 32-byte Ed25519 seed (RFC 8032).
	Seed string `json:"seed"`
	// CreatedAt is RFC3339; it feeds the SigningKey metadata.
	CreatedAt string `json:"created_at"`
	// Status is "active" or "retiring".
	Status string `json:"status"`
	// Purpose is "webhook" (see keyPurposeWebhook).
	Purpose string `json:"purpose"`
}

// FileKeySource is the KeySource for a mounted secrets file. It is immutable
// after load: the file is read once at startup, and the process must restart
// (or Wave-2 rotation must land) to pick up new material. Values returned by
// its methods are copies, so a caller can never mutate the loaded material.
type FileKeySource struct {
	active   SigningKey
	all      []SigningKey // active first
	tokenKey []byte
	warnings []string
}

// LoadFileKeySource reads and strictly parses a keys file. Everything is
// fail-closed: a missing file, unknown field, short seed or token key, an
// unknown status or purpose, no (or more than one) active key — each refuses
// to load rather than guessing. Error messages name fields and reasons but
// never echo key material.
func LoadFileKeySource(path string) (*FileKeySource, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("keys file: %w", err)
	}
	var warnings []string
	if info.Mode().Perm()&0o077 != 0 {
		warnings = append(warnings,
			fmt.Sprintf("keys file %s is group- or world-accessible (mode %04o); restrict it to 0600", path, info.Mode().Perm()))
	}
	raw, err := os.ReadFile(path) // #nosec G304 -- the operator-configured secrets mount path
	if err != nil {
		return nil, fmt.Errorf("keys file: %w", err)
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var doc keysFileDoc
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("keys file %s: %w", path, err)
	}
	if dec.More() {
		return nil, fmt.Errorf("keys file %s: trailing data after the document", path)
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
	var active *SigningKey
	retiring := make([]SigningKey, 0, len(doc.SigningKeys))
	for i, e := range doc.SigningKeys {
		key, err := parseKeysFileEntry(e)
		if err != nil {
			return nil, fmt.Errorf("keys file %s: signing_keys[%d]: %w", path, i, err)
		}
		switch key.Status {
		case keyStatusActive:
			if active != nil {
				return nil, fmt.Errorf("keys file %s: more than one active signing key", path)
			}
			active = &key
		case keyStatusRetiring:
			retiring = append(retiring, key)
		default:
			// parseKeysFileEntry already rejected other statuses.
			return nil, fmt.Errorf("keys file %s: signing_keys[%d]: unreachable status", path, i)
		}
	}
	if active == nil {
		return nil, fmt.Errorf("keys file %s: no active signing key", path)
	}

	return &FileKeySource{
		active:   *active,
		all:      append([]SigningKey{*active}, retiring...),
		tokenKey: tokenKey,
		warnings: warnings,
	}, nil
}

func parseKeysFileEntry(e keysFileEntry) (SigningKey, error) {
	if e.Purpose != keyPurposeWebhook {
		return SigningKey{}, fmt.Errorf("unknown purpose %q (this build accepts %q)", e.Purpose, keyPurposeWebhook)
	}
	if e.Status != keyStatusActive && e.Status != keyStatusRetiring {
		return SigningKey{}, fmt.Errorf("unknown status %q (want %q or %q)", e.Status, keyStatusActive, keyStatusRetiring)
	}
	seed, err := base64.RawURLEncoding.DecodeString(e.Seed)
	if err != nil {
		return SigningKey{}, errors.New("seed: invalid base64url")
	}
	if len(seed) != ed25519.SeedSize {
		return SigningKey{}, fmt.Errorf("seed: %d bytes, want %d", len(seed), ed25519.SeedSize)
	}
	createdAt, err := time.Parse(time.RFC3339, e.CreatedAt)
	if err != nil {
		return SigningKey{}, errors.New("created_at: not RFC3339")
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return SigningKey{}, errors.New("seed: cannot derive public key")
	}
	return SigningKey{
		Kid:       deriveKid(pub),
		Public:    pub,
		Private:   priv,
		CreatedAt: createdAt,
		Status:    e.Status,
	}, nil
}

// LoadSigningKey returns the file's single active webhook signing key. now is
// unused: the file, not the clock, decides which key is active.
func (s *FileKeySource) LoadSigningKey(time.Time) (SigningKey, error) {
	return s.active, nil
}

// SigningKeys returns the file's keys, active first, as a fresh slice.
func (s *FileKeySource) SigningKeys() ([]SigningKey, error) {
	out := make([]SigningKey, len(s.all))
	copy(out, s.all)
	return out, nil
}

// LoadTokenKey returns a copy of the file's HMAC token key.
func (s *FileKeySource) LoadTokenKey() ([]byte, error) {
	out := make([]byte, len(s.tokenKey))
	copy(out, s.tokenKey)
	return out, nil
}

// Warnings are non-fatal custody findings from the load (today: overly broad
// file permissions), for the caller to log. Never key material.
func (s *FileKeySource) Warnings() []string {
	return append([]string(nil), s.warnings...)
}

// Kids returns the loaded key ids (active first) for startup logging — the
// one piece of key identity that is safe to log.
func (s *FileKeySource) Kids() []string {
	out := make([]string, len(s.all))
	for i, k := range s.all {
		out[i] = k.Kid + "(" + k.Status + ")"
	}
	return out
}
