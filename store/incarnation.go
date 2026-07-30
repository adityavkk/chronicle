package store

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

const incarnationBytes = 16

// NewIncarnationID returns an opaque identifier for one created stream
// incarnation. It is independent of wall-clock time so delete and recreate are
// distinguishable even when the store clock is frozen or moves backward.
func NewIncarnationID() (string, error) {
	var raw [incarnationBytes]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate stream incarnation: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
