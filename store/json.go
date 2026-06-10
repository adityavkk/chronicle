// Ported from the Durable Streams reference Caddy plugin
// (packages/caddy-plugin/store/memory_store.go processJSONAppend @ 82f9963),
// exported so every store backend shares one JSON-mode implementation.
package store

import (
	"bytes"
	"encoding/json"
)

// ProcessJSONAppend validates a JSON-mode request body and splits it into
// per-message frames per PROTOCOL.md §9.1: a top-level array is flattened
// exactly one level (each element one message); any other JSON value is a
// single message. Empty arrays are allowed only when allowEmpty is true
// (PUT create); on append they are ErrEmptyJSONArray.
func ProcessJSONAppend(data []byte, allowEmpty bool) ([][]byte, error) {
	if !json.Valid(data) {
		return nil, ErrInvalidJSON
	}

	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var arr []json.RawMessage
		if err := json.Unmarshal(trimmed, &arr); err != nil {
			return nil, ErrInvalidJSON
		}
		if len(arr) == 0 {
			if !allowEmpty {
				return nil, ErrEmptyJSONArray
			}
			return [][]byte{}, nil
		}
		// Flatten one level
		result := make([][]byte, len(arr))
		for i, elem := range arr {
			result[i] = []byte(elem)
		}
		return result, nil
	}

	// Single value
	return [][]byte{trimmed}, nil
}
