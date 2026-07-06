package webhook

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// strictJSONUnmarshal decodes exactly one JSON value from data into v,
// rejecting both unknown fields AND any trailing data after the value.
//
// json.Decoder.More() is NOT sufficient for the trailing-data half: it
// returns false whenever the next byte is '}' or ']', so a document like
// `{...}}` or `{...}]` slips through (verified empirically). A second Decode
// that must return io.EOF is airtight — the next value, a stray brace, or any
// non-whitespace byte all make it fail. Every chronicle token and key-file
// parser routes through here so the advertised strict grammar (RFC 8725
// §3.11/§3.12 for tokens; fail-closed custody for the key file) actually
// holds, not merely for a trailing object but for every trailing byte.
func strictJSONUnmarshal(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("unexpected trailing data after JSON value")
		}
		return fmt.Errorf("unexpected trailing data after JSON value: %w", err)
	}
	return nil
}
