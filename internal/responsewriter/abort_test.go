package responsewriter

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestIsAbortRecognisesOnlyTheAbortSentinel(t *testing.T) {
	for _, tc := range []struct {
		name      string
		recovered any
		want      bool
	}{
		{"the sentinel", http.ErrAbortHandler, true},
		{"the sentinel, wrapped", fmt.Errorf("flush: %w", http.ErrAbortHandler), true},
		{"another error", errors.New("boom"), false},
		{"a string panic", "boom", false},
		{"no panic", nil, false},
	} {
		if got := IsAbort(tc.recovered); got != tc.want {
			t.Errorf("%s: IsAbort = %v, want %v", tc.name, got, tc.want)
		}
	}
}
