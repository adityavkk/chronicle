package responsewriter

import (
	"errors"
	"net/http"
)

// IsAbort reports whether a recovered panic value is http.ErrAbortHandler,
// the sentinel the SSE paths raise to end a committed stream they cannot
// finish. Middleware that recovers to record the outcome treats it as an
// abort, not a failure, and re-panics either way.
func IsAbort(recovered any) bool {
	err, ok := recovered.(error)
	return ok && errors.Is(err, http.ErrAbortHandler)
}
