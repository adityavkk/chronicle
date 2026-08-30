package webhook

import (
	"net/http"
	"net/url"
)

// TargetPolicy is an optional deployment adapter for a narrowly allowed
// private webhook route. The Manager consults it at exactly two points:
//
//   - AllowTarget, when a webhook URL is admitted at subscription create,
//     ahead of the SSRF rules: an admitted target skips them and every other
//     URL is classified as usual, so a private target the policy does not
//     admit is rejected (400 WEBHOOK_URL_REJECTED) and is never dialed;
//   - PrepareRequest, on every delivery attempt — retries included — after
//     the envelope is signed and immediately before the request enters the
//     HTTP client; an error fails that attempt before any transport activity
//     and schedules the normal retry.
//
// The protocol core still owns the body and the envelope signature; a policy
// may add platform routing identity on top of them.
type TargetPolicy interface {
	// AllowTarget returns true only for a private target that the deployment
	// explicitly admits in addition to Chronicle's normal SSRF rules.
	AllowTarget(target *url.URL) bool
	// PrepareRequest validates the final method and destination and may add
	// platform-owned routing headers. It must not consume or replace the body.
	PrepareRequest(req *http.Request) error
}
