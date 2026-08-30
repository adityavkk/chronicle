package main

import (
	"fmt"
	"net/http"

	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// webhookEgress is the webhook egress adapter active in this process: the
// webhook.TargetPolicy that admits its one narrowly allowed private route and
// the HTTP client that reaches it (nil keeps the Manager's default client).
// The zero value means no adapter is configured. name is stamped by
// foldWebhookEgress from the loader's registration; a loader leaves it zero.
type webhookEgress struct {
	name   string
	policy webhook.TargetPolicy
	client *http.Client
}

// webhookEgressLoader is one deployment adapter's registration: load reads the
// adapter's configuration through lookup (os.LookupEnv in main) and returns
// its egress, or a zero webhookEgress when the adapter is not configured. A
// partial or invalid configuration is an error — fail closed rather than widen
// the SSRF exception. An adapter is active only when it returns a policy.
type webhookEgressLoader struct {
	name string
	load func(lookup func(string) (string, bool)) (webhookEgress, error)
}

// webhookEgressLoaders is the registration hook for deployment adapters, which
// live in their own files and append their loader from init(); main folds the
// slice with loadWebhookEgress. Loaders run in registration order and at most
// one may be active in a process.
var webhookEgressLoaders []webhookEgressLoader

// loadWebhookEgress folds the registered adapters into the single active
// egress for this process (see foldWebhookEgress).
func loadWebhookEgress(allowPrivate bool, lookup func(string) (string, bool)) (webhookEgress, error) {
	return foldWebhookEgress(webhookEgressLoaders, allowPrivate, lookup)
}

// foldWebhookEgress runs every loader and returns the one active egress, or
// the zero value when none is configured. Two active adapters are refused, and
// so is an active adapter combined with broad private webhook access
// (-webhook-allow-private): the adapter exists to admit exactly one private
// route, and the broad mode would make that exception meaningless.
func foldWebhookEgress(loaders []webhookEgressLoader, allowPrivate bool, lookup func(string) (string, bool)) (webhookEgress, error) {
	var active webhookEgress
	for _, loader := range loaders {
		egress, err := loader.load(lookup)
		if err != nil {
			return webhookEgress{}, err
		}
		if egress.policy == nil {
			continue
		}
		if active.policy != nil {
			return webhookEgress{}, fmt.Errorf("%s and %s webhook egress adapters are mutually exclusive", active.name, loader.name)
		}
		egress.name = loader.name
		active = egress
	}
	if active.policy != nil && allowPrivate {
		return webhookEgress{}, fmt.Errorf("%s webhook egress adapter cannot be combined with -webhook-allow-private", active.name)
	}
	return active, nil
}
