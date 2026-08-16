package auth

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// ServicePolicyConfig is one service identity's explicit authority. A trusted
// gateway delegates all actions and namespaces to the exact named identity.
type ServicePolicyConfig struct {
	Identity       string
	Actions        []Action
	Namespaces     []string
	TrustedGateway bool
}

// ServicePolicies is the immutable service authorization policy set. Its zero
// value grants nothing.
type ServicePolicies struct {
	byIdentity map[string]servicePolicy
}

type servicePolicy struct {
	actions        map[Action]struct{}
	namespaces     []StreamPath
	trustedGateway bool
}

// NewServicePolicies validates and normalizes a complete policy set.
func NewServicePolicies(configs []ServicePolicyConfig) (ServicePolicies, error) {
	if len(configs) == 0 {
		return ServicePolicies{}, errors.New("service policy is empty")
	}
	policies := ServicePolicies{byIdentity: make(map[string]servicePolicy, len(configs))}
	for i, cfg := range configs {
		identity := strings.TrimSpace(cfg.Identity)
		if identity == "" {
			return ServicePolicies{}, fmt.Errorf("service policy entry %d: identity is required", i+1)
		}
		if identity != cfg.Identity {
			return ServicePolicies{}, fmt.Errorf("service policy entry %d: identity must not have surrounding whitespace", i+1)
		}
		if _, exists := policies.byIdentity[identity]; exists {
			return ServicePolicies{}, fmt.Errorf("service policy entry %d: duplicate identity %q", i+1, identity)
		}

		policy := servicePolicy{
			actions:        make(map[Action]struct{}, len(cfg.Actions)),
			namespaces:     make([]StreamPath, 0, len(cfg.Namespaces)),
			trustedGateway: cfg.TrustedGateway,
		}
		for _, action := range cfg.Actions {
			if !validServiceAction(action) {
				return ServicePolicies{}, fmt.Errorf("service policy entry %d: unknown action %q", i+1, action.String())
			}
			if _, duplicate := policy.actions[action]; duplicate {
				return ServicePolicies{}, fmt.Errorf("service policy entry %d: duplicate action %q", i+1, action.String())
			}
			policy.actions[action] = struct{}{}
		}
		seenNamespaces := make(map[string]struct{}, len(cfg.Namespaces))
		for _, raw := range cfg.Namespaces {
			path, err := NormalizeStreamPath(raw)
			if err != nil {
				return ServicePolicies{}, fmt.Errorf("service policy entry %d: invalid namespace %q", i+1, raw)
			}
			if _, duplicate := seenNamespaces[path.String()]; duplicate {
				return ServicePolicies{}, fmt.Errorf("service policy entry %d: duplicate namespace %q", i+1, path.String())
			}
			seenNamespaces[path.String()] = struct{}{}
			policy.namespaces = append(policy.namespaces, path)
		}
		if cfg.TrustedGateway && (len(policy.actions) > 0 || len(policy.namespaces) > 0) {
			return ServicePolicies{}, fmt.Errorf("service policy entry %d: trusted_gateway must not set actions or namespaces", i+1)
		}
		if !cfg.TrustedGateway && (len(policy.actions) == 0 || len(policy.namespaces) == 0) {
			return ServicePolicies{}, fmt.Errorf("service policy entry %d: non-gateway policy requires actions and namespaces", i+1)
		}
		policies.byIdentity[identity] = policy
	}
	return policies, nil
}

// ParseServicePolicies parses the strict mounted JSON policy document. The
// accepted shape is {"services":[{"identity":"...","actions":["read"],
// "namespaces":["tenant-a"],"trusted_gateway":false}]}.
func ParseServicePolicies(data []byte) (ServicePolicies, error) {
	type policyJSON struct {
		Identity       string   `json:"identity"`
		Actions        []string `json:"actions"`
		Namespaces     []string `json:"namespaces"`
		TrustedGateway bool     `json:"trusted_gateway"`
	}
	var document struct {
		Services []policyJSON `json:"services"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&document); err != nil {
		return ServicePolicies{}, fmt.Errorf("service policy JSON: %w", err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return ServicePolicies{}, err
	}
	configs := make([]ServicePolicyConfig, len(document.Services))
	for i, entry := range document.Services {
		actions := make([]Action, len(entry.Actions))
		for j, raw := range entry.Actions {
			action, ok := parseServiceAction(raw)
			if !ok {
				return ServicePolicies{}, fmt.Errorf("service policy entry %d: unknown action %q", i+1, raw)
			}
			actions[j] = action
		}
		configs[i] = ServicePolicyConfig{
			Identity:       entry.Identity,
			Actions:        actions,
			Namespaces:     entry.Namespaces,
			TrustedGateway: entry.TrustedGateway,
		}
	}
	return NewServicePolicies(configs)
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return errors.New("service policy JSON: multiple values")
	} else if !errors.Is(err, io.EOF) {
		return fmt.Errorf("service policy JSON: %w", err)
	}
	return nil
}

func parseServiceAction(raw string) (Action, bool) {
	switch raw {
	case "read":
		return ActionRead, true
	case "append":
		return ActionAppend, true
	case "create":
		return ActionCreate, true
	case "delete":
		return ActionDelete, true
	case "subscribe":
		return ActionSubscribe, true
	case "link":
		return ActionLink, true
	case "claim":
		return ActionClaim, true
	default:
		return 0, false
	}
}

func validServiceAction(action Action) bool {
	return action >= ActionRead && action <= ActionClaim
}

// HasIdentity reports whether identity has an explicit policy.
func (p ServicePolicies) HasIdentity(identity string) bool {
	_, ok := p.byIdentity[identity]
	return ok
}

// Len returns the number of explicitly configured service identities.
func (p ServicePolicies) Len() int { return len(p.byIdentity) }

// TrustedGatewayIdentities returns the exact subjects granted delegated trust.
func (p ServicePolicies) TrustedGatewayIdentities() []string {
	out := make([]string, 0)
	for identity, policy := range p.byIdentity {
		if policy.trustedGateway {
			out = append(out, identity)
		}
	}
	sort.Strings(out)
	return out
}

// SPIFFEIdentities returns policy identities that are SPIFFE URIs.
func (p ServicePolicies) SPIFFEIdentities() []string {
	out := make([]string, 0, len(p.byIdentity))
	for identity := range p.byIdentity {
		if strings.HasPrefix(identity, "spiffe://") {
			out = append(out, identity)
		}
	}
	sort.Strings(out)
	return out
}

// AuthorizeAction evaluates only the identity and action grant. It is used for
// idempotent operations whose target does not exist, so there is no stream path
// whose existence can safely be exposed.
func (p ServicePolicies) AuthorizeAction(principal Principal, action Action) (Decision, bool) {
	if principal.Kind() != KindService || principal.Subject() == "" {
		return Deny(ReasonUnauthenticated, "missing verified service identity"), false
	}
	policy, ok := p.byIdentity[principal.Subject()]
	if !ok {
		return Deny(ReasonForbidden, "service identity has no policy"), false
	}
	if policy.trustedGateway {
		return Allow(), true
	}
	if _, ok := policy.actions[action]; !ok {
		return Deny(ReasonForbidden, "service policy does not allow "+action.String()), false
	}
	return Allow(), false
}

// Authorize evaluates one verified service principal against its exact policy.
// Every protected path must fall under a configured namespace. delegated is
// true only when the exact subject has trusted_gateway enabled.
func (p ServicePolicies) Authorize(principal Principal, action Action, paths ...StreamPath) (decision Decision, delegated bool) {
	decision, delegated = p.AuthorizeAction(principal, action)
	if !decision.Allowed() || delegated {
		return decision, delegated
	}
	if len(paths) == 0 {
		return Deny(ReasonForbidden, "service authorization has no protected stream path"), false
	}
	policy := p.byIdentity[principal.Subject()]
	for _, path := range paths {
		if path.IsZero() || !PathWithinPrefixes(path, policy.namespaces) {
			return Deny(ReasonForbidden, "service namespaces do not cover this stream"), false
		}
	}
	return Allow(), false
}

// TrustedGateway reports whether the exact verified service subject has the
// explicit delegated-gateway designation.
func (p ServicePolicies) TrustedGateway(principal Principal) bool {
	policy, ok := p.byIdentity[principal.Subject()]
	return principal.Kind() == KindService && ok && policy.trustedGateway
}
