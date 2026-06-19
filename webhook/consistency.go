package webhook

import (
	"fmt"
	"strings"
	"time"
)

// ConsistencyTier is the explicit durability/freshness knob from the
// horizontal-scale DR design. The generation fence remains the safety boundary;
// the tier only controls durability work after a fence-minting write.
type ConsistencyTier uint8

const (
	// ConsistencyTierUnspecified means the request omitted the tier and the
	// deployment default should be applied before validation or storage.
	ConsistencyTierUnspecified ConsistencyTier = iota
	// ConsistencyTierA is the default eventual tier; it does not issue
	// Redis WAIT/WAITAOF after fence-minting writes.
	ConsistencyTierA
	// ConsistencyTierB adds active-passive durability checks with WAIT 1 and
	// WAITAOF 1 1 after writes that mint a new generation fence.
	ConsistencyTierB
	// ConsistencyTierC is reserved for stronger semantics and is explicitly
	// unsupported on the Redis substrate.
	ConsistencyTierC
)

const defaultConsistencyTier = ConsistencyTierA

// fenceDurabilityTimeout bounds WAIT/WAITAOF. Redis uses milliseconds, and the
// DR design targets the AOF fsync interval rather than an unbounded wait.
var fenceDurabilityTimeout = time.Second

// ParseConsistencyTier parses a user supplied tier name. Empty is left
// unspecified so callers can apply a deployment default exactly once.
func ParseConsistencyTier(raw string) (ConsistencyTier, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "":
		return ConsistencyTierUnspecified, nil
	case "A":
		return ConsistencyTierA, nil
	case "B":
		return ConsistencyTierB, nil
	case "C":
		return ConsistencyTierC, nil
	default:
		return ConsistencyTierUnspecified, fmt.Errorf("unknown consistency tier %q", raw)
	}
}

func (t ConsistencyTier) String() string {
	switch t {
	case ConsistencyTierA:
		return "A"
	case ConsistencyTierB:
		return "B"
	case ConsistencyTierC:
		return "C"
	case ConsistencyTierUnspecified:
		return ""
	default:
		return fmt.Sprintf("ConsistencyTier(%d)", t)
	}
}

func (t ConsistencyTier) storageValue() string {
	if t == ConsistencyTierUnspecified {
		return defaultConsistencyTier.String()
	}
	return t.String()
}

// NormalizeConsistencyTier applies the deployment default. Tier C is still a
// first-class value here; validation rejects it on the Redis substrate.
func NormalizeConsistencyTier(t, deploymentDefault ConsistencyTier) ConsistencyTier {
	if deploymentDefault == ConsistencyTierUnspecified {
		deploymentDefault = defaultConsistencyTier
	}
	if t == ConsistencyTierUnspecified {
		return deploymentDefault
	}
	return t
}

// ValidateConsistencyTier returns an empty string when the tier is supported.
func ValidateConsistencyTier(t ConsistencyTier) string {
	switch t {
	case ConsistencyTierA, ConsistencyTierB:
		return ""
	case ConsistencyTierC:
		return "consistency_tier C is not supported by Redis; Redis WAIT/WAITAOF are durability only"
	case ConsistencyTierUnspecified:
		return "consistency_tier must be defaulted before validation"
	default:
		return fmt.Sprintf("unknown consistency_tier %d", t)
	}
}

type fenceDurabilityPolicy struct {
	waitReplicas int
	aofLocal     int
	aofReplicas  int
	timeout      time.Duration
}

func (p fenceDurabilityPolicy) enabled() bool {
	return p.waitReplicas > 0 || p.aofLocal > 0 || p.aofReplicas > 0
}

func fenceDurabilityForTier(t ConsistencyTier) (fenceDurabilityPolicy, error) {
	switch t {
	case ConsistencyTierA, ConsistencyTierUnspecified:
		return fenceDurabilityPolicy{}, nil
	case ConsistencyTierB:
		return fenceDurabilityPolicy{
			waitReplicas: 1,
			aofLocal:     1,
			aofReplicas:  1,
			timeout:      fenceDurabilityTimeout,
		}, nil
	case ConsistencyTierC:
		return fenceDurabilityPolicy{}, fmt.Errorf("consistency tier C is unsupported on Redis")
	default:
		return fenceDurabilityPolicy{}, fmt.Errorf("unknown consistency tier %d", t)
	}
}
