package webhook

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
)

// ClampLeaseTTL applies the PROTOCOL §6.2 bounds: a zero/absent value becomes
// the 30 s default; anything outside 1 s–10 min is clamped into range.
func ClampLeaseTTL(ms int64) int64 {
	if ms == 0 {
		return DefaultLeaseTTLMs
	}
	if ms < MinLeaseTTLMs {
		return MinLeaseTTLMs
	}
	if ms > MaxLeaseTTLMs {
		return MaxLeaseTTLMs
	}
	return ms
}

// NormalizeConfig produces the canonical form used for hashing and storage:
// the lease TTL is clamped, and streams[] is trimmed, de-duplicated, and sorted
// so the hash is independent of input order or repetition (PROTOCOL §6.2).
func NormalizeConfig(c Config) Config {
	return NormalizeConfigWithDefault(c, defaultConsistencyTier)
}

// NormalizeConfigWithDefault produces the canonical form used for hashing and
// storage, applying the deployment default for an omitted consistency tier.
func NormalizeConfigWithDefault(c Config, deploymentTier ConsistencyTier) Config {
	c.LeaseTTLMs = ClampLeaseTTL(c.LeaseTTLMs)
	c.Streams = normalizeStreams(c.Streams)
	c.ConsistencyTier = NormalizeConsistencyTier(c.ConsistencyTier, deploymentTier)
	return c
}

func normalizeStreams(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.Trim(s, "/")
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil
	}
	return out
}

// ConfigHash hashes the normalized configuration for idempotent re-confirmation
// (PROTOCOL §6.2). The hash includes type, pattern, normalized streams[],
// delivery configuration, lease_ttl_ms, and description. The field-tagged,
// length-prefixed encoding makes the hash unambiguous across field boundaries.
func ConfigHash(c Config) string {
	return configHash(c, true)
}

func legacyConfigHash(c Config) string {
	return configHash(c, false)
}

func configHash(c Config, includeConsistency bool) string {
	n := NormalizeConfig(c)
	var b strings.Builder
	writeField(&b, "type", string(n.Type))
	writeField(&b, "pattern", n.Pattern)
	writeField(&b, "streams", strings.Join(n.Streams, "\x1f"))
	writeField(&b, "webhook_url", n.WebhookURL)
	writeField(&b, "wake_stream", n.WakeStream)
	writeField(&b, "lease_ttl_ms", strconv.FormatInt(n.LeaseTTLMs, 10))
	if includeConsistency {
		writeField(&b, "consistency_tier", n.ConsistencyTier.storageValue())
	}
	writeField(&b, "description", n.Description)
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// writeField appends a length-prefixed "key=<len>:value;" token so no value can
// be confused with a field boundary or with an adjacent field.
func writeField(b *strings.Builder, key, val string) {
	b.WriteString(key)
	b.WriteByte('=')
	b.WriteString(strconv.Itoa(len(val)))
	b.WriteByte(':')
	b.WriteString(val)
	b.WriteByte(';')
}

// ValidateConfig checks the structural requirements of PROTOCOL §6.2 that do not
// depend on I/O. It returns an empty string when valid, or a human-readable
// reason for a 400. SSRF validation of the webhook URL is separate (ssrf.go)
// because it needs DNS resolution.
func ValidateConfig(c Config) string {
	switch c.Type {
	case DispatchWebhook, DispatchPullWake:
	default:
		return "type must be \"webhook\" or \"pull-wake\""
	}
	if c.Pattern == "" && len(c.Streams) == 0 {
		return "at least one of pattern or streams is required"
	}
	if c.Type == DispatchWebhook && c.WebhookURL == "" {
		return "webhook.url is required for type \"webhook\""
	}
	if c.Type == DispatchPullWake && c.WakeStream == "" {
		return "wake_stream is required for type \"pull-wake\""
	}
	if reason := ValidateConsistencyTier(c.ConsistencyTier); reason != "" {
		return reason
	}
	return ""
}
