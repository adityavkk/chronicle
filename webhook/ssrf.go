package webhook

import (
	"net"
	"net/url"
	"strings"
)

// IPResolver resolves a host name to IP addresses. It is injected so the URL
// classifier stays pure and testable; the Manager supplies net.DefaultResolver.
type IPResolver func(host string) ([]net.IP, error)

// ClassifyWebhookURL enforces the SSRF rules of PROTOCOL §6.2 and §12.8. It
// returns ok=true when the URL may receive deliveries, or ok=false with the
// WEBHOOK_URL_REJECTED-worthy reason otherwise. Rules:
//
//   - scheme must be http or https;
//   - loopback / "localhost" is the development exception and is always allowed
//     (this is the conformance receiver at http://127.0.0.1:<port>);
//   - any private (RFC1918), link-local (including the 169.254.169.254 metadata
//     endpoint), unique-local, or unspecified target is rejected;
//   - a non-loopback http target is rejected — production webhooks require https.
//
// resolve is only called for non-IP-literal hosts; IP-literal hosts (the
// conformance cases) need no DNS.
func ClassifyWebhookURL(rawURL string, resolve IPResolver) (ok bool, reason string) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false, "webhook url is not a valid URL"
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return false, "webhook url scheme must be http or https"
	}
	host := u.Hostname()
	if host == "" {
		return false, "webhook url has no host"
	}

	// The development exception: a literal-loopback or "localhost" host is
	// allowed over either scheme. Checked by name first so DNS rebinding of
	// "localhost" to a public address cannot smuggle past the exception.
	if isLoopbackHost(host) {
		return true, ""
	}

	ips, err := hostIPs(host, resolve)
	if err != nil || len(ips) == 0 {
		return false, "webhook url host does not resolve"
	}
	for _, ip := range ips {
		if isBlockedIP(ip) {
			return false, "webhook url resolves to a private, loopback, or link-local address"
		}
	}

	// A public target reached over plain http is rejected; production requires
	// https. (Loopback http was already accepted above.)
	if scheme != "https" {
		return false, "production webhook url must use https"
	}
	return true, ""
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

func hostIPs(host string, resolve IPResolver) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	if resolve == nil {
		// No resolver and a non-literal host: cannot prove safety, so reject.
		return nil, nil
	}
	return resolve(host)
}

// isBlockedIP reports whether an address is in a range that MUST be rejected
// for SSRF safety. Loopback is intentionally NOT blocked here — it is the
// development exception handled by the caller; an IP-literal loopback host has
// already returned allowed before reaching this function.
func isBlockedIP(ip net.IP) bool {
	return ip.IsPrivate() || // RFC1918 + IPv6 unique-local (fc00::/7)
		ip.IsLinkLocalUnicast() || // 169.254.0.0/16 (incl. metadata) + fe80::/10
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() // 0.0.0.0 / ::
}
