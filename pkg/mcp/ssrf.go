package mcp

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
)

const maxHTTPErrorBytes = 512

type sanitizedHTTPError struct {
	err     error
	message string
}

func (e sanitizedHTTPError) Error() string { return e.message }
func (e sanitizedHTTPError) Unwrap() error { return e.err }

// validateURL parses raw and rejects targets unsafe to fetch (SSRF guard):
// non-HTTP(S) schemes, embedded credentials, and IP literals in blocked ranges.
// Domain hosts are additionally checked against known metadata hostnames here.
// Outbound clients must use resolvePinnedIPs/doPinnedRequest so DNS validation
// and the actual connection share the same resolved address.
//
// Loopback is allowed for services on the same machine.
func validateURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		// url.Parse returns a *url.Error whose message embeds the raw URL
		// including its query string. Sanitize it so ?api-key=... secrets do not
		// leak into tool responses.
		return nil, fmt.Errorf("invalid URL: %w", sanitizeHTTPError(err))
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return nil, fmt.Errorf("disallowed URL scheme %q: only http and https are permitted", u.Scheme)
	}
	if u.User != nil {
		return nil, errors.New("URLs with embedded credentials are not permitted")
	}
	// Hostname removes IPv6 brackets before literal-IP classification.
	host := u.Hostname()
	if host == "" {
		return nil, errors.New("URL has no host")
	}
	if ip := net.ParseIP(host); ip != nil {
		if isBlockedIP(ip) {
			return nil, fmt.Errorf("URL resolves to blocked address: %s", ip)
		}
		return u, nil
	}
	// Fully qualified and bare hostnames resolve identically, so normalize all
	// trailing dots before checking metadata-service names.
	switch strings.TrimRight(strings.ToLower(host), ".") {
	case "metadata.google.internal", "metadata.google.com", "instance-data":
		return nil, fmt.Errorf("URL resolves to blocked metadata service: %s", host)
	}
	return u, nil
}

// isBlockedIPv4 reports whether an IPv4 address is in a blocked range:
// private (RFC-1918), link-local (169.254/16, incl. the 169.254.169.254
// metadata IP), unspecified, broadcast, and CGNAT/shared address space
// (RFC-6598 100.64.0.0/10). Loopback (127/8) is allowed.
func isBlockedIPv4(ip4 net.IP) bool {
	if ip4.IsLoopback() {
		return false
	}
	if ip4.IsPrivate() || ip4.IsLinkLocalUnicast() || ip4.IsUnspecified() {
		return true
	}
	if ip4.IsMulticast() || ip4.Equal(net.IPv4bcast) {
		return true
	}
	// RFC-6598 carrier-grade NAT / shared address space (100.64.0.0/10). Cloud
	// and bare-metal providers bind internal/management services here.
	if ip4[0] == 100 && (ip4[1]&0xc0) == 0x40 {
		return true
	}
	// Non-public special-purpose ranges should never be MCP egress targets.
	// This includes "this network", protocol assignments, benchmarking,
	// documentation examples, deprecated relays, multicast, and reserved space.
	return ip4[0] == 0 ||
		(ip4[0] == 192 && ip4[1] == 0 && ip4[2] == 0) ||
		(ip4[0] == 192 && ip4[1] == 0 && ip4[2] == 2) ||
		(ip4[0] == 192 && ip4[1] == 88 && ip4[2] == 99) ||
		(ip4[0] == 198 && (ip4[1] == 18 || ip4[1] == 19)) ||
		(ip4[0] == 198 && ip4[1] == 51 && ip4[2] == 100) ||
		(ip4[0] == 203 && ip4[1] == 0 && ip4[2] == 113) ||
		ip4[0] >= 224
}

var blockedIPv6Prefixes = []netip.Prefix{
	netip.MustParsePrefix("::/96"),          // IPv4-compatible and other reserved low addresses
	netip.MustParsePrefix("64:ff9b:1::/48"), // local-use translation prefix (RFC 8215)
	netip.MustParsePrefix("100::/64"),       // discard-only
	netip.MustParsePrefix("2001:2::/48"),    // benchmarking
	netip.MustParsePrefix("2001:db8::/32"),  // documentation
	netip.MustParsePrefix("fec0::/10"),      // deprecated site-local addresses
}

// isBlockedIP reports whether an IP is in a blocked range. IPv4-mapped IPv6
// addresses (e.g. ::ffff:10.0.0.1) collapse to IPv4 via To4() and are checked
// as IPv4, closing that bypass. Loopback (127/8, ::1) is allowed.
func isBlockedIP(ip net.IP) bool {
	if ip4 := ip.To4(); ip4 != nil {
		return isBlockedIPv4(ip4)
	}
	// IPv6-to-IPv4 transition encodings can bypass the IPv4 blocklist, such as a
	// malicious AAAA record returning NAT64 for 169.254.169.254.
	if v4 := embeddedIPv4(ip); v4 != nil {
		// A transition address is not local loopback even when its embedded value
		// is 127/8; allowing it would delegate that trust decision to a NAT64/6to4
		// gateway. Only a literal 127/8 or ::1 receives the local exception.
		if v4.IsLoopback() || isBlockedIPv4(v4) {
			return true
		}
	}
	if ip.IsLoopback() { // ::1
		return false
	}
	// IsPrivate covers IPv6 unique-local (fc00::/7); IsLinkLocalUnicast covers
	// fe80::/10; plus the unspecified address ::.
	if ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsPrivate() || ip.IsMulticast() {
		return true
	}
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return true
	}
	for _, prefix := range blockedIPv6Prefixes {
		if prefix.Contains(addr.Unmap()) {
			return true
		}
	}
	return false
}

// embeddedIPv4 returns the IPv4 address embedded in an IPv6 transition address
// (NAT64 64:ff9b::/96 per RFC 6052, or 6to4 2002::/16 per RFC 3056) as a 4-byte
// net.IP, or nil if none is embedded.
func embeddedIPv4(ip net.IP) net.IP {
	ip16 := ip.To16()
	if ip16 == nil {
		return nil
	}
	// NAT64 prefix 64:ff9b::/96; the last 32 bits are the IPv4 address.
	if ip16[0] == 0x00 && ip16[1] == 0x64 && ip16[2] == 0xff && ip16[3] == 0x9b &&
		ip16[4] == 0 && ip16[5] == 0 && ip16[6] == 0 && ip16[7] == 0 &&
		ip16[8] == 0 && ip16[9] == 0 && ip16[10] == 0 && ip16[11] == 0 {
		return net.IP{ip16[12], ip16[13], ip16[14], ip16[15]}
	}
	// 6to4 prefix 2002::/16; bytes 2..6 are the IPv4 address.
	if ip16[0] == 0x20 && ip16[1] == 0x02 {
		return net.IP{ip16[2], ip16[3], ip16[4], ip16[5]}
	}
	return nil
}

// canonicalOrigin returns a normalized HTTP(S) origin suitable for exact
// credential binding. It ignores path/query/fragment, normalizes
// default ports and IP spellings, and does not treat DNS aliases as equivalent.
func canonicalOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", errors.New("invalid endpoint URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("disallowed URL scheme %q", u.Scheme)
	}
	if u.User != nil {
		return "", errors.New("endpoint URLs with embedded credentials are not permitted")
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "" {
		return "", errors.New("endpoint URL has no host")
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		host = addr.Unmap().String()
	}

	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	} else {
		portNum, err := strconv.Atoi(port)
		if err != nil || portNum < 1 || portNum > 65535 {
			return "", errors.New("endpoint URL has an invalid port")
		}
		port = strconv.Itoa(portNum)
	}
	return u.Scheme + "://" + net.JoinHostPort(host, port), nil
}

// sanitizeEndpointForDisplay returns a canonical origin with an explicit port
// and no trailing slash. It is useful in structured endpoint fields and for
// origin-bound credential diagnostics.
func sanitizeEndpointForDisplay(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid-url>"
	}
	u.User = nil
	u.RawQuery = ""
	u.Fragment = ""
	u.Path = ""
	u.RawPath = ""
	origin, err := canonicalOrigin(u.String())
	if err != nil {
		return "<invalid-url>"
	}
	return origin
}

// sanitizeURLForDisplay renders an origin-only URL (with a conventional trailing
// slash), stripping credentials, path, query, and fragment. Hosted RPC services
// may carry API keys in either query strings or path segments.
func sanitizeURLForDisplay(raw string) string {
	origin := sanitizeEndpointForDisplay(raw)
	if origin == "<invalid-url>" {
		return "<invalid-url>"
	}
	return origin + "/"
}

// sanitizeHTTPError preserves the underlying error while bounding and
// redacting endpoint-controlled display text.
func sanitizeHTTPError(err error) error {
	if err == nil {
		return nil
	}
	message := ""
	var uerr *url.Error
	if errors.As(err, &uerr) {
		cause := errors.New("request failed")
		if uerr.Err != nil {
			cause = uerr.Err
		}
		message = fmt.Sprintf(
			"%s %s: %s",
			redactUntrustedText(uerr.Op),
			sanitizeEndpointForDisplay(uerr.URL),
			redactUntrustedText(cause.Error()),
		)
	} else {
		message = redactUntrustedText(err.Error())
	}
	message, _ = truncateUTF8Bytes(message, maxHTTPErrorBytes)
	return sanitizedHTTPError{err: err, message: message}
}
