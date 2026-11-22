package dashboard

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"
)

const readerOriginChanged = "reader_origin_changed"

var errManagedFeedNotReady = errors.New("dashboard feed is not ready")

// ValidateManagedOrigin rejects common spellings that browsers rewrite before
// sending Host and Origin, since the managed reader compares those exactly.
func ValidateManagedOrigin(origin string) error {
	if err := ValidatePublicOrigin(origin); err != nil {
		return err
	}
	if !strings.HasPrefix(origin, "https://") {
		return errors.New("managed dashboard origin must start with lowercase https://")
	}
	u, err := url.Parse(origin)
	if err != nil {
		return err
	}
	host, port := u.Hostname(), u.Port()
	bracketed := strings.HasPrefix(u.Host, "[")
	if u.Host != strings.ToLower(u.Host) || bracketed != strings.Contains(host, ":") ||
		!canonicalManagedHostname(host) || port == "443" || strings.HasPrefix(port, "0") {
		return errors.New("dashboard origin must use a lowercase ASCII hostname or canonical IP address, omit the default HTTPS port and use a canonical port number")
	}
	return nil
}

func canonicalManagedHostname(host string) bool {
	if addr, err := netip.ParseAddr(host); err == nil {
		if addr.Is4In6() {
			bytes := addr.As16()
			return host == fmt.Sprintf("::ffff:%x:%x", binary.BigEndian.Uint16(bytes[12:14]), binary.BigEndian.Uint16(bytes[14:16]))
		}
		return addr.Zone() == "" && host == addr.String()
	}
	if strings.ContainsAny(host, ":[]%") || strings.IndexFunc(host, func(r rune) bool { return r > 127 }) >= 0 {
		return false
	}
	// Browsers parse hosts ending in a number as IPv4 addresses.
	domain := strings.TrimSuffix(host, ".")
	last := domain[strings.LastIndexByte(domain, '.')+1:]
	return last != "" && strings.Trim(last, "0123456789") != "" &&
		!(strings.HasPrefix(last, "0x") && strings.Trim(last[2:], "0123456789abcdef") == "")
}

func nodeReaderOrigin(ctx context.Context, socket string) (string, error) {
	info, err := os.Lstat(socket)
	if err != nil {
		return "", fmt.Errorf("inspect dashboard feed socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return "", fmt.Errorf("dashboard feed path %q is not a socket", socket)
	}
	c := newCollector(socket)
	defer c.client.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://node/snapshot", nil)
	if err != nil {
		return "", err
	}
	started := time.Now()
	response, err := c.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("read dashboard origin from the private feed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusServiceUnavailable || response.StatusCode == http.StatusTooManyRequests {
		return "", fmt.Errorf("%w (HTTP %d); check node startup", errManagedFeedNotReady, response.StatusCode)
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("dashboard feed returned HTTP %d; check the feed socket and node version", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxPrivateSnapshotBytes+1))
	if err != nil {
		return "", fmt.Errorf("read dashboard snapshot from the private feed: %w", err)
	}
	received := time.Now()
	now := started.Add(received.Sub(started))
	remaining, hasLease, validLease := statusLease(response.Header)
	var snapshot Snapshot
	if len(body) > MaxPrivateSnapshotBytes || !validLease || json.Unmarshal(body, &snapshot) != nil || snapshot.validate() != nil || snapshot.SampledAt.After(now.Add(time.Second)) {
		return "", errors.New("dashboard feed returned an invalid snapshot; check the node and reader versions and the host clock")
	}
	if !clusterClockConsistent(c.clockOrigin, received) || !currentManagedSample(started, received, now, snapshot.SampledAt, remaining, hasLease) {
		return "", fmt.Errorf("%w: dashboard feed did not provide fresh status; check node readiness and the host clock", errManagedFeedNotReady)
	}
	origins := response.Header.Values(readerOriginHeader)
	if len(origins) == 0 {
		return "", errors.New("dashboard feed does not declare an origin; set dashboard.public_origin on the node and use a matched release")
	}
	if len(origins) != 1 {
		return "", errors.New("dashboard feed declares more than one public origin")
	}
	if err := ValidateManagedOrigin(origins[0]); err != nil {
		return "", fmt.Errorf("invalid dashboard origin in the private feed: %w", err)
	}
	return origins[0], nil
}

// currentManagedSample applies both status deadlines before an origin is trusted.
func currentManagedSample(started, received, now, sampled time.Time, remaining time.Duration, hasLease bool) bool {
	remainingAge := StaleAfter - max(0, now.Sub(sampled))
	if remainingAge <= 0 {
		return false
	}
	if !hasLease {
		return true
	}
	until := started.Add(remaining)
	return statusLeaseContinuous(until.Sub(received), until.Round(0).Sub(received.Round(0)))
}

func waitForManagedOrigin(ctx context.Context, socket string) (string, error) {
	ticker := time.NewTicker(SampleInterval)
	defer ticker.Stop()
	var nextLog time.Time
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		origin, err := nodeReaderOrigin(ctx, socket)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		var timeout net.Error
		if err == nil || !(errors.Is(err, errManagedFeedNotReady) || errors.Is(err, os.ErrNotExist) ||
			errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
			errors.As(err, &timeout) && timeout.Timeout()) {
			return origin, err
		}
		if now := time.Now(); !now.Before(nextLog) {
			slog.Info("dashboard reader waiting for the private feed", "error", err)
			nextLog = now.Add(time.Minute)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// CheckReaderFromNode discovers the HTTPS origin and checks a fresh snapshot
// from the same configured feed without binding an HTTP listener.
func CheckReaderFromNode(ctx context.Context, socket, address string, options PublicOptions) (string, error) {
	if err := validateReaderAddress(socket, address); err != nil {
		return "", err
	}
	origin, err := nodeReaderOrigin(ctx, socket)
	if err != nil {
		return "", err
	}
	if err := checkReader(ctx, socket, address, origin, options, true); err != nil {
		return "", err
	}
	return origin, nil
}

// ServeReaderFromNode binds a loopback reader using the HTTPS origin declared
// in the private feed. A later change in that origin exits for a fresh lookup.
func ServeReaderFromNode(ctx context.Context, socket, address string, options PublicOptions) error {
	if err := validateReaderAddress(socket, address); err != nil {
		return err
	}
	origin, err := waitForManagedOrigin(ctx, socket)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	return listen(ctx, socket, address, origin, options, true)
}
