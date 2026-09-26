package mcp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/internal/mcpwire"
)

const (
	mithrilEndpointHeader  = mcpwire.EndpointHeader
	mithrilMetricsEndpoint = mcpwire.MetricsEndpoint
	mithrilPprofEndpoint   = mcpwire.PprofEndpoint
	outboundHTTPTimeout    = 10 * time.Second
)

type ipResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

// readCappedBody reads resp.Body up to max bytes, rejecting an over-cap body
// even when Content-Length is absent or lies. The caller checks status first.
func readCappedBody(resp *http.Response, max int64) ([]byte, error) {
	if resp.ContentLength > max {
		return nil, fmt.Errorf("response too large: %d bytes exceeds %d byte limit", resp.ContentLength, max)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if err != nil {
		return nil, sanitizeHTTPError(err)
	}
	if int64(len(body)) > max {
		return nil, fmt.Errorf("response body too large: exceeds %d byte limit", max)
	}
	return body, nil
}

func requireMithrilEndpoint(resp *http.Response, expected string) error {
	if resp.Header.Get(mithrilEndpointHeader) != expected {
		return errors.New("configured endpoint is not the expected Mithril service")
	}
	return nil
}

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func resolvePinnedIPs(ctx context.Context, resolver ipResolver, u *url.URL) ([]net.IP, error) {
	host := u.Hostname()
	if host == "" {
		return nil, errors.New("URL has no host")
	}
	if literal := net.ParseIP(host); literal != nil {
		if isBlockedIP(literal) {
			return nil, fmt.Errorf("URL resolves to blocked address: %s", literal)
		}
		return []net.IP{literal}, nil
	}
	addresses, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve URL hostname %q: %w", host, err)
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("URL hostname %q resolved to no addresses", host)
	}
	localhost := strings.EqualFold(strings.TrimSuffix(host, "."), "localhost")
	result := make([]net.IP, 0, len(addresses))
	for _, address := range addresses {
		if address.IP.IsLoopback() != localhost {
			if localhost {
				return nil, errors.New("localhost must resolve only to loopback addresses")
			}
			return nil, errors.New("non-local hostname resolves to a loopback address")
		}
		if isBlockedIP(address.IP) {
			return nil, fmt.Errorf("URL hostname resolves to blocked address: %s", address.IP)
		}
		result = append(result, address.IP)
	}
	return result, nil
}

type contextDialer func(context.Context, string, string) (net.Conn, error)

type cancelOnCloseBody struct {
	httpBody io.ReadCloser
	cancel   context.CancelFunc
}

func (body *cancelOnCloseBody) Read(p []byte) (int, error) { return body.httpBody.Read(p) }
func (body *cancelOnCloseBody) Close() error {
	err := body.httpBody.Close()
	body.cancel()
	return err
}

func pinnedDialContext(ips []net.IP, dial contextDialer) contextDialer {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("invalid resolved dial address: %w", err)
		}
		var lastErr error
		for _, ip := range ips {
			conn, err := dial(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		if lastErr == nil {
			lastErr = errors.New("no validated IP addresses")
		}
		return nil, lastErr
	}
}

// doPinnedRequest validates one DNS result and connects only to those IPs while
// preserving the HTTP host and TLS server name.
func doPinnedRequest(ctx context.Context, req *http.Request, timeout time.Duration) (*http.Response, error) {
	operationCtx, cancel := context.WithTimeout(ctx, timeout)
	ips, err := resolvePinnedIPs(operationCtx, net.DefaultResolver, req.URL)
	if err != nil {
		cancel()
		return nil, err
	}
	if req.URL.Scheme != "https" {
		for _, ip := range ips {
			if !ip.IsLoopback() {
				cancel()
				return nil, errors.New("unencrypted HTTP is permitted only for loopback targets")
			}
		}
	}
	req = req.Clone(operationCtx)
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:             nil,
		DialContext:       pinnedDialContext(ips, dialer.DialContext),
		ForceAttemptHTTP2: true,
		DisableKeepAlives: true,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: req.URL.Hostname(),
		},
	}
	client := &http.Client{Timeout: timeout, CheckRedirect: noRedirect, Transport: transport}
	response, err := client.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	response.Body = &cancelOnCloseBody{httpBody: response.Body, cancel: cancel}
	return response, nil
}
