// Package harmonic connects a validator to a Harmonic block engine and relayer.
package harmonic

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/version"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	connectTimeout = 10 * time.Second
	// Fixed HTTP/2 windows disable BDP estimation, which restarts at 64 KiB
	// and would stall a bursty block stream.
	streamWindowBytes     = 16 << 20
	connWindowBytes       = 64 << 20
	maxRecvMessageBytes   = 64 << 20
	readBufferBytes       = 1 << 20
	socketRecvBufferBytes = 8 << 20
	// HTTP/2 PINGs run even while idle; an unanswered PING fails the
	// connection.
	defaultPingInterval = 10 * time.Second
	pingTimeout         = 10 * time.Second
	tcpKeepAliveIdle    = 60 * time.Second
	tcpKeepAliveEvery   = 10 * time.Second
	tcpKeepAliveProbes  = 3
)

// Endpoint is a block engine or relayer URL.
type Endpoint struct {
	// Target is host:port.
	Target string
	TLS    bool
	// ServerName is the TLS server name: the URL host unless overridden.
	ServerName string
}

// ParseEndpoint parses an http:// or https:// endpoint URL. A non-empty
// serverName overrides the TLS server name.
func ParseEndpoint(raw, serverName string) (Endpoint, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return Endpoint{}, fmt.Errorf("parse endpoint %q: %w", raw, err)
	}
	var ep Endpoint
	switch u.Scheme {
	case "https":
		ep.TLS = true
	case "http":
	default:
		return Endpoint{}, fmt.Errorf("endpoint %q: scheme must be http or https", raw)
	}
	host := u.Hostname()
	if host == "" {
		return Endpoint{}, fmt.Errorf("endpoint %q has no host", raw)
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if ep.TLS {
			port = "443"
		}
	}
	ep.Target = net.JoinHostPort(host, port)
	ep.ServerName = host
	if serverName != "" {
		ep.ServerName = serverName
	}
	return ep, nil
}

// DialConfig configures a connection to one endpoint.
type DialConfig struct {
	Endpoint Endpoint
	// InsecureSkipVerify disables TLS certificate verification. Development
	// only.
	InsecureSkipVerify bool
	// Credentials, if set, attaches per-RPC metadata such as a Bearer.
	Credentials credentials.PerRPCCredentials
	// PingInterval defaults to 10s.
	PingInterval time.Duration
	// Dialer replaces the TCP dialer, for tests.
	Dialer func(ctx context.Context, addr string) (net.Conn, error)
}

// Dial opens a connection and waits until it is ready, for at most 10s.
func Dial(ctx context.Context, cfg DialConfig) (*grpc.ClientConn, error) {
	ping := cfg.PingInterval
	if ping <= 0 {
		ping = defaultPingInterval
	}
	transport := insecure.NewCredentials()
	if cfg.Endpoint.TLS {
		if cfg.InsecureSkipVerify {
			mlog.Log.Infof("WARNING: Harmonic endpoint %s: TLS certificate verification is disabled", cfg.Endpoint.Target)
		}
		transport = credentials.NewTLS(&tls.Config{
			ServerName:         cfg.Endpoint.ServerName,
			MinVersion:         tls.VersionTLS13,
			InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // explicit development switch
		})
	}
	dialer := cfg.Dialer
	if dialer == nil {
		dialer = dialTCP
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithContextDialer(dialer),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                ping,
			Timeout:             pingTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithInitialWindowSize(streamWindowBytes),
		grpc.WithInitialConnWindowSize(connWindowBytes),
		grpc.WithReadBufferSize(readBufferBytes),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMessageBytes)),
		grpc.WithUserAgent("mithril/" + version.Semver()),
	}
	if cfg.Credentials != nil {
		opts = append(opts, grpc.WithPerRPCCredentials(cfg.Credentials))
	}
	conn, err := grpc.NewClient("passthrough:///"+cfg.Endpoint.Target, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", cfg.Endpoint.Target, err)
	}
	ctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	conn.Connect()
	for state := conn.GetState(); state != connectivity.Ready; state = conn.GetState() {
		if !conn.WaitForStateChange(ctx, state) {
			conn.Close()
			return nil, fmt.Errorf("connect %s: %w (last state %s)", cfg.Endpoint.Target, ctx.Err(), state)
		}
	}
	return conn, nil
}

func dialTCP(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{KeepAliveConfig: net.KeepAliveConfig{
		Enable:   true,
		Idle:     tcpKeepAliveIdle,
		Interval: tcpKeepAliveEvery,
		Count:    tcpKeepAliveProbes,
	}}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetReadBuffer(socketRecvBufferBytes)
	}
	return conn, nil
}
