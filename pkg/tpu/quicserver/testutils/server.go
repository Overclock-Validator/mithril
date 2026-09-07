// Package testutils provides bench/fuzz helpers for the TPU QUIC server.
//
//	go run ./cmd/tpu-server -listen 127.0.0.1:10000
//	go run ./cmd/tpu-bench -addr 127.0.0.1:10000 -duration 30s
package testutils

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/tpu"
	"github.com/Overclock-Validator/mithril/pkg/tpu/pipeline"
	"github.com/Overclock-Validator/mithril/pkg/tpu/quicserver"
)

// ServerOptions configures a long-running TPU QUIC server with pipeline.
type ServerOptions struct {
	Listen        string
	StatsInterval time.Duration
	Config        quicserver.ServerConfig
	Pipeline      pipeline.Config
}

// DefaultServerOptions returns defaults suitable for local bench/fuzz runs.
func DefaultServerOptions() ServerOptions {
	cfg := quicserver.DefaultServerConfig()
	cfg.MaxConnectionsPerIP = cfg.MaxConnections
	return ServerOptions{
		Listen:        "127.0.0.1:0",
		StatsInterval: 5 * time.Second,
		Config:        cfg,
		Pipeline: pipeline.Config{
			SigverifyWorkers: runtime.GOMAXPROCS(0),
		},
	}
}

// ParseServerFlags parses server flags from args.
func ParseServerFlags(fs *flag.FlagSet, args []string) (ServerOptions, error) {
	opts := DefaultServerOptions()
	maxConnectionsPerIP := -1

	fs.StringVar(&opts.Listen, "listen", opts.Listen, "UDP listen address for the TPU QUIC server")
	fs.DurationVar(&opts.StatsInterval, "stats-interval", opts.StatsInterval, "Interval for printing server stats; 0 disables reporting")
	fs.IntVar(&opts.Config.MaxConnections, "max-connections", opts.Config.MaxConnections, "Maximum concurrent QUIC connections")
	fs.IntVar(&opts.Config.MaxStreamsPerConnection, "max-streams-per-conn", opts.Config.MaxStreamsPerConnection, "Maximum concurrent uni-streams per connection")
	fs.IntVar(&maxConnectionsPerIP, "max-connections-per-ip", -1, "Maximum concurrent QUIC connections per IP; default is max-connections")
	fs.IntVar(&opts.Pipeline.SigverifyWorkers, "sigverify-workers", opts.Pipeline.SigverifyWorkers, "Sigverify worker count")

	if err := fs.Parse(args); err != nil {
		return ServerOptions{}, err
	}
	if opts.StatsInterval < 0 {
		return ServerOptions{}, fmt.Errorf("stats-interval must be >= 0")
	}
	if maxConnectionsPerIP < 0 {
		opts.Config.MaxConnectionsPerIP = opts.Config.MaxConnections
	} else {
		opts.Config.MaxConnectionsPerIP = maxConnectionsPerIP
	}
	return opts, nil
}

// RunServer starts a TPU QUIC server wired into the ingress pipeline.
func RunServer(ctx context.Context, opts ServerOptions) error {
	udpAddr, err := parseListenAddr(opts.Listen)
	if err != nil {
		return fmt.Errorf("parse listen address: %w", err)
	}

	_, identity, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("generate server identity: %w", err)
	}

	tpuCfg := tpu.Config{
		Name:       "testutils-tpu",
		ListenAddr: udpAddr.String(),
		Identity:   identity,
		QUIC:       opts.Config,
		Pipeline:   opts.Pipeline,
	}
	svc, err := tpu.Start(ctx, tpuCfg)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = svc.Stop(shutdownCtx)
	}()

	fmt.Fprintf(os.Stdout, "tpu server listening on %s\n", svc.ListenAddr())

	if opts.StatsInterval > 0 {
		go reportServerStats(ctx, svc, opts.StatsInterval)
	}

	<-ctx.Done()
	return ctx.Err()
}

func reportServerStats(ctx context.Context, svc *tpu.TPU, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastPackets, lastBytes uint64
	lastAt := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			stats := svc.Stats()
			packets := stats.Pipeline.Sink.InPackets
			bytes := stats.Pipeline.Sink.InBytes
			elapsed := now.Sub(lastAt).Seconds()
			if elapsed <= 0 {
				continue
			}

			packetDelta := packets - lastPackets
			byteDelta := bytes - lastBytes
			pps, gbps := tpu.IntervalRates(packetDelta, byteDelta, elapsed)

			fmt.Fprintf(
				os.Stdout,
				"stats pps=%.0f gbps=%.3f sink=%d dedup_out=%d sigverify=%d sanitize_drop=%d version_drop=%d dedup_drop=%d sig_drop=%d ingress_drop=%d open_conns=%d active_streams=%d\n",
				pps,
				gbps,
				packets,
				stats.Pipeline.Dedup.OutPackets,
				stats.Pipeline.Sigverify.VerifiedPackets,
				stats.Pipeline.Dedup.DroppedSanitize,
				stats.Pipeline.Dedup.DroppedUnsupportedVersion,
				stats.Pipeline.Dedup.DroppedDedup,
				stats.Pipeline.Sigverify.DroppedSigverify,
				stats.Pipeline.Ingress.DroppedFull,
				stats.QUIC.OpenConnections.Load(),
				stats.QUIC.ActiveStreams.Load(),
			)

			lastPackets = packets
			lastBytes = bytes
			lastAt = now
		}
	}
}

// MainServer is the CLI entrypoint for cmd/tpu-server.
func MainServer(args []string) int {
	fs := flag.NewFlagSet("tpu-server", flag.ExitOnError)
	opts, err := ParseServerFlags(fs, args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tpu-server: %v\n", err)
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := RunServer(ctx, opts); err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "tpu-server: %v\n", err)
		return 1
	}
	return 0
}

func parseListenAddr(listen string) (*net.UDPAddr, error) {
	if listen == "" {
		return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}, nil
	}
	host, portStr, err := net.SplitHostPort(listen)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, fmt.Errorf("invalid listen host %q", host)
	}
	return &net.UDPAddr{IP: ip, Port: port}, nil
}
