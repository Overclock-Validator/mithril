package bench

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/quicserver"
	"github.com/Overclock-Validator/mithril/pkg/tpu/quicserver/testutils"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
)

// QUICOptions configures the QUIC wire benchmark stage.
type QUICOptions struct {
	Options
	Connections int
}

func (o QUICOptions) normalized() QUICOptions {
	out := o
	out.Options = out.Options.normalized()
	if out.Connections <= 0 {
		out.Connections = 4
	}
	return out
}

type ingressCounter struct {
	packets atomic.Uint64
	bytes   atomic.Uint64
}

func startIngressCounter(ctx context.Context, cap int) (chan<- packet.Packet, *ingressCounter) {
	if cap <= 0 {
		cap = 1 << 14
	}
	ch := make(chan packet.Packet, cap)
	counter := &ingressCounter{}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case pkt, ok := <-ch:
				if !ok {
					return
				}
				counter.packets.Add(1)
				counter.bytes.Add(uint64(pkt.Len()))
				pkt.Release()
			}
		}
	}()
	return ch, counter
}

// RunQUIC benchmarks QUIC ingress throughput without downstream pipeline stages.
func RunQUIC(ctx context.Context, opts QUICOptions) (Result, error) {
	opts = opts.normalized()
	const name = "quic"

	fmt.Fprintf(os.Stderr, "[%s] precomputing %d signed transfers...\n", name, opts.PoolSize)
	pool := txfixture.NewPool(opts.PoolSize)
	payload := pool.Wire(0)
	if len(payload) == 0 {
		return Result{}, fmt.Errorf("empty signed transfer payload")
	}

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		return Result{}, fmt.Errorf("listen udp: %w", err)
	}
	defer udpConn.Close()

	holder, err := identity.NewEphemeralHolder()
	if err != nil {
		return Result{}, fmt.Errorf("generate identity: %w", err)
	}

	ingressCtx, ingressCancel := context.WithCancel(ctx)
	defer ingressCancel()
	ingress, counter := startIngressCounter(ingressCtx, 1<<14)

	cfg := quicserver.DefaultServerConfig()
	cfg.Ingress = ingress
	result, err := quicserver.Spawn(
		ctx,
		"bench-tpu-quic",
		[]quicserver.QuicSocket{quicserver.QuicSocketFromUDP(udpConn)},
		holder,
		cfg,
	)
	if err != nil {
		return Result{}, err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = result.Close(shutdownCtx)
	}()

	addr := result.Listeners[0].Addr().String()
	client, err := testutils.NewClient()
	if err != nil {
		return Result{}, err
	}

	start := time.Now()
	benchCtx, benchCancel := context.WithTimeout(ctx, opts.Duration)
	defer benchCancel()

	var lastPackets, lastBytes uint64
	lastAt := start

	var wg sync.WaitGroup
	for i := 0; i < opts.Connections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runQUICConn(benchCtx, client, addr, payload)
		}()
	}

	if opts.ProgressEvery > 0 {
		go func() {
			ticker := time.NewTicker(opts.ProgressEvery)
			defer ticker.Stop()
			for {
				select {
				case <-benchCtx.Done():
					return
				case now := <-ticker.C:
					curPackets := counter.packets.Load()
					curBytes := counter.bytes.Load()
					elapsed := now.Sub(lastAt).Seconds()
					if elapsed <= 0 {
						continue
					}
					printProgress(name, curPackets-lastPackets, curBytes-lastBytes, elapsed, curPackets, curBytes)
					lastPackets = curPackets
					lastBytes = curBytes
					lastAt = now
				}
			}
		}()
	}

	wg.Wait()

	benchResult := finalizeResult(name, counter.packets.Load(), counter.bytes.Load(), start)
	printDone(benchResult)
	return benchResult, nil
}

func runQUICConn(ctx context.Context, client *testutils.Client, addr string, payload []byte) {
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	conn, err := client.Dial(dialCtx, addr)
	cancel()
	if err != nil {
		return
	}
	defer conn.CloseWithError(0, "bench done")

	for ctx.Err() == nil {
		streamCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := client.Send(streamCtx, conn, payload)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
	}
}
