package testutils

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/tpu"
	"github.com/Overclock-Validator/mithril/pkg/tpu/quicserver"
)

// BenchOptions configures the TPU QUIC bench client.
type BenchOptions struct {
	Addr          string
	Duration      time.Duration
	Connections   int
	PayloadSize   int
	Warmup        time.Duration
	ProgressEvery time.Duration
	DialTimeout   time.Duration
	StreamTimeout time.Duration
}

// DefaultBenchOptions returns defaults suitable for local bench runs.
func DefaultBenchOptions() BenchOptions {
	return BenchOptions{
		Addr:          "127.0.0.1:0",
		Duration:      10 * time.Second,
		Connections:   1,
		PayloadSize:   64,
		Warmup:        0,
		ProgressEvery: time.Second,
		DialTimeout:   5 * time.Second,
		StreamTimeout: 3 * time.Second,
	}
}

// BenchResult summarizes a completed bench run.
type BenchResult struct {
	Sent    uint64
	Failed  uint64
	Bytes   uint64
	Elapsed time.Duration
	Pps     float64
	Gbps    float64
}

// ParseBenchFlags parses bench client flags from args.
func ParseBenchFlags(fs *flag.FlagSet, args []string) (BenchOptions, error) {
	opts := DefaultBenchOptions()

	fs.StringVar(&opts.Addr, "addr", opts.Addr, "TPU QUIC server address (host:port)")
	fs.DurationVar(&opts.Duration, "duration", opts.Duration, "Bench duration after warmup")
	fs.IntVar(&opts.Connections, "connections", opts.Connections, "Concurrent QUIC connections")
	fs.IntVar(&opts.PayloadSize, "payload-size", opts.PayloadSize, fmt.Sprintf("Transaction payload size in bytes (max %d)", quicserver.PacketDataSize))
	fs.DurationVar(&opts.Warmup, "warmup", opts.Warmup, "Warmup duration excluded from results")
	fs.DurationVar(&opts.ProgressEvery, "progress-every", opts.ProgressEvery, "Progress reporting interval; 0 disables")
	fs.DurationVar(&opts.DialTimeout, "dial-timeout", opts.DialTimeout, "QUIC dial timeout")
	fs.DurationVar(&opts.StreamTimeout, "stream-timeout", opts.StreamTimeout, "Per-transaction stream timeout")

	if err := fs.Parse(args); err != nil {
		return BenchOptions{}, err
	}
	if opts.Addr == "" {
		return BenchOptions{}, fmt.Errorf("addr is required")
	}
	if opts.Duration <= 0 {
		return BenchOptions{}, fmt.Errorf("duration must be > 0")
	}
	if opts.Connections <= 0 {
		return BenchOptions{}, fmt.Errorf("connections must be > 0")
	}
	if opts.PayloadSize <= 0 || opts.PayloadSize > quicserver.PacketDataSize {
		return BenchOptions{}, fmt.Errorf("payload-size must be in [1, %d]", quicserver.PacketDataSize)
	}
	return opts, nil
}

// RunBench hammers a TPU QUIC server with one uni-stream per transaction.
func RunBench(ctx context.Context, opts BenchOptions) (BenchResult, error) {
	client, err := NewClient()
	if err != nil {
		return BenchResult{}, err
	}

	payload := make([]byte, opts.PayloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	var sent atomic.Uint64
	var failed atomic.Uint64
	var bytes atomic.Uint64

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if opts.Warmup > 0 {
		warmupCtx, warmupCancel := context.WithTimeout(runCtx, opts.Warmup)
		var wg sync.WaitGroup
		for i := 0; i < opts.Connections; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				benchConnLoop(warmupCtx, client, opts, payload, &sent, &failed, &bytes)
			}()
		}
		wg.Wait()
		warmupCancel()
		sent.Store(0)
		failed.Store(0)
		bytes.Store(0)
	}

	start := time.Now()
	benchCtx, benchCancel := context.WithTimeout(runCtx, opts.Duration)
	defer benchCancel()

	var wg sync.WaitGroup
	for i := 0; i < opts.Connections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			benchConnLoop(benchCtx, client, opts, payload, &sent, &failed, &bytes)
		}()
	}

	if opts.ProgressEvery > 0 {
		go reportBenchProgress(benchCtx, start, &sent, &failed, &bytes, opts.ProgressEvery)
	}

	wg.Wait()
	elapsed := time.Since(start)

	result := BenchResult{
		Sent:    sent.Load(),
		Failed:  failed.Load(),
		Bytes:   bytes.Load(),
		Elapsed: elapsed,
	}
	if secs := elapsed.Seconds(); secs > 0 {
		result.Pps, result.Gbps = tpu.AverageRates(result.Sent, result.Bytes, secs)
	}
	return result, nil
}

func benchConnLoop(
	ctx context.Context,
	client *Client,
	opts BenchOptions,
	payload []byte,
	sent *atomic.Uint64,
	failed *atomic.Uint64,
	bytes *atomic.Uint64,
) {
	dialCtx, cancel := context.WithTimeout(ctx, opts.DialTimeout)
	conn, err := client.Dial(dialCtx, opts.Addr)
	cancel()
	if err != nil {
		failed.Add(1)
		return
	}
	defer conn.CloseWithError(0, "bench done")

	for ctx.Err() == nil {
		streamCtx, cancel := context.WithTimeout(ctx, opts.StreamTimeout)
		err := client.Send(streamCtx, conn, payload)
		cancel()
		if err != nil {
			failed.Add(1)
			if ctx.Err() != nil {
				return
			}
			continue
		}
		sent.Add(1)
		bytes.Add(uint64(len(payload)))
	}
}

func reportBenchProgress(
	ctx context.Context,
	start time.Time,
	sent *atomic.Uint64,
	failed *atomic.Uint64,
	bytes *atomic.Uint64,
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastSent, lastBytes uint64
	lastAt := start

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			curSent := sent.Load()
			curBytes := bytes.Load()
			elapsed := now.Sub(lastAt).Seconds()
			if elapsed <= 0 {
				continue
			}
			sentDelta := curSent - lastSent
			byteDelta := curBytes - lastBytes
			pps, gbps := tpu.IntervalRates(sentDelta, byteDelta, elapsed)
			fmt.Fprintf(
				os.Stdout,
				"bench pps=%.0f gbps=%.3f sent=%d failed=%d bytes=%d\n",
				pps,
				gbps,
				curSent,
				failed.Load(),
				curBytes,
			)
			lastSent = curSent
			lastBytes = curBytes
			lastAt = now
		}
	}
}

// MainBench is the CLI entrypoint for cmd/tpu-bench.
func MainBench(args []string) int {
	fs := flag.NewFlagSet("tpu-bench", flag.ExitOnError)
	opts, err := ParseBenchFlags(fs, args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tpu-bench: %v\n", err)
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	result, err := RunBench(ctx, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tpu-bench: %v\n", err)
		return 1
	}

	fmt.Fprintf(
		os.Stdout,
		"done pps=%.0f gbps=%.3f sent=%d failed=%d bytes=%d elapsed=%s\n",
		result.Pps,
		result.Gbps,
		result.Sent,
		result.Failed,
		result.Bytes,
		result.Elapsed,
	)
	return 0
}
