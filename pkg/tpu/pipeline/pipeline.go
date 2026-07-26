package pipeline

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/sigverify"
	"github.com/Overclock-Validator/mithril/pkg/tpu/dedup"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/sink"
)

// Config wires the TPU ingress pipeline stages.
type Config struct {
	SigverifyWorkers   int
	DedupCacheCapacity int
	IngressCap         int
	DedupOutCap        int
	VerifiedCap        int
	Sink               sink.Receiver
}

func (c Config) normalized() Config {
	out := c
	if out.SigverifyWorkers <= 0 {
		out.SigverifyWorkers = runtime.GOMAXPROCS(0)
	}
	if out.IngressCap <= 0 {
		out.IngressCap = 4096
	}
	if out.DedupOutCap <= 0 {
		out.DedupOutCap = 4096
	}
	if out.VerifiedCap <= 0 {
		out.VerifiedCap = 4096
	}
	if out.DedupCacheCapacity <= 0 {
		out.DedupCacheCapacity = dedup.DefaultCacheCapacity
	}
	if out.Sink == nil {
		out.Sink = &sink.Noop{}
	}
	return out
}

// Stats aggregates per-stage counters.
type Stats struct {
	Ingress   IngressStats
	Dedup     dedup.Stats
	Sigverify SigverifyStats
	Sink      sink.Stats
}

type IngressStats struct {
	EnqueuedPackets uint64
	EnqueuedBytes   uint64
	DroppedFull     uint64
}

type SigverifyStats struct {
	InPackets        uint64
	InBytes          uint64
	VerifiedPackets  uint64
	VerifiedBytes    uint64
	DroppedSigverify uint64
}

// Pipeline funnels QUIC ingress through dedup/sanitize, sigverify workers, and a sink.
type Pipeline struct {
	ingress  chan packet.Packet
	stats    Stats
	cfg      Config
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// Start launches the pipeline stages and returns the ingress send channel for QUIC.
func Start(parent context.Context, cfg Config) (*Pipeline, chan<- packet.Packet) {
	_ = parent
	cfg = cfg.normalized()

	p := &Pipeline{
		ingress: make(chan packet.Packet, cfg.IngressCap),
		cfg:     cfg,
	}

	dedupOut := make(chan packet.Packet, cfg.DedupOutCap)
	verified := make(chan packet.Packet, cfg.VerifiedCap)

	dedupStage := dedup.NewStage(p.ingress, dedupOut, dedup.NewCache(cfg.DedupCacheCapacity))
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(dedupOut)
		dedupStage.Run(&p.stats.Dedup)
	}()

	startSigverifyPool(&p.wg, dedupOut, verified, cfg.SigverifyWorkers, &p.stats.Sigverify)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		sink.Run(verified, cfg.Sink)
	}()

	return p, p.ingress
}

func (p *Pipeline) Stats() Stats {
	stats := Stats{
		Ingress: IngressStats{
			EnqueuedPackets: atomic.LoadUint64(&p.stats.Ingress.EnqueuedPackets),
			EnqueuedBytes:   atomic.LoadUint64(&p.stats.Ingress.EnqueuedBytes),
			DroppedFull:     atomic.LoadUint64(&p.stats.Ingress.DroppedFull),
		},
		Dedup: p.stats.Dedup.Snapshot(),
		Sigverify: SigverifyStats{
			InPackets:        atomic.LoadUint64(&p.stats.Sigverify.InPackets),
			InBytes:          atomic.LoadUint64(&p.stats.Sigverify.InBytes),
			VerifiedPackets:  atomic.LoadUint64(&p.stats.Sigverify.VerifiedPackets),
			VerifiedBytes:    atomic.LoadUint64(&p.stats.Sigverify.VerifiedBytes),
			DroppedSigverify: atomic.LoadUint64(&p.stats.Sigverify.DroppedSigverify),
		},
	}
	if noop, ok := p.cfg.Sink.(*sink.Noop); ok {
		stats.Sink = noop.Snapshot()
	}
	return stats
}

func (p *Pipeline) IngressStats() *IngressStats {
	return &p.stats.Ingress
}

func (p *Pipeline) Stop() {
	p.stopOnce.Do(func() {
		close(p.ingress)
		p.wg.Wait()
	})
}

// TryEnqueue attempts to hand a packet to ingress without blocking.
func TryEnqueue(ingress chan<- packet.Packet, pkt packet.Packet, stats *IngressStats) bool {
	select {
	case ingress <- pkt:
		if stats != nil {
			atomic.AddUint64(&stats.EnqueuedPackets, 1)
			atomic.AddUint64(&stats.EnqueuedBytes, uint64(pkt.Len()))
		}
		return true
	default:
		if stats != nil {
			atomic.AddUint64(&stats.DroppedFull, 1)
		}
		pkt.Release()
		return false
	}
}

func startSigverifyPool(
	wg *sync.WaitGroup,
	in <-chan packet.Packet,
	out chan<- packet.Packet,
	workers int,
	stats *SigverifyStats,
) {
	var verifyWG sync.WaitGroup
	for i := 0; i < workers; i++ {
		verifyWG.Add(1)
		wg.Add(1)
		go func() {
			defer verifyWG.Done()
			defer wg.Done()
			runSigverifyWorker(in, out, workers, stats)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		verifyWG.Wait()
		close(out)
	}()
}

// runSigverifyWorker verifies a DRAINED GROUP of packets per pass rather than
// one packet at a time.
//
// A transaction carries one or two signatures, and the vectorized backend
// verifies eight per group, so packet-at-a-time verification pays for a whole
// group and uses one lane of it. Draining costs nothing when ingress is quiet —
// the worker still takes exactly the packet it blocked for, and forwards it
// with unchanged latency — and turns a burst into free width.
func runSigverifyWorker(
	in <-chan packet.Packet,
	out chan<- packet.Packet,
	workers int,
	stats *SigverifyStats,
) {
	// Worker-local scratch, reused across groups.
	var (
		group    []packet.Packet
		payloads [][]byte
		verdicts []bool
		verifier batchVerifier
	)
	for pkt := range in {
		group = sigverify.Drain(group, pkt, in,
			sigverify.FairShare(len(in), workers, sigverify.MaxDrain()))

		payloads = payloads[:0]
		for _, p := range group {
			data := p.Data()
			atomic.AddUint64(&stats.InPackets, 1)
			atomic.AddUint64(&stats.InBytes, uint64(len(data)))
			payloads = append(payloads, data)
		}

		if cap(verdicts) < len(payloads) {
			verdicts = make([]bool, len(payloads))
		}
		verdicts = verdicts[:len(payloads)]
		verifier.Verify(payloads, verdicts)

		for i, p := range group {
			if !verdicts[i] {
				atomic.AddUint64(&stats.DroppedSigverify, 1)
				p.Release()
				continue
			}
			atomic.AddUint64(&stats.VerifiedPackets, 1)
			atomic.AddUint64(&stats.VerifiedBytes, uint64(len(payloads[i])))
			out <- p
		}

		// Released packets must not stay reachable through the scratch slices
		// until the next group happens to overwrite that index.
		clear(group)
		clear(payloads)
	}
}
