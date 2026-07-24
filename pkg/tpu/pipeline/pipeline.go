package pipeline

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/sigverifytelemetry"
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

const (
	telemetryDispatchMaxPackets = 64
)

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
			runSigverifyWorker(in, out, stats)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		verifyWG.Wait()
		close(out)
	}()
}

func runSigverifyWorker(
	in <-chan packet.Packet,
	out chan<- packet.Packet,
	stats *SigverifyStats,
) {
	for {
		pkt, ok := <-in
		if !ok {
			return
		}
		mode := sigverifytelemetry.CurrentMode()
		if mode == sigverifytelemetry.CollectionDisabled {
			processSigverifyPacket(pkt, out, stats)
			continue
		}
		if mode == sigverifytelemetry.CollectionPassive {
			queued := len(in)
			dispatch := sigverifytelemetry.ReserveSignatureDispatch(sigverifytelemetry.SourceTPU, queued, queued, 1)
			prepared := prepareTelemetrySigverifyPacket(pkt)
			counts := [1]uint16{prepared.signatures}
			dispatch.Ready(counts[:])
			processTelemetrySigverifyPacket(prepared, dispatch.Job(0), out, stats)
			continue
		}

		rawPackets, queuedBefore, queuedAfter, inputClosed := collectSigverifyTelemetryBatch(pkt, in)
		dispatch := sigverifytelemetry.ReserveSignatureDispatch(
			sigverifytelemetry.SourceTPU,
			queuedBefore,
			queuedAfter,
			len(rawPackets),
		)
		packets := make([]telemetrySigverifyPacket, len(rawPackets))
		signatureCounts := make([]uint16, len(rawPackets))
		for i, raw := range rawPackets {
			packets[i] = prepareTelemetrySigverifyPacket(raw)
			signatureCounts[i] = packets[i].signatures
		}
		dispatch.Ready(signatureCounts)
		for i, claimed := range packets {
			processTelemetrySigverifyPacket(claimed, dispatch.Job(i), out, stats)
		}
		if inputClosed {
			return
		}
	}
}

func processTelemetrySigverifyPacket(prepared telemetrySigverifyPacket, dispatch sigverifytelemetry.DispatchJob, out chan<- packet.Packet, stats *SigverifyStats) {
	data := prepared.packet.Data()
	atomic.AddUint64(&stats.InPackets, 1)
	atomic.AddUint64(&stats.InBytes, uint64(len(data)))

	if !verifyTelemetrySigverifyPacket(prepared, dispatch) {
		atomic.AddUint64(&stats.DroppedSigverify, 1)
		prepared.packet.Release()
		return
	}

	atomic.AddUint64(&stats.VerifiedPackets, 1)
	atomic.AddUint64(&stats.VerifiedBytes, uint64(len(data)))
	out <- prepared.packet
}

func processSigverifyPacket(pkt packet.Packet, out chan<- packet.Packet, stats *SigverifyStats) {
	data := pkt.Data()
	atomic.AddUint64(&stats.InPackets, 1)
	atomic.AddUint64(&stats.InBytes, uint64(len(data)))

	if !verifyPacket(data) {
		atomic.AddUint64(&stats.DroppedSigverify, 1)
		pkt.Release()
		return
	}

	atomic.AddUint64(&stats.VerifiedPackets, 1)
	atomic.AddUint64(&stats.VerifiedBytes, uint64(len(data)))
	out <- pkt
}

// collectSigverifyTelemetryBatch is used only by explicit scheduling
// simulation. It claims packets that are already visible and stops at the
// profiling cap without waiting. Parsing happens only after the dispatch claim
// has been reserved in the shared event timeline.
func collectSigverifyTelemetryBatch(first packet.Packet, in <-chan packet.Packet) ([]packet.Packet, int, int, bool) {
	rawPackets := make([]packet.Packet, 0, telemetryDispatchMaxPackets)
	rawPackets = append(rawPackets, first)
	queuedBefore := len(in)
	inputClosed := false

collect:
	for len(rawPackets) < telemetryDispatchMaxPackets {
		select {
		case pkt, ok := <-in:
			if !ok {
				inputClosed = true
				break collect
			}
			rawPackets = append(rawPackets, pkt)
		default:
			break collect
		}
	}
	queuedAfter := len(in)
	return rawPackets, queuedBefore, queuedAfter, inputClosed
}
