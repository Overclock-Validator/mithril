package dedup

import (
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/wire"
	"github.com/cespare/xxhash/v2"
)

// Stats tracks dedup+sanitize stage throughput.
type Stats struct {
	InPackets                 uint64
	InBytes                   uint64
	OutPackets                uint64
	OutBytes                  uint64
	DroppedSanitize           uint64
	DroppedUnsupportedVersion uint64
	DroppedDedup              uint64
}

// Snapshot returns a race-free copy while the stage is running.
func (s *Stats) Snapshot() Stats {
	if s == nil {
		return Stats{}
	}
	return Stats{
		InPackets:                 atomic.LoadUint64(&s.InPackets),
		InBytes:                   atomic.LoadUint64(&s.InBytes),
		OutPackets:                atomic.LoadUint64(&s.OutPackets),
		OutBytes:                  atomic.LoadUint64(&s.OutBytes),
		DroppedSanitize:           atomic.LoadUint64(&s.DroppedSanitize),
		DroppedUnsupportedVersion: atomic.LoadUint64(&s.DroppedUnsupportedVersion),
		DroppedDedup:              atomic.LoadUint64(&s.DroppedDedup),
	}
}

// Stage is a single-threaded xxhash dedup + wire sanitation funnel.
type Stage struct {
	In          <-chan packet.Packet
	Out         chan<- packet.Packet
	cache       *Cache
	txV1Enabled func() bool
}

// NewStage returns a dedup stage. Out should be a buffered channel.
func NewStage(in <-chan packet.Packet, out chan<- packet.Packet, cache *Cache, txV1Enabled func() bool) *Stage {
	if cache == nil {
		cache = NewCache(DefaultCacheCapacity)
	}
	return &Stage{
		In:          in,
		Out:         out,
		cache:       cache,
		txV1Enabled: txV1Enabled,
	}
}

// Run processes packets until In is closed.
func (s *Stage) Run(stats *Stats) {
	for pkt := range s.In {
		s.handle(pkt, stats)
	}
}

func (s *Stage) handle(pkt packet.Packet, stats *Stats) {
	if s.filter(pkt, stats) {
		s.Out <- pkt
	}
}

// filter runs sanitize + dedup. Returns true when the packet should be forwarded.
// On false the packet is released.
func (s *Stage) filter(pkt packet.Packet, stats *Stats) bool {
	return filterWire(pkt.Data(), s.cache, stats, s.txV1Enabled, true, func() { pkt.Release() })
}

// FilterWire runs sanitize+dedup on wire bytes without packet allocation.
// Returns true when the transaction would be forwarded.
func FilterWire(data []byte, cache *Cache, stats *Stats, onDrop func()) bool {
	return filterWire(data, cache, stats, nil, false, onDrop)
}

// filterWire rejects feature-disabled V1 packets before they touch the dedup
// cache. That matters at the activation boundary: a valid packet observed just
// before activation must remain admissible when it is resent just after it.
// A nil callback means no version policy for standalone dedup callers; the
// production Stage always receives the current-bank callback.
func filterWire(data []byte, cache *Cache, stats *Stats, txV1Enabled func() bool, enforceVersionPolicy bool, onDrop func()) bool {
	if stats != nil {
		atomic.AddUint64(&stats.InPackets, 1)
		atomic.AddUint64(&stats.InBytes, uint64(len(data)))
	}

	view, err := wire.Sanitize(data)
	if err != nil {
		if stats != nil {
			atomic.AddUint64(&stats.DroppedSanitize, 1)
		}
		if onDrop != nil {
			onDrop()
		}
		return false
	}
	if enforceVersionPolicy && view.Version == wire.VersionV1 && (txV1Enabled == nil || !txV1Enabled()) {
		if stats != nil {
			atomic.AddUint64(&stats.DroppedUnsupportedVersion, 1)
		}
		if onDrop != nil {
			onDrop()
		}
		return false
	}

	if cache.Seen(xxhash.Sum64(view.FirstSignature())) {
		if stats != nil {
			atomic.AddUint64(&stats.DroppedDedup, 1)
		}
		if onDrop != nil {
			onDrop()
		}
		return false
	}

	if stats != nil {
		atomic.AddUint64(&stats.OutPackets, 1)
		atomic.AddUint64(&stats.OutBytes, uint64(len(data)))
	}
	return true
}

// ProcessOne is for microbenchmarks and unit tests without a downstream channel.
func ProcessOne(pkt packet.Packet, cache *Cache, stats *Stats) {
	if FilterWire(pkt.Data(), cache, stats, func() { pkt.Release() }) {
		pkt.Release()
	}
}
