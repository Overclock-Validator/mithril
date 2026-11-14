package turbine

import (
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

const socketMemoryFreshness = 30 * time.Second

// SocketDropDelta is the change between two successful reads of the same
// owner-held socket. A counter decrease is a reset, not a wrap.
type SocketDropDelta struct {
	Known                bool
	Drops                uint64
	StartedAt, SampledAt time.Time
}

// SocketMemorySample is one detached kernel read and its optional drop delta.
type SocketMemorySample struct {
	Observation metrics.SocketObservation
	DropDelta   SocketDropDelta
}

// Fresh reports whether a known sample is within the fixed source window.
func (s SocketMemorySample) Fresh(now time.Time) bool {
	elapsed := now.Sub(s.Observation.SampledAt)
	return s.Observation.Known && elapsed >= 0 && elapsed <= socketMemoryFreshness
}

// ReceiverSocketMemory contains only detached values from one receiver-owned
// live socket and its optional distinct repair socket.
type ReceiverSocketMemory struct {
	Generation     uint64
	RepairEnabled  bool
	Live           SocketMemorySample
	Repair         SocketMemorySample
	ReadActivity   SocketReadActivity
	RepairRequests RepairRequestActivity
}

// RepairRequestActivity counts payload prefixes accepted by WriteToUDP for
// repair requests on this socket generation. It excludes pongs and does not
// establish remote delivery or successful repair. Known false is not zero.
type RepairRequestActivity struct {
	Enabled              bool
	StartedAt, SampledAt time.Time
	Bytes                uint64
	Known                bool
	Window               SocketReadWindow
}

type repairRequestRecorder struct {
	conn      *net.UDPConn
	startedAt time.Time
	bytes     socketReadCounter
	previous  RepairRequestActivity // Accessed only under the socket owner lock.
}

// SocketReadActivity counts UDP payload bytes copied by actual socket reads,
// including invalid packets and truncated prefixes. It excludes direct packet
// injection, spool/FEC and network headers. Known false is not measured zero.
type SocketReadActivity struct {
	Enabled                  bool
	StartedAt, SampledAt     time.Time
	LiveBytes, RepairBytes   uint64
	LiveKnown, RepairKnown   bool
	LiveWindow, RepairWindow SocketReadWindow
}

// SocketReadWindow is a same-recorder copied-byte delta. Duration is monotonic;
// it does not infer full wire traffic or successful validation/delivery.
type SocketReadWindow struct {
	Known                bool
	StartedAt, SampledAt time.Time
	Duration             time.Duration
	Bytes                uint64
}

type socketReadCounter struct{ bytes atomic.Uint64 }

func (c *socketReadCounter) add(n uint64) {
	a := &c.bytes
	for {
		before := a.Load()
		if before == math.MaxUint64 {
			return
		}
		after := uint64(math.MaxUint64)
		if n < math.MaxUint64-before {
			after = before + n
		}
		if a.CompareAndSwap(before, after) {
			return
		}
	}
}

type socketReadRecorder struct {
	startedAt    time.Time
	live, repair socketReadCounter
	previous     SocketReadActivity // Accessed only under the socket owner lock.
}

func socketReadWindow(previousAt, now time.Time, before, after uint64, previousKnown, known bool) SocketReadWindow {
	elapsed := now.Sub(previousAt)
	if !previousKnown || !known || elapsed <= 0 || elapsed > socketMemoryFreshness || after < before {
		return SocketReadWindow{}
	}
	return SocketReadWindow{Known: true, StartedAt: previousAt, SampledAt: now, Duration: elapsed, Bytes: after - before}
}

// EnableSocketReadActivity must be selected before Run. It also observes repair
// request writes. Reattaching sockets creates new recorders; old I/O keeps its
// old recorder, never the new one.
func (r *UDPReceiver) EnableSocketReadActivity() { r.recordSocketReads = true }

func (r *UDPReceiver) socketReadCounter(conn *net.UDPConn, repair bool) *socketReadCounter {
	r.socketMemory.mu.Lock()
	defer r.socketMemory.mu.Unlock()
	o := &r.socketMemory
	if conn == nil || o.reads == nil {
		return nil
	}
	if repair && conn == o.repair {
		return &o.reads.repair
	}
	if !repair && conn == o.live {
		return &o.reads.live
	}
	return nil
}

type socketDropBaseline struct {
	known     bool
	drops     uint32
	sampledAt time.Time
}

type receiverSocketMemoryOwner struct {
	mu         sync.Mutex
	generation uint64
	live       *net.UDPConn
	repair     *net.UDPConn
	baseline   [2]socketDropBaseline
	observe    func(*net.UDPConn) metrics.SocketObservation
	reads      *socketReadRecorder
	requests   *repairRequestRecorder
}

func (r *UDPReceiver) attachSocketMemoryOwners(live, repair *net.UDPConn) uint64 {
	r.socketMemory.mu.Lock()
	defer r.socketMemory.mu.Unlock()
	r.socketMemory.generation++
	r.socketMemory.live, r.socketMemory.repair = live, repair
	r.socketMemory.baseline = [2]socketDropBaseline{}
	r.socketMemory.reads = nil
	r.socketMemory.requests = nil
	if r.recordSocketReads {
		r.socketMemory.reads = &socketReadRecorder{startedAt: time.Now()}
		if r.repairClient != nil && repair != nil {
			r.socketMemory.requests = &repairRequestRecorder{conn: repair, startedAt: r.socketMemory.reads.startedAt}
		}
	}
	if r.repairClient != nil {
		r.repairClient.requestWrites.Store(r.socketMemory.requests)
	}
	return r.socketMemory.generation
}

func (r *UDPReceiver) detachSocketMemoryOwners(generation uint64) {
	r.socketMemory.mu.Lock()
	defer r.socketMemory.mu.Unlock()
	if r.socketMemory.generation != generation {
		return
	}
	r.socketMemory.generation++
	r.socketMemory.live, r.socketMemory.repair = nil, nil
	r.socketMemory.baseline = [2]socketDropBaseline{}
	r.socketMemory.reads = nil
	r.socketMemory.requests = nil
	if r.repairClient != nil {
		r.repairClient.requestWrites.Store(nil)
	}
}

// ObserveSocketMemory samples each current receiver-owned socket once. It is
// stateful and must be called serially by one low-rate owner. It retains no
// descriptor and refuses a result if ownership changed during I/O.
func (r *UDPReceiver) ObserveSocketMemory() (ReceiverSocketMemory, bool) {
	r.socketMemory.mu.Lock()
	generation := r.socketMemory.generation
	live, repair := r.socketMemory.live, r.socketMemory.repair
	observe := r.socketMemory.observe
	r.socketMemory.mu.Unlock()
	if live == nil {
		return ReceiverSocketMemory{}, false
	}
	if observe == nil {
		observe = metrics.ObserveUDPSocketMemory
	}
	liveObservation := observe(live)
	var repairObservation metrics.SocketObservation
	if repair != nil {
		repairObservation = observe(repair)
	} else {
		repairObservation.Reason = metrics.SocketNotProvided
	}

	r.socketMemory.mu.Lock()
	defer r.socketMemory.mu.Unlock()
	if r.socketMemory.generation != generation || r.socketMemory.live != live || r.socketMemory.repair != repair {
		return ReceiverSocketMemory{}, false
	}
	result := ReceiverSocketMemory{
		Generation: generation, RepairEnabled: repair != nil,
		Live:   r.socketMemory.sample(liveObservation, 0),
		Repair: r.socketMemory.sample(repairObservation, 1),
	}
	if reads := r.socketMemory.reads; reads != nil {
		liveBytes := reads.live.bytes.Load()
		repairBytes := reads.repair.bytes.Load()
		var requestBytes uint64
		if requests := r.socketMemory.requests; requests != nil {
			requestBytes = requests.bytes.bytes.Load()
		}
		result.ReadActivity = SocketReadActivity{Enabled: true, StartedAt: reads.startedAt, SampledAt: time.Now(),
			LiveKnown: liveBytes != math.MaxUint64, RepairKnown: repair != nil && repairBytes != math.MaxUint64}
		if result.ReadActivity.LiveKnown {
			result.ReadActivity.LiveBytes = liveBytes
		}
		if result.ReadActivity.RepairKnown {
			result.ReadActivity.RepairBytes = repairBytes
		}
		current, previous := &result.ReadActivity, reads.previous
		current.LiveWindow = socketReadWindow(previous.SampledAt, current.SampledAt, previous.LiveBytes, current.LiveBytes, previous.LiveKnown, current.LiveKnown)
		current.RepairWindow = socketReadWindow(previous.SampledAt, current.SampledAt, previous.RepairBytes, current.RepairBytes, previous.RepairKnown, current.RepairKnown)
		reads.previous = *current
		if requests := r.socketMemory.requests; requests != nil {
			result.RepairRequests = RepairRequestActivity{Enabled: true, StartedAt: requests.startedAt,
				SampledAt: current.SampledAt, Known: requestBytes != math.MaxUint64}
			if result.RepairRequests.Known {
				result.RepairRequests.Bytes = requestBytes
			}
			current, previous := &result.RepairRequests, requests.previous
			current.Window = socketReadWindow(previous.SampledAt, current.SampledAt, previous.Bytes, current.Bytes, previous.Known, current.Known)
			requests.previous = *current
		}
	}
	return result, true
}

func (o *receiverSocketMemoryOwner) sample(observation metrics.SocketObservation, index int) SocketMemorySample {
	sample := SocketMemorySample{Observation: observation}
	baseline := &o.baseline[index]
	if !observation.Known {
		*baseline = socketDropBaseline{}
		return sample
	}
	if baseline.known {
		elapsed := observation.SampledAt.Sub(baseline.sampledAt)
		if elapsed > 0 && elapsed <= socketMemoryFreshness && observation.Memory.Drops >= baseline.drops {
			sample.DropDelta = SocketDropDelta{Known: true, Drops: uint64(observation.Memory.Drops - baseline.drops),
				StartedAt: baseline.sampledAt, SampledAt: observation.SampledAt}
		}
	}
	*baseline = socketDropBaseline{known: true, drops: observation.Memory.Drops, sampledAt: observation.SampledAt}
	return sample
}
