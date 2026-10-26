package packet

// DataSize is the largest transaction payload accepted by TPU QUIC. Legacy
// and v0 transactions remain capped at 1232 bytes by wire sanitization; the
// larger buffer is for SIMD-0385 v1 transactions.
const DataSize = 4096

// Source identifies where a packet entered the TPU.
type Source uint8

const (
	// SourceQUIC is this validator's own TPU QUIC listener.
	SourceQUIC Source = iota
	// SourceRelayer is a relayer forwarding packets to this validator.
	SourceRelayer
	// SourceBlockEngine is a block engine streaming packets to this validator.
	SourceBlockEngine
	// NumSources is the number of sources.
	NumSources
)

func (s Source) String() string {
	switch s {
	case SourceQUIC:
		return "quic"
	case SourceRelayer:
		return "relayer"
	case SourceBlockEngine:
		return "block_engine"
	default:
		return "unknown"
	}
}

// Packet is a received TPU transaction payload moving through the pipeline.
type Packet struct {
	buf  []byte
	len  int
	pool *Pool
	slot uint32
	src  Source
}

// Owned returns a packet that owns its wire bytes.
func Owned(wire []byte) Packet {
	return OwnedFrom(wire, SourceQUIC)
}

// OwnedFrom returns a packet from src that owns its wire bytes.
func OwnedFrom(wire []byte, src Source) Packet {
	out := make([]byte, len(wire))
	copy(out, wire)
	return Packet{buf: out, len: len(wire), src: src}
}

// FromPool returns a packet backed by a pooled buffer. Call Release when done.
func FromPool(pool *Pool, buf []byte, n int, slot uint32) Packet {
	return Packet{buf: buf, len: n, pool: pool, slot: slot}
}

func (p Packet) Data() []byte {
	if p.len <= 0 {
		return nil
	}
	return p.buf[:p.len]
}

func (p Packet) Len() int {
	return p.len
}

func (p Packet) Source() Source {
	return p.src
}

func (p Packet) Release() {
	if p.pool != nil {
		p.pool.Release(p.slot)
		p.pool = nil
	}
}
