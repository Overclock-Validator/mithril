package packet

// DataSize is the largest transaction payload accepted by TPU QUIC. Legacy
// and v0 transactions remain capped at 1232 bytes by wire sanitization; the
// larger buffer is for SIMD-0385 v1 transactions.
const DataSize = 4096

// Packet is a received TPU transaction payload moving through the pipeline.
type Packet struct {
	buf  []byte
	len  int
	pool *Pool
	slot uint32
}

// Owned returns a packet that owns its wire bytes.
func Owned(wire []byte) Packet {
	out := make([]byte, len(wire))
	copy(out, wire)
	return Packet{buf: out, len: len(wire)}
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

func (p Packet) Release() {
	if p.pool != nil {
		p.pool.Release(p.slot)
		p.pool = nil
	}
}
