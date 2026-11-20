package turbine

import "github.com/gagliardetto/solana-go"

// fecRecoveryScratch holds buffers that FEC recovery needs only for the
// duration of one recoverFEC call. It belongs to a SlotAssembler and, like the
// encoder cache next to it, is only touched while a.mu is held, so at most one
// recovery uses it at a time.
//
// Nothing in here may be referenced after recoverFEC returns:
//   - reconstructed shards are copied into a freshly allocated payload by
//     recoveredDataShred before they become a Shred;
//   - reconstructed coding shards and the coding template copy exist only to
//     compute Merkle leaves;
//   - Merkle proofs are copied out of nodes into each recovered payload.
//
// release drops every pointer to received shred payloads so the scratch does
// not keep finished slots alive.
type fecRecoveryScratch struct {
	shards      [][]byte
	bufs        [][]byte
	required    []bool
	byIndex     []*Shred
	nodes       []solana.Hash
	codePayload []byte
}

// recoveryScratch returns the area owned by this assembler under its lock.
func (a *SlotAssembler) recoveryScratch() *fecRecoveryScratch {
	if a.recovery == nil {
		a.recovery = &fecRecoveryScratch{}
	}
	return a.recovery
}

// shardSlice returns n nil shard entries.
func (s *fecRecoveryScratch) shardSlice(n int) [][]byte {
	if cap(s.shards) < n {
		s.shards = make([][]byte, n)
	}
	s.shards = s.shards[:n]
	clear(s.shards)
	return s.shards
}

// emptyShard returns a zero-length slice with capacity for size bytes,
// private to shard index i. Reed-Solomon fills zero-length entries in place
// when their capacity suffices, and overwrites every byte it hands back.
func (s *fecRecoveryScratch) emptyShard(i, size int) []byte {
	if len(s.bufs) <= i {
		s.bufs = append(s.bufs, make([][]byte, i+1-len(s.bufs))...)
	}
	if cap(s.bufs[i]) < size {
		s.bufs[i] = make([]byte, size)
	}
	return s.bufs[i][:0]
}

func (s *fecRecoveryScratch) requiredSlice(n int) []bool {
	if cap(s.required) < n {
		s.required = make([]bool, n)
	}
	s.required = s.required[:n]
	clear(s.required)
	return s.required
}

func (s *fecRecoveryScratch) shredsByIndex(n int) []*Shred {
	if cap(s.byIndex) < n {
		s.byIndex = make([]*Shred, n)
	}
	s.byIndex = s.byIndex[:n]
	clear(s.byIndex)
	return s.byIndex
}

func (s *fecRecoveryScratch) codingPayload(template []byte) []byte {
	if cap(s.codePayload) < len(template) {
		s.codePayload = make([]byte, len(template))
	}
	s.codePayload = s.codePayload[:len(template)]
	copy(s.codePayload, template)
	return s.codePayload
}

func (s *fecRecoveryScratch) merkleNodes(n int) []solana.Hash {
	if cap(s.nodes) < n {
		s.nodes = make([]solana.Hash, 0, n)
	}
	return s.nodes[:0]
}

func (s *fecRecoveryScratch) release() {
	clear(s.shards)
	clear(s.byIndex)
}
