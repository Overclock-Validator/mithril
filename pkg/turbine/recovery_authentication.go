package turbine

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/gagliardetto/solana-go"
)

// authenticateRecoveredFEC follows Agave's merkle::recover: reconstruct every
// missing shard, rebuild the full tree (including coding shreds), and compare
// its root with a received shred's authenticated root before publishing data.
// Signature verification of received shreds is the ingress caller's contract;
// Reed-Solomon reconstruction alone is never evidence of authenticity.
func (a *SlotAssembler) authenticateRecoveredFEC(f *fecState, shards [][]byte, recovered []*Shred) error {
	// Agave takes its template from the highest-position received coding shred.
	var code *Shred
	for _, s := range f.coding {
		if code == nil || s.Position > code.Position {
			code = s
		}
	}
	if code == nil {
		return fmt.Errorf("recover FEC: missing coding template")
	}
	proofSize, chained, _, ok := merkleVariantInfo(code.Variant)
	if !ok || int(proofSize) != bits.Len(uint(len(shards)-1)) {
		return fmt.Errorf("recover FEC: invalid Merkle proof depth")
	}
	if code.Index < uint32(code.Position) {
		return fmt.Errorf("recover FEC: invalid coding index")
	}
	codeBase := code.Index - uint32(code.Position)
	if uint64(codeBase)+uint64(f.layout.codingShreds)-1 > math.MaxUint32 {
		return fmt.Errorf("recover FEC: coding index overflow")
	}
	expected, err := code.MerkleRoot()
	if err != nil {
		return err
	}
	encoder, err := a.fecEncoder(f.layout)
	if err != nil {
		return err
	}
	// Present shards are read-only. Only absent coding shards remain after the
	// caller's data recovery, so this also checks commitments to missing parity.
	if err = encoder.Reconstruct(shards); err != nil {
		return fmt.Errorf("recover FEC authentication: %w", err)
	}
	dataCount := int(f.layout.dataShreds)
	data := make(map[uint32]*Shred, len(recovered))
	for _, s := range recovered {
		// Chained root and retransmitter signature are outside the erasure region.
		// Copy the template suffix now, then replace its proof after authenticating.
		copy(s.Payload[shredSignatureSize+f.layout.shardSize:], code.Payload[codingHeaderSize+f.layout.shardSize:])
		data[s.Index-f.fecSetIndex] = s
	}
	nodes := make([]solana.Hash, 0, merkleTreeSize(len(shards)))
	for i, shard := range shards {
		var s *Shred
		if i < dataCount {
			s = f.data[uint32(i)]
			if s == nil {
				s = data[uint32(i)]
			}
		} else {
			pos := uint16(i - dataCount)
			s = f.coding[pos]
			if s == nil {
				payload := append([]byte(nil), code.Payload...)
				binary.LittleEndian.PutUint32(payload[shredIndexOffset:], codeBase+uint32(pos))
				binary.LittleEndian.PutUint16(payload[codingPositionOffset:], pos)
				copy(payload[codingHeaderSize:], shard)
				// Only header fields and bytes consumed by merkleLeaf are needed here.
				copyOfCode := *code
				copyOfCode.Payload = payload
				copyOfCode.Index = codeBase + uint32(pos)
				copyOfCode.Position = pos
				s = &copyOfCode
			}
		}
		if s == nil {
			return fmt.Errorf("recover FEC: missing reconstructed data %d", i)
		}
		leaf, err := s.merkleLeaf()
		if err != nil {
			return err
		}
		nodes = append(nodes, leaf)
	}
	for size := len(shards); size > 1; size = (size + 1) >> 1 {
		offset := len(nodes) - size
		for i := 0; i < size; i += 2 {
			right := min(i+1, size-1)
			nodes = append(nodes, merkleHashNode(nodes[offset+i][:merkleProofEntrySize], nodes[offset+right][:merkleProofEntrySize]))
		}
	}
	if nodes[len(nodes)-1] != expected {
		return fmt.Errorf("%w: recovered FEC Merkle root mismatch slot=%d fec_set=%d", ErrInvalidSignature, f.slot, f.fecSetIndex)
	}
	proofOffset := shredSignatureSize + f.layout.shardSize
	if chained {
		proofOffset += merkleRootSize
	}
	for _, s := range recovered {
		writeMerkleProof(s.Payload[proofOffset:], nodes, int(s.Index-f.fecSetIndex), len(shards))
	}
	return nil
}
