package turbine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/klauspost/reedsolomon"
)

// recoverMerkleDataPackets restores missing wire packets from one previously
// signature-verified FEC set. It does not authenticate an arbitrary sender.
func recoverMerkleDataPackets(packets [][]byte) ([][]byte, error) {
	if len(packets) == 0 || len(packets) > 256 {
		return nil, fmt.Errorf("invalid recovery packet count %d", len(packets))
	}
	shreds := make([]*Shred, len(packets))
	var code *Shred
	for i, packet := range packets {
		shred, err := ParseShred(packet)
		if err != nil {
			return nil, err
		}
		shreds[i] = shred
		if shred.Type == ShredTypeCode {
			code = shred
		}
	}
	if code == nil {
		return nil, fmt.Errorf("recovery needs a coding shred")
	}
	nd, nc := int(code.NumDataShreds), int(code.NumCodingShreds)
	if nd == 0 || nc == 0 || nd+nc > 256 || code.Position >= code.NumCodingShreds || code.Index < uint32(code.Position) || uint64(code.FECSetIndex)+uint64(nd) > math.MaxUint32 {
		return nil, fmt.Errorf("invalid recovery layout")
	}
	codeBase := code.Index - uint32(code.Position)
	if uint64(codeBase)+uint64(nc) > math.MaxUint32 {
		return nil, fmt.Errorf("coding index overflow")
	}
	proofSize, chained, resigned, ok := merkleVariantInfo(code.Variant)
	if !ok {
		return nil, ErrUnsupportedShred
	}
	root, err := code.MerkleRoot()
	if err != nil {
		return nil, err
	}
	dataVariant, ok := merkleCounterpartVariant(code.Variant, ShredTypeData)
	if !ok {
		return nil, ErrUnsupportedShred
	}
	capacity, err := merkleCapacity(codingPayloadSize, codingHeaderSize, proofSize, chained, resigned)
	if err != nil {
		return nil, err
	}
	dataCap, err := merkleCapacity(dataPayloadSize, dataHeaderSize, proofSize, chained, resigned)
	if err != nil {
		return nil, err
	}
	shards := make([][]byte, nd+nc)
	all := make([][]byte, nd+nc)
	for _, shred := range shreds {
		if shred.Slot != code.Slot || shred.FECSetIndex != code.FECSetIndex || shred.Version != code.Version || shred.Signature != code.Signature {
			return nil, fmt.Errorf("mixed recovery FEC headers")
		}
		index := int(shred.Index - shred.FECSetIndex)
		if shred.Type == ShredTypeCode {
			if shred.Variant != code.Variant || shred.NumDataShreds != code.NumDataShreds || shred.NumCodingShreds != code.NumCodingShreds || shred.Position >= code.NumCodingShreds || shred.Index != codeBase+uint32(shred.Position) {
				return nil, fmt.Errorf("mixed recovery coding layout")
			}
			index = nd + int(shred.Position)
		} else if shred.Variant != dataVariant || shred.Index < code.FECSetIndex || index >= nd {
			return nil, fmt.Errorf("invalid recovery data index")
		}
		gotRoot, err := shred.MerkleRoot()
		if err != nil {
			return nil, err
		}
		if gotRoot != root {
			return nil, fmt.Errorf("mixed recovery Merkle roots")
		}
		if all[index] != nil {
			if !bytes.Equal(all[index], shred.Payload) {
				return nil, fmt.Errorf("conflicting recovery duplicate")
			}
			continue
		}
		shard, err := shred.erasureShard()
		if err != nil {
			return nil, err
		}
		if len(shard) != capacity {
			return nil, fmt.Errorf("mixed recovery shard size")
		}
		all[index], shards[index] = shred.Payload, shard
	}
	var missing []int
	for i := 0; i < nd; i++ {
		if all[i] == nil {
			missing = append(missing, i)
		}
	}
	if len(missing) == 0 {
		return nil, nil
	}
	encoder, err := reedsolomon.New(nd, nc)
	if err != nil {
		return nil, err
	}
	if err := encoder.Reconstruct(shards); err != nil {
		return nil, err
	}
	fec := fecState{slot: code.Slot, fecSetIndex: code.FECSetIndex, coding: map[uint16]*Shred{code.Position: code}}
	for i := range all {
		if all[i] != nil {
			continue
		}
		if i < nd {
			shred, err := fec.recoveredDataShred(uint32(i), shards[i])
			if err != nil {
				return nil, err
			}
			if int(binary.LittleEndian.Uint16(shred.Payload[dataSizeOffset:])) > dataHeaderSize+dataCap || uint64(shred.ParentOffset) > shred.Slot {
				return nil, fmt.Errorf("invalid recovered data header")
			}
			all[i] = shred.Payload
			copy(all[i][dataHeaderSize+dataCap:], code.Payload[codingHeaderSize+capacity:])
		} else {
			packet := bytes.Clone(code.Payload)
			binary.LittleEndian.PutUint32(packet[shredIndexOffset:], codeBase+uint32(i-nd))
			binary.LittleEndian.PutUint16(packet[codingPositionOffset:], uint16(i-nd))
			copy(packet[codingHeaderSize:codingHeaderSize+capacity], shards[i])
			all[i] = packet
		}
	}
	tree, err := buildMerkleTree(all)
	if err != nil {
		return nil, err
	}
	if tree[len(tree)-1] != root {
		return nil, fmt.Errorf("recovered Merkle root mismatch")
	}
	proofOffset := dataHeaderSize + dataCap
	if chained {
		proofOffset += merkleRootSize
	}
	recovered := make([][]byte, 0, len(missing))
	for _, index := range missing {
		proof := makeMerkleProof(tree, index, len(all))
		if len(proof) != int(proofSize) {
			return nil, fmt.Errorf("invalid recovered proof size")
		}
		for i, entry := range proof {
			copy(all[index][proofOffset+i*merkleProofEntrySize:], entry[:])
		}
		recovered = append(recovered, all[index])
	}
	return recovered, nil
}
