package turbine

import (
	"crypto/ed25519"
	"encoding/binary"
	"fmt"

	"github.com/gagliardetto/solana-go"
	"github.com/klauspost/reedsolomon"
)

const (
	codingShredsPerFECBlock = 32
	proofEntriesFor32x32    = 6
)

// ShredGenerator builds merkle FEC shreds from a serialized byte buffer.
type ShredGenerator struct {
	Slot          uint64
	ParentSlot    uint64
	Version       uint16
	ReferenceTick uint8
}

func dataCapacity(proofSize uint8, resigned bool) int {
	capacity := dataPayloadSize - dataHeaderSize - merkleRootSize - int(proofSize)*merkleProofEntrySize
	if resigned {
		capacity -= shredSignatureSize
	}
	return capacity
}

func codeCapacity(proofSize uint8, resigned bool) int {
	capacity := codingPayloadSize - codingHeaderSize - merkleRootSize - int(proofSize)*merkleProofEntrySize
	if resigned {
		capacity -= shredSignatureSize
	}
	return capacity
}

func chainedDataVariant(proofSize uint8, resigned bool) byte {
	if resigned {
		return merkleDataResigned | proofSize
	}
	return merkleDataChained | proofSize
}

func chainedCodeVariant(proofSize uint8, resigned bool) byte {
	if resigned {
		return merkleCodeResigned | proofSize
	}
	return merkleCodeChained | proofSize
}

func referenceTickFlags(referenceTick uint8) byte {
	return referenceTick & shredFlagTickMask
}

// MakeShredsFromData serializes data into merkle FEC shreds.
// Returns shred packets, the final chained merkle root, and the next shred/code indices.
func (g *ShredGenerator) MakeShredsFromData(
	leader solana.PrivateKey,
	data []byte,
	isLastInSlot bool,
	chainedMerkleRoot solana.Hash,
	nextShredIndex uint32,
	nextCodeIndex uint32,
) ([][]byte, solana.Hash, uint32, uint32, error) {
	if g.Slot < g.ParentSlot || g.Slot-g.ParentSlot > uint64(^uint16(0)) {
		return nil, solana.Hash{}, nextShredIndex, nextCodeIndex, fmt.Errorf("invalid parent slot %d for slot %d", g.ParentSlot, g.Slot)
	}
	proofSize := uint8(proofEntriesFor32x32)
	unsignedCap := dataCapacity(proofSize, false)
	signedCap := dataCapacity(proofSize, true)
	unsignedBatch := dataShredsPerFECBlock * unsignedCap
	signedBatch := dataShredsPerFECBlock * signedCap

	parentOffset := uint16(g.Slot - g.ParentSlot)
	flags := referenceTickFlags(g.ReferenceTick)

	var unsignedData, signedData []byte
	if isLastInSlot {
		if len(data) > signedBatch {
			split := len(data) - signedBatch
			unsignedData = data[:split]
			signedData = data[split:]
		} else {
			signedData = data
		}
	} else {
		unsignedData = data
	}

	var packets [][]byte
	dataIndex := nextShredIndex
	codeIndex := nextCodeIndex
	chainedRoot := chainedMerkleRoot

	// DATA_COMPLETE ends the serialized component, which may span FEC sets.
	for len(unsignedData) >= unsignedBatch {
		batch := unsignedData[:unsignedBatch]
		unsignedData = unsignedData[unsignedBatch:]
		batchPackets, root, err := g.makeFECBatch(leader, batch, unsignedCap, proofSize, false, parentOffset, flags, len(unsignedData) == 0 && len(signedData) == 0, false, chainedRoot, dataIndex, codeIndex)
		if err != nil {
			return nil, solana.Hash{}, dataIndex, codeIndex, err
		}
		packets = append(packets, batchPackets...)
		chainedRoot = root
		dataIndex += dataShredsPerFECBlock
		codeIndex += codingShredsPerFECBlock
	}

	if len(unsignedData) > 0 || (len(packets) == 0 && !isLastInSlot) {
		batchPackets, root, err := g.makeFECBatch(leader, unsignedData, unsignedCap, proofSize, false, parentOffset, flags, len(signedData) == 0, false, chainedRoot, dataIndex, codeIndex)
		if err != nil {
			return nil, solana.Hash{}, dataIndex, codeIndex, err
		}
		packets = append(packets, batchPackets...)
		chainedRoot = root
		dataIndex += dataShredsPerFECBlock
		codeIndex += codingShredsPerFECBlock
	}

	if len(signedData) > 0 || (len(packets) == 0 && isLastInSlot) {
		batchPackets, root, err := g.makeFECBatch(leader, signedData, signedCap, proofSize, true, parentOffset, flags, true, isLastInSlot, chainedRoot, dataIndex, codeIndex)
		if err != nil {
			return nil, solana.Hash{}, dataIndex, codeIndex, err
		}
		packets = append(packets, batchPackets...)
		chainedRoot = root
		dataIndex += dataShredsPerFECBlock
		codeIndex += codingShredsPerFECBlock
	}

	return packets, chainedRoot, dataIndex, codeIndex, nil
}

func (g *ShredGenerator) makeFECBatch(
	leader solana.PrivateKey,
	data []byte,
	dataCap int,
	proofSize uint8,
	resigned bool,
	parentOffset uint16,
	flags byte,
	dataComplete bool,
	isLastInSlot bool,
	chainedMerkleRoot solana.Hash,
	dataIndex uint32,
	codeIndex uint32,
) ([][]byte, solana.Hash, error) {
	fecSetIndex := dataIndex
	dataVariant := chainedDataVariant(proofSize, resigned)
	codeVariant := chainedCodeVariant(proofSize, resigned)

	dataPackets := make([][]byte, dataShredsPerFECBlock)
	for i := 0; i < dataShredsPerFECBlock; i++ {
		start := i * dataCap
		end := start + dataCap
		var chunk []byte
		if start < len(data) {
			if end > len(data) {
				end = len(data)
			}
			chunk = data[start:end]
		}
		packet := make([]byte, dataPayloadSize)
		packet[shredVariantOffset] = dataVariant
		binary.LittleEndian.PutUint64(packet[shredSlotOffset:], g.Slot)
		binary.LittleEndian.PutUint32(packet[shredIndexOffset:], dataIndex+uint32(i))
		binary.LittleEndian.PutUint16(packet[shredVersionOffset:], g.Version)
		binary.LittleEndian.PutUint32(packet[shredFECSetIndexOffset:], fecSetIndex)
		binary.LittleEndian.PutUint16(packet[dataParentOffsetOffset:], parentOffset)
		packet[dataFlagsOffset] = flags
		size := dataHeaderSize + len(chunk)
		binary.LittleEndian.PutUint16(packet[dataSizeOffset:], uint16(size))
		copy(packet[dataHeaderSize:], chunk)
		dataPackets[i] = packet
	}

	codePackets := make([][]byte, codingShredsPerFECBlock)
	for i := 0; i < codingShredsPerFECBlock; i++ {
		packet := make([]byte, codingPayloadSize)
		packet[shredVariantOffset] = codeVariant
		binary.LittleEndian.PutUint64(packet[shredSlotOffset:], g.Slot)
		binary.LittleEndian.PutUint32(packet[shredIndexOffset:], codeIndex+uint32(i))
		binary.LittleEndian.PutUint16(packet[shredVersionOffset:], g.Version)
		binary.LittleEndian.PutUint32(packet[shredFECSetIndexOffset:], fecSetIndex)
		binary.LittleEndian.PutUint16(packet[codingNumDataOffset:], dataShredsPerFECBlock)
		binary.LittleEndian.PutUint16(packet[codingNumCodingOffset:], codingShredsPerFECBlock)
		binary.LittleEndian.PutUint16(packet[codingPositionOffset:], uint16(i))
		codePackets[i] = packet
	}

	allPackets := append(dataPackets, codePackets...)
	if isLastInSlot {
		for i := len(dataPackets) - 1; i >= 0; i-- {
			dataPackets[i][dataFlagsOffset] |= shredFlagLastShredInSlot
			break
		}
	} else if dataComplete && len(dataPackets) > 0 {
		dataPackets[len(dataPackets)-1][dataFlagsOffset] |= shredFlagDataComplete
	}

	root, err := finishErasureBatch(leader, allPackets, chainedMerkleRoot, proofSize, resigned)
	if err != nil {
		return nil, solana.Hash{}, err
	}
	return allPackets, root, nil
}

func finishErasureBatch(
	leader solana.PrivateKey,
	packets [][]byte,
	chainedMerkleRoot solana.Hash,
	proofSize uint8,
	resigned bool,
) (solana.Hash, error) {
	encoder, err := reedsolomon.New(dataShredsPerFECBlock, codingShredsPerFECBlock)
	if err != nil {
		return solana.Hash{}, err
	}

	shards := make([][]byte, len(packets))
	for i, packet := range packets {
		shred, err := ParseShred(packet)
		if err != nil {
			return solana.Hash{}, fmt.Errorf("parse batch shred %d: %w", i, err)
		}
		shard, err := shred.erasureShard()
		if err != nil {
			return solana.Hash{}, fmt.Errorf("erasure shard %d: %w", i, err)
		}
		shards[i] = shard
	}
	if err := encoder.Encode(shards); err != nil {
		return solana.Hash{}, fmt.Errorf("reed-solomon encode: %w", err)
	}

	for i, packet := range packets {
		shred, err := ParseShred(packet)
		if err != nil {
			return solana.Hash{}, err
		}
		proofSizeInfo, chained, resignedFlag, ok := merkleVariantInfo(shred.Variant)
		if !ok {
			return solana.Hash{}, ErrUnsupportedShred
		}
		_ = proofSizeInfo
		_ = chained
		_ = resignedFlag

		capacity, err := merkleCapacity(len(packet), dataHeaderSize, proofSize, true, resigned)
		if shred.Type == ShredTypeCode {
			capacity, err = merkleCapacity(len(packet), codingHeaderSize, proofSize, true, resigned)
		}
		if err != nil {
			return solana.Hash{}, err
		}
		rootOffset := dataHeaderSize + capacity
		if shred.Type == ShredTypeCode {
			rootOffset = codingHeaderSize + capacity
		}
		copy(packet[rootOffset:rootOffset+merkleRootSize], chainedMerkleRoot[:])

		if shred.Type == ShredTypeCode {
			start := codingHeaderSize
			end := start + capacity
			copy(packet[start:end], shards[i])
		} else {
			start := shredSignatureSize
			end := dataHeaderSize + capacity
			copy(packet[start:end], shards[i])
		}
	}

	nodes, err := buildMerkleTree(packets)
	if err != nil {
		return solana.Hash{}, err
	}
	root := nodes[len(nodes)-1]
	sig := ed25519.Sign(ed25519.PrivateKey(leader), root[:])

	for _, packet := range packets {
		copy(packet[shredSignatureOffset:shredSignatureSize], sig)
		shred, err := ParseShred(packet)
		if err != nil {
			return solana.Hash{}, err
		}
		leafIndex, err := shred.merkleLeafIndex()
		if err != nil {
			return solana.Hash{}, err
		}
		proof := makeMerkleProof(nodes, leafIndex, len(packets))
		capacity, err := merkleCapacity(len(packet), dataHeaderSize, proofSize, true, resigned)
		if shred.Type == ShredTypeCode {
			capacity, err = merkleCapacity(len(packet), codingHeaderSize, proofSize, true, resigned)
		}
		if err != nil {
			return solana.Hash{}, err
		}
		proofOffset := dataHeaderSize + capacity + merkleRootSize
		if shred.Type == ShredTypeCode {
			proofOffset = codingHeaderSize + capacity + merkleRootSize
		}
		for j, entry := range proof {
			copy(packet[proofOffset+j*merkleProofEntrySize:], entry[:])
		}
		if resigned {
			retransmitOffset := proofOffset + int(proofSize)*merkleProofEntrySize
			copy(packet[retransmitOffset:retransmitOffset+shredSignatureSize], sig)
		}
	}
	return root, nil
}

func buildMerkleTree(packets [][]byte) ([]solana.Hash, error) {
	leaves := make([]solana.Hash, len(packets))
	for i, packet := range packets {
		shred, err := ParseShred(packet)
		if err != nil {
			return nil, err
		}
		leaf, err := shred.merkleLeaf()
		if err != nil {
			return nil, err
		}
		leaves[i] = leaf
	}

	nodes := make([]solana.Hash, 0, merkleTreeSize(len(leaves)))
	nodes = append(nodes, leaves...)
	for size := len(leaves); size > 1; size = (size + 1) >> 1 {
		offset := len(nodes) - size
		for index := offset; index < offset+size; index += 2 {
			other := index + 1
			if other >= offset+size {
				other = offset + size - 1
			}
			nodes = append(nodes, merkleHashNode(nodes[index][:merkleProofEntrySize], nodes[other][:merkleProofEntrySize]))
		}
	}
	return nodes, nil
}

func makeMerkleProof(nodes []solana.Hash, index int, size int) [][20]byte {
	if index >= size {
		return nil
	}
	var proof [][20]byte
	offset := 0
	for size > 1 {
		sibling := (index ^ 1)
		if sibling >= size {
			sibling = size - 1
		}
		entry := nodes[offset+sibling]
		var buf [20]byte
		copy(buf[:], entry[:merkleProofEntrySize])
		proof = append(proof, buf)
		offset += size
		size = (size + 1) >> 1
		index >>= 1
	}
	return proof
}

func (s *Shred) merkleLeafIndex() (int, error) {
	if s == nil {
		return 0, ErrUnsupportedShred
	}
	switch s.Type {
	case ShredTypeData:
		if s.Index < s.FECSetIndex {
			return 0, ErrInvalidDataShred
		}
		return int(s.Index - s.FECSetIndex), nil
	case ShredTypeCode:
		return int(s.NumDataShreds) + int(s.Position), nil
	default:
		return 0, ErrUnsupportedShred
	}
}

func (s *Shred) merkleLeaf() (solana.Hash, error) {
	proofSize, chained, resigned, ok := merkleVariantInfo(s.Variant)
	if !ok {
		return solana.Hash{}, ErrUnsupportedShred
	}
	var headerSize, payloadSize int
	switch s.Type {
	case ShredTypeData:
		headerSize = dataHeaderSize
		payloadSize = dataPayloadSize
	case ShredTypeCode:
		headerSize = codingHeaderSize
		payloadSize = codingPayloadSize
	default:
		return solana.Hash{}, ErrUnsupportedShred
	}
	capacity, err := merkleCapacity(payloadSize, headerSize, proofSize, chained, resigned)
	if err != nil {
		return solana.Hash{}, err
	}
	end := headerSize + capacity
	if chained {
		end += merkleRootSize
	}
	if end > len(s.Payload) || shredSignatureSize > end {
		return solana.Hash{}, ErrShortShred
	}
	return merkleHashLeaf(s.Payload[shredSignatureSize:end]), nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
