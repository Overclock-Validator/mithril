package turbine

import (
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/gagliardetto/solana-go"
	"github.com/klauspost/reedsolomon"
)

const (
	codingShredsPerFECBlock = 32
	proofEntriesFor32x32    = 6
)

var erasureEncoderPool sync.Pool

func acquireErasureEncoder() (reedsolomon.Encoder, error) {
	if encoder := erasureEncoderPool.Get(); encoder != nil {
		return encoder.(reedsolomon.Encoder), nil
	}
	return reedsolomon.New(dataShredsPerFECBlock, codingShredsPerFECBlock)
}

func releaseErasureEncoder(encoder reedsolomon.Encoder) {
	if encoder != nil {
		erasureEncoderPool.Put(encoder)
	}
}

// ShredGenerator builds merkle FEC shreds from a serialized byte buffer.
type ShredGenerator struct {
	Slot          uint64
	ParentSlot    uint64
	Version       uint16
	ReferenceTick uint8
}

// shredPackets retains the roots already computed during generation, in FEC
// order, so broadcast can commit to them without parsing the packets again.
type shredPackets struct {
	packets           [][]byte
	fecSetRoots       []solana.Hash
	chainedMerkleRoot solana.Hash
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
	batch, nextData, nextCode, err := g.makeShredsFromData(
		leader, data, isLastInSlot, chainedMerkleRoot, nextShredIndex, nextCodeIndex,
	)
	return batch.packets, batch.chainedMerkleRoot, nextData, nextCode, err
}

func (g *ShredGenerator) makeShredsFromData(
	leader solana.PrivateKey,
	data []byte,
	isLastInSlot bool,
	chainedMerkleRoot solana.Hash,
	nextShredIndex uint32,
	nextCodeIndex uint32,
) (shredPackets, uint32, uint32, error) {
	if g.Slot < g.ParentSlot || g.Slot-g.ParentSlot > uint64(^uint16(0)) {
		return shredPackets{}, nextShredIndex, nextCodeIndex, fmt.Errorf("invalid parent slot %d for slot %d", g.ParentSlot, g.Slot)
	}
	// The 32+32 coding matrix is invariant across every FEC set in this
	// operation. Building it requires a Vandermonde inversion, so retain the
	// encoder for the whole payload rather than reconstructing it per set.
	encoder, err := acquireErasureEncoder()
	if err != nil {
		return shredPackets{}, nextShredIndex, nextCodeIndex, err
	}
	defer releaseErasureEncoder(encoder)
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
	var fecSetRoots []solana.Hash
	dataIndex := nextShredIndex
	codeIndex := nextCodeIndex
	chainedRoot := chainedMerkleRoot

	for len(unsignedData) >= unsignedBatch {
		batch := unsignedData[:unsignedBatch]
		unsignedData = unsignedData[unsignedBatch:]
		// DATA_COMPLETE marks the end of the serialized component, not the end
		// of every FEC set. A full unsigned batch is complete only when no
		// unsigned remainder or signed-last batch follows it.
		dataComplete := len(unsignedData) == 0 && len(signedData) == 0
		batchPackets, root, err := g.makeFECBatch(encoder, leader, batch, unsignedCap, proofSize, false, parentOffset, flags, dataComplete, false, chainedRoot, dataIndex, codeIndex)
		if err != nil {
			return shredPackets{}, dataIndex, codeIndex, err
		}
		packets = append(packets, batchPackets...)
		fecSetRoots = append(fecSetRoots, root)
		chainedRoot = root
		dataIndex += dataShredsPerFECBlock
		codeIndex += codingShredsPerFECBlock
	}

	if len(unsignedData) > 0 || (len(packets) == 0 && !isLastInSlot) {
		dataComplete := len(signedData) == 0
		batchPackets, root, err := g.makeFECBatch(encoder, leader, unsignedData, unsignedCap, proofSize, false, parentOffset, flags, dataComplete, false, chainedRoot, dataIndex, codeIndex)
		if err != nil {
			return shredPackets{}, dataIndex, codeIndex, err
		}
		packets = append(packets, batchPackets...)
		fecSetRoots = append(fecSetRoots, root)
		chainedRoot = root
		dataIndex += dataShredsPerFECBlock
		codeIndex += codingShredsPerFECBlock
	}

	if len(signedData) > 0 || (len(packets) == 0 && isLastInSlot) {
		batchPackets, root, err := g.makeFECBatch(encoder, leader, signedData, signedCap, proofSize, true, parentOffset, flags, true, isLastInSlot, chainedRoot, dataIndex, codeIndex)
		if err != nil {
			return shredPackets{}, dataIndex, codeIndex, err
		}
		packets = append(packets, batchPackets...)
		fecSetRoots = append(fecSetRoots, root)
		chainedRoot = root
		dataIndex += dataShredsPerFECBlock
		codeIndex += codingShredsPerFECBlock
	}

	return shredPackets{
		packets: packets, fecSetRoots: fecSetRoots, chainedMerkleRoot: chainedRoot,
	}, dataIndex, codeIndex, nil
}

func (g *ShredGenerator) makeFECBatch(
	encoder reedsolomon.Encoder,
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

	root, err := finishErasureBatch(encoder, leader, allPackets, chainedMerkleRoot, proofSize, resigned)
	if err != nil {
		return nil, solana.Hash{}, err
	}
	return allPackets, root, nil
}

func finishErasureBatch(
	encoder reedsolomon.Encoder,
	leader solana.PrivateKey,
	packets [][]byte,
	chainedMerkleRoot solana.Hash,
	proofSize uint8,
	resigned bool,
) (solana.Hash, error) {
	if len(packets) != dataShredsPerFECBlock+codingShredsPerFECBlock {
		return solana.Hash{}, fmt.Errorf("invalid FEC packet count %d", len(packets))
	}
	dataCap, err := merkleCapacity(dataPayloadSize, dataHeaderSize, proofSize, true, resigned)
	if err != nil {
		return solana.Hash{}, err
	}
	codeCap, err := merkleCapacity(codingPayloadSize, codingHeaderSize, proofSize, true, resigned)
	if err != nil {
		return solana.Hash{}, err
	}
	dataVariant := chainedDataVariant(proofSize, resigned)
	codeVariant := chainedCodeVariant(proofSize, resigned)

	// These packets were constructed immediately above, so retain direct views
	// of their erasure regions. ParseShred is intentionally a defensive,
	// owning parser for untrusted network packets; using it here would allocate
	// and copy every packet several times only to copy the same bytes back.
	shards := make([][]byte, len(packets))
	for i, packet := range packets {
		if i < dataShredsPerFECBlock {
			if len(packet) < dataPayloadSize || packet[shredVariantOffset] != dataVariant {
				return solana.Hash{}, fmt.Errorf("invalid generated data shred %d", i)
			}
			shards[i] = packet[shredSignatureSize : dataHeaderSize+dataCap]
			continue
		}
		if len(packet) < codingPayloadSize || packet[shredVariantOffset] != codeVariant {
			return solana.Hash{}, fmt.Errorf("invalid generated coding shred %d", i-dataShredsPerFECBlock)
		}
		shards[i] = packet[codingHeaderSize : codingHeaderSize+codeCap]
	}
	if err := encoder.Encode(shards); err != nil {
		return solana.Hash{}, fmt.Errorf("reed-solomon encode: %w", err)
	}

	for i, packet := range packets {
		rootOffset := dataHeaderSize + dataCap
		if i >= dataShredsPerFECBlock {
			rootOffset = codingHeaderSize + codeCap
		}
		copy(packet[rootOffset:rootOffset+merkleRootSize], chainedMerkleRoot[:])
	}

	nodes := buildGeneratedMerkleTree(packets, dataCap, codeCap)
	root := nodes[len(nodes)-1]
	sig := ed25519.Sign(ed25519.PrivateKey(leader), root[:])

	for i, packet := range packets {
		copy(packet[shredSignatureOffset:shredSignatureSize], sig)
		proofOffset := dataHeaderSize + dataCap + merkleRootSize
		if i >= dataShredsPerFECBlock {
			proofOffset = codingHeaderSize + codeCap + merkleRootSize
		}
		proofEntries := writeMerkleProof(packet[proofOffset:], nodes, i, len(packets))
		if proofEntries != int(proofSize) {
			return solana.Hash{}, fmt.Errorf("generated merkle proof has %d entries, want %d", proofEntries, proofSize)
		}
		if resigned {
			retransmitOffset := proofOffset + int(proofSize)*merkleProofEntrySize
			copy(packet[retransmitOffset:retransmitOffset+shredSignatureSize], sig)
		}
	}
	return root, nil
}

// buildGeneratedMerkleTree hashes the fixed packet order emitted by
// makeFECBatch: 32 data shreds followed by 32 coding shreds. Callers must have
// already validated the packet sizes and variants in finishErasureBatch.
func buildGeneratedMerkleTree(packets [][]byte, dataCap, codeCap int) []solana.Hash {
	leaves := make([]solana.Hash, len(packets))
	for i, packet := range packets {
		end := dataHeaderSize + dataCap + merkleRootSize
		if i >= dataShredsPerFECBlock {
			end = codingHeaderSize + codeCap + merkleRootSize
		}
		leaves[i] = merkleHashLeaf(packet[shredSignatureSize:end])
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
	return nodes
}

// writeMerkleProof writes the truncated sibling hashes directly into a packet.
// The generated FEC tree has fixed depth, so materializing a temporary proof
// slice for every one of its 64 packets only adds allocator and copy traffic.
func writeMerkleProof(dst []byte, nodes []solana.Hash, index, size int) int {
	entries := 0
	offset := 0
	for size > 1 {
		sibling := index ^ 1
		if sibling >= size {
			sibling = size - 1
		}
		copy(dst[entries*merkleProofEntrySize:], nodes[offset+sibling][:merkleProofEntrySize])
		entries++
		offset += size
		size = (size + 1) >> 1
		index >>= 1
	}
	return entries
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
