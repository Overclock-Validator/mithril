package turbine

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	narya "github.com/Overclock-Validator/narya-ed25519/ed25519"
	"github.com/gagliardetto/solana-go"
)

const (
	packetDataSize = 1232

	shredSignatureOffset   = 0
	shredSignatureSize     = 64
	shredVariantOffset     = 64
	shredSlotOffset        = 65
	shredIndexOffset       = 73
	shredVersionOffset     = 77
	shredFECSetIndexOffset = 79

	dataParentOffsetOffset = 83
	dataFlagsOffset        = 85
	dataSizeOffset         = 86
	dataHeaderSize         = 88
	dataPayloadSize        = 1203

	codingNumDataOffset   = 83
	codingNumCodingOffset = 85
	codingPositionOffset  = 87
	codingHeaderSize      = 89
	codingPayloadSize     = packetDataSize - 4

	merkleRootSize       = 32
	merkleProofEntrySize = 20

	merkleHashPrefixLeaf = "\x00SOLANA_MERKLE_SHREDS_LEAF"
	merkleHashPrefixNode = "\x01SOLANA_MERKLE_SHREDS_NODE"

	legacyDataVariant = byte(0b1010_0101)
	legacyCodeVariant = byte(0b0101_1010)

	merkleVariantTypeMask = byte(0xf0)
	merkleCodeVariant     = byte(0x40)
	merkleCodeChained     = byte(0x60)
	merkleCodeResigned    = byte(0x70)
	merkleDataVariant     = byte(0x80)
	merkleDataChained     = byte(0x90)
	merkleDataResigned    = byte(0xb0)

	shredFlagTickMask        = byte(0b0011_1111)
	shredFlagDataComplete    = byte(0b0100_0000)
	shredFlagLastShredInSlot = byte(0b1100_0000)
)

var (
	ErrShortShred         = errors.New("short shred packet")
	ErrUnsupportedShred   = errors.New("unsupported shred variant")
	ErrInvalidDataShred   = errors.New("invalid data shred")
	ErrCodingShredIgnored = errors.New("coding shred ignored")
	ErrInvalidSignature   = errors.New("invalid shred signature")
)

type ShredType uint8

const (
	ShredTypeData ShredType = iota
	ShredTypeCode
)

// TurbineSeedTypeByte returns the legacy shred-type byte hashed into turbine RNG seeds.
func (t ShredType) TurbineSeedTypeByte() byte {
	switch t {
	case ShredTypeData:
		return legacyDataVariant
	case ShredTypeCode:
		return legacyCodeVariant
	default:
		return byte(t)
	}
}

type Shred struct {
	Signature   solana.Signature
	Variant     byte
	Type        ShredType
	Slot        uint64
	Index       uint32
	Version     uint16
	FECSetIndex uint32

	ParentOffset uint16
	Flags        byte
	Data         []byte
	Payload      []byte

	NumDataShreds   uint16
	NumCodingShreds uint16
	Position        uint16
	Recovered       bool
}

func ParseShred(packet []byte) (*Shred, error) {
	if len(packet) <= dataHeaderSize {
		return nil, fmt.Errorf("%w: got %d bytes", ErrShortShred, len(packet))
	}

	variant := packet[shredVariantOffset]
	shredType, err := classifyVariant(variant)
	if err != nil {
		return nil, err
	}
	if shredType == ShredTypeCode {
		return parseCodingShred(packet, variant)
	}
	return parseDataShred(packet, variant)
}

func parseDataShred(packet []byte, variant byte) (*Shred, error) {
	size := int(binary.LittleEndian.Uint16(packet[dataSizeOffset : dataSizeOffset+2]))
	if size < dataHeaderSize || size > len(packet) || size > packetDataSize {
		return nil, fmt.Errorf("%w: data size %d packet size %d", ErrInvalidDataShred, size, len(packet))
	}
	payloadSize := len(packet)
	if isMerkleVariant(variant) && payloadSize >= dataPayloadSize {
		payloadSize = dataPayloadSize
	}
	payload := make([]byte, payloadSize)
	copy(payload, packet[:payloadSize])
	var data []byte
	if size <= payloadSize {
		data = payload[dataHeaderSize:size]
	} else {
		// Preserve parsing of non-canonical packets whose declared data extends
		// into a Merkle packet trailer. Valid Merkle shreds take the zero-copy
		// view above; this cold fallback retains the prior parser behavior.
		data = append([]byte(nil), packet[dataHeaderSize:size]...)
	}

	shred := &Shred{
		Variant:      variant,
		Type:         ShredTypeData,
		Slot:         binary.LittleEndian.Uint64(packet[shredSlotOffset : shredSlotOffset+8]),
		Index:        binary.LittleEndian.Uint32(packet[shredIndexOffset : shredIndexOffset+4]),
		Version:      binary.LittleEndian.Uint16(packet[shredVersionOffset : shredVersionOffset+2]),
		FECSetIndex:  binary.LittleEndian.Uint32(packet[shredFECSetIndexOffset : shredFECSetIndexOffset+4]),
		ParentOffset: binary.LittleEndian.Uint16(packet[dataParentOffsetOffset : dataParentOffsetOffset+2]),
		Flags:        packet[dataFlagsOffset],
		Data:         data,
		Payload:      payload,
	}
	copy(shred.Signature[:], payload[shredSignatureOffset:shredSignatureSize])
	return shred, nil
}

func parseCodingShred(packet []byte, variant byte) (*Shred, error) {
	if !isMerkleVariant(variant) {
		return nil, ErrCodingShredIgnored
	}
	if len(packet) < codingPayloadSize {
		return nil, fmt.Errorf("%w: coding payload size %d", ErrShortShred, len(packet))
	}

	payload := make([]byte, codingPayloadSize)
	copy(payload, packet[:codingPayloadSize])
	shred := &Shred{
		Variant:         variant,
		Type:            ShredTypeCode,
		Slot:            binary.LittleEndian.Uint64(packet[shredSlotOffset : shredSlotOffset+8]),
		Index:           binary.LittleEndian.Uint32(packet[shredIndexOffset : shredIndexOffset+4]),
		Version:         binary.LittleEndian.Uint16(packet[shredVersionOffset : shredVersionOffset+2]),
		FECSetIndex:     binary.LittleEndian.Uint32(packet[shredFECSetIndexOffset : shredFECSetIndexOffset+4]),
		NumDataShreds:   binary.LittleEndian.Uint16(packet[codingNumDataOffset : codingNumDataOffset+2]),
		NumCodingShreds: binary.LittleEndian.Uint16(packet[codingNumCodingOffset : codingNumCodingOffset+2]),
		Position:        binary.LittleEndian.Uint16(packet[codingPositionOffset : codingPositionOffset+2]),
		Payload:         payload,
	}
	copy(shred.Signature[:], payload[shredSignatureOffset:shredSignatureSize])
	return shred, nil
}

func classifyVariant(variant byte) (ShredType, error) {
	switch {
	case variant == legacyDataVariant:
		return ShredTypeData, nil
	case variant == legacyCodeVariant:
		return ShredTypeCode, nil
	case variant&merkleVariantTypeMask == merkleCodeVariant,
		variant&merkleVariantTypeMask == merkleCodeChained,
		variant&merkleVariantTypeMask == merkleCodeResigned:
		return ShredTypeCode, nil
	case variant&merkleVariantTypeMask == merkleDataVariant,
		variant&merkleVariantTypeMask == merkleDataChained,
		variant&merkleVariantTypeMask == merkleDataResigned:
		return ShredTypeData, nil
	default:
		return 0, fmt.Errorf("%w: 0x%02x", ErrUnsupportedShred, variant)
	}
}

func isMerkleVariant(variant byte) bool {
	switch variant & merkleVariantTypeMask {
	case merkleCodeVariant, merkleCodeChained, merkleCodeResigned, merkleDataVariant, merkleDataChained, merkleDataResigned:
		return true
	default:
		return false
	}
}

func merkleVariantInfo(variant byte) (proofSize byte, chained bool, resigned bool, ok bool) {
	switch variant & merkleVariantTypeMask {
	case merkleCodeVariant, merkleDataVariant:
		return variant & 0x0f, false, false, true
	case merkleCodeChained, merkleDataChained:
		return variant & 0x0f, true, false, true
	case merkleCodeResigned, merkleDataResigned:
		return variant & 0x0f, true, true, true
	default:
		return 0, false, false, false
	}
}

func merkleCounterpartVariant(variant byte, shredType ShredType) (byte, bool) {
	proofSize, chained, resigned, ok := merkleVariantInfo(variant)
	if !ok {
		return 0, false
	}
	var prefix byte
	switch shredType {
	case ShredTypeData:
		if resigned {
			prefix = merkleDataResigned
		} else if chained {
			prefix = merkleDataChained
		} else {
			prefix = merkleDataVariant
		}
	case ShredTypeCode:
		if resigned {
			prefix = merkleCodeResigned
		} else if chained {
			prefix = merkleCodeChained
		} else {
			prefix = merkleCodeVariant
		}
	default:
		return 0, false
	}
	return prefix | proofSize, true
}

func merkleCapacity(payloadSize, headerSize int, proofSize byte, chained bool, resigned bool) (int, error) {
	capacity := payloadSize - headerSize - int(proofSize)*merkleProofEntrySize
	if chained {
		capacity -= merkleRootSize
	}
	if resigned {
		capacity -= shredSignatureSize
	}
	if capacity < 0 {
		return 0, fmt.Errorf("%w: proof size %d", ErrUnsupportedShred, proofSize)
	}
	return capacity, nil
}

func (s *Shred) erasureShard() ([]byte, error) {
	if s == nil || !isMerkleVariant(s.Variant) {
		return nil, ErrUnsupportedShred
	}
	proofSize, chained, resigned, ok := merkleVariantInfo(s.Variant)
	if !ok {
		return nil, ErrUnsupportedShred
	}

	var start, headerSize, payloadSize int
	switch s.Type {
	case ShredTypeData:
		start = shredSignatureSize
		headerSize = dataHeaderSize
		payloadSize = dataPayloadSize
	case ShredTypeCode:
		start = codingHeaderSize
		headerSize = codingHeaderSize
		payloadSize = codingPayloadSize
	default:
		return nil, ErrUnsupportedShred
	}
	capacity, err := merkleCapacity(payloadSize, headerSize, proofSize, chained, resigned)
	if err != nil {
		return nil, err
	}
	end := headerSize + capacity
	if end > len(s.Payload) || start > end {
		return nil, fmt.Errorf("%w: erasure slice %d:%d payload %d", ErrShortShred, start, end, len(s.Payload))
	}
	return s.Payload[start:end], nil
}

// EmbeddedChainedMerkleRoot returns the previous FEC set's merkle root embedded
// in a chained merkle shred.
func (s *Shred) EmbeddedChainedMerkleRoot() (solana.Hash, error) {
	if s == nil || !isMerkleVariant(s.Variant) {
		return solana.Hash{}, ErrUnsupportedShred
	}
	proofSize, chained, resigned, ok := merkleVariantInfo(s.Variant)
	if !ok || !chained {
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
	rootOffset := headerSize + capacity
	if rootOffset+merkleRootSize > len(s.Payload) {
		return solana.Hash{}, fmt.Errorf("%w: chained merkle root slice %d:%d payload %d", ErrShortShred, rootOffset, rootOffset+merkleRootSize, len(s.Payload))
	}
	var root solana.Hash
	copy(root[:], s.Payload[rootOffset:rootOffset+merkleRootSize])
	return root, nil
}

func (s *Shred) MerkleRoot() (solana.Hash, error) {
	if s == nil || !isMerkleVariant(s.Variant) {
		return solana.Hash{}, ErrUnsupportedShred
	}
	proofSize, chained, resigned, ok := merkleVariantInfo(s.Variant)
	if !ok {
		return solana.Hash{}, ErrUnsupportedShred
	}

	var headerSize, payloadSize, index int
	switch s.Type {
	case ShredTypeData:
		if s.Index < s.FECSetIndex {
			return solana.Hash{}, fmt.Errorf("%w: data shred index %d before fec_set_index %d", ErrInvalidDataShred, s.Index, s.FECSetIndex)
		}
		headerSize = dataHeaderSize
		payloadSize = dataPayloadSize
		index = int(s.Index - s.FECSetIndex)
	case ShredTypeCode:
		headerSize = codingHeaderSize
		payloadSize = codingPayloadSize
		index = int(s.NumDataShreds) + int(s.Position)
	default:
		return solana.Hash{}, ErrUnsupportedShred
	}

	capacity, err := merkleCapacity(payloadSize, headerSize, proofSize, chained, resigned)
	if err != nil {
		return solana.Hash{}, err
	}
	proofOffset := headerSize + capacity
	if chained {
		proofOffset += merkleRootSize
	}
	proofEnd := proofOffset + int(proofSize)*merkleProofEntrySize
	if proofEnd > len(s.Payload) || shredSignatureSize > proofOffset {
		return solana.Hash{}, fmt.Errorf("%w: merkle proof slice %d:%d payload %d", ErrShortShred, proofOffset, proofEnd, len(s.Payload))
	}

	root := merkleHashLeaf(s.Payload[shredSignatureSize:proofOffset])
	proof := s.Payload[proofOffset:proofEnd]
	for len(proof) > 0 {
		entry := proof[:merkleProofEntrySize]
		if index%2 == 0 {
			root = merkleHashNode(root[:merkleProofEntrySize], entry)
		} else {
			root = merkleHashNode(entry, root[:merkleProofEntrySize])
		}
		index >>= 1
		proof = proof[merkleProofEntrySize:]
	}
	if index != 0 {
		return solana.Hash{}, fmt.Errorf("%w: invalid merkle proof path", ErrInvalidSignature)
	}
	return root, nil
}

// RetransmitterSignature returns the hop signature carried by a resigned
// Merkle shred. It is distinct from Signature, which always belongs to the
// slot leader and authenticates the FEC-set Merkle root.
func (s *Shred) RetransmitterSignature() (solana.Signature, error) {
	var signature solana.Signature
	offset, err := s.retransmitterSignatureOffset()
	if err != nil {
		return signature, err
	}
	if offset+shredSignatureSize > len(s.Payload) {
		return signature, fmt.Errorf("%w: retransmitter signature slice %d:%d payload %d", ErrShortShred, offset, offset+shredSignatureSize, len(s.Payload))
	}
	copy(signature[:], s.Payload[offset:offset+shredSignatureSize])
	return signature, nil
}

func (s *Shred) retransmitterSignatureOffset() (int, error) {
	if s == nil || !isMerkleVariant(s.Variant) {
		return 0, ErrUnsupportedShred
	}
	proofSize, chained, resigned, ok := merkleVariantInfo(s.Variant)
	if !ok || !resigned {
		return 0, ErrUnsupportedShred
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
		return 0, ErrUnsupportedShred
	}
	capacity, err := merkleCapacity(payloadSize, headerSize, proofSize, chained, resigned)
	if err != nil {
		return 0, err
	}
	offset := headerSize + capacity
	if chained {
		offset += merkleRootSize
	}
	offset += int(proofSize) * merkleProofEntrySize
	return offset, nil
}

func (s *Shred) VerifySignature(leader solana.PublicKey) error {
	root, err := s.MerkleRoot()
	if err != nil {
		return err
	}
	if !narya.VerifyStrict(leader[:], root[:], s.Signature[:]) {
		return fmt.Errorf("%w: slot %d shred %d", ErrInvalidSignature, s.Slot, s.Index)
	}
	return nil
}

func merkleHashLeaf(data []byte) solana.Hash {
	return hashv([][]byte{[]byte(merkleHashPrefixLeaf), data})
}

func merkleHashNode(left []byte, right []byte) solana.Hash {
	return hashv([][]byte{[]byte(merkleHashPrefixNode), left, right})
}

func hashv(parts [][]byte) solana.Hash {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write(part)
	}
	var out solana.Hash
	copy(out[:], h.Sum(nil))
	return out
}

func (s *Shred) DataComplete() bool {
	return s.Flags&shredFlagDataComplete != 0
}

func (s *Shred) LastInSlot() bool {
	return s.Flags&shredFlagLastShredInSlot == shredFlagLastShredInSlot
}

func (s *Shred) ReferenceTick() byte {
	return s.Flags & shredFlagTickMask
}

func (s *Shred) ParentSlot() uint64 {
	if uint64(s.ParentOffset) > s.Slot {
		return 0
	}
	return s.Slot - uint64(s.ParentOffset)
}
