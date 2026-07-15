package turbine

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	bin "github.com/gagliardetto/binary"
)

const (
	versionedMarkerTagV1 = uint16(1)
	versionedInnerTagV1  = uint8(1)
	// An entry always contains num_hashes, a 32-byte hash, and a transaction
	// count before any transaction bytes.
	minimumEntryWireSize = 8 + 32 + 8
)

// MarshalBlockComponent serializes a component using the Alpenglow wincode layout.
func MarshalBlockComponent(component BlockComponent) ([]byte, error) {
	switch {
	case component.IsEntryBatch():
		return marshalEntryBatch(component.EntryBatch)
	case component.IsMarker():
		return marshalBlockMarker(component.Marker)
	case component.IsEmptyEntryBatchAbort():
		return make([]byte, 8), nil
	default:
		return nil, ErrInvalidBlockComponent
	}
}

// UnmarshalBlockComponent deserializes wincode-encoded block component bytes.
func UnmarshalBlockComponent(data []byte) (BlockComponent, error) {
	if len(data) < 8 {
		return BlockComponent{}, fmt.Errorf("%w: short buffer", ErrInvalidBlockComponent)
	}
	entryCount := binary.LittleEndian.Uint64(data[:8])
	if entryCount == 0 {
		if len(data) == 8 {
			return BlockComponent{}, nil
		}
		marker, err := unmarshalVersionedBlockMarker(data[8:])
		if err != nil {
			return BlockComponent{}, err
		}
		return BlockComponent{Marker: marker}, nil
	}
	entries, err := unmarshalEntryBatch(data)
	if err != nil {
		return BlockComponent{}, err
	}
	return BlockComponent{EntryBatch: entries}, nil
}

func marshalEntryBatch(entries []Entry) ([]byte, error) {
	var buf bytes.Buffer
	enc := bin.NewEncoderWithEncoding(&buf, bin.EncodingBin)
	if err := enc.WriteUint64(uint64(len(entries)), bin.LE); err != nil {
		return nil, err
	}
	for i := range entries {
		if err := marshalEntry(enc, entries[i]); err != nil {
			return nil, fmt.Errorf("marshal entry %d: %w", i, err)
		}
	}
	return buf.Bytes(), nil
}

func unmarshalEntryBatch(data []byte) ([]Entry, error) {
	var dec bin.Decoder
	dec.SetEncoding(bin.EncodingBin)
	dec.Reset(data)
	numEntries, err := dec.ReadUint64(bin.LE)
	if err != nil {
		return nil, err
	}
	if numEntries > uint64(dec.Remaining()/minimumEntryWireSize) {
		return nil, fmt.Errorf("%w: entry count %d exceeds remaining bytes %d", ErrInvalidBlockComponent, numEntries, dec.Remaining())
	}
	entries := make([]Entry, numEntries)
	for i := uint64(0); i < numEntries; i++ {
		if err := entries[i].UnmarshalWithDecoder(&dec); err != nil {
			return nil, fmt.Errorf("read entry %d: %w", i, err)
		}
	}
	if dec.Remaining() != 0 {
		return nil, fmt.Errorf("%w: entry batch has %d trailing bytes", ErrInvalidBlockComponent, dec.Remaining())
	}
	return entries, nil
}

func marshalEntry(enc *bin.Encoder, entry Entry) error {
	if err := enc.WriteUint64(entry.NumHashes, bin.LE); err != nil {
		return err
	}
	if err := enc.WriteBytes(entry.Hash[:], false); err != nil {
		return err
	}
	if err := enc.WriteUint64(uint64(len(entry.Txns)), bin.LE); err != nil {
		return err
	}
	for i := range entry.Txns {
		if err := entry.Txns[i].MarshalWithEncoder(enc); err != nil {
			return err
		}
	}
	return nil
}

func marshalBlockMarker(marker *BlockMarker) ([]byte, error) {
	if marker == nil {
		return nil, ErrInvalidBlockComponent
	}
	inner, err := marshalBlockMarkerV1(marker)
	if err != nil {
		return nil, err
	}
	var out []byte
	out = appendUint64(out, 0)
	out = appendUint16(out, versionedMarkerTagV1)
	out = append(out, inner...)
	return out, nil
}

func marshalBlockMarkerV1(marker *BlockMarker) ([]byte, error) {
	var body []byte
	switch marker.Kind {
	case MarkerBlockFooter:
		if marker.Footer == nil {
			return nil, ErrInvalidBlockComponent
		}
		body, _ = marshalVersionedBlockFooter(*marker.Footer)
		return appendTLV(0, body), nil
	case MarkerBlockHeader:
		if marker.Header == nil {
			return nil, ErrInvalidBlockComponent
		}
		body, _ = marshalVersionedBlockHeader(*marker.Header)
		return appendTLV(1, body), nil
	case MarkerUpdateParent:
		if marker.UpdateParent == nil {
			return nil, ErrInvalidBlockComponent
		}
		body, _ = marshalVersionedUpdateParent(*marker.UpdateParent)
		return appendTLV(2, body), nil
	case MarkerGenesisCertificate:
		if marker.GenesisCert == nil {
			return nil, ErrInvalidBlockComponent
		}
		body, _ = marshalGenesisCert(*marker.GenesisCert)
		return appendTLV(3, body), nil
	default:
		return nil, ErrUnknownMarkerKind
	}
}

func unmarshalVersionedBlockMarker(data []byte) (*BlockMarker, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("%w: short marker version", ErrInvalidBlockComponent)
	}
	tag := binary.LittleEndian.Uint16(data[:2])
	if tag != versionedMarkerTagV1 {
		return nil, fmt.Errorf("%w: unsupported marker version %d", ErrInvalidBlockComponent, tag)
	}
	return unmarshalBlockMarkerV1(data[2:])
}

func unmarshalBlockMarkerV1(data []byte) (*BlockMarker, error) {
	if len(data) < 3 {
		return nil, fmt.Errorf("%w: short marker tlv", ErrInvalidBlockComponent)
	}
	kind := BlockMarkerKind(data[0])
	innerLen := binary.LittleEndian.Uint16(data[1:3])
	if int(innerLen)+3 != len(data) {
		return nil, fmt.Errorf("%w: marker payload length %d does not match remaining %d", ErrInvalidBlockComponent, innerLen, len(data)-3)
	}
	inner := data[3 : 3+innerLen]
	switch kind {
	case MarkerBlockFooter:
		footer, err := unmarshalVersionedBlockFooter(inner)
		if err != nil {
			return nil, err
		}
		return &BlockMarker{Kind: kind, Footer: footer}, nil
	case MarkerBlockHeader:
		header, err := unmarshalVersionedBlockHeader(inner)
		if err != nil {
			return nil, err
		}
		return &BlockMarker{Kind: kind, Header: header}, nil
	case MarkerUpdateParent:
		update, err := unmarshalVersionedUpdateParent(inner)
		if err != nil {
			return nil, err
		}
		return &BlockMarker{Kind: kind, UpdateParent: update}, nil
	case MarkerGenesisCertificate:
		cert, err := unmarshalGenesisCert(inner)
		if err != nil {
			return nil, err
		}
		return &BlockMarker{Kind: kind, GenesisCert: cert}, nil
	default:
		return nil, ErrUnknownMarkerKind
	}
}

func marshalVersionedBlockHeader(header BlockHeader) ([]byte, error) {
	var out []byte
	out = append(out, versionedInnerTagV1)
	out = appendUint64(out, header.ParentSlot)
	out = append(out, header.ParentBlockID[:]...)
	return out, nil
}

func unmarshalVersionedBlockHeader(data []byte) (*BlockHeader, error) {
	if len(data) != 1+8+32 {
		return nil, fmt.Errorf("%w: block header length %d, want %d", ErrInvalidBlockComponent, len(data), 1+8+32)
	}
	if data[0] != versionedInnerTagV1 {
		return nil, fmt.Errorf("%w: unsupported header version", ErrInvalidBlockComponent)
	}
	var header BlockHeader
	header.ParentSlot = binary.LittleEndian.Uint64(data[1:9])
	copy(header.ParentBlockID[:], data[9:41])
	return &header, nil
}

func marshalVersionedUpdateParent(update UpdateParent) ([]byte, error) {
	var out []byte
	out = append(out, versionedInnerTagV1)
	out = appendUint64(out, update.NewParentSlot)
	out = append(out, update.NewParentBlockID[:]...)
	return out, nil
}

func unmarshalVersionedUpdateParent(data []byte) (*UpdateParent, error) {
	if len(data) != 1+8+32 {
		return nil, fmt.Errorf("%w: update parent length %d, want %d", ErrInvalidBlockComponent, len(data), 1+8+32)
	}
	if data[0] != versionedInnerTagV1 {
		return nil, fmt.Errorf("%w: unsupported update parent version", ErrInvalidBlockComponent)
	}
	var update UpdateParent
	update.NewParentSlot = binary.LittleEndian.Uint64(data[1:9])
	copy(update.NewParentBlockID[:], data[9:41])
	return &update, nil
}

func marshalVersionedBlockFooter(footer BlockFooter) ([]byte, error) {
	var out []byte
	out = append(out, versionedInnerTagV1)
	out = append(out, footer.BankHash[:]...)
	out = appendUint64(out, footer.BlockProducerTimeNanos)
	if len(footer.BlockUserAgent) > 255 {
		return nil, fmt.Errorf("%w: user agent too long", ErrInvalidBlockComponent)
	}
	out = append(out, byte(len(footer.BlockUserAgent)))
	out = append(out, footer.BlockUserAgent...)
	out = appendWincodeOption(out, footer.BlockFinalCert)
	out = appendWincodeOption(out, footer.SkipRewardCert)
	out = appendWincodeOption(out, footer.NotarRewardCert)
	return out, nil
}

func unmarshalVersionedBlockFooter(data []byte) (*BlockFooter, error) {
	if len(data) < 1+32+8+1 {
		return nil, fmt.Errorf("%w: short block footer", ErrInvalidBlockComponent)
	}
	if data[0] != versionedInnerTagV1 {
		return nil, fmt.Errorf("%w: unsupported footer version", ErrInvalidBlockComponent)
	}
	pos := 1
	var footer BlockFooter
	copy(footer.BankHash[:], data[pos:pos+32])
	pos += 32
	footer.BlockProducerTimeNanos = binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	agentLen := int(data[pos])
	pos++
	if pos+agentLen > len(data) {
		return nil, fmt.Errorf("%w: footer user agent overflow", ErrInvalidBlockComponent)
	}
	footer.BlockUserAgent = append([]byte(nil), data[pos:pos+agentLen]...)
	pos += agentLen
	var err error
	footer.BlockFinalCert, pos, err = readWincodeOptionBytes(data, pos, wincodeOptionFinalCert)
	if err != nil {
		return nil, err
	}
	footer.SkipRewardCert, pos, err = readWincodeOptionBytes(data, pos, wincodeOptionSkipReward)
	if err != nil {
		return nil, err
	}
	footer.NotarRewardCert, pos, err = readWincodeOptionBytes(data, pos, wincodeOptionNotarReward)
	if err != nil {
		return nil, err
	}
	if pos != len(data) {
		return nil, fmt.Errorf("%w: block footer has %d trailing bytes", ErrInvalidBlockComponent, len(data)-pos)
	}
	return &footer, nil
}

func marshalGenesisCert(cert GenesisCertMarker) ([]byte, error) {
	var out []byte
	out = appendUint64(out, cert.Slot)
	out = append(out, cert.BlockID[:]...)
	out = append(out, cert.BLSSignature[:]...)
	out = appendUint64(out, uint64(len(cert.Bitmap)))
	out = append(out, cert.Bitmap...)
	return out, nil
}

func unmarshalGenesisCert(data []byte) (*GenesisCertMarker, error) {
	if len(data) < 8+32+192+8 {
		return nil, fmt.Errorf("%w: short genesis cert", ErrInvalidBlockComponent)
	}
	var cert GenesisCertMarker
	cert.Slot = binary.LittleEndian.Uint64(data[:8])
	copy(cert.BlockID[:], data[8:40])
	copy(cert.BLSSignature[:], data[40:232])
	bitmapLen := binary.LittleEndian.Uint64(data[232:240])
	if bitmapLen > 512 {
		return nil, fmt.Errorf("%w: genesis cert bitmap length %d exceeds 512", ErrInvalidBlockComponent, bitmapLen)
	}
	if bitmapLen > uint64(len(data)-240) || 240+int(bitmapLen) != len(data) {
		return nil, fmt.Errorf("%w: genesis cert bitmap length %d does not match remaining %d", ErrInvalidBlockComponent, bitmapLen, len(data)-240)
	}
	cert.Bitmap = append([]byte(nil), data[240:240+bitmapLen]...)
	return &cert, nil
}

func appendTLV(tag uint8, body []byte) []byte {
	if len(body) > int(^uint16(0)) {
		panic("marker body too large")
	}
	out := []byte{tag}
	out = appendUint16(out, uint16(len(body)))
	out = append(out, body...)
	return out
}

func appendUint64(out []byte, v uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return append(out, buf[:]...)
}

func appendUint16(out []byte, v uint16) []byte {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	return append(out, buf[:]...)
}

const blsSignatureCompressedSize = 96

func votesAggregateEncodedLen(data []byte) (int, error) {
	if len(data) < blsSignatureCompressedSize+2 {
		return 0, fmt.Errorf("%w: short votes aggregate", ErrInvalidBlockComponent)
	}
	bitmapLen := int(binary.LittleEndian.Uint16(data[blsSignatureCompressedSize : blsSignatureCompressedSize+2]))
	total := blsSignatureCompressedSize + 2 + bitmapLen
	if total > len(data) {
		return 0, fmt.Errorf("%w: votes aggregate overflows buffer", ErrInvalidBlockComponent)
	}
	return total, nil
}

func finalCertificateEncodedLen(data []byte) (int, error) {
	if len(data) < 8+32 {
		return 0, fmt.Errorf("%w: short final certificate", ErrInvalidBlockComponent)
	}
	pos := 8 + 32
	n, err := votesAggregateEncodedLen(data[pos:])
	if err != nil {
		return 0, err
	}
	pos += n
	if pos >= len(data) {
		return 0, fmt.Errorf("%w: truncated final certificate notar option", ErrInvalidBlockComponent)
	}
	switch data[pos] {
	case 0:
		return pos + 1, nil
	case 1:
		pos++
		n, err = votesAggregateEncodedLen(data[pos:])
		if err != nil {
			return 0, err
		}
		return pos + n, nil
	default:
		return 0, fmt.Errorf("%w: invalid final certificate notar option tag %d", ErrInvalidBlockComponent, data[pos])
	}
}

type wincodeOptionKind int

const (
	wincodeOptionGeneric wincodeOptionKind = iota
	wincodeOptionFinalCert
	wincodeOptionSkipReward
	wincodeOptionNotarReward
)

func appendWincodeOption(out []byte, value []byte) []byte {
	if len(value) == 0 {
		return append(out, 0)
	}
	out = append(out, 1)
	return append(out, value...)
}

func readWincodeOptionBytes(data []byte, pos int, kind wincodeOptionKind) ([]byte, int, error) {
	if pos >= len(data) {
		return nil, pos, fmt.Errorf("%w: truncated wincode option", ErrInvalidBlockComponent)
	}
	switch data[pos] {
	case 0:
		return nil, pos + 1, nil
	case 1:
		return readWincodeOptionPayload(data, pos+1, kind)
	default:
		return nil, pos, fmt.Errorf("%w: invalid wincode option tag %d", ErrInvalidBlockComponent, data[pos])
	}
}

func readWincodeOptionPayload(data []byte, pos int, kind wincodeOptionKind) ([]byte, int, error) {
	if pos >= len(data) {
		return nil, pos, fmt.Errorf("%w: truncated wincode option payload", ErrInvalidBlockComponent)
	}
	var (
		n   int
		err error
	)
	switch kind {
	case wincodeOptionSkipReward:
		n, err = rewardcerts.SkipRewardCertificateEncodedLen(data[pos:])
	case wincodeOptionNotarReward:
		n, err = rewardcerts.NotarRewardCertificateEncodedLen(data[pos:])
	case wincodeOptionFinalCert:
		n, err = finalCertificateEncodedLen(data[pos:])
	default:
		n = len(data) - pos
	}
	if err != nil {
		return nil, pos, err
	}
	if n < 0 || pos+n > len(data) {
		return nil, pos, fmt.Errorf("%w: wincode option payload overflow", ErrInvalidBlockComponent)
	}
	end := pos + n
	return append([]byte(nil), data[pos:end]...), end, nil
}

func appendOptionBytes(out []byte, value []byte) []byte {
	if value == nil {
		return append(out, 0)
	}
	out = append(out, 1)
	out = appendUint16(out, uint16(len(value)))
	return append(out, value...)
}

func readOptionBytes(data []byte, pos int) ([]byte, int, error) {
	if pos >= len(data) {
		return nil, pos, fmt.Errorf("%w: truncated option", ErrInvalidBlockComponent)
	}
	if data[pos] == 0 {
		return nil, pos + 1, nil
	}
	if pos+3 > len(data) {
		return nil, pos, fmt.Errorf("%w: truncated option length", ErrInvalidBlockComponent)
	}
	length := int(binary.LittleEndian.Uint16(data[pos+1 : pos+3]))
	pos += 3
	if pos+length > len(data) {
		return nil, pos, fmt.Errorf("%w: option payload overflow", ErrInvalidBlockComponent)
	}
	value := append([]byte(nil), data[pos:pos+length]...)
	return value, pos + length, nil
}

// InferIsEntryBatch reports whether serialized bytes begin with a non-zero entry count.
func InferIsEntryBatch(data []byte) bool {
	return len(data) >= 8 && binary.LittleEndian.Uint64(data[:8]) != 0
}

// InferIsBlockMarker reports whether serialized bytes begin with a zero entry count prefix.
func InferIsBlockMarker(data []byte) bool {
	return len(data) >= 8 && binary.LittleEndian.Uint64(data[:8]) == 0 && len(data) > 8
}
