// Package transactionhistory stores bounded, fork-qualified transaction records.
// A completed write is a storage acknowledgement, not proof of canonicality or
// metadata fidelity. Callers must validate the capture contract and chain view.
package transactionhistory

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/tpu/wire"
)

// CaptureVersion identifies the only capture contract accepted by this store.
const CaptureVersion uint32 = 1

const (
	formatVersion    = uint32(1)
	completionSize   = 196
	recordHeaderSize = 72
)

// BlockID identifies a block within the store's immutable genesis namespace.
type BlockID struct {
	Slot      uint64
	Blockhash [32]byte
}

// Block contains ordered records from a validated completed block boundary.
type Block struct {
	ID                BlockID
	ParentSlot        uint64
	PreviousBlockhash [32]byte
	Bankhash          [32]byte
	ParentBankhash    [32]byte
	CaptureVersion    uint32
	Records           []Record
}

// Record holds an owned wire transaction and opaque versioned metadata bytes.
// The adapter, not this storage layer, validates metadata semantics.
type Record struct {
	FirstSignature [64]byte
	Transaction    []byte
	Metadata       []byte
}

// Ref identifies one transaction position without choosing a canonical fork.
type Ref struct {
	Block          BlockID
	Position       uint32
	FirstSignature [64]byte
}

// Limits bounds application copies and batches, not engine or process memory.
// Every field must be positive and no greater than DefaultLimits.
type Limits struct {
	MaxTransactions  uint32
	MaxRecordBytes   uint64
	MaxBlockBytes    uint64
	MaxBatchBytes    uint64
	MaxSignatureRefs uint32
}

// DefaultLimits returns per-operation ceilings, not whole-process resource limits.
func DefaultLimits() Limits {
	return Limits{MaxTransactions: 16384, MaxRecordBytes: 1 << 20, MaxBlockBytes: 64 << 20, MaxBatchBytes: 80 << 20, MaxSignatureRefs: 128}
}

func (l Limits) validate() error {
	d := DefaultLimits()
	if l.MaxTransactions == 0 || l.MaxTransactions > d.MaxTransactions ||
		l.MaxRecordBytes <= recordHeaderSize || l.MaxRecordBytes > d.MaxRecordBytes ||
		l.MaxBlockBytes == 0 || l.MaxBlockBytes > d.MaxBlockBytes ||
		l.MaxBatchBytes == 0 || l.MaxBatchBytes > d.MaxBatchBytes ||
		l.MaxSignatureRefs == 0 || l.MaxSignatureRefs > d.MaxSignatureRefs {
		return errors.New("invalid transaction history limits")
	}
	return nil
}

type completion struct {
	block  Block
	count  uint32
	bytes  uint64
	digest [32]byte
}

func blockKey(kind byte, id BlockID) []byte {
	key := make([]byte, 41)
	key[0] = kind
	binary.BigEndian.PutUint64(key[1:9], id.Slot)
	copy(key[9:], id.Blockhash[:])
	return key
}

func transactionKey(id BlockID, position uint32) []byte {
	return binary.BigEndian.AppendUint32(blockKey('t', id), position)
}

func signatureKey(ref Ref) []byte {
	key := make([]byte, 1, 109)
	key[0] = 's'
	key = append(key, ref.FirstSignature[:]...)
	key = binary.BigEndian.AppendUint64(key, ref.Block.Slot)
	key = append(key, ref.Block.Blockhash[:]...)
	return binary.BigEndian.AppendUint32(key, ref.Position)
}

func prefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 255 {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func encodeCompletion(c completion) []byte {
	v := make([]byte, 0, completionSize)
	v = binary.BigEndian.AppendUint32(v, formatVersion)
	v = binary.BigEndian.AppendUint64(v, c.block.ID.Slot)
	v = append(v, c.block.ID.Blockhash[:]...)
	v = binary.BigEndian.AppendUint64(v, c.block.ParentSlot)
	v = append(v, c.block.PreviousBlockhash[:]...)
	v = append(v, c.block.Bankhash[:]...)
	v = append(v, c.block.ParentBankhash[:]...)
	v = binary.BigEndian.AppendUint32(v, c.block.CaptureVersion)
	v = binary.BigEndian.AppendUint32(v, c.count)
	v = binary.BigEndian.AppendUint64(v, c.bytes)
	return append(v, c.digest[:]...)
}

func decodeCompletion(v []byte, id BlockID, limits Limits) (completion, error) {
	var c completion
	if len(v) != completionSize || binary.BigEndian.Uint32(v[:4]) != formatVersion {
		return c, ErrUnavailable
	}
	c.block.ID.Slot = binary.BigEndian.Uint64(v[4:12])
	copy(c.block.ID.Blockhash[:], v[12:44])
	c.block.ParentSlot = binary.BigEndian.Uint64(v[44:52])
	copy(c.block.PreviousBlockhash[:], v[52:84])
	copy(c.block.Bankhash[:], v[84:116])
	copy(c.block.ParentBankhash[:], v[116:148])
	c.block.CaptureVersion = binary.BigEndian.Uint32(v[148:152])
	c.count = binary.BigEndian.Uint32(v[152:156])
	c.bytes = binary.BigEndian.Uint64(v[156:164])
	copy(c.digest[:], v[164:196])
	if c.block.ID != id || c.block.CaptureVersion != CaptureVersion || c.count > limits.MaxTransactions || c.bytes > limits.MaxBlockBytes || (c.count == 0) != (c.bytes == 0) {
		return completion{}, ErrUnavailable
	}
	return c, nil
}

func recordSize(r Record) uint64 {
	return recordHeaderSize + uint64(len(r.Transaction)) + uint64(len(r.Metadata))
}

func validateRecord(r Record, limits Limits) error {
	if recordSize(r) > limits.MaxRecordBytes || len(r.Metadata) == 0 {
		return ErrLimit
	}
	view, err := wire.Sanitize(r.Transaction)
	if err != nil {
		return fmt.Errorf("decode history transaction: %w", err)
	}
	if !bytes.Equal(view.FirstSignature(), r.FirstSignature[:]) {
		return errors.New("history transaction signature or wire length mismatch")
	}
	return nil
}

func encodeRecord(r Record) []byte {
	v := make([]byte, 0, int(recordSize(r)))
	v = append(v, r.FirstSignature[:]...)
	v = binary.BigEndian.AppendUint32(v, uint32(len(r.Transaction)))
	v = binary.BigEndian.AppendUint32(v, uint32(len(r.Metadata)))
	v = append(v, r.Transaction...)
	return append(v, r.Metadata...)
}

// viewRecord borrows engine bytes; callers must validate before copying them.
func viewRecord(v []byte, limits Limits) (Record, error) {
	var r Record
	if len(v) < recordHeaderSize || uint64(len(v)) > limits.MaxRecordBytes {
		return r, ErrUnavailable
	}
	wire := uint64(binary.BigEndian.Uint32(v[64:68]))
	meta := uint64(binary.BigEndian.Uint32(v[68:72]))
	if wire+meta != uint64(len(v)-recordHeaderSize) {
		return r, ErrUnavailable
	}
	copy(r.FirstSignature[:], v[:64])
	r.Transaction = v[recordHeaderSize : recordHeaderSize+wire]
	r.Metadata = v[recordHeaderSize+wire:]
	if err := validateRecord(r, limits); err != nil {
		return Record{}, errors.Join(ErrUnavailable, err)
	}
	return r, nil
}

func ownedRecord(r Record) Record {
	r.Transaction = bytes.Clone(r.Transaction)
	r.Metadata = bytes.Clone(r.Metadata)
	return r
}

func setSize(key, value uint64) uint64 {
	var buffer [10]byte
	return 1 + uint64(binary.PutUvarint(buffer[:], key)) + key + uint64(binary.PutUvarint(buffer[:], value)) + value
}

func deleteSize(key uint64) uint64 {
	var buffer [10]byte
	return 1 + uint64(binary.PutUvarint(buffer[:], key)) + key
}

func prepareBlock(block Block, limits Limits) (completion, error) {
	c := completion{block: block, count: uint32(len(block.Records))}
	c.block.Records = nil
	if len(block.Records) > int(limits.MaxTransactions) || block.CaptureVersion != CaptureVersion {
		return c, ErrLimit
	}
	batchBytes := uint64(12) + setSize(41, completionSize)
	for _, record := range block.Records {
		size := recordSize(record)
		if size > limits.MaxRecordBytes || size > limits.MaxBlockBytes-c.bytes {
			return c, ErrLimit
		}
		c.bytes += size
		batchBytes += setSize(45, size) + setSize(109, 2*sha256.Size)
		if batchBytes > limits.MaxBatchBytes {
			return c, ErrLimit
		}
	}
	if batchBytes > limits.MaxBatchBytes {
		return c, ErrLimit
	}
	h := sha256.New()
	h.Write(encodeCompletion(c)[:164])
	for _, record := range block.Records {
		if err := validateRecord(record, limits); err != nil {
			return c, err
		}
		var sizes [8]byte
		binary.BigEndian.PutUint32(sizes[:4], uint32(len(record.Transaction)))
		binary.BigEndian.PutUint32(sizes[4:], uint32(len(record.Metadata)))
		h.Write(record.FirstSignature[:])
		h.Write(sizes[:])
		h.Write(record.Transaction)
		h.Write(record.Metadata)
	}
	copy(c.digest[:], h.Sum(nil))
	return c, nil
}
