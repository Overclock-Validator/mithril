package blockprod

import (
	"bytes"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// entryBatchOverheadBytes is the wincode prefix for a one-entry batch:
// entry count, num_hashes, hash, and transaction count.
const entryBatchOverheadBytes = 8 + 8 + 32 + 8

// EntryBuilder accumulates forged transactions into Alpenglow-style entry batches.
type EntryBuilder struct {
	limits costmodel.Limits

	pendingTxns   []solana.Transaction
	pendingWire   int
	flushedBytes  int
	reservedBytes int
	entryHash     solana.Hash
}

func NewEntryBuilder(limits costmodel.Limits, entryHash solana.Hash) *EntryBuilder {
	return &EntryBuilder{
		limits:    limits,
		entryHash: entryHash,
	}
}

func (b *EntryBuilder) PendingCount() int {
	return len(b.pendingTxns)
}

func (b *EntryBuilder) PendingWireBytes() int {
	return b.pendingWire
}

func (b *EntryBuilder) FlushedBytes() int {
	return b.flushedBytes
}

func (b *EntryBuilder) ReservedBytes() int {
	return b.reservedBytes
}

// SlotBytes is flushed entry bytes plus the current pending entry and any
// in-flight schedule reservation.
func (b *EntryBuilder) SlotBytes() int {
	pending := 0
	if len(b.pendingTxns) > 0 {
		pending = b.projectedBytes(0)
	}
	return b.flushedBytes + pending + b.reservedBytes
}

func (b *EntryBuilder) wouldOverflowBatch(nextWire int) bool {
	return len(b.pendingTxns) > 0 && b.projectedBytes(nextWire) > int(b.limits.MaxBatchBytes)
}

func (b *EntryBuilder) wouldExceedSlot(nextWire int) bool {
	maxEntry := int(b.limits.MaxEntryBytes)
	if maxEntry <= 0 {
		return false
	}
	return b.SlotBytes()+b.admitBytes(nextWire) > maxEntry
}

func (b *EntryBuilder) admitBytes(nextWire int) int {
	if b.wouldOverflowBatch(nextWire) || len(b.pendingTxns) == 0 {
		return entryBatchOverheadBytes + nextWire
	}
	return nextWire
}

func (b *EntryBuilder) reserve(wireSize int) bool {
	if b.wouldExceedSlot(wireSize) {
		return false
	}
	b.reservedBytes += b.admitBytes(wireSize)
	return true
}

func (b *EntryBuilder) rebateReserved(wireSize int) {
	b.consumeReserved(wireSize)
}

func (b *EntryBuilder) consumeReserved(wireSize int) {
	n := b.admitBytes(wireSize)
	if b.reservedBytes < n {
		b.reservedBytes = 0
		return
	}
	b.reservedBytes -= n
}

func (b *EntryBuilder) dropReservation() {
	b.reservedBytes = 0
}

// Append adds a forged transaction. The pending entry is held until the next
// transaction would overflow one FEC set. A short leftover is only emitted by
// Flush (slot end / Freeze).
func (b *EntryBuilder) Append(tx solana.Transaction, wireSize int) ([]turbine.Entry, int, bool) {
	if wireSize <= 0 {
		wire, err := tx.MarshalBinary()
		if err != nil {
			return nil, 0, false
		}
		wireSize = len(wire)
	}

	b.consumeReserved(wireSize)
	if b.wouldOverflowBatch(wireSize) {
		flushed, batchBytes := b.flushLocked()
		b.pendingTxns = append(b.pendingTxns[:0], tx)
		b.pendingWire = wireSize
		return flushed, batchBytes, true
	}

	b.pendingTxns = append(b.pendingTxns, tx)
	b.pendingWire += wireSize
	return nil, 0, false
}

func (b *EntryBuilder) projectedBytes(nextWire int) int {
	return entryBatchOverheadBytes + b.pendingWire + nextWire
}

// Flush emits the current pending transactions as a single PoH entry.
func (b *EntryBuilder) Flush() ([]turbine.Entry, int) {
	if len(b.pendingTxns) == 0 {
		return nil, 0
	}
	return b.flushLocked()
}

func (b *EntryBuilder) CurrentEntryHash() solana.Hash {
	return b.entryHash
}

func (b *EntryBuilder) flushLocked() ([]turbine.Entry, int) {
	txns := append([]solana.Transaction(nil), b.pendingTxns...)
	entryHash := turbine.NextAlpenglowEntryHash(b.entryHash, 1, txns)
	entries := []turbine.Entry{{
		NumHashes: 1,
		Hash:      entryHash,
		Txns:      txns,
	}}
	b.entryHash = entryHash
	batchBytes, err := marshalEntryBatchBytes(entries)
	if err != nil {
		return nil, 0
	}
	b.flushedBytes += len(batchBytes)
	b.pendingTxns = b.pendingTxns[:0]
	b.pendingWire = 0
	return entries, len(batchBytes)
}

func marshalEntryBatchBytes(entries []turbine.Entry) ([]byte, error) {
	var buf bytes.Buffer
	enc := bin.NewEncoderWithEncoding(&buf, bin.EncodingBin)
	if err := enc.WriteUint64(uint64(len(entries)), bin.LE); err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if err := enc.WriteUint64(entry.NumHashes, bin.LE); err != nil {
			return nil, err
		}
		if err := enc.WriteBytes(entry.Hash[:], false); err != nil {
			return nil, err
		}
		if err := enc.WriteUint64(uint64(len(entry.Txns)), bin.LE); err != nil {
			return nil, err
		}
		for i := range entry.Txns {
			if err := entry.Txns[i].MarshalWithEncoder(enc); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}
