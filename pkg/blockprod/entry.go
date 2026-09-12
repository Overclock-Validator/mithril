package blockprod

import (
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// entryBatchOverheadBytes is the wincode prefix for a one-entry batch:
// entry count, num_hashes, hash, and transaction count.
const entryBatchOverheadBytes = 8 + 8 + 32 + 8

// EntryBuilder accumulates forged transactions into Alpenglow-style entry batches.
type EntryBuilder struct {
	limits costmodel.Limits

	pendingTxns            []solana.Transaction
	pendingWire            int
	pendingSerializedBytes int
	flushedBytes           int
	reservedBytes          int
	entryHash              solana.Hash
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

// Append adds a forged transaction. When the batch byte budget is exceeded it
// returns the flushed entry batch and resets the pending buffer.
// Appended transactions must remain immutable; batches retain their nested slices.
func (b *EntryBuilder) Append(tx solana.Transaction, wireSize int) ([]turbine.Entry, int, bool) {
	// MarshalWithEncoder writes these transaction bytes without framing. Size
	// each new transaction once rather than reserializing the pending batch.
	// Keep this separate from the caller's wire-size hint, which may differ
	// from the serialized representation used in the emitted component.
	wire, err := tx.MarshalBinary()
	if err != nil {
		return nil, 0, false
	}
	if wireSize <= 0 {
		wireSize = len(wire)
	}

	b.consumeReserved(wireSize)
	if b.wouldOverflowBatch(len(wire)) {
		flushed, batchBytes := b.flushLocked()
		b.pendingTxns = append(b.pendingTxns[:0], tx)
		b.pendingWire = wireSize
		b.pendingSerializedBytes = len(wire)
		return flushed, batchBytes, true
	}

	b.pendingTxns = append(b.pendingTxns, tx)
	b.pendingWire += wireSize
	b.pendingSerializedBytes += len(wire)
	return nil, 0, false
}

func (b *EntryBuilder) projectedBytes(nextWire int) int {
	return entryBatchOverheadBytes + b.pendingSerializedBytes + nextWire
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
	// Append measured canonical transaction sizes; hashing does not change length.
	batchBytes := entryBatchOverheadBytes + b.pendingSerializedBytes
	b.flushedBytes += batchBytes
	b.pendingTxns = b.pendingTxns[:0]
	b.pendingWire = 0
	b.pendingSerializedBytes = 0
	return entries, batchBytes
}
