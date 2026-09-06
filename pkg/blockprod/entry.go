package blockprod

import (
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// EntryBuilder emits one entry per batch: an entry count, num_hashes, a
// 32-byte hash, and a transaction count precede the serialized transactions.
const singleEntryBatchHeaderBytes = 8 + 8 + 32 + 8

// EntryBuilder accumulates forged transactions into Alpenglow-style entry batches.
type EntryBuilder struct {
	limits costmodel.Limits

	pendingTxns            []solana.Transaction
	pendingWire            int
	pendingSerializedBytes int
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

	nextBytes := singleEntryBatchHeaderBytes + b.pendingSerializedBytes + len(wire)
	if len(b.pendingTxns) > 0 && nextBytes > int(b.limits.MaxBatchBytes) {
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
	// Append already measured each transaction's canonical encoding. The
	// entry hash changes the bytes, but not the fixed-size entry header.
	batchBytes := singleEntryBatchHeaderBytes + b.pendingSerializedBytes
	b.pendingTxns = b.pendingTxns[:0]
	b.pendingWire = 0
	b.pendingSerializedBytes = 0
	return entries, batchBytes
}
