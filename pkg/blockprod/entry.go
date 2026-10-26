package blockprod

import (
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// entryBatchOverheadBytes is the wincode prefix for a one-entry batch:
// entry count, num_hashes, hash, and transaction count.
const entryBatchOverheadBytes = 8 + 8 + 32 + 8

// EntryReservation is slot entry-byte budget held from schedule time, while
// the bank lock is released, until its transactions are forged or it is
// rebated. The zero value holds nothing.
type EntryReservation struct {
	bytes int
}

// Bytes is the entry-byte budget the reservation holds.
func (r EntryReservation) Bytes() int {
	return r.bytes
}

// EntryBuilder accumulates forged transactions into Alpenglow-style entry batches.
type EntryBuilder struct {
	limits costmodel.Limits

	pendingTxns            []solana.Transaction
	pendingSerializedBytes int
	pendingWire            int
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

// SlotBytes is flushed entry bytes plus the current pending entry and all
// outstanding schedule reservations.
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

// wouldExceedSlotN reports whether appending transactions of these
// serialized sizes in order, before the bank lock is released, would pass the
// slot entry-byte budget. It follows the batching appendSerialized does.
func (b *EntryBuilder) wouldExceedSlotN(sizes []int) bool {
	maxEntry := int(b.limits.MaxEntryBytes)
	if maxEntry <= 0 {
		return false
	}
	total := b.SlotBytes()
	pending, pendingBytes := len(b.pendingTxns), b.pendingSerializedBytes
	for _, size := range sizes {
		switch {
		case pending > 0 && entryBatchOverheadBytes+pendingBytes+size > int(b.limits.MaxBatchBytes):
			// The pending batch is already counted; the transaction starts a new one.
			total += entryBatchOverheadBytes + size
			pending, pendingBytes = 1, size
		case pending == 0:
			total += entryBatchOverheadBytes + size
			pending, pendingBytes = 1, size
		default:
			total += size
			pending++
			pendingBytes += size
		}
	}
	return total > maxEntry
}

func (b *EntryBuilder) admitBytes(nextWire int) int {
	if b.wouldOverflowBatch(nextWire) || len(b.pendingTxns) == 0 {
		return entryBatchOverheadBytes + nextWire
	}
	return nextWire
}

// reservationBytes bounds what appending transactions of these sizes can add
// to the slot however they are split into batches, since each may start one.
// Holders interleave, so a reservation cannot rely on the current batch.
func reservationBytes(wireSizes []int) int {
	n := 0
	for _, size := range wireSizes {
		n += entryBatchOverheadBytes + size
	}
	return n
}

func (b *EntryBuilder) reserve(wireSizes []int) (EntryReservation, bool) {
	n := reservationBytes(wireSizes)
	if maxEntry := int(b.limits.MaxEntryBytes); maxEntry > 0 && b.SlotBytes()+n > maxEntry {
		return EntryReservation{}, false
	}
	b.reservedBytes += n
	return EntryReservation{bytes: n}, true
}

// release returns a reservation. Freeze and Close drop every reservation, so a
// holder may release after its bytes are already gone.
func (b *EntryBuilder) release(r EntryReservation) {
	b.reservedBytes -= min(r.bytes, b.reservedBytes)
}

func (b *EntryBuilder) dropReservation() {
	b.reservedBytes = 0
}

// Append adds a forged transaction. The pending entry is held until the next
// transaction would overflow the configured batch target. A short leftover is only emitted by
// Flush (slot end / Freeze). Appended transactions must remain immutable.
func (b *EntryBuilder) Append(tx solana.Transaction, wireSize int) ([]turbine.Entry, int, bool) {
	serializedSize, err := serializedTransactionSize(&tx)
	if err != nil {
		return nil, 0, false
	}
	return b.appendSerialized(tx, wireSize, serializedSize)
}

// serializedTransactionSize validates the complete transaction, not just its
// message. In particular, v1 signature-count errors surface only here.
func serializedTransactionSize(tx *solana.Transaction) (int, error) {
	wire, err := tx.MarshalBinary()
	if err != nil {
		_ = statsd.Count(statsd.BlockProductionEntrySerializationErrors, 1, nil)
		mlog.Log.Errorf("entry builder: cannot serialize transaction: %v", err)
		return 0, err
	}
	return len(wire), nil
}

// appendSerialized cannot fail after bank state is applied: the caller has
// already serialized this exact transaction and must keep it immutable. Keep
// canonical component size separate from the transport reservation-size hint.
func (b *EntryBuilder) appendSerialized(tx solana.Transaction, wireSize, serializedSize int) ([]turbine.Entry, int, bool) {
	if wireSize <= 0 {
		wireSize = serializedSize
	}

	if b.wouldOverflowBatch(serializedSize) {
		flushed, batchBytes := b.flushLocked()
		b.pendingTxns = append(b.pendingTxns[:0], tx)
		b.pendingSerializedBytes = serializedSize
		b.pendingWire = wireSize
		return flushed, batchBytes, true
	}

	b.pendingTxns = append(b.pendingTxns, tx)
	b.pendingSerializedBytes += serializedSize
	b.pendingWire += wireSize
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
	batchBytes := entryBatchOverheadBytes + b.pendingSerializedBytes
	b.flushedBytes += batchBytes
	b.pendingTxns = b.pendingTxns[:0]
	b.pendingWire = 0
	b.pendingSerializedBytes = 0
	return entries, batchBytes
}
