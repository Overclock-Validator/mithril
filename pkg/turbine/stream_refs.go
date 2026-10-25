package turbine

import (
	"sync/atomic"

	"github.com/gagliardetto/solana-go"
)

// StreamAccountRefs copies the accounts a decoded transaction batch names:
// every static account key and top-level program (in order, not deduplicated:
// it is built on the receive path, so it only copies), and its address-table
// lookups. The decode worker builds it before the batch is published, from
// transactions nothing else can touch yet; it never changes afterwards and
// consumers must not modify it. Replay later resolves address tables into the
// batch's own messages, so readers outside the stream must use this copy,
// never the transactions.
type StreamAccountRefs struct {
	Keys     []solana.PublicKey
	Programs []solana.PublicKey
	Tables   []solana.PublicKey
	Lookups  []StreamLookup
}

// StreamLookup is one address-table lookup: an index into Tables and the
// selected entries (writable then read-only).
type StreamLookup struct {
	Table   int
	Indexes []uint8
}

var streamRefsHook atomic.Pointer[func(slot uint64, refs *StreamAccountRefs)]

// SetStreamRefsHook installs fn to receive every decoded transaction batch's
// account references, for any slot, on the decode worker just before the batch
// is published (nil removes it), so a consumer has seen every batch of a block
// before the block can be emitted. fn must not block. While no hook is set,
// batches are not summarized.
func SetStreamRefsHook(fn func(slot uint64, refs *StreamAccountRefs)) {
	if fn == nil {
		streamRefsHook.Store(nil)
		return
	}
	streamRefsHook.Store(&fn)
}

// SummarizeAccountRefs copies the account references of txs (see
// StreamAccountRefs). Callers must own txs while it runs.
func SummarizeAccountRefs(txs []*solana.Transaction) *StreamAccountRefs {
	n := 0
	for _, tx := range txs {
		if tx != nil {
			n += len(tx.Message.AccountKeys)
		}
	}
	refs := &StreamAccountRefs{Keys: make([]solana.PublicKey, 0, n)}
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		keys := tx.Message.AccountKeys
		refs.Keys = append(refs.Keys, keys...)
		for _, ix := range tx.Message.Instructions {
			if int(ix.ProgramIDIndex) < len(keys) {
				refs.Programs = append(refs.Programs, keys[ix.ProgramIDIndex])
			}
		}
		if tx.Message.GetVersion() != solana.MessageVersionV0 {
			continue
		}
		for i := range tx.Message.AddressTableLookups {
			lookup := &tx.Message.AddressTableLookups[i]
			indexes := make([]uint8, 0, len(lookup.WritableIndexes)+len(lookup.ReadonlyIndexes))
			indexes = append(indexes, lookup.WritableIndexes...)
			indexes = append(indexes, lookup.ReadonlyIndexes...)
			refs.Lookups = append(refs.Lookups, StreamLookup{Table: len(refs.Tables), Indexes: indexes})
			refs.Tables = append(refs.Tables, lookup.AccountKey)
		}
	}
	return refs
}
