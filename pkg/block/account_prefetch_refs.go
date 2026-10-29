package block

import "github.com/gagliardetto/solana-go"

// AccountPrefetchRefs copies the accounts a decoded transaction batch names:
// every static account key and top-level program (in order, not deduplicated:
// it is built on the receive path, so it only copies), and its address-table
// lookups. The decode worker builds it before the batch is published, from
// transactions nothing else can touch yet; it never changes afterwards and
// consumers must not modify it. Replay later resolves address tables into the
// batch's own messages, so readers outside the stream must use this copy,
// never the transactions.
type AccountPrefetchRefs struct {
	Keys     []solana.PublicKey
	Programs []solana.PublicKey
	Tables   []solana.PublicKey
	Lookups  []AccountPrefetchLookup
}

// AccountPrefetchLookup is one address-table lookup: an index into Tables and the
// selected entries (writable then read-only).
type AccountPrefetchLookup struct {
	Table   int
	Indexes []uint8
}

// SetAccountPrefetchRefs attaches an immutable summary before this block is
// published. The caller must own the block and must not mutate refs afterward.
// Its lifetime follows the block, including time spent outside the prefetch
// window. It is deliberately not serialized or reconstructed from RPC data.
func (b *Block) SetAccountPrefetchRefs(refs *AccountPrefetchRefs) {
	b.accountPrefetchRefs = refs
}

// AccountPrefetchRefs returns the completion summary. Callers must not mutate it.
func (b *Block) AccountPrefetchRefs() *AccountPrefetchRefs {
	return b.accountPrefetchRefs
}
