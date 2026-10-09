package replay

import (
	"runtime"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

// Preparation owns private, immutable maps. It never publishes a status or
// authorizes a bank: commit still checks coverage, lineage, and duplicates.
type preparedTransactionStatusDelta struct {
	identities *b.PreparedTransactionMessageIdentities
	delta      transactionStatusDelta
}

type transactionStatusPreparation struct {
	done     chan struct{}
	prepared *preparedTransactionStatusDelta
	duration time.Duration
}

// Replay joins this task on every exit, including rejected banks. It only reads
// the immutable identities, so account loading and ALT resolution can proceed.
func (c *TransactionStatusCache) startStatusPreparation(plan blockTransactionExecutionPlan) *transactionStatusPreparation {
	// Small-block measurements show dispatch/join costs as much as the work.
	// With one Go execution thread preparation cannot overlap execution at all.
	if plan.messageIdentities.Len() <= 32 || runtime.GOMAXPROCS(0) == 1 {
		return nil
	}
	p := &transactionStatusPreparation{done: make(chan struct{})}
	go func() {
		defer close(p.done)
		start := time.Now()
		p.prepared = c.prepareTransactionStatusDelta(plan.messageIdentities)
		p.duration = time.Since(start)
	}()
	return p
}

func (p *transactionStatusPreparation) wait() *preparedTransactionStatusDelta {
	if p == nil {
		return nil
	}
	<-p.done
	return p.prepared
}

func countTransactionStatusGroups(identities *b.PreparedTransactionMessageIdentities) map[solana.Hash]int {
	counts := make(map[solana.Hash]int)
	for i := 0; i < identities.Len(); i++ {
		counts[identities.Identity(i).RecentBlockhash]++
	}
	return counts
}

func buildTransactionStatusDelta(identities *b.PreparedTransactionMessageIdentities, counts map[solana.Hash]int, indexes map[solana.Hash]uint8) transactionStatusDelta {
	delta := make(transactionStatusDelta, len(counts))
	for blockhash, count := range counts {
		delta[blockhash] = &transactionStatusGroup{keyIndex: indexes[blockhash], keys: make(map[transactionStatusKey]struct{}, count)}
	}
	for i := 0; i < identities.Len(); i++ {
		identity := identities.Identity(i)
		group := delta[identity.RecentBlockhash]
		group.keys[sliceTransactionStatusKey(identity.MessageHash, group.keyIndex)] = struct{}{}
	}
	return delta
}

func (c *TransactionStatusCache) prepareTransactionStatusDelta(identities *b.PreparedTransactionMessageIdentities) *preparedTransactionStatusDelta {
	counts := countTransactionStatusGroups(identities)
	indexes := make(map[solana.Hash]uint8, len(counts))
	// Copy offsets only, never share mutable visible maps with the worker.
	c.mu.RLock()
	for blockhash := range counts {
		if visible := c.visible[blockhash]; visible != nil {
			indexes[blockhash] = visible.keyIndex
		}
	}
	c.mu.RUnlock()
	return &preparedTransactionStatusDelta{identities: identities, delta: buildTransactionStatusDelta(identities, counts, indexes)}
}
