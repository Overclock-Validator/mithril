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
	identities   *b.PreparedTransactionMessageIdentities
	delta        transactionStatusDelta
	indexBatches map[solana.Hash]*transactionStatusIndexBatch
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
	return buildTransactionStatusDeltaWithBatches(identities, counts, indexes, nil)
}

func buildTransactionStatusDeltaWithBatches(identities *b.PreparedTransactionMessageIdentities, counts map[solana.Hash]int, indexes map[solana.Hash]uint8, batches map[solana.Hash]*transactionStatusIndexBatch) transactionStatusDelta {
	delta := make(transactionStatusDelta, len(counts))
	for blockhash, count := range counts {
		delta[blockhash] = &transactionStatusGroup{keyIndex: indexes[blockhash], keys: make(map[transactionStatusKey]struct{}, count)}
		if batches != nil && count >= 1024 {
			batches[blockhash] = &transactionStatusIndexBatch{keys: make([]transactionStatusKey, 0, count)}
		}
	}
	var previous solana.Hash
	var group *transactionStatusGroup
	var batch *transactionStatusIndexBatch
	for i := 0; i < identities.Len(); i++ {
		identity := identities.Identity(i)
		if group == nil || identity.RecentBlockhash != previous {
			previous = identity.RecentBlockhash
			group = delta[previous]
			batch = batches[previous]
		}
		key := sliceTransactionStatusKey(identity.MessageHash, group.keyIndex)
		before := len(group.keys)
		group.keys[key] = struct{}{}
		if batch != nil && len(group.keys) != before {
			batch.append(key)
		}
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
	batches := make(map[solana.Hash]*transactionStatusIndexBatch, len(counts))
	delta := buildTransactionStatusDeltaWithBatches(identities, counts, indexes, batches)
	for _, batch := range batches {
		batch.partition()
	}
	return &preparedTransactionStatusDelta{identities: identities, delta: delta, indexBatches: batches}
}
