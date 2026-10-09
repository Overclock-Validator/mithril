package replay

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

func (c *TransactionStatusCache) legacyCommitStatusForBenchmark(block *b.Block, plan blockTransactionExecutionPlan) error {
	if block == nil || plan.messageIdentities == nil || !plan.messageIdentities.MatchesBlock(block) {
		return errors.New("prepared transaction message identities do not match block")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.coverageComplete {
		return &IncompleteTransactionStatusCoverageError{CachedRoot: c.rootedThrough}
	}
	// Parent lineage and ancestor status are mutable, so both remain under the
	// publication lock even when hashing and same-bank deduplication happened
	// earlier. This keeps commit safe across a concurrent branch transition.
	if err := c.validateParentLocked(block); err != nil {
		return err
	}
	if err := c.validateAncestorTransactionsLocked(block.Slot, plan.messageIdentities); err != nil {
		return err
	}

	delta := make(transactionStatusDelta)
	for index := 0; index < plan.messageIdentities.Len(); index++ {
		identity := plan.messageIdentities.Identity(index)
		blockhash := identity.RecentBlockhash
		group := delta[blockhash]
		if group == nil {
			keyIndex := uint8(0)
			if visible := c.visible[blockhash]; visible != nil {
				keyIndex = visible.keyIndex
			}
			group = &transactionStatusGroup{
				keyIndex: keyIndex,
				keys:     make(map[transactionStatusKey]struct{}),
			}
			delta[blockhash] = group
		}
		group.keys[sliceTransactionStatusKey(identity.MessageHash, group.keyIndex)] = struct{}{}
	}

	if err := c.legacyAddStatusForBenchmark(delta); err != nil {
		return err
	}
	c.tip = &transactionStatusNode{
		slot:       block.Slot,
		blockID:    solana.Hash(block.AlpenglowBlockID),
		hasBlockID: block.HasAlpenglowBlockID,
		parent:     c.tip,
		delta:      delta,
	}
	return nil
}

// Frozen production commit algorithm before publication optimization. This is
// an independent baseline, including its original visible-index allocation.
// Benchmark fixtures never reuse validation receipts on this path. This frozen
// helper deliberately omits validation-version bumps and must not be used by
// production callers or copied as a model for mutating the live cache.
func (c *TransactionStatusCache) legacyAddStatusForBenchmark(delta transactionStatusDelta) error {
	for blockhash, deltaGroup := range delta {
		if group := c.visible[blockhash]; group != nil && group.keyIndex != deltaGroup.keyIndex {
			return fmt.Errorf("transaction status blockhash %s uses inconsistent key indexes %d and %d",
				blockhash, group.keyIndex, deltaGroup.keyIndex)
		}
	}
	for blockhash, deltaGroup := range delta {
		group := c.visible[blockhash]
		if group == nil {
			group = &visibleTransactionStatusGroup{
				keyIndex: deltaGroup.keyIndex,
				keys:     make(map[transactionStatusKey]uint16),
			}
			c.visible[blockhash] = group
		}
		for key := range deltaGroup.keys {
			group.keys[key]++
		}
	}
	return nil
}

// BenchmarkTransactionStatusPublication times only status publication, with
// prepared message identities. No execution, disk I/O, signing or networking.
// prepared_commit excludes delta preparation; prepared_total includes it and
// goroutine dispatch/join, with no execution overlap. Neither measures replay.
// validated_commit also excludes the successful pre-execution ancestor scan.
// invalidated_commit roots between validation and commit, forcing a full recheck.
// Each iteration restores the same ancestor contents; existing maps retain
// steady-state capacity. Fixture creation, seeding and unwind are not timed.
func BenchmarkTransactionStatusPublication(tb *testing.B) {
	const count = 33760
	for _, groups := range []int{1, 4} {
		for _, existing := range []bool{false, true} {
			tb.Run(fmt.Sprintf("groups_%d/existing_%t", groups, existing), func(tb *testing.B) {
				txs := benchmarkUniqueTransactions(count * 2)
				for i, tx := range txs {
					binary.LittleEndian.PutUint32(tx.Message.RecentBlockhash[:], uint32(i%groups+1))
				}
				parent := statusCacheTestBlock(10, txs[:count]...)
				if !existing {
					parent.Transactions = nil
				}
				blk := statusCacheTestBlock(11, txs[count:]...)
				plan, err := planBlockTransactionExecution(blk)
				if err != nil {
					tb.Fatal(err)
				}
				for _, name := range []string{"legacy", "sized", "prepared_total", "prepared_commit", "validated_commit", "invalidated_commit"} {
					tb.Run(name, func(tb *testing.B) {
						cache := NewTransactionStatusCache()
						if err := cache.CommitBlock(parent); err != nil {
							tb.Fatal(err)
						}
						tb.ReportAllocs()
						tb.ResetTimer()
						for range tb.N {
							var err error
							if name == "prepared_commit" || name == "validated_commit" || name == "invalidated_commit" {
								tb.StopTimer()
								prepared := cache.prepareTransactionStatusDelta(plan.messageIdentities)
								validation, validationErr := cache.validateBlockForPublication(blk, plan)
								if validationErr != nil {
									tb.Fatal(validationErr)
								}
								if name == "invalidated_commit" {
									cache.Root(10)
								}
								tb.StartTimer()
								if name == "prepared_commit" {
									err = cache.commitBlockWithPreparedDelta(blk, plan, prepared)
								} else {
									err = cache.commitBlockWithValidation(blk, plan, prepared, validation)
								}
							} else if name == "prepared_total" {
								prepared := cache.startStatusPreparation(plan).wait()
								err = cache.commitBlockWithPreparedDelta(blk, plan, prepared)
							} else if name == "legacy" {
								err = cache.legacyCommitStatusForBenchmark(blk, plan)
							} else {
								err = cache.commitBlockWithPlan(blk, plan)
							}
							if err != nil {
								tb.Fatal(err)
							}
							tb.StopTimer()
							if got := cache.tip.slot; got != 11 {
								tb.Fatalf("tip=%d", got)
							}
							if err := cache.Unwind(11); err != nil {
								tb.Fatal(err)
							}
							tb.StartTimer()
						}
					})
				}
			})
		}
	}
}
