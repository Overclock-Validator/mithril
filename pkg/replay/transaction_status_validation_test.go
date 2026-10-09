package replay

import (
	"errors"
	"math"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestStatusValidationInvalidation(t *testing.T) {
	for _, action := range []string{"commit", "unwind", "round_trip", "root", "bind", "restore"} {
		t.Run(action, func(t *testing.T) {
			cache := NewTransactionStatusCache()
			if err := cache.CommitBlock(statusCacheTestBlock(10)); err != nil {
				t.Fatal(err)
			}
			blk := statusCacheTestBlock(11, statusCacheTestTransaction(1, 2, 3))
			plan, err := planBlockTransactionExecution(blk)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := cache.validateBlockForPublication(blk, plan)
			if err != nil {
				t.Fatal(err)
			}
			if !receipt.reusableForLocked(cache, plan.messageIdentities) {
				t.Fatal("unchanged receipt not reusable")
			}
			switch action {
			case "commit", "round_trip":
				err = cache.CommitBlock(statusCacheTestBlock(11))
				if err == nil && action == "round_trip" {
					err = cache.Unwind(11)
				}
			case "unwind":
				err = cache.Unwind(10)
			case "root":
				cache.Root(10)
			case "bind":
				err = cache.BindTipBlockID(10, solana.Hash{1})
			case "restore":
				var data []byte
				data, err = cache.SnapshotThrough(10)
				if err == nil {
					cache, err = NewTransactionStatusCacheFromSnapshot(data)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if receipt.reusableForLocked(cache, plan.messageIdentities) {
				t.Fatal("receipt survived " + action)
			}
		})
	}
}

func TestStatusValidationCannotCrossCacheOrIdentity(t *testing.T) {
	good := NewTransactionStatusCache()
	bad := NewTransactionStatusCache()
	tx := statusCacheTestTransaction(1, 2, 3)
	if err := good.CommitBlock(statusCacheTestBlock(10)); err != nil {
		t.Fatal(err)
	}
	if err := bad.CommitBlock(statusCacheTestBlock(10, tx)); err != nil {
		t.Fatal(err)
	}
	blk := statusCacheTestBlock(11, tx)
	plan, err := planBlockTransactionExecution(blk)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := good.validateBlockForPublication(blk, plan)
	if err != nil {
		t.Fatal(err)
	}
	// Both caches have the same version and parent slot, but different contents.
	if good.validationVersion != bad.validationVersion {
		t.Fatal("fixture must have equal versions")
	}
	prepared := bad.prepareTransactionStatusDelta(plan.messageIdentities)
	var already *AncestorAlreadyProcessedTransactionMessagesError
	if err := bad.commitBlockWithValidation(blk, plan, prepared, receipt); !errors.As(err, &already) {
		t.Fatalf("foreign cache: %v", err)
	}

	unique := statusCacheTestBlock(11, statusCacheTestTransaction(4, 5, 6))
	uniquePlan, err := planBlockTransactionExecution(unique)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err = bad.validateBlockForPublication(unique, uniquePlan)
	if err != nil {
		t.Fatal(err)
	}
	if err := bad.commitBlockWithValidation(blk, plan, prepared, receipt); !errors.As(err, &already) {
		t.Fatalf("foreign identities: %v", err)
	}
	failed, err := bad.validateBlockForPublication(blk, plan)
	if err == nil || failed.cache != nil {
		t.Fatalf("failed validation returned a receipt: %+v, %v", failed, err)
	}
}

func TestStatusValidationSaturation(t *testing.T) {
	cache := NewTransactionStatusCache()
	cache.validationVersion = math.MaxUint64 - 1
	blk := statusCacheTestBlock(1)
	plan, err := planBlockTransactionExecution(blk)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := cache.validateBlockForPublication(blk, plan)
	if err != nil {
		t.Fatal(err)
	}
	cache.Root(0)
	cache.Root(0)
	if cache.validationVersion != math.MaxUint64 || receipt.reusableForLocked(cache, plan.messageIdentities) {
		t.Fatal("generation wrapped or old receipt reusable")
	}
	receipt, err = cache.validateBlockForPublication(blk, plan)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.reusableForLocked(cache, plan.messageIdentities) {
		t.Fatal("saturated cache allowed reuse")
	}
	if err := cache.commitBlockWithValidation(blk, plan, nil, receipt); err != nil {
		t.Fatal(err)
	}
}

func TestStatusValidationStillChecksBlockBinding(t *testing.T) {
	cache := NewTransactionStatusCache()
	blk := statusCacheTestBlock(1, statusCacheTestTransaction(1, 2, 3))
	plan, err := planBlockTransactionExecution(blk)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := cache.validateBlockForPublication(blk, plan)
	if err != nil {
		t.Fatal(err)
	}
	prepared := cache.prepareTransactionStatusDelta(plan.messageIdentities)
	blk.Transactions[0] = statusCacheTestTransaction(4, 5, 6)
	if err := cache.commitBlockWithValidation(blk, plan, prepared, receipt); err == nil {
		t.Fatal("replaced transaction accepted")
	}
}
