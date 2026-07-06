package rpcclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	//"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

func (fetcher *RpcClient) GetBlock(slot uint64) (*rpc.GetBlockResult, error) {
	return fetcher.client.GetBlock(context.TODO(), slot)
}

func (fetcher *RpcClient) GetBlockConfirmed(slot uint64) (*rpc.GetBlockResult, error) {
	includeRewards := true
	maxSupportedTxVer := uint64(0)

	var result *rpc.GetBlockResult
	var err error

	for count := 0; count < 10; count++ {
		result, err = fetcher.client.GetBlockWithOpts(
			context.TODO(),
			slot,
			&rpc.GetBlockOpts{
				MaxSupportedTransactionVersion: &maxSupportedTxVer,
				Commitment:                     rpc.CommitmentConfirmed,
				TransactionDetails:             rpc.TransactionDetailsFull,
				Rewards:                        &includeRewards,
			},
		)

		if err == nil {
			break
		} else {
			if strings.Contains(err.Error(), fmt.Sprintf("Slot %d was skipped", slot)) {
				return nil, SlotSkipped
			} else {
				//mlog.Log.Debugf("fetch block %d failed - retrying", slot)
			}
		}
	}

	return result, err
}

var SlotSkipped = errors.New("slot skipped")

// GetBlockFinalizedOnce fetches a block at FINALIZED commitment with a single
// attempt and a hard timeout. Used by the trailing verifier, which does its
// own scheduling/backoff and must never block the caller for long.
func (fetcher *RpcClient) GetBlockFinalizedOnce(slot uint64) (*rpc.GetBlockResult, error) {
	includeRewards := false
	maxSupportedTxVer := uint64(0)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := fetcher.client.GetBlockWithOpts(
		ctx,
		slot,
		&rpc.GetBlockOpts{
			MaxSupportedTransactionVersion: &maxSupportedTxVer,
			Commitment:                     rpc.CommitmentFinalized,
			TransactionDetails:             rpc.TransactionDetailsFull,
			Rewards:                        &includeRewards,
		},
	)
	if err != nil {
		if strings.Contains(err.Error(), fmt.Sprintf("Slot %d was skipped", slot)) {
			return nil, SlotSkipped
		}
		return nil, err
	}
	return result, nil
}

// GetBlockConfirmedOnce fetches a block with a single RPC attempt (no internal retry).
// Use this with rate-limited parallel fetching where the scheduler handles retries.
// Uses a 30-second timeout to prevent worker stalls on hung RPC connections.
func (fetcher *RpcClient) GetBlockConfirmedOnce(slot uint64) (*rpc.GetBlockResult, error) {
	includeRewards := true
	maxSupportedTxVer := uint64(0)

	// Use a timeout to prevent indefinite hangs on slow/stuck RPC connections
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := fetcher.client.GetBlockWithOpts(
		ctx,
		slot,
		&rpc.GetBlockOpts{
			MaxSupportedTransactionVersion: &maxSupportedTxVer,
			Commitment:                     rpc.CommitmentConfirmed,
			TransactionDetails:             rpc.TransactionDetailsFull,
			Rewards:                        &includeRewards,
		},
	)

	if err != nil {
		if strings.Contains(err.Error(), fmt.Sprintf("Slot %d was skipped", slot)) {
			return nil, SlotSkipped
		}
		return nil, err
	}

	return result, nil
}

// GetBlocksWithLimitConfirmed fetches a confirmed slot listing beginning at startSlot.
// This is useful for confirming that a slot is genuinely absent before treating it as skipped.
func (fetcher *RpcClient) GetBlocksWithLimitConfirmed(startSlot uint64, limit uint64) ([]uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := fetcher.client.GetBlocksWithLimit(
		ctx,
		startSlot,
		limit,
		rpc.CommitmentConfirmed,
	)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	return *result, nil
}

func (fetcher *RpcClient) GetBlockFinalized(slot uint64) (*rpc.GetBlockResult, error) {
	includeRewards := true
	maxSupportedTxVer := uint64(0)

	var result *rpc.GetBlockResult
	var err error

	for count := 0; count < 10; count++ {
		result, err = fetcher.client.GetBlockWithOpts(
			context.TODO(),
			slot,
			&rpc.GetBlockOpts{
				MaxSupportedTransactionVersion: &maxSupportedTxVer,
				Commitment:                     rpc.CommitmentFinalized,
				TransactionDetails:             rpc.TransactionDetailsFull,
				Rewards:                        &includeRewards,
			},
		)

		if err == nil {
			break
		} else {
			if strings.Contains(err.Error(), fmt.Sprintf("Slot %d was skipped", slot)) {
				return nil, SlotSkipped
			} else {
				//mlog.Log.Debugf("fetch block %d failed - retrying", slot)
			}
		}
	}

	return result, err
}

func (fetcher *RpcClient) GetRewardsForSlot(slot uint64) ([]rpc.BlockReward, error) {
	return fetcher.GetRewardsForSlotWithCommitment(slot, rpc.CommitmentFinalized, 0)
}

func (fetcher *RpcClient) GetRewardsForSlotWithCommitment(slot uint64, commitment rpc.CommitmentType, timeout time.Duration) ([]rpc.BlockReward, error) {
	includeRewards := true
	maxSupportedTxVer := uint64(0)

	ctx := context.TODO()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
	}

	result, err := fetcher.client.GetBlockWithOpts(
		ctx,
		slot,
		&rpc.GetBlockOpts{
			MaxSupportedTransactionVersion: &maxSupportedTxVer,
			Commitment:                     commitment,
			TransactionDetails:             rpc.TransactionDetailsNone,
			Rewards:                        &includeRewards,
		},
	)
	if err != nil {
		return nil, err
	}

	return result.Rewards, err
}

func (fetcher *RpcClient) GetNumRewardPartitions(slot uint64) (uint64, error) {
	includeRewards := true
	maxSupportedTxVer := uint64(0)

	var lastErr error

	for attempt := uint64(0); attempt < 20; attempt++ {
		result, err := fetcher.client.GetBlockWithOpts(
			context.TODO(),
			slot+attempt,
			&rpc.GetBlockOpts{
				MaxSupportedTransactionVersion: &maxSupportedTxVer,
				Commitment:                     rpc.CommitmentFinalized,
				TransactionDetails:             rpc.TransactionDetailsNone,
				Rewards:                        &includeRewards,
			},
		)

		if err != nil {
			if strings.Contains(err.Error(), fmt.Sprintf("Slot %d was skipped", slot+attempt)) {
				// Slot was skipped, try next slot
				continue
			}
			// Other error - wait and retry
			lastErr = err
			if attempt < 19 {
				waitTime := time.Duration(1<<attempt) * time.Second // 1s, 2s, 4s, 8s...
				if waitTime > 30*time.Second {
					waitTime = 30 * time.Second
				}
				mlog.Log.Infof("might be too early for slot %d, retrying in %v (attempt %d/20): %v", slot, waitTime, attempt+1, err)
				time.Sleep(waitTime)
			}
			continue
		}

		// Got a block - check if numRewardPartitions field is present
		if result.NumRewardPartitions == nil {
			// Some RPC nodes return blocks without this field - treat as retryable
			lastErr = fmt.Errorf("block %d missing numRewardPartitions field", slot+attempt)
			if attempt < 19 {
				waitTime := time.Duration(1<<attempt) * time.Second
				if waitTime > 30*time.Second {
					waitTime = 30 * time.Second
				}
				mlog.Log.Infof("block %d missing numRewardPartitions field (RPC may be lagging), retrying in %v (attempt %d/20)",
					slot+attempt, waitTime, attempt+1)
				time.Sleep(waitTime)
			}
			continue
		}

		// Success - got block with numRewardPartitions
		return *result.NumRewardPartitions, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("unable to fetch numRewardPartitions after 20 attempts")
	}
	return 0, lastErr
}

func (fetcher *RpcClient) GetStakingRewardSlots(startSlot uint64, numPartitions uint64) ([]uint64, error) {
	result, err := fetcher.client.GetBlocksWithLimit(
		context.TODO(),
		startSlot+1,
		numPartitions,
		rpc.CommitmentFinalized)

	if err != nil {
		return nil, err
	}

	return *result, nil
}

func (fetcher *RpcClient) GetRewardSlots(slot uint64) ([]rpc.BlockReward, *uint64, error) {
	includeRewards := true
	maxSupportedTxVer := uint64(0)

	result, err := fetcher.client.GetBlockWithOpts(
		context.TODO(),
		slot,
		&rpc.GetBlockOpts{
			MaxSupportedTransactionVersion: &maxSupportedTxVer,
			Commitment:                     rpc.CommitmentFinalized,
			TransactionDetails:             rpc.TransactionDetailsNone,
			Rewards:                        &includeRewards,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	return result.Rewards, result.NumRewardPartitions, err
}

func (fetcher *RpcClient) GetLatestBlockConfirmed() (*rpc.GetBlockResult, error) {
	result, err := fetcher.client.GetLatestBlockhash(context.TODO(), rpc.CommitmentConfirmed)
	if err != nil {
		return nil, err
	}

	slot := result.Context.Slot

	return fetcher.GetBlockConfirmed(slot)
}

func (fetcher *RpcClient) GetLatestBlockFinalized() (*rpc.GetBlockResult, error) {
	result, err := fetcher.client.GetLatestBlockhash(context.TODO(), rpc.CommitmentFinalized)
	if err != nil {
		return nil, err
	}

	slot := result.Context.Slot

	return fetcher.GetBlockFinalized(slot)
}

func (fetcher *RpcClient) GetLeaderForSlot(slot uint64) (solana.PublicKey, error) {
	leader, err := fetcher.client.GetSlotLeaders(context.TODO(), slot, 1)
	if err != nil {
		return solana.PublicKey{}, err
	}
	return leader[0], err
}

func (fetcher *RpcClient) GetBlockTime(slot uint64) (int64, error) {
	ts, err := fetcher.client.GetBlockTime(context.TODO(), slot)
	if err != nil {
		return 0, rpc.ErrNotConfirmed
	}
	return int64(*ts), err
}

func (fetcher *RpcClient) GetSlot() (uint64, error) {
	slot, err := fetcher.client.GetSlot(context.TODO(), rpc.CommitmentConfirmed)
	if err != nil {
		return 0, rpc.ErrNotConfirmed
	}
	return slot, err
}

// GetSlotWithTimeout returns the current slot with a timeout (confirmed commitment).
// Useful for health probes where we don't want to block indefinitely.
func (fetcher *RpcClient) GetSlotWithTimeout(timeout time.Duration) (uint64, error) {
	return fetcher.GetSlotWithTimeoutAndCommitment(timeout, rpc.CommitmentConfirmed)
}

// GetSlotProcessedWithTimeout returns the current slot with processed commitment.
// Processed is the most recent slot - useful for measuring true distance to chain tip.
func (fetcher *RpcClient) GetSlotProcessedWithTimeout(timeout time.Duration) (uint64, error) {
	return fetcher.GetSlotWithTimeoutAndCommitment(timeout, rpc.CommitmentProcessed)
}

// GetSlotWithTimeoutAndCommitment returns the current slot with specified commitment and timeout.
func (fetcher *RpcClient) GetSlotWithTimeoutAndCommitment(timeout time.Duration, commitment rpc.CommitmentType) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	slot, err := fetcher.client.GetSlot(ctx, commitment)
	if err != nil {
		return 0, err
	}
	return slot, nil
}

func (fetcher *RpcClient) DownloadBlocksToFile(outDir string, slot uint64, num int64) {
	var fetchForever bool
	if num < 0 {
		fetchForever = true
	}

	endSlot := slot + uint64(num)
	slotMu := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	workers := 10
	wg.Add(int(workers))
	for i := 0; i < int(workers); i++ {
		go func() {
			defer wg.Done()
			for {
				slotMu.Lock()
				s := slot
				slot++
				slotMu.Unlock()
				if !fetchForever && s > endSlot {
					return
				}

				blockResult, err := fetcher.GetBlockFinalized(s)
				if err == SlotSkipped {
					continue
				} else if err != nil {
					mlog.Log.Errorf("error fetching slot=%d: %v", s, err)
					continue
				}
				blockFilename := filepath.Join(filepath.Clean(outDir), fmt.Sprintf("%d.json", s))
				saveBlockToFile(blockFilename, blockResult)
			}
		}()
	}
	wg.Wait()
}

func saveBlockToFile(filename string, b *rpc.GetBlockResult) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(b)
}
