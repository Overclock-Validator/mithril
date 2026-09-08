package block

import (
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	tpuwire "github.com/Overclock-Validator/mithril/pkg/tpu/wire"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go/rpc"
)

func FromBlockResult(blockResult *rpc.GetBlockResult, slot uint64, rpcc *rpcclient.RpcClient) (*Block, error) {
	if blockResult == nil {
		return nil, fmt.Errorf("convert RPC block at slot %d: nil block result", slot)
	}
	block := new(Block)
	block.Slot = slot

	for txIndex, tx := range blockResult.Transactions {
		if tx.Transaction != nil {
			if encoded := tx.Transaction.GetBinary(); encoded != nil {
				if _, err := tpuwire.Sanitize(encoded); err != nil {
					return nil, fmt.Errorf("convert RPC block at slot %d transaction %d: invalid wire transaction: %w", slot, txIndex, err)
				}
			}
		}
		txParsed, err := tx.GetTransaction()
		if err != nil {
			return nil, fmt.Errorf("convert RPC block at slot %d transaction %d: %w", slot, txIndex, err)
		}
		if err := txverify.SanitizeTransaction(txParsed); err != nil {
			return nil, fmt.Errorf("convert RPC block at slot %d transaction %d: sanitize: %w", slot, txIndex, err)
		}
		block.Transactions = append(block.Transactions, txParsed)
		block.TxMetas = append(block.TxMetas, tx.Meta)
		block.Versions = append(block.Versions, uint8(txParsed.Message.GetVersion()))
	}

	block.Blockhash = blockResult.Blockhash
	block.LastBlockhash = blockResult.PreviousBlockhash

	if blockResult.BlockTime == nil {
		mlog.Log.Infof("slot %d had nil BlockTime field", slot)
	} else {
		block.UnixTimestamp = int64(*blockResult.BlockTime)
	}

	block.Rewards = blockResult.Rewards
	if blockResult.NumRewardPartitions != nil {
		block.NumRewardPartitions = *blockResult.NumRewardPartitions
	} else {
		block.NumRewardPartitions = math.MaxUint64
	}
	blockReward := blockRewardRewards(blockResult.Rewards)

	if !global.ManageLeaderSchedule() {
		if blockReward != nil {
			block.BlockReward = &BlockRewardsInfo{Leader: blockReward.Pubkey, Lamports: uint64(blockReward.Lamports), PostBalance: blockReward.PostBalance}
			block.Leader = blockReward.Pubkey
		} else {
			if rpcc != nil {
				leaderForSlot, err := rpcc.GetLeaderForSlot(slot)
				if err == nil {
					block.BlockReward = &BlockRewardsInfo{Leader: leaderForSlot}
					block.Leader = leaderForSlot
				}
			}
		}
	}

	for _, tx := range block.Transactions {
		block.NumSignatures += uint64(tx.Message.Header.NumRequiredSignatures)
	}

	return block, nil
}

func blockRewardRewards(rewards []rpc.BlockReward) *rpc.BlockReward {
	for _, reward := range rewards {
		if string(reward.RewardType) == "Fee" {
			return &reward
		}
	}

	return nil
}
