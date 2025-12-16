package block

import (
	"context"
	"fmt"
	"math"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/rpcpool/yellowstone-grpc/examples/golang/proto"
)

type LeaderFetcher interface {
	GetLeaderForSlot(slot uint64) (solana.PublicKey, error)
}

func FromLaserStream(lsBlock *proto.SubscribeUpdateBlock, rpcc LeaderFetcher) *Block {
	block := &Block{}

	block.Slot = lsBlock.GetSlot()

	for _, tx := range lsBlock.Transactions {
		convertedTx := lsTransactionToTransaction(tx.Transaction)
		block.Transactions = append(block.Transactions, convertedTx)
		block.NumSignatures += uint64(convertedTx.Message.Header.NumRequiredSignatures)

		meta := lsTxMetaToTxMeta(tx.Meta)
		block.TxMetas = append(block.TxMetas, meta)
	}

	block.Blockhash = solana.MustHashFromBase58(lsBlock.Blockhash)
	block.LastBlockhash = solana.MustHashFromBase58(lsBlock.ParentBlockhash)
	block.UnixTimestamp = lsBlock.BlockTime.Timestamp
	block.BlockHeight = lsBlock.BlockHeight.BlockHeight

	for _, r := range lsBlock.Rewards.Rewards {
		convertedReward := lsBlockRewardToBlockReward(r)
		block.Rewards = append(block.Rewards, convertedReward)
	}

	if lsBlock.Rewards.NumPartitions != nil {
		block.NumRewardPartitions = lsBlock.Rewards.NumPartitions.NumPartitions
	} else {
		block.NumRewardPartitions = math.MaxUint64
	}

	blockReward := blockRewardRewards(block.Rewards)
	if blockReward != nil {
		block.BlockReward = &BlockRewardsInfo{Leader: blockReward.Pubkey, Lamports: uint64(blockReward.Lamports), PostBalance: blockReward.PostBalance}
	} else {
		if rpcc != nil {
			result, err := RetryWithExponentialBackoff(context.Background(), maxRetriesGetLeaderForSlot, func(retryCtx context.Context) (interface{}, error) {
				return rpcc.GetLeaderForSlot(lsBlock.Slot)
			})

			if err != nil {
				panic(fmt.Sprintf("unable to get blockreward for slot %d after %d attempts: %v", lsBlock.Slot, maxRetriesGetLeaderForSlot, err))
			}

			leaderForSlot := result.(solana.PublicKey)
			block.BlockReward = &BlockRewardsInfo{Leader: leaderForSlot}
		}
	}

	return block
}

func lsBlockRewardToBlockReward(reward *proto.Reward) rpc.BlockReward {
	r := rpc.BlockReward{}
	r.Pubkey = solana.MustPublicKeyFromBase58(reward.Pubkey)
	r.Lamports = reward.Lamports
	r.PostBalance = reward.PostBalance
	r.RewardType = rpc.RewardType(reward.RewardType.String())
	return r
}

func lsTxMetaToTxMeta(lsMeta *proto.TransactionStatusMeta) *rpc.TransactionMeta {
	meta := &rpc.TransactionMeta{}
	meta.Err = lsMeta.Err
	meta.Fee = lsMeta.Fee
	meta.PreBalances = lsMeta.PreBalances
	meta.PostBalances = lsMeta.PostBalances

	for _, loadedAddr := range lsMeta.LoadedReadonlyAddresses {
		convertedAddr := solana.PublicKeyFromBytes(loadedAddr)
		meta.LoadedAddresses.ReadOnly = append(meta.LoadedAddresses.ReadOnly, convertedAddr)
	}

	for _, loadedAddr := range lsMeta.LoadedWritableAddresses {
		convertedAddr := solana.PublicKeyFromBytes(loadedAddr)
		meta.LoadedAddresses.ReadOnly = append(meta.LoadedAddresses.Writable, convertedAddr)
	}

	return meta
}

func lsTransactionToTransaction(lsTx *proto.Transaction) *solana.Transaction {
	tx := &solana.Transaction{}

	// signatures
	for _, s := range lsTx.Signatures {
		convertedSig := solana.SignatureFromBytes(s)
		tx.Signatures = append(tx.Signatures, convertedSig)
	}

	// message
	if !lsTx.Message.Versioned {
		tx.Message.SetVersion(1)
	}

	for _, acctKey := range lsTx.Message.AccountKeys {
		convertedAcctKey := solana.PublicKeyFromBytes(acctKey)
		tx.Message.AccountKeys = append(tx.Message.AccountKeys, convertedAcctKey)
	}

	tx.Message.Header.NumReadonlySignedAccounts = uint8(lsTx.Message.Header.NumReadonlySignedAccounts)
	tx.Message.Header.NumReadonlyUnsignedAccounts = uint8(lsTx.Message.Header.NumReadonlyUnsignedAccounts)
	tx.Message.Header.NumRequiredSignatures = uint8(lsTx.Message.Header.NumRequiredSignatures)

	tx.Message.RecentBlockhash = solana.HashFromBytes(lsTx.Message.RecentBlockhash)

	for _, instr := range lsTx.Message.Instructions {
		convertedInstr := lsInstrToInstr(instr)
		tx.Message.Instructions = append(tx.Message.Instructions, convertedInstr)
	}

	for _, addrTableLookup := range lsTx.Message.AddressTableLookups {
		convertedAtl := lsAddrTableLookupToAddrTableLookup(addrTableLookup)
		tx.Message.AddressTableLookups = append(tx.Message.AddressTableLookups, convertedAtl)
	}

	return tx
}

func lsInstrToInstr(instr *proto.CompiledInstruction) solana.CompiledInstruction {
	compiledInstr := solana.CompiledInstruction{}
	compiledInstr.ProgramIDIndex = uint16(instr.ProgramIdIndex)

	for _, acct := range instr.Accounts {
		compiledInstr.Accounts = append(compiledInstr.Accounts, uint16(acct))
	}

	compiledInstr.Data = instr.Data
	return compiledInstr
}

func lsAddrTableLookupToAddrTableLookup(atl *proto.MessageAddressTableLookup) solana.MessageAddressTableLookup {
	converted := solana.MessageAddressTableLookup{}
	converted.AccountKey = solana.PublicKeyFromBytes(atl.AccountKey)
	converted.ReadonlyIndexes = atl.ReadonlyIndexes
	converted.WritableIndexes = atl.WritableIndexes
	return converted
}
