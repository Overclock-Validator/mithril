package block

import (
	//laserstream "github.com/helius-labs/laserstream-sdk/go"
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/rpcpool/yellowstone-grpc/examples/golang/proto"
)

// FromLaserStream converts Laserstream's legacy/V0-only protobuf block into
// Mithril's native representation. Laserstream is an optional ingestion path;
// malformed or schema-incompatible input must fail closed rather than reach
// replay's panic-oriented invariant checks.
func FromLaserStream(lsBlock *proto.SubscribeUpdateBlock, rpcc *rpcclient.RpcClient) (*Block, error) {
	if lsBlock == nil {
		return nil, fmt.Errorf("Laserstream returned a nil block")
	}
	if lsBlock.BlockTime == nil {
		return nil, fmt.Errorf("Laserstream slot %d has no block time", lsBlock.Slot)
	}
	if lsBlock.Rewards == nil {
		return nil, fmt.Errorf("Laserstream slot %d has no rewards", lsBlock.Slot)
	}

	block := &Block{}

	block.Slot = lsBlock.GetSlot()

	for txIdx, tx := range lsBlock.Transactions {
		if tx == nil {
			return nil, fmt.Errorf("Laserstream slot %d transaction %d is nil", lsBlock.Slot, txIdx)
		}
		convertedTx, err := lsTransactionToTransaction(tx.Transaction)
		if err != nil {
			return nil, fmt.Errorf("Laserstream slot %d transaction %d: %w", lsBlock.Slot, txIdx, err)
		}
		meta, err := lsTxMetaToTxMeta(tx.Meta)
		if err != nil {
			return nil, fmt.Errorf("Laserstream slot %d transaction %d metadata: %w", lsBlock.Slot, txIdx, err)
		}
		expectedBalances := len(convertedTx.Message.AccountKeys) +
			len(meta.LoadedAddresses.Writable) + len(meta.LoadedAddresses.ReadOnly)
		if len(meta.PreBalances) != expectedBalances || len(meta.PostBalances) != expectedBalances {
			return nil, fmt.Errorf("Laserstream slot %d transaction %d metadata balance count mismatch: accounts=%d pre=%d post=%d",
				lsBlock.Slot, txIdx, expectedBalances, len(meta.PreBalances), len(meta.PostBalances))
		}
		block.Transactions = append(block.Transactions, convertedTx)
		block.NumSignatures += uint64(convertedTx.Message.Header.NumRequiredSignatures)
		block.Versions = append(block.Versions, uint8(convertedTx.Message.GetVersion()))
		block.TxMetas = append(block.TxMetas, meta)
	}

	var err error
	block.Blockhash, err = solana.HashFromBase58(lsBlock.Blockhash)
	if err != nil {
		return nil, fmt.Errorf("Laserstream slot %d has invalid blockhash: %w", lsBlock.Slot, err)
	}
	block.LastBlockhash, err = solana.HashFromBase58(lsBlock.ParentBlockhash)
	if err != nil {
		return nil, fmt.Errorf("Laserstream slot %d has invalid parent blockhash: %w", lsBlock.Slot, err)
	}
	block.UnixTimestamp = lsBlock.BlockTime.Timestamp

	// rewards
	for rewardIdx, r := range lsBlock.Rewards.Rewards {
		convertedReward, err := lsBlockRewardToBlockReward(r)
		if err != nil {
			return nil, fmt.Errorf("Laserstream slot %d reward %d: %w", lsBlock.Slot, rewardIdx, err)
		}
		block.Rewards = append(block.Rewards, convertedReward)
	}

	if lsBlock.Rewards.NumPartitions != nil {
		block.NumRewardPartitions = lsBlock.Rewards.NumPartitions.NumPartitions
	} else {
		block.NumRewardPartitions = math.MaxUint64
	}

	blockReward := blockRewardRewards(block.Rewards)
	if blockReward != nil {
		if blockReward.Lamports < 0 {
			return nil, fmt.Errorf("Laserstream slot %d fee reward has negative lamports %d", lsBlock.Slot, blockReward.Lamports)
		}
		block.BlockReward = &BlockRewardsInfo{Leader: blockReward.Pubkey, Lamports: uint64(blockReward.Lamports), PostBalance: blockReward.PostBalance}
	} else {
		if rpcc != nil {
			leaderForSlot, err := rpcc.GetLeaderForSlot(lsBlock.Slot)
			if err != nil {
				return nil, fmt.Errorf("Laserstream slot %d leader lookup: %w", lsBlock.Slot, err)
			}
			block.BlockReward = &BlockRewardsInfo{Leader: leaderForSlot}
		}
	}

	return block, nil
}

func lsBlockRewardToBlockReward(reward *proto.Reward) (rpc.BlockReward, error) {
	if reward == nil {
		return rpc.BlockReward{}, fmt.Errorf("nil reward")
	}
	pubkey, err := solana.PublicKeyFromBase58(reward.Pubkey)
	if err != nil {
		return rpc.BlockReward{}, fmt.Errorf("invalid reward pubkey: %w", err)
	}
	r := rpc.BlockReward{}
	r.Pubkey = pubkey
	r.Lamports = reward.Lamports
	r.PostBalance = reward.PostBalance
	r.RewardType = rpc.RewardType(reward.RewardType.String())
	return r, nil
}

func lsTxMetaToTxMeta(lsMeta *proto.TransactionStatusMeta) (*rpc.TransactionMeta, error) {
	if lsMeta == nil {
		return nil, fmt.Errorf("nil transaction metadata")
	}
	meta := &rpc.TransactionMeta{}
	meta.Err = lsMeta.Err
	meta.Fee = lsMeta.Fee
	meta.PreBalances = lsMeta.PreBalances
	meta.PostBalances = lsMeta.PostBalances

	for idx, loadedAddr := range lsMeta.LoadedReadonlyAddresses {
		if len(loadedAddr) != solana.PublicKeyLength {
			return nil, fmt.Errorf("readonly loaded address %d has invalid length %d", idx, len(loadedAddr))
		}
		convertedAddr := solana.PublicKeyFromBytes(loadedAddr)
		meta.LoadedAddresses.ReadOnly = append(meta.LoadedAddresses.ReadOnly, convertedAddr)
	}

	for idx, loadedAddr := range lsMeta.LoadedWritableAddresses {
		if len(loadedAddr) != solana.PublicKeyLength {
			return nil, fmt.Errorf("writable loaded address %d has invalid length %d", idx, len(loadedAddr))
		}
		convertedAddr := solana.PublicKeyFromBytes(loadedAddr)
		meta.LoadedAddresses.Writable = append(meta.LoadedAddresses.Writable, convertedAddr)
	}

	return meta, nil
}

func lsTransactionToTransaction(lsTx *proto.Transaction) (*solana.Transaction, error) {
	if lsTx == nil {
		return nil, fmt.Errorf("nil transaction")
	}
	if lsTx.Message == nil {
		return nil, fmt.Errorf("transaction has no message")
	}
	if len(lsTx.Message.ProtoReflect().GetUnknown()) != 0 {
		return nil, fmt.Errorf("unsupported transaction message fields: this Laserstream schema has no TxV1 config and cannot represent TxV1")
	}
	if lsTx.Message.Header == nil {
		return nil, fmt.Errorf("transaction message has no header")
	}
	if lsTx.Message.Header.NumReadonlySignedAccounts > math.MaxUint8 ||
		lsTx.Message.Header.NumReadonlyUnsignedAccounts > math.MaxUint8 ||
		lsTx.Message.Header.NumRequiredSignatures > math.MaxUint8 {
		return nil, fmt.Errorf("transaction header exceeds legacy/V0 u8 limits")
	}
	if lsTx.Message.Versioned && len(lsTx.Message.AddressTableLookups) == 0 {
		return nil, fmt.Errorf("ambiguous versioned transaction without address table lookups: this Laserstream schema cannot distinguish V0 from TxV1")
	}
	if !lsTx.Message.Versioned && len(lsTx.Message.AddressTableLookups) != 0 {
		return nil, fmt.Errorf("legacy transaction unexpectedly contains address table lookups")
	}

	tx := &solana.Transaction{}

	// signatures
	for idx, s := range lsTx.Signatures {
		if len(s) != solana.SignatureLength {
			return nil, fmt.Errorf("signature %d has invalid length %d", idx, len(s))
		}
		convertedSig := solana.SignatureFromBytes(s)
		tx.Signatures = append(tx.Signatures, convertedSig)
	}

	for idx, acctKey := range lsTx.Message.AccountKeys {
		if len(acctKey) != solana.PublicKeyLength {
			return nil, fmt.Errorf("account key %d has invalid length %d", idx, len(acctKey))
		}
		convertedAcctKey := solana.PublicKeyFromBytes(acctKey)
		tx.Message.AccountKeys = append(tx.Message.AccountKeys, convertedAcctKey)
	}

	tx.Message.Header.NumReadonlySignedAccounts = uint8(lsTx.Message.Header.NumReadonlySignedAccounts)
	tx.Message.Header.NumReadonlyUnsignedAccounts = uint8(lsTx.Message.Header.NumReadonlyUnsignedAccounts)
	tx.Message.Header.NumRequiredSignatures = uint8(lsTx.Message.Header.NumRequiredSignatures)

	if len(lsTx.Message.RecentBlockhash) != len(solana.Hash{}) {
		return nil, fmt.Errorf("recent blockhash has invalid length %d", len(lsTx.Message.RecentBlockhash))
	}
	tx.Message.RecentBlockhash = solana.HashFromBytes(lsTx.Message.RecentBlockhash)

	for idx, instr := range lsTx.Message.Instructions {
		convertedInstr, err := lsInstrToInstr(instr)
		if err != nil {
			return nil, fmt.Errorf("instruction %d: %w", idx, err)
		}
		tx.Message.Instructions = append(tx.Message.Instructions, convertedInstr)
	}

	if lsTx.Message.Versioned {
		lookups := make([]solana.MessageAddressTableLookup, len(lsTx.Message.AddressTableLookups))
		for idx, addrTableLookup := range lsTx.Message.AddressTableLookups {
			lookup, err := lsAddrTableLookupToAddrTableLookup(addrTableLookup)
			if err != nil {
				return nil, fmt.Errorf("address table lookup %d: %w", idx, err)
			}
			lookups[idx] = lookup
		}
		tx.Message.SetAddressTableLookups(lookups)
	}
	if err := txverify.SanitizeTransaction(tx); err != nil {
		return nil, fmt.Errorf("invalid legacy/V0 transaction: %w", err)
	}

	return tx, nil
}

func lsInstrToInstr(instr *proto.CompiledInstruction) (solana.CompiledInstruction, error) {
	if instr == nil {
		return solana.CompiledInstruction{}, fmt.Errorf("nil instruction")
	}
	if instr.ProgramIdIndex > math.MaxUint8 {
		return solana.CompiledInstruction{}, fmt.Errorf("program ID index %d exceeds legacy/V0 u8 limit", instr.ProgramIdIndex)
	}
	compiledInstr := solana.CompiledInstruction{}
	compiledInstr.ProgramIDIndex = uint16(instr.ProgramIdIndex)

	for _, acct := range instr.Accounts {
		compiledInstr.Accounts = append(compiledInstr.Accounts, uint16(acct))
	}

	compiledInstr.Data = instr.Data
	return compiledInstr, nil
}

func lsAddrTableLookupToAddrTableLookup(atl *proto.MessageAddressTableLookup) (solana.MessageAddressTableLookup, error) {
	if atl == nil {
		return solana.MessageAddressTableLookup{}, fmt.Errorf("nil address table lookup")
	}
	if len(atl.AccountKey) != solana.PublicKeyLength {
		return solana.MessageAddressTableLookup{}, fmt.Errorf("account key has invalid length %d", len(atl.AccountKey))
	}
	converted := solana.MessageAddressTableLookup{}
	converted.AccountKey = solana.PublicKeyFromBytes(atl.AccountKey)
	converted.ReadonlyIndexes = atl.ReadonlyIndexes
	converted.WritableIndexes = atl.WritableIndexes
	return converted, nil
}
