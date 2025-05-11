package replay

import (
	"fmt"
	"math"

	//"runtime/debug"

	//"runtime/pprof"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rent"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/snapshot"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/mithril/pkg/util"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

type BlockRewardsInfo struct {
	Leader      solana.PublicKey
	Lamports    uint64
	PostBalance uint64
}

type Block struct {
	Slot                   uint64
	ParentSlot             uint64
	BlockHeight            uint64
	Epoch                  uint64
	Transactions           []*solana.Transaction
	BankHash               [32]byte
	EpochAcctsHash         []byte
	EahWorkaroundBankhash  []byte
	HasEahWorkaround       bool
	ParentBankhash         [32]byte
	NumSignatures          uint64
	Blockhash              [32]byte
	ExpectedBankhash       [32]byte
	TxMetas                []*rpc.TransactionMeta
	Leader                 solana.PublicKey
	BlockReward            *BlockRewardsInfo
	LastBlockhash          [32]byte
	UnixTimestamp          int64
	StakeAccts             map[solana.PublicKey]bool
	VoteAccts              map[solana.PublicKey]uint64
	VoteTimestamps         map[solana.PublicKey]sealevel.BlockTimestamp
	TotalEpochStake        uint64
	Features               *features.Features
	UpdatedAccts           []solana.PublicKey
	PartitionedRewardsInfo *rewards.PartitionedRewardDistributionInfo
	Rewards                []rpc.BlockReward
	NumRewardPartitions    uint64
}

func resolveAddrTableLookups(accountsDb *accountsdb.AccountsDb, block *Block) error {
	tables := make(map[solana.PublicKey]solana.PublicKeySlice)

	for idx, tx := range block.Transactions {
		mlog.Log.Debugf("resolveAddrTableLookups for transaction %d", idx)

		if !tx.Message.IsVersioned() {
			continue
		}

		var skipLookup bool
		for _, addrTableKey := range tx.Message.GetAddressTableLookups().GetTableIDs() {
			acct, err := accountsDb.GetAccount(block.Slot, addrTableKey)
			if err != nil {
				mlog.Log.Debugf("unable to get address lookup table account: %s", addrTableKey)
				skipLookup = true
				break
			}

			addrLookupTable, err := sealevel.UnmarshalAddressLookupTable(acct.Data)
			if err != nil {
				return err
			}

			tables[addrTableKey] = addrLookupTable.Addresses
		}

		if skipLookup {
			continue
		}

		err := tx.Message.SetAddressTables(tables)
		if err != nil {
			return err
		}

		err = tx.Message.ResolveLookups()
		if err != nil {
			return err
		}
	}

	return nil
}

func extractAndDedupeBlockAccts(block *Block) []solana.PublicKey {
	var numPubkeys int
	for _, tx := range block.Transactions {
		numPubkeys += len(tx.Message.AccountKeys)
	}

	numPubkeys += len(block.UpdatedAccts)

	pubkeys := make([]solana.PublicKey, 0, numPubkeys)
	for _, tx := range block.Transactions {
		for _, pubkey := range tx.Message.AccountKeys {
			pubkeys = append(pubkeys, pubkey)
		}
	}

	for _, pubkey := range block.UpdatedAccts {
		pubkeys = append(pubkeys, pubkey)
	}

	pubkeys = util.DedupePubkeys(pubkeys)
	return pubkeys
}

func isNativeProgram(pubkey solana.PublicKey) bool {
	if pubkey == sealevel.SystemProgramAddr || pubkey == sealevel.BpfLoaderUpgradeableAddr ||
		pubkey == sealevel.BpfLoader2Addr || pubkey == sealevel.BpfLoaderDeprecatedAddr ||
		pubkey == sealevel.VoteProgramAddr || pubkey == sealevel.StakeProgramAddr ||
		pubkey == sealevel.ConfigProgramAddr || pubkey == sealevel.StakeProgramConfigAddr ||
		pubkey == sealevel.NativeLoaderAddr {
		return true
	} else {
		return false
	}
}

func isSysvar(pubkey solana.PublicKey) bool {
	if pubkey == sealevel.SysvarClockAddr || pubkey == sealevel.SysvarEpochScheduleAddr ||
		pubkey == sealevel.SysvarFeesAddr || pubkey == sealevel.SysvarInstructionsAddr ||
		pubkey == sealevel.SysvarRecentBlockHashesAddr || pubkey == sealevel.SysvarRentAddr ||
		pubkey == sealevel.SysvarRewardsAddr || pubkey == sealevel.SysvarSlotHashesAddr ||
		pubkey == sealevel.SysvarSlotHistoryAddr || pubkey == sealevel.SysvarStakeHistoryAddr {
		return true
	} else {
		return false
	}
}

func cacheConstantSysvars(acctsDb *accountsdb.AccountsDb) {
	{
		acct, err := acctsDb.GetAccount(0, sealevel.SysvarEpochScheduleAddr)
		if err != nil {
			panic("unable to get epochschedule when caching sysvars")
		}
		decoder := bin.NewBinDecoder(acct.Data)
		var epochSchedule sealevel.SysvarEpochSchedule
		epochSchedule.MustUnmarshalWithDecoder(decoder)
		sealevel.SysvarCache.EpochSchedule.Sysvar = &epochSchedule
		sealevel.SysvarCache.EpochSchedule.Acct = acct
	}

	{
		acct, err := acctsDb.GetAccount(0, sealevel.SysvarRentAddr)
		if err != nil {
			panic("unable to get rent sysvar when caching sysvars")
		}
		var rent sealevel.SysvarRent
		decoder := bin.NewBinDecoder(acct.Data)
		rent.MustUnmarshalWithDecoder(decoder)
		sealevel.SysvarCache.Rent.Sysvar = &rent
		sealevel.SysvarCache.Rent.Acct = acct
	}

	{
		acct, err := acctsDb.GetAccount(0, sealevel.SysvarFeesAddr)
		if err != nil {
			panic("nable to get fees sysvar when caching sysvars")
		}
		var fees sealevel.SysvarFees
		decoder := bin.NewBinDecoder(acct.Data)
		fees.MustUnmarshalWithDecoder(decoder)
		sealevel.SysvarCache.Fees.Sysvar = &fees
		sealevel.SysvarCache.Fees.Acct = acct
	}
}

func loadBlockAccountsAndUpdateSysvars(accountsDb *accountsdb.AccountsDb, block *Block) (accounts.Accounts, error) {
	err := resolveAddrTableLookups(accountsDb, block)
	if err != nil {
		return nil, err
	}

	dedupedAccts := extractAndDedupeBlockAccts(block)
	accts := accounts.NewMemAccounts()

	for _, pk := range dedupedAccts {

		// retrieve account from accountsdb
		acct, err := accountsDb.GetAccount(block.Slot, pk)

		// add the account to the slice, add a 'blank' account if the account doesn't exist,
		// or return an error
		if err == accountsdb.ErrNoAccount {
			if isNativeProgram(pk) {
				acct = &accounts.Account{Key: pk, Owner: sealevel.NativeLoaderAddr, Executable: true, Lamports: 1}
				mlog.Log.Debugf("no account: %s, using empty owned by Native Loader\n", pk)
			} else {
				acct = &accounts.Account{Key: pk, Owner: sealevel.SystemProgramAddr, RentEpoch: math.MaxUint64}
				mlog.Log.Debugf("no account: %s, using empty owned by System program\n", pk)
			}
		} else if err != nil {
			return nil, err
		} else {
			if acct.Lamports == 0 {
				acct = &accounts.Account{Key: pk, Owner: sealevel.SystemProgramAddr, RentEpoch: math.MaxUint64}
			} else {
				mlog.Log.Debugf("found account in loadBlockAccounts for: %s\n", acct.Key)
			}
		}

		var pkBytes [32]byte
		copy(pkBytes[:], pk.Bytes())

		err = accts.SetAccount(&pkBytes, acct)
		if err != nil {
			return nil, err
		}
	}

	// load sysvar accounts and assign them to the sysvar cache
	{
		// update and cache clock sysvar
		{
			clockAcct, err := accountsDb.GetAccount(block.Slot, sealevel.SysvarClockAddr)
			if err != nil {
				panic("unable to retrieve clock sysvar when updating clock")
			}
			decoder := bin.NewBinDecoder(clockAcct.Data)
			var clock sealevel.SysvarClock

			err = clock.UnmarshalWithDecoder(decoder)
			if err != nil {
				panic(fmt.Sprintf("unable to unmarshal clock sysvar"))
			}

			err = updateClockSysvar(&clock, block)
			if err != nil {
				panic(fmt.Sprintf("failed to update clock sysvar: %s", err))
			}

			newClockBytes := clock.MustMarshal()
			copy(clockAcct.Data, newClockBytes)
			sealevel.SysvarCache.Clock.Sysvar = &clock
			sealevel.SysvarCache.Clock.Acct = clockAcct

			var sysvarPkBytes [32]byte
			copy(sysvarPkBytes[:], sealevel.SysvarClockAddr[:])
			err = accts.SetAccount(&sysvarPkBytes, clockAcct)
			if err != nil {
				panic("unable to set clock sysvar to accts")
			}
		}

		// update and cache SlotHashes sysvar
		{
			slotHashesAcct, err := accountsDb.GetAccount(block.Slot, sealevel.SysvarSlotHashesAddr)
			if err != nil {
				panic("unable to retrieve slothashes sysvar from acctsdb")
			}

			decoder := bin.NewBinDecoder(slotHashesAcct.Data)
			var slotHashes sealevel.SysvarSlotHashes

			err = slotHashes.UnmarshalWithDecoder(decoder)
			if err != nil {
				panic(fmt.Sprintf("unable to unmarshal slothashes sysvar"))
			}

			slotHashes.Update(block.Slot, block.ParentSlot, block.ParentBankhash)
			newSlotHashesBytes := slotHashes.MustMarshal()
			copy(slotHashesAcct.Data, newSlotHashesBytes)
			sealevel.SysvarCache.SlotHashes.Sysvar = &slotHashes
			sealevel.SysvarCache.SlotHashes.Acct = slotHashesAcct

			var sysvarPkBytes [32]byte
			copy(sysvarPkBytes[:], sealevel.SysvarSlotHashesAddr[:])
			err = accts.SetAccount(&sysvarPkBytes, slotHashesAcct)
			if err != nil {
				panic("unable to set slothashes sysvar to accountsdb")
			}
		}

		// cache RecentBlockhashes sysvar
		{
			recentBlockhashesAcct, err := accountsDb.GetAccount(block.Slot, sealevel.SysvarRecentBlockHashesAddr)
			if err != nil {
				panic("unable to get recentblockhashes")
			}
			decoder := bin.NewBinDecoder(recentBlockhashesAcct.Data)
			var recentBlockhashes sealevel.SysvarRecentBlockhashes
			recentBlockhashes.MustUnmarshalWithDecoder(decoder)
			sealevel.SysvarCache.RecentBlockHashes.Sysvar = &recentBlockhashes
			sealevel.SysvarCache.RecentBlockHashes.Acct = recentBlockhashesAcct

			var sysvarPkBytes [32]byte
			copy(sysvarPkBytes[:], sealevel.SysvarRecentBlockHashesAddr[:])
			err = accts.SetAccount(&sysvarPkBytes, recentBlockhashesAcct)
			if err != nil {
				panic("unable to set recentblockhashes sysvar to accts")
			}
		}

		// cache SlotHistory sysvar
		{
			slotHistoryAcct, err := accountsDb.GetAccount(block.Slot, sealevel.SysvarSlotHistoryAddr)
			if err != nil {
				panic("unable to get slothistory")
			}
			decoder := bin.NewBinDecoder(slotHistoryAcct.Data)
			var slotHistory sealevel.SysvarSlotHistory
			slotHistory.MustUnmarshalWithDecoder(decoder)
			sealevel.SysvarCache.SlotHistory.Sysvar = &slotHistory
			sealevel.SysvarCache.SlotHistory.Acct = slotHistoryAcct

			var sysvarPkBytes [32]byte
			copy(sysvarPkBytes[:], sealevel.SysvarSlotHistoryAddr[:])
			err = accts.SetAccount(&sysvarPkBytes, slotHistoryAcct)
			if err != nil {
				panic("unable to set clock sysvar to accts")
			}
		}

		// cache StakeHistory sysvar
		{
			stakeHistoryAcct, err := accountsDb.GetAccount(block.Slot, sealevel.SysvarStakeHistoryAddr)
			if err != nil {
				panic("unable to get stakehistory")
			}
			decoder := bin.NewBinDecoder(stakeHistoryAcct.Data)
			var stakeHistory sealevel.SysvarStakeHistory
			stakeHistory.MustUnmarshalWithDecoder(decoder)
			sealevel.SysvarCache.StakeHistory.Sysvar = &stakeHistory
			sealevel.SysvarCache.StakeHistory.Acct = stakeHistoryAcct

			var sysvarPkBytes [32]byte
			copy(sysvarPkBytes[:], sealevel.SysvarStakeHistoryAddr[:])
			err = accts.SetAccount(&sysvarPkBytes, stakeHistoryAcct)
			if err != nil {
				panic("unable to set stakehistory sysvar to accts")
			}
		}

		// cache LastRestartSlot sysvar
		{
			lastRestartSlotAcct, err := accountsDb.GetAccount(block.Slot, sealevel.SysvarLastRestartSlotAddr)
			if err != nil {
				panic("unable to get last restart slot sysvar acct")
			}
			decoder := bin.NewBinDecoder(lastRestartSlotAcct.Data)
			var lastRestartSlot sealevel.SysvarLastRestartSlot
			lastRestartSlot.MustUnmarshalWithDecoder(decoder)
			sealevel.SysvarCache.LastRestartSlot.Sysvar = &lastRestartSlot
			sealevel.SysvarCache.LastRestartSlot.Acct = lastRestartSlotAcct

			var sysvarPkBytes [32]byte
			copy(sysvarPkBytes[:], sealevel.SysvarLastRestartSlotAddr[:])
			err = accts.SetAccount(&sysvarPkBytes, lastRestartSlotAcct)
			if err != nil {
				panic("unable to set last restart slot sysvar to accts")
			}
		}
	}

	return accts, nil
}

func scanAndEnableFeatures(acctsDb *accountsdb.AccountsDb, slot uint64, startOfEpoch bool) (*features.Features, []solana.PublicKey) {
	newlyActivatedFeatures := make([]solana.PublicKey, 0)
	acctsToStore := make([]*accounts.Account, 0)

	f := features.NewFeaturesDefault()

	for _, featureGate := range features.AllFeatureGates {
		acct, err := acctsDb.GetAccount(slot, featureGate.Address)
		if err == nil {
			if acct.Owner != sealevel.FeatureAddr {
				continue
			}

			featureAcct := features.UnmarshalFeatureAcct(acct.Data)

			// already activated
			if featureAcct.ActivatedAt != nil && slot >= *featureAcct.ActivatedAt {
				f.EnableFeature(featureGate, *featureAcct.ActivatedAt)
				mlog.Log.Debugf("enabled *already* enabled feature: %s, %s", featureGate.Name, solana.PublicKeyFromBytes(featureGate.Address[:]))
			}

			if featureAcct.ActivatedAt == nil && startOfEpoch {
				newFeatureAcct := &features.FeatureAcct{ActivatedAt: &slot}
				newFeatureAcctBytes, err := features.MarshalFeatureAcct(newFeatureAcct)
				if err != nil {
					panic(err)
				}

				acct.Data = newFeatureAcctBytes
				acctsToStore = append(acctsToStore, acct)

				newlyActivatedFeatures = append(newlyActivatedFeatures, featureGate.Address)
				f.EnableFeature(featureGate, slot)
				mlog.Log.Debugf("enabled pending feature: %s, %s", featureGate.Name, solana.PublicKeyFromBytes(featureGate.Address[:]))
			}
		}
	}

	if len(acctsToStore) != 0 {
		err := acctsDb.StoreAccounts(acctsToStore, slot)
		if err != nil {
			panic(err)
		}
	}

	mlog.Log.Debugf("scanAndEnableFeatures, modified features:\n")
	for _, feat := range newlyActivatedFeatures {
		mlog.Log.Debugf("feature: %s", feat)
	}

	return f, newlyActivatedFeatures
}

func blockRewardRewards(rewards []rpc.BlockReward) *rpc.BlockReward {
	for _, reward := range rewards {
		if string(reward.RewardType) == "Fee" {
			return &reward
		}
	}

	return nil
}

func newBlockFromBlockResult(blockResult *rpc.GetBlockResult) (*Block, error) {
	block := new(Block)

	for _, tx := range blockResult.Transactions {
		txParsed, err := tx.GetTransaction()
		if err != nil {
			return nil, err
		}
		block.Transactions = append(block.Transactions, txParsed)
		block.TxMetas = append(block.TxMetas, tx.Meta)
	}

	block.Blockhash = blockResult.Blockhash
	block.LastBlockhash = blockResult.PreviousBlockhash
	block.UnixTimestamp = int64(*blockResult.BlockTime)
	block.BlockHeight = *blockResult.BlockHeight
	block.Rewards = blockResult.Rewards

	if blockResult.NumRewardPartitions != nil {
		block.NumRewardPartitions = *blockResult.NumRewardPartitions
	} else {
		block.NumRewardPartitions = math.MaxUint64
	}

	blockReward := blockRewardRewards(blockResult.Rewards)
	if blockReward != nil {
		block.BlockReward = &BlockRewardsInfo{Leader: blockReward.Pubkey, Lamports: uint64(blockReward.Lamports), PostBalance: blockReward.PostBalance}
	}

	for _, tx := range block.Transactions {
		block.NumSignatures += uint64(tx.Message.Header.NumRequiredSignatures)
	}

	return block, nil
}

func setupInitialVoteAcctsAndStakeAccts(block *Block, snapshotManifest *snapshot.SnapshotManifest) {
	block.VoteTimestamps = make(map[solana.PublicKey]sealevel.BlockTimestamp)
	block.StakeAccts = make(map[solana.PublicKey]bool)
	block.VoteAccts = make(map[solana.PublicKey]uint64)

	for _, va := range snapshotManifest.Bank.Stakes.VoteAccounts {
		ts := sealevel.BlockTimestamp{Slot: va.Value.LastTimestampSlot, Timestamp: va.Value.LastTimestampTs}
		block.VoteTimestamps[va.Key] = ts
		block.VoteAccts[va.Key] = va.Stake
		block.TotalEpochStake += va.Stake
	}

	for _, sa := range snapshotManifest.Bank.Stakes.StakeDelegations {
		block.StakeAccts[sa.Account] = true
	}
}

func configureInitialBlock(block *Block, snapshotManifest *snapshot.SnapshotManifest, epochCtx *ReplayCtx) {
	block.ParentBankhash = snapshotManifest.Bank.Hash
	block.ParentSlot = snapshotManifest.Bank.Slot
	block.EpochAcctsHash = epochCtx.EpochAcctsHash
	setupInitialVoteAcctsAndStakeAccts(block, snapshotManifest)
	snapshotManifest = nil
}

func configureBlock(block *Block, epochCtx *ReplayCtx, lastSlotCtx *sealevel.SlotCtx) {
	copy(block.ParentBankhash[:], lastSlotCtx.FinalBankhash)
	block.StakeAccts = lastSlotCtx.StakeAccts
	block.VoteTimestamps = lastSlotCtx.VoteTimestamps
	block.VoteAccts = lastSlotCtx.VoteAccts
	block.ParentSlot = lastSlotCtx.Slot
	block.EpochAcctsHash = epochCtx.EpochAcctsHash
}

func ReplayBlocks(acctsDb *accountsdb.AccountsDb, acctsDbPath string, snapshotManifest *snapshot.SnapshotManifest, startSlot, endSlot uint64, rpcEndpoint string, updateAcctsDb bool) error {
	rpcc := rpcclient.NewRpcClient(rpcEndpoint)
	cacheConstantSysvars(acctsDb)
	epochSchedule := sealevel.SysvarCache.EpochSchedule.Sysvar

	var err error
	var currentSlot uint64
	currentEpoch := epochSchedule.GetEpoch(startSlot)
	var lastSlotCtx *sealevel.SlotCtx
	var partitionedEpochRewardsEnabled bool
	var partitionedRewardsInfo *rewards.PartitionedRewardDistributionInfo
	var featuresActivatedInFirstSlot []solana.PublicKey

	replayCtx := newReplayCtx(snapshotManifest)

	isFirstSlotInEpoch := epochSchedule.FirstSlotInEpoch(currentEpoch) == startSlot
	replayCtx.CurrentFeatures, featuresActivatedInFirstSlot = scanAndEnableFeatures(acctsDb, startSlot, isFirstSlotInEpoch)
	partitionedEpochRewardsEnabled = replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochReward) || replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)

	var statsCounter uint64
	var timeAccumulator float64
	var justCrossedEpochBoundary bool

	streamChan := make(chan *Block, endSlot-startSlot)
	blockStream := NewBlockStream(rpcc, streamChan, startSlot, endSlot, 2000)
	blockStream.downloadInitialBlocks()
	go blockStream.startAsyncBlockStream()

	for block := range streamChan {
		start := time.Now()

		currentSlot = block.Slot
		if currentSlot == startSlot {
			configureInitialBlock(block, snapshotManifest, replayCtx)

		} else {
			configureBlock(block, replayCtx, lastSlotCtx)
		}

		block.Epoch = epochSchedule.GetEpoch(currentSlot)

		// epoch boundary
		if block.Epoch != currentEpoch {
			mlog.Log.Infof("epoch boundary")

			var newlyActivatedFeatures []solana.PublicKey
			replayCtx.CurrentFeatures, newlyActivatedFeatures = scanAndEnableFeatures(acctsDb, currentSlot, true)
			partitionedEpochRewardsEnabled = replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochReward) || replayCtx.CurrentFeatures.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)

			var updatedPks []solana.PublicKey
			partitionedRewardsInfo, updatedPks, block.VoteAccts = handleEpochTransition(acctsDb, rpcc, partitionedEpochRewardsEnabled, lastSlotCtx, replayCtx, epochSchedule, replayCtx.CurrentFeatures, block, currentEpoch)
			if partitionedEpochRewardsEnabled {
				block.UpdatedAccts = append(block.UpdatedAccts, updatedPks...)
			}
			block.UpdatedAccts = append(block.UpdatedAccts, newlyActivatedFeatures...)
			currentEpoch = block.Epoch
			justCrossedEpochBoundary = true
		} else if currentSlot == startSlot && partitionedEpochRewardsEnabled {
			partitionedRewardsInfo = rewards.DeterminePartitionedStakingRewardsInfo(rpcc, epochSchedule, &replayCtx.Inflation, replayCtx.Capitalization, block.Epoch, block.Epoch-1, currentSlot, replayCtx.SlotsPerYear, replayCtx.CurrentFeatures)
			if startSlot <= partitionedRewardsInfo.LastStakingRewardSlot {
				calculatePartitionedEpochRewardsDuringRewardsWindow(partitionedRewardsInfo, acctsDb, block, epochSchedule, startSlot, currentEpoch, replayCtx.CurrentFeatures)
			}
		}

		block.Features = replayCtx.CurrentFeatures
		block.PartitionedRewardsInfo = partitionedRewardsInfo

		if len(block.Rewards) > 1 && partitionedEpochRewardsEnabled && currentSlot >= partitionedRewardsInfo.FirstStakingRewardSlot && currentSlot <= partitionedRewardsInfo.LastStakingRewardSlot {
			rewardPks := distributePartitionedEpochRewardsForSlot(acctsDb, replayCtx, partitionedRewardsInfo, currentSlot, block.BlockHeight, partitionedRewardsInfo.LastStakingRewardSlot)
			block.UpdatedAccts = append(block.UpdatedAccts, rewardPks...)
		}

		if len(featuresActivatedInFirstSlot) != 0 {
			block.UpdatedAccts = append(block.UpdatedAccts, featuresActivatedInFirstSlot...)
			featuresActivatedInFirstSlot = make([]solana.PublicKey, 0)
		}

		// workaround for skipping the soon-to-be obsolete EAH
		if block.Slot == partitionedRewardsInfo.EahStopOffsetSlot {
			if replayCtx.HasEpochAcctsHash {
				block.EpochAcctsHash = replayCtx.EpochAcctsHash
			} else {
				block.EahWorkaroundBankhash, err = fetchBankhashForSlot(rpcc, block.Slot)
				if err != nil {
					panic(fmt.Sprintf("unable to fetch bankhash for EAH workaround for slot %d", block.Slot))
				}
				block.HasEahWorkaround = true
			}
		}

		lastSlotCtx, err = ProcessBlock(acctsDb, block, updateAcctsDb)
		if err != nil {
			mlog.Log.Errorf("error encountered during block replay: %s\n", err)
			break
		} else {
			mlog.Log.Debugf("block replayed successfully.\n")
		}
		replayCtx.Capitalization -= lastSlotCtx.LamportsBurnt

		slotReplayDuration := time.Since(start)
		mlog.Log.Infof("replayed slot %d - bankhash: %s  (slot replay time: %fs)", block.Slot, base58.Encode(lastSlotCtx.FinalBankhash), slotReplayDuration.Seconds())
		statsd.Count("slot_replays", 1, nil, 1)
		statsd.Distribution("slot_replay_duration_ms.distribution", float64(slotReplayDuration.Nanoseconds())/1e6, nil, 1)
		statsd.Gauge("epoch", float64(block.Epoch), nil, 1)
		statsd.Gauge("slot", float64(block.Slot), nil, 1)
		statsd.Distribution("txs_per_block", float64(len(block.Transactions)), nil, 1)
		if !justCrossedEpochBoundary {
			statsCounter++
			timeAccumulator += slotReplayDuration.Seconds()

			if statsCounter == 100 {
				mlog.Log.Infof("(average slot replay time over 100 slots: %f)\n", timeAccumulator/float64(statsCounter))
				timeAccumulator = 0
				statsCounter = 0
			}
		} else {
			justCrossedEpochBoundary = false
		}
	}

	return nil
}

func runIncinerator(slotCtx *sealevel.SlotCtx) {
	incineratorAcct, err := slotCtx.GetAccount(sealevel.IncineratorAddr)
	if err != nil {
		return
	}
	newIncineratorAcct := &accounts.Account{Key: sealevel.IncineratorAddr, Owner: sealevel.SystemProgramAddr, RentEpoch: math.MaxUint64}
	slotCtx.SetAccount(sealevel.IncineratorAddr, newIncineratorAcct)
	slotCtx.LamportsBurnt += incineratorAcct.Lamports
}

func compileWritableAndModifiedAccts(slotCtx *sealevel.SlotCtx, block *Block, rentAccts []*accounts.Account) ([]*accounts.Account, []*accounts.Account) {
	writableAccts := make([]*accounts.Account, 0, len(slotCtx.WritableAccts)+len(block.UpdatedAccts)+len(rentAccts)+4)
	modifiedAccts := make([]*accounts.Account, 0, len(slotCtx.ModifiedAccts)+len(block.UpdatedAccts)+len(rentAccts)+4)

	for pk := range slotCtx.WritableAccts {
		acct, _ := slotCtx.GetAccount(pk)
		writableAccts = append(writableAccts, acct)
	}

	for pk := range slotCtx.ModifiedAccts {
		acct, _ := slotCtx.GetAccount(pk)
		modifiedAccts = append(modifiedAccts, acct)
	}

	for _, pk := range block.UpdatedAccts {
		//mlog.Log.Debugf("adding updated acct for bankhash: %s", pk)
		acct, err := slotCtx.GetAccount(pk)
		if err != nil {
			panic(fmt.Sprintf("unable to fetch %s from accountsdb for inclusion in bankhash", pk))
		}
		writableAccts = append(writableAccts, acct)
		modifiedAccts = append(modifiedAccts, acct)
	}

	writableAccts = append(writableAccts, rentAccts...)
	modifiedAccts = append(modifiedAccts, rentAccts...)

	sysvarAccts := collectAndUpdateSysvarAcctsForAdh(slotCtx)
	writableAccts = append(writableAccts, sysvarAccts...)
	modifiedAccts = append(modifiedAccts, sysvarAccts...)

	return writableAccts, modifiedAccts
}

func newSlotCtx(block *Block, accts accounts.Accounts, acctsDb *accountsdb.AccountsDb) *sealevel.SlotCtx {
	slotCtx := &sealevel.SlotCtx{Slot: block.Slot, Epoch: block.Epoch, ParentSlot: block.ParentSlot,
		Blockhash: block.Blockhash, LastBlockhash: block.LastBlockhash, Accounts: accts,
		AccountsDb: acctsDb, Replay: true, Features: block.Features, StakeAccts: block.StakeAccts,
		VoteAccts: block.VoteAccts, VoteTimestamps: block.VoteTimestamps, TotalEpochStake: block.TotalEpochStake,
		EpochsAcctHash: block.EpochAcctsHash, EahWorkaroundBankhash: block.EahWorkaroundBankhash,
		HasEahWorkaround: block.HasEahWorkaround}

	slotCtx.ModifiedAccts = make(map[solana.PublicKey]bool)
	slotCtx.WritableAccts = make(map[solana.PublicKey]bool)

	return slotCtx
}

func ProcessBlock(acctsDb *accountsdb.AccountsDb, block *Block, updateAcctsDb bool) (*sealevel.SlotCtx, error) {
	mlog.Log.Debugf("replaying slot %d, epoch %d", block.Slot, block.Epoch)

	// gather up all accounts referenced in the block
	accts, err := loadBlockAccountsAndUpdateSysvars(acctsDb, block)
	if err != nil {
		panic(fmt.Sprintf("unable to load slot accounts and update sysvars: %s", err))
	}

	slotCtx := newSlotCtx(block, accts, acctsDb)
	var txFeeAccumulator fees.TxFeeInfoAccumulator

	// process & execute each transaction in turn
	for idx, tx := range block.Transactions {
		mlog.Log.Debugf("[+] executing transaction %d (slot %d, epoch %d), %s", idx+1, block.Slot, block.Epoch, tx.Signatures[0])
		txFeeInfo, txErr := ProcessTransaction(slotCtx, tx, block.TxMetas[idx])
		if txErr != nil {
			mlog.Log.Debugf("tx %d returned error: %s\n", idx+1, txErr)
		}

		// check for success-failure return value divergences
		if txErr == nil && block.TxMetas[idx].Err != nil {
			mlog.Log.Infof("tx %s return value divergence: txErr was nil, but onchain err was %+v", tx.Signatures[0], block.TxMetas[idx].Err)
		} else if txErr != nil && block.TxMetas[idx].Err == nil {
			mlog.Log.Infof("tx %s return value divergence: txErr was %+v, but onchain err was nil", tx.Signatures[0], txErr)
		}

		txFeeAccumulator.Add(txFeeInfo)
	}

	if block.BlockReward != nil {
		// distribute tx fees to the slot leader
		slotCtx.LamportsBurnt = fees.DistributeTxFeesToSlotLeader(acctsDb, slotCtx, block.BlockReward.Leader, &txFeeAccumulator)
		slotCtx.RecordModifiedAcct(block.BlockReward.Leader)
		mlog.Log.Debugf("from RPC fees for leader: %d, post-balance: %d (%s)", block.BlockReward.Lamports, block.BlockReward.PostBalance, block.BlockReward.Leader)
	}

	epochSchedule := sealevel.SysvarCache.EpochSchedule.Sysvar
	rentSysvar := sealevel.SysvarCache.Rent.Sysvar
	rentAccts := rent.CollectRentEagerly(slotCtx, rentSysvar, epochSchedule)

	runIncinerator(slotCtx)

	writableAccts, modifiedAccts := compileWritableAndModifiedAccts(slotCtx, block, rentAccts)
	if len(modifiedAccts) > 0 && updateAcctsDb {
		mlog.Log.Debugf("updating accountsdb")
		err = acctsDb.StoreAccounts(modifiedAccts, slotCtx.Slot)
	} else {
		mlog.Log.Debugf("accountsdb not updated")
	}

	mlog.Log.Debugf("\ncalculating accts delta hash for %d eligible accounts. len of rentAccts = %d", len(writableAccts), len(rentAccts))

	// EAH workaround
	if slotCtx.HasEahWorkaround {
		slotCtx.FinalBankhash = slotCtx.EahWorkaroundBankhash
		return slotCtx, err
	}

	// calculate ADH and bankhash
	acctDeltaHash := calculateAcctsDeltaHash(writableAccts)
	slotCtx.FinalBankhash = calculateBankHash(slotCtx, acctDeltaHash, block.ParentBankhash, block.NumSignatures, block.Blockhash)

	return slotCtx, err
}
