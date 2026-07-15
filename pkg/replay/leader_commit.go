package replay

import (
	"fmt"
	"path/filepath"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rent"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// CommitLeaderInput finalizes a locally forged leader slot into AccountsDB.
type CommitLeaderInput struct {
	AcctsDb                 *accountsdb.AccountsDb
	SlotCtx                 *sealevel.SlotCtx
	Block                   *b.Block
	EpochSchedule           *sealevel.SysvarEpochSchedule
	TxFeeAccumulator        fees.TxFeeInfoAccumulator
	AlpenglowClock          bool
	AlpenglowShredVersion   uint16
	FooterTimestamp         int64
	FooterProducerTimeNanos uint64
}

// PreparedLeaderCommit is a fully executed local slot that has not yet been
// published into replay. Publication waits until its footer/ending tick was
// successfully broadcast and therefore its Alpenglow block ID is known.
type PreparedLeaderCommit struct {
	AcctsDb        *accountsdb.AccountsDb
	SlotCtx        *sealevel.SlotCtx
	Block          *b.Block
	ModifiedAccts  []*accounts.Account
	ReplaySnapshot *ReplayHeadSnapshot
}

// CommitLeaderSlot completes forged leader execution without making the state
// durable or visible to replay yet.
func CommitLeaderSlot(in CommitLeaderInput) (*PreparedLeaderCommit, error) {
	if in.AcctsDb == nil || in.SlotCtx == nil || in.Block == nil || in.EpochSchedule == nil {
		return nil, fmt.Errorf("missing commit input")
	}

	slotCtx := in.SlotCtx
	block := in.Block
	slotCtx.Epoch = block.Epoch
	slotCtx.Blockhash = block.Blockhash
	// Propagate the derived fee rate governor onto the committed slot context so the
	// RecentBlockhashes push uses the correct lamports_per_signature and the chain tip
	// carries it forward to the next (possibly consecutive leader) slot.
	if block.FeeRateGovernor != nil {
		slotCtx.FeeRateGovernor = block.FeeRateGovernor
	}
	if in.FooterProducerTimeNanos > 0 {
		block.FooterProducerTimeNanos = in.FooterProducerTimeNanos
	}

	if err := updateLeaderSysvars(in.AcctsDb, slotCtx, block, in.EpochSchedule, in.AlpenglowClock, in.FooterTimestamp); err != nil {
		return nil, err
	}

	if in.AlpenglowClock {
		if err := ApplyAlpenglowVoteRewards(in.AcctsDb, nil, slotCtx, block, in.EpochSchedule, block.SkipRewardCert, block.NotarRewardCert, block.BlockFinalCert, in.AlpenglowShredVersion); err != nil {
			return nil, err
		}
	}

	if len(block.Transactions) > 0 {
		loadParent := func(pk solana.PublicKey) (*accounts.Account, error) {
			return slotCtx.GetAccountFromAccountsDb(pk)
		}
		feeDist := fees.DistributeTxFeesToSlotLeader(in.AcctsDb, slotCtx, block.Leader, &in.TxFeeAccumulator, loadParent)
		slotCtx.LamportsBurnt = feeDist.LamportsBurnt
		if !feeDist.FeeCollector.IsZero() {
			slotCtx.RecordModifiedAcct(feeDist.FeeCollector)
		}
	}

	rentSysvar := sealevel.SysvarCache.Rent.Sysvar
	rentAccts := rent.CollectRentEagerly(slotCtx, rentSysvar, in.EpochSchedule)
	runIncinerator(slotCtx)

	writableAccts, modifiedAccts := compileWritableAndModifiedAccts(slotCtx, block, rentAccts, true)
	if err := ensureParentAcctsForModified(in.AcctsDb, slotCtx, nil); err != nil {
		return nil, err
	}
	slotCtx.FinalBankhash = bankhash.CalculateBankHash(slotCtx, writableAccts, modifiedAccts, block.ParentBankhash, block.NumSignatures, block.Blockhash)

	if in.AlpenglowClock {
		if err := verifyAlpenglowBlockFooter(slotCtx, block, in.AlpenglowClock); err != nil {
			return nil, err
		}
	}

	accountsDbDir := filepath.Join(in.AcctsDb.AcctsDir, "..")
	detailsInput := bankhash.SlotDetailsFromLeaderCommit(
		slotCtx,
		block.ParentBankhash,
		block.Blockhash,
		modifiedAccts,
		footerProducerTimeNanosPtr(in.FooterProducerTimeNanos),
		footerTimestampPtr(in.FooterTimestamp),
	)
	if err := bankhash.WriteLeaderBankHashDetails(accountsDbDir, detailsInput); err != nil {
		mlog.Log.Warnf("leader slot %d bank hash details: %v", slotCtx.Slot, err)
	}

	slotCtx.Blockhash = block.Blockhash
	slotCtx.NumSignatures = block.NumSignatures
	return &PreparedLeaderCommit{
		AcctsDb:       in.AcctsDb,
		SlotCtx:       slotCtx,
		Block:         block,
		ModifiedAccts: modifiedAccts,
		ReplaySnapshot: captureSlotStateSnapshot(
			slotCtx,
			block.BlockHeight,
			global.TransactionCount()+uint64(len(block.Transactions)),
		),
	}, nil
}

// FinalizeLeaderCommit publishes a locally produced block into the same
// speculative/finality path as a turbine-received block. Legacy modes retain
// the direct StoreAccounts behavior.
func FinalizeLeaderCommit(prepared *PreparedLeaderCommit, blockID solana.Hash) error {
	if prepared == nil || prepared.AcctsDb == nil || prepared.SlotCtx == nil || prepared.Block == nil {
		return fmt.Errorf("incomplete prepared leader commit")
	}
	slotCtx := prepared.SlotCtx
	block := prepared.Block
	block.HasAlpenglowBlockID = blockID != (solana.Hash{})
	block.AlpenglowBlockID = blockID
	chainedRoot, hasChainedRoot := global.AlpenglowChainedMerkleRoot(block.Slot)
	block.HasAlpenglowLastChainedRoot = hasChainedRoot
	block.AlpenglowLastChainedRoot = chainedRoot

	if prepared.AcctsDb.RootedDurable {
		if !block.HasAlpenglowBlockID {
			return fmt.Errorf("leader slot %d has no Alpenglow block ID", block.Slot)
		}
		if !block.HasAlpenglowLastChainedRoot {
			return fmt.Errorf("leader slot %d has no Alpenglow chained root", block.Slot)
		}
		if prepared.ReplaySnapshot == nil {
			return fmt.Errorf("leader slot %d has no captured replay snapshot", block.Slot)
		}
		prepared.ReplaySnapshot.HasAlpenglowIdentity = true
		prepared.ReplaySnapshot.AlpenglowBlockID = blockID
		prepared.ReplaySnapshot.AlpenglowChainedRoot = chainedRoot
		if err := stageActiveSpeculativeCommit(&DeferredBlockCommit{
			SlotCtx:                 slotCtx,
			ModifiedAccts:           prepared.ModifiedAccts,
			BlockSlot:               block.Slot,
			BlockHeight:             block.BlockHeight,
			Bankhash:                append([]byte(nil), slotCtx.FinalBankhash...),
			HasAlpenglowBlockID:     true,
			AlpenglowBlockID:        blockID,
			HasAlpenglowChainedRoot: true,
			AlpenglowChainedRoot:    chainedRoot,
			AwaitingReplaySnapshot:  true,
			CapturedSnapshot:        prepared.ReplaySnapshot,
		}); err != nil {
			return fmt.Errorf("stage local leader slot %d: %w", block.Slot, err)
		}
	} else {
		commitSlot.Store(slotCtx.Slot)
		commitInProgress.Store(true)
		persistedBankhash := append([]byte(nil), slotCtx.FinalBankhash...)
		stakeIndexDir := filepath.Join(prepared.AcctsDb.AcctsDir, "..")
		afterStoreAccounts := func() {
			if err := prepared.AcctsDb.StoreBankHashForSlot(slotCtx.Slot, persistedBankhash); err != nil {
				mlog.Log.Infof("unable to store bankhash for leader slot %d: %v", slotCtx.Slot, err)
			}
			if flushed, err := global.FlushPendingStakePubkeys(stakeIndexDir); err != nil {
				mlog.Log.Errorf("failed to flush stake pubkey index: %v", err)
			} else if flushed > 0 {
				mlog.Log.Debugf("flushed %d new stake pubkeys to index", flushed)
			}
			commitInProgress.Store(false)
			commitSlot.Store(0)
		}
		if len(prepared.ModifiedAccts) > 0 {
			if err := prepared.AcctsDb.StoreAccounts(prepared.ModifiedAccts, slotCtx.Slot, afterStoreAccounts); err != nil {
				commitInProgress.Store(false)
				commitSlot.Store(0)
				return err
			}
		} else {
			afterStoreAccounts()
		}
	}

	global.IncrTransactionCount(uint64(len(block.Transactions)))
	global.SetSlot(block.Slot)
	global.SetEpoch(block.Epoch)
	global.SetLatestBlockHash(block.Blockhash)
	global.SetBlockHeight(block.BlockHeight)
	RegisterLocalLeaderCommitDetails(LocalLeaderCommit{
		SlotCtx:       slotCtx,
		Block:         block,
		ModifiedAccts: prepared.ModifiedAccts,
	})
	return nil
}

func footerProducerTimeNanosPtr(nanos uint64) *uint64 {
	if nanos == 0 {
		return nil
	}
	return &nanos
}

func footerTimestampPtr(ts int64) *int64 {
	if ts == 0 {
		return nil
	}
	return &ts
}

func applyAlpenglowFooterClock(slotCtx *sealevel.SlotCtx, block *b.Block, epochSchedule *sealevel.SysvarEpochSchedule) error {
	footerUnixTimestamp, ok, err := alpenglowFooterUnixTimestamp(block)
	if err != nil {
		return fmt.Errorf("slot %d alpenglow footer clock: %w", block.Slot, err)
	}
	if !ok {
		return nil
	}

	clockAcct, err := slotCtx.GetAccount(sealevel.SysvarClockAddr)
	if err != nil {
		return fmt.Errorf("unable to get clock sysvar for Alpenglow footer update: %w", err)
	}

	var clock sealevel.SysvarClock
	if err := clock.UnmarshalWithDecoder(bin.NewBinDecoder(clockAcct.Data)); err != nil {
		return fmt.Errorf("unable to unmarshal clock sysvar for Alpenglow footer update: %w", err)
	}
	footerBlock := *block
	footerBlock.UnixTimestamp = footerUnixTimestamp
	if err := updateClockSysvarFromAlpenglowFooter(&clock, &footerBlock, epochSchedule); err != nil {
		return err
	}

	copy(clockAcct.Data, clock.MustMarshal())
	if err := slotCtx.SetAccount(sealevel.SysvarClockAddr, clockAcct); err != nil {
		return err
	}
	slotCtx.RecordModifiedAcct(sealevel.SysvarClockAddr)
	sealevel.SysvarCache.Clock.Sysvar = &clock
	sealevel.SysvarCache.Clock.Acct = clockAcct
	if err := updateAlpenglowNanosecondClockAccount(slotCtx, block); err != nil {
		return err
	}
	return nil
}

func updateLeaderSysvars(acctsDb *accountsdb.AccountsDb, slotCtx *sealevel.SlotCtx, block *b.Block, epochSchedule *sealevel.SysvarEpochSchedule, alpenglowClock bool, footerTimestamp int64) error {
	clockAcct, err := slotCtx.GetAccountFromAccountsDb(sealevel.SysvarClockAddr)
	if err != nil {
		return fmt.Errorf("load clock sysvar: %w", err)
	}
	// AccountsDb may hand back a pointer shared with its internal caches; clone before
	// mutating in place so the cache (and thus the parent lt-hash baseline read later by
	// ensureParentAcctsForModified) keeps the true pre-update parent state.
	clockAcct = clockAcct.Clone()
	var clock sealevel.SysvarClock
	if err := clock.UnmarshalWithDecoder(bin.NewBinDecoder(clockAcct.Data)); err != nil {
		return fmt.Errorf("decode clock sysvar: %w", err)
	}
	if err := updateClockSysvarForMode(&clock, block, epochSchedule, alpenglowClock); err != nil {
		return err
	}
	if alpenglowClock && footerTimestamp > 0 {
		block.UnixTimestamp = footerTimestamp
		if err := updateClockSysvarFromAlpenglowFooter(&clock, block, epochSchedule); err != nil {
			return err
		}
		if err := updateAlpenglowNanosecondClockAccount(slotCtx, block); err != nil {
			return err
		}
	}
	copy(clockAcct.Data, clock.MustMarshal())
	if err := slotCtx.SetAccount(sealevel.SysvarClockAddr, clockAcct); err != nil {
		return err
	}
	slotCtx.RecordModifiedAcct(sealevel.SysvarClockAddr)
	sealevel.SysvarCache.Clock.Sysvar = &clock
	sealevel.SysvarCache.Clock.Acct = clockAcct

	slotHashesAcct, err := slotCtx.GetAccountFromAccountsDb(sealevel.SysvarSlotHashesAddr)
	if err != nil {
		return fmt.Errorf("load slothashes sysvar: %w", err)
	}
	slotHashesAcct = slotHashesAcct.Clone()
	var slotHashes sealevel.SysvarSlotHashes
	if sealevel.SysvarCache.SlotHashes.Sysvar != nil {
		slotHashes = *sealevel.SysvarCache.SlotHashes.Sysvar
	} else if err := slotHashes.UnmarshalWithDecoder(bin.NewBinDecoder(slotHashesAcct.Data)); err != nil {
		return fmt.Errorf("decode slothashes sysvar: %w", err)
	}
	slotHashes.Update(block.Slot, block.ParentSlot, block.ParentBankhash)
	copy(slotHashesAcct.Data, slotHashes.MustMarshal())
	if err := slotCtx.SetAccount(sealevel.SysvarSlotHashesAddr, slotHashesAcct); err != nil {
		return err
	}
	slotCtx.RecordModifiedAcct(sealevel.SysvarSlotHashesAddr)
	sealevel.SysvarCache.SlotHashes.Sysvar = &slotHashes
	sealevel.SysvarCache.SlotHashes.Acct = slotHashesAcct

	if block.Blockhash == ([32]byte{}) {
		return fmt.Errorf("leader commit: entry blockhash missing for slot %d", block.Slot)
	}

	recentAcct, err := slotCtx.GetAccountFromAccountsDb(sealevel.SysvarRecentBlockHashesAddr)
	if err != nil {
		return fmt.Errorf("load recent blockhashes sysvar: %w", err)
	}
	recentAcct = recentAcct.Clone()
	recent, err := cloneRecentBlockhashesFromCache()
	if err != nil {
		return fmt.Errorf("RecentBlockhashes cache for slot %d: %w", block.Slot, err)
	}
	slotCtx.LatestEvictedBlockhash = recent.PushLatest(block.Blockhash, block.FeeRateGovernor.LamportsPerSignature)
	copy(recentAcct.Data, recent.MustMarshal())
	if err := slotCtx.SetAccount(sealevel.SysvarRecentBlockHashesAddr, recentAcct); err != nil {
		return err
	}
	slotCtx.RecordModifiedAcct(sealevel.SysvarRecentBlockHashesAddr)
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &recent
	sealevel.SysvarCache.RecentBlockHashes.Acct = recentAcct

	slotHistoryAcct, err := slotCtx.GetAccountFromAccountsDb(sealevel.SysvarSlotHistoryAddr)
	if err != nil {
		return fmt.Errorf("load slot history sysvar: %w", err)
	}
	slotHistoryAcct = slotHistoryAcct.Clone()
	var slotHistory sealevel.SysvarSlotHistory
	if sealevel.SysvarCache.SlotHistory.Sysvar != nil {
		slotHistory = *sealevel.SysvarCache.SlotHistory.Sysvar
	} else if err := slotHistory.UnmarshalWithDecoder(bin.NewBinDecoder(slotHistoryAcct.Data)); err != nil {
		return fmt.Errorf("decode slot history sysvar: %w", err)
	}
	slotHistory.Add(block.Slot)
	slotHistory.SetNextSlot(block.Slot + 1)
	copy(slotHistoryAcct.Data, slotHistory.MustMarshal())
	if err := slotCtx.SetAccount(sealevel.SysvarSlotHistoryAddr, slotHistoryAcct); err != nil {
		return err
	}
	slotCtx.RecordModifiedAcct(sealevel.SysvarSlotHistoryAddr)
	sealevel.SysvarCache.SlotHistory.Sysvar = &slotHistory
	sealevel.SysvarCache.SlotHistory.Acct = slotHistoryAcct

	return nil
}

func ensureLeaderParentAccts(acctsDb *accountsdb.AccountsDb, slotCtx *sealevel.SlotCtx) error {
	return ensureParentAcctsForModified(acctsDb, slotCtx, nil)
}

func ensureParentAcctsForModified(acctsDb *accountsdb.AccountsDb, slotCtx *sealevel.SlotCtx, spec *SpeculativeReplay) error {
	if slotCtx.Features == nil || !slotCtx.Features.IsActive(features.AccountsLtHash) {
		return nil
	}

	slotCtx.AcctMapsMu.Lock()
	modified := make([]solana.PublicKey, 0, len(slotCtx.ModifiedAccts))
	for pk := range slotCtx.ModifiedAccts {
		modified = append(modified, pk)
	}
	slotCtx.AcctMapsMu.Unlock()
	if len(modified) == 0 {
		return nil
	}

	if slotCtx.ParentAccts == nil {
		slotCtx.ParentAccts = accounts.NewMemAccountsWithLen(uint64(len(modified)))
	}

	for _, pk := range modified {
		if slotCtx.LtHashAlreadyApplied(pk) {
			continue
		}
		if _, err := slotCtx.GetParentAccount(pk); err == nil {
			continue
		}
		var acct *accounts.Account
		var err error
		if spec != nil {
			acct, err = loadAccountForBlockReplay(acctsDb, spec, slotCtx.ParentSlot, slotCtx.Slot, pk)
		} else {
			acct, err = slotCtx.GetAccountFromAccountsDb(pk)
		}
		if err != nil {
			return fmt.Errorf("load parent acct %s for slot %d: %w", pk, slotCtx.Slot, err)
		}
		key := [32]byte(pk)
		if err := slotCtx.ParentAccts.SetAccountWithoutLock(key, acct.Clone()); err != nil {
			return err
		}
	}
	return nil
}
