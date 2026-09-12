package replay

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/rent"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// CommitLeaderInput contains the state required to freeze a locally forged bank.
// CommitLeaderSlot is in-memory only: it mutates the producer SlotCtx and
// computes the footer bank hash. Replay adopts that SlotCtx; it does not
// re-execute the block. Durable promotion still goes through the fork-aware
// WorkingSet after the slot is observed.
type CommitLeaderInput struct {
	AcctsDb                 *accountsdb.AccountsDb
	SlotCtx                 *sealevel.SlotCtx
	Block                   *b.Block
	TxFeeAccumulator        fees.TxFeeInfoAccumulator
	AlpenglowClock          bool
	AlpenglowShredVersion   uint16
	FooterTimestamp         int64
	FooterProducerTimeNanos uint64
}

// PrepareLeaderSlotSysvars derives the child bank's dynamic sysvars from the
// immutable parent-bank snapshot installed by NewLeaderSlotCtx.  It never
// consults the mutable process-global cache or unrooted account tail.
func PrepareLeaderSlotSysvars(slotCtx *sealevel.SlotCtx, block *b.Block, alpenglowClock bool) error {
	if slotCtx == nil || block == nil {
		return fmt.Errorf("missing leader slot preparation input")
	}
	if slotCtx.ParentAccts == nil {
		slotCtx.ParentAccts = accounts.NewMemAccounts()
	}

	bankSysvars := slotCtx.BankSysvars()
	if bankSysvars == nil {
		return fmt.Errorf("leader slot %d has no pinned bank sysvar snapshot", slotCtx.Slot)
	}
	bankEpochSchedule, ok := bankSysvars.EpochSchedule()
	if !ok {
		return fmt.Errorf("leader parent snapshot has no EpochSchedule sysvar")
	}
	epochSchedule := &bankEpochSchedule
	clock, ok := bankSysvars.Clock()
	if !ok {
		return fmt.Errorf("leader parent snapshot has no Clock sysvar")
	}
	clockAcct, ok := bankSysvars.CloneAccount(sealevel.SysvarClockAddr)
	if !ok {
		return fmt.Errorf("leader parent snapshot has no Clock account")
	}
	if err := updateClockSysvarForMode(&clock, block, epochSchedule, alpenglowClock); err != nil {
		return err
	}
	clockAcct.Data = clock.MustMarshal()
	if err := slotCtx.SetAccount(clockAcct.Key, clockAcct); err != nil {
		return err
	}

	slotHashes, ok := bankSysvars.SlotHashes()
	if !ok {
		return fmt.Errorf("leader parent snapshot has no SlotHashes sysvar")
	}
	slotHashes = append(sealevel.SysvarSlotHashes(nil), slotHashes...)
	slotHashesAcct, ok := bankSysvars.CloneAccount(sealevel.SysvarSlotHashesAddr)
	if !ok {
		return fmt.Errorf("leader parent snapshot has no SlotHashes account")
	}
	slotHashes.Update(block.Slot, block.ParentSlot, block.ParentBankhash)
	slotHashesAcct.Data = slotHashes.MustMarshal()
	if err := slotCtx.SetAccount(slotHashesAcct.Key, slotHashesAcct); err != nil {
		return err
	}
	bankSysvars, err := bankSysvars.WithAccounts(clockAcct, slotHashesAcct)
	if err != nil {
		return fmt.Errorf("derive leader bank sysvars: %w", err)
	}
	if err := slotCtx.PublishBankSysvars(bankSysvars); err != nil {
		return err
	}
	return nil
}

// CommitLeaderSlot freezes a forged bank and computes the footer bank hash. It
// does not write AccountsDB, update global replay progress, or bypass forkchoice.
// Replay later adopts the returned SlotCtx instead of running ProcessBlock.
func CommitLeaderSlot(in CommitLeaderInput) (*sealevel.SlotCtx, error) {
	if in.AcctsDb == nil || in.SlotCtx == nil || in.Block == nil {
		return nil, fmt.Errorf("missing leader finalization input")
	}
	slotCtx, block := in.SlotCtx, in.Block
	bankSysvars := slotCtx.BankSysvars()
	if bankSysvars == nil {
		return nil, fmt.Errorf("leader slot %d has no bank sysvar snapshot", slotCtx.Slot)
	}
	bankEpochSchedule, ok := bankSysvars.EpochSchedule()
	if !ok {
		return nil, fmt.Errorf("leader slot %d has no bank-local EpochSchedule sysvar", slotCtx.Slot)
	}
	epochSchedule := &bankEpochSchedule
	block.FooterProducerTimeNanos = in.FooterProducerTimeNanos
	block.UnixTimestamp = in.FooterTimestamp
	slotCtx.Blockhash = block.Blockhash
	slotCtx.Epoch = block.Epoch
	slotCtx.FeeRateGovernor = block.FeeRateGovernor
	slotCtx.NumSignatures = block.NumSignatures

	preparedClock, hasPreparedClock := slotCtx.BankSysvars().Clock()
	if !hasPreparedClock || preparedClock.Slot != block.Slot {
		if err := PrepareLeaderSlotSysvars(slotCtx, block, in.AlpenglowClock); err != nil {
			return nil, err
		}
	}
	if in.AlpenglowClock {
		if err := validateAlpenglowFooterNanosecondClock(slotCtx, block); err != nil {
			return nil, err
		}
		// This is a speculative producer bank until the forged block is adopted
		// in slot order. Keep its Clock slot-local: publishing it to the
		// global replay cache here would replace the true parent Clock before
		// the next network block loads and would produce a different LtHash.
		if err := applyAlpenglowFooterClockLocal(slotCtx, block, epochSchedule); err != nil {
			return nil, err
		}
		if err := updateAlpenglowNanosecondClockAccount(slotCtx, block); err != nil {
			return nil, err
		}
		if err := ApplyAlpenglowVoteRewards(slotCtx, block, epochSchedule, block.SkipRewardCert, block.NotarRewardCert, block.BlockFinalCert, in.AlpenglowShredVersion); err != nil {
			return nil, err
		}
	}

	if len(block.Transactions) > 0 {
		slotCtx.LamportsBurnt = fees.DistributeTxFeesToSlotLeader(in.AcctsDb, slotCtx, block.Leader, &in.TxFeeAccumulator)
		slotCtx.RecordModifiedAcct(block.Leader)
	}
	var rentSysvar *sealevel.SysvarRent
	if bankSysvars := slotCtx.BankSysvars(); bankSysvars != nil {
		if bankRent, ok := bankSysvars.Rent(); ok {
			rentSysvar = &bankRent
		}
	}
	if rentSysvar == nil {
		return nil, fmt.Errorf("leader slot %d has no bank-local Rent sysvar", slotCtx.Slot)
	}
	rentAccts := rent.CollectRentEagerly(slotCtx, rentSysvar, epochSchedule)
	runIncinerator(slotCtx)
	if err := finishLeaderSysvars(slotCtx, block); err != nil {
		return nil, err
	}
	writable, modified := compileLeaderAccounts(slotCtx, block, rentAccts)
	if err := ensureParentAccountsForModified(slotCtx, modified); err != nil {
		return nil, err
	}
	slotCtx.FinalBankhash = bankhash.CalculateBankHash(slotCtx, writable, modified, block.ParentBankhash, block.NumSignatures, block.Blockhash)
	copy(block.ExpectedBankhash[:], slotCtx.FinalBankhash)
	block.HasExpectedBankhash = true
	return slotCtx, nil
}

func finishLeaderSysvars(slotCtx *sealevel.SlotCtx, block *b.Block) error {
	if err := finalizeBankSysvars(slotCtx); err != nil {
		return err
	}
	slotCtx.RecordModifiedAcct(sealevel.SysvarRecentBlockHashesAddr)
	slotCtx.RecordModifiedAcct(sealevel.SysvarSlotHistoryAddr)
	slotCtx.RecordModifiedAcct(sealevel.SysvarClockAddr)
	slotCtx.RecordModifiedAcct(sealevel.SysvarSlotHashesAddr)
	return nil
}

func ensureParentAccountsForModified(slotCtx *sealevel.SlotCtx, modified []*accounts.Account) error {
	if slotCtx.Features == nil || !slotCtx.Features.IsActive(features.AccountsLtHash) {
		return nil
	}
	for _, modifiedAcct := range modified {
		if modifiedAcct == nil {
			continue
		}
		key := modifiedAcct.Key
		if _, err := slotCtx.GetParentAccount(key); err == nil {
			continue
		}
		acct, err := slotCtx.GetAccountFromAccountsDb(key)
		if err != nil {
			acct = &accounts.Account{Key: key}
		}
		if err := slotCtx.ParentAccts.SetAccountWithoutLock(key, acct.Clone()); err != nil {
			return err
		}
	}
	return nil
}

func compileLeaderAccounts(slotCtx *sealevel.SlotCtx, block *b.Block, rentAccts []*accounts.Account) ([]*accounts.Account, []*accounts.Account) {
	adhRemoved := accountsDeltaHashRemoved(slotCtx)
	var writable []*accounts.Account
	var seenWritable map[solana.PublicKey]struct{}
	if !adhRemoved {
		writable = make([]*accounts.Account, 0, len(slotCtx.WritableAccts)+len(rentAccts)+8)
		seenWritable = make(map[solana.PublicKey]struct{})
	}
	modified := make([]*accounts.Account, 0, len(slotCtx.ModifiedAccts)+len(rentAccts)+8)
	seenModified := make(map[solana.PublicKey]struct{})
	addWritable := func(acct *accounts.Account) {
		if adhRemoved || acct == nil {
			return
		}
		if _, ok := seenWritable[acct.Key]; ok {
			return
		}
		seenWritable[acct.Key] = struct{}{}
		writable = append(writable, acct)
	}
	addModified := func(acct *accounts.Account) {
		if acct == nil {
			return
		}
		if _, ok := seenModified[acct.Key]; ok {
			return
		}
		seenModified[acct.Key] = struct{}{}
		modified = append(modified, acct)
	}
	if !adhRemoved {
		for key := range slotCtx.WritableAccts {
			acct, _ := slotCtx.GetAccount(key)
			addWritable(acct)
		}
	}
	for key := range slotCtx.ModifiedAccts {
		acct, _ := slotCtx.GetAccount(key)
		addModified(acct)
		if !adhRemoved {
			addWritable(acct)
		}
	}
	for _, acct := range block.EpochUpdatedAccts {
		if acct == nil {
			continue
		}
		current, err := slotCtx.GetAccount(acct.Key)
		if err != nil {
			current = acct
		}
		if !adhRemoved {
			addWritable(current)
		}
		addModified(current)
	}
	for _, acct := range rentAccts {
		if !adhRemoved {
			addWritable(acct)
		}
		addModified(acct)
	}
	return writable, modified
}
