package blockprod

import (
	"fmt"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// ParentContext carries the parent bank metadata needed to commit a leader slot.
type ParentContext struct {
	// ReplayGeneration binds every field below to one atomic replay-tip
	// publication. It changes on replay advance, reset, and fork switch.
	ReplayGeneration           uint64
	ParentSlot                 uint64
	ParentBankhash             solana.Hash
	ParentBlockID              solana.Hash
	HasParentBlockID           bool
	ParentChainedMerkleRoot    solana.Hash
	HasParentChainedMerkleRoot bool
	ParentLastEntryHash        solana.Hash
	ParentLastBlockhash        solana.Hash
	ParentBlockHeight          uint64
	LatestEvictedBlockhash     [32]byte
	EpochRewardsActive         bool
	PrevNumSigs                uint64 // signatures processed in the parent bank; fee-governor input only
	PrevFeeGovernor            *sealevel.FeeRateGovernor
	AcctsLtHash                *lthash.LtHash
	Features                   *features.Features
	BankSysvars                *sealevel.BankSysvars
	EpochStakes                map[solana.PublicKey]uint64 // immutable; shared by banks in one epoch
	TotalEpochStake            uint64
	NanosecondClockAccount     *accounts.Account
	HasNanosecondClockAccount  bool
	UnrootedRead               sealevel.AccountReader
	TransactionStatuses        *replay.TransactionStatusView
	GenesisParent              *replay.GenesisReplayBootstrap // only the verified slot-0 parent
	VoteTimestamps             map[solana.PublicKey]sealevel.BlockTimestamp
}

// NewLeaderSlotCtx builds a forge-ready slot context at the chain tip.
func NewLeaderSlotCtx(slot, parentSlot uint64, acctsDb *accountsdb.AccountsDb, parent ParentContext, epochSchedule *sealevel.SysvarEpochSchedule) (*sealevel.SlotCtx, error) {
	feats := leaderFeatures(parent.Features)
	var genesisSlotHashes *accounts.Account
	if parent.GenesisParent != nil {
		if parent.ParentSlot != parentSlot || !parent.HasParentBlockID || parent.ParentBlockID != (solana.Hash{}) {
			return nil, fmt.Errorf("genesis leader requires the explicit zero certificate parent")
		}
		var err error
		genesisSlotHashes, err = parent.GenesisParent.FirstChildSlotHashes(slot, parentSlot, parent.ParentBankhash, parent.BankSysvars)
		if err != nil {
			return nil, err
		}
	}

	if parent.BankSysvars != nil {
		if bankEpochSchedule, ok := parent.BankSysvars.EpochSchedule(); ok {
			epochSchedule = &bankEpochSchedule
		} else {
			return nil, fmt.Errorf("parent bank snapshot has no EpochSchedule sysvar")
		}
	}
	var epoch uint64
	if epochSchedule != nil {
		epoch = epochSchedule.GetEpoch(slot)
	}

	lastBlockhash := parent.ParentLastBlockhash
	if lastBlockhash == (solana.Hash{}) {
		lastBlockhash = parent.ParentLastEntryHash
	}
	prevFee := parent.PrevFeeGovernor
	if prevFee == nil {
		prevFee = &sealevel.FeeRateGovernor{PrevLamportsPerSignature: 5000, LamportsPerSignature: 5000}
	}
	feeGovernor := sealevel.NewFeeRateGovernorDerived(prevFee, parent.PrevNumSigs)
	if feeGovernor.PrevLamportsPerSignature == 0 {
		feeGovernor.PrevLamportsPerSignature = 5000
	}

	slotCtx := &sealevel.SlotCtx{
		Slot:                   slot,
		ParentSlot:             parentSlot,
		Epoch:                  epoch,
		Accounts:               accounts.NewMemAccounts(),
		ParentAccts:            accounts.NewMemAccounts(),
		AccountsDb:             acctsDb,
		UnrootedRead:           parent.UnrootedRead,
		Features:               feats,
		FeeRateGovernor:        feeGovernor,
		LastBlockhash:          lastBlockhash,
		LatestEvictedBlockhash: parent.LatestEvictedBlockhash,
		AcctMapsMu:             &sync.Mutex{},
		ModifiedAccts:          make(map[solana.PublicKey]bool),
		WritableAccts:          make(map[solana.PublicKey]bool),
		VoteTimestampMu:        &sync.Mutex{},
		VoteTimestamps:         make(map[solana.PublicKey]sealevel.BlockTimestamp),
		// EpochStakes is immutable for the epoch and is replaced, rather than
		// mutated, at an epoch transition. Share it so leader creation does not
		// copy the validator stake map every slot.
		VoteAccts:       parent.EpochStakes,
		TotalEpochStake: parent.TotalEpochStake,
		// Signature count is bank-local. Parent.PrevNumSigs feeds the fee
		// governor above, while the new child bank starts at zero.
		NumSignatures: 0,
	}
	if parent.AcctsLtHash != nil {
		slotCtx.AcctsLtHash = parent.AcctsLtHash.Clone()
	}
	if slotCtx.VoteAccts == nil {
		slotCtx.VoteAccts = make(map[solana.PublicKey]uint64)
	}
	for key, timestamp := range parent.VoteTimestamps {
		slotCtx.VoteTimestamps[key] = timestamp
	}

	if parent.BankSysvars != nil {
		childSysvars, err := parent.BankSysvars.Derive(slot)
		if err != nil {
			return nil, fmt.Errorf("derive bank sysvars for leader slot %d: %w", slot, err)
		}
		if err := parent.BankSysvars.RangeAccountViews(func(pubkey solana.PublicKey, acct *accounts.Account) error {
			if acct == nil {
				return nil
			}
			parentAcct := acct.Clone()
			parentAcct.Key = pubkey
			if err := slotCtx.ParentAccts.SetAccountWithoutLock(pubkey, parentAcct); err != nil {
				return err
			}
			currentAcct := acct.Clone()
			currentAcct.Key = pubkey
			return slotCtx.SetAccount(pubkey, currentAcct)
		}); err != nil {
			return nil, fmt.Errorf("install parent bank sysvars for leader slot %d: %w", slot, err)
		}
		if err := sealevel.RangeBankSysvarAddresses(func(pubkey solana.PublicKey) error {
			if _, present := parent.BankSysvars.AccountView(pubkey); present {
				return nil
			}
			// Preserve known absence and prevent an ordinary account load from
			// falling through to a newer replay generation.
			tombstone := &accounts.Account{Key: pubkey}
			if err := slotCtx.ParentAccts.SetAccountWithoutLock(pubkey, tombstone.Clone()); err != nil {
				return fmt.Errorf("pin absent parent sysvar %s: %w", pubkey, err)
			}
			if err := slotCtx.SetAccount(pubkey, tombstone); err != nil {
				return fmt.Errorf("install absent current sysvar %s: %w", pubkey, err)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		if genesisSlotHashes != nil {
			if err := slotCtx.SetAccount(genesisSlotHashes.Key, genesisSlotHashes); err != nil {
				return nil, err
			}
			childSysvars, err = childSysvars.WithAccounts(genesisSlotHashes)
			if err != nil {
				return nil, err
			}
		}
		if err := slotCtx.PublishBankSysvars(childSysvars); err != nil {
			return nil, fmt.Errorf("publish bank sysvars for leader slot %d: %w", slot, err)
		}
	}

	nanoClockAddr := replay.NanosecondClockAccountAddr(parent.Features)
	if parent.HasNanosecondClockAccount {
		if parent.NanosecondClockAccount == nil {
			return nil, fmt.Errorf("parent nanosecond clock marked present without an account")
		}
		parentNanoClock := parent.NanosecondClockAccount.Clone()
		parentNanoClock.Key = nanoClockAddr
		if err := slotCtx.ParentAccts.SetAccountWithoutLock(nanoClockAddr, parentNanoClock); err != nil {
			return nil, fmt.Errorf("install parent nanosecond clock: %w", err)
		}
		currentNanoClock := parent.NanosecondClockAccount.Clone()
		currentNanoClock.Key = nanoClockAddr
		if err := slotCtx.SetAccount(nanoClockAddr, currentNanoClock); err != nil {
			return nil, fmt.Errorf("install current nanosecond clock: %w", err)
		}
	} else if parent.BankSysvars != nil {
		// BankSysvars being present makes absence explicit rather than unknown.
		// Pin the zero before-value so footer creation cannot consult a mutable
		// replay reader while calculating the child bank's LtHash delta.
		absentNanoClock := &accounts.Account{Key: nanoClockAddr}
		if err := slotCtx.ParentAccts.SetAccountWithoutLock(nanoClockAddr, absentNanoClock.Clone()); err != nil {
			return nil, fmt.Errorf("pin absent parent nanosecond clock: %w", err)
		}
		if err := slotCtx.SetAccount(nanoClockAddr, absentNanoClock); err != nil {
			return nil, fmt.Errorf("install absent current nanosecond clock: %w", err)
		}
	}

	return slotCtx, nil
}

func leaderFeatures(parent *features.Features) *features.Features {
	if parent == nil {
		return features.NewFeaturesDefault()
	}
	return parent.Clone()
}
