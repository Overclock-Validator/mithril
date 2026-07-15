package blockprod

import (
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// ParentContext carries the parent bank metadata needed to commit a leader slot.
type ParentContext struct {
	ParentBankhash      solana.Hash
	ParentLastEntryHash solana.Hash
	PrevNumSigs         uint64
	PrevFeeGovernor     *sealevel.FeeRateGovernor
	AcctsLtHash         *lthash.LtHash
	Features            *features.Features
}

// NewLeaderSlotCtx builds a forge-ready slot context at the chain tip.
func NewLeaderSlotCtx(slot, parentSlot uint64, acctsDb *accountsdb.AccountsDb, parent ParentContext, epochSchedule *sealevel.SysvarEpochSchedule) (*sealevel.SlotCtx, error) {
	feats := leaderFeatures(parent.Features)

	var epoch uint64
	if epochSchedule != nil {
		epoch = epochSchedule.GetEpoch(slot)
	}

	lastBlockhash := global.LatestBlockHash()
	prevFee := parent.PrevFeeGovernor
	if prevFee == nil {
		prevFee = &sealevel.FeeRateGovernor{PrevLamportsPerSignature: 5000, LamportsPerSignature: 5000}
	}
	feeGovernor := sealevel.NewFeeRateGovernorDerived(prevFee, parent.PrevNumSigs)
	if feeGovernor.PrevLamportsPerSignature == 0 {
		feeGovernor.PrevLamportsPerSignature = 5000
	}

	slotCtx := &sealevel.SlotCtx{
		Slot:       slot,
		ParentSlot: parentSlot,
		Epoch:      epoch,
		Accounts:   accounts.NewMemAccounts(),
		AccountsDb: acctsDb,
		AccountLoader: func(parent uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
			return replay.ResolveActiveAccount(acctsDb, parent, pubkey)
		},
		Features:        feats,
		FeeRateGovernor: feeGovernor,
		LastBlockhash:   lastBlockhash,
		AcctMapsMu:      &sync.Mutex{},
		ModifiedAccts:   make(map[solana.PublicKey]bool),
		WritableAccts:   make(map[solana.PublicKey]bool),
		NumSignatures:   parent.PrevNumSigs,
	}
	if parent.AcctsLtHash != nil {
		slotCtx.AcctsLtHash = parent.AcctsLtHash.Clone()
	}

	if sealevel.SysvarCache.Rent.Sysvar == nil {
		rent := sealevel.NewDefaultRentSysvar()
		sealevel.SysvarCache.Rent.Sysvar = &rent
	}
	return slotCtx, nil
}

func leaderFeatures(parent *features.Features) *features.Features {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	if parent == nil {
		return feats
	}
	for gate, info := range *parent {
		if info.Enabled {
			feats.EnableFeature(gate, info.ActivationSlot)
		}
	}
	return feats
}

// DefaultBankHash returns the working bank hash used in Alpenglow footers.
func DefaultBankHash(bank *WorkingBank) solana.Hash {
	if bank == nil || bank.SlotCtx() == nil {
		return solana.Hash{}
	}
	slotCtx := bank.SlotCtx()
	if len(slotCtx.FinalBankhash) > 0 {
		return solana.HashFromBytes(slotCtx.FinalBankhash)
	}
	return slotCtx.LastBlockhash
}
