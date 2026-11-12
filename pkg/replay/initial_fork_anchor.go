package replay

import (
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
)

// initialParentBankSysvars deliberately ignores process-global decoded caches:
// a recovery in the same process can leave them on the abandoned branch. Only
// fields explicitly carried by the durable resume context override DB bytes.
func initialParentBankSysvars(source sealevel.AccountReader, parentSlot uint64, resume *ResumeState, persisted *state.MithrilState) (*sealevel.BankSysvars, error) {
	var owned []*accounts.Account
	err := sealevel.RangeBankSysvarAddresses(func(key solana.PublicKey) error {
		acct, err := source.GetAccount(parentSlot, key)
		if errors.Is(err, accountsdb.ErrNoAccount) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("load initial parent sysvar %s: %w", key, err)
		}
		if acct == nil {
			return nil
		}
		acct = acct.Clone()
		acct.Key = key
		if resume != nil {
			switch key {
			case sealevel.SysvarClockAddr:
				if len(resume.Clock) > 0 {
					acct.Data = append([]byte(nil), resume.Clock...)
				}
			case sealevel.SysvarRecentBlockHashesAddr:
				if resume.RecentBlockhashes != nil {
					acct.Data = resume.RecentBlockhashes.MustMarshal()
				}
			case sealevel.SysvarSlotHashesAddr:
				if resume.SlotHashes != nil {
					acct.Data = resume.SlotHashes.MustMarshal()
				}
			}
		} else if key == sealevel.SysvarRecentBlockHashesAddr && persisted != nil && len(persisted.ManifestRecentBlockhashes) > 0 {
			queue := DecodeRecentBlockhashes(persisted.ManifestRecentBlockhashes)
			acct.Data = queue.MustMarshal()
		}
		owned = append(owned, acct)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sealevel.NewBankSysvarsOwned(parentSlot, owned...)
}

// seedInitialRootAnchor runs after first-block cache configuration, before any
// epoch transition or transaction. The parent has no retained account layer,
// but needs an immutable metadata anchor for a fork before the first fold.
func seedInitialRootAnchor(tail *unrootedTail, source sealevel.AccountReader, block *b.Block, resume *ResumeState, persisted *state.MithrilState, forkState *replayForkState) error {
	if tail == nil || tail.rootContext != nil {
		return nil
	}
	if block == nil || forkState == nil || block.AcctsLtHash == nil || block.PrevFeeRateGovernor == nil || block.BlockHeight == 0 {
		return fmt.Errorf("initial fork parent is missing configured metadata")
	}
	sysvars, err := initialParentBankSysvars(source, block.ParentSlot, resume, persisted)
	if err != nil {
		return err
	}
	clock, hasClock := sysvars.Clock()
	_, hasRent := sysvars.Rent()
	_, hasSchedule := sysvars.EpochSchedule()
	_, hasHistory := sysvars.StakeHistory()
	recent, hasRecent := sysvars.RecentBlockhashes()
	hashes, hasHashes := sysvars.SlotHashes()
	if !hasClock || !hasRent || !hasSchedule || !hasHistory || !hasRecent || !hasHashes || clock.Slot != block.ParentSlot {
		return fmt.Errorf("initial fork parent %d has incomplete or mismatched bank sysvars", block.ParentSlot)
	}
	// Startup may already have activated features for the first child bank.
	// Retain the parent generation instead; the boundary will activate the
	// selected sibling's features again if this first bank is discarded.
	parentForkState := *forkState
	// Startup feature migrations can already have charged/burned lamports in
	// replayCtx. The root is the pre-migration checkpoint, not that first-child
	// working context; replaying a sibling must apply the migration exactly once.
	if resume != nil {
		parentForkState.ctx.Capitalization = resume.Capitalization
	} else if persisted != nil {
		parentForkState.ctx.Capitalization = persisted.ManifestCapitalization
	} else {
		return fmt.Errorf("initial fork parent is missing checkpoint capitalization")
	}
	if forkState.ctx.CurrentFeatures != nil {
		parentFeatures := forkState.ctx.CurrentFeatures.Clone()
		for gate, activation := range parentFeatures.All() {
			if activation.Enabled && activation.ActivationSlot > block.ParentSlot {
				parentFeatures.DisableFeature(gate)
			}
		}
		applyAlpenglowRuntimeFeatureOverrides(parentFeatures, block.ParentSlot)
		parentForkState.ctx.CurrentFeatures = parentFeatures
		parentForkState.rewardsEnabled = parentFeatures.IsActive(features.EnablePartitionedEpochReward) || parentFeatures.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)
	}
	count := global.TransactionCount()
	ctx := &state.ResumeContext{
		Slot: block.ParentSlot, BlockHeight: block.BlockHeight - 1, Epoch: forkState.epoch,
		Bankhash: base58.Encode(block.ParentBankhash[:]), AcctsLtHash: base64.StdEncoding.EncodeToString(block.AcctsLtHash.Hash()),
		NumSignatures: block.PrevNumSignatures, LamportsPerSignature: block.PrevFeeRateGovernor.LamportsPerSignature,
		PrevLamportsPerSig: block.PrevFeeRateGovernor.PrevLamportsPerSignature,
		EvictedBlockhash:   base58.Encode(block.LatestEvictedBlockhash[:]), Blockhash: base58.Encode(block.LastBlockhash[:]),
		RecentBlockhashes: EncodeRecentBlockhashes(&recent), SlotHashes: EncodeSlotHashes(&hashes),
		Clock: base64.StdEncoding.EncodeToString(clock.MustMarshal()), TransactionCount: &count,
		Capitalization: parentForkState.ctx.Capitalization, SlotsPerYear: forkState.ctx.SlotsPerYear,
		InflationInitial: forkState.ctx.Inflation.Initial, InflationTerminal: forkState.ctx.Inflation.Terminal,
		InflationTaper: forkState.ctx.Inflation.Taper, InflationFoundation: forkState.ctx.Inflation.FoundationVal,
		InflationFoundationTerm: forkState.ctx.Inflation.FoundationTerm,
	}
	if resume != nil && resume.HasParentAlpenglowBlockID && resume.HasParentAlpenglowChainedMerkleRoot {
		ctx.AlpenglowBlockID = resume.ParentAlpenglowBlockID.String()
		ctx.AlpenglowChainedMerkleRoot = resume.ParentAlpenglowChainedMerkleRoot.String()
	}
	tail.rootContext, tail.rootBankSysvars = ctx, sysvars
	tail.rootEpochCaches, tail.rootForkState = global.CaptureEpochCaches(), &parentForkState
	global.PruneVoteCacheJournal(block.ParentSlot)
	return nil
}
