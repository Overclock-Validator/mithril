package replay

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// RootedRPCBankContext reconstructs execution parameters for the durable root.
// It does not activate features, write accounts or publish process-global caches.
// The caller must publish it only with the matching scalar root and fence all
// later execution-account reads independently.
func RootedRPCBankContext(ctx context.Context, db *accountsdb.AccountsDb, s *state.MithrilState, resume *ResumeState, alpenglow bool) (*ChainTipSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if db == nil || s == nil {
		return nil, fmt.Errorf("rooted execution state is unavailable")
	}
	slot, height := s.ManifestParentSlot, s.ManifestBlockHeight
	bankhash, recentEntries, evicted := s.ManifestParentBankhash, s.ManifestRecentBlockhashes, s.ManifestEvictedBlockhash
	fees := reconstructFeeRateGovernor(s)
	var clockBytes []byte
	var slotHashes *sealevel.SysvarSlotHashes
	rooted := s.LastRootedContext
	if rooted != nil {
		if rooted.Slot != s.LastRootedSlot {
			return nil, fmt.Errorf("rooted execution context slot %d does not match durable root %d", rooted.Slot, s.LastRootedSlot)
		}
		slot, height, bankhash = rooted.Slot, rooted.BlockHeight, rooted.Bankhash
		recentEntries, evicted = rooted.RecentBlockhashes, rooted.EvictedBlockhash
		if rooted.Clock == "" || rooted.SlotHashes == nil {
			return nil, fmt.Errorf("rooted execution sysvar overrides are unavailable at slot %d", slot)
		}
		var err error
		clockBytes, err = base64.StdEncoding.DecodeString(rooted.Clock)
		if err != nil {
			return nil, fmt.Errorf("decode rooted execution clock: %w", err)
		}
		hashes := make(sealevel.SysvarSlotHashes, len(rooted.SlotHashes))
		for i, entry := range rooted.SlotHashes {
			hash, err := solana.HashFromBase58(entry.Hash)
			if err != nil {
				return nil, fmt.Errorf("decode rooted slot hash at index %d: %w", i, err)
			}
			hashes[i] = sealevel.SlotHash{Slot: entry.Slot, Hash: hash}
		}
		slotHashes = &hashes
		if fees != nil {
			fees.LamportsPerSignature, fees.PrevLamportsPerSignature = rooted.LamportsPerSignature, rooted.PrevLamportsPerSig
		}
	}
	if fees == nil || len(recentEntries) == 0 || (slot > 0 && height == 0) {
		return nil, fmt.Errorf("rooted execution blockhash or fee metadata is unavailable at slot %d", slot)
	}
	lastHash, err := solana.HashFromBase58(recentEntries[0].Blockhash)
	if err != nil || lastHash == (solana.Hash{}) {
		return nil, fmt.Errorf("invalid rooted execution blockhash at slot %d", slot)
	}
	if rooted != nil && rooted.Blockhash != lastHash.String() {
		return nil, fmt.Errorf("rooted execution blockhash does not match its recent queue at slot %d", slot)
	}
	parentHash, err := solana.HashFromBase58(bankhash)
	if err != nil {
		return nil, fmt.Errorf("decode rooted execution bankhash: %w", err)
	}
	evictedHash, err := solana.HashFromBase58(evicted)
	if err != nil {
		return nil, fmt.Errorf("decode rooted execution evicted blockhash: %w", err)
	}
	recent := make(sealevel.SysvarRecentBlockhashes, len(recentEntries))
	for i, entry := range recentEntries {
		hash, err := solana.HashFromBase58(entry.Blockhash)
		if err != nil {
			return nil, fmt.Errorf("decode rooted recent blockhash at index %d: %w", i, err)
		}
		recent[i] = sealevel.RecentBlockHashesEntry{Blockhash: hash, FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: entry.LamportsPerSignature}}
	}
	if recent[0].FeeCalculator.LamportsPerSignature != fees.LamportsPerSignature {
		return nil, fmt.Errorf("rooted execution blockhash fee does not match its governor at slot %d", slot)
	}

	version, stable := db.CommittedAccountVersion()
	if !stable || !rpcExecutionRootMatches(db, slot) {
		return nil, fmt.Errorf("durable execution account view is unavailable at slot %d", slot)
	}
	var keys []solana.PublicKey
	if err := sealevel.RangeBankSysvarAddresses(func(key solana.PublicKey) error { keys = append(keys, key); return nil }); err != nil {
		return nil, err
	}
	sysvarCount := len(keys)
	for _, gate := range features.AllFeatureGates {
		keys = append(keys, gate.Address)
	}
	loaded := make([]*accounts.Account, len(keys))
	remaining := uint64(16 << 20)
	for i, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		acct, err := db.GetAccountWithDataLimit(slot, key, remaining)
		if errors.Is(err, accountsdb.ErrNoAccount) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read rooted execution account %s: %w", key, err)
		}
		if acct == nil || uint64(len(acct.Data)) > remaining {
			return nil, fmt.Errorf("invalid rooted execution account read for %s", key)
		}
		loaded[i] = acct
		remaining -= uint64(len(acct.Data))
	}
	owned := make([]*accounts.Account, 0, sysvarCount)
	for _, acct := range loaded[:sysvarCount] {
		if acct == nil || acct.Lamports == 0 {
			continue
		}
		acct = acct.Clone()
		switch [32]byte(acct.Key) {
		case sealevel.SysvarRecentBlockHashesAddr:
			acct.Data = recent.MustMarshal()
		case sealevel.SysvarClockAddr:
			if rooted != nil {
				acct.Data = clockBytes
			}
		case sealevel.SysvarSlotHashesAddr:
			if slotHashes != nil {
				acct.Data = slotHashes.MustMarshal()
			}
		}
		owned = append(owned, acct)
	}
	sysvars, err := sealevel.NewBankSysvarsOwned(slot, owned...)
	if err != nil {
		return nil, err
	}
	if err := sysvars.ValidateForExecution(); err != nil {
		return nil, err
	}
	clock, _ := sysvars.Clock()
	schedule, ok := bankEpochScheduleFromState(s)
	if !ok {
		value, _ := sysvars.EpochSchedule()
		schedule = &value
	}
	if schedule.SlotsPerEpoch == 0 || clock.Slot != slot || clock.Epoch != schedule.GetEpoch(slot) || (rooted != nil && rooted.Epoch != clock.Epoch) {
		return nil, fmt.Errorf("rooted execution clock or epoch does not match slot %d", slot)
	}
	f := features.NewFeaturesDefault()
	for i, gate := range features.AllFeatureGates {
		acct := loaded[sysvarCount+i]
		if acct == nil || acct.Lamports == 0 || acct.Owner != addresses.FeatureAddr {
			continue
		}
		var feature features.FeatureAcct
		if err := feature.UnmarshalWithDecoder(bin.NewBinDecoder(acct.Data)); err != nil {
			return nil, fmt.Errorf("decode rooted feature %s: %w", gate.Name, err)
		}
		if feature.ActivatedAt != nil && *feature.ActivatedAt <= slot {
			f.EnableFeature(gate, *feature.ActivatedAt)
		}
	}
	if alpenglow {
		applyAlpenglowRuntimeFeatureOverrides(f, slot)
	}
	stakes, err := rpcExecutionStakes(s, resume, slot, clock.Epoch, schedule.GetEpoch(s.ManifestParentSlot))
	if err != nil {
		return nil, err
	}
	latest, stable := db.CommittedAccountVersion()
	if !stable || version != latest || !rpcExecutionRootMatches(db, slot) {
		return nil, fmt.Errorf("durable execution account view changed at slot %d", slot)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &ChainTipSnapshot{Slot: slot, Bankhash: parentHash, LastBlockhash: lastHash, BlockHeight: height,
		LatestEvictedBlockhash: evictedHash, Features: f, PrevFeeGovernor: fees, BankSysvars: sysvars,
		EpochStakes: stakes.Stakes, TotalEpochStake: stakes.TotalStake, AccountVersion: version, AccountVersionsReady: true}, nil
}

func rpcExecutionRootMatches(db *accountsdb.AccountsDb, slot uint64) bool {
	if db.RootedDurable {
		return db.DurableThrough() == 0 || db.DurableThrough() == slot
	}
	return db.CommittedAccountSlot() == 0 || db.CommittedAccountSlot() == slot
}

func rpcExecutionStakes(s *state.MithrilState, resume *ResumeState, slot, epoch, snapshotEpoch uint64) (epochstakes.Snapshot, error) {
	key := safemath.SaturatingAddU64(epoch, 1)
	var data []byte
	persisted := false
	if rooted := s.LastRootedContext; rooted != nil {
		if resume != nil && resume.ParentSlot == slot && len(resume.ComputedEpochStakes) != 0 {
			data = resume.ComputedEpochStakes[key]
			persisted = true
		} else if len(s.ComputedEpochStakes) != 0 {
			data = []byte(s.ComputedEpochStakes[key])
			persisted = true
		} else if epoch > snapshotEpoch {
			return epochstakes.Snapshot{}, fmt.Errorf("rooted execution epoch %d has no persisted stake generation %d", epoch, key)
		}
	}
	if !persisted {
		seeds, _, err := prepareManifestEpochStakesForRuntime(s, epoch, snapshotEpoch)
		if err != nil {
			return epochstakes.Snapshot{}, err
		}
		for _, seed := range seeds {
			if seed.runtimeEpoch == key {
				data = seed.data
				break
			}
		}
	}
	cache := epochstakes.NewEpochStakesCache()
	loaded, err := cache.DeserializeAndLoadEpoch(data)
	if err != nil || loaded != key {
		return epochstakes.Snapshot{}, fmt.Errorf("rooted execution stake generation %d is unavailable or invalid", key)
	}
	view, ok := cache.Snapshot(key)
	if !ok || len(view.Stakes) == 0 {
		return epochstakes.Snapshot{}, fmt.Errorf("rooted execution stake generation %d is empty", key)
	}
	return view, nil
}
