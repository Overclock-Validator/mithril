package replay

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/wincode"
	"github.com/gagliardetto/solana-go"
)

const rewardEpochDelegatedStakeRecordLen = 32 + 8

func rewardEpochDelegatedStakesMaxDataLen() uint64 {
	// epoch + Vec length + the bounded validator records.
	return 8 + 8 + uint64(alpenglow.MaximumVATValidators)*rewardEpochDelegatedStakeRecordLen
}

func sortedPubkeys[V any](values map[solana.PublicKey]V) []solana.PublicKey {
	keys := make([]solana.PublicKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i][:], keys[j][:]) < 0
	})
	return keys
}

func encodeRewardEpochDelegatedStakes(
	epoch uint64,
	admitted map[solana.PublicKey]uint64,
	rewardEpochEffectiveStakes map[solana.PublicKey]uint64,
) ([]byte, error) {
	if len(admitted) > alpenglow.MaximumVATValidators {
		return nil, fmt.Errorf("reward epoch delegated stakes has %d validators, limit %d", len(admitted), alpenglow.MaximumVATValidators)
	}
	keys := sortedPubkeys(admitted)
	w := wincode.NewWriter(16 + len(keys)*rewardEpochDelegatedStakeRecordLen)
	w.WriteU64(epoch)
	w.WriteU64(uint64(len(keys)))
	for _, key := range keys {
		w.WriteBytes(key[:])
		w.WriteU64(rewardEpochEffectiveStakes[key])
	}
	return w.Bytes(), nil
}

func missingParentAccount(key solana.PublicKey) *accounts.Account {
	return &accounts.Account{Key: key, Owner: a.SystemProgramAddr, RentEpoch: math.MaxUint64}
}

// stageRewardEpochDelegatedStakes mirrors Agave's
// RewardEpochDelegatedStakes::set. The account is part of consensus state, not
// merely a replay cache: omitting it changes both capitalization and bank hash.
func stageRewardEpochDelegatedStakes(
	acctsDb *accountsdb.AccountsDb,
	readSlot, storeSlot, rewardedEpoch uint64,
	admitted map[solana.PublicKey]uint64,
	rewardEpochEffectiveStakes map[solana.PublicKey]uint64,
	replayCtx *ReplayCtx,
	bankFeatures ...*features.Features,
) (*accounts.Account, *accounts.Account, error) {
	data, err := encodeRewardEpochDelegatedStakes(rewardedEpoch, admitted, rewardEpochEffectiveStakes)
	if err != nil {
		return nil, nil, err
	}
	rnt := sealevel.SysvarCache.Rent.Sysvar
	if rnt == nil {
		return nil, nil, fmt.Errorf("rent sysvar unavailable while storing reward epoch delegated stakes")
	}

	key := RewardEpochDelegatedStakesAccountAddr(bankFeatures...)
	parent, err := acctsDb.GetAccount(readSlot, key)
	if err != nil {
		if !errors.Is(err, accountsdb.ErrNoAccount) {
			return nil, nil, fmt.Errorf("load reward epoch delegated stakes account: %w", err)
		}
		parent = missingParentAccount(key)
	} else {
		parent = parent.Clone()
	}

	updated := &accounts.Account{
		Key:        key,
		Lamports:   rnt.MinimumBalance(rewardEpochDelegatedStakesMaxDataLen()),
		Data:       data,
		Owner:      a.SystemProgramAddr,
		Executable: false,
		RentEpoch:  0,
	}
	if err := acctsDb.StoreAccounts([]*accounts.Account{updated}, storeSlot, nil); err != nil {
		return nil, nil, fmt.Errorf("store reward epoch delegated stakes account: %w", err)
	}
	if updated.Lamports >= parent.Lamports {
		replayCtx.Capitalization = safemath.SaturatingAddU64(replayCtx.Capitalization, updated.Lamports-parent.Lamports)
	} else {
		replayCtx.Capitalization = safemath.SaturatingSubU64(replayCtx.Capitalization, parent.Lamports-updated.Lamports)
	}
	return updated.Clone(), parent, nil
}

// applyAlpenglowBoundaryVAT transfers the epoch admission ticket from every
// admitted vote account to the incinerator. ProcessBlock burns the incinerator
// later in the same bank, matching Agave's ordering (VAT before commission
// payout, incinerator burn before bank freeze).
func applyAlpenglowBoundaryVAT(
	acctsDb *accountsdb.AccountsDb,
	readSlot, storeSlot, burnPerValidator uint64,
	admitted map[solana.PublicKey]uint64,
) ([]*accounts.Account, []*accounts.Account, error) {
	if burnPerValidator == 0 || len(admitted) == 0 {
		return nil, nil, nil
	}

	keys := sortedPubkeys(admitted)
	updated := make([]*accounts.Account, 0, len(keys)+1)
	parents := make([]*accounts.Account, 0, len(keys)+1)
	toStore := make([]*accounts.Account, 0, len(keys)+1)
	var totalVAT uint64
	for _, key := range keys {
		parent, err := acctsDb.GetAccount(readSlot, key)
		if err != nil {
			return nil, nil, fmt.Errorf("load VAT vote account %s: %w", key, err)
		}
		if parent.Lamports < burnPerValidator {
			return nil, nil, fmt.Errorf("VAT vote account %s has %d lamports, burn requires %d", key, parent.Lamports, burnPerValidator)
		}
		parents = append(parents, parent.Clone())
		acct := parent.Clone()
		acct.Lamports -= burnPerValidator
		updated = append(updated, acct.Clone())
		toStore = append(toStore, acct)
		var addErr error
		totalVAT, addErr = safemath.CheckedAddU64(totalVAT, burnPerValidator)
		if addErr != nil {
			return nil, nil, fmt.Errorf("accumulate epoch VAT: %w", addErr)
		}
	}

	incineratorParent, err := acctsDb.GetAccount(readSlot, a.IncineratorAddr)
	if err != nil {
		if !errors.Is(err, accountsdb.ErrNoAccount) {
			return nil, nil, fmt.Errorf("load incinerator for epoch VAT: %w", err)
		}
		incineratorParent = missingParentAccount(a.IncineratorAddr)
	} else {
		incineratorParent = incineratorParent.Clone()
	}
	incinerator := incineratorParent.Clone()
	incinerator.Key = a.IncineratorAddr
	incinerator.Owner = a.SystemProgramAddr
	incinerator.Executable = false
	incinerator.Lamports, err = safemath.CheckedAddU64(incinerator.Lamports, totalVAT)
	if err != nil {
		return nil, nil, fmt.Errorf("credit incinerator with epoch VAT: %w", err)
	}
	parents = append(parents, incineratorParent)
	updated = append(updated, incinerator.Clone())
	toStore = append(toStore, incinerator)

	if err := acctsDb.StoreAccounts(toStore, storeSlot, nil); err != nil {
		return nil, nil, fmt.Errorf("store epoch VAT accounts: %w", err)
	}
	return updated, parents, nil
}

// coalesceEpochAccountUpdates retains the original parent and final state for
// each key when several boundary phases touch the same account (VAT followed
// by voting commission is the important case).
func coalesceEpochAccountUpdates(updated, parents []*accounts.Account) ([]*accounts.Account, []*accounts.Account) {
	if len(updated) == 0 {
		return updated, parents
	}
	order := make([]solana.PublicKey, 0, len(updated))
	finalByKey := make(map[solana.PublicKey]*accounts.Account, len(updated))
	parentByKey := make(map[solana.PublicKey]*accounts.Account, len(updated))
	for i, acct := range updated {
		if acct == nil || i >= len(parents) || parents[i] == nil {
			continue
		}
		if _, exists := finalByKey[acct.Key]; !exists {
			order = append(order, acct.Key)
			parentByKey[acct.Key] = parents[i].Clone()
		}
		finalByKey[acct.Key] = acct.Clone()
	}
	coalescedUpdated := make([]*accounts.Account, 0, len(order))
	coalescedParents := make([]*accounts.Account, 0, len(order))
	for _, key := range order {
		coalescedUpdated = append(coalescedUpdated, finalByKey[key])
		coalescedParents = append(coalescedParents, parentByKey[key])
	}
	return coalescedUpdated, coalescedParents
}
