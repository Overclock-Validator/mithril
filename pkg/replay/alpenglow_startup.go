package replay

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

type alpenglowStartupAccountReader interface {
	GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error)
}

// ValidateAlpenglowStartupState rejects a local bank that cannot safely be
// used as the parent of an operational Alpenglow replay.  This deliberately
// consults only AccountsDB: startup must not trust an RPC peer to describe the
// consensus state represented by the snapshot we are about to extend.
func ValidateAlpenglowStartupState(acctsDb *accountsdb.AccountsDb, parentSlot uint64) error {
	if acctsDb == nil {
		return fmt.Errorf("Alpenglow startup state at parent slot %d: AccountsDB is nil", parentSlot)
	}
	return validateAlpenglowStartupState(acctsDb, parentSlot)
}

func validateAlpenglowStartupState(reader alpenglowStartupAccountReader, parentSlot uint64) error {
	activationSlot, err := loadAlpenglowFeatureActivation(reader, parentSlot)
	if err != nil {
		return fmt.Errorf("Alpenglow startup state at parent slot %d: %w", parentSlot, err)
	}

	// A populated alpenclock is an on-chain marker that migration has passed
	// Alpenglow genesis.  Before that point Agave validly falls back to the Clock
	// sysvar, but Mithril cannot safely join: it intentionally does not implement
	// TowerBFT or the TowerBFT -> Alpenglow migration protocol.
	clock, err := requireAlpenglowMetadataAccount(
		reader,
		parentSlot,
		"nanosecond clock",
		NanosecondClockAccountAddr(),
	)
	if err != nil {
		if errors.Is(err, accountsdb.ErrNoAccount) {
			return fmt.Errorf(
				"deployed feature %s activated at slot %d, but its nanosecond-clock PDA %s is absent; the local bank is pre-Alpenglow-genesis or incompatible, and this binary does not implement TowerBFT migration: %w",
				features.AlpenglowFeatureGateAddress,
				activationSlot,
				NanosecondClockAccountAddr(),
				err,
			)
		}
		return err
	}
	if len(clock.Data) != nanosecondClockDataLen {
		return fmt.Errorf("nanosecond-clock PDA %s has data length %d, expected %d",
			clock.Key, len(clock.Data), nanosecondClockDataLen)
	}

	// Agave creates this account at the boundary that activates Alpenglow (or
	// inserts it into an Alpenglow-at-genesis ledger).  Requiring and decoding it
	// catches a stale/wrong PDA derivation before voting or block production can
	// start, instead of waiting for a footer bank-hash mismatch.
	voteReward, err := requireAlpenglowMetadataAccount(
		reader,
		parentSlot,
		"vote reward",
		VoteRewardAccountAddr(),
	)
	if err != nil {
		return err
	}
	if _, err := decodeEpochInflationAccountState(voteReward.Data); err != nil {
		return fmt.Errorf("vote-reward PDA %s contains invalid epoch-inflation state: %w", voteReward.Key, err)
	}

	// This PDA is created at an epoch boundary and can validly be absent before
	// the first such boundary (including the first epoch of an
	// Alpenglow-at-genesis cluster). The startup validator does not reconstruct
	// that boundary history, so absence is tolerated; when present, however, it
	// must be the exact bounded, sorted representation Agave writes.
	rewardStakes, err := reader.GetAccount(parentSlot, RewardEpochDelegatedStakesAccountAddr())
	switch {
	case err == nil:
		if err := validateAlpenglowMetadataAccount("reward-epoch delegated stakes", RewardEpochDelegatedStakesAccountAddr(), rewardStakes); err != nil {
			return err
		}
		if err := validateRewardEpochDelegatedStakesData(rewardStakes.Data); err != nil {
			return fmt.Errorf("reward-epoch delegated-stakes PDA %s contains invalid state: %w", rewardStakes.Key, err)
		}
	case errors.Is(err, accountsdb.ErrNoAccount):
		// Valid before the first Alpenglow epoch boundary; see above.
	default:
		return fmt.Errorf("read reward-epoch delegated-stakes PDA %s at slot %d: %w",
			RewardEpochDelegatedStakesAccountAddr(), parentSlot, err)
	}

	return nil
}

func loadAlpenglowFeatureActivation(reader alpenglowStartupAccountReader, slot uint64) (uint64, error) {
	key := solana.PublicKey(features.Alpenglow.Address)
	acct, err := reader.GetAccount(slot, key)
	if err != nil {
		if errors.Is(err, accountsdb.ErrNoAccount) {
			return 0, fmt.Errorf(
				"deployed Alpenglow feature account %s is absent; refusing operational Alpenglow mode because this binary does not implement TowerBFT or migration: %w",
				features.AlpenglowFeatureGateAddress,
				err,
			)
		}
		return 0, fmt.Errorf("read deployed Alpenglow feature account %s at slot %d: %w", key, slot, err)
	}
	if acct == nil {
		return 0, fmt.Errorf("deployed Alpenglow feature account %s returned nil", key)
	}
	if acct.Key != key {
		return 0, fmt.Errorf("Alpenglow feature lookup for %s returned account %s", key, acct.Key)
	}
	if acct.Lamports == 0 {
		return 0, fmt.Errorf("deployed Alpenglow feature account %s has zero lamports", key)
	}
	if acct.Owner != a.FeatureAddr {
		return 0, fmt.Errorf("deployed Alpenglow feature account %s has owner %s, expected %s",
			key, solana.PublicKey(acct.Owner), solana.PublicKey(a.FeatureAddr))
	}
	if acct.Executable {
		return 0, fmt.Errorf("deployed Alpenglow feature account %s is unexpectedly executable", key)
	}

	decoder := bin.NewBinDecoder(acct.Data)
	var featureAcct features.FeatureAcct
	if err := featureAcct.UnmarshalWithDecoder(decoder); err != nil {
		return 0, fmt.Errorf("decode deployed Alpenglow feature account %s: %w", key, err)
	}
	if decoder.HasRemaining() {
		return 0, fmt.Errorf("deployed Alpenglow feature account %s has %d trailing data byte(s)", key, decoder.Remaining())
	}
	if featureAcct.ActivatedAt == nil {
		return 0, fmt.Errorf("deployed Alpenglow feature account %s is not activated", key)
	}
	if *featureAcct.ActivatedAt > slot {
		return 0, fmt.Errorf("deployed Alpenglow feature account %s activates at future slot %d (local parent is %d)",
			key, *featureAcct.ActivatedAt, slot)
	}
	return *featureAcct.ActivatedAt, nil
}

func requireAlpenglowMetadataAccount(
	reader alpenglowStartupAccountReader,
	slot uint64,
	name string,
	key solana.PublicKey,
) (*accounts.Account, error) {
	acct, err := reader.GetAccount(slot, key)
	if err != nil {
		return nil, fmt.Errorf("read Alpenglow %s PDA %s at slot %d: %w", name, key, slot, err)
	}
	if err := validateAlpenglowMetadataAccount(name, key, acct); err != nil {
		return nil, err
	}
	return acct, nil
}

func validateAlpenglowMetadataAccount(name string, key solana.PublicKey, acct *accounts.Account) error {
	if acct == nil {
		return fmt.Errorf("Alpenglow %s PDA %s returned nil", name, key)
	}
	if acct.Key != key {
		return fmt.Errorf("Alpenglow %s PDA lookup for %s returned account %s", name, key, acct.Key)
	}
	if acct.Lamports == 0 {
		return fmt.Errorf("Alpenglow %s PDA %s has zero lamports", name, key)
	}
	if acct.Owner != a.SystemProgramAddr {
		return fmt.Errorf("Alpenglow %s PDA %s has owner %s, expected system program %s",
			name, key, solana.PublicKey(acct.Owner), solana.PublicKey(a.SystemProgramAddr))
	}
	if acct.Executable {
		return fmt.Errorf("Alpenglow %s PDA %s is unexpectedly executable", name, key)
	}
	return nil
}

func validateRewardEpochDelegatedStakesData(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("data length %d is smaller than the 16-byte header", len(data))
	}
	count := uint64(data[8]) |
		uint64(data[9])<<8 |
		uint64(data[10])<<16 |
		uint64(data[11])<<24 |
		uint64(data[12])<<32 |
		uint64(data[13])<<40 |
		uint64(data[14])<<48 |
		uint64(data[15])<<56
	if count > uint64(alpenglowMaximumStartupValidators()) {
		return fmt.Errorf("validator count %d exceeds limit %d", count, alpenglowMaximumStartupValidators())
	}
	wantLen := 16 + int(count)*rewardEpochDelegatedStakeRecordLen
	if len(data) != wantLen {
		return fmt.Errorf("data length %d does not match validator count %d (expected %d)", len(data), count, wantLen)
	}

	var previous []byte
	for i := 0; i < int(count); i++ {
		offset := 16 + i*rewardEpochDelegatedStakeRecordLen
		key := data[offset : offset+solana.PublicKeyLength]
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			return fmt.Errorf("validator pubkeys are not strictly sorted at record %d", i)
		}
		previous = key
	}
	return nil
}

// Kept behind a function so the startup validator and the consensus encoder
// cannot quietly acquire different bounds during a refactor.
func alpenglowMaximumStartupValidators() int {
	return int((rewardEpochDelegatedStakesMaxDataLen() - 16) / rewardEpochDelegatedStakeRecordLen)
}

func validateAlpenglowRuntimeFeatureSet(f *features.Features, slot uint64) error {
	if f == nil || !f.IsActive(features.Alpenglow) {
		return fmt.Errorf(
			"refusing Alpenglow replay at slot %d: deployed feature %s is not active in the loaded feature set",
			slot,
			features.AlpenglowFeatureGateAddress,
		)
	}
	return nil
}
