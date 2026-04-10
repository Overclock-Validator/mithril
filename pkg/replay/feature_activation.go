package replay

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const (
	upgradeableLoaderProgramStateSize        = 36
	upgradeableLoaderBufferMetadataSize      = 37
	upgradeableLoaderProgramDataMetadataSize = 45
)

var splTokenProgramID = base58.MustDecodeFromString("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
var pTokenProgramBuffer = base58.MustDecodeFromString("ptok6rngomXrDbWf5v5Mkmu5CEbB51hzSCPDoj9DrvF")

type loaderV2ProgramMigration struct {
	modifiedAccts  []*accounts.Account
	parentAccts    []*accounts.Account
	lamportsToBurn uint64
	lamportsToFund uint64
}

func scanAndEnableFeatures(acctsDb *accountsdb.AccountsDb, replayCtx *ReplayCtx, slot uint64, startOfEpoch bool) (*features.Features, []*accounts.Account, []*accounts.Account) {
	parentAccts := make([]*accounts.Account, 0)
	modifiedAccts := make([]*accounts.Account, 0)

	f := features.NewFeaturesDefault()

	for _, featureGate := range features.AllFeatureGates {
		acct, err := acctsDb.GetAccount(slot, featureGate.Address)
		if err != nil {
			continue
		}
		if acct.Owner != a.FeatureAddr {
			continue
		}

		featureAcct := features.UnmarshalFeatureAcct(acct.Data)

		// feature already activated. set it as activated on new features object.
		if featureAcct.ActivatedAt != nil && slot >= *featureAcct.ActivatedAt {
			f.EnableFeature(featureGate, *featureAcct.ActivatedAt)
		}

		// the feature needs activating, so marshal up the feature account state as
		// activated, store the parent account and modified account, and enable the
		// feature on the new features object.
		if featureAcct.ActivatedAt == nil && startOfEpoch {
			parentAccts = append(parentAccts, acct.Clone())

			newFeatureAcct := &features.FeatureAcct{ActivatedAt: &slot}
			newFeatureAcctBytes, err := features.MarshalFeatureAcct(newFeatureAcct)
			if err != nil {
				panic(err)
			}

			acct.Data = newFeatureAcctBytes
			modifiedAccts = append(modifiedAccts, acct)
			f.EnableFeature(featureGate, slot)

			mlog.Log.Infof("Activated new feature %s at slot %d", featureGate.Name, slot)
		}
	}

	if len(modifiedAccts) != 0 {
		if err := acctsDb.StoreAccounts(modifiedAccts, slot, nil); err != nil {
			panic(err)
		}
	}

	for _, featureAcct := range modifiedAccts {
		if f.IsActive(features.DeprecateRentExemptionThreshold) && featureAcct.Key == features.DeprecateRentExemptionThreshold.Address {
			modifiedRentAcct, parentRentAcct, err := applyDeprecateRentExemptionThresholdActivation(acctsDb, slot)
			if err != nil {
				panic(err)
			}
			modifiedAccts = append(modifiedAccts, modifiedRentAcct)
			parentAccts = append(parentAccts, parentRentAcct)
		}

		if f.IsActive(features.ReplaceSplTokenWithPToken) && featureAcct.Key == features.ReplaceSplTokenWithPToken.Address {
			migratedAccts, parentMigratedAccts, err := applyReplaceSplTokenWithPTokenActivation(acctsDb, replayCtx, slot, f)
			if err != nil {
				mlog.Log.Warnf("Failed to replace SPL Token with p-token buffer '%s': %v", pTokenProgramBuffer, err)
				continue
			}

			modifiedAccts = append(modifiedAccts, migratedAccts...)
			parentAccts = append(parentAccts, parentMigratedAccts...)
		}
	}

	return f, modifiedAccts, parentAccts
}

func applyDeprecateRentExemptionThresholdActivation(acctsDb *accountsdb.AccountsDb, slot uint64) (*accounts.Account, *accounts.Account, error) {
	rentSysvar := sealevel.SysvarRent{
		LamportsPerUint8Year: uint64(float64(sealevel.SysvarCache.Rent.Sysvar.LamportsPerUint8Year) * sealevel.SysvarCache.Rent.Sysvar.ExemptionThreshold),
		ExemptionThreshold:   1.0,
		BurnPercent:          sealevel.SysvarCache.Rent.Sysvar.BurnPercent,
	}

	rentAcct, err := acctsDb.GetAccount(slot, sealevel.SysvarRentAddr)
	if err != nil {
		return nil, nil, err
	}
	parentRentAcct := rentAcct.Clone()

	newRentSysvarBytes := rentSysvar.MustMarshal()
	copy(rentAcct.Data, newRentSysvarBytes)
	if err := acctsDb.StoreAccounts([]*accounts.Account{rentAcct}, slot, nil); err != nil {
		return nil, nil, err
	}

	sealevel.SysvarCache.Rent.Sysvar = &rentSysvar
	sealevel.SysvarCache.Rent.Acct = rentAcct

	return rentAcct, parentRentAcct, nil
}

func marshalUpgradeableLoaderStateSized(state *sealevel.UpgradeableLoaderState, size int) ([]byte, error) {
	var buf bytes.Buffer
	encoder := bin.NewBinEncoder(&buf)
	if err := state.MarshalWithEncoder(encoder); err != nil {
		return nil, err
	}

	serialized := buf.Bytes()
	if len(serialized) > size {
		return nil, fmt.Errorf("serialized upgradeable loader state length %d exceeds fixed size %d", len(serialized), size)
	}

	stateBytes := make([]byte, size)
	copy(stateBytes, serialized)
	return stateBytes, nil
}

func deriveUpgradeableLoaderProgramDataAddress(programAddress solana.PublicKey) (solana.PublicKey, error) {
	programDataAddress, _, err := solana.FindProgramAddress([][]byte{programAddress.Bytes()}, a.BpfLoaderUpgradeableAddr)
	return programDataAddress, err
}

func migrationAccountExists(acct *accounts.Account) bool {
	if acct == nil {
		return false
	}

	var zeroOwner [32]byte
	return acct.Lamports != 0 || len(acct.Data) != 0 || acct.Owner != zeroOwner || acct.Executable
}

func buildLoaderV2ProgramUpgradeToLoaderV3(
	slot uint64,
	rentSysvar *sealevel.SysvarRent,
	targetProgram *accounts.Account,
	sourceBuffer *accounts.Account,
	existingProgramData *accounts.Account,
	allowPrefunded bool,
) (*loaderV2ProgramMigration, error) {
	if rentSysvar == nil {
		return nil, errors.New("rent sysvar unavailable for p-token migration")
	}
	if targetProgram == nil {
		return nil, errors.New("missing SPL Token program account")
	}
	if targetProgram.Owner != a.BpfLoader2Addr {
		return nil, fmt.Errorf("SPL Token program owner %s is not loader v2", solana.PublicKeyFromBytes(targetProgram.Owner[:]))
	}
	if !targetProgram.Executable {
		return nil, errors.New("SPL Token program account is not executable")
	}
	if sourceBuffer == nil {
		return nil, errors.New("missing p-token source buffer account")
	}
	if sourceBuffer.Owner != a.BpfLoaderUpgradeableAddr {
		return nil, fmt.Errorf("p-token source buffer owner %s is not loader v3", solana.PublicKeyFromBytes(sourceBuffer.Owner[:]))
	}
	if len(sourceBuffer.Data) < upgradeableLoaderBufferMetadataSize {
		return nil, errors.New("p-token source buffer data too short")
	}

	sourceBufferState, err := sealevel.UnmarshalUpgradeableLoaderState(sourceBuffer.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode p-token source buffer state: %w", err)
	}
	if sourceBufferState.Type != sealevel.UpgradeableLoaderStateTypeBuffer {
		return nil, fmt.Errorf("p-token source buffer has invalid state type %d", sourceBufferState.Type)
	}

	programDataAddress, err := deriveUpgradeableLoaderProgramDataAddress(targetProgram.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to derive programdata address for %s: %w", targetProgram.Key, err)
	}

	programDataParentAcct := &accounts.Account{Key: programDataAddress, RentEpoch: math.MaxUint64}
	if migrationAccountExists(existingProgramData) {
		if !allowPrefunded {
			return nil, fmt.Errorf("programdata account %s already exists", programDataAddress)
		}
		if existingProgramData.Owner != a.SystemProgramAddr {
			return nil, fmt.Errorf("prefunded programdata account %s has unexpected owner %s", programDataAddress, solana.PublicKeyFromBytes(existingProgramData.Owner[:]))
		}
		programDataParentAcct = existingProgramData.Clone()
	}

	elfBytes := sourceBuffer.Data[upgradeableLoaderBufferMetadataSize:]

	programStateBytes, err := marshalUpgradeableLoaderStateSized(&sealevel.UpgradeableLoaderState{
		Type: sealevel.UpgradeableLoaderStateTypeProgram,
		Program: sealevel.UpgradeableLoaderStateProgram{
			ProgramDataAddress: programDataAddress,
		},
	}, upgradeableLoaderProgramStateSize)
	if err != nil {
		return nil, fmt.Errorf("failed to encode migrated program account state: %w", err)
	}

	programDataStateBytes, err := marshalUpgradeableLoaderStateSized(&sealevel.UpgradeableLoaderState{
		Type: sealevel.UpgradeableLoaderStateTypeProgramData,
		ProgramData: sealevel.UpgradeableLoaderStateProgramData{
			Slot:                    slot,
			UpgradeAuthorityAddress: nil,
		},
	}, upgradeableLoaderProgramDataMetadataSize)
	if err != nil {
		return nil, fmt.Errorf("failed to encode migrated programdata account state: %w", err)
	}

	programDataBytes := make([]byte, upgradeableLoaderProgramDataMetadataSize+len(elfBytes))
	copy(programDataBytes, programDataStateBytes)
	copy(programDataBytes[upgradeableLoaderProgramDataMetadataSize:], elfBytes)

	newProgramAcct := &accounts.Account{
		Slot:       slot,
		Key:        targetProgram.Key,
		Lamports:   rentSysvar.MinimumBalance(upgradeableLoaderProgramStateSize),
		Data:       programStateBytes,
		Owner:      a.BpfLoaderUpgradeableAddr,
		Executable: true,
		RentEpoch:  math.MaxUint64,
	}

	newProgramDataAcct := &accounts.Account{
		Slot:       slot,
		Key:        programDataAddress,
		Lamports:   rentSysvar.MinimumBalance(uint64(len(programDataBytes))),
		Data:       programDataBytes,
		Owner:      a.BpfLoaderUpgradeableAddr,
		Executable: false,
		RentEpoch:  math.MaxUint64,
	}

	clearedSourceBuffer := &accounts.Account{
		Slot:      slot,
		Key:       sourceBuffer.Key,
		RentEpoch: math.MaxUint64,
	}

	lamportsToBurn := targetProgram.Lamports + sourceBuffer.Lamports
	if migrationAccountExists(existingProgramData) {
		lamportsToBurn += existingProgramData.Lamports
	}

	return &loaderV2ProgramMigration{
		modifiedAccts: []*accounts.Account{
			newProgramAcct,
			newProgramDataAcct,
			clearedSourceBuffer,
		},
		parentAccts: []*accounts.Account{
			targetProgram.Clone(),
			programDataParentAcct,
			sourceBuffer.Clone(),
		},
		lamportsToBurn: lamportsToBurn,
		lamportsToFund: newProgramAcct.Lamports + newProgramDataAcct.Lamports,
	}, nil
}

func applyCapitalizationChange(replayCtx *ReplayCtx, lamportsToBurn uint64, lamportsToFund uint64) error {
	if replayCtx == nil {
		return errors.New("replay context unavailable for capitalization update")
	}

	switch {
	case lamportsToBurn > lamportsToFund:
		diff := lamportsToBurn - lamportsToFund
		if replayCtx.Capitalization < diff {
			return fmt.Errorf("capitalization underflow applying migration delta %d", diff)
		}
		replayCtx.Capitalization -= diff
	case lamportsToFund > lamportsToBurn:
		diff := lamportsToFund - lamportsToBurn
		if math.MaxUint64-replayCtx.Capitalization < diff {
			return fmt.Errorf("capitalization overflow applying migration delta %d", diff)
		}
		replayCtx.Capitalization += diff
	}

	return nil
}

func applyReplaceSplTokenWithPTokenActivation(
	acctsDb *accountsdb.AccountsDb,
	replayCtx *ReplayCtx,
	slot uint64,
	f *features.Features,
) ([]*accounts.Account, []*accounts.Account, error) {
	targetProgram, err := acctsDb.GetAccount(slot, splTokenProgramID)
	if err != nil {
		return nil, nil, err
	}

	sourceBuffer, err := acctsDb.GetAccount(slot, pTokenProgramBuffer)
	if err != nil {
		return nil, nil, err
	}

	programDataAddress, err := deriveUpgradeableLoaderProgramDataAddress(splTokenProgramID)
	if err != nil {
		return nil, nil, err
	}

	var existingProgramData *accounts.Account
	existingProgramData, err = acctsDb.GetAccount(slot, programDataAddress)
	if err != nil && !errors.Is(err, accountsdb.ErrNoAccount) {
		return nil, nil, err
	}
	if errors.Is(err, accountsdb.ErrNoAccount) {
		existingProgramData = nil
	}

	if err := sealevel.ValidateUpgradeableLoaderProgram(sourceBuffer.Data[upgradeableLoaderBufferMetadataSize:], f); err != nil {
		return nil, nil, fmt.Errorf("failed to validate p-token program bytes: %w", err)
	}

	migration, err := buildLoaderV2ProgramUpgradeToLoaderV3(
		slot,
		sealevel.SysvarCache.Rent.Sysvar,
		targetProgram,
		sourceBuffer,
		existingProgramData,
		f.IsActive(features.RelaxProgramdataAccountCheckMigration),
	)
	if err != nil {
		return nil, nil, err
	}

	if err := applyCapitalizationChange(replayCtx, migration.lamportsToBurn, migration.lamportsToFund); err != nil {
		return nil, nil, err
	}

	acctsDb.RemoveProgramFromCache(splTokenProgramID)

	if err := acctsDb.StoreAccounts(migration.modifiedAccts, slot, nil); err != nil {
		return nil, nil, err
	}

	return migration.modifiedAccts, migration.parentAccts, nil
}
