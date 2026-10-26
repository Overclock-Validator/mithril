package rpcserver

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func simulationBankFixture(t *testing.T) *replay.ChainTipSnapshot {
	return simulationBankFixtureAt(t, 42, solana.Hash{3}, 6000)
}

func simulationBankFixtureAt(t *testing.T, slot uint64, hash solana.Hash, fee uint64) *replay.ChainTipSnapshot {
	t.Helper()
	clock := sealevel.SysvarClock{Slot: slot, Epoch: slot / 100}
	rent := sealevel.SysvarRent{LamportsPerUint8Year: 3480, ExemptionThreshold: 2}
	recent := sealevel.SysvarRecentBlockhashes{{Blockhash: [32]byte(hash), FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: fee}}}
	history := sealevel.SysvarSlotHistory{NextSlot: slot + 1}
	schedule := make([]byte, 33)
	binary.LittleEndian.PutUint64(schedule[:8], 100)
	bankSysvars, err := sealevel.NewBankSysvars(slot,
		&accounts.Account{Key: sealevel.SysvarClockAddr, Lamports: 1, Data: clock.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarRentAddr, Lamports: 1, Data: rent.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarEpochScheduleAddr, Lamports: 1, Data: schedule},
		&accounts.Account{Key: sealevel.SysvarSlotHashesAddr, Lamports: 1, Data: make([]byte, 8)},
		&accounts.Account{Key: sealevel.SysvarStakeHistoryAddr, Lamports: 1, Data: make([]byte, 8)},
		&accounts.Account{Key: sealevel.SysvarLastRestartSlotAddr, Lamports: 1, Data: make([]byte, 8)},
		&accounts.Account{Key: sealevel.SysvarRecentBlockHashesAddr, Lamports: 1, Data: recent.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarSlotHistoryAddr, Lamports: 1, Data: history.MustMarshal()},
	)
	require.NoError(t, err)
	require.NoError(t, bankSysvars.ValidateForExecution())
	return &replay.ChainTipSnapshot{
		Slot: slot, LastBlockhash: hash, LatestEvictedBlockhash: [32]byte{2},
		Features: features.NewFeaturesDefault(), BankSysvars: bankSysvars,
		PrevFeeGovernor: &sealevel.FeeRateGovernor{LamportsPerSignature: fee, PrevLamportsPerSignature: 5000},
		EpochStakes:     map[solana.PublicKey]uint64{{1}: 9}, TotalEpochStake: 9,
	}
}

func TestSimulationBankContextOwnsWritableParameters(t *testing.T) {
	bank := simulationBankFixture(t)
	db := &accountsdb.AccountsDb{}
	first, err := newSimulationBankContext(bank, db, db)
	require.NoError(t, err)
	second, err := newSimulationBankContext(bank, db, db)
	require.NoError(t, err)
	require.Equal(t, uint64(42), first.Slot)
	require.Equal(t, [32]byte{3}, first.LastBlockhash)
	require.Equal(t, [32]byte{2}, first.LatestEvictedBlockhash)
	require.Equal(t, uint64(6000), first.FeeRateGovernor.PrevLamportsPerSignature,
		"nonce execution must pair the completed bank's hash with its queue-head fee, not its parent's fee")
	require.Equal(t, uint64(5000), bank.PrevFeeGovernor.PrevLamportsPerSignature)
	require.Same(t, bank.BankSysvars, first.BankSysvars())
	require.Equal(t, uint64(9), first.VoteAccts[solana.PublicKey{1}])
	require.Equal(t, uint64(9), first.TotalEpochStake)
	first.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 42)
	first.FeeRateGovernor.LamportsPerSignature = 99
	first.ModifiedAccts[solana.PublicKey{1}] = true
	require.False(t, bank.Features.IsActive(features.VirtualAddressSpaceAdjustments))
	require.False(t, second.Features.IsActive(features.VirtualAddressSpaceAdjustments))
	require.Equal(t, uint64(6000), second.FeeRateGovernor.LamportsPerSignature)
	require.Empty(t, second.ModifiedAccts)
}

func TestSimulationBankContextTransactionAgeAndNonceRecovery(t *testing.T) {
	bank := simulationBankFixture(t)
	db := &accountsdb.AccountsDb{}
	ctx, err := newSimulationBankContext(bank, db, db)
	require.NoError(t, err)
	for _, check := range []struct {
		hash  solana.Hash
		valid bool
	}{{bank.LastBlockhash, true}, {solana.Hash(bank.LatestEvictedBlockhash), true}, {solana.Hash{99}, false}} {
		tx := &solana.Transaction{Message: solana.Message{RecentBlockhash: check.hash}}
		require.Equal(t, check.valid, sealevel.IsTransactionAgeValid(tx, nil, ctx))
	}

	nonceKey, authority := solana.PublicKey{7}, solana.PublicKey{8}
	previousNonce := [32]byte{9}
	state := sealevel.NonceStateVersions{Type: sealevel.NonceVersionCurrent, Current: sealevel.NonceData{
		IsInitialized: true, Authority: authority, DurableNonce: previousNonce,
		FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
	}}
	encoded, err := state.Marshal()
	require.NoError(t, err)
	require.NoError(t, ctx.SetAccount(nonceKey, &accounts.Account{Key: nonceKey, Owner: addresses.SystemProgramAddr, Lamports: 1, Data: encoded}))
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, sealevel.SystemProgramInstrTypeAdvanceNonceAccount)
	instruction := sealevel.Instruction{ProgramId: addresses.SystemProgramAddr, Data: data, Accounts: []sealevel.AccountMeta{
		{Pubkey: nonceKey, IsWritable: true}, {Pubkey: authority, IsSigner: true},
	}}
	tx := &solana.Transaction{Message: solana.Message{RecentBlockhash: previousNonce}}
	require.True(t, sealevel.IsTransactionAgeValid(tx, []sealevel.Instruction{instruction}, ctx))
	key, advanced := sealevel.MaybeAdvanceNonceAccountForFailedTx(ctx, tx, instruction)
	require.True(t, advanced)
	require.Equal(t, nonceKey, key)
	updated, err := ctx.GetAccount(nonceKey)
	require.NoError(t, err)
	decoded, err := sealevel.UnmarshalNonceStateVersions(updated.Data)
	require.NoError(t, err)
	expectedNonce := sha256.Sum256(append([]byte("DURABLE_NONCE"), bank.LastBlockhash[:]...))
	require.Equal(t, expectedNonce, decoded.State().DurableNonce)
	require.Equal(t, uint64(6000), decoded.State().FeeCalculator.LamportsPerSignature)
	require.Equal(t, uint64(5000), bank.PrevFeeGovernor.PrevLamportsPerSignature)
	require.Equal(t, previousNonce, state.State().DurableNonce)
}

func TestSimulationBankContextRejectsIncompleteOrMixedParameters(t *testing.T) {
	for _, invalid := range []string{"features", "fees", "sysvars", "slot", "hash", "fee-head", "rent", "reader"} {
		t.Run(invalid, func(t *testing.T) {
			bank := simulationBankFixture(t)
			db := &accountsdb.AccountsDb{}
			var reader sealevel.AccountReader = db
			switch invalid {
			case "features":
				bank.Features = nil
			case "fees":
				bank.PrevFeeGovernor = nil
			case "sysvars":
				bank.BankSysvars = nil
			case "slot":
				bank.Slot++
			case "hash":
				bank.LastBlockhash = solana.Hash{99}
			case "fee-head":
				bank.PrevFeeGovernor.LamportsPerSignature++
			case "rent":
				bank.BankSysvars = bank.BankSysvars.Without(sealevel.SysvarRentAddr)
			case "reader":
				reader = nil
			}
			_, err := newSimulationBankContext(bank, db, reader)
			require.Error(t, err)
		})
	}
}

func TestRootedExecutionParametersCannotSurviveScalarReplacement(t *testing.T) {
	server := &RpcServer{}
	bank := simulationBankFixture(t)
	bank.BlockHeight = 30
	server.SetRootedBankState(42, 30, 70)
	server.SetRootedExecutionBank(bank)
	require.Same(t, bank, server.rootedBank.Load().executionBank)
	server.SetRootedBankState(43, 31, 71)
	server.SetRootedExecutionBank(bank)
	require.Nil(t, server.rootedBank.Load().executionBank)
	require.Equal(t, uint64(43), server.rootedBank.Load().Slot)
}
