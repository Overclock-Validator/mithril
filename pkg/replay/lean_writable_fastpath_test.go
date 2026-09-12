package replay

import (
	"math"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeanWritableFastPathOmitsExecutionResultWhenADHDisabled(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	slotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)

	destBefore, err := slotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)

	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:     slotCtx,
		Transaction: tx,
		LeanResult:  true,
	})
	require.Nil(t, output.ProcessingResult.TransactionError)
	require.NotNil(t, output.ExecCtx)
	assert.Nil(t, output.ProcessingResult.ProcessedTransaction)
	assert.Nil(t, output.ExecutionResult, "ADH-disabled lean replay must not materialize writable result collections")

	require.NoError(t, ApplySuccessfulTransaction(slotCtx, output))
	assert.Empty(t, slotCtx.WritableAccts, "ADH-removed lean publication must not build the unused writable set")
	destAfter, err := slotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	assert.Greater(t, destAfter.Lamports, destBefore.Lamports, "omitting the result must not skip touched-account publication")
}

func TestRecordModifiedAcctOnlyTracksLegacyWritableState(t *testing.T) {
	key := txfixture.DestPubkey()

	t.Run("accounts delta hash removed", func(t *testing.T) {
		slotCtx, cleanup := newCommitTestSlotCtx()
		defer cleanup()
		slotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)

		slotCtx.RecordModifiedAcct(key)
		assert.Contains(t, slotCtx.ModifiedAccts, key)
		assert.NotContains(t, slotCtx.WritableAccts, key)
	})

	t.Run("legacy accounts delta hash", func(t *testing.T) {
		slotCtx, cleanup := newCommitTestSlotCtx()
		defer cleanup()

		slotCtx.RecordModifiedAcct(key)
		assert.Contains(t, slotCtx.ModifiedAccts, key)
		assert.Contains(t, slotCtx.WritableAccts, key)
	})
}

func TestLeanWritableFastPathPreservesExecutionResultWhenADHEnabled(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	require.False(t, slotCtx.Features.IsActive(features.RemoveAccountsDeltaHash))

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)

	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:     slotCtx,
		Transaction: tx,
		LeanResult:  true,
	})
	require.Nil(t, output.ProcessingResult.TransactionError)
	require.NotNil(t, output.ExecutionResult, "legacy ADH still consumes the complete writable account set")
	assert.ElementsMatch(t,
		[]solana.PublicKey{txfixture.PayerPubkey(), txfixture.DestPubkey()},
		output.ExecutionResult.WritableAccounts,
	)
	assert.Len(t, output.ExecutionResult.WritableAccountSet, len(output.ExecutionResult.WritableAccounts))
	for _, key := range output.ExecutionResult.WritableAccounts {
		assert.Contains(t, output.ExecutionResult.WritableAccountSet, key)
	}

	require.NoError(t, ApplySuccessfulTransaction(slotCtx, output))
	assert.Contains(t, slotCtx.WritableAccts, txfixture.PayerPubkey())
	assert.Contains(t, slotCtx.WritableAccts, txfixture.DestPubkey())
}

func TestProcessTransactionUsesLeanWritableFastPath(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	slotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)
	destBefore, err := slotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)

	var sigverify sync.WaitGroup
	feeInfo, computeUnits, err := ProcessTransaction(
		slotCtx,
		&sigverify,
		tx,
		nil,
		nil,
		nil,
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, feeInfo)
	assert.NotZero(t, computeUnits)
	destAfter, err := slotCtx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	assert.Greater(t, destAfter.Lamports, destBefore.Lamports)
	assert.Contains(t, slotCtx.ModifiedAccts, txfixture.DestPubkey())
}

func TestLeanWritableFastPathKeepsRichResultWhenADHDisabled(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	slotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)

	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)

	output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
		SlotCtx:     slotCtx,
		Transaction: tx,
	})
	require.Nil(t, output.ProcessingResult.TransactionError)
	require.NotNil(t, output.ProcessingResult.ProcessedTransaction)
	require.NotNil(t, output.ExecutionResult)
	assert.NotEmpty(t, output.ExecutionResult.AccountUpdates)
	assert.NotEmpty(t, output.ExecutionResult.WritableAccounts)
	assert.NotEmpty(t, output.ExecutionResult.WritableAccountSet)
	require.NoError(t, ApplySuccessfulTransaction(slotCtx, output))
	assert.Empty(t, slotCtx.WritableAccts, "ADH-removed rich publication must not build the unused writable set")
	assert.Contains(t, slotCtx.ModifiedAccts, txfixture.DestPubkey())
}

func TestApplySuccessfulTransactionLegacyRecordsWritableUntouchedAccount(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	require.False(t, slotCtx.Features.IsActive(features.RemoveAccountsDeltaHash))

	key := txfixture.DestPubkey()
	acct, err := slotCtx.GetAccount(key)
	require.NoError(t, err)
	execCtx := &sealevel.ExecutionCtx{
		Features: *slotCtx.Features,
		TransactionContext: &sealevel.TransactionCtx{
			Accounts: sealevel.TransactionAccounts{
				Accounts:  []*accounts.Account{acct},
				Shared:    []bool{true},
				Locked:    []bool{false},
				Touched:   []bool{false},
				AcctMetas: []*sealevel.AccountMeta{{Pubkey: key, IsWritable: true}},
			},
		},
	}
	output := LoadAndExecuteTransactionOutput{
		ExecCtx: execCtx,
		ExecutionResult: &TransactionExecutionResult{
			WritableAccounts:   []solana.PublicKey{key},
			WritableAccountSet: map[solana.PublicKey]struct{}{key: {}},
		},
	}

	require.NoError(t, ApplySuccessfulTransaction(slotCtx, output))
	assert.Contains(t, slotCtx.WritableAccts, key, "legacy ADH requires writable-but-untouched accounts")
	assert.NotContains(t, slotCtx.ModifiedAccts, key)
}

func TestApplySuccessfulTransactionRejectsNilResultWhenADHEnabled(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	require.False(t, slotCtx.Features.IsActive(features.RemoveAccountsDeltaHash))

	key := txfixture.DestPubkey()
	acct, err := slotCtx.GetAccount(key)
	require.NoError(t, err)
	output := LoadAndExecuteTransactionOutput{
		ExecCtx: &sealevel.ExecutionCtx{
			Features: *slotCtx.Features,
			TransactionContext: &sealevel.TransactionCtx{
				Accounts: sealevel.TransactionAccounts{
					Accounts:  []*accounts.Account{acct},
					Shared:    []bool{false},
					Locked:    []bool{false},
					Touched:   []bool{true},
					AcctMetas: []*sealevel.AccountMeta{{Pubkey: key, IsWritable: true}},
				},
			},
		},
	}

	require.Error(t, ApplySuccessfulTransaction(slotCtx, output))
	assert.NotContains(t, slotCtx.ModifiedAccts, key, "invalid nil result must be rejected before publication")
}

func TestLeanWritableFastPathRejectsFailedOutputBeforePublication(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	slotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)

	key := solana.PublicKey{0xD0}
	output := LoadAndExecuteTransactionOutput{
		ProcessingResult: TransactionProcessingResult{
			TransactionError: &TransactionError{ErrorType: TransactionErrorInstructionError},
		},
		ExecCtx: &sealevel.ExecutionCtx{
			TransactionContext: &sealevel.TransactionCtx{
				Accounts: sealevel.TransactionAccounts{
					Accounts:  []*accounts.Account{{Key: key, Lamports: 1, Owner: addresses.StakeProgramAddr, Data: []byte{1, 0, 0, 0}}},
					Shared:    []bool{false},
					Locked:    []bool{false},
					Touched:   []bool{true},
					AcctMetas: []*sealevel.AccountMeta{{Pubkey: key, IsWritable: true}},
				},
			},
		},
	}

	require.Error(t, ApplySuccessfulTransaction(slotCtx, output))
	assert.NotContains(t, slotCtx.ModifiedAccts, key)
}

func TestLeanWritableFastPathPreservesWritableStakeAndVoteBookkeeping(t *testing.T) {
	global.ClearPendingStakePubkeys()
	resetVoteStakeDirty()
	defer global.ClearPendingStakePubkeys()
	defer resetVoteStakeDirty()

	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	slotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)

	stakeKey := solana.PublicKey{0xD1}
	voteKey := solana.PublicKey{0xD2}
	global.PutVoteCacheItem(voteKey, &sealevel.VoteStateVersions{})
	defer global.DeleteVoteCacheItem(voteKey)

	// These accounts are writable but deliberately untouched. A non-zero
	// stake-state discriminator is enough for recordStakeDelegation to classify
	// the stake account as initialized.
	stakeAcct := &accounts.Account{
		Key:       stakeKey,
		Lamports:  1,
		Data:      []byte{1, 0, 0, 0},
		Owner:     addresses.StakeProgramAddr,
		RentEpoch: math.MaxUint64,
	}
	// Model a formerly cached vote account that was reassigned away from the
	// vote program. Legacy all-writable bookkeeping must evict its stale cache.
	reassignedVoteAcct := &accounts.Account{
		Key:       voteKey,
		Lamports:  1,
		Owner:     addresses.SystemProgramAddr,
		RentEpoch: math.MaxUint64,
	}

	txAccounts := sealevel.TransactionAccounts{
		Accounts: []*accounts.Account{stakeAcct, reassignedVoteAcct},
		Shared:   []bool{false, false},
		Locked:   []bool{false, false},
		Touched:  []bool{false, false},
		AcctMetas: []*sealevel.AccountMeta{
			{Pubkey: stakeKey, IsWritable: true},
			{Pubkey: voteKey, IsWritable: true},
		},
	}
	output := LoadAndExecuteTransactionOutput{
		ExecCtx: &sealevel.ExecutionCtx{
			SlotCtx: slotCtx,
			TransactionContext: &sealevel.TransactionCtx{
				Accounts: txAccounts,
			},
		},
		// ExecutionResult is deliberately nil: this is the ADH-disabled lean
		// contract. State publication derives from Touched, while vote/stake
		// bookkeeping preserves the legacy effective-writable-account contract.
	}

	require.NoError(t, ApplySuccessfulTransaction(slotCtx, output))
	require.Nil(t, global.VoteCacheItem(voteKey), "reassigned writable vote account must be removed from the vote cache")
	pending := global.PendingStakeEntriesSnapshot()
	require.Len(t, pending, 1)
	assert.Equal(t, stakeKey, pending[0].Pubkey)
	assert.Equal(t, slotCtx.Slot, voteStakeDirtySlot.Load())
	assert.NotContains(t, slotCtx.ModifiedAccts, stakeKey, "untouched state must not be published")
	assert.NotContains(t, slotCtx.ModifiedAccts, voteKey, "untouched state must not be published")
}

func TestTransactionAccountIsWritableUnionsDuplicateKeys(t *testing.T) {
	payerKey := solana.PublicKey{0xE0}
	duplicateKey := solana.PublicKey{0xE1}
	readonlyKey := solana.PublicKey{0xE2}
	execCtx := &sealevel.ExecutionCtx{
		TransactionContext: &sealevel.TransactionCtx{
			Accounts: sealevel.TransactionAccounts{
				Accounts: []*accounts.Account{
					{Key: payerKey},
					{Key: duplicateKey},
					{Key: duplicateKey},
					{Key: readonlyKey},
				},
				AcctMetas: []*sealevel.AccountMeta{
					{Pubkey: payerKey},
					{Pubkey: duplicateKey},
					{Pubkey: duplicateKey, IsWritable: true},
					{Pubkey: readonlyKey},
				},
			},
		},
	}

	assert.True(t, transactionAccountIsWritable(execCtx, 0), "payer is always writable")
	assert.True(t, transactionAccountIsWritable(execCtx, 1), "a duplicate inherits writability from every matching meta")
	assert.True(t, transactionAccountIsWritable(execCtx, 2))
	assert.False(t, transactionAccountIsWritable(execCtx, 3))
}

func TestCompileLeaderAccountsGatesWritableListOnADH(t *testing.T) {
	for _, tc := range []struct {
		name         string
		removeADH    bool
		wantWritable bool
	}{
		{name: "removed", removeADH: true, wantWritable: false},
		{name: "enabled", removeADH: false, wantWritable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slotCtx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			if tc.removeADH {
				slotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)
			}

			key := txfixture.DestPubkey()
			slotCtx.WritableAccts[key] = true
			slotCtx.ModifiedAccts[key] = true
			writable, modified := compileLeaderAccounts(slotCtx, &b.Block{}, nil)

			if tc.wantWritable {
				require.Len(t, writable, 1)
				assert.Equal(t, key, writable[0].Key)
			} else {
				assert.Empty(t, writable)
			}
			require.Len(t, modified, 1, "ADH removal must not discard LtHash/store inputs")
			assert.Equal(t, key, modified[0].Key)
		})
	}
}

func TestNewSlotCtxRetainsCanonicalModifiedJournalAfterADHRemoval(t *testing.T) {
	featureSet := features.NewFeaturesDefault()
	featureSet.EnableFeature(features.RemoveAccountsDeltaHash, 0)
	parent := accounts.NewMemAccounts()
	overlay := accounts.NewOverlayAccounts(parent)
	block := &b.Block{
		Slot:     43,
		Features: featureSet,
	}
	slotCtx := newSlotCtx(block, overlay, parent, nil, nil, 8)

	key := solana.PublicKey{0xa1}
	acct := &accounts.Account{Key: key, Lamports: 10}
	require.NoError(t, slotCtx.SetAccount(key, acct))
	slotCtx.RecordModifiedAcct(key)

	assert.Contains(t, slotCtx.ModifiedAccts, key,
		"the canonical journal remains required even when AccountsDeltaHash is removed")
	assert.Empty(t, slotCtx.WritableAccts,
		"AccountsDeltaHash removal may omit only the writable-account journal")
}

func TestCanonicalModifiedJournalDoesNotResurrectBurnedEpochVAT(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	slotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)
	overlay := accounts.NewOverlayAccounts(slotCtx.Accounts)
	slotCtx.Accounts = overlay

	stagedVAT := &accounts.Account{
		Key:       addresses.IncineratorAddr,
		Lamports:  76_800_000_000,
		Owner:     addresses.SystemProgramAddr,
		RentEpoch: math.MaxUint64,
	}
	require.NoError(t, slotCtx.SetAccount(addresses.IncineratorAddr, stagedVAT))

	runIncinerator(slotCtx)
	require.Equal(t, uint64(76_800_000_000), slotCtx.LamportsBurnt)

	_, modified := compileLeaderAccounts(slotCtx, &b.Block{EpochUpdatedAccts: []*accounts.Account{stagedVAT}}, nil)
	require.Len(t, modified, 1)
	require.Equal(t, solana.PublicKey(addresses.IncineratorAddr), modified[0].Key)
	require.Zero(t, modified[0].Lamports, "bank hash/store input must contain the post-burn account")
	require.Equal(t, uint64(math.MaxUint64), modified[0].RentEpoch)
}

func TestCompileWritableAndModifiedAcctsGatesWritableListOnADH(t *testing.T) {
	for _, tc := range []struct {
		name         string
		removeADH    bool
		wantWritable bool
	}{
		{name: "removed", removeADH: true, wantWritable: false},
		{name: "enabled", removeADH: false, wantWritable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slotCtx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			if tc.removeADH {
				slotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)
			}

			modifiedKey := txfixture.DestPubkey()
			slotCtx.WritableAccts[modifiedKey] = true
			slotCtx.ModifiedAccts[modifiedKey] = true

			epochAcct := &accounts.Account{Key: solana.PublicKey{0xF1}, Lamports: 1}
			require.NoError(t, slotCtx.SetAccount(epochAcct.Key, epochAcct))
			rentAcct := &accounts.Account{Key: solana.PublicKey{0xF2}, Lamports: 1}

			slotHistory := sealevel.SysvarSlotHistory{
				Bits: sealevel.SlotHistoryBitvec{
					Bits: sealevel.SlotHistoryInner{BlocksLen: 1, Blocks: []uint64{0}},
					Len:  64,
				},
				NextSlot: slotCtx.Slot,
			}

			clock := sealevel.SysvarClock{}
			slotHashes := sealevel.SysvarSlotHashes{}
			sysvarAccts := []*accounts.Account{
				{Key: sealevel.SysvarClockAddr, Lamports: 1, Data: clock.MustMarshal()},
				{Key: sealevel.SysvarRecentBlockHashesAddr, Lamports: 1, Data: make([]byte, 8+150*40)},
				{Key: sealevel.SysvarSlotHashesAddr, Lamports: 1, Data: slotHashes.MustMarshal()},
				{Key: sealevel.SysvarSlotHistoryAddr, Lamports: 1, Data: slotHistory.MustMarshal()},
			}
			for _, acct := range sysvarAccts {
				require.NoError(t, slotCtx.SetAccount(acct.Key, acct))
			}
			slotCtx.Blockhash = [32]byte{0xA5}
			require.NoError(t, finalizeBankSysvars(slotCtx))

			block := &b.Block{EpochUpdatedAccts: []*accounts.Account{epochAcct}}
			writable, modified := compileWritableAndModifiedAccts(slotCtx, block, []*accounts.Account{rentAcct})

			wantKeys := []solana.PublicKey{
				modifiedKey,
				epochAcct.Key,
				rentAcct.Key,
				sealevel.SysvarClockAddr,
				sealevel.SysvarRecentBlockHashesAddr,
				sealevel.SysvarSlotHashesAddr,
				sealevel.SysvarSlotHistoryAddr,
			}
			if tc.wantWritable {
				assert.ElementsMatch(t, wantKeys, replayTestAccountKeys(writable))
			} else {
				assert.Empty(t, writable)
			}
			assert.ElementsMatch(t, wantKeys, replayTestAccountKeys(modified), "ADH removal must not discard LtHash/store inputs")

			modifiedByKey := make(map[solana.PublicKey]*accounts.Account, len(modified))
			for _, acct := range modified {
				modifiedByKey[acct.Key] = acct
			}
			compiledRecent := modifiedByKey[sealevel.SysvarRecentBlockHashesAddr]
			compiledSlotHistory := modifiedByKey[sealevel.SysvarSlotHistoryAddr]
			require.NotNil(t, compiledRecent)
			require.NotNil(t, compiledSlotHistory)
			bankRecent, ok := slotCtx.BankSysvars().RecentBlockhashes()
			require.True(t, ok)
			expectedRecentData := bankRecent.MustMarshal()
			assert.Equal(t, expectedRecentData, compiledRecent.Data[:len(expectedRecentData)],
				"bank-hash input must retain the finalized RecentBlockhashes update")
			bankHistory, ok := slotCtx.BankSysvars().SlotHistory()
			require.True(t, ok)
			assert.Equal(t, bankHistory.MustMarshal(), compiledSlotHistory.Data,
				"bank-hash input must retain the finalized SlotHistory update")

			storedRecent, err := slotCtx.GetAccount(sealevel.SysvarRecentBlockHashesAddr)
			require.NoError(t, err)
			storedSlotHistory, err := slotCtx.GetAccount(sealevel.SysvarSlotHistoryAddr)
			require.NoError(t, err)
			assert.Equal(t, compiledRecent.Data, storedRecent.Data,
				"frozen SlotCtx and bank-hash input must use identical RecentBlockhashes bytes")
			assert.Equal(t, compiledSlotHistory.Data, storedSlotHistory.Data,
				"frozen SlotCtx and bank-hash input must use identical SlotHistory bytes")

			assert.Equal(t, slotCtx.Slot+1, bankHistory.NextSlot, "required sysvar updates must still run when ADH is removed")
			assert.NotZero(t, bankHistory.Bits.Bits.Blocks[0]&(uint64(1)<<(slotCtx.Slot%64)))
			assert.Equal(t, slotCtx.Blockhash, bankRecent[0].Blockhash)
		})
	}
}

func replayTestAccountKeys(accts []*accounts.Account) []solana.PublicKey {
	keys := make([]solana.PublicKey, 0, len(accts))
	for _, acct := range accts {
		if acct != nil {
			keys = append(keys, acct.Key)
		}
	}
	return keys
}

func BenchmarkLeanWritableFastPathResultMaterialization(b *testing.B) {
	for _, tc := range []struct {
		name       string
		disableADH bool
		wantResult bool
	}{
		{name: "ADH_disabled_fast_path", disableADH: true, wantResult: false},
		{name: "ADH_enabled_legacy", disableADH: false, wantResult: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.StopTimer()
			slotCtx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			if tc.disableADH {
				slotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)
			}
			tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
			require.NoError(b, err)
			b.ReportAllocs()
			b.StartTimer()

			for b.Loop() {
				output := LoadAndExecuteTransaction(LoadAndExecuteTransactionInput{
					SlotCtx:     slotCtx,
					Transaction: tx,
					LeanResult:  true,
				})
				if output.ProcessingResult.TransactionError != nil {
					b.Fatal(output.ProcessingResult.TransactionError)
				}
				if (output.ExecutionResult != nil) != tc.wantResult {
					b.Fatalf("ExecutionResult presence = %t, want %t", output.ExecutionResult != nil, tc.wantResult)
				}
			}
		})
	}
}
