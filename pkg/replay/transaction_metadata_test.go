package replay

import (
	"encoding/json"
	"errors"
	"maps"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func TestTransactionMetadataCommittedBalances(t *testing.T) {
	for _, failed := range []bool{false, true} {
		for _, removeADH := range []bool{false, true} {
			var baseline []*accounts.Account
			var modified, writable map[solana.PublicKey]bool
			var baselineFee, baselineUnits uint64
			for _, enabled := range []bool{false, true} {
				ctx, cleanup := newCommitTestSlotCtx()
				func() {
					defer cleanup()
					if removeADH {
						ctx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)
					}
					instructions := []solana.Instruction{system.NewTransferInstruction(8, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build()}
					if failed {
						instructions = append(instructions, system.NewTransferInstruction(20_000_000_000, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build())
					}
					tx := signLifecycleInstructions(t, instructions...)
					var capture *transactionMetadataCapture
					if enabled {
						capture = &transactionMetadataCapture{}
					}
					var wg sync.WaitGroup
					fee, units, executionErr := processTransaction(ctx, &wg, tx, nil, nil, nil, false, capture)
					require.Equal(t, failed, executionErr != nil)
					require.NotNil(t, fee)
					var state []*accounts.Account
					for _, key := range tx.Message.AccountKeys {
						account, err := ctx.GetAccount(key)
						require.NoError(t, err)
						state = append(state, account)
					}
					if !enabled {
						baseline, baselineFee, baselineUnits = state, fee.TotalFee, units
						modified, writable = maps.Clone(ctx.ModifiedAccts), maps.Clone(ctx.WritableAccts)
						return
					}
					require.Equal(t, baseline, state)
					require.Equal(t, baselineFee, fee.TotalFee)
					require.Equal(t, baselineUnits, units)
					require.Equal(t, modified, ctx.ModifiedAccts)
					require.Equal(t, writable, ctx.WritableAccts)
					require.NoError(t, capture.Err)
					record := capture.Record
					require.NotNil(t, record)
					require.Equal(t, fee.TotalFee, record.Fee)
					require.Equal(t, units, record.ComputeUnitsConsumed)
					for i, key := range record.AccountKeys {
						require.Equal(t, state[i].Lamports, record.PostBalances[i])
						switch key {
						case txfixture.PayerPubkey():
							require.Equal(t, uint64(10_000_000_000), record.PreBalances[i])
						case txfixture.DestPubkey():
							require.Equal(t, uint64(10_000_000), record.PreBalances[i])
							if failed {
								require.Equal(t, record.PreBalances[i], record.PostBalances[i])
							}
						case solana.SystemProgramID:
							require.Equal(t, uint64(1), record.PreBalances[i])
							require.Equal(t, uint64(1), record.PostBalances[i])
						}
					}
					if failed {
						var status map[string][]json.RawMessage
						require.NoError(t, json.Unmarshal(record.Error, &status))
						require.Equal(t, json.RawMessage("1"), status["InstructionError"][0])
					} else {
						require.JSONEq(t, "null", string(record.Error))
					}
					wire, err := tx.MarshalBinary()
					require.NoError(t, err)
					require.Equal(t, wire, record.Transaction)
					tx.Message.AccountKeys[0] = solana.PublicKey{}
					tx.Message.Instructions[0].Data[0] ^= 0xff
					require.Equal(t, wire, record.Transaction)
					require.Equal(t, txfixture.PayerPubkey(), record.AccountKeys[0])
				}()
			}
		}
	}
}

func TestTransactionMetadataNoOpAndRejected(t *testing.T) {
	for _, noOp := range []bool{false, true} {
		ctx, cleanup := newCommitTestSlotCtx()
		func() {
			defer cleanup()
			ctx.Features.EnableFeature(features.EnableTxV1, 0)
			if noOp {
				ctx.Features.EnableFeature(features.RelaxFeePayerConstraint, 0)
			}
			tx := signedV1Transfer(t, solana.TransactionConfig{}.WithComputeUnitLimit(12_345).WithLoadedAccountsDataSizeLimit(54_321))
			payer, err := ctx.GetAccount(txfixture.PayerPubkey())
			require.NoError(t, err)
			payer.Owner = addresses.VoteProgramAddr
			require.NoError(t, ctx.SetAccount(txfixture.PayerPubkey(), payer))
			capture := transactionMetadataCapture{Record: &transactionMetadata{}, Err: errors.New("previous capture")}
			var wg sync.WaitGroup
			fee, _, executionErr := processTransaction(ctx, &wg, tx, nil, nil, nil, false, &capture)
			require.Error(t, executionErr)
			require.NoError(t, capture.Err)
			if !noOp {
				require.Nil(t, fee)
				require.Nil(t, capture.Record)
				return
			}
			require.NotNil(t, fee)
			require.NotNil(t, capture.Record)
			require.Zero(t, capture.Record.Fee)
			require.Zero(t, capture.Record.ComputeUnitsConsumed)
			require.Equal(t, capture.Record.PreBalances, capture.Record.PostBalances)
			require.JSONEq(t, `"InvalidAccountForFee"`, string(capture.Record.Error))
		}()
	}
}

func TestTransactionMetadataVersionedWire(t *testing.T) {
	for _, version := range []solana.MessageVersion{solana.MessageVersionV0, solana.MessageVersionV1} {
		ctx, cleanup := newCommitTestSlotCtx()
		func() {
			defer cleanup()
			var tx *solana.Transaction
			var wire []byte
			var err error
			if version == solana.MessageVersionV0 {
				wire = signedV0TransferViaTableWire(t, 7)
				tx, err = solana.TransactionFromBytes(wire)
				require.NoError(t, err)
				_, err = prepareTransactionMetadata(ctx, tx, nil)
				require.ErrorContains(t, err, "requires resolved address lookups")
				require.NoError(t, tx.Message.SetAddressTables(map[solana.PublicKey]solana.PublicKeySlice{lifecycleTableKey: {txfixture.DestPubkey()}}))
				require.NoError(t, tx.Message.ResolveLookups())
			} else {
				ctx.Features.EnableFeature(features.EnableTxV1, 0)
				tx = signedV1Transfer(t, solana.TransactionConfig{}.WithComputeUnitLimit(200_000).WithLoadedAccountsDataSizeLimit(54_321))
				wire, err = tx.MarshalBinary()
				require.NoError(t, err)
			}
			var capture transactionMetadataCapture
			fee, _, err := processTransaction(ctx, &sync.WaitGroup{}, tx, nil, nil, nil, false, &capture)
			require.NoError(t, err)
			require.NotNil(t, fee)
			require.NoError(t, capture.Err)
			require.NotNil(t, capture.Record)
			require.Equal(t, wire, capture.Record.Transaction)
			require.Equal(t, []solana.PublicKey(tx.Message.AccountKeys), capture.Record.AccountKeys)
			decoded, err := solana.TransactionFromBytes(capture.Record.Transaction)
			require.NoError(t, err)
			require.Equal(t, version, decoded.Message.GetVersion())
		}()
	}
}

func TestTransactionMetadataRejectsOversizedInput(t *testing.T) {
	ctx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(7))
	require.NoError(t, err)
	tx.Message.Instructions[0].Data = make([]byte, 1233)
	metadata, err := prepareTransactionMetadata(ctx, tx, nil)
	require.Error(t, err)
	require.Nil(t, metadata)
}

type metadataAccountReader func(uint64, solana.PublicKey) (*accounts.Account, error)

func (read metadataAccountReader) GetAccount(slot uint64, key solana.PublicKey) (*accounts.Account, error) {
	return read(slot, key)
}

func (read metadataAccountReader) GetAccountWithDataLimit(slot uint64, key solana.PublicKey, limit uint64) (*accounts.Account, error) {
	account, err := read(slot, key)
	if err == nil && account != nil && uint64(len(account.Data)) > limit {
		return nil, accountsdb.ErrAccountDataLimit
	}
	return account, err
}

type metadataFailingAccounts struct {
	accounts.Accounts
	err    error
	failed bool
}

func (view *metadataFailingAccounts) GetAccount(key *[32]byte) (*accounts.Account, error) {
	if *key == addresses.SystemProgramAddr && !view.failed {
		view.failed = true
		return nil, view.err
	}
	return view.Accounts.GetAccount(key)
}

func TestTransactionMetadataReadFailureDoesNotChangeExecution(t *testing.T) {
	ctx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	readErr := errors.New("account read failed")
	ctx.Accounts = &metadataFailingAccounts{Accounts: ctx.Accounts, err: readErr}
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(7))
	require.NoError(t, err)
	var wg sync.WaitGroup
	var capture transactionMetadataCapture
	fee, _, executionErr := processTransaction(ctx, &wg, tx, nil, nil, nil, false, &capture)
	require.NoError(t, executionErr)
	require.NotNil(t, fee)
	require.ErrorIs(t, capture.Err, readErr)
	require.Nil(t, capture.Record)
	destination, err := ctx.GetAccount(txfixture.DestPubkey())
	require.NoError(t, err)
	require.Equal(t, uint64(10_000_008), destination.Lamports)
}

func TestTransactionMetadataAccountViews(t *testing.T) {
	key := solana.PublicKey{7}
	ctx := &sealevel.SlotCtx{Slot: 42, Accounts: accounts.NewMemAccounts()}
	_, err := transactionAccountBalances(ctx, []solana.PublicKey{key})
	require.ErrorContains(t, err, "no backing account view")
	readErr := errors.New("storage read failed")
	for _, tc := range []struct {
		account *accounts.Account
		err     error
		want    uint64
		valid   bool
	}{
		{account: &accounts.Account{Lamports: 12}, want: 12, valid: true},
		{err: accountsdb.ErrNoAccount, valid: true},
		{err: readErr},
		{},
	} {
		ctx.UnrootedRead = metadataAccountReader(func(slot uint64, got solana.PublicKey) (*accounts.Account, error) {
			require.Equal(t, ctx.Slot, slot)
			require.Equal(t, key, got)
			return tc.account, tc.err
		})
		balances, err := transactionAccountBalances(ctx, []solana.PublicKey{key})
		require.Equal(t, tc.valid, err == nil)
		if tc.valid {
			require.Equal(t, []uint64{tc.want}, balances)
		}
	}
	require.NoError(t, ctx.SetAccount(key, &accounts.Account{Key: key}))
	ctx.UnrootedRead = metadataAccountReader(func(uint64, solana.PublicKey) (*accounts.Account, error) {
		t.Fatal("a local tombstone must not fall through")
		return nil, readErr
	})
	balances, err := transactionAccountBalances(ctx, []solana.PublicKey{key})
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, balances)
}

func BenchmarkProcessTransactionMetadata(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		b.Run(name, func(b *testing.B) {
			ctx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
			require.NoError(b, err)
			var capture *transactionMetadataCapture
			if enabled {
				capture = &transactionMetadataCapture{}
			}
			var wg sync.WaitGroup
			b.ReportAllocs()
			for b.Loop() {
				fee, _, err := processTransaction(ctx, &wg, tx, nil, nil, nil, false, capture)
				if err != nil || fee == nil {
					b.Fatalf("execution: %v", err)
				}
				if capture != nil && (capture.Err != nil || capture.Record == nil) {
					b.Fatalf("capture: %v", capture.Err)
				}
			}
		})
	}
}
