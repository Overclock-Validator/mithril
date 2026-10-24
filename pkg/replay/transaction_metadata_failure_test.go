package replay

import (
	"crypto/sha256"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

type metadataPublicationAccounts struct {
	accounts.Accounts
	panicOnWrite bool
	wrote        bool
	postReadErr  error
}

func (view *metadataPublicationAccounts) SetAccount(key *[32]byte, account *accounts.Account) error {
	if view.panicOnWrite {
		panic("metadata test publication panic")
	}
	if err := view.Accounts.SetAccount(key, account); err != nil {
		return err
	}
	view.wrote = true
	return nil
}

func (view *metadataPublicationAccounts) GetAccount(key *[32]byte) (*accounts.Account, error) {
	if view.wrote && view.postReadErr != nil && *key == addresses.SystemProgramAddr {
		return nil, view.postReadErr
	}
	return view.Accounts.GetAccount(key)
}

func metadataFailureState(t *testing.T, view accounts.Accounts, keys []solana.PublicKey) []*accounts.Account {
	t.Helper()
	var state []*accounts.Account
	for _, key := range keys {
		account, err := view.GetAccount((*[32]byte)(&key))
		require.NoError(t, err)
		state = append(state, account.Clone())
	}
	return state
}

func TestTransactionMetadataPanicLeavesNoRecord(t *testing.T) {
	var baseline []*accounts.Account
	for _, enabled := range []bool{false, true} {
		ctx, cleanup := newCommitTestSlotCtx()
		func() {
			defer cleanup()
			base := ctx.Accounts
			ctx.Accounts = &metadataPublicationAccounts{Accounts: base, panicOnWrite: true}
			tx := signLifecycleInstructions(t, system.NewTransferInstruction(8, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build())
			before := metadataFailureState(t, base, tx.Message.AccountKeys)
			var capture *transactionMetadataCapture
			if enabled {
				capture = &transactionMetadataCapture{Record: &transactionMetadata{}, Err: errors.New("stale capture")}
			}
			var wg sync.WaitGroup
			require.PanicsWithValue(t, "metadata test publication panic", func() {
				processTransaction(ctx, &wg, tx, nil, nil, nil, false, capture)
			})
			wg.Wait()
			state := metadataFailureState(t, base, tx.Message.AccountKeys)
			require.Equal(t, before, state)
			if !enabled {
				baseline = state
				return
			}
			require.Equal(t, baseline, state)
			require.Nil(t, capture.Record)
			require.NoError(t, capture.Err)
		}()
	}
}

func TestTransactionMetadataPostReadFailurePreservesCommit(t *testing.T) {
	var baseline []*accounts.Account
	var baselineFee, baselineUnits uint64
	readErr := errors.New("post-commit account read failed")
	for _, enabled := range []bool{false, true} {
		ctx, cleanup := newCommitTestSlotCtx()
		func() {
			defer cleanup()
			base := ctx.Accounts
			view := &metadataPublicationAccounts{Accounts: base, postReadErr: readErr}
			ctx.Accounts = view
			tx := signLifecycleInstructions(t, system.NewTransferInstruction(8, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build())
			var capture *transactionMetadataCapture
			if enabled {
				capture = &transactionMetadataCapture{}
			}
			var wg sync.WaitGroup
			fee, units, err := processTransaction(ctx, &wg, tx, nil, nil, nil, false, capture)
			wg.Wait()
			require.NoError(t, err)
			require.NotNil(t, fee)
			require.True(t, view.wrote)
			state := metadataFailureState(t, base, tx.Message.AccountKeys)
			require.Equal(t, uint64(10_000_000_000-5_000-8), state[0].Lamports)
			require.Equal(t, uint64(10_000_008), state[1].Lamports)
			if !enabled {
				baseline, baselineFee, baselineUnits = state, fee.TotalFee, units
				return
			}
			require.Equal(t, baseline, state)
			require.Equal(t, baselineFee, fee.TotalFee)
			require.Equal(t, baselineUnits, units)
			require.ErrorIs(t, capture.Err, readErr)
			require.ErrorContains(t, capture.Err, "read post-balances")
			require.Nil(t, capture.Record)
		}()
	}
}

func TestTransactionMetadataFeesOnlyAndNonceRollback(t *testing.T) {
	for _, kind := range []string{"fees-only", "nonce fees-only", "nonce instruction failure"} {
		t.Run(kind, func(t *testing.T) {
			var baseline []*accounts.Account
			var baselineFee, baselineUnits uint64
			for _, enabled := range []bool{false, true} {
				ctx, cleanup := newCommitTestSlotCtx()
				func() {
					defer cleanup()
					payerKey := txfixture.PayerPubkey()
					payer, err := ctx.GetAccount(payerKey)
					require.NoError(t, err)
					payer.RentEpoch = 7
					require.NoError(t, ctx.SetAccount(payerKey, payer))
					blockhash := txfixture.TestBlockhash()
					var instructions []solana.Instruction
					nonceKey := solana.PublicKey{0xD5}
					initialNonce := solana.Hash{0xAA}
					usesNonce := kind != "fees-only"
					if usesNonce {
						// Reuse the durable-nonce recipe from failed_publication_integration_test.
						nonce := sealevel.NonceStateVersions{Type: sealevel.NonceVersionCurrent, Current: sealevel.NonceData{
							IsInitialized: true, Authority: payerKey, DurableNonce: initialNonce,
							FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
						}}
						data, err := nonce.Marshal()
						require.NoError(t, err)
						require.NoError(t, ctx.SetAccount(nonceKey, &accounts.Account{Key: nonceKey, Owner: addresses.SystemProgramAddr,
							Lamports: 10_000_000, Data: data, RentEpoch: math.MaxUint64}))
						ctx.LastBlockhash = [32]byte{0x77}
						blockhash = initialNonce
						recent := sealevel.SysvarCache.RecentBlockHashes.Sysvar
						require.NoError(t, ctx.SetAccount(solana.SysVarRecentBlockHashesPubkey, &accounts.Account{
							Key: solana.SysVarRecentBlockHashesPubkey, Owner: addresses.SysvarOwnerAddr, Lamports: 1,
							Data: recent.MustMarshal(), RentEpoch: math.MaxUint64,
						}))
						instructions = append(instructions, system.NewAdvanceNonceAccountInstruction(nonceKey, solana.SysVarRecentBlockHashesPubkey, payerKey).Build())
					}
					if kind == "nonce instruction failure" {
						instructions = append(instructions, solana.NewInstruction(addresses.SystemProgramAddr, nil, []byte{0xff, 0xff, 0xff, 0xff}))
					} else {
						// A present tombstone produces a processable program-load failure.
						missingProgram := solana.PublicKey{0xD6}
						require.NoError(t, ctx.SetAccount(missingProgram, &accounts.Account{Key: missingProgram}))
						instructions = append(instructions, solana.NewInstruction(missingProgram, nil, nil))
					}
					tx, err := solana.NewTransaction(instructions, blockhash, solana.TransactionPayer(payerKey))
					require.NoError(t, err)
					privateKey := txfixture.PayerPrivateKey()
					_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
						if key == payerKey {
							return &privateKey
						}
						return nil
					})
					require.NoError(t, err)
					before := metadataFailureState(t, ctx.Accounts, tx.Message.AccountKeys)
					var capture *transactionMetadataCapture
					if enabled {
						capture = &transactionMetadataCapture{}
					}
					var wg sync.WaitGroup
					fee, units, executionErr := processTransaction(ctx, &wg, tx, nil, nil, nil, false, capture)
					wg.Wait()
					require.Error(t, executionErr)
					require.NotNil(t, fee)
					require.Equal(t, uint64(5000), fee.TotalFee)
					state := metadataFailureState(t, ctx.Accounts, tx.Message.AccountKeys)
					require.Equal(t, before[0].Lamports-fee.TotalFee, state[0].Lamports)
					if usesNonce {
						require.Equal(t, uint64(math.MaxUint64), state[0].RentEpoch)
						nonceAccount, err := ctx.GetAccount(nonceKey)
						require.NoError(t, err)
						nonce, err := sealevel.UnmarshalNonceStateVersions(nonceAccount.Data)
						require.NoError(t, err)
						want := sha256.Sum256(append([]byte("DURABLE_NONCE"), ctx.LastBlockhash[:]...))
						require.Equal(t, want, nonce.State().DurableNonce)
					} else {
						require.Equal(t, uint64(7), state[0].RentEpoch)
					}
					if !enabled {
						baseline, baselineFee, baselineUnits = state, fee.TotalFee, units
						return
					}
					require.Equal(t, baseline, state)
					require.Equal(t, baselineFee, fee.TotalFee)
					require.Equal(t, baselineUnits, units)
					require.NoError(t, capture.Err)
					require.NotNil(t, capture.Record)
					require.Equal(t, fee.TotalFee, capture.Record.Fee)
					require.Equal(t, units, capture.Record.ComputeUnitsConsumed)
					for i := range state {
						require.Equal(t, before[i].Lamports, capture.Record.PreBalances[i])
						require.Equal(t, state[i].Lamports, capture.Record.PostBalances[i])
					}
					if kind == "nonce instruction failure" {
						require.JSONEq(t, `{"InstructionError":[1,"InvalidInstructionData"]}`, string(capture.Record.Error))
					} else {
						require.JSONEq(t, `"ProgramAccountNotFound"`, string(capture.Record.Error))
					}
				}()
			}
		})
	}
}
