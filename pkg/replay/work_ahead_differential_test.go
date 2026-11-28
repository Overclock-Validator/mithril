package replay

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func TestWorkAheadDifferentialCommit(t *testing.T) {
	enabled, predict, nonce, verify := workAheadEnabled, workAheadPredictPayer, workAheadNonce, workAheadVerify
	workAheadEnabled, workAheadPredictPayer, workAheadNonce, workAheadVerify = true, true, true, false
	t.Cleanup(func() {
		workAheadEnabled, workAheadPredictPayer, workAheadNonce, workAheadVerify = enabled, predict, nonce, verify
	})

	payer := txfixture.PayerPubkey()
	dest := txfixture.DestPubkey()
	nonceKey := testPk(83)
	oldNonce := solana.Hash{0xAA}
	transfer := func(amount uint64) solana.Instruction {
		return system.NewTransferInstruction(amount, payer, dest).Build()
	}
	const tooMuch = 100_000_000_000
	advance := system.NewAdvanceNonceAccountInstruction(nonceKey, sealevel.SysvarRecentBlockHashesAddr, payer).Build()
	sign := func(hash solana.Hash, instructions ...solana.Instruction) *solana.Transaction {
		tx, err := solana.NewTransaction(instructions, hash, solana.TransactionPayer(payer))
		require.NoError(t, err)
		key := txfixture.PayerPrivateKey()
		_, err = tx.Sign(func(solana.PublicKey) *solana.PrivateKey { return &key })
		require.NoError(t, err)
		return tx
	}
	recent := txfixture.TestBlockhash()
	fails := sign(recent, transfer(tooMuch))
	succeeds := sign(recent, transfer(100))
	// A funded system-owned account cannot be invoked as a program.
	invalidProgram := testPk(84)
	feesOnly := sign(recent, solana.NewInstruction(invalidProgram, nil, nil))
	nonceFailure := sign(oldNonce, advance, transfer(tooMuch))
	nonceSuccess := sign(oldNonce, advance, transfer(1))

	slot := func(t *testing.T) *sealevel.SlotCtx {
		ctx, cleanup := newCommitTestSlotCtx()
		t.Cleanup(cleanup)
		state := sealevel.NonceStateVersions{Type: sealevel.NonceVersionCurrent, Current: sealevel.NonceData{
			IsInitialized: true, Authority: payer, DurableNonce: oldNonce,
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
		}}
		data, err := state.Marshal()
		require.NoError(t, err)
		require.NoError(t, ctx.Accounts.SetAccountWithoutLock(nonceKey, &accounts.Account{
			Key: nonceKey, Owner: addresses.SystemProgramAddr, Lamports: 2_000_000, Data: data, RentEpoch: math.MaxUint64,
		}))
		require.NoError(t, ctx.Accounts.SetAccountWithoutLock(sealevel.SysvarRecentBlockHashesAddr, &accounts.Account{
			Key: sealevel.SysvarRecentBlockHashesAddr, Owner: addresses.SysvarOwnerAddr, Lamports: 1,
			Data: sealevel.SysvarCache.RecentBlockHashes.Sysvar.MustMarshal(),
		}))
		require.NoError(t, ctx.Accounts.SetAccountWithoutLock(invalidProgram, &accounts.Account{
			Key: invalidProgram, Owner: addresses.SystemProgramAddr, Lamports: 1_000_000, RentEpoch: math.MaxUint64,
		}))
		ctx.Accounts = accounts.NewOverlayAccounts(ctx.Accounts)
		return ctx
	}

	for _, tc := range []struct {
		name          string
		first, second *solana.Transaction
		adopt         bool
		wantErr       error
		capture       bool
	}{
		{"predicted fee debit", fails, succeeds, true, nil, false},
		{"successful prerequisite invalidates predicted debit", succeeds, feesOnly, false, TxErrInvalidProgramForExecution, false},
		{"fee-only load failure", fails, feesOnly, true, TxErrInvalidProgramForExecution, false},
		{"fee-only prerequisite", feesOnly, succeeds, true, nil, false},
		{"durable nonce success", fails, nonceSuccess, true, nil, false},
		{"durable nonce rollback", fails, nonceFailure, true, nil, false},
		{"failed nonce prerequisite", nonceFailure, succeeds, true, nil, false},
		{"reused durable nonce", nonceFailure, nonceSuccess, false, TxErrInvalidBlockhash, false},
		{"adopted execution retains history metadata", fails, succeeds, true, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, got := slot(t), slot(t)
			txs := []*solana.Transaction{tc.first, tc.second}
			w := newWorkAhead(got, &b.Block{Transactions: txs}, []bool{true, true}, nil, tc.capture)
			require.NotNil(t, w)
			// Supply the worker arena normally installed at node startup.
			pool := workAheadArenas
			workAheadArenas = make(chan *arena.Arena[sealevel.BorrowedAccount], 1)
			workAheadArenas <- arena.New[sealevel.BorrowedAccount](64)
			defer func() { workAheadArenas = pool }()
			defer clearWorkAheadRan(got.Slot)
			defer w.finish(got.Slot)
			// Force the same legal interleaving every time. Execution, claim,
			// fallback and commit all use the replay implementation.
			e := w.entry(1)
			require.True(t, w.queueLocked(e, 1, []uint32{0}, 1))
			w.run(<-w.queue)
			require.False(t, e.panicked)
			if tc.second == nonceSuccess {
				require.Nil(t, e.output.ProcessingResult.TransactionError, "early execution saw a usable nonce")
			}
			if tc.second == nonceFailure {
				failure := e.output.ProcessingResult.TransactionError
				require.NotNil(t, failure)
				require.ErrorIs(t, failure.InstructionError, sealevel.SystemProgErrResultWithNegativeLamports)
				require.True(t, e.output.ExecCtx.TransactionContext.NonceAcctAdvanced)
			}
			require.NotNil(t, e.predicted[[32]byte(payer)])
			require.Empty(t, got.Accounts.(*accounts.OverlayAccounts).DeltaAccounts())

			for i, tx := range txs {
				var adopted *LoadAndExecuteTransactionOutput
				if i == 1 {
					var release func()
					adopted, release = w.claim(i)
					defer release()
					require.Equal(t, tc.adopt, adopted != nil)
					if adopted != nil {
						canonical := LoadAndExecuteTransaction(transactionExecutionInput(want, tx, nil, nil, false, false, tc.capture))
						require.Empty(t, compareOutputs(adopted, &canonical))
						if tc.capture {
							require.IsType(t, &sealevel.LogRecorder{}, adopted.ExecCtx.Log)
						}
					}
				}
				var wantCapture, gotCapture *transactionMetadataCapture
				if tc.capture {
					wantCapture, gotCapture = &transactionMetadataCapture{}, &transactionMetadataCapture{}
				}
				wantFee, wantCU, wantErr := processTransaction(want, nil, tx, nil, nil, nil, false, nil, nil, wantCapture, nil)
				gotFee, gotCU, gotErr := processTransaction(got, nil, tx, nil, nil, nil, false, adopted, nil, gotCapture, nil)
				if tc.capture {
					require.NoError(t, wantCapture.Err)
					require.NoError(t, gotCapture.Err)
					require.NotNil(t, wantCapture.Record)
					require.Equal(t, wantCapture.Record, gotCapture.Record)
				}
				require.Equal(t, wantFee, gotFee)
				require.Equal(t, wantCU, gotCU)
				require.Equal(t, wantErr, gotErr)
				if i == 1 && tc.wantErr != nil {
					require.ErrorIs(t, gotErr, tc.wantErr)
				}
				switch tx {
				case succeeds:
					require.NoError(t, gotErr)
				case fails, nonceFailure:
					require.Error(t, gotErr)
					require.Positive(t, gotCU)
				case feesOnly:
					require.ErrorIs(t, gotErr, TxErrInvalidProgramForExecution)
					require.Zero(t, gotCU)
				}
				if tx == nonceSuccess && !tc.adopt {
					require.Nil(t, gotFee, "a consumed nonce cannot be charged again")
				} else {
					require.NotNil(t, gotFee)
					require.EqualValues(t, 5000, gotFee.TotalFee)
				}
				require.Equal(t, want.ModifiedAccts, got.ModifiedAccts)
				require.ElementsMatch(t, want.Accounts.(*accounts.OverlayAccounts).DeltaAccounts(), got.Accounts.(*accounts.OverlayAccounts).DeltaAccounts())
				if tx == nonceFailure || (tx == nonceSuccess && tc.adopt) {
					acct, err := got.GetAccount(nonceKey)
					require.NoError(t, err)
					state, err := sealevel.UnmarshalNonceStateVersions(acct.Data)
					require.NoError(t, err)
					require.NotEqual(t, oldNonce, solana.Hash(state.State().DurableNonce))
				}
			}
		})
	}
}
