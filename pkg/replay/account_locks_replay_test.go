package replay

import (
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func resignTransferWithPayer(t *testing.T, tx *solana.Transaction) {
	t.Helper()
	tx.Signatures = nil
	payerKey := txfixture.PayerPrivateKey()
	_, err := tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(txfixture.PayerPubkey()) {
			return &payerKey
		}
		return nil
	})
	require.NoError(t, err)
}

// duplicateAccountKeyTransfer names the destination twice:
// [payer, dest, system] -> [payer, dest, dest, system].
func duplicateAccountKeyTransfer(t *testing.T, seq uint64) *solana.Transaction {
	t.Helper()
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(seq))
	require.NoError(t, err)
	keys := tx.Message.AccountKeys
	require.Len(t, keys, 3)
	tx.Message.AccountKeys = solana.PublicKeySlice{keys[0], keys[1], keys[1], keys[2]}
	for i := range tx.Message.Instructions {
		if tx.Message.Instructions[i].ProgramIDIndex == 2 {
			tx.Message.Instructions[i].ProgramIDIndex = 3
		}
	}
	resignTransferWithPayer(t, tx)
	return tx
}

// tooManyAccountLocksTransfer names 3 static keys plus 62 looked-up addresses,
// one account more than the lock limit without IncreaseTxAccountLockLimit.
func tooManyAccountLocksTransfer(t *testing.T, seq uint64) *solana.Transaction {
	t.Helper()
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(seq))
	require.NoError(t, err)
	_, err = tx.Message.SetVersion(solana.MessageVersionV0)
	require.NoError(t, err)
	indexes := make(solana.Uint8SliceAsNum, 62)
	for i := range indexes {
		indexes[i] = uint8(i)
	}
	tx.Message.AddressTableLookups = solana.MessageAddressTableLookupSlice{{
		AccountKey:      solana.NewWallet().PublicKey(),
		ReadonlyIndexes: indexes,
	}}
	resignTransferWithPayer(t, tx)
	return tx
}

func TestProcessTransactionRejectsAccountLockViolationsWithoutCharging(t *testing.T) {
	cases := []struct {
		name string
		tx   func(*testing.T) *solana.Transaction
		want error
	}{
		{"duplicate account key", func(t *testing.T) *solana.Transaction { return duplicateAccountKeyTransfer(t, 11) }, TxErrAccountLoadedTwice},
		{"too many account locks", func(t *testing.T) *solana.Transaction { return tooManyAccountLocksTransfer(t, 12) }, TxErrTooManyAccountLocks},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slotCtx, cleanup := newCommitTestSlotCtx()
			defer cleanup()
			payerBefore, err := slotCtx.GetAccount(txfixture.PayerPubkey())
			require.NoError(t, err)

			var sigverify sync.WaitGroup
			feeInfo, cu, err := ProcessTransaction(slotCtx, &sigverify, tc.tx(t), nil, nil, nil, false)
			sigverify.Wait()

			require.ErrorIs(t, err, tc.want)
			require.Nil(t, feeInfo, "a transaction that cannot be processed must not return a fee")
			require.Zero(t, cu)
			payerAfter, err := slotCtx.GetAccount(txfixture.PayerPubkey())
			require.NoError(t, err)
			require.Equal(t, payerBefore.Lamports, payerAfter.Lamports)
		})
	}
}

func TestStreamingGroupRejectsDuplicateAccountKeys(t *testing.T) {
	for _, workers := range []int{0, 4} {
		env := newGroupExecutionEnv(t, workers, 10_000_000)
		defer env.cleanup()
		txs := transferTransactions(t, 2, 1)
		txs[1] = duplicateAccountKeyTransfer(t, 3)
		err := env.exec.executeTransactionGroup(txs, nil, false)
		require.ErrorIs(t, err, TxErrAccountLoadedTwice, "workers=%d", workers)
	}
}
