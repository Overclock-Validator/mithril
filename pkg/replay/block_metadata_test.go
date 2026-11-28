package replay

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func blockMetadataGroupOutcome(t *testing.T, env *groupExecutionEnv) groupExecutionOutcome {
	t.Helper()
	parent, err := env.parent.GetAccountWithoutLock(txfixture.PayerPubkey())
	require.NoError(t, err)
	return groupExecutionOutcome{
		payer: env.lamports(t, txfixture.PayerPubkey()), dest: env.lamports(t, txfixture.DestPubkey()),
		fees: env.exec.txFeeAccumulator.TotalFees, cu: env.exec.totalCU,
		processed: env.exec.processedTxCount, sigs: env.exec.processedSignatures,
		executeMask: append([]bool(nil), env.exec.execute...), modified: env.modifiedKeys(),
		parentPayerLamports: parent.Lamports,
	}
}

func requireBlockMetadataOrder(t *testing.T, txs []*solana.Transaction, records []*transactionMetadata) {
	t.Helper()
	require.Len(t, records, len(txs))
	for i, tx := range txs {
		wire, err := tx.MarshalBinary()
		require.NoError(t, err)
		record := records[i]
		require.NotNil(t, record)
		require.Equal(t, wire, record.Transaction, "transaction %d wire order", i)
		require.Equal(t, []solana.PublicKey(tx.Message.AccountKeys), record.AccountKeys)
		require.Equal(t, uint64(5_000), record.Fee)
		require.Empty(t, record.PreTokenBalances)
		require.Empty(t, record.PostTokenBalances)
		require.Equal(t, transactionTokenSnapshot{}, record.preTokens)
		require.Equal(t, transactionTokenSnapshot{}, record.postTokens)
		if i > 0 {
			require.Equal(t, records[i-1].PostBalances, record.PreBalances, "transaction %d committed boundary", i)
		}
		if i < 2 {
			require.JSONEq(t, "null", string(record.Error))
		} else {
			require.JSONEq(t, `{"InsufficientFundsForRent":{"account_index":0}}`, string(record.Error))
		}
	}
}

func TestBlockMetadataGroupExecutionParity(t *testing.T) {
	txs := transferTransactions(t, 12, 999_000)
	for _, workers := range []int{0, 1, 4} {
		for _, splits := range [][]int{nil, {1, 3, 7}, {0, 2, 2, 12}} {
			t.Run(fmt.Sprintf("workers%d/splits%v", workers, splits), func(t *testing.T) {
				want := runGroups(t, workers, 3_200_000, txs, splits)
				env := newGroupExecutionEnv(t, workers, 3_200_000)
				defer env.cleanup()
				var capture blockMetadataCapture
				env.exec.metadata = &capture
				start := 0
				for _, end := range append(append([]int(nil), splits...), len(txs)) {
					require.NoError(t, env.exec.executeTransactionGroup(txs[start:end], nil, false))
					start = end
				}
				require.Equal(t, want, blockMetadataGroupOutcome(t, env))
				require.NoError(t, capture.Err)
				require.Nil(t, capture.Record, "joined groups are not a completed bank")
				requireBlockMetadataOrder(t, txs, capture.records)
				env.exec.close()
				require.ErrorContains(t, capture.Err, "did not finish")
				require.Nil(t, capture.records)
			})
		}
	}
}

func TestBlockMetadataFailureDoesNotChangeGroupExecution(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			txs := transferTransactions(t, 4, 999_000)
			tokenKey, mintKey := solana.PublicKey{0xD1}, solana.PublicKey{0xD2}
			// These extra read-only keys do not change the System transfer. The
			// absent external mint is read only by metadata hydration.
			txs[2].Message.AccountKeys = append(txs[2].Message.AccountKeys, tokenKey, solana.TokenProgramID)
			txs[2].Message.Header.NumReadonlyUnsignedAccounts += 2
			key := txfixture.PayerPrivateKey()
			_, err := txs[2].Sign(func(pk solana.PublicKey) *solana.PrivateKey {
				if pk == key.PublicKey() {
					return &key
				}
				return nil
			})
			require.NoError(t, err)
			verifiedIdentities(t, txs)
			readErr := errors.New("external mint read failed")
			var outcomes [2]groupExecutionOutcome
			for enabled := range 2 {
				env := newGroupExecutionEnv(t, workers, 3_200_000)
				defer env.cleanup()
				require.NoError(t, env.source.mem.SetAccountWithoutLock(tokenKey,
					tokenCaptureAccount(tokenKey, mintKey, solana.TokenProgramID, 17, 165)))
				reads := 0
				env.exec.slotCtx.UnrootedRead = metadataAccountReader(func(_ uint64, key solana.PublicKey) (*accounts.Account, error) {
					if key == mintKey {
						reads++
						return nil, readErr
					}
					return nil, accountsdb.ErrNoAccount
				})
				var capture blockMetadataCapture
				if enabled == 1 {
					env.exec.metadata = &capture
				}
				require.NoError(t, env.exec.executeTransactionGroup(txs[:2], nil, false))
				if enabled == 1 {
					require.Len(t, capture.records, 2)
				}
				require.NoError(t, env.exec.executeTransactionGroup(txs[2:3], nil, false))
				require.NoError(t, env.exec.executeTransactionGroup(txs[3:], nil, false))
				outcomes[enabled] = blockMetadataGroupOutcome(t, env)
				if enabled == 1 {
					require.ErrorIs(t, capture.Err, readErr)
					require.Nil(t, capture.Record)
					require.Nil(t, capture.records, "earlier groups must not survive incomplete capture")
					require.Equal(t, 1, reads)
				} else {
					require.Zero(t, reads, "disabled capture must not hydrate external mints")
				}
				env.exec.close()
			}
			require.Equal(t, outcomes[0], outcomes[1])
		})
	}
}

func TestBlockMetadataProcessBlockParity(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			env := newLifecycleEnv(t)
			txs := transferTransactions(t, 12, 999_000)
			want := lifecycleWholeBlock(t, env, txs, workers)
			tail := &lifecycleTail{durable: env.durable}
			block := env.block(txs)
			block.MarkTransactionSignaturesVerified()
			var capture blockMetadataCapture
			ctx, err := processBlock(env.acctsDb, block, env.epochSchedule, workers, nil,
				&persistedTracker{}, tail, NewTransactionStatusCache(), false, env.parent, &capture)
			require.NoError(t, err)
			got := lifecycleOutcomeOf(t, ctx, tail)
			requireSameLifecycleOutcome(t, want, got)
			require.Equal(t, want.delta, got.delta, "all committed account fields")
			require.NoError(t, capture.Err)
			require.NotNil(t, capture.Record)
			require.Nil(t, capture.records)
			require.Equal(t, block.Slot, capture.Record.Slot)
			require.Equal(t, block.ParentSlot, capture.Record.ParentSlot)
			require.Equal(t, block.Blockhash, capture.Record.Blockhash)
			require.Equal(t, block.ParentBankhash, capture.Record.ParentBankhash)
			require.Equal(t, ctx.FinalBankhash, capture.Record.Bankhash[:])
			requireBlockMetadataOrder(t, txs, capture.Record.Transactions)
		})
	}
}

func TestBlockMetadataIndependentTransactions(t *testing.T) {
	const count = 8
	txs := make([]*solana.Transaction, count)
	for i := range txs {
		key := solana.NewWallet().PrivateKey
		tx, err := solana.NewTransaction([]solana.Instruction{
			system.NewTransferInstruction(uint64(i+1), key.PublicKey(), solana.PublicKey{0xE1, byte(i)}).Build(),
		}, txfixture.TestBlockhash(), solana.TransactionPayer(key.PublicKey()))
		require.NoError(t, err)
		_, err = tx.Sign(func(pk solana.PublicKey) *solana.PrivateKey {
			if pk == key.PublicKey() {
				return &key
			}
			return nil
		})
		require.NoError(t, err)
		txs[i] = tx
	}
	verifiedIdentities(t, txs)
	var states [2]map[solana.PublicKey]*accounts.Account
	var computeUnits [2]uint64
	for enabled := range 2 {
		env := newGroupExecutionEnv(t, 4, 10_000_000)
		defer env.cleanup()
		for _, tx := range txs {
			for _, key := range tx.Message.AccountKeys[:2] {
				require.NoError(t, env.source.mem.SetAccountWithoutLock(key, &accounts.Account{
					Key: key, Owner: solana.SystemProgramID, Lamports: 10_000_000, RentEpoch: ^uint64(0),
				}))
			}
		}
		var capture blockMetadataCapture
		if enabled == 1 {
			env.exec.metadata = &capture
		}
		require.NoError(t, env.exec.executeTransactionGroup(txs, nil, false))
		require.Equal(t, uint64(count*5_000), env.exec.txFeeAccumulator.TotalFees)
		computeUnits[enabled] = env.exec.totalCU
		states[enabled] = make(map[solana.PublicKey]*accounts.Account)
		for i, tx := range txs {
			for _, key := range tx.Message.AccountKeys {
				account, err := env.exec.slotCtx.GetAccountShared(key)
				require.NoError(t, err)
				states[enabled][key] = account.Clone()
			}
			if enabled == 1 {
				wire, err := tx.MarshalBinary()
				require.NoError(t, err)
				require.NoError(t, capture.Err)
				require.Len(t, capture.records, count)
				record := capture.records[i]
				require.Equal(t, wire, record.Transaction)
				require.JSONEq(t, "null", string(record.Error))
				require.Equal(t, []uint64{10_000_000, 10_000_000, 1}, record.PreBalances)
				require.Equal(t, []uint64{10_000_000 - 5_000 - uint64(i+1), 10_000_000 + uint64(i+1), 1}, record.PostBalances)
			}
		}
		env.exec.close()
	}
	require.Equal(t, states[0], states[1])
	require.Equal(t, computeUnits[0], computeUnits[1])
}

func TestBlockMetadataStreamFinalization(t *testing.T) {
	previous := StreamingExecutionCfg
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	t.Cleanup(func() { StreamingExecutionCfg = previous })
	env := newLifecycleEnv(t)
	txs := transferTransactions(t, 12, 999_000)
	tail := &lifecycleTail{durable: env.durable}
	block := env.block(txs)
	block.MarkTransactionSignaturesVerified()
	var whole blockMetadataCapture
	ctx, err := processBlock(env.acctsDb, block, env.epochSchedule, 4, nil,
		&persistedTracker{}, tail, NewTransactionStatusCache(), false, env.parent, &whole)
	require.NoError(t, err)
	require.NoError(t, whole.Err)
	require.NotNil(t, whole.Record)
	want := lifecycleOutcomeOf(t, ctx, tail)

	for _, tc := range []struct {
		name   string
		splits []int
		suffix int
	}{{"one group", nil, 0}, {"several groups", []int{3, 7}, 0},
		{"groups and suffix", []int{4}, 3}, {"only suffix", nil, 12},
		{"single transactions", []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			disabled, _ := lifecycleStream(t, env, txs, tc.splits, tc.suffix, 4)
			var capture blockMetadataCapture
			got, _ := lifecycleStreamCapture(t, env, txs, tc.splits, tc.suffix, 4, &capture)
			requireSameLifecycleOutcome(t, want, got)
			require.Equal(t, disabled, got, "streaming capture off/on")
			require.Equal(t, want.delta, got.delta)
			require.NoError(t, capture.Err)
			require.Equal(t, whole.Record, capture.Record, "complete ordered record survives accepted stream closure")
			require.Nil(t, capture.records)
		})
	}
}

func TestBlockMetadataDisabledGuardsDoNotAllocate(t *testing.T) {
	var capture *blockMetadataCapture
	var group *transactionMetadataGroup
	allocations := testing.AllocsPerRun(100, func() {
		group = capture.beginGroup(nil, nil, 0)
		group.transaction(0)
		capture.finishGroup(group)
		capture.seal(nil, nil)
	})
	require.Nil(t, group)
	require.Zero(t, allocations, "nil capture guards, not the full execution path")
}

func TestBlockMetadataDiscardAndSealGuards(t *testing.T) {
	h := newStreamingTestHarness(t)
	var capture blockMetadataCapture
	h.env.exec.metadata = &capture
	require.NoError(t, h.env.exec.executeTransactionGroup(transferTransactions(t, 2, 100), nil, false))
	require.Len(t, capture.records, 2)
	h.exec.discard("metadata capture test")
	require.True(t, h.env.exec.closed)
	require.ErrorContains(t, capture.Err, "did not finish")
	require.Nil(t, capture.Record)
	require.Nil(t, capture.records)

	// These are structural guards only; successful bank sealing is exercised
	// through the actual ProcessBlock tail above, not a fabricated bank hash.
	for _, tc := range []struct {
		name    string
		count   int
		hashLen int
	}{{"missing transaction", 0, 32}, {"missing bank hash", 1, 0}, {"short bank hash", 1, 31}} {
		t.Run(tc.name, func(t *testing.T) {
			guard := blockMetadataCapture{records: make([]*transactionMetadata, tc.count)}
			guard.seal(&b.Block{Transactions: make([]*solana.Transaction, 1)}, &sealevel.SlotCtx{FinalBankhash: make([]byte, tc.hashLen)})
			require.ErrorContains(t, guard.Err, "incomplete")
			require.Nil(t, guard.Record)
			require.Nil(t, guard.records)
		})
	}
}
